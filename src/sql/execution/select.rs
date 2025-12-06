use crate::engines::{EngineConfig, EngineName};
use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::CommandRunner;
use crate::sql::compiled_filter::{BinOp, CompiledFilter};
use crate::sql::sql_parser::ScanSource;
use crate::storage::value::ArchivedValue;
use crate::storage::{Column, ColumnDef, Mark, OutputTable, TableDef, TablePartInfo, Value};
use std::cell::RefCell;

use rayon::prelude::*;
use rkyv::vec::ArchivedVec;
use sqlparser::ast::Expr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

thread_local! {
    static LOCAL_BUFFER: RefCell<Vec<Vec<Value>>> = const { RefCell::new(Vec::new()) };
}

struct ScanConfig {
    result: Arc<RwLock<Vec<Column>>>,
    infos: Vec<TablePartInfo>,
    use_filter_optimization: bool,
    compiled_filter: Option<CompiledFilter>,
    table_col_defs: Vec<ColumnDef>,
    pk_col_defs: Vec<ColumnDef>,
    result_col_defs: Vec<ColumnDef>,
    index_granularity: usize,
    table_def: TableDef,
    limit: Option<u64>,
    offset: u64,
}

impl CommandRunner {
    /// Executes SELECT operation by scanning all table parts.
    ///
    /// Reads all table parts, optionally filters and orders data.
    ///
    /// Returns:
    ///   * Ok: `OutputTable` with success status
    ///   * Error: `TableNotFound`, `CouldNotReadData` or `Internal` on failure
    pub fn select(
        table_def: ScanSource,
        columns_to_read: Vec<ColumnDef>,
        filter: Option<Box<Expr>>,
        order_by: Option<&Vec<Vec<ColumnDef>>>,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<OutputTable> {
        let table_def = match table_def {
            ScanSource::Table(table_def) => table_def,
            ScanSource::Subquery(_) => {
                return Err(Error::Internal(
                    "Subqueries should've been removed during optimization. Cannot proceed"
                        .to_string(),
                ));
            }
        };
        let Some(table_config) = TABLE_DATA.get(&table_def) else {
            return Err(Error::TableNotFound);
        };
        let index_granularity = table_config.metadata.settings.index_granularity as usize;

        let avg_rows = Self::estimate_avg_rows(limit, index_granularity);

        let mut result = Vec::new();
        Self::add_columns(&mut result, columns_to_read.clone(), avg_rows);

        let mut compiled_filter = None;
        let mut use_filter_optimization = false;

        if let Some(filter) = filter {
            let filter = CompiledFilter::compile(*filter, &table_config.metadata.schema.columns)?;

            let mut columns_to_filter = Vec::new();

            filter.get_column_defs(&mut columns_to_filter);
            compiled_filter = Some(filter);

            let columns_to_filter: Vec<_> = columns_to_filter
                .into_iter()
                .map(|col_idx| table_config.metadata.schema.columns[col_idx].clone())
                .collect();

            // TODO: allow partial cmp, e.g., part is in PK, part is not.
            if columns_to_filter
                .iter()
                .all(|col_def| table_config.metadata.schema.primary_key.contains(col_def))
            {
                use_filter_optimization = true;
            }
            Self::add_columns(&mut result, columns_to_filter, avg_rows);
        }

        if let Some(order_by) = &order_by {
            Self::add_columns(
                &mut result,
                order_by.iter().flatten().cloned().collect(),
                avg_rows,
            );
        }

        let result_col_defs: Vec<_> = result.iter().map(|col| col.column_def.clone()).collect();
        let result = Arc::new(RwLock::new(result));

        Self::scan_table_parts(ScanConfig {
            result: Arc::clone(&result),
            infos: table_config.infos.clone(),
            use_filter_optimization,
            compiled_filter,
            table_col_defs: table_config.metadata.schema.columns.clone(),
            pk_col_defs: table_config.metadata.schema.primary_key.clone(),
            result_col_defs,
            index_granularity,
            table_def: table_def.clone(),
            limit,
            offset,
        })?;

        let result = Arc::try_unwrap(result)
            .map_err(|_| {
                Error::Internal("Some threads are leaked and have not finished.".to_string())
            })?
            .into_inner()
            .map_err(|error| Error::Internal(format!("Failed to get inner Arc data: {error}")))?;

        let result = Self::apply_post_processing(
            result,
            order_by,
            &table_config.metadata.settings.engine,
            &table_config.metadata.schema.primary_key,
            &columns_to_read,
            limit,
            offset,
        )?;

        Ok(OutputTable::new(result))
    }

    fn load_values<'a>(
        marks: &'a [Mark],
        pk_col_defs: &[ColumnDef],
        col_def: &ColumnDef,
    ) -> Vec<&'a Value> {
        marks
            .iter()
            .map(|mark| {
                let idx = pk_col_defs
                    .iter()
                    .position(|pk_col_def| pk_col_def == col_def);
                if let Some(idx) = idx {
                    &mark.index[idx]
                } else {
                    &Value::Null
                }
            })
            .collect()
    }

    fn parse_complex_filter_granule(
        marks: &[Mark],
        filter: &CompiledFilter,
        pk_col_defs: &[ColumnDef],
        table_col_defs: &[ColumnDef],
    ) -> Vec<usize> {
        match filter {
            CompiledFilter::Compare { col_idx, op, value } => {
                let values = Self::load_values(marks, pk_col_defs, &table_col_defs[*col_idx]);

                match *op {
                    BinOp::Eq => {
                        let start = values.partition_point(|&v| v < value);
                        let start = start.saturating_sub(1);
                        let end = values.partition_point(|&v| v <= value);
                        (start..end).collect()
                    }
                    BinOp::NotEq => (0..marks.len()).collect(), // cannot determine if it's present without reading
                    BinOp::Lt => {
                        let end = values.partition_point(|&v| v < value);
                        (0..end).collect()
                    }
                    BinOp::LtEq => {
                        let end = values.partition_point(|&v| v <= value);
                        (0..end).collect()
                    }
                    BinOp::Gt => {
                        let start = values.partition_point(|&v| v <= value);
                        let start = start.saturating_sub(1);
                        (start..marks.len()).collect()
                    }
                    BinOp::GtEq => {
                        let start = values.partition_point(|&v| v < value);
                        let start = start.saturating_sub(1);
                        (start..marks.len()).collect()
                    }
                }
            }
            CompiledFilter::CompareColumns {
                left_idx,
                op,
                right_idx,
            } => {
                let left_values = Self::load_values(marks, pk_col_defs, &table_col_defs[*left_idx]);
                let right_values =
                    Self::load_values(marks, pk_col_defs, &table_col_defs[*right_idx]);

                left_values
                    .into_iter()
                    .zip(right_values)
                    .enumerate()
                    .filter_map(|(idx, (a, b))| {
                        if CompiledFilter::cmp_vals(a, b, op) {
                            Some(idx)
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            CompiledFilter::Or(a, b) => {
                let mut left =
                    Self::parse_complex_filter_granule(marks, a, pk_col_defs, table_col_defs);
                let right =
                    Self::parse_complex_filter_granule(marks, b, pk_col_defs, table_col_defs);

                for i in right {
                    if !left.contains(&i) {
                        left.push(i);
                    }
                }

                left
            }
            CompiledFilter::And(a, b) => {
                let mut left =
                    Self::parse_complex_filter_granule(marks, a, pk_col_defs, table_col_defs);
                let right =
                    Self::parse_complex_filter_granule(marks, b, pk_col_defs, table_col_defs);

                left.retain(|idx| right.contains(idx));
                left
            }
            CompiledFilter::Not(inner) => {
                let result =
                    Self::parse_complex_filter_granule(marks, inner, pk_col_defs, table_col_defs);
                (0..marks.len()).filter(|x| !result.contains(x)).collect()
            }
            CompiledFilter::Const(value) => {
                if *value {
                    (0..marks.len()).collect()
                } else {
                    Vec::new()
                }
            }
            CompiledFilter::Column(col_idx) => {
                let left_values = Self::load_values(marks, pk_col_defs, &table_col_defs[*col_idx]);

                left_values
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, &value)| {
                        if let Value::Bool(val) = value
                            && !*val
                        {
                            None
                        } else {
                            Some(idx)
                        }
                    })
                    .collect()
            }
        }
    }

    fn estimate_avg_rows(limit: Option<u64>, index_granularity: usize) -> usize {
        if let Some(limit) = limit {
            (limit as usize).min(5 * index_granularity)
        } else {
            5 * index_granularity
        }
    }

    fn add_columns(result: &mut Vec<Column>, columns_defs: Vec<ColumnDef>, avg_rows: usize) {
        for column_def in columns_defs {
            if !result.iter().any(|col| col.column_def == column_def) {
                result.push(Column {
                    column_def,
                    data: Vec::with_capacity(avg_rows),
                });
            }
        }
    }

    fn scan_table_parts(config: ScanConfig) -> Result<()> {
        let ScanConfig {
            result,
            infos,
            use_filter_optimization,
            compiled_filter,
            table_col_defs,
            pk_col_defs,
            result_col_defs,
            index_granularity,
            table_def,
            limit,
            offset,
        } = config;

        let table_col_defs = &table_col_defs;
        let pk_col_defs = &pk_col_defs;
        let table_def = &table_def;
        let should_stop = Arc::new(AtomicBool::new(false));
        let result_col_defs = Arc::new(result_col_defs);
        let total_len = Arc::new(AtomicUsize::new(0));

        for part_info in &infos {
            if should_stop.load(Ordering::Relaxed) {
                break;
            }

            let mut file_mmaps = Vec::with_capacity(part_info.column_defs.len());

            for col_def in &part_info.column_defs {
                let mmap = Column::open_as_mmap(&part_info.get_column_path(table_def, col_def))?;
                Column::validate_mmap(&mmap, &col_def.name)?;

                file_mmaps.push(mmap);
            }

            let file_mmaps = Arc::new(file_mmaps);

            let marks_to_scan: Vec<_> =
                if use_filter_optimization && let Some(compiled_filter) = &compiled_filter {
                    let marks_indexes = Self::parse_complex_filter_granule(
                        &part_info.marks,
                        compiled_filter,
                        pk_col_defs,
                        table_col_defs,
                    );
                    marks_indexes
                        .into_iter()
                        .map(|mark_idx| &part_info.marks[mark_idx].info)
                        .collect()
                } else {
                    part_info.marks.iter().map(|mark| &mark.info).collect()
                };
            if should_stop.load(Ordering::Relaxed) {
                break;
            }

            marks_to_scan
                .par_chunks(10)
                .try_for_each(|chunk_granule_marks| {
                    LOCAL_BUFFER.with(|buffer| {
                        let mut buffer = buffer.borrow_mut();
                        *buffer = vec![Vec::with_capacity(index_granularity); result_col_defs.len()];
                    });

                    if should_stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }

                    let mut granule_buffer = GranuleBuffer {
                        data_bytes: vec![None; result_col_defs.len()],
                        mask: Vec::with_capacity(index_granularity),
                    };

                    for &granule_marks in chunk_granule_marks {
                        if should_stop.load(Ordering::Relaxed) {
                            return Ok(());
                        }

                        let mut row_count = None;

                        for (file_and_col_idx, file_mmap) in file_mmaps.iter().enumerate()
                        {

                            let result_idx = result_col_defs.iter().position(|col_def| {
                                *col_def == part_info.column_defs[file_and_col_idx]
                            });
                            if let Some(result_idx) = result_idx {
                                let granule_bytes = TablePartInfo::get_granule_bytes_decompressed(
                                    file_mmap,
                                    &granule_marks[file_and_col_idx],
                                    &result_col_defs[result_idx].constraints.compression_type,
                                )?;
                                if row_count.is_none() {
                                    row_count = Some(unsafe {
                                        rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(
                                            &granule_bytes,
                                        )
                                        .len()
                                    });
                                }
                                granule_buffer.data_bytes[result_idx] = Some(granule_bytes);
                            }
                        }

                        if let Some(row_count) = row_count {
                            if let Some(compiled_filter) = &compiled_filter {
                                granule_buffer.fill_mask(
                                    compiled_filter,
                                    &result_col_defs,
                                    table_col_defs,
                                    row_count,
                                )?;
                            }

                            let mut archived_values = Vec::with_capacity(granule_buffer.data_bytes.len());

                            for col in &granule_buffer.data_bytes {
                                if let Some(col_bytes) = col {
                                    let values = unsafe {
                                        rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(
                                            col_bytes,
                                        )
                                    };
                                    archived_values.push(Some(values));
                                } else {
                                    archived_values.push(None);
                                }
                            }
                            let allowed_count = granule_buffer.mask.iter().filter(|x| **x).count();
                            if should_stop.load(Ordering::Relaxed) {
                                return Ok(());
                            }

                            for (idx, col_values) in archived_values.iter().enumerate() {
                                let col_values = if let Some(col_values_) = col_values {
                                    let mut res = Vec::with_capacity(col_values_.len());
                                    for (val_idx, col_value) in col_values_.iter().enumerate() {
                                        if granule_buffer.mask.is_empty()
                                            || granule_buffer.mask[val_idx]
                                        {
                                            let col_values =
                                                rkyv::deserialize::<Value, rkyv::rancor::Error>(
                                                    col_value,
                                                )
                                                .map_err(|error| {
                                                    Error::CouldNotReadData(format!("Could not deserialize value in column ({}): {error}", result_col_defs[idx].name))
                                                })?;
                                            res.push(col_values);
                                        }
                                    }

                                    res
                                } else {
                                    vec![Value::Null; allowed_count]
                                };
                                LOCAL_BUFFER.with(|buffer| {
                                    let mut buffer = buffer.borrow_mut();
                                    buffer[idx].extend(col_values);
                                });
                            }

                            total_len.fetch_add(allowed_count, Ordering::Relaxed);

                            if let Some(limit) = limit && total_len.load(Ordering::Relaxed) as u64 >= limit.saturating_add(offset) {
                                    should_stop.store(true, Ordering::Relaxed);
                                    return Ok(());
                            }

                            for archived_vec in &mut granule_buffer.data_bytes {
                                *archived_vec = None;
                            }
                            granule_buffer.mask.clear();
                        }
                    }
                    let mut guard = result.write().map_err(|error| Error::Internal(format!("RwLock poisoning while reading: {error}")))?;
                    for (idx, col) in LOCAL_BUFFER.take().into_iter().enumerate() {
                        guard[idx].data.extend(col);
                    }

                    Ok(())
                })?;
        }

        Ok(())
    }

    fn apply_post_processing(
        mut result: Vec<Column>,
        order_by: Option<&Vec<Vec<ColumnDef>>>,
        engine_name: &EngineName,
        pk_col_defs: &[ColumnDef],
        columns_to_read: &[ColumnDef],
        limit: Option<u64>,
        offset: u64,
    ) -> Result<Vec<Column>> {
        if let Some(sort_by) = &order_by {
            let engine = engine_name.get_engine(EngineConfig::default());
            for sort_by_ in *sort_by {
                result = engine.order_columns(result, sort_by_, pk_col_defs)?;
            }
        }

        result.retain(|col| columns_to_read.contains(&col.column_def));

        let row_count = result.first().map_or(0, |col| col.data.len());

        let offset = offset.min(row_count as u64) as usize;
        for column in &mut result {
            column.data.drain(0..offset);
        }

        if let Some(limit) = limit {
            let limit = limit.min(result.first().map_or(0, |col| col.data.len()) as u64);
            for column in &mut result {
                column.data.truncate(limit as usize);
            }
        }
        Ok(result)
    }
}

#[derive(Debug)]
struct GranuleBuffer {
    data_bytes: Vec<Option<Vec<u8>>>,
    mask: Vec<bool>,
}

impl GranuleBuffer {
    fn fill_mask(
        &mut self,
        filter: &CompiledFilter,
        granule_col_defs: &[ColumnDef],
        table_col_defs: &[ColumnDef],
        row_count: usize,
    ) -> Result<()> {
        let mask = Self::eval_filter_vectorized(
            filter,
            &self.data_bytes,
            granule_col_defs,
            table_col_defs,
            row_count,
        )?;

        self.mask.extend(mask);
        Ok(())
    }

    fn eval_filter_vectorized(
        filter: &CompiledFilter,
        granule_data: &[Option<Vec<u8>>],
        granule_col_defs: &[ColumnDef],
        table_col_defs: &[ColumnDef],
        row_count: usize,
    ) -> Result<Vec<bool>> {
        match filter {
            CompiledFilter::Compare { col_idx, op, value } => {
                let data_idx = granule_col_defs
                    .iter()
                    .position(|col_def| *col_def == table_col_defs[*col_idx]);

                if let Some(data_idx) = data_idx
                    && let Some(col_data) = &granule_data[data_idx]
                {
                    let values =
                        unsafe { rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(col_data) };
                    Ok(values
                        .iter()
                        .map(|row_value| CompiledFilter::cmp_vals(row_value, value, op))
                        .collect())
                } else {
                    Ok(vec![false; row_count])
                }
            }
            CompiledFilter::CompareColumns {
                left_idx,
                op,
                right_idx,
            } => {
                let left_data_idx = granule_col_defs
                    .iter()
                    .position(|col_def| *col_def == table_col_defs[*left_idx]);
                let right_data_idx = granule_col_defs
                    .iter()
                    .position(|col_def| *col_def == table_col_defs[*right_idx]);

                match (left_data_idx, right_data_idx) {
                    (Some(left_idx), Some(right_idx)) => {
                        match (&granule_data[left_idx], &granule_data[right_idx]) {
                            (Some(left_data), Some(right_data)) => {
                                let left_values = unsafe {
                                    rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(left_data)
                                };
                                let right_values = unsafe {
                                    rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(right_data)
                                };
                                Ok(left_values
                                    .iter()
                                    .zip(right_values.iter())
                                    .map(|(left_val, right_val)| {
                                        CompiledFilter::cmp_vals(left_val, right_val, op)
                                    })
                                    .collect())
                            }
                            (Some(left_data), None) => {
                                let left_values = unsafe {
                                    rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(left_data)
                                };

                                Ok(left_values
                                    .iter()
                                    .map(|left_val| {
                                        CompiledFilter::cmp_vals(left_val, &ArchivedValue::Null, op)
                                    })
                                    .collect())
                            }
                            (None, Some(right_data)) => {
                                let right_values = unsafe {
                                    rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(right_data)
                                };

                                Ok(right_values
                                    .iter()
                                    .map(|right_val| {
                                        CompiledFilter::cmp_vals(
                                            &ArchivedValue::Null,
                                            right_val,
                                            op,
                                        )
                                    })
                                    .collect())
                            } // TODO: optimize
                            (None, None) => Ok(vec![false; row_count]),
                        }
                    }
                    (Some(left_idx), None) => {
                        if let Some(left_data) = &granule_data[left_idx] {
                            let left_values = unsafe {
                                rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(left_data)
                            };
                            Ok(left_values
                                .iter()
                                .map(|left_val| {
                                    CompiledFilter::cmp_vals(left_val, &ArchivedValue::Null, op)
                                })
                                .collect())
                        } else {
                            Ok(vec![false; row_count])
                        }
                    }
                    (None, Some(right_idx)) => {
                        if let Some(right_data) = &granule_data[right_idx] {
                            let right_values = unsafe {
                                rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(right_data)
                            };

                            Ok(right_values
                                .iter()
                                .map(|right_val| {
                                    CompiledFilter::cmp_vals(&ArchivedValue::Null, right_val, op)
                                })
                                .collect())
                        } else {
                            Ok(vec![false; row_count])
                        }
                    }
                    (None, None) => Ok(vec![false; row_count]),
                }
            }
            CompiledFilter::And(left, right) => {
                let left_mask = Self::eval_filter_vectorized(
                    left,
                    granule_data,
                    granule_col_defs,
                    table_col_defs,
                    row_count,
                )?;
                let right_mask = Self::eval_filter_vectorized(
                    right,
                    granule_data,
                    granule_col_defs,
                    table_col_defs,
                    row_count,
                )?;

                Ok(left_mask
                    .into_iter()
                    .zip(right_mask)
                    .map(|(l, r)| l && r)
                    .collect())
            }
            CompiledFilter::Or(left, right) => {
                let left_mask = Self::eval_filter_vectorized(
                    left,
                    granule_data,
                    granule_col_defs,
                    table_col_defs,
                    row_count,
                )?;
                let right_mask = Self::eval_filter_vectorized(
                    right,
                    granule_data,
                    granule_col_defs,
                    table_col_defs,
                    row_count,
                )?;

                Ok(left_mask
                    .into_iter()
                    .zip(right_mask)
                    .map(|(l, r)| l || r)
                    .collect())
            }
            CompiledFilter::Not(inner) => {
                let mask = Self::eval_filter_vectorized(
                    inner,
                    granule_data,
                    granule_col_defs,
                    table_col_defs,
                    row_count,
                )?;

                Ok(mask.into_iter().map(|b| !b).collect())
            }
            CompiledFilter::Column(col_idx) => {
                let data_idx = granule_col_defs
                    .iter()
                    .position(|col_def| *col_def == table_col_defs[*col_idx]);

                if let Some(data_idx) = data_idx
                    && let Some(col_data) = &granule_data[data_idx]
                {
                    let values =
                        unsafe { rkyv::access_unchecked::<ArchivedVec<ArchivedValue>>(col_data) };

                    Ok(values
                        .iter()
                        .map(|value| {
                            if let ArchivedValue::Bool(val) = value {
                                *val
                            } else {
                                true
                            }
                        })
                        .collect())
                } else {
                    Ok(vec![false; row_count])
                }
            }
            CompiledFilter::Const(value) => Ok(vec![*value; row_count]),
        }
    }
}
