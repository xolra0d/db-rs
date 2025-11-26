use crate::engines::EngineConfig;
use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::sql_parser::ScanSource;
use crate::sql::{CommandRunner, parse_ident};
use crate::storage::{Column, ColumnDef, Mark, OutputTable, Value};

use sqlparser::ast::{BinaryOperator as BinOp, Expr, UnaryOperator, Value as SQLValue};

impl CommandRunner {
    pub fn select(
        table_def: ScanSource,
        columns_to_read: &[ColumnDef],
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

        let mut result = Vec::with_capacity(columns_to_read.len());

        for column_def in columns_to_read {
            result.push(Column {
                column_def: column_def.clone(),
                data: Vec::new(),
            });
        }

        let mut columns_to_filter = Vec::new();

        let use_filter_optimization = if let Some(ref filter) = filter {
            Self::extract_filter_columns(
                filter,
                &table_config.metadata.schema.columns,
                &mut columns_to_filter,
            )?;

            columns_to_filter
                .iter()
                .all(|col_def| table_config.metadata.schema.primary_key.contains(col_def))
        } else {
            false
        };

        for column_def in &columns_to_filter {
            // TODO: allow partial cmp, e.g., part is in PK, part is not.
            if !result.iter().any(|col| col.column_def == *column_def) {
                result.push(Column {
                    column_def: column_def.clone(),
                    data: Vec::new(),
                });
            }
        }

        if let Some(sort_by) = &order_by {
            for column_def in sort_by.iter().flatten() {
                if !result.iter().any(|col| col.column_def == *column_def) {
                    result.push(Column {
                        column_def: column_def.clone(),
                        data: Vec::new(),
                    });
                }
            }
        }

        for part_info in &table_config.infos {
            let marks_all = if use_filter_optimization {
                let marks_indexes = Self::parse_complex_filter_granule(
                    &part_info.marks,
                    &table_config.metadata.schema.primary_key,
                    *filter.as_ref().unwrap().clone(),
                )?;

                let marks_infos: Vec<_> = marks_indexes
                    .into_iter()
                    .map(|mark_idx| part_info.marks[mark_idx].info.clone())
                    .collect();

                let mut marks_all = vec![vec![]; part_info.column_defs.len()];
                for mark in marks_infos {
                    for idx in 0..part_info.column_defs.len() {
                        marks_all[idx].push(mark[idx].clone());
                    }
                }

                marks_all
            } else {
                let mut marks_all = vec![vec![]; part_info.column_defs.len()];
                for mark in part_info.marks.iter().map(|m| &m.info) {
                    for idx in 0..part_info.column_defs.len() {
                        marks_all[idx].push(mark[idx].clone());
                    }
                }
                marks_all
            };

            for col in &mut result {
                if let Some(storage_idx) = part_info
                    .column_defs
                    .iter()
                    .position(|col_def| col_def == &col.column_def)
                {
                    let marks = &marks_all[storage_idx];
                    let column_data = part_info.read_column(&table_def, &col.column_def, marks)?;
                    col.data.extend(column_data.data);
                } else {
                    col.data
                        .extend(vec![Value::Null; part_info.row_count as usize]);
                }
            }
        }

        if let Some(filter) = filter {
            let row_count = result.first().map_or(0, |col| col.data.len());

            let row_column_defs: Vec<_> = result
                .iter()
                .filter_map(|col| {
                    if columns_to_filter.contains(&col.column_def) {
                        Some(col.column_def.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let mut rows_to_keep = Vec::with_capacity(row_count);
            for row_idx in 0..row_count {
                let row_values: Vec<_> = result
                    .iter()
                    .filter_map(|col| {
                        if columns_to_filter.contains(&col.column_def) {
                            Some(col.data[row_idx].clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if Self::parse_complex_filter_row_is_allowed(
                    &row_column_defs,
                    &row_values,
                    *filter.clone(),
                )? {
                    rows_to_keep.push(row_idx);
                }
            }

            for column in &mut result {
                let mut old_data = std::mem::take(&mut column.data);
                column.data = rows_to_keep
                    .iter()
                    .map(|&idx| std::mem::take(&mut old_data[idx]))
                    .collect();
            }
        }

        if let Some(sort_by) = &order_by {
            let engine = table_config
                .metadata
                .settings
                .engine
                .get_engine(EngineConfig::default());
            for sort_by_ in *sort_by {
                result = engine.order_columns(result, sort_by_)?;
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

        Ok(OutputTable::new(result))
    }

    fn parse_complex_filter_granule(
        marks: &[Mark],
        pk_col_defs: &[ColumnDef],
        filter: Expr,
    ) -> Result<Vec<usize>> {
        match filter {
            Expr::BinaryOp { op, left, right } => match op {
                BinOp::Gt | BinOp::GtEq | BinOp::Lt | BinOp::LtEq | BinOp::Eq | BinOp::NotEq => {
                    Self::binary_cmp_granule(marks, pk_col_defs, op, *left, *right)
                }
                BinOp::And => {
                    let mut left = Self::parse_complex_filter_granule(marks, pk_col_defs, *left)?;
                    let right = Self::parse_complex_filter_granule(marks, pk_col_defs, *right)?;

                    left.retain(|idx| right.contains(idx));

                    Ok(left)
                }
                BinOp::Or => {
                    let mut left = Self::parse_complex_filter_granule(marks, pk_col_defs, *left)?;
                    let right = Self::parse_complex_filter_granule(marks, pk_col_defs, *right)?;

                    left.extend(right);
                    left.sort_unstable();
                    left.dedup();
                    Ok(left)
                }
                _ => Ok(vec![]),
            },
            Expr::UnaryOp { op, expr } => {
                if let UnaryOperator::Not = op {
                    let not_to_include =
                        Self::parse_complex_filter_granule(marks, pk_col_defs, *expr)?;
                    Ok((0..marks.len())
                        .filter(|idx| !not_to_include.contains(idx))
                        .collect())
                } else {
                    Err(Error::InvalidSource)
                }
            }
            Expr::Value(value) => {
                if let SQLValue::Boolean(bool) = value.value {
                    if bool {
                        Ok((0..marks.len()).collect())
                    } else {
                        Ok(Vec::new())
                    }
                } else {
                    Ok((0..marks.len()).collect())
                }
            }
            Expr::Identifier(_) => {
                Ok((0..marks.len()).collect()) // todo: replace when minmax index is introduced
            }
            expr => Err(Error::UnsupportedFilter(format!(
                "Unsupported expression type in filter: {expr}"
            ))),
        }
    }

    fn binary_cmp_granule(
        marks: &[Mark],
        pk_col_defs: &[ColumnDef],
        op: BinOp,
        left: Expr,
        right: Expr,
    ) -> Result<Vec<usize>> {
        match (left, right) {
            (Expr::Identifier(left), Expr::Value(right)) => {
                let left = parse_ident(&left, pk_col_defs)?;
                let right = Value::try_from((right.value, &left.field_type))?;

                if left.field_type != right.get_type() {
                    return Err(Error::InvalidSource);
                }

                let left_index = pk_col_defs
                    .iter()
                    .position(|col| col.name == left.name)
                    .ok_or(Error::InvalidSource)?;

                let values: Vec<&Value> =
                    marks.iter().map(|mark| &mark.index[left_index]).collect();

                let result_indices = match op {
                    BinOp::Eq => {
                        let start = values.partition_point(|&v| v < &right);
                        let start = start.saturating_sub(1);
                        let end = values.partition_point(|&v| v <= &right);
                        (start..end).collect()
                    }
                    BinOp::NotEq => (0..marks.len()).collect(), // cannot determine if it's present without reading
                    BinOp::Lt => {
                        let end = values.partition_point(|&v| v < &right);
                        (0..end).collect()
                    }
                    BinOp::LtEq => {
                        let end = values.partition_point(|&v| v <= &right);
                        (0..end).collect()
                    }
                    BinOp::Gt => {
                        let start = values.partition_point(|&v| v <= &right);
                        let start = start.saturating_sub(1);
                        (start..marks.len()).collect()
                    }
                    BinOp::GtEq => {
                        let start = values.partition_point(|&v| v < &right);
                        let start = start.saturating_sub(1);
                        (start..marks.len()).collect()
                    }
                    _ => {
                        return Err(Error::UnsupportedFilter(format!(
                            "Not supported operator: ({op})",
                        )));
                    }
                };

                Ok(result_indices)
            }
            (Expr::Value(left), Expr::Identifier(right)) => {
                let flipped_op = Self::flip_bin_op_simple(op)?;
                Self::binary_cmp_granule(
                    marks,
                    pk_col_defs,
                    flipped_op,
                    Expr::Identifier(right),
                    Expr::Value(left),
                )
            }
            (Expr::Value(left), Expr::Value(right)) => {
                let left = Self::parse_sql_value(left.value)?;
                let right = Self::parse_sql_value(right.value)?;

                let result = Self::evaluate_binary_op(&op, &left, &right)?;
                if result {
                    Ok((0..marks.len()).collect())
                } else {
                    Ok(Vec::new())
                }
            }
            (left, right) => Err(Error::UnsupportedFilter(format!(
                "invalid filter left and right: ({left}), ({right})"
            ))),
        }
    }

    fn parse_complex_filter_row_is_allowed(
        row_column_defs: &[ColumnDef],
        row_values: &[Value],
        filter: Expr,
    ) -> Result<bool> {
        match filter {
            Expr::BinaryOp { op, left, right } => match op {
                BinOp::Gt | BinOp::GtEq | BinOp::Lt | BinOp::LtEq | BinOp::Eq | BinOp::NotEq => {
                    Self::binary_cmp_row(row_column_defs, row_values, op, *left, *right)
                }
                BinOp::And => {
                    let left = Self::parse_complex_filter_row_is_allowed(
                        row_column_defs,
                        row_values,
                        *left,
                    )?;
                    let right = Self::parse_complex_filter_row_is_allowed(
                        row_column_defs,
                        row_values,
                        *right,
                    )?;

                    Ok(left && right)
                }
                BinOp::Or => {
                    let left = Self::parse_complex_filter_row_is_allowed(
                        row_column_defs,
                        row_values,
                        *left,
                    )?;
                    let right = Self::parse_complex_filter_row_is_allowed(
                        row_column_defs,
                        row_values,
                        *right,
                    )?;

                    Ok(left || right)
                }
                _ => Ok(true),
            },
            Expr::UnaryOp { op, expr } => {
                if let UnaryOperator::Not = op {
                    Self::parse_complex_filter_row_is_allowed(row_column_defs, row_values, *expr)
                        .map(|x| !x)
                } else {
                    Err(Error::InvalidSource)
                }
            }
            Expr::Value(value) => {
                if let SQLValue::Boolean(value) = value.value {
                    Ok(value)
                } else {
                    Ok(true)
                }
            }
            Expr::Identifier(ident) => {
                let Some(column_position) = row_column_defs
                    .iter()
                    .position(|col_def| col_def.name == ident.value)
                else {
                    return Err(Error::InvalidSource);
                };
                let value = row_values
                    .get(column_position)
                    .ok_or(Error::InvalidSource)?;
                if let Value::Bool(value) = value {
                    Ok(*value)
                } else {
                    Ok(true)
                }
            }
            expr => Err(Error::UnsupportedFilter(format!(
                "Unsupported expression type in filter: {expr}"
            ))),
        }
    }

    fn binary_cmp_row(
        row_column_defs: &[ColumnDef],
        row_values: &[Value],
        op: BinOp,
        left: Expr,
        right: Expr,
    ) -> Result<bool> {
        match (left, right) {
            (Expr::Value(left), Expr::Value(right)) => {
                let left = Self::parse_sql_value(left.value)?;
                let right = Self::parse_sql_value(right.value)?;

                Self::evaluate_binary_op(&op, &left, &right)
            }
            (Expr::Identifier(left), Expr::Value(right)) => {
                let left = parse_ident(&left, row_column_defs)?;
                let right = Value::try_from((right.value, &left.field_type))?;

                let Some(left_pos) = row_column_defs.iter().position(|col_def| *col_def == left)
                else {
                    return Err(Error::InvalidSource);
                };
                let left = row_values[left_pos].clone();

                Self::evaluate_binary_op(&op, &left, &right)
            }
            (Expr::Value(left), Expr::Identifier(right)) => {
                let flipped_op = Self::flip_bin_op_simple(op)?;

                Self::binary_cmp_row(
                    row_column_defs,
                    row_values,
                    flipped_op,
                    Expr::Identifier(right),
                    Expr::Value(left),
                )
            }
            (Expr::Identifier(left), Expr::Identifier(right)) => {
                let left = parse_ident(&left, row_column_defs)?;
                let right = parse_ident(&right, row_column_defs)?;

                let Some(left_pos) = row_column_defs.iter().position(|col_def| *col_def == left)
                else {
                    return Err(Error::InvalidSource);
                };
                let left = row_values[left_pos].clone();

                let Some(right_pos) = row_column_defs.iter().position(|col_def| *col_def == right)
                else {
                    return Err(Error::InvalidSource);
                };
                let right = row_values[right_pos].clone();

                Self::evaluate_binary_op(&op, &left, &right)
            }
            _ => Err(Error::InvalidSource),
        }
    }

    fn evaluate_binary_op(op: &BinOp, left: &Value, right: &Value) -> Result<bool> {
        if left.get_type() != right.get_type() {
            return Err(Error::InvalidSource);
        }

        Ok(match op {
            BinOp::Eq => left == right,
            BinOp::NotEq => left != right,
            BinOp::Lt => left < right,
            BinOp::LtEq => left <= right,
            BinOp::Gt => left > right,
            BinOp::GtEq => left >= right,
            _ => {
                return Err(Error::UnsupportedFilter(format!(
                    "not supported operator: ({op})",
                )));
            }
        })
    }

    fn parse_sql_value(value: SQLValue) -> Result<Value> {
        match value {
            SQLValue::Null => Ok(Value::Null),
            SQLValue::SingleQuotedString(s)
            | SQLValue::TripleSingleQuotedString(s)
            | SQLValue::TripleDoubleQuotedString(s) => Ok(Value::String(s)),
            SQLValue::Number(number, _) => Ok(Value::Int64(
                number.parse().map_err(|_| Error::InvalidSource)?,
            )),
            SQLValue::Boolean(b) => Ok(Value::Bool(b)),
            _ => Err(Error::InvalidSource),
        }
    }

    /// Extracts column names referenced in filter expression and adds them to `columns_to_read` if not already present.
    fn extract_filter_columns(
        expr: &Expr,
        columns: &[ColumnDef],
        columns_to_filter: &mut Vec<ColumnDef>,
    ) -> Result<()> {
        match expr {
            Expr::Identifier(ident) => {
                let column_def = parse_ident(ident, columns)?;

                if !columns_to_filter.contains(&column_def) {
                    columns_to_filter.push(column_def);
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::extract_filter_columns(left, columns, columns_to_filter)?;
                Self::extract_filter_columns(right, columns, columns_to_filter)?;
                Ok(())
            }
            Expr::UnaryOp { expr, .. } => {
                Self::extract_filter_columns(expr, columns, columns_to_filter)?;
                Ok(())
            }
            Expr::Value(_) => Ok(()),
            _ => Err(Error::UnsupportedCommand(
                "Unsupported expression in WHERE clause".to_string(),
            )),
        }
    }

    fn flip_bin_op_simple(op: BinOp) -> Result<BinOp> {
        Ok(match op {
            BinOp::Lt => BinOp::Gt,
            BinOp::LtEq => BinOp::GtEq,
            BinOp::Gt => BinOp::Lt,
            BinOp::GtEq => BinOp::LtEq,
            BinOp::Eq | BinOp::NotEq => op,
            _ => return Err(Error::InvalidSource),
        })
    }
}

#[cfg(test)]
mod tests {
    mod parse_single_binary_cmp {
        use crate::error::Result;
        use crate::sql::CommandRunner;
        use crate::storage::{ColumnDef, CompressionType, Mark, Value, ValueType};
        use sqlparser::ast::{
            BinaryOperator as BinOp, Expr, Ident, Value as SQLValue, ValueWithSpan,
        };
        use sqlparser::tokenizer::Span;

        fn mark(index: Vec<Value>) -> Mark {
            Mark {
                index,
                info: Vec::new(),
            }
        }

        fn num(n: i64) -> Expr {
            Expr::Value(ValueWithSpan {
                value: SQLValue::Number(n.to_string(), false),
                span: Span::empty(),
            })
        }

        fn id() -> Expr {
            Expr::Identifier(Ident::new("id"))
        }

        fn cmp(op: BinOp, left: Expr, right: Expr) -> Result<Vec<usize>> {
            let (marks, column_defs) = get_marks_column_defs();
            CommandRunner::binary_cmp_granule(&marks, &column_defs, op, left, right)
        }

        fn get_marks_column_defs() -> (Vec<Mark>, Vec<ColumnDef>) {
            let marks = vec![
                mark(vec![Value::Int64(1)]),
                mark(vec![Value::Int64(8193)]),
                mark(vec![Value::Int64(8193)]),
                mark(vec![Value::Int64(16385)]),
                mark(vec![Value::Int64(24577)]),
            ];

            let column_defs = vec![ColumnDef {
                name: "id".to_string(),
                field_type: ValueType::Int64,
                constraints: vec![],
                compression_type: CompressionType::None,
            }];

            (marks, column_defs)
        }

        #[test]
        fn test_eq() {
            assert_eq!(cmp(BinOp::Eq, id(), num(9000)).unwrap(), [2]);
            assert_eq!(cmp(BinOp::Eq, id(), num(8193)).unwrap(), [0, 1, 2]);
            assert_eq!(cmp(BinOp::Eq, id(), num(30000)).unwrap(), [4]);
            assert_eq!(cmp(BinOp::Eq, id(), num(0)).unwrap(), []);
        }

        #[test]
        fn test_not_eq() {
            assert_eq!(cmp(BinOp::NotEq, id(), num(5)).unwrap(), [0, 1, 2, 3, 4]);
        }

        #[test]
        fn test_gt() {
            assert_eq!(cmp(BinOp::Gt, id(), num(9000)).unwrap(), [2, 3, 4]);
            assert_eq!(cmp(BinOp::Gt, id(), num(8193)).unwrap(), [2, 3, 4]);
            assert_eq!(cmp(BinOp::Gt, id(), num(30000)).unwrap(), [4]);
            assert_eq!(cmp(BinOp::Gt, id(), num(0)).unwrap(), [0, 1, 2, 3, 4]);
        }

        #[test]
        fn test_gt_eq() {
            assert_eq!(cmp(BinOp::GtEq, id(), num(9000)).unwrap(), [2, 3, 4]);
            assert_eq!(cmp(BinOp::GtEq, id(), num(8193)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::GtEq, id(), num(30000)).unwrap(), [4]);
            assert_eq!(cmp(BinOp::GtEq, id(), num(0)).unwrap(), [0, 1, 2, 3, 4]);
        }

        #[test]
        fn test_lt() {
            assert_eq!(cmp(BinOp::Lt, id(), num(9000)).unwrap(), [0, 1, 2]);
            assert_eq!(cmp(BinOp::Lt, id(), num(8193)).unwrap(), [0]);
            assert_eq!(cmp(BinOp::Lt, id(), num(30000)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::Lt, id(), num(0)).unwrap(), []);
        }

        #[test]
        fn test_lt_eq() {
            assert_eq!(cmp(BinOp::LtEq, id(), num(9000)).unwrap(), [0, 1, 2]);
            assert_eq!(cmp(BinOp::LtEq, id(), num(8193)).unwrap(), [0, 1, 2]);
            assert_eq!(cmp(BinOp::LtEq, id(), num(30000)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::LtEq, id(), num(0)).unwrap(), []);
        }

        #[test]
        fn test_lt_eq_gt_eq_rev() {
            assert_eq!(
                cmp(BinOp::LtEq, num(9000), id()).unwrap(),
                cmp(BinOp::GtEq, id(), num(9000)).unwrap()
            );
            assert_eq!(
                cmp(BinOp::LtEq, num(8193), id()).unwrap(),
                cmp(BinOp::GtEq, id(), num(8193)).unwrap()
            );
            assert_eq!(
                cmp(BinOp::GtEq, num(30000), id()).unwrap(),
                cmp(BinOp::LtEq, id(), num(30000)).unwrap()
            );
            assert_eq!(
                cmp(BinOp::GtEq, num(0), id()).unwrap(),
                cmp(BinOp::LtEq, id(), num(0)).unwrap()
            );
        }

        #[test]
        fn test_values_only() {
            assert_eq!(cmp(BinOp::Eq, num(2), num(3)).unwrap(), []);
            assert_eq!(cmp(BinOp::NotEq, num(2), num(2)).unwrap(), []);
            assert_eq!(cmp(BinOp::Gt, num(2), num(2)).unwrap(), []);
            assert_eq!(cmp(BinOp::GtEq, num(1), num(2)).unwrap(), []);
            assert_eq!(cmp(BinOp::Lt, num(2), num(1)).unwrap(), []);
            assert_eq!(cmp(BinOp::LtEq, num(2), num(1)).unwrap(), []);

            assert_eq!(cmp(BinOp::Eq, num(2), num(2)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::NotEq, num(2), num(3)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::Gt, num(3), num(2)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::GtEq, num(2), num(2)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::Lt, num(1), num(2)).unwrap(), [0, 1, 2, 3, 4]);
            assert_eq!(cmp(BinOp::LtEq, num(2), num(3)).unwrap(), [0, 1, 2, 3, 4]);
        }
    }
}
