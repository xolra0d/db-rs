use scc::Guard;
use sqlparser::ast::{BinaryOperator, Expr, Value as SQLValue};
use std::collections::HashMap;
use std::collections::hash_map::Entry;

use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::CommandRunner;
use crate::storage::Mark;
use crate::storage::value::compare_values;
use crate::storage::{Column, ColumnDef, OutputTable, TableDef, Value};

impl CommandRunner {
    /// Checks if a mark should be read based on the filter expression.
    /// Returns true if the mark might contain matching rows.
    fn should_read_mark(expr: &Expr, mark: &Mark, order_by_columns: &[ColumnDef]) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if let (Expr::Identifier(ident), Expr::Value(val)) = (left.as_ref(), right.as_ref())
                    && let Some(order_idx) =
                        order_by_columns.iter().position(|c| c.name == ident.value)
                    && order_idx < mark.index.len()
                {
                    let mark_value = &mark.index[order_idx];
                    let filter_value = Self::sql_value_to_value(&val.value)?;

                    return match op {
                        BinaryOperator::Eq => {
                            // For equality, skip marks where min > value (all rows are too large)
                            Ok(compare_values(mark_value, &filter_value)?
                                != std::cmp::Ordering::Greater)
                        }
                        BinaryOperator::NotEq => Err(Error::UnsupportedCommand(
                            "Currently unsupported.".to_string(),
                        )),
                        BinaryOperator::Lt => {
                            // For <, skip marks where min >= value (all rows are too large or equal)
                            Ok(compare_values(mark_value, &filter_value)?
                                == std::cmp::Ordering::Less)
                        }
                        BinaryOperator::LtEq => {
                            // For <=, skip marks where min > value (all rows are too large)
                            Ok(compare_values(mark_value, &filter_value)?
                                != std::cmp::Ordering::Greater)
                        }
                        BinaryOperator::Gt | BinaryOperator::GtEq => {
                            // For > and >=, we can't skip any marks since we only have min values
                            // and any mark could contain rows satisfying the condition
                            Ok(true)
                        }
                        _ => Ok(true),
                    };
                }
                Ok(true)
            }
            _ => Ok(true),
        }
    }

    /// Converts sqlparser Value to our Value type.
    fn sql_value_to_value(val: &SQLValue) -> Result<Value> {
        match val {
            SQLValue::SingleQuotedString(s)
            | SQLValue::TripleSingleQuotedString(s)
            | SQLValue::TripleDoubleQuotedString(s) => Ok(Value::String(s.clone())),
            SQLValue::Number(number, _) => {
                if let Ok(i) = number.parse::<i64>() {
                    Ok(Value::Int64(i))
                } else if let Ok(i) = number.parse::<u64>() {
                    Ok(Value::UInt64(i))
                } else {
                    Err(Error::UnsupportedCommand(
                        "Unsupported number format".to_string(),
                    ))
                }
            }
            SQLValue::Boolean(b) => Ok(Value::Bool(*b)),
            SQLValue::Null => Ok(Value::Null),
            _ => Err(Error::UnsupportedCommand(
                "Unsupported value type".to_string(),
            )),
        }
    }

    /// Evaluates a filter expression for a single row.
    fn evaluate_filter(
        expr: &Expr,
        row_idx: usize,
        columns: &HashMap<String, Column>,
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expr(left, row_idx, columns)?;
                let right_val = Self::evaluate_expr(right, row_idx, columns)?;

                match op {
                    BinaryOperator::Eq => {
                        Ok(compare_values(&left_val, &right_val)? == std::cmp::Ordering::Equal)
                    }
                    BinaryOperator::NotEq => {
                        Ok(compare_values(&left_val, &right_val)? != std::cmp::Ordering::Equal)
                    }
                    BinaryOperator::Lt => {
                        Ok(compare_values(&left_val, &right_val)? == std::cmp::Ordering::Less)
                    }
                    BinaryOperator::Gt => {
                        Ok(compare_values(&left_val, &right_val)? == std::cmp::Ordering::Greater)
                    }
                    BinaryOperator::LtEq => {
                        Ok(compare_values(&left_val, &right_val)? != std::cmp::Ordering::Greater)
                    }
                    BinaryOperator::GtEq => {
                        Ok(compare_values(&left_val, &right_val)? != std::cmp::Ordering::Less)
                    }
                    _ => Err(Error::UnsupportedCommand(format!(
                        "Unsupported operator: {:?}",
                        op
                    ))),
                }
            }
            _ => Err(Error::UnsupportedCommand(
                "Only binary operations supported in WHERE".to_string(),
            )),
        }
    }

    /// Evaluates an expression to a Value.
    fn evaluate_expr(
        expr: &Expr,
        row_idx: usize,
        columns: &HashMap<String, Column>,
    ) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let col = columns.get(&ident.value).ok_or_else(|| {
                    Error::UnsupportedCommand(format!("Column '{}' not found", ident.value))
                })?;
                col.data
                    .get(row_idx)
                    .cloned()
                    .ok_or_else(|| Error::CouldNotReadData("Row index out of bounds".to_string()))
            }
            Expr::Value(val) => Self::sql_value_to_value(&val.value),
            _ => Err(Error::UnsupportedCommand(
                "Unsupported expression in WHERE".to_string(),
            )),
        }
    }

    /// Executes SELECT operation by reading column data from all table parts.
    ///
    /// Reads requested columns from each part and merges them into result.
    ///
    /// Returns:
    ///   * Ok: OutputTable with selected columns
    ///   * Error: TableNotFound or CouldNotReadData on read failure
    pub fn select(
        table_def: TableDef,
        column_defs: Vec<ColumnDef>,
        filter: Option<Box<Expr>>,
    ) -> Result<OutputTable> {
        let guard = Guard::new();
        let Some(table_config) = TABLE_DATA.peek(&table_def, &guard) else {
            return Err(Error::TableNotFound);
        };

        let mut columns_to_read = column_defs.clone();
        if let Some(ref filter_expr) = filter {
            Self::extract_filter_columns(
                filter_expr,
                &table_config.metadata.schema.columns,
                &mut columns_to_read,
            )?;
        }

        let mut column_map: HashMap<String, Column> = HashMap::new();

        for column_def in &columns_to_read {
            for part_info in &table_config.infos {
                if part_info.column_defs.contains(column_def) {
                    let col_idx = part_info
                        .column_defs
                        .iter()
                        .position(|def| def == column_def)
                        .ok_or_else(|| {
                            Error::CouldNotReadData("Column not found in part".to_string())
                        })?;

                    let mark_infos: Vec<_> = if let Some(ref filter_expr) = filter {
                        part_info
                            .marks
                            .iter()
                            .filter_map(|mark| {
                                match Self::should_read_mark(
                                    filter_expr,
                                    mark,
                                    &table_config.metadata.schema.order_by,
                                )
                                .ok()?
                                {
                                    true => mark.info.get(col_idx).cloned(),
                                    false => None,
                                }
                            })
                            .collect()
                    } else {
                        part_info
                            .marks
                            .iter()
                            .filter_map(|mark| mark.info.get(col_idx).cloned())
                            .collect()
                    };

                    let column = part_info.read_column(&table_def, column_def, &mark_infos)?;

                    match column_map.entry(column.column_def.name.clone()) {
                        Entry::Occupied(mut e) => e.get_mut().data.extend(column.data),
                        Entry::Vacant(e) => {
                            e.insert(column);
                        }
                    }
                } else {
                    let null_row_count = if let Some(ref filter_expr) = filter {
                        part_info
                            .marks
                            .iter()
                            .filter(|mark| {
                                Self::should_read_mark(
                                    filter_expr,
                                    mark,
                                    &table_config.metadata.schema.order_by,
                                )
                                .unwrap_or(true)
                            })
                            .map(|_| table_config.metadata.settings.index_granularity as usize)
                            .sum::<usize>()
                            .min(part_info.row_count as usize)
                    } else {
                        part_info.row_count as usize
                    };

                    let null_column = Column {
                        column_def: column_def.clone(),
                        data: vec![Value::Null; null_row_count],
                    };

                    match column_map.entry(column_def.name.clone()) {
                        Entry::Occupied(mut e) => e.get_mut().data.extend(null_column.data),
                        Entry::Vacant(e) => {
                            e.insert(null_column);
                        }
                    }
                }
            }
        }

        if let Some(filter_expr) = filter {
            let row_count = column_map
                .values()
                .next()
                .map(|c| c.data.len())
                .unwrap_or(0);
            let mut keep_rows = Vec::with_capacity(row_count);

            for row_idx in 0..row_count {
                keep_rows.push(Self::evaluate_filter(&filter_expr, row_idx, &column_map)?);
            }

            for col in column_map.values_mut() {
                let filtered_data: Vec<Value> = col
                    .data
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if keep_rows[idx] {
                            Some(val.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                col.data = filtered_data;
            }
        }

        let result_columns = column_defs
            .iter()
            .filter_map(|def| column_map.remove(&def.name))
            .collect();

        Ok(OutputTable::new(result_columns))
    }

    /// Extracts column names referenced in filter expression and adds them to columns_to_read if not already present.
    fn extract_filter_columns(
        expr: &Expr,
        schema_columns: &[ColumnDef],
        columns_to_read: &mut Vec<ColumnDef>,
    ) -> Result<()> {
        match expr {
            Expr::Identifier(ident) => {
                let col = schema_columns
                    .iter()
                    .find(|c| c.name == ident.value)
                    .ok_or_else(|| {
                        Error::UnsupportedCommand(format!(
                            "Column '{}' not found in schema",
                            ident.value
                        ))
                    })?;
                if !columns_to_read.contains(col) {
                    columns_to_read.push(col.clone());
                }
                Ok(())
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::extract_filter_columns(left, schema_columns, columns_to_read)?;
                Self::extract_filter_columns(right, schema_columns, columns_to_read)?;
                Ok(())
            }
            Expr::Value(_) => Ok(()),
            _ => Err(Error::UnsupportedCommand(
                "Unsupported expression in WHERE clause".to_string(),
            )),
        }
    }
}
