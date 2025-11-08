use crate::engines::{Engine, EngineConfig};
use crate::error::{Error, Result};
use crate::storage::{Column, ColumnDef};

#[allow(dead_code)]
pub struct MergeTreeEngine {
    config: EngineConfig,
}

impl MergeTreeEngine {
    pub const fn new(config: EngineConfig) -> Self {
        Self { config }
    }
}

impl Engine for MergeTreeEngine {
    fn name(&self) -> &'static str {
        "MergeTree"
    }

    fn order_columns(
        &self,
        mut columns: Vec<Column>,
        order_by: &[ColumnDef],
    ) -> Result<Vec<Column>> {
        if order_by.is_empty() || columns.is_empty() {
            return Err(Error::NoColumnsSpecified);
        }

        let row_count = columns[0].data.len();

        if columns.iter().any(|col| col.data.len() != row_count) {
            return Err(Error::InvalidColumnsSpecified);
        }

        let order_by_indices: Vec<usize> = order_by
            .iter()
            .filter_map(|order_col| columns.iter().position(|col| col.column_def == *order_col))
            .collect();

        if order_by_indices.is_empty() {
            return Err(Error::NoColumnsSpecified);
        }

        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_by(|&a, &b| {
            for &col_idx in &order_by_indices {
                let col_a = &columns[col_idx].data[a];
                let col_b = &columns[col_idx].data[b];

                let cmp = compare_values(col_a, col_b);
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        for column in &mut columns {
            let mut sorted_data = Vec::with_capacity(row_count);
            for &idx in &indices {
                sorted_data.push(column.data[idx].clone());
            }
            column.data = sorted_data;
        }

        Ok(columns)
    }
}

fn compare_values(a: &crate::storage::Value, b: &crate::storage::Value) -> std::cmp::Ordering {
    use crate::storage::Value;

    match (a, b) {
        (Value::Int8(x), Value::Int8(y)) => x.cmp(y),
        (Value::Int16(x), Value::Int16(y)) => x.cmp(y),
        (Value::Int32(x), Value::Int32(y)) => x.cmp(y),
        (Value::Int64(x), Value::Int64(y)) => x.cmp(y),
        (Value::UInt8(x), Value::UInt8(y)) => x.cmp(y),
        (Value::UInt16(x), Value::UInt16(y)) => x.cmp(y),
        (Value::UInt32(x), Value::UInt32(y)) => x.cmp(y),
        (Value::UInt64(x), Value::UInt64(y)) => x.cmp(y),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Uuid(x), Value::Uuid(y)) => x.cmp(y),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}
