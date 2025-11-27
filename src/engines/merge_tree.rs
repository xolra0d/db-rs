use crate::engines::{Engine, EngineConfig};
use crate::error::{Error, Result};
use crate::storage::{Column, ColumnDef};

use std::cmp::Ordering;

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
    fn order_columns(
        &self,
        mut columns: Vec<Column>,
        order_by: &[ColumnDef],
        _primary_key: &[ColumnDef],
    ) -> Result<Vec<Column>> {
        if order_by.is_empty() || columns.is_empty() {
            return Err(Error::NoColumnsSpecified);
        }

        let row_count = columns[0].data.len();

        if columns.iter().any(|col| col.data.len() != row_count) {
            return Err(Error::InvalidColumnsSpecified);
        }

        let mut order_by_indices = Vec::with_capacity(order_by.len());
        for order_col in order_by {
            let Some(idx) = columns
                .iter()
                .position(|col| col.column_def.name == order_col.name)
            else {
                return Err(Error::InvalidColumnsSpecified);
            };
            order_by_indices.push(idx);
        }

        let mut indices: Vec<usize> = (0..row_count).collect();

        indices.sort_by(|&a, &b| {
            for &col_idx in &order_by_indices {
                let col_a = &columns[col_idx].data[a];
                let col_b = &columns[col_idx].data[b];

                let cmp = col_a
                    .partial_cmp(col_b)
                    .expect("Values in the same column are of the same type and ARE comparable");

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
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

// todo: add tests
