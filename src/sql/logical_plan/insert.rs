use scc::Guard;
use sqlparser::ast::{Expr, Insert, SetExpr, TableObject};

use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::sql_parser::LogicalPlan;
use crate::storage::{Column, ColumnDefOption, TableDef, Value};

impl LogicalPlan {
    /// Parses INSERT statement into LogicalPlan::Insert variant.
    ///
    /// Validates that:
    /// - Table exists and columns are valid
    /// - All NOT NULL and ORDER BY columns are provided
    /// - Values match column types
    ///
    /// Returns:
    ///   * Ok: LogicalPlan::Insert with validated columns and data
    ///   * Error: TableNotFound, InvalidColumnName, InvalidColumnsSpecified, InvalidSource, or EmptySource
    pub fn from_insert(insert: &Insert) -> Result<Self> {
        let TableObject::TableName(ref table) = insert.table else {
            return Err(Error::UnsupportedCommand(
                "Currently not supporting table functions".to_string(),
            ));
        };
        let table_def = TableDef::try_from(table)?;

        let guard = Guard::new();
        let Some(table_config) = TABLE_DATA.peek(&table_def, &guard) else {
            return Err(Error::TableNotFound);
        };

        if insert.columns.is_empty() {
            return Err(Error::NoColumnsSpecified);
        }
        let mut insert_columns = Vec::with_capacity(insert.columns.len());
        for input_column in &insert.columns {
            let column_def = table_config
                .metadata
                .schema
                .columns
                .iter()
                .find(|x| x.name == input_column.value)
                .ok_or(Error::InvalidColumnName(input_column.value.clone()))?;
            insert_columns.push(column_def.clone())
        }

        let missing_not_null = table_config
            .metadata
            .schema
            .columns
            .iter()
            .filter(|col| !insert_columns.contains(col))
            .any(|col| {
                col.constraints
                    .iter()
                    .any(|c| c.option == ColumnDefOption::NotNull)
            });

        if missing_not_null {
            return Err(Error::InvalidColumnsSpecified);
        }

        for order_col in &table_config.metadata.schema.order_by {
            if !insert_columns.iter().any(|c| c == order_col) {
                return Err(Error::InvalidColumnsSpecified);
            }
        }

        let mut columns: Vec<Column> = insert_columns
            .into_iter()
            .map(|x| Column {
                column_def: x,
                data: Vec::new(),
            })
            .collect();

        let Some(ref source) = insert.source else {
            return Err(Error::InvalidSource);
        };
        let SetExpr::Values(ref source) = *source.body else {
            return Err(Error::InvalidSource);
        };

        if source.rows.is_empty() {
            return Err(Error::EmptySource);
        }

        let len_first = source.rows[0].len();

        if source.rows.iter().any(|x| x.len() != len_first) {
            return Err(Error::InvalidSource);
        }

        if len_first != columns.len() {
            return Err(Error::InvalidSource);
        }

        for row in &source.rows {
            for (col_idx, expr) in row.iter().enumerate() {
                let Expr::Value(sql_value) = expr else {
                    return Err(Error::InvalidSource);
                };
                let column_type = &columns[col_idx].column_def.field_type;
                let value = Value::try_from((sql_value.value.clone(), column_type))?;
                columns[col_idx].data.push(value);
            }
        }

        Ok(LogicalPlan::Insert { table_def, columns })
    }
}
