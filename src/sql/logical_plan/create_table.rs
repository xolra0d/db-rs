use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::sql::sql_parser::LogicalPlan;
use crate::storage::{ColumnDef, TableDef, ValueType};
use sqlparser::ast::{CreateTable, CreateTableOptions, Expr, OneOrManyWithParens, SqlOption};
use std::collections::HashSet;

impl LogicalPlan {
    pub fn from_create_table(create_table: &CreateTable) -> Result<Self> {
        let table_def = TableDef::try_from(&create_table.name)?;

        let table_exists = table_def.exists_or_err();
        if create_table.if_not_exists && table_exists.is_ok() {
            return Ok(Self::Skip);
        }
        if table_exists.is_ok() {
            return Err(Error::TableAlreadyExists);
        }

        let mut columns: Vec<ColumnDef> = Vec::with_capacity(create_table.columns.len());
        let mut columns_names: HashSet<&String> =
            HashSet::with_capacity(create_table.columns.len());

        for table_column in &create_table.columns {
            let column_name = &table_column.name.value;

            if !Self::validate_name(column_name) {
                return Err(Error::InvalidColumnName(column_name.to_owned()));
            }
            if !columns_names.insert(column_name) {
                return Err(Error::InvalidColumnName(column_name.to_owned()));
            }

            let field_type = ValueType::try_from(&table_column.data_type)?;

            columns.push(ColumnDef {
                name: column_name.clone(),
                field_type,
            });
        }

        let engine = Self::parse_engine_from_table_options(&create_table.table_options)?;

        let order_by = Self::parse_order_by(
            create_table.order_by.as_ref(),
            &columns.iter().collect::<Vec<_>>(),
        )?;

        Ok(Self::CreateTable {
            name: table_def,
            columns,
            engine,
            order_by,
        })
    }

    pub fn parse_engine_from_table_options(
        table_options: &CreateTableOptions,
    ) -> Result<EngineName> {
        match table_options {
            CreateTableOptions::None => Ok(EngineName::MergeTree),
            CreateTableOptions::Plain(options) => {
                if options.len() != 1 {
                    return Err(Error::InvalidEngineName);
                }

                let SqlOption::NamedParenthesizedList(ref option) = options[0] else {
                    return Err(Error::InvalidEngineName);
                };

                let name = option.key.value.to_lowercase();
                if name != "engine" {
                    return Err(Error::InvalidEngineName);
                }
                let key = option.name.as_ref().ok_or(Error::InvalidEngineName)?;
                EngineName::try_from(key.value.as_str())
            }
            _ => Err(Error::InvalidEngineName),
        }
    }

    pub fn parse_order_by(
        options: Option<&OneOrManyWithParens<Expr>>,
        columns: &[&ColumnDef],
    ) -> Result<Vec<ColumnDef>> {
        let order_by_params = *options.as_ref().ok_or(Error::InvalidOrderBy)?;
        if order_by_params.is_empty() {
            return Err(Error::InvalidOrderBy);
        }

        let mut order_by = Vec::with_capacity(order_by_params.len());
        let mut order_by_names = HashSet::with_capacity(order_by_params.len());

        for param in order_by_params {
            let Expr::Identifier(param_ident) = param else {
                return Err(Error::InvalidOrderBy);
            };
            let column_name = &param_ident.value;

            let column_def = columns
                .iter()
                .find(|col| col.name == *column_name)
                .ok_or(Error::InvalidOrderBy)?;

            if !order_by_names.insert(column_name) {
                return Err(Error::InvalidOrderBy);
            }

            order_by.push((*column_def).clone());
        }
        Ok(order_by)
    }
}
