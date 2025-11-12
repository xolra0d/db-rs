use scc::Guard;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, TableFactor};

use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::sql_parser::LogicalPlan;
use crate::storage::TableDef;

impl LogicalPlan {
    pub fn from_query(query: &Query) -> Result<Self> {
        match query.body.as_ref() {
            SetExpr::Select(select) => Self::parse_select(select),
            _ => unimplemented!(),
        }
    }

    pub fn parse_select(select: &Select) -> Result<Self> {
        if select.from.len() != 1 {
            return Err(Error::UnsupportedCommand(
                "Currently do no support multiple table selects".to_string(),
            ));
        }
        let table = &select.from[0];
        let TableFactor::Table { name, .. } = &table.relation else {
            unimplemented!()
        };
        let table_def = TableDef::try_from(name)?;

        if select.projection.is_empty() {
            return Err(Error::UnsupportedCommand(
                "No projection specified.".to_string(),
            ));
        }

        let guard = Guard::new();
        let Some(table_config) = TABLE_DATA.peek(&table_def, &guard) else {
            return Err(Error::TableNotFound);
        };

        let mut plan = Self::Scan { table: table_def };

        let mut columns = Vec::with_capacity(select.projection.len());

        for projection in &select.projection {
            let SelectItem::UnnamedExpr(expr) = projection else {
                return Err(Error::UnsupportedCommand(
                    "Unsupported projection expression.".to_string(),
                ));
            };
            let Expr::Identifier(ident) = expr else {
                return Err(Error::UnsupportedCommand(
                    "Unsupported projection expression.".to_string(),
                ));
            };

            let column = table_config
                .metadata
                .schema
                .columns
                .iter()
                .find(|col| col.name == ident.value)
                .ok_or(Error::UnsupportedCommand(
                    "Unsupported projection expression.".to_string(),
                ))?;
            if columns.contains(column) {
                return Err(Error::UnsupportedCommand(
                    "Unsupported projection expression.".to_string(),
                ));
            }
            columns.push(column.clone());
        }

        if let Some(ref selection) = select.selection {
            plan = LogicalPlan::Filter {
                expr: Box::new(selection.clone()),
                plan: Box::new(plan),
            };
        }

        plan = LogicalPlan::Projection {
            columns,
            plan: Box::new(plan),
        };

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_parse_select_multiple_tables_via_sql() {
        let dialect = GenericDialect {};
        let sql = "SELECT id FROM db.table1, db.table2";
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        if let sqlparser::ast::Statement::Query(query) = &statements[0] {
            let result = LogicalPlan::from_query(query);
            assert!(result.is_err());
            if let Err(Error::UnsupportedCommand(msg)) = result {
                assert!(msg.contains("multiple table"));
            }
        } else {
            panic!("Expected query statement");
        }
    }
}
