use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::storage::{ColumnDef, TableDef};
use sqlparser::ast::Statement;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub enum LogicalPlan {
    Skip,
    CreateDatabase {
        name: String,
    },
    CreateTable {
        name: TableDef,
        columns: Vec<ColumnDef>,
        engine: EngineName,
        order_by: Vec<ColumnDef>,
    },
}

impl TryFrom<&str> for LogicalPlan {
    type Error = Error;

    fn try_from(sql: &str) -> Result<Self> {
        let dialect = ClickHouseDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|_| Error::SqlToAstConversion)?;

        let statement = ast.first().ok_or(Error::EmptyStatement)?;

        match statement {
            Statement::CreateTable(create_table) => Self::from_create_table(create_table),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Self::from_create_database(db_name, *if_not_exists),
            _ => Err(Error::UnsupportedCommand(statement.to_string())),
        }
    }
}

impl LogicalPlan {
    pub fn validate_name(name: &str) -> bool {
        name.chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    }

    pub fn optimize_self(self) -> Self {
        self
    }
}

#[derive(Debug)]
pub enum PhysicalPlan {
    Skip,
    CreateDatabase {
        name: String,
    },
    CreateTable {
        name: TableDef,
        columns: Vec<ColumnDef>,
        engine: EngineName,
        order_by: Vec<ColumnDef>,
    },
}

impl From<LogicalPlan> for PhysicalPlan {
    fn from(plan: LogicalPlan) -> Self {
        match plan {
            LogicalPlan::Skip => Self::Skip,
            LogicalPlan::CreateDatabase { name } => Self::CreateDatabase { name },
            LogicalPlan::CreateTable {
                name,
                columns,
                engine,
                order_by,
            } => Self::CreateTable {
                name,
                columns,
                engine,
                order_by,
            },
        }
    }
}
