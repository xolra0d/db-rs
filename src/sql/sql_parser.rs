use sqlparser::ast::Statement;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::storage::{Column, ColumnDef, TableDef};

/// High level representation of the SQL query.
#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    /// No tasks need to be done. Skip.
    Skip,

    /// Create a database.
    CreateDatabase { name: String },

    /// Create a table.
    CreateTable {
        name: TableDef,
        columns: Vec<ColumnDef>,
        engine: EngineName,
        order_by: Vec<ColumnDef>,
    },

    /// Insert values.
    Insert {
        table_def: TableDef,
        columns: Vec<Column>,
    },
}

/// Tries to convert SQL to LogicalPlan by using Datafusion SQLParser
/// Currently supported commands
///   1. `CREATE DATABASE`
///   2. `CREATE TABLE`
///   3. `INSERT INTO`
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
            Statement::Insert(insert) => Self::from_insert(insert),
            _ => Err(Error::UnsupportedCommand(statement.to_string())),
        }
    }
}

impl LogicalPlan {
    /// Plan optimization
    pub fn optimize_self(self) -> Self {
        self
    }
}

/// Lower level representation of the Logical Plan.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// No tasks need to be done. Skip.
    Skip,

    /// Create a database.
    CreateDatabase { name: String },

    /// Create a table.
    CreateTable {
        name: TableDef,
        columns: Vec<ColumnDef>,
        engine: EngineName,
        order_by: Vec<ColumnDef>,
    },

    /// Insert values.
    Insert {
        table_def: TableDef,
        columns: Vec<Column>,
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
            LogicalPlan::Insert { table_def, columns } => Self::Insert { table_def, columns },
        }
    }
}
