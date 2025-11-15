use sqlparser::ast::{Expr, Statement};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};
use crate::storage::table_metadata::TableSettings;
use crate::storage::{Column, ColumnDef, TableDef};

/// High level representation of the SQL query.
#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    /// No tasks need to be done. Skip.
    Skip,

    /// Create a database.
    CreateDatabase {
        name: String,
    },

    /// Create a table.
    CreateTable {
        name: TableDef,
        columns: Vec<ColumnDef>,
        settings: TableSettings,
        order_by: Vec<ColumnDef>,
        primary_key: Vec<ColumnDef>,
    },

    /// Insert values.
    Insert {
        table_def: TableDef,
        columns: Vec<Column>,
    },

    Scan {
        table: TableDef,
    },

    Projection {
        columns: Vec<ColumnDef>,
        plan: Box<LogicalPlan>,
    },

    Filter {
        expr: Box<Expr>,
        plan: Box<LogicalPlan>,
    },
}

/// Tries to convert SQL to `LogicalPlan` by using Datafusion `SQLParser`
/// Currently supported commands
///   1. `CREATE DATABASE`
///   2. `CREATE TABLE`
///   3. `INSERT INTO`
impl TryFrom<&str> for LogicalPlan {
    type Error = Error;

    fn try_from(sql: &str) -> Result<Self> {
        let dialect = ClickHouseDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|error| Error::SqlToAstConversion(error.to_string()))?;
        if ast.len() != 1 {
            return Err(Error::SqlToAstConversion(
                "Currently support only statement per request".to_string(),
            ));
        }

        match &ast[0] {
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => Self::from_create_database(db_name, *if_not_exists),
            Statement::CreateTable(create_table) => Self::from_create_table(create_table),

            Statement::Insert(insert) => Self::from_insert(insert),
            Statement::Query(query) => Self::from_query(query),

            statement => Err(Error::UnsupportedCommand(statement.to_string())),
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
        settings: TableSettings,
        order_by: Vec<ColumnDef>,
        primary_key: Vec<ColumnDef>,
    },

    /// Insert values.
    Insert {
        table_def: TableDef,
        columns: Vec<Column>,
    },

    /// Select columns from table.
    Select {
        table_def: TableDef,
        columns: Vec<ColumnDef>,
        filter: Option<Box<Expr>>,
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
                settings,
                order_by,
                primary_key,
            } => Self::CreateTable {
                name,
                columns,
                settings,
                order_by,
                primary_key,
            },
            LogicalPlan::Insert { table_def, columns } => Self::Insert { table_def, columns },
            LogicalPlan::Projection { columns, plan } => match *plan {
                LogicalPlan::Filter { expr, plan } => {
                    if let LogicalPlan::Scan { table } = *plan {
                        Self::Select {
                            table_def: table,
                            columns,
                            filter: Some(expr),
                        }
                    } else {
                        unimplemented!("Filter without Scan not yet supported")
                    }
                }
                LogicalPlan::Scan { table } => Self::Select {
                    table_def: table,
                    columns,
                    filter: None,
                },
                _ => unimplemented!("Projection without Scan not yet supported"),
            },
            _ => unimplemented!("Projection without Scan not yet supported"),
        }
    }
}
