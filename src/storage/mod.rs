pub mod table_metadata;
mod table_part;
pub mod value;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::fmt;
use std::path::PathBuf;
use std::time::SystemTime;

use crate::CONFIG;
use crate::error::{Error, Result};
pub use crate::storage::table_metadata::{TableMetadata, TableSchema, TableSettings};
pub use crate::storage::table_part::{TablePart, load_all_parts_on_startup};
pub use crate::storage::value::{Value, ValueType};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum ColumnDefOption {
    Null,
    NotNull,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ColumnDefConstraint {
    pub name: Option<String>,
    pub option: ColumnDefOption,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub field_type: ValueType,
    pub constraints: Vec<ColumnDefConstraint>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Column {
    pub column_def: ColumnDef,
    pub data: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct OutputTable {
    columns: Vec<Column>,
}

impl OutputTable {
    /// Builds a simple OK response table.
    pub fn build_ok() -> Self {
        Self {
            columns: vec![Column {
                column_def: ColumnDef {
                    name: "OK".to_string(),
                    field_type: ValueType::String,
                    constraints: Vec::new(),
                },
                data: vec![Value::String("OK".to_string())],
            }],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableDef {
    pub table: String,
    pub database: String,
}

impl fmt::Display for TableDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl TableDef {
    /// Returns filesystem path for this table.
    pub fn get_path(&self) -> PathBuf {
        CONFIG.get_db_dir().join(&self.database).join(&self.table)
    }

    /// Checks if table exists by verifying database directory and .metadata file.
    ///
    /// Returns: Ok or DatabaseNotFound/TableNotFound error
    pub fn exists_or_err(&self) -> Result<()> {
        let mut path = CONFIG.get_db_dir().join(&self.database);
        if !path.exists() {
            return Err(Error::DatabaseNotFound);
        }

        path.push(&self.table);
        path.push(".metadata");

        if !path.exists() {
            return Err(Error::TableNotFound);
        }

        Ok(())
    }
}

impl TryFrom<&ObjectName> for TableDef {
    type Error = Error;
    fn try_from(object_name: &ObjectName) -> Result<Self> {
        let names = &object_name.0;
        if names.len() != 2 {
            return Err(Error::UnsupportedCommand(
                "You should provide table name in form `database_name.table_name`".to_string(),
            ));
        }

        let ObjectNamePart::Identifier(ref database) = names[0] else {
            return Err(Error::UnsupportedCommand(
                "Currently unimplemented.".to_string(),
            ));
        };
        let database = database.value.to_string();

        let ObjectNamePart::Identifier(ref table) = names[1] else {
            return Err(Error::UnsupportedCommand(
                "Currently unimplemented.".to_string(),
            ));
        };
        let table = table.value.clone();

        let table_def = Self { table, database };

        Ok(table_def)
    }
}

/// Returns current Unix timestamp in milliseconds.
///
/// Returns: u64 timestamp or SystemTimeWentBackword error
pub fn get_unix_time() -> Result<u64> {
    let now: SystemTime = SystemTime::now();
    u64::try_from(
        now.duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::SystemTimeWentBackword)?
            .as_millis(),
    )
    .map_err(|_| Error::SystemTimeWentBackword)
}
