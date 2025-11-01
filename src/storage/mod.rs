pub mod table_metadata;
pub mod value;

use crate::config::CONFIG;
use crate::error::{Error, Result};
pub use crate::storage::value::{Value, ValueType};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef {
    pub name: String,
    pub field_type: ValueType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub column_def: ColumnDef,
    pub data: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct OutputTable {
    columns: Vec<Column>,
}

impl OutputTable {
    pub fn try_new(columns: Vec<Column>) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::EmptyTable);
        }

        let column_data_len = columns[0].data.len();

        for column in columns.iter().skip(1) {
            if column.data.len() != column_data_len {
                return Err(Error::ColumnLengthDiff);
            }
        }

        Ok(Self { columns })
    }

    pub fn build_ok() -> Self {
        Self {
            columns: vec![Column {
                column_def: ColumnDef {
                    name: "OK".to_string(),
                    field_type: ValueType::String,
                },
                data: vec![Value::String("OK".to_string())],
            }],
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub table: String,
    pub database: String,
}

impl TableDef {
    pub fn get_path(&self) -> PathBuf {
        CONFIG.get_db_dir().join(&self.database).join(&self.table)
    }

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
