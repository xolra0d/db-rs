mod compression;
pub mod table_metadata;
mod table_part;
pub mod value;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::CONFIG;
use crate::error::{Error, Result};
pub use crate::storage::compression::CompressionType;
use crate::storage::table_metadata::TABLE_METADATA_FILENAME;
pub use crate::storage::table_metadata::{TableMetadata, TableSchema, TableSettings};
pub use crate::storage::table_part::{Mark, TablePart, TablePartInfo, load_all_parts_on_startup};
pub use crate::storage::value::{Value, ValueType};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Constraints {
    pub nullable: bool,
    pub default: Option<Value>,
    pub compression_type: CompressionType,
}

impl Default for Constraints {
    fn default() -> Self {
        Self {
            nullable: true,
            default: None,
            compression_type: CompressionType::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub field_type: ValueType,
    pub constraints: Constraints,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Column {
    pub column_def: ColumnDef,
    pub data: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct OutputTable {
    pub columns: Vec<Column>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_time: Option<Duration>,
}

impl OutputTable {
    /// Creates new `OutputTable` with provided columns.
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            execution_time: None,
        }
    }

    /// Sets the execution time for this output table.
    pub fn with_execution_time(mut self, duration: Duration) -> Self {
        self.execution_time = Some(duration);
        self
    }

    /// Builds a simple OK response table.
    pub fn build_ok() -> Self {
        Self {
            columns: vec![Column {
                column_def: ColumnDef {
                    name: "OK".to_string(),
                    field_type: ValueType::String,
                    constraints: Constraints::default(),
                },
                data: vec![Value::String("OK".to_string())],
            }],
            execution_time: None,
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
        write!(f, "({}.{})", self.database, self.table)
    }
}

impl TableDef {
    /// Returns filesystem path for this table.
    pub fn get_path(&self) -> PathBuf {
        CONFIG.get_db_dir().join(&self.database).join(&self.table)
    }

    /// Checks if table exists by verifying database directory and `TABLE_METADATA_FILENAME` file.
    ///
    /// Returns: Ok or DatabaseNotFound/TableNotFound error
    pub fn exists_or_err(&self) -> Result<()> {
        let mut path = CONFIG.get_db_dir().join(&self.database);
        if !path.exists() {
            return Err(Error::DatabaseNotFound);
        }

        path.push(&self.table);
        path.push(TABLE_METADATA_FILENAME);

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
        let database = database.value.clone();

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
/// Returns: u64 timestamp or `SystemTimeWentBackword` error
pub fn get_unix_time() -> Result<u64> {
    let now: SystemTime = SystemTime::now();
    u64::try_from(
        now.duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| Error::SystemTimeWentBackword)?
            .as_millis(),
    )
    .map_err(|_| Error::SystemTimeWentBackword)
}

/// Reads a specific file with crc32.
///
/// Returns: T or `CouldNotReadData` on failure
pub fn read_file_with_crc<T>(path: &Path, magic_bytes: &[u8]) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let file_bytes = std::fs::read(path)
        .map_err(|e| Error::CouldNotReadData(format!("Failed to read column file: {e}")))?;

    if file_bytes.len() <= magic_bytes.len() + 4 {
        return Err(Error::CouldNotReadData("Column file too small".to_string()));
    }

    let file_magic_bytes = &file_bytes[0..magic_bytes.len()];
    if file_magic_bytes != magic_bytes {
        return Err(Error::CouldNotReadData(
            "Invalid magic bytes in column file".to_string(),
        ));
    }

    let data_bytes = &file_bytes[magic_bytes.len()..(file_bytes.len() - 4)];

    let expected_crc = u32::from_le_bytes([
        file_bytes[file_bytes.len() - 4],
        file_bytes[file_bytes.len() - 3],
        file_bytes[file_bytes.len() - 2],
        file_bytes[file_bytes.len() - 1],
    ]);

    let actual_crc = crc32fast::hash(data_bytes);
    if expected_crc != actual_crc {
        return Err(Error::CouldNotReadData(
            "CRC mismatch in column file".to_string(),
        ));
    }

    let file = bincode::serde::decode_from_slice(data_bytes, bincode::config::standard())
        .map(|x| x.0)
        .map_err(|e| Error::CouldNotReadData(format!("Failed to deserialize column: {e}")))?;

    Ok(file)
}

/// Writes to a specific file with crc32.
///
/// Returns: () or `CouldNotInsertData` on failure
pub fn write_file_with_crc<T>(data: &T, path: &PathBuf, magic_bytes: &[u8]) -> Result<()>
where
    T: Serialize,
{
    let mut bytes = Vec::from(magic_bytes);

    let data_bytes = bincode::serde::encode_to_vec(data, bincode::config::standard())
        .map_err(|e| Error::CouldNotInsertData(format!("Failed to serialize column: {e}")))?;

    let crc = crc32fast::hash(&data_bytes);

    bytes.extend(data_bytes);
    bytes.extend(crc.to_le_bytes());

    std::fs::write(path, bytes)
        .map_err(|e| Error::CouldNotInsertData(format!("Failed to write file: {e}")))
}
