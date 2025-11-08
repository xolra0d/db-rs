use std::io::ErrorKind;
use crc32fast;
use serde::{Deserialize, Serialize};

use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::storage::{ColumnDef, TableDef, get_unix_time};

const MAGIC_BYTES: &[u8] = b"THMETA".as_slice();
const VERSION: u16 = 1;

pub mod flags {
    pub const NONE: u32 = 0x0000_0000;
    pub const _COMPRESSED: u32 = 0x0000_0001;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
    pub order_by: Vec<ColumnDef>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TableMetadata {
    pub version: u16,
    pub flags: u32,
    pub row_count: u64,
    pub created_at: u64,
    pub engine: EngineName,
    pub schema: TableSchema,
}

impl TableMetadata {
    /// Creates new table metadata with current timestamp and default flags.
    ///
    /// Returns: TableMetadata or error from get_unix_time()
    pub fn try_new(schema: TableSchema, engine: EngineName) -> Result<Self> {
        Ok(Self {
            version: VERSION,
            flags: flags::NONE,
            row_count: 0,
            created_at: get_unix_time()?,
            engine,
            schema,
        })
    }

    /// Reads and validates table metadata from .metadata file.
    ///
    /// Validates magic bytes and CRC32 checksum.
    ///
    /// Returns: TableMetadata or InvalidTable/TableNotFound error
    pub fn read_from(table_def: &TableDef) -> Result<Self> {
        let metadata_path = table_def.get_path().join(".metadata");
        let file_bytes = std::fs::read(metadata_path).map_err(|error| match error.kind() {
            ErrorKind::NotFound => Error::TableNotFound,
            _ => Error::InvalidTable,
        })?;

        if file_bytes.len() <= MAGIC_BYTES.len() {
            return Err(Error::InvalidTable);
        }

        let file_magic_bytes = &file_bytes[0..MAGIC_BYTES.len()];
        if file_magic_bytes != MAGIC_BYTES {
            return Err(Error::InvalidTable);
        }
        let metadata_bytes = &file_bytes[MAGIC_BYTES.len()..(file_bytes.len() - 4)];

        let metadata =
            bincode::serde::decode_from_slice(metadata_bytes, bincode::config::standard())
                .map(|x| x.0)
                .map_err(|_| Error::InvalidTable)?;

        let crc = u32::from_le_bytes([
            file_bytes[file_bytes.len() - 4],
            file_bytes[file_bytes.len() - 3],
            file_bytes[file_bytes.len() - 2],
            file_bytes[file_bytes.len() - 1],
        ]);

        if crc != crc32fast::hash(metadata_bytes) {
            return Err(Error::InvalidTable);
        }

        Ok(metadata)
    }

    /// Writes metadata to .metadata file with magic bytes and CRC32 checksum.
    ///
    /// Returns: Ok or InvalidTable on serialization/write failure
    pub fn write_to(&self, table_def: &TableDef) -> Result<()> {
        let mut bytes = Vec::from(MAGIC_BYTES);

        let metadata_bytes = bincode::serde::encode_to_vec(self, bincode::config::standard())
            .map_err(|_| Error::InvalidTable)?;
        let crc = crc32fast::hash(&metadata_bytes);

        bytes.extend(metadata_bytes);
        bytes.extend(crc.to_le_bytes());

        std::fs::write(table_def.get_path().join(".metadata"), bytes)
            .map_err(|_| Error::InvalidTable)
    }
}
