use crate::engines::EngineName;
use crate::error::{Error, Result};
use crate::storage::{ColumnDef, TableDef};

use crc32fast;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

const MAGIC_BYTES: &[u8] = b"THMETA".as_slice();
const VERSION: u16 = 1;

pub mod flags {
    pub const NONE: u32 = 0x0000_0000;
    pub const _COMPRESSED: u32 = 0x0000_0001;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
    pub order_by: Vec<ColumnDef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub version: u16,
    pub flags: u32,
    pub row_count: u64,
    pub created_at: u64,
    pub engine: EngineName,
    pub schema: TableSchema,
}

impl TableMetadata {
    pub fn try_new(schema: TableSchema, engine: EngineName) -> Result<Self> {
        let now = SystemTime::now();
        let current_timestamp = u64::try_from(
            now.duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| Error::SystemTimeWentBackword)?
                .as_millis(),
        )
        .map_err(|_| Error::SystemTimeWentBackword)?;

        Ok(Self {
            version: VERSION,
            flags: flags::NONE,
            row_count: 0,
            created_at: current_timestamp,
            engine,
            schema,
        })
    }

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
