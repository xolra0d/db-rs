use serde::{Deserialize, Serialize};

use crate::engines::EngineName;
use crate::error::Result;
use crate::storage::{ColumnDef, get_unix_time};

pub const TABLE_METADATA_MAGIC_BYTES: &[u8] = b"THMETA".as_slice();
pub const TABLE_METADATA_FILENAME: &str = ".metadata";

const VERSION: u16 = 1;

pub mod flags {
    pub const NONE: u32 = 0x0000_0000;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
    pub order_by: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnDef>,
}

/// Table settings parsed from options received in CREATE command.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TableSettings {
    pub index_granularity: u32,
    pub engine: EngineName,
}

impl Default for TableSettings {
    fn default() -> Self {
        TableSettings {
            index_granularity: 8192,
            engine: EngineName::MergeTree,
        }
    }
}

/// Single immutable table metadata, stored as file (`TABLE_METADATA_FILENAME`)
/// Used to get global table configuration
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TableMetadata {
    pub version: u16,
    pub flags: u32,
    pub created_at: u64,
    pub settings: TableSettings,
    pub schema: TableSchema,
}

impl TableMetadata {
    /// Creates new table metadata with current timestamp and default flags.
    ///
    /// Returns: TableMetadata or error from get_unix_time()
    pub fn try_new(schema: TableSchema, settings: TableSettings) -> Result<Self> {
        Ok(Self {
            version: VERSION,
            flags: flags::NONE,
            created_at: get_unix_time()?,
            settings,
            schema,
        })
    }
}
