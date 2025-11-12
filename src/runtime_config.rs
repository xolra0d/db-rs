use scc::HashIndex;

use crate::storage::{TableDef, TableMetadata, TablePartInfo};

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub metadata: TableMetadata,
    pub infos: Vec<TablePartInfo>,
}

// Using HashIndex, as it's optimized for read access
// by providing lock-free reads
pub static TABLE_DATA: std::sync::LazyLock<HashIndex<TableDef, TableConfig>> =
    std::sync::LazyLock::new(HashIndex::default);
