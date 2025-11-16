use dashmap::DashMap;

use crate::storage::{TableDef, TableMetadata, TablePartInfo};

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub metadata: TableMetadata,
    pub infos: Vec<TablePartInfo>,
}

pub static TABLE_DATA: std::sync::LazyLock<DashMap<TableDef, TableConfig>> =
    std::sync::LazyLock::new(DashMap::default);
