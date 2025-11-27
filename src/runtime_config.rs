use dashmap::DashMap;
use std::sync::atomic::AtomicU32;

use crate::storage::{TableDef, TableMetadata, TablePartInfo};

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub metadata: TableMetadata,
    pub infos: Vec<TablePartInfo>,
}

pub static TABLE_DATA: std::sync::LazyLock<DashMap<TableDef, TableConfig>> =
    std::sync::LazyLock::new(DashMap::default);

/// Signifies when it's ok to lock `TABLE_DATA` to merge `TablePart`
pub static DATABASE_LOAD: std::sync::LazyLock<AtomicU32> =
    std::sync::LazyLock::new(AtomicU32::default);

pub struct ComplexityGuard {
    complexity: u32,
}

impl ComplexityGuard {
    pub fn new(complexity: u32) -> Self {
        Self { complexity }
    }
}

impl Drop for ComplexityGuard {
    fn drop(&mut self) {
        DATABASE_LOAD.fetch_sub(self.complexity, std::sync::atomic::Ordering::Relaxed);
    }
}
