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
    pub complexity: u32,
}

impl Drop for ComplexityGuard {
    /// Decrements the global `DATABASE_LOAD` counter by this guard's `complexity` when the guard is dropped.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::sync::atomic::Ordering;
    ///
    /// // initialize global counter
    /// DATABASE_LOAD.store(10, Ordering::Relaxed);
    ///
    /// {
    ///     let _guard = ComplexityGuard { complexity: 4 };
    ///     // `_guard` remains in scope; counter still 10
    ///     assert_eq!(DATABASE_LOAD.load(Ordering::Relaxed), 10);
    /// } // `_guard` is dropped here, subtracting 4 from DATABASE_LOAD
    ///
    /// assert_eq!(DATABASE_LOAD.load(Ordering::Relaxed), 6);
    /// ```
    fn drop(&mut self) {
        DATABASE_LOAD.fetch_sub(self.complexity, std::sync::atomic::Ordering::Relaxed);
    }
}