mod merge_tree;

use crate::engines::merge_tree::MergeTreeEngine;
use crate::error::{Error, Result};
use crate::storage::Column;
use crate::storage::ColumnDef;
use serde::{Deserialize, Serialize};

/// Interface for every engine to follow.
pub trait Engine {
    /// Orders columns for insert by `order_by`.
    fn order_columns(&self, columns: Vec<Column>, order_by: &[ColumnDef]) -> Result<Vec<Column>>;
}

/// Used for storing engine name in metadata.
#[derive(Debug, Serialize, Deserialize, Eq, Hash, PartialEq, Clone)]
pub enum EngineName {
    MergeTree,
}

impl TryFrom<&str> for EngineName {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self> {
        match value {
            "MergeTree" => Ok(Self::MergeTree),
            _ => Err(Error::InvalidEngineName),
        }
    }
}

/// Engine configuration. Used to configure engine before running.
#[derive(Default)]
pub struct EngineConfig {}

/// Returns engine implementation for the given engine name.
pub fn get_engine(name: &EngineName, config: EngineConfig) -> Box<dyn Engine> {
    match name {
        EngineName::MergeTree => Box::new(MergeTreeEngine::new(config)),
    }
}
