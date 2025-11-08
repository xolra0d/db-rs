mod merge_tree;

use crate::engines::merge_tree::MergeTreeEngine;
use crate::error::{Error, Result};
use crate::storage::Column;
use crate::storage::ColumnDef;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub trait Engine {
    fn name(&self) -> &'static str;
    fn order_columns(&self, columns: Vec<Column>, order_by: &[ColumnDef]) -> Result<Vec<Column>>;
}

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

#[derive(Default)]
pub struct EngineConfig {}

/// Returns engine implementation for the given engine name.
///
/// Uses provided config or defaults if None.
#[allow(dead_code)]
pub fn get_engine(name: &EngineName, config: Option<EngineConfig>) -> Box<dyn Engine> {
    match name {
        EngineName::MergeTree => Box::new(MergeTreeEngine::new(config.unwrap_or_default())),
    }
}
