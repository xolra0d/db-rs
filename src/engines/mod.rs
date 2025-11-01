mod merge_tree;

use crate::engines::merge_tree::MergeTreeEngine;
use crate::error::Error;
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub struct EngineConfig {}

#[allow(dead_code)]
pub trait Engine {
    fn name(&self) -> &'static str;
}

#[allow(dead_code)]
pub fn get_engine(name: &str, config: EngineConfig) -> Option<Box<dyn Engine>> {
    match name {
        "MergeTree" => Some(Box::new(MergeTreeEngine::new(config))),
        _ => None,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EngineName {
    MergeTree,
}

impl TryFrom<&str> for EngineName {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "MergeTree" => Ok(Self::MergeTree),
            _ => Err(Error::InvalidEngineName),
        }
    }
}
