use crate::engines::{Engine, EngineConfig};

#[allow(dead_code)]
pub struct MergeTreeEngine {
    config: EngineConfig,
}

impl MergeTreeEngine {
    pub const fn new(config: EngineConfig) -> Self {
        Self { config }
    }
}

impl Engine for MergeTreeEngine {
    fn name(&self) -> &'static str {
        "MergeTree"
    }
}
