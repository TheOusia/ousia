// ousia/src/ledger/asset.rs
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub code: String,
    pub unit: i64, // Maximum value per ValueObject
}

impl Asset {
    pub fn new(code: &str, unit: i64) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            code: code.to_string(),
            unit,
        }
    }
}
