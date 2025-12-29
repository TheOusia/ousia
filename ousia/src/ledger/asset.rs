// ousia/src/ledger/asset.rs
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Ulid,
    pub code: String,
    pub unit: i64, // Maximum value per ValueObject
}

impl Asset {
    pub fn new(code: String, unit: i64) -> Self {
        Self {
            id: Ulid::new(),
            code,
            unit,
        }
    }
}
