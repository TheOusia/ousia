// ledger/src/asset.rs
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub code: String,
    pub unit: u64,
    pub decimals: u8,
}

impl Asset {
    pub fn new(code: &str, unit: u64, decimals: u8) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            code: code.to_string(),
            unit,
            decimals,
        }
    }

    pub fn to_internal(&self, display_amount: f64) -> u64 {
        (display_amount * 10_f64.powi(self.decimals as i32)) as u64
    }

    pub fn to_display(&self, internal_amount: u64) -> f64 {
        internal_amount as f64 / 10_f64.powi(self.decimals as i32)
    }
}
