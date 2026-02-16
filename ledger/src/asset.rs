// ledger/src/asset.rs
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub code: String,
    pub unit: i64,
    pub decimals: u8,
}

impl Asset {
    pub fn new(code: &str, unit: i64, decimals: u8) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            code: code.to_string(),
            unit,
            decimals,
        }
    }
    
    pub fn fiat(code: &str) -> Self {
        Self::new(code, 10_000, 2)
    }
    
    pub fn crypto(code: &str, decimals: u8) -> Self {
        Self::new(code, 1_000_000_000_000_000_000, decimals)
    }
    
    pub fn to_internal(&self, display_amount: f64) -> i64 {
        (display_amount * 10_f64.powi(self.decimals as i32)) as i64
    }
    
    pub fn to_display(&self, internal_amount: i64) -> f64 {
        internal_amount as f64 / 10_f64.powi(self.decimals as i32)
    }
}
