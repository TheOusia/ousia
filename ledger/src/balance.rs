// ledger/src/balance.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub owner: Uuid,
    pub asset: Uuid,
    pub available: u64,
    pub reserved: u64,
    pub total: u64,
    pub timestamp: DateTime<Utc>,
}

impl Balance {
    pub fn new(owner_id: Uuid, asset_id: Uuid) -> Self {
        Self {
            owner: owner_id,
            asset: asset_id,
            available: 0,
            reserved: 0,
            total: 0,
            timestamp: Utc::now(),
        }
    }

    pub fn from_value_objects(owner: Uuid, asset: Uuid, alive_sum: u64, reserved_sum: u64) -> Self {
        Self {
            owner,
            asset,
            available: alive_sum,
            reserved: reserved_sum,
            total: alive_sum + reserved_sum,
            timestamp: Utc::now(),
        }
    }
}
