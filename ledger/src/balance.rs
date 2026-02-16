// ledger/src/balance.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub owner: Uuid,
    pub asset: Uuid,
    pub available: i64,
    pub reserved: i64,
    pub total: i64,
    pub updated_at: DateTime<Utc>,
}

impl Balance {
    pub fn new(owner_id: Uuid, asset_id: Uuid) -> Self {
        Self {
            owner: owner_id,
            asset: asset_id,
            available: 0,
            reserved: 0,
            total: 0,
            updated_at: Utc::now(),
        }
    }

    pub fn from_value_objects(owner: Uuid, asset: Uuid, alive_sum: i64, reserved_sum: i64) -> Self {
        Self {
            owner,
            asset,
            available: alive_sum,
            reserved: reserved_sum,
            total: alive_sum + reserved_sum,
            updated_at: Utc::now(),
        }
    }
}
