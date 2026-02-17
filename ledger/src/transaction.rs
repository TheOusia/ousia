// ledger/src/transaction.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Uuid,
    pub asset: Uuid,
    pub code: String,
    pub sender: Option<Uuid>,
    pub receiver: Option<Uuid>,
    pub burned_amount: u64,
    pub minted_amount: u64,
    pub metadata: String,
    pub created_at: DateTime<Utc>,
}

impl Transaction {
    pub fn new(
        asset_id: Uuid,
        asset_name: String,
        sender: Option<Uuid>,
        receiver: Option<Uuid>,
        burned_amount: u64,
        minted_amount: u64,
        metadata: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            asset: asset_id,
            code: asset_name,
            sender,
            receiver,
            burned_amount,
            minted_amount,
            metadata,
            created_at: Utc::now(),
        }
    }
}
