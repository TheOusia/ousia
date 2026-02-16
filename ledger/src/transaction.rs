// ledger/src/transaction.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Uuid,
    pub asset: Uuid,
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
        sender: Option<Uuid>,
        receiver: Option<Uuid>,
        burned_amount: u64,
        minted_amount: u64,
        metadata: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            asset: asset_id,
            sender,
            receiver,
            burned_amount,
            minted_amount,
            metadata,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionHandle {
    pub transaction_id: Uuid,
    pub asset_id: Uuid,
    pub sender: Option<Uuid>,
    pub receiver: Option<Uuid>,
    pub amount: u64,
}

impl TransactionHandle {
    pub(crate) fn new(transaction: &Transaction) -> Self {
        Self {
            transaction_id: transaction.id,
            asset_id: transaction.asset,
            sender: transaction.sender,
            receiver: transaction.receiver,
            amount: transaction.minted_amount,
        }
    }
}
