// ousia/src/ledger/transaction.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: Ulid,
    pub asset: Ulid,
    pub sender: Option<Ulid>,
    pub receiver: Option<Ulid>,
    pub burned_amount: i64,
    pub minted_amount: i64,
    pub metadata: String,
    pub created_at: DateTime<Utc>,
}

impl Transaction {
    pub fn new(
        asset_id: Ulid,
        sender: Option<Ulid>,
        receiver: Option<Ulid>,
        burned_amount: i64,
        minted_amount: i64,
        metadata: String,
    ) -> Self {
        Self {
            id: Ulid::new(),
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

/// Handle returned from Money operations for potential reversion
#[derive(Debug, Clone)]
pub struct TransactionHandle {
    pub transaction_id: Ulid,
    pub asset_id: Ulid,
    pub sender: Option<Ulid>,
    pub receiver: Option<Ulid>,
    pub amount: i64,
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

    /// Revert the transaction by creating a compensating transaction
    /// Burns from receiver, mints back to sender
    pub async fn revert(self, _reason: String) -> Result<Ulid, crate::MoneyError> {
        // This will be implemented in the Money module
        // which has access to the LedgerSystem
        todo!("Revert implementation requires Money context")
    }
}
