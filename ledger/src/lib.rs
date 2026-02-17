// ledger/src/lib.rs
pub mod adapters;
pub mod asset;
pub mod balance;
pub mod error;
pub mod money;
pub mod transaction;
pub mod value_object;

pub use asset::Asset;
pub use balance::Balance;
use chrono::{DateTime, Utc};
pub use error::MoneyError;
pub use money::{ExecutionPlan, LedgerContext, Money, MoneySlice, Operation, TransactionContext};
pub use transaction::Transaction;
pub use value_object::{ValueObject, ValueObjectState};

use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) fn hash_idempotency_key(key: &str) -> String {
    blake3::hash(key.as_bytes()).to_hex().to_string()
}

/// Internal ledger adapter trait
#[async_trait]
pub trait LedgerAdapter: Send + Sync {
    /// Execute the complete operation plan atomically.
    /// Implementors MUST:
    /// 1. BEGIN a database transaction
    /// 2. SELECT FOR UPDATE the required value objects (from `locks`)
    /// 3. Verify sum >= required amount — return InsufficientFunds if not
    /// 4. Execute all operations
    /// 5. COMMIT on success, ROLLBACK on any error
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, u64)],
    ) -> Result<(), MoneyError>;

    // READ OPERATIONS
    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError>;
    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError>;
    async fn get_transactions_for_owner(
        &self,
        owner: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError>;
    async fn check_idempotency_key(&self, key: &str) -> Result<(), MoneyError>;
    async fn get_transaction_by_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Transaction, MoneyError>;
    async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError>;
    async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError>;
}

/// Initialize the ledger system with an adapter
pub struct LedgerSystem {
    adapter: Arc<dyn LedgerAdapter>,
}

impl LedgerSystem {
    pub fn new(adapter: Box<dyn LedgerAdapter>) -> Self {
        Self {
            adapter: adapter.into(),
        }
    }

    /// Get adapter reference
    pub fn adapter(&self) -> &dyn LedgerAdapter {
        self.adapter.as_ref()
    }

    /// Get adapter Arc (for creating contexts)
    pub fn adapter_arc(&self) -> Arc<dyn LedgerAdapter> {
        Arc::clone(&self.adapter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_asset_conversion() {
        let usd = Asset::new("USD", 10_000, 2);
        assert_eq!(usd.to_internal(100.50), 10050);
        assert_eq!(usd.to_display(10050), 100.50);

        let eth = Asset::new("ETH", 1_000_000_000_000_000_000u64, 18);
        let one_eth = 1_000_000_000_000_000_000u64;
        assert_eq!(eth.to_display(one_eth), 1.0);
    }

    #[test]
    fn test_value_object_states() {
        assert!(matches!(ValueObjectState::Alive, ValueObjectState::Alive));
        assert!(matches!(
            ValueObjectState::Reserved,
            ValueObjectState::Reserved
        ));
        assert!(matches!(ValueObjectState::Burned, ValueObjectState::Burned));
    }
}
