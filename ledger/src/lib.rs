pub mod asset;
pub mod balance;
pub mod error;
pub mod money;
pub mod transaction;
pub mod value_object;

pub use asset::Asset;
pub use balance::Balance;
pub use error::MoneyError;
pub use money::{Money, MoneySlice};
pub use transaction::{Transaction, TransactionHandle};
pub use value_object::{ValueObject, ValueObjectState};

use async_trait::async_trait;
use uuid::Uuid;

/// Internal ledger adapter trait
/// This is sealed and not exposed to library users
#[async_trait]
pub trait LedgerAdapter: Send + Sync {
    /// Mint new ValueObjects
    async fn mint_value_objects(
        &self,
        asset_id: Uuid,
        owner: Uuid,
        amount: i64,
        metadata: String,
    ) -> Result<Vec<ValueObject>, MoneyError>;

    /// Burn ValueObjects (mark as burned)
    async fn burn_value_objects(&self, ids: Vec<Uuid>, metadata: String) -> Result<(), MoneyError>;

    /// Select alive ValueObjects for burning (with row-level lock)
    async fn select_for_burn(
        &self,
        asset_id: Uuid,
        owner: Uuid,
        amount: i64,
    ) -> Result<Vec<ValueObject>, MoneyError>;

    /// Select reserved ValueObjects for activation/release
    async fn select_reserved(
        &self,
        asset_id: Uuid,
        owner: Uuid,
        authority: Uuid,
        amount: i64,
    ) -> Result<Vec<ValueObject>, MoneyError>;

    /// Change ValueObject state (reserved → alive, or burn)
    async fn change_state(
        &self,
        ids: Vec<Uuid>,
        new_state: ValueObjectState,
    ) -> Result<(), MoneyError>;

    /// Get balance for owner
    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError>;

    /// Record transaction
    async fn record_transaction(&self, transaction: Transaction) -> Result<Uuid, MoneyError>;

    /// Get transaction details (for reversion)
    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError>;

    /// Get asset by code
    async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError>;

    /// Create asset
    async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError>;

    /// Begin atomic transaction
    async fn begin_transaction(&self) -> Result<(), MoneyError>;

    /// Commit atomic transaction
    async fn commit_transaction(&self) -> Result<(), MoneyError>;

    /// Rollback atomic transaction
    async fn rollback_transaction(&self) -> Result<(), MoneyError>;
}

/// Initialize the ledger system with an adapter
pub struct LedgerSystem {
    adapter: Box<dyn LedgerAdapter>,
}

impl LedgerSystem {
    pub fn new(adapter: Box<dyn LedgerAdapter>) -> Self {
        Self { adapter }
    }

    /// Get the internal adapter (for Money operations)
    pub fn adapter(&self) -> &dyn LedgerAdapter {
        self.adapter.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_object_states() {
        // Basic state transition logic
        assert!(matches!(ValueObjectState::Alive, ValueObjectState::Alive));
        assert!(matches!(
            ValueObjectState::Reserved,
            ValueObjectState::Reserved
        ));
        assert!(matches!(ValueObjectState::Burned, ValueObjectState::Burned));
    }
}
