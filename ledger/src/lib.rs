// ledger/src/lib.rs
pub mod account;
pub mod adapters;
pub mod asset;
pub mod balance;
pub mod error;
pub mod holding;
pub mod money;
pub mod transaction;
pub mod value_object;

pub use account::{Account, AccountBalance, AccountQuery};
pub use asset::Asset;
pub use balance::Balance;
use chrono::{DateTime, Utc};
pub use error::MoneyError;
pub use holding::{Holding, Portfolio};
pub use money::{ExecutionPlan, LedgerContext, Money, MoneySlice, Operation, TransactionContext};
pub use transaction::Transaction;
pub use value_object::{ValueObject, ValueObjectState};

use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

/// Error for an adapter that has not implemented the account registry.
/// Deliberately an error rather than an empty result — see the trait.
fn unsupported(method: &str) -> MoneyError {
    MoneyError::Storage(format!(
        "this LedgerAdapter does not implement the account registry ({method})"
    ))
}

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

    /// All assets held by `owner` with a non-zero balance.
    async fn get_holdings(&self, owner: Uuid) -> Result<Vec<Holding>, MoneyError>;

    /// All transactions that touched `asset_id` within `timespan`.
    async fn get_transactions_for_asset(
        &self,
        asset_id: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError>;

    // ACCOUNT REGISTRY
    //
    // Descriptive only — see `account::Account`. An owner never has to be
    // registered to hold a balance, so nothing in the money path depends
    // on any of this.
    //
    // Each method has a default that *errors*, so adding the registry
    // does not break an existing third-party adapter at compile time
    // while still refusing to quietly answer "no accounts" — a silently
    // empty registry is precisely the invisible-account problem this
    // exists to solve. The bundled Postgres and in-memory adapters
    // override all of them.

    /// Register `account`, or update the label/kind/metadata of the one
    /// already registered for its `owner`. Idempotent: safe to call on
    /// every application start, which is the intended usage for accounts
    /// that are known statically.
    ///
    /// `created_at` is preserved on update. Returns
    /// [`MoneyError::Storage`] if `key` is already taken by a *different*
    /// owner — a key identifies exactly one account for the life of the
    /// ledger.
    async fn register_account(&self, account: &Account) -> Result<Account, MoneyError> {
        let _ = account;
        Err(unsupported("register_account"))
    }

    /// Look up by ledger owner id. `None` when the owner has never been
    /// registered — which is not an error, just an undescribed account.
    async fn get_account(&self, owner: Uuid) -> Result<Option<Account>, MoneyError> {
        let _ = owner;
        Err(unsupported("get_account"))
    }

    /// Look up by stable key. This is the lookup that still works when
    /// whatever table `owner` came from is gone.
    async fn get_account_by_key(&self, key: &str) -> Result<Option<Account>, MoneyError> {
        let _ = key;
        Err(unsupported("get_account_by_key"))
    }

    /// Enumerate registered accounts. The only way to discover accounts
    /// without already knowing their uuids.
    async fn list_accounts(&self, query: &AccountQuery) -> Result<Vec<Account>, MoneyError> {
        let _ = query;
        Err(unsupported("list_accounts"))
    }

    /// Enumerate registered accounts together with their balance in one
    /// asset. One round trip instead of N+1 — the shape an internal
    /// accounts dashboard needs.
    async fn list_account_balances(
        &self,
        asset_id: Uuid,
        query: &AccountQuery,
    ) -> Result<Vec<AccountBalance>, MoneyError> {
        let _ = (asset_id, query);
        Err(unsupported("list_account_balances"))
    }

    /// Retire an account. Idempotent; re-archiving keeps the original
    /// timestamp. There is no delete — see [`Account`].
    async fn archive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        let _ = owner;
        Err(unsupported("archive_account"))
    }

    /// Bring an archived account back. Idempotent.
    async fn unarchive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        let _ = owner;
        Err(unsupported("unarchive_account"))
    }
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
