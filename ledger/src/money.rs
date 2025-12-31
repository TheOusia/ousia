// ousia/src/ledger/money.rs
use std::sync::Arc;
use ulid::Ulid;

use super::{Balance, LedgerSystem, MoneyError, Transaction, TransactionHandle, ValueObject};

/// Money is a capability handle for (asset, owner)
/// It does not hold state or balance
#[derive(Clone)]
pub struct Money {
    asset_code: String,
    owner: Ulid,
    system: Arc<LedgerSystem>,
}

impl Money {
    /// Create a new Money capability handle
    pub fn new(system: Arc<LedgerSystem>, asset_code: impl Into<String>, owner: Ulid) -> Self {
        Self {
            asset_code: asset_code.into(),
            owner,
            system,
        }
    }

    /// Execute an atomic money operation
    pub async fn atomic<F, R>(f: F) -> Result<(R, TransactionHandle), MoneyError>
    where
        F: FnOnce() -> Result<R, MoneyError>,
    {
        // Begin transaction
        // Execute closure
        // Validate all slices consumed
        // Commit
        todo!("Atomic execution context")
    }

    /// Mint new money (deposit authority)
    pub async fn mint(
        asset_code: impl Into<String>,
        owner: Ulid,
        amount: i64,
        metadata: String,
        system: Arc<LedgerSystem>,
    ) -> Result<TransactionHandle, MoneyError> {
        Self::mint_internal(asset_code.into(), owner, amount, metadata, system, None).await
    }

    /// Mint with idempotency key (for webhook handling)
    pub async fn mint_idempotent(
        idempotency_key: String,
        asset_code: impl Into<String>,
        owner: Ulid,
        amount: i64,
        metadata: String,
        system: Arc<LedgerSystem>,
    ) -> Result<TransactionHandle, MoneyError> {
        Self::mint_internal(
            asset_code.into(),
            owner,
            amount,
            metadata,
            system,
            Some(idempotency_key),
        )
        .await
    }

    async fn mint_internal(
        asset_code: String,
        owner: Ulid,
        amount: i64,
        metadata: String,
        system: Arc<LedgerSystem>,
        idempotency_key: Option<String>,
    ) -> Result<TransactionHandle, MoneyError> {
        if amount <= 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = system.adapter();
        let asset = adapter.get_asset(&asset_code).await?;

        // Mint ValueObjects
        adapter
            .mint_value_objects(asset.id, owner, amount, format!("mint:{}", metadata))
            .await?;

        // Record transaction
        let transaction = Transaction::new(asset.id, None, Some(owner), 0, amount, metadata);

        let tx_id = adapter.record_transaction(transaction.clone()).await?;

        Ok(TransactionHandle::new(&transaction))
    }

    /// Reserve money (escrow authority)
    pub async fn reserve(
        asset_code: impl Into<String>,
        from: Ulid,
        for_authority: Ulid,
        amount: i64,
        metadata: String,
        system: Arc<LedgerSystem>,
    ) -> Result<TransactionHandle, MoneyError> {
        if amount <= 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = system.adapter();
        let asset = adapter.get_asset(&asset_code.into()).await?;

        // Burn from sender
        let to_burn = adapter.select_for_burn(asset.id, from, amount).await?;
        let burn_ids: Vec<Ulid> = to_burn.iter().map(|vo| vo.id).collect();

        if to_burn.iter().map(|vo| vo.amount).sum::<i64>() < amount {
            return Err(MoneyError::InsufficientFunds);
        }

        adapter
            .burn_value_objects(burn_ids, format!("reserve:{}", metadata))
            .await?;

        adapter
            .mint_value_objects(
                asset.id,
                for_authority,
                amount,
                format!("reserve:{}", metadata),
            )
            .await?;

        // Record transaction
        let transaction = Transaction::new(
            asset.id,
            Some(from),
            Some(for_authority),
            amount,
            amount,
            format!("reserve:{}", metadata),
        );

        adapter.record_transaction(transaction.clone()).await?;

        Ok(TransactionHandle::new(&transaction))
    }

    /// Create a slice for planning (ephemeral)
    pub fn slice(&self, amount: i64) -> Result<MoneySlice, MoneyError> {
        if amount <= 0 {
            return Err(MoneyError::InvalidAmount);
        }

        Ok(MoneySlice {
            parent: self.clone(),
            amount,
            consumed: false,
        })
    }

    // /// Fragment amount into ValueObjects
    // fn fragment_amount(
    //     amount: i64,
    //     unit: i64,
    //     asset_id: Ulid,
    //     owner: Ulid,
    //     reserved_for: Option<Ulid>,
    // ) -> Result<Vec<ValueObject>, MoneyError> {
    //     let mut fragments = Vec::new();
    //     let mut remaining = amount;

    //     while remaining > 0 {
    //         let chunk = remaining.min(unit);

    //         let vo = if let Some(authority) = reserved_for {
    //             ValueObject::new_reserved(asset_id, owner, chunk, authority)
    //         } else {
    //             ValueObject::new_alive(asset_id, owner, chunk)
    //         };

    //         fragments.push(vo);
    //         remaining -= chunk;
    //     }

    //     Ok(fragments)
    // }
}

/// MoneySlice - ephemeral planning construct
/// Only exists inside Money::atomic blocks
pub struct MoneySlice {
    parent: Money,
    amount: i64,
    consumed: bool,
}

impl MoneySlice {
    /// Create a sub-slice from this slice
    pub fn slice(&mut self, amount: i64) -> Result<MoneySlice, MoneyError> {
        if amount <= 0 || amount > self.amount {
            return Err(MoneyError::InvalidAmount);
        }

        self.amount -= amount;

        Ok(MoneySlice {
            parent: self.parent.clone(),
            amount,
            consumed: false,
        })
    }

    /// Transfer this slice to a recipient
    pub async fn transfer_to(
        mut self,
        recipient: Ulid,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if self.consumed {
            return Err(MoneyError::UnconsumedSlice);
        }

        let adapter = self.parent.system.adapter();
        let asset = adapter.get_asset(&self.parent.asset_code).await?;

        // Burn from sender
        let to_burn = adapter
            .select_for_burn(asset.id, self.parent.owner, self.amount)
            .await?;
        let burn_ids: Vec<Ulid> = to_burn.iter().map(|vo| vo.id).collect();

        if to_burn.iter().map(|vo| vo.amount).sum::<i64>() < self.amount {
            return Err(MoneyError::InsufficientFunds);
        }

        adapter
            .burn_value_objects(burn_ids, format!("transfer:{}", metadata))
            .await?;

        // Mint to receiver
        adapter
            .mint_value_objects(
                asset.id,
                recipient,
                self.amount,
                format!("transfer:{}", metadata),
            )
            .await?;

        // Calculate change
        let burned_total: i64 = to_burn.iter().map(|vo| vo.amount).sum();
        let change = burned_total - self.amount;

        if change > 0 {
            // Mint change back to sender
            adapter
                .mint_value_objects(
                    asset.id,
                    self.parent.owner,
                    change,
                    format!("change:{}", metadata),
                )
                .await?;
        }

        // Record transaction
        let transaction = Transaction::new(
            asset.id,
            Some(self.parent.owner),
            Some(recipient),
            burned_total,
            self.amount,
            metadata,
        );

        adapter.record_transaction(transaction).await?;

        self.consumed = true;
        Ok(())
    }

    /// Burn this slice (no recipient)
    pub async fn burn(mut self, metadata: String) -> Result<(), MoneyError> {
        if self.consumed {
            return Err(MoneyError::UnconsumedSlice);
        }

        let adapter = self.parent.system.adapter();
        let asset = adapter.get_asset(&self.parent.asset_code).await?;

        // Select and burn
        let to_burn = adapter
            .select_for_burn(asset.id, self.parent.owner, self.amount)
            .await?;
        let burn_ids: Vec<Ulid> = to_burn.iter().map(|vo| vo.id).collect();

        if to_burn.iter().map(|vo| vo.amount).sum::<i64>() < self.amount {
            return Err(MoneyError::InsufficientFunds);
        }

        adapter
            .burn_value_objects(burn_ids, format!("burn:{}", metadata))
            .await?;

        // Record transaction
        let transaction = Transaction::new(
            asset.id,
            Some(self.parent.owner),
            None,
            self.amount,
            0,
            metadata,
        );

        adapter.record_transaction(transaction).await?;

        self.consumed = true;
        Ok(())
    }

    /// Check if consumed
    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

impl Drop for MoneySlice {
    #[cfg(test)]
    fn drop(&mut self) {
        if !self.consumed && self.amount > 0 {
            println!("MoneySlice dropped without being consumed")
        }
    }

    #[cfg(not(test))]
    fn drop(&mut self) {
        if !self.consumed && self.amount > 0 {
            // This should cause atomic block to fail
            panic!("MoneySlice dropped without being consumed");
        }
    }
}

/// Balance query (read-only)
impl Balance {
    pub async fn get(
        asset_code: impl Into<String>,
        owner: Ulid,
        system: Arc<LedgerSystem>,
    ) -> Result<Balance, MoneyError> {
        let adapter = system.adapter();
        let asset = adapter.get_asset(&asset_code.into()).await?;
        adapter.get_balance(asset.id, owner).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{Asset, LedgerAdapter, ValueObjectState};

    use super::*;

    #[test]
    fn test_money_slice_amount_validation() {
        // Test that invalid amounts are rejected
        let system = Arc::new(LedgerSystem::new(Box::new(MockAdapter)));
        let money = Money::new(system, "USD", Ulid::new());

        assert!(money.slice(0).is_err());
        assert!(money.slice(-100).is_err());
        assert!(money.slice(100).is_ok());
    }

    struct MockAdapter;

    #[async_trait::async_trait]
    impl LedgerAdapter for MockAdapter {
        async fn mint_value_objects(
            &self,
            _asset_id: Ulid,
            _owner: Ulid,
            _amount: i64,
            _metadata: String,
        ) -> Result<Vec<ValueObject>, MoneyError> {
            Ok(vec![])
        }

        async fn burn_value_objects(
            &self,
            _ids: Vec<Ulid>,
            _metadata: String,
        ) -> Result<(), MoneyError> {
            Ok(())
        }

        async fn select_for_burn(
            &self,
            _asset_id: Ulid,
            _owner: Ulid,
            _amount: i64,
        ) -> Result<Vec<ValueObject>, MoneyError> {
            Ok(vec![])
        }

        async fn select_reserved(
            &self,
            _asset_id: Ulid,
            _owner: Ulid,
            _authority: Ulid,
            _amount: i64,
        ) -> Result<Vec<ValueObject>, MoneyError> {
            Ok(vec![])
        }

        async fn change_state(
            &self,
            _ids: Vec<Ulid>,
            _new_state: ValueObjectState,
        ) -> Result<(), MoneyError> {
            Ok(())
        }

        async fn get_balance(&self, _asset_id: Ulid, _owner: Ulid) -> Result<Balance, MoneyError> {
            Ok(Balance::new(Ulid::new(), Ulid::new()))
        }

        async fn record_transaction(&self, _transaction: Transaction) -> Result<Ulid, MoneyError> {
            Ok(Ulid::new())
        }

        async fn get_transaction(&self, _tx_id: Ulid) -> Result<Transaction, MoneyError> {
            Err(MoneyError::TransactionNotFound)
        }

        async fn get_asset(&self, _code: &str) -> Result<Asset, MoneyError> {
            Ok(Asset::new("USD", 10_000))
        }

        async fn create_asset(&self, _asset: Asset) -> Result<(), MoneyError> {
            Ok(())
        }

        async fn begin_transaction(&self) -> Result<(), MoneyError> {
            Ok(())
        }

        async fn commit_transaction(&self) -> Result<(), MoneyError> {
            Ok(())
        }

        async fn rollback_transaction(&self) -> Result<(), MoneyError> {
            Ok(())
        }
    }
}
