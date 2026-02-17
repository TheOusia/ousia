use std::collections::HashMap;

use crate::{
    Asset, Balance, ExecutionPlan, LedgerAdapter, MoneyError, Operation, Transaction, ValueObject,
    ValueObjectState,
};
use sqlx::Row;
use uuid::Uuid;

pub trait PostgresLedgerAdapter {
    fn get_pool(&self) -> sqlx::PgPool;
}

#[async_trait::async_trait]
pub trait PostgresSchemaLedgerAdapter {
    /// Initialize the schema for the internal ledger.
    /// This function should only be called for standalone ledger.
    /// If using Ousia. Call init_schema() on the adapter.
    async fn init_ledger_schema(&self) -> Result<(), MoneyError>;
}

#[async_trait::async_trait]
impl<T> PostgresSchemaLedgerAdapter for T
where
    T: PostgresLedgerAdapter + Send + Sync,
{
    async fn init_ledger_schema(&self) -> Result<(), MoneyError> {
        let mut tx = self
            .get_pool()
            .begin()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Assets table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_assets (
                id UUID PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                unit BIGINT NOT NULL,
                decimals SMALLINT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // ValueObjects table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_value_objects (
                id UUID PRIMARY KEY,
                asset UUID NOT NULL REFERENCES ledger_assets(id),
                owner UUID NOT NULL,
                amount BIGINT NOT NULL CHECK (amount > 0),
                state TEXT NOT NULL CHECK (state IN ('alive', 'reserved', 'burned')),
                reserved_for UUID,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Indexes for ValueObjects
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_value_objects_asset_owner_state
            ON ledger_value_objects(asset, owner, state)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_value_objects_owner
            ON ledger_value_objects(owner)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Transactions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_transactions (
                id UUID PRIMARY KEY,
                asset UUID NOT NULL REFERENCES ledger_assets(id),
                sender UUID,
                receiver UUID,
                burned_amount BIGINT NOT NULL,
                minted_amount BIGINT NOT NULL,
                metadata TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_transactions_asset
            ON ledger_transactions(asset)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_transactions_sender
            ON ledger_transactions(sender)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_transactions_receiver
            ON ledger_transactions(receiver)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[async_trait::async_trait]
trait PostgresInternalLedgerAdapter {
    async fn select_for_burn_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<Vec<ValueObject>, MoneyError>;

    async fn mint_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<(), MoneyError>;

    async fn mint_reserved_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        authority: Uuid,
    ) -> Result<(), MoneyError>;

    async fn burn_value_objects_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ids: Vec<Uuid>,
    ) -> Result<(), MoneyError>;

    async fn record_transaction_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: Transaction,
    ) -> Result<(), MoneyError>;

    async fn get_asset_by_id(&self, asset_id: Uuid) -> Result<Asset, MoneyError>;

    fn fragment_amount(
        &self,
        amount: u64,
        unit: u64,
        asset_id: Uuid,
        owner: Uuid,
        reserved_for: Option<Uuid>,
    ) -> Vec<ValueObject>;
}

#[async_trait::async_trait]
impl<T> PostgresInternalLedgerAdapter for T
where
    T: PostgresLedgerAdapter + Send + Sync,
{
    async fn select_for_burn_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<Vec<ValueObject>, MoneyError> {
        let rows = sqlx::query(
            r#"
            SELECT id, asset, owner, amount, state, reserved_for, created_at
            FROM ledger_value_objects
            WHERE asset = $1 AND owner = $2 AND state = 'alive'
            ORDER BY amount ASC
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(asset_id)
        .bind(owner)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let mut selected = Vec::new();
        let mut total = 0u64;

        for row in rows {
            let vo_amount =
                row.try_get::<i64, _>("amount")
                    .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;

            let vo = ValueObject {
                id: row
                    .try_get("id")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
                asset: row
                    .try_get("asset")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
                owner: row
                    .try_get("owner")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
                amount: vo_amount,
                state: ValueObjectState::Alive,
                reserved_for: None,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
            };

            selected.push(vo);
            total += vo_amount;

            if total >= amount {
                break;
            }
        }

        Ok(selected)
    }

    async fn mint_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<(), MoneyError> {
        // Get asset to determine fragmentation unit
        let asset = self.get_asset_by_id(asset_id).await?;

        // Fragment the amount
        let fragments = self.fragment_amount(amount, asset.unit, asset_id, owner, None);

        // Insert all fragments
        for fragment in fragments {
            sqlx::query(
                r#"
                INSERT INTO ledger_value_objects (id, asset, owner, amount, state, reserved_for, created_at)
                VALUES ($1, $2, $3, $4, 'alive', NULL, NOW())
                "#,
            )
            .bind(fragment.id)
            .bind(fragment.asset)
            .bind(fragment.owner)
            .bind(fragment.amount as i64)
            .execute(&mut **tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn mint_reserved_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        authority: Uuid,
    ) -> Result<(), MoneyError> {
        let asset = self.get_asset_by_id(asset_id).await?;
        let fragments = self.fragment_amount(amount, asset.unit, asset_id, owner, Some(authority));

        for fragment in fragments {
            sqlx::query(
                r#"
                INSERT INTO ledger_value_objects (id, asset, owner, amount, state, reserved_for, created_at)
                VALUES ($1, $2, $3, $4, 'reserved', $5, NOW())
                "#,
            )
            .bind(fragment.id)
            .bind(fragment.asset)
            .bind(fragment.owner)
            .bind(fragment.amount as i64)
            .bind(authority)
            .execute(&mut **tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn burn_value_objects_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        ids: Vec<Uuid>,
    ) -> Result<(), MoneyError> {
        for id in ids {
            sqlx::query(
                r#"
                UPDATE ledger_value_objects
                SET state = 'burned'
                WHERE id = $1
                "#,
            )
            .bind(id)
            .execute(&mut **tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn record_transaction_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: Transaction,
    ) -> Result<(), MoneyError> {
        sqlx::query(
            r#"
            INSERT INTO ledger_transactions (id, asset, sender, receiver, burned_amount, minted_amount, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(transaction.id)
        .bind(transaction.asset)
        .bind(transaction.sender)
        .bind(transaction.receiver)
        .bind(transaction.burned_amount as i64)
        .bind(transaction.minted_amount as i64)
        .bind(transaction.metadata)
        .bind(transaction.created_at)
        .execute(&mut **tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn get_asset_by_id(&self, asset_id: Uuid) -> Result<Asset, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT id, code, unit, decimals
            FROM ledger_assets
            WHERE id = $1
            "#,
        )
        .bind(asset_id)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

        Ok(Asset {
            id: row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            code: row
                .try_get("code")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            unit: row
                .try_get::<i64, _>("unit")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64,
            decimals: row
                .try_get::<i16, _>("decimals")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u8,
        })
    }

    fn fragment_amount(
        &self,
        amount: u64,
        unit: u64,
        asset_id: Uuid,
        owner: Uuid,
        reserved_for: Option<Uuid>,
    ) -> Vec<ValueObject> {
        let mut fragments = Vec::new();
        let mut remaining = amount;

        while remaining > 0 {
            let chunk = remaining.min(unit);

            let vo = if let Some(authority) = reserved_for {
                ValueObject::new_reserved(asset_id, owner, chunk, authority)
            } else {
                ValueObject::new_alive(asset_id, owner, chunk)
            };

            fragments.push(vo);
            remaining -= chunk;
        }

        fragments
    }
}

#[async_trait::async_trait]
impl<T> LedgerAdapter for T
where
    T: PostgresLedgerAdapter + PostgresInternalLedgerAdapter + Send + Sync,
{
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, u64)],
    ) -> Result<(), MoneyError> {
        let mut tx = self
            .get_pool()
            .begin()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // ── Phase 1: Lock & verify ─────────────────────────────────────────────
        // HashMap<(asset_id, owner) -> (locked_vo_ids, total_locked)>
        let mut locked: HashMap<(Uuid, Uuid), (Vec<Uuid>, u64)> = HashMap::new();

        for (asset_id, owner, required) in locks {
            let rows = sqlx::query(
                r#"
            SELECT id, amount
            FROM ledger_value_objects
            WHERE asset = $1 AND owner = $2 AND state = 'alive'
            ORDER BY amount ASC
            FOR UPDATE SKIP LOCKED
            "#,
            )
            .bind(asset_id)
            .bind(owner)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

            let mut ids = Vec::new();
            let mut total = 0u64;

            for row in rows {
                let id: Uuid = row
                    .try_get("id")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;
                let amount: i64 = row
                    .try_get("amount")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;

                ids.push(id);
                total += amount as u64;

                if total >= *required {
                    break;
                }
            }

            // Checked INSIDE the lock — this is the real double-spend guard
            if total < *required {
                tx.rollback().await.ok();
                return Err(MoneyError::InsufficientFunds);
            }

            locked.insert((*asset_id, *owner), (ids, total));
        }

        // ── Phase 2: Execute operations ────────────────────────────────────────
        let mut used: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        for op in plan.operations() {
            match op {
                Operation::Mint {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    self.mint_internal_tx(&mut tx, *asset_id, *owner, *amount)
                        .await?;
                }
                Operation::Burn {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *owner)).or_insert(0) += amount;
                }
                Operation::Transfer {
                    asset_id,
                    from,
                    to,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *from)).or_insert(0) += amount;
                    self.mint_internal_tx(&mut tx, *asset_id, *to, *amount)
                        .await?;
                }
                Operation::Reserve {
                    asset_id,
                    from,
                    for_authority,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *from)).or_insert(0) += amount;
                    self.mint_reserved_internal_tx(
                        &mut tx,
                        *asset_id,
                        *for_authority,
                        *amount,
                        *for_authority,
                    )
                    .await?;
                }
                Operation::RecordTransaction { transaction } => {
                    self.record_transaction_internal_tx(&mut tx, transaction.clone())
                        .await?;
                }
            }
        }

        // ── Phase 3: Burn locked VOs, mint change ──────────────────────────────
        for ((asset_id, owner), (ids, total_locked)) in &locked {
            let total_used = used.get(&(*asset_id, *owner)).copied().unwrap_or(0);

            // Burn every locked VO
            for id in ids {
                sqlx::query("UPDATE ledger_value_objects SET state = 'burned' WHERE id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;
            }

            // Mint change
            let change = total_locked - total_used;
            if change > 0 {
                self.mint_internal_tx(&mut tx, *asset_id, *owner, change)
                    .await?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError> {
        // PostgreSQL SUM returns NUMERIC, we need to cast to BIGINT
        let alive_sum: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(amount), 0)::BIGINT
            FROM ledger_value_objects
            WHERE asset = $1 AND owner = $2 AND state = 'alive'
            "#,
        )
        .bind(asset_id)
        .bind(owner)
        .fetch_one(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let reserved_sum: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(amount), 0)::BIGINT
            FROM ledger_value_objects
            WHERE asset = $1 AND owner = $2 AND state = 'reserved'
            "#,
        )
        .bind(asset_id)
        .bind(owner)
        .fetch_one(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(Balance::from_value_objects(
            owner,
            asset_id,
            alive_sum as u64,
            reserved_sum as u64,
        ))
    }

    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT lt.id, lt.asset, a.code, lt.sender, lt.receiver, lt.burned_amount, lt.minted_amount, lt.metadata, lt.created_at
            FROM ledger_transactions lt
            LEFT JOIN assets a ON lt.asset = a.id
            WHERE lt.id = $1
            "#,
        )
        .bind(tx_id)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or(MoneyError::TransactionNotFound)?;

        Ok(Transaction {
            id: row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            asset: row
                .try_get("asset")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            code: row
                .try_get("code")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            sender: row
                .try_get("sender")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            receiver: row
                .try_get("receiver")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            burned_amount: row
                .try_get::<i64, _>("burned_amount")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64,
            minted_amount: row
                .try_get::<i64, _>("minted_amount")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64,
            metadata: row
                .try_get("metadata")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
        })
    }

    async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT id, code, unit, decimals
            FROM ledger_assets
            WHERE code = $1
            "#,
        )
        .bind(code)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or_else(|| MoneyError::AssetNotFound(code.to_string()))?;

        Ok(Asset {
            id: row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            code: row
                .try_get("code")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            unit: row
                .try_get::<i64, _>("unit")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64,
            decimals: row
                .try_get::<i16, _>("decimals")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u8,
        })
    }

    async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError> {
        sqlx::query(
            r#"
            INSERT INTO ledger_assets (id, code, unit, decimals, created_at)
            VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (code) DO UPDATE SET unit = $3, decimals = $4
            "#,
        )
        .bind(asset.id)
        .bind(asset.code)
        .bind(asset.unit as i64)
        .bind(asset.decimals as i16)
        .execute(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        Ok(())
    }
}
