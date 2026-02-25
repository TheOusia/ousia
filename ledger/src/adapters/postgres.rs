use std::collections::HashMap;

use crate::{
    Asset, Balance, ExecutionPlan, Holding, LedgerAdapter, MoneyError, Operation, Transaction,
    ValueObject,
};
use chrono::{DateTime, Utc};
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
        //
        // Include created_at in the composite indexes so the FIFO ORDER BY
        // created_at ASC in the lock query is satisfied from the index alone.
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_value_objects_asset_owner_state_created
            ON ledger_value_objects(asset, owner, state, created_at ASC)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_value_objects_owner_state_created
            ON ledger_value_objects(owner, state, created_at ASC)
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

        // Partial index over live VOs only — burned rows are cold/archivable and
        // should not bloat the index used by live queries.
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_value_objects_live
            ON ledger_value_objects(asset, owner, created_at ASC)
            WHERE state != 'burned'
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

        // Transaction idempotency table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_transaction_idempotency_keys (
                key TEXT NOT NULL PRIMARY KEY,
                transaction_id UUID NOT NULL REFERENCES ledger_transactions(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_ledger_transaction_idempotency_keys_transaction_id
            ON ledger_transaction_idempotency_keys(transaction_id)
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

// ── Fragmentation ─────────────────────────────────────────────────────────────
//
// `unit`          — preferred chunk size (soft, natural denomination).
// `max_fragments` — hard cap on total VO count per mint (default 1_000).
//
// chunk = max(unit, ceil(amount / max_fragments))
//
// `unit` wins when the amount is small enough. When the amount would produce
// more fragments than the budget allows, chunk scales up past `unit`
// automatically so the count always stays ≤ max_fragments.

const DEFAULT_MAX_FRAGMENTS: u64 = 1_000;

fn fragment_amount_smart(
    amount: u64,
    unit: u64,
    max_fragments: u64,
    asset_id: Uuid,
    owner: Uuid,
    reserved_for: Option<Uuid>,
) -> Vec<ValueObject> {
    debug_assert!(unit > 0, "unit must be > 0");
    debug_assert!(max_fragments > 0, "max_fragments must be > 0");

    if amount == 0 {
        return vec![];
    }

    let min_chunk = (amount + max_fragments - 1) / max_fragments; // ceil div
    let chunk = unit.max(min_chunk);

    let mut fragments = Vec::new();
    let mut remaining = amount;

    while remaining > 0 {
        let vo_amount = remaining.min(chunk);
        let vo = match reserved_for {
            Some(authority) => ValueObject::new_reserved(asset_id, owner, vo_amount, authority),
            None => ValueObject::new_alive(asset_id, owner, vo_amount),
        };
        fragments.push(vo);
        remaining -= vo_amount;
    }

    fragments
}

// ─────────────────────────────────────────────────────────────────────────────

#[async_trait::async_trait]
trait PostgresInternalLedgerAdapter {
    async fn mint_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<(), MoneyError>;

    // Change mints call this directly so they can pass burned_count as the
    // fragment budget, consolidating rather than blindly re-fragmenting.
    async fn mint_internal_tx_with_max_fragments(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        max_fragments: u64,
    ) -> Result<(), MoneyError>;

    async fn mint_reserved_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        authority: Uuid,
    ) -> Result<(), MoneyError>;

    async fn record_transaction_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: Transaction,
    ) -> Result<(), MoneyError>;

    async fn get_asset_by_id(&self, asset_id: Uuid) -> Result<Asset, MoneyError>;

    /// Hard cap on fragment count per mint. Defaults to 1,000.
    /// Override per-adapter if needed.
    fn max_fragments(&self) -> u64 {
        DEFAULT_MAX_FRAGMENTS
    }
}

#[async_trait::async_trait]
impl<T> PostgresInternalLedgerAdapter for T
where
    T: PostgresLedgerAdapter + Send + Sync,
{
    async fn mint_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<(), MoneyError> {
        self.mint_internal_tx_with_max_fragments(tx, asset_id, owner, amount, self.max_fragments())
            .await
    }

    async fn mint_internal_tx_with_max_fragments(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        max_fragments: u64,
    ) -> Result<(), MoneyError> {
        let asset = self.get_asset_by_id(asset_id).await?;
        let fragments =
            fragment_amount_smart(amount, asset.unit, max_fragments, asset_id, owner, None);

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
        let fragments = fragment_amount_smart(
            amount,
            asset.unit,
            self.max_fragments(),
            asset_id,
            owner,
            Some(authority),
        );

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

    async fn record_transaction_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: Transaction,
    ) -> Result<(), MoneyError> {
        // Insert idempotency key FIRST — if it conflicts, bail before touching transactions
        if let Some(ref raw_key) = transaction.idempotency_key {
            let hash = crate::hash_idempotency_key(raw_key);

            let inserted = sqlx::query(
                r#"
                INSERT INTO ledger_transaction_idempotency_keys (key, transaction_id, created_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO NOTHING
                RETURNING key
                "#,
            )
            .bind(&hash)
            .bind(transaction.id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

            if inserted.is_none() {
                return Err(MoneyError::DuplicateIdempotencyKey(transaction.id));
            }
        }

        sqlx::query(
            r#"
            INSERT INTO ledger_transactions
                (id, asset, sender, receiver, burned_amount, minted_amount, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(transaction.id)
        .bind(transaction.asset)
        .bind(transaction.sender)
        .bind(transaction.receiver)
        .bind(transaction.burned_amount as i64)
        .bind(transaction.minted_amount as i64)
        .bind(&transaction.metadata)
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

    // max_fragments has a default impl above; override per-adapter if needed.
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
        // Select oldest VOs first (FIFO) so burned rows age out predictably and
        // can be archived by a background job once cold.
        // HashMap<(asset_id, owner) -> (locked_vo_ids, total_locked)>
        let mut locked: HashMap<(Uuid, Uuid), (Vec<Uuid>, u64)> = HashMap::new();

        for (asset_id, owner, required) in locks {
            let rows = sqlx::query(
                r#"
            SELECT id, amount
            FROM ledger_value_objects
            WHERE asset = $1 AND owner = $2 AND state = 'alive'
            ORDER BY created_at ASC
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
                Operation::Settle {
                    asset_id,
                    authority,
                    receiver,
                    amount,
                    ..
                } => {
                    // Lock reserved VOs owned by authority, FIFO order
                    let rows = sqlx::query(
                        r#"
                        SELECT id, amount
                        FROM ledger_value_objects
                        WHERE asset = $1 AND owner = $2 AND state = 'reserved'
                        ORDER BY created_at ASC
                        FOR UPDATE SKIP LOCKED
                        "#,
                    )
                    .bind(asset_id)
                    .bind(authority)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;

                    let mut ids_to_burn: Vec<Uuid> = Vec::new();
                    let mut total_reserved = 0u64;

                    for row in rows {
                        let id: Uuid = row
                            .try_get("id")
                            .map_err(|e| MoneyError::Storage(e.to_string()))?;
                        let amt: i64 = row
                            .try_get("amount")
                            .map_err(|e| MoneyError::Storage(e.to_string()))?;
                        ids_to_burn.push(id);
                        total_reserved += amt as u64;
                        if total_reserved >= *amount {
                            break;
                        }
                    }

                    if total_reserved < *amount {
                        tx.rollback().await.ok();
                        return Err(MoneyError::InsufficientFunds);
                    }

                    let burned_count = ids_to_burn.len() as u64;

                    // Burn selected reserved VOs
                    for id in &ids_to_burn {
                        sqlx::query(
                            "UPDATE ledger_value_objects SET state = 'burned' WHERE id = $1",
                        )
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| MoneyError::Storage(e.to_string()))?;
                    }

                    // Return change as reserved VOs for authority
                    let change = total_reserved - *amount;
                    if change > 0 {
                        self.mint_reserved_internal_tx(&mut tx, *asset_id, *authority, change, *authority)
                            .await?;
                    }

                    // Mint alive VOs for receiver, consolidated into at most burned_count fragments
                    self.mint_internal_tx_with_max_fragments(
                        &mut tx,
                        *asset_id,
                        *receiver,
                        *amount,
                        burned_count,
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
            // Use burned_count as the fragment budget for change — this consolidates
            // rather than re-fragmenting. Each spend is a compaction opportunity.
            let burned_count = ids.len() as u64;

            // Burn every locked VO
            for id in ids {
                sqlx::query("UPDATE ledger_value_objects SET state = 'burned' WHERE id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;
            }

            // Mint change — consolidated into at most burned_count fragments.
            let change = total_locked - total_used;
            if change > 0 {
                self.mint_internal_tx_with_max_fragments(
                    &mut tx,
                    *asset_id,
                    *owner,
                    change,
                    burned_count,
                )
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

    async fn check_idempotency_key(&self, key: &str) -> Result<(), MoneyError> {
        let hash = crate::hash_idempotency_key(key);

        let row = sqlx::query(
            r#"
            SELECT transaction_id
            FROM ledger_transaction_idempotency_keys
            WHERE key = $1
            "#,
        )
        .bind(&hash)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        if let Some(row) = row {
            let tx_id: Uuid = row
                .try_get("transaction_id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            return Err(MoneyError::DuplicateIdempotencyKey(tx_id));
        }

        Ok(())
    }

    async fn get_transaction_by_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Transaction, MoneyError> {
        let hash = crate::hash_idempotency_key(key);

        let row = sqlx::query(
            r#"
            SELECT
                lt.id, ik.key as idempotency_key, lt.asset, la.code,
                lt.sender, lt.receiver,
                lt.burned_amount, lt.minted_amount,
                lt.metadata, lt.created_at
            FROM ledger_transaction_idempotency_keys ik
            JOIN ledger_transactions lt ON ik.transaction_id = lt.id
            JOIN ledger_assets la ON lt.asset = la.id
            WHERE ik.key = $1
            "#,
        )
        .bind(&hash)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or(MoneyError::TransactionNotFound)?;

        Ok(Transaction {
            id: row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
            idempotency_key: row
                .try_get("idempotency_key")
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

    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT lt.id, ik.key as idempotency_key, lt.asset, a.code, lt.sender, lt.receiver, lt.burned_amount, lt.minted_amount, lt.metadata, lt.created_at
            FROM ledger_transactions lt
            LEFT JOIN assets a ON lt.asset = a.id
            LEFT JOIN ledger_transaction_idempotency_keys ik ON ik.transaction_id = lt.id
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
            idempotency_key: row
                .try_get("idempotency_key")
                .map_err(|e| MoneyError::Storage(e.to_string()))?,
        })
    }

    async fn get_transactions_for_owner(
        &self,
        owner: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError> {
        let rows = sqlx::query(
            r#"
            SELECT lt.id, ik.key as idempotency_key, lt.asset, a.code, lt.sender, lt.receiver, lt.burned_amount, lt.minted_amount, lt.metadata, lt.created_at
            FROM ledger_transactions lt
            LEFT JOIN ledger_assets a ON lt.asset = a.id
            LEFT JOIN ledger_transaction_idempotency_keys ik ON ik.transaction_id = lt.id
            WHERE (lt.sender = $1 OR lt.receiver = $1) AND lt.created_at BETWEEN $2 AND $3
            "#,
        )
        .bind(owner)
        .bind(timespan[0])
        .bind(timespan[1])
        .fetch_all(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let mut transactions = Vec::new();
        for row in rows {
            let id = row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let asset = row
                .try_get("asset")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let code = row
                .try_get("code")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let sender = row
                .try_get("sender")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let receiver = row
                .try_get("receiver")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;

            let burned_amount =
                row.try_get::<i64, _>("burned_amount")
                    .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let minted_amount =
                row.try_get::<i64, _>("minted_amount")
                    .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let metadata = row
                .try_get("metadata")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let created_at = row
                .try_get("created_at")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;

            let idempotency_key = row
                .try_get("idempotency_key")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;

            transactions.push(Transaction {
                id,
                idempotency_key,
                asset,
                code,
                sender,
                receiver,
                burned_amount,
                minted_amount,
                metadata,
                created_at,
            });
        }

        Ok(transactions)
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

    async fn get_holdings(&self, owner: Uuid) -> Result<Vec<Holding>, MoneyError> {
        let rows = sqlx::query(
            r#"
            SELECT
                la.id, la.code, la.unit, la.decimals,
                COALESCE(SUM(vo.amount) FILTER (WHERE vo.state = 'alive'), 0)::BIGINT  AS alive_sum,
                COALESCE(SUM(vo.amount) FILTER (WHERE vo.state = 'reserved'), 0)::BIGINT AS reserved_sum
            FROM ledger_value_objects vo
            JOIN ledger_assets la ON vo.asset = la.id
            WHERE vo.owner = $1
            GROUP BY la.id, la.code, la.unit, la.decimals
            HAVING COALESCE(SUM(vo.amount), 0) > 0
            "#,
        )
        .bind(owner)
        .fetch_all(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let mut holdings = Vec::new();
        for row in rows {
            let asset_id: Uuid = row
                .try_get("id")
                .map_err(|e| MoneyError::Storage(e.to_string()))?;
            let asset = Asset {
                id: asset_id,
                code: row
                    .try_get("code")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
                unit: row
                    .try_get::<i64, _>("unit")
                    .map_err(|e| MoneyError::Storage(e.to_string()))? as u64,
                decimals: row
                    .try_get::<i16, _>("decimals")
                    .map_err(|e| MoneyError::Storage(e.to_string()))? as u8,
            };
            let alive = row
                .try_get::<i64, _>("alive_sum")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let reserved = row
                .try_get::<i64, _>("reserved_sum")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let balance = Balance::from_value_objects(owner, asset_id, alive, reserved);
            holdings.push(Holding::new(asset, balance));
        }

        Ok(holdings)
    }

    async fn get_transactions_for_asset(
        &self,
        asset_id: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError> {
        let rows = sqlx::query(
            r#"
            SELECT lt.id, ik.key as idempotency_key, lt.asset, la.code,
                   lt.sender, lt.receiver, lt.burned_amount, lt.minted_amount,
                   lt.metadata, lt.created_at
            FROM ledger_transactions lt
            LEFT JOIN ledger_assets la ON lt.asset = la.id
            LEFT JOIN ledger_transaction_idempotency_keys ik ON ik.transaction_id = lt.id
            WHERE lt.asset = $1 AND lt.created_at BETWEEN $2 AND $3
            "#,
        )
        .bind(asset_id)
        .bind(timespan[0])
        .bind(timespan[1])
        .fetch_all(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let mut transactions = Vec::new();
        for row in rows {
            transactions.push(Transaction {
                id: row
                    .try_get("id")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?,
                idempotency_key: row
                    .try_get("idempotency_key")
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
            });
        }

        Ok(transactions)
    }
}
