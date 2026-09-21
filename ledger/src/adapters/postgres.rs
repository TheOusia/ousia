use std::collections::HashMap;

use crate::{
    Account, AccountBalance, AccountQuery, Asset, Balance, ExecutionPlan, Holding, LedgerAdapter,
    MoneyError, Operation, Transaction, ValueObject,
};
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

pub trait PostgresLedgerAdapter {
    fn get_pool(&self) -> sqlx::PgPool;
}

#[async_trait::async_trait]
pub trait PostgresSchemaLedgerAdapter {
    /// Initialise the ledger schema, optionally seeding per-asset
    /// partitions of `ledger_value_objects`.
    ///
    /// `assets` is the caller-supplied list of asset codes (e.g.
    /// `&["USD", "NGN"]`). The library reads no environment variables
    /// — the caller decides where the list comes from. Pass `&[]` to
    /// create only the catch-all `_default` partition; assets can be
    /// added by calling again later with a wider list.
    ///
    /// Idempotent: safe to call on every application start.
    async fn init_ledger_schema(&self, assets: &[&str]) -> Result<(), MoneyError>;
}

#[async_trait::async_trait]
impl<T> PostgresSchemaLedgerAdapter for T
where
    T: PostgresLedgerAdapter + Send + Sync,
{
    async fn init_ledger_schema(&self, assets: &[&str]) -> Result<(), MoneyError> {
        let assets = normalize_asset_codes(assets)?;
        let mut tx = self
            .get_pool()
            .begin()
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Which schema the ledger's tables live in is a deployment
        // decision, read off the connection's own `search_path` rather
        // than passed in — see `resolve_target_schema`. Created here so a
        // deployment pointing at a fresh schema works on first boot.
        let schema = resolve_target_schema(&mut tx).await?;
        sqlx::query(&format!(
            r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
            quote_ident(&schema)
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Unqualified DDL below lands in `schema`: Postgres creates into
        // the first entry on the path regardless of what exists further
        // along it. `public` stays behind it so extensions installed
        // there keep resolving.
        sqlx::query(&format!(
            r#"SET LOCAL search_path TO "{}", public"#,
            quote_ident(&schema)
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Accounts registry — descriptive metadata for ledger owner ids.
        // Deliberately *not* referenced by a foreign key from
        // `ledger_value_objects`: registration is optional and every
        // balance that predates this table must keep working. See
        // `crate::account::Account`.
        //
        // `key` is UNIQUE because it is the account's durable identity —
        // the handle that still resolves when whatever row `owner` came
        // from has been lost. There is no DELETE path; `archived_at`
        // retires an account while keeping it readable forever.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_accounts (
                owner       UUID PRIMARY KEY,
                key         TEXT NOT NULL UNIQUE,
                label       TEXT NOT NULL,
                kind        TEXT NOT NULL,
                metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                archived_at TIMESTAMPTZ
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_ledger_accounts_kind
            ON ledger_accounts(kind, created_at DESC)
            "#,
        )
        .execute(&mut *tx)
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

        // ValueObjects table, partitioned by asset_code so each asset
        // gets its own physical partition with independent vacuum and
        // statistics. The asset UUID is kept alongside the code for
        // referential integrity (FK to ledger_assets); the code is
        // denormalised onto each row so PG can route writes.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ledger_value_objects (
                id           UUID NOT NULL,
                asset        UUID NOT NULL REFERENCES ledger_assets(id),
                asset_code   TEXT NOT NULL,
                owner        UUID NOT NULL,
                amount       BIGINT NOT NULL CHECK (amount > 0),
                state        TEXT NOT NULL CHECK (state IN ('alive', 'reserved', 'burned')),
                reserved_for UUID,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (asset_code, id)
            ) PARTITION BY LIST (asset_code)
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // Catch-all partition so writes succeed even before the caller
        // has registered any specific asset code via this argument.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ledger_value_objects_default \
             PARTITION OF ledger_value_objects DEFAULT",
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        for code in &assets {
            create_vo_partition(&mut tx, code).await?;
        }
        drop_orphaned_vo_partitions(&mut tx, &assets).await?;

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

/// `USD` partitions as `ledger_value_objects_usd`. The `-` → `_` step only
/// matters for codes registered before codes were restricted to letters, so
/// their existing partitions are still recognised. Mirrors
/// `partition_name_segment` in the ousia postgres adapter.
fn ledger_partition_segment(code: &str) -> String {
    code.to_lowercase().replace('-', "_")
}

fn storage_err(e: sqlx::Error) -> MoneyError {
    MoneyError::Storage(e.to_string())
}

/// Create the partition for `code` (normalised: upper-case letters). Rows for that code
/// may already sit in `_default` (minted before the asset was listed);
/// Postgres refuses to add the partition while they're there, so move them.
async fn create_vo_partition(conn: &mut sqlx::PgConnection, code: &str) -> Result<(), MoneyError> {
    let table = format!("ledger_value_objects_{}", ledger_partition_segment(code));
    // (table exists, its partition bound if it is a partition of ledger_value_objects)
    let (exists, bound): (bool, Option<String>) = sqlx::query_as(
        "SELECT to_regclass($1) IS NOT NULL, \
                (SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c \
                 JOIN pg_inherits i ON i.inhrelid = c.oid \
                 WHERE c.oid = to_regclass($1) \
                   AND i.inhparent = 'ledger_value_objects'::regclass)",
    )
    .bind(&table)
    .fetch_one(&mut *conn)
    .await
    .map_err(storage_err)?;
    if exists {
        // A same-named partition may hold a different code, e.g. a `usd` partition
        // created before codes were normalised to upper case; rows for this code
        // would then silently go to `_default`. Codes are letters only, so the
        // quoted literal is unambiguous.
        return match bound {
            Some(b) if b.contains(&format!("'{code}'")) => Ok(()),
            Some(b) => Err(MoneyError::InvalidAssetCode(format!(
                "{code:?} maps to partition {table}, which already holds {b}"
            ))),
            None => Err(MoneyError::InvalidAssetCode(format!(
                "{code:?} maps to {table}, which exists but is not a ledger_value_objects partition"
            ))),
        };
    }

    // Blocks concurrent writes to `_default` until commit, so no new row
    // for this code can land there between the move and the CREATE.
    sqlx::query("LOCK TABLE ledger_value_objects_default IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *conn)
        .await
        .map_err(storage_err)?;
    sqlx::query(
        "CREATE TEMP TABLE IF NOT EXISTS ledger_vo_move \
         (LIKE ledger_value_objects) ON COMMIT DROP",
    )
    .execute(&mut *conn)
    .await
    .map_err(storage_err)?;
    sqlx::query(
        "WITH moved AS (DELETE FROM ledger_value_objects_default WHERE asset_code = $1 RETURNING *) \
         INSERT INTO ledger_vo_move SELECT * FROM moved",
    )
    .bind(code)
    .execute(&mut *conn)
    .await
    .map_err(storage_err)?;
    sqlx::query(&format!(
        "CREATE TABLE {table} PARTITION OF ledger_value_objects FOR VALUES IN ('{code}')"
    ))
    .execute(&mut *conn)
    .await
    .map_err(storage_err)?;
    for sql in [
        "INSERT INTO ledger_value_objects SELECT * FROM ledger_vo_move",
        "TRUNCATE ledger_vo_move",
    ] {
        sqlx::query(sql).execute(&mut *conn).await.map_err(storage_err)?;
    }
    Ok(())
}

/// Drop per-asset partitions that are empty and belong to neither a listed
/// code nor a registered asset. `init_schema` calls this with an empty list on
/// every start, so registered assets must keep their partitions regardless.
async fn drop_orphaned_vo_partitions(
    conn: &mut sqlx::PgConnection,
    listed: &[String],
) -> Result<(), MoneyError> {
    let registered: Vec<String> = sqlx::query_scalar("SELECT code FROM ledger_assets")
        .fetch_all(&mut *conn)
        .await
        .map_err(storage_err)?;
    let keep: std::collections::HashSet<String> = listed
        .iter()
        .chain(registered.iter())
        .map(|c| format!("ledger_value_objects_{}", ledger_partition_segment(c)))
        .chain(std::iter::once("ledger_value_objects_default".to_string()))
        .collect();

    let partitions: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'ledger_value_objects'::regclass",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(storage_err)?;

    for partition in partitions.into_iter().filter(|p| !keep.contains(p)) {
        let quoted = format!("\"{}\"", quote_ident(&partition));
        // Lock before the emptiness check: an uncommitted write into this
        // partition finishes first and is then seen, instead of being dropped.
        sqlx::query(&format!("LOCK TABLE {quoted} IN ACCESS EXCLUSIVE MODE"))
            .execute(&mut *conn)
            .await
            .map_err(storage_err)?;
        let has_rows: bool = sqlx::query_scalar(&format!("SELECT EXISTS (SELECT 1 FROM {quoted})"))
            .fetch_one(&mut *conn)
            .await
            .map_err(storage_err)?;
        if has_rows {
            eprintln!("[ledger warn] partition {partition} has no registered asset but still holds rows; keeping it");
            continue;
        }
        sqlx::query(&format!("DROP TABLE {quoted}"))
            .execute(&mut *conn)
            .await
            .map_err(storage_err)?;
    }
    Ok(())
}

/// Normalised, de-duplicated codes. Normalisation also makes them safe to
/// splice into partition DDL: letters only, within the identifier limit.
fn normalize_asset_codes(assets: &[&str]) -> Result<Vec<String>, MoneyError> {
    let mut out: Vec<String> = Vec::new();
    for code in assets {
        let code = crate::asset::normalize_asset_code(code)?;
        if !out.contains(&code) {
            out.push(code);
        }
    }
    Ok(out)
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
                INSERT INTO ledger_value_objects (id, asset, asset_code, owner, amount, state, reserved_for, created_at)
                VALUES ($1, $2, $3, $4, $5, 'alive', NULL, NOW())
                "#,
            )
            .bind(fragment.id)
            .bind(fragment.asset)
            .bind(&asset.code)
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
                INSERT INTO ledger_value_objects (id, asset, asset_code, owner, amount, state, reserved_for, created_at)
                VALUES ($1, $2, $3, $4, $5, 'reserved', $6, NOW())
                "#,
            )
            .bind(fragment.id)
            .bind(fragment.asset)
            .bind(&asset.code)
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
        // Insert the transaction FIRST. `ledger_transaction_idempotency_keys.transaction_id`
        // is a NOT DEFERRABLE FK into this table, so the referenced row must already
        // exist in this same DB transaction before the idempotency key can reference
        // it — inserting the key first (as this used to) makes every idempotent
        // mint/burn violate the FK unconditionally. Returning an error below still
        // rolls back this whole transaction (including this insert), so ordering it
        // this way costs nothing on the duplicate-key path.
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
                // Someone already holds this key — look up the transaction that
                // actually owns it so the error points somewhere real, rather than
                // at this transaction's id, which is about to be rolled back.
                let existing = sqlx::query(
                    r#"
                    SELECT transaction_id
                    FROM ledger_transaction_idempotency_keys
                    WHERE key = $1
                    "#,
                )
                .bind(&hash)
                .fetch_one(&mut **tx)
                .await
                .map_err(|e| MoneyError::Storage(e.to_string()))?;

                let existing_id: Uuid = existing
                    .try_get("transaction_id")
                    .map_err(|e| MoneyError::Storage(e.to_string()))?;

                return Err(MoneyError::DuplicateIdempotencyKey(existing_id));
            }
        }

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

        // ── Phase 0: Apply mints ───────────────────────────────────────────────
        // Mints create new value; they don't compete for existing VOs. Running
        // them first means Phase 1's FOR UPDATE sees them as ordinary alive
        // rows, so a reserve/transfer in the same atomic block can be funded
        // by an in-plan mint without any special-case Phase 3 accounting.
        for op in plan.operations() {
            if let Operation::Mint {
                asset_id,
                owner,
                amount,
                ..
            } = op
            {
                self.mint_internal_tx(&mut tx, *asset_id, *owner, *amount)
                    .await?;
            }
        }

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
        // Mints already applied in Phase 0.
        let mut used: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        for op in plan.operations() {
            match op {
                Operation::Mint { .. } => {}
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
            LEFT JOIN ledger_assets a ON lt.asset = a.id
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
        let code = crate::asset::normalize_asset_code(code)?;
        let row = sqlx::query(
            r#"
            SELECT id, code, unit, decimals
            FROM ledger_assets
            WHERE code = $1
            "#,
        )
        .bind(&code)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?
        .ok_or_else(|| MoneyError::AssetNotFound(code.clone()))?;

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
        let code = crate::asset::normalize_asset_code(&asset.code)?;
        sqlx::query(
            r#"
            INSERT INTO ledger_assets (id, code, unit, decimals, created_at)
            VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (code) DO UPDATE SET unit = $3, decimals = $4
            "#,
        )
        .bind(asset.id)
        .bind(code)
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

    // ------------------------------------------------------------------ //
    //  Account registry                                                   //
    // ------------------------------------------------------------------ //

    async fn register_account(&self, account: &Account) -> Result<Account, MoneyError> {
        // Upsert on `owner`. `created_at` is taken from the existing row on
        // conflict so re-registering on every boot doesn't keep resetting
        // an account's age. The `key` UNIQUE constraint does the rest: if
        // this key already belongs to a *different* owner the insert fails,
        // which is the intended outcome — one key, one account, forever.
        let row = sqlx::query(
            r#"
            INSERT INTO ledger_accounts (owner, key, label, kind, metadata, created_at, updated_at, archived_at)
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NULL)
            ON CONFLICT (owner) DO UPDATE SET
                key        = EXCLUDED.key,
                label      = EXCLUDED.label,
                kind       = EXCLUDED.kind,
                metadata   = EXCLUDED.metadata,
                updated_at = NOW()
            RETURNING owner, key, label, kind, metadata, created_at, updated_at, archived_at
            "#,
        )
        .bind(account.owner)
        .bind(&account.key)
        .bind(&account.label)
        .bind(&account.kind)
        .bind(&account.metadata)
        .fetch_one(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        account_from_row(&row)
    }

    async fn get_account(&self, owner: Uuid) -> Result<Option<Account>, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT owner, key, label, kind, metadata, created_at, updated_at, archived_at
            FROM ledger_accounts WHERE owner = $1
            "#,
        )
        .bind(owner)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        row.as_ref().map(account_from_row).transpose()
    }

    async fn get_account_by_key(&self, key: &str) -> Result<Option<Account>, MoneyError> {
        let row = sqlx::query(
            r#"
            SELECT owner, key, label, kind, metadata, created_at, updated_at, archived_at
            FROM ledger_accounts WHERE key = $1
            "#,
        )
        .bind(key)
        .fetch_optional(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        row.as_ref().map(account_from_row).transpose()
    }

    async fn list_accounts(&self, query: &AccountQuery) -> Result<Vec<Account>, MoneyError> {
        // `$1 IS NULL OR kind = $1` keeps this one prepared statement
        // instead of concatenating a WHERE clause per filter combination.
        let rows = sqlx::query(
            r#"
            SELECT owner, key, label, kind, metadata, created_at, updated_at, archived_at
            FROM ledger_accounts
            WHERE ($1::TEXT IS NULL OR kind = $1)
              AND ($2::BOOL OR archived_at IS NULL)
            ORDER BY created_at DESC, owner
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(query.kind.as_deref())
        .bind(query.include_archived)
        .bind(query.effective_limit())
        .bind(query.effective_offset())
        .fetch_all(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        rows.iter().map(account_from_row).collect()
    }

    async fn list_account_balances(
        &self,
        asset_id: Uuid,
        query: &AccountQuery,
    ) -> Result<Vec<AccountBalance>, MoneyError> {
        // LEFT JOIN, not JOIN: a registered account with no value objects
        // yet is a real account with a zero balance, and dropping it from
        // an internal-accounts listing would be the wrong answer.
        let rows = sqlx::query(
            r#"
            SELECT
                a.owner, a.key, a.label, a.kind, a.metadata,
                a.created_at, a.updated_at, a.archived_at,
                COALESCE(SUM(vo.amount) FILTER (WHERE vo.state = 'alive'), 0)::BIGINT    AS alive_sum,
                COALESCE(SUM(vo.amount) FILTER (WHERE vo.state = 'reserved'), 0)::BIGINT AS reserved_sum
            FROM ledger_accounts a
            LEFT JOIN ledger_value_objects vo
                   ON vo.owner = a.owner AND vo.asset = $1
            WHERE ($2::TEXT IS NULL OR a.kind = $2)
              AND ($3::BOOL OR a.archived_at IS NULL)
            GROUP BY a.owner, a.key, a.label, a.kind, a.metadata,
                     a.created_at, a.updated_at, a.archived_at
            ORDER BY a.created_at DESC, a.owner
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(asset_id)
        .bind(query.kind.as_deref())
        .bind(query.include_archived)
        .bind(query.effective_limit())
        .bind(query.effective_offset())
        .fetch_all(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let account = account_from_row(row)?;
            let alive = row
                .try_get::<i64, _>("alive_sum")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let reserved = row
                .try_get::<i64, _>("reserved_sum")
                .map_err(|e| MoneyError::Storage(e.to_string()))? as u64;
            let balance = Balance::from_value_objects(account.owner, asset_id, alive, reserved);
            out.push(AccountBalance { account, balance });
        }

        Ok(out)
    }

    async fn archive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        // `WHERE archived_at IS NULL` makes a repeat call a no-op rather
        // than moving the retirement date forward.
        sqlx::query(
            "UPDATE ledger_accounts SET archived_at = NOW(), updated_at = NOW() \
             WHERE owner = $1 AND archived_at IS NULL",
        )
        .bind(owner)
        .execute(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn unarchive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        sqlx::query(
            "UPDATE ledger_accounts SET archived_at = NULL, updated_at = NOW() \
             WHERE owner = $1 AND archived_at IS NOT NULL",
        )
        .bind(owner)
        .execute(&self.get_pool())
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;
        Ok(())
    }
}

fn account_from_row(row: &sqlx::postgres::PgRow) -> Result<Account, MoneyError> {
    Ok(Account {
        owner: row
            .try_get("owner")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        key: row
            .try_get("key")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        label: row
            .try_get("label")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        kind: row
            .try_get("kind")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        metadata: row
            .try_get("metadata")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        created_at: row
            .try_get("created_at")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
        archived_at: row
            .try_get("archived_at")
            .map_err(|e| MoneyError::Storage(e.to_string()))?,
    })
}

// ---------------------------------------------------------------------- //
//  Schema resolution                                                      //
// ---------------------------------------------------------------------- //

/// Escape a Postgres identifier's embedded double quotes. The caller wraps
/// the result in quotes itself.
fn quote_ident(raw: &str) -> String {
    raw.replace('"', "\"\"")
}

/// The schema this deployment's ledger tables belong in.
///
/// Read from the live connection's `search_path` rather than from an
/// argument, so it is configured in the same place as the rest of the
/// connection — the database URL. With libpq that is the `options`
/// parameter:
///
/// ```text
/// postgres://user:pw@host/db?options=-csearch_path%3Dmealgro
/// ```
///
/// The first entry wins; `"$user"` is skipped (a per-role default, not a
/// deployment's choice); an empty or unset path falls back to `public`, so
/// a deployment that configures nothing keeps its tables exactly where
/// they already are.
///
/// `current_schema()` is deliberately not used — it resolves to the first
/// schema that already **exists**, which on a first boot is never the one
/// about to be created.
async fn resolve_target_schema(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<String, MoneyError> {
    let raw: String = sqlx::query_scalar("SHOW search_path")
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| MoneyError::Storage(e.to_string()))?;

    Ok(first_search_path_entry(&raw).unwrap_or_else(|| "public".to_string()))
}

/// First usable schema name in a `SHOW search_path` result.
///
/// Postgres renders the path as a comma-separated list whose entries may
/// be quoted (`"my schema"`, with `""` as an embedded quote). `$user` is
/// skipped. `None` means nothing usable, which the caller reads as
/// `public`.
fn first_search_path_entry(raw: &str) -> Option<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            match entry
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
            {
                Some(inner) => inner.replace("\"\"", "\""),
                None => entry.to_string(),
            }
        })
        .find(|entry| !entry.is_empty() && entry != "$user")
}

#[cfg(test)]
mod schema_resolution_tests {
    use super::first_search_path_entry;

    #[test]
    fn unset_path_falls_back_to_public_at_the_call_site() {
        assert_eq!(first_search_path_entry(""), None);
        assert_eq!(first_search_path_entry("\"$user\""), None);
    }

    #[test]
    fn skips_the_per_role_placeholder() {
        assert_eq!(
            first_search_path_entry("\"$user\", public").as_deref(),
            Some("public")
        );
    }

    #[test]
    fn takes_the_first_real_entry() {
        assert_eq!(
            first_search_path_entry("mealgro, public").as_deref(),
            Some("mealgro")
        );
        assert_eq!(
            first_search_path_entry("  tenant_a ,public").as_deref(),
            Some("tenant_a")
        );
    }

    #[test]
    fn unquotes_and_unescapes() {
        assert_eq!(
            first_search_path_entry("\"my schema\", public").as_deref(),
            Some("my schema")
        );
        assert_eq!(
            first_search_path_entry("\"odd\"\"name\"").as_deref(),
            Some("odd\"name")
        );
    }
}

#[cfg(test)]
mod asset_code_tests {
    use super::normalize_asset_codes;

    #[test]
    fn dedupes_case_insensitively() {
        assert_eq!(normalize_asset_codes(&["usd", "USD", "Ngn"]).unwrap(), ["USD", "NGN"]);
    }

    #[test]
    fn rejects_non_letters() {
        for bad in ["", "MG-POINT", "U-S-D", "usdc_e", "X1", "US D", "USD'); DROP TABLE objects; --"] {
            assert!(normalize_asset_codes(&["USD", bad]).is_err(), "{bad:?} should be rejected");
        }
    }
}
