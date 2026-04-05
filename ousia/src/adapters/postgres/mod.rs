mod adapter_impl;
mod helper;
mod traversal_impl;
mod unique_impl;

#[cfg(feature = "ledger")]
mod ledger_impl;

use sqlx::{PgPool, Postgres, Transaction};

use crate::adapters::Error;

/// PostgreSQL adapter using a partitioned JSON storage model.
///
/// # Schema overview
///
/// All three main tables (`objects`, `edges`, `unique_constraints`) use
/// `PARTITION BY LIST (type)` so each registered type gets its own physical
/// table with independent vacuum, statistics, and index sizing.
///
/// Call [`PostgresAdapter::init_schema_typed`] at application startup with
/// the full list of object and edge type names.  For tests or minimal setups
/// call [`PostgresAdapter::init_schema`] which is equivalent to
/// `init_schema_typed(&[], &[])` — only the `_default` catch-all partitions
/// are created.
pub struct PostgresAdapter {
    pub(crate) pool: PgPool,
}

impl PostgresAdapter {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    // ------------------------------------------------------------------ //
    //  Public schema helpers                                               //
    // ------------------------------------------------------------------ //

    /// Initialise the schema with no pre-declared type partitions.
    ///
    /// Equivalent to `init_schema_typed(&[], &[])`.  All rows land in the
    /// `_default` catch-all partitions until specific partitions are created
    /// via [`Self::init_schema_typed`].
    pub async fn init_schema(&self) -> Result<(), Error> {
        self.init_schema_typed(&[], &[]).await
    }

    /// Initialise (or upgrade) the production schema.
    ///
    /// Safe to call on every application start — all operations use
    /// `IF NOT EXISTS` and are run inside a single transaction.
    ///
    /// ## What it does
    /// 1. Creates `sequence_registry` and `ousia_meta` bookkeeping tables.
    /// 2. Creates partitioned `objects`, `edges`, `unique_constraints` tables
    ///    (parent + indexes + storage tuning).
    /// 3. Creates `_default` catch-all partitions for all three tables.
    /// 4. Creates a dedicated partition for every type in `obj_types` /
    ///    `edge_types`.
    /// 5. Drops orphaned empty partitions that are no longer in the registry
    ///    (non-empty orphans are logged as warnings and skipped).
    /// 6. Drops redundant thin indexes left over from the non-partitioned era.
    ///
    /// ## Partition naming
    /// - Object partitions: `objects_{type_lower}`, `unique_constraints_{type_lower}`
    /// - Edge partitions: `edges_{type_lower}`
    /// - `{type_lower}` is the type name lowercased with `-` → `_`.
    pub async fn init_schema_typed(
        &self,
        obj_types: &[&'static str],
        edge_types: &[&'static str],
    ) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Self::create_bookkeeping_tables(&mut tx).await?;
        Self::create_objects_table(&mut tx).await?;
        Self::create_edges_table(&mut tx).await?;
        Self::create_unique_constraints_table(&mut tx).await?;
        Self::create_default_partitions(&mut tx).await?;
        Self::create_type_partitions(&mut tx, obj_types, edge_types).await?;
        Self::drop_old_thin_indexes(&mut tx).await?;
        Self::drop_orphaned_partitions(&mut tx, "objects", obj_types).await?;
        Self::drop_orphaned_partitions(&mut tx, "edges", edge_types).await?;
        Self::drop_orphaned_uc_partitions(&mut tx, obj_types).await?;

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        #[cfg(feature = "ledger")]
        {
            use ledger::adapters::postgres::PostgresSchemaLedgerAdapter;
            self.init_ledger_schema().await.map_err(|me| match me {
                ledger::MoneyError::Storage(e) => Error::Storage(e),
                _ => Error::Storage(me.to_string()),
            })?;
        }

        Ok(())
    }

    /// Ensure a named DB-native sequence exists and is registered in
    /// `sequence_registry`.  Idempotent — safe to call on every request.
    ///
    /// Sequences are created with `CACHE 1` (default) so values are strictly
    /// sequential.  For high-throughput sequences run
    /// `ALTER SEQUENCE <name> CACHE 100` after creation — see `TUNING.md`.
    pub(crate) async fn ensure_sequence(&self, name: &str) -> Result<(), Error> {
        // Quote the identifier so names containing hyphens or other special
        // characters are accepted by PG (e.g. "my-key" → `"my-key"`).
        let create_sql = format!(
            "CREATE SEQUENCE IF NOT EXISTS {}",
            pg_quote_ident(name)
        );
        sqlx::query(&create_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            "INSERT INTO sequence_registry (id, name) \
             VALUES (gen_random_uuid(), $1) ON CONFLICT (name) DO NOTHING",
        )
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  ousia_meta helpers (used by check_schema in Engine)                //
    // ------------------------------------------------------------------ //

    pub(crate) async fn read_schema_hash_impl(
        &self,
        type_name: &'static str,
    ) -> Result<Option<String>, Error> {
        let key = format!("schema:{}", type_name);
        sqlx::query_scalar::<_, String>("SELECT value FROM ousia_meta WHERE key = $1")
            .bind(&key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub(crate) async fn upsert_schema_hash_impl(
        &self,
        type_name: &'static str,
        hash: &str,
    ) -> Result<(), Error> {
        let key = format!("schema:{}", type_name);
        sqlx::query(
            "INSERT INTO ousia_meta (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&key)
        .bind(hash)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  Private schema builders                                             //
    // ------------------------------------------------------------------ //

    async fn create_bookkeeping_tables(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sequence_registry (
                id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ousia_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_objects_table(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.objects (
                id         UUID        NOT NULL,
                type       TEXT        NOT NULL,
                owner      UUID        NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                data       JSONB       NOT NULL DEFAULT '{}'::jsonb,
                index_meta JSONB       NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (type, id)
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Covering index: owner scoped queries with timestamp sort
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_owner_created
                ON objects(type, owner, created_at DESC)
                INCLUDE (id, updated_at)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_owner_updated
                ON objects(type, owner, updated_at DESC)
                INCLUDE (id, created_at)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Global type-scoped range queries (no owner filter)
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_created
                ON objects(type, created_at DESC)
                INCLUDE (owner, id)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_updated
                ON objects(type, updated_at DESC)
                INCLUDE (owner, id)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // GIN for index_meta field queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_index_meta
                ON objects USING GIN (index_meta jsonb_path_ops)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_edges_table(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.edges (
                "from"     UUID        NOT NULL,
                "to"       UUID        NOT NULL,
                type       TEXT        NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                data       JSONB       NOT NULL DEFAULT '{}'::jsonb,
                index_meta JSONB       NOT NULL DEFAULT '{}'::jsonb
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Unique constraint includes partition key
        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_key
                ON edges("from", "to", type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Covering outgoing-edge index — index-only scan for all common reads
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_from_covering
                ON edges("from", type, created_at DESC)
                INCLUDE ("to", data, index_meta)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Covering incoming-edge index
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_to_covering
                ON edges("to", type, created_at DESC)
                INCLUDE ("from", data, index_meta)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_index_meta
                ON edges USING GIN (index_meta jsonb_path_ops)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_unique_constraints_table(
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.unique_constraints (
                id    UUID NOT NULL,
                type  TEXT NOT NULL,
                key   TEXT NOT NULL,
                field TEXT NOT NULL,
                PRIMARY KEY (type, key)
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Reverse lookup by object id (used for cascade deletes)
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_unique_id
                ON unique_constraints(id)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_default_partitions(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS objects_default PARTITION OF objects DEFAULT",
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query("CREATE TABLE IF NOT EXISTS edges_default PARTITION OF edges DEFAULT")
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS unique_constraints_default \
             PARTITION OF unique_constraints DEFAULT",
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_type_partitions(
        tx: &mut Transaction<'_, Postgres>,
        obj_types: &[&'static str],
        edge_types: &[&'static str],
    ) -> Result<(), Error> {
        for &type_name in obj_types {
            let safe = partition_name_segment(type_name);

            let sql = format!(
                "CREATE TABLE IF NOT EXISTS objects_{safe} \
                 PARTITION OF objects FOR VALUES IN ('{type_name}')"
            );
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;

            let sql = format!(
                "CREATE TABLE IF NOT EXISTS unique_constraints_{safe} \
                 PARTITION OF unique_constraints FOR VALUES IN ('{type_name}')"
            );
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        for &type_name in edge_types {
            let safe = partition_name_segment(type_name);
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS edges_{safe} \
                 PARTITION OF edges FOR VALUES IN ('{type_name}')"
            );
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn drop_old_thin_indexes(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        for name in &[
            "idx_objects_type_owner",
            "idx_objects_type_owner_created",
            "idx_objects_type_owner_updated",
            "idx_edges_from_key",
            "idx_edges_to_key",
            "idx_unique_type_key",
        ] {
            sqlx::query(&format!("DROP INDEX IF EXISTS {}", name))
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    }

    /// Drop empty orphaned partitions of `parent_table`.
    /// `known_types` is the current registry — all other non-default partitions
    /// whose names follow the `{parent}_{safe}` convention are candidates.
    async fn drop_orphaned_partitions(
        tx: &mut Transaction<'_, Postgres>,
        parent_table: &str,
        known_types: &[&'static str],
    ) -> Result<(), Error> {
        let known_names: Vec<String> = known_types
            .iter()
            .map(|t| format!("{}_{}", parent_table, partition_name_segment(t)))
            .chain(std::iter::once(format!("{}_default", parent_table)))
            .collect();

        let partitions: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::text \
             FROM pg_class c \
             JOIN pg_inherits i ON i.inhrelid = c.oid \
             JOIN pg_class p ON p.oid = i.inhparent \
             WHERE p.relname = $1",
        )
        .bind(parent_table)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        for partition in partitions {
            if known_names.contains(&partition) {
                continue;
            }
            let count: i64 =
                sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {} LIMIT 1", partition))
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;

            if count == 0 {
                sqlx::query(&format!("DROP TABLE IF EXISTS {}", partition))
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
            } else {
                eprintln!(
                    "[ousia warn] Orphaned partition '{}' has {} row(s) — skipping drop. \
                     Detach or migrate manually.",
                    partition, count
                );
            }
        }

        Ok(())
    }

    async fn drop_orphaned_uc_partitions(
        tx: &mut Transaction<'_, Postgres>,
        obj_types: &[&'static str],
    ) -> Result<(), Error> {
        Self::drop_orphaned_partitions(tx, "unique_constraints", obj_types).await
    }
}

/// Convert a type name to a safe partition name segment.
/// Lowercases and replaces `-` with `_`.
fn partition_name_segment(type_name: &str) -> String {
    type_name.to_lowercase().replace('-', "_")
}

/// Wrap an identifier in double-quotes, escaping any embedded double-quotes.
/// E.g. `my-key` → `"my-key"`, `weird"name` → `"weird""name"`.
pub(crate) fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Return the identifier in the form expected by `nextval()` / `last_value`:
/// a single-quoted string containing the double-quoted identifier.
/// E.g. `my-key` → `'"my-key"'`.
pub(crate) fn pg_quote_ident_as_literal(name: &str) -> String {
    format!("'{}'", pg_quote_ident(name))
}
