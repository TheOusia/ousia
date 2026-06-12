mod adapter_impl;
mod geo_impl;
mod helper;
mod traversal_impl;
mod unique_impl;

#[cfg(feature = "ledger")]
mod ledger_impl;

use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::adapters::Error;

/// PostgreSQL adapter using a partitioned storage model.
///
/// # Schema overview
///
/// All four core tables (`objects`, `object_edges`, `object_constraints`,
/// `object_geo`) use `PARTITION BY LIST (type)` so each Object / Edge type
/// gets its own physical partition with independent vacuum, statistics,
/// and index sizing.
///
/// [`PostgresAdapter::init_schema`] reads the [`crate::manifest::MANIFEST`]
/// distributed slice (populated by `#[derive(OusiaObject)]` and
/// `#[derive(OusiaEdge)]` at link time) and creates one partition per
/// declared type, plus the per-partition `FOREIGN KEY ... ON DELETE
/// CASCADE` so that deleting a parent object automatically removes its
/// constraints / geo rows / edges. The manifest is also written to
/// `target/ousia.json` for tooling.
///
/// ## Partition naming
/// - `objects_{safe}` / `object_constraints_{safe}` / `object_geo_{safe}`
/// - `object_edges_{safe}` where `{safe}` is the lowercased type with
///   `-` → `_`.
pub struct PostgresAdapter {
    pub(crate) pool: PgPool,
}

impl PostgresAdapter {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    // ------------------------------------------------------------------ //
    //  Public schema entry point                                          //
    // ------------------------------------------------------------------ //

    /// Initialise (or upgrade) the schema.
    ///
    /// Reads `crate::manifest::MANIFEST` for the set of Object / Edge
    /// types linked into the binary, then:
    ///
    /// 1. Enables the `postgis` extension.
    /// 2. Creates bookkeeping tables (`sequences`, `ousia_meta`).
    /// 3. Creates the four partitioned parent tables (`objects`,
    ///    `object_edges`, `object_constraints`, `object_geo`).
    /// 4. For each Object type: creates `objects_<t>`, a `(id)` unique
    ///    index (needed for FK references), and the matching
    ///    `object_constraints_<t>` / `object_geo_<t>` partitions with
    ///    `FK ... ON DELETE CASCADE` back to `objects_<t>(id)`.
    /// 5. For each Edge type: creates `object_edges_<t>` with
    ///    `FK (from) → objects_<from_type>(id)` and
    ///    `FK (to) → objects_<to_type>(id)`, both `ON DELETE CASCADE`.
    /// 6. Drops empty orphaned partitions whose type is no longer in
    ///    the manifest (non-empty orphans are logged and skipped).
    /// 7. Writes the manifest to `target/ousia.json` as a tooling
    ///    artifact (best-effort — write failure is logged, not fatal).
    ///
    /// Safe to call on every application start: all schema operations
    /// use `IF NOT EXISTS` / `IF EXISTS` and run inside a single
    /// transaction.
    ///
    /// Composed schema-hash drift checking lives in
    /// [`crate::Engine::check_schema`] and runs separately after
    /// `init_schema` completes.
    pub async fn init_schema(&self) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Self::create_bookkeeping_tables(&mut tx).await?;
        Self::create_parent_objects(&mut tx).await?;
        Self::create_parent_object_edges(&mut tx).await?;
        Self::create_parent_object_constraints(&mut tx).await?;
        Self::create_parent_object_geo(&mut tx).await?;

        let object_types = crate::manifest::object_types_sorted();
        let edge_entries = crate::manifest::edge_entries_sorted();

        // Objects + their FK-dependent children first, so edges can FK to
        // populated partitions in the second pass.
        for type_name in &object_types {
            Self::create_object_partition(&mut tx, type_name).await?;
            Self::create_constraints_partition(&mut tx, type_name).await?;
            Self::create_geo_partition(&mut tx, type_name).await?;
        }

        for entry in &edge_entries {
            Self::create_edge_partition(
                &mut tx,
                entry.type_name,
                entry.from_type.unwrap_or(""),
                entry.to_type.unwrap_or(""),
            )
            .await?;
        }

        // Drop orphaned (empty) partitions for types removed from the manifest.
        Self::drop_orphaned_partitions(&mut tx, "objects", &object_types).await?;
        Self::drop_orphaned_partitions(&mut tx, "object_constraints", &object_types).await?;
        Self::drop_orphaned_partitions(&mut tx, "object_geo", &object_types).await?;
        let edge_type_names: Vec<&'static str> =
            edge_entries.iter().map(|e| e.type_name).collect();
        Self::drop_orphaned_partitions(&mut tx, "object_edges", &edge_type_names).await?;

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Best-effort: emit target/ousia.json for tooling. Failure here is
        // never fatal — the manifest lives in memory regardless.
        Self::emit_manifest_json();

        // Composed schema-hash drift check. Runs after the schema is
        // committed so a major-version mismatch surfaces as a hard error
        // and aborts startup; minor drift logs a warning and updates the
        // stored hash. See [`check_composed_schema_hash`] for semantics.
        self.check_composed_schema_hash().await?;

        // Auto-initialise the ledger bookkeeping tables when the `ledger`
        // feature is on, with no asset partitions (writes will land in
        // the `_default` partition). Call
        // `init_ledger_schema(&["USD", ...])` separately to register
        // per-asset partitions.
        #[cfg(feature = "ledger")]
        {
            use ledger::adapters::postgres::PostgresSchemaLedgerAdapter;
            self.init_ledger_schema(&[])
                .await
                .map_err(|me| Error::Storage(me.to_string()))?;
        }

        Ok(())
    }

    /// Verify the composed schema hash matches what's stored in
    /// `ousia_meta`. Semantics:
    ///
    /// - **First call** (no stored entry): record the current
    ///   `major.minor:hash` and return.
    /// - **Exact match**: no-op.
    /// - **Same major + minor + hash drift OR minor bump**: warn via
    ///   `eprintln!` and overwrite the stored value. Additive schema
    ///   changes land here.
    /// - **Major bump**: return [`Error::SchemaMigrationRequired`]
    ///   without touching the stored value. The operator reconciles
    ///   and resets the entry manually.
    async fn check_composed_schema_hash(&self) -> Result<(), Error> {
        let current_hash = self.compute_postgres_schema_hash().await?;
        let current_prefix = schema_version_prefix();
        let current_entry = format!("{}:{}", current_prefix, current_hash);

        let stored = self.read_schema_hash_impl("composed").await?;

        match stored {
            None => {
                self.upsert_schema_hash_impl("composed", &current_entry).await?;
            }
            Some(ref s) if s == &current_entry => {}
            Some(stored_entry) => {
                let (stored_major, stored_hash) = parse_schema_entry(&stored_entry);
                let current_major = current_prefix.split('.').next().unwrap_or("0");

                if stored_major != current_major {
                    return Err(Error::SchemaMigrationRequired(format!(
                        "Postgres schema hash major-version mismatch: stored '{}', \
                         current '{}'. The on-disk structure differs across a major \
                         release — reconcile manually, then reset ousia_meta \
                         WHERE key = 'schema:composed'.",
                        stored_major, current_major
                    )));
                }

                eprintln!(
                    "[ousia warn] Postgres schema drift within major v{} \
                     (stored {}…, current {}…). The on-disk structure changed since \
                     the last successful init_schema (added/dropped indexes, \
                     altered columns, new partitions, …). Updating stored entry.",
                    current_major,
                    &stored_hash[..stored_hash.len().min(12)],
                    &current_hash[..current_hash.len().min(12)],
                );
                self.upsert_schema_hash_impl("composed", &current_entry).await?;
            }
        }
        Ok(())
    }

    /// Hash the *actual* on-disk shape of the ousia-owned tables —
    /// columns + types + nullability + defaults, indexes, constraints,
    /// and partition keys. Per-type partitions of `objects` /
    /// `object_edges` / … and per-asset partitions of
    /// `ledger_value_objects` are excluded: they're derivatives of the
    /// partition key on the parent, so the parent shape captures the
    /// structural invariant.
    ///
    /// This is what `check_composed_schema_hash` compares against
    /// `ousia_meta.schema:composed`. The hash answers a single
    /// question: "is what Postgres reports today the same on-disk
    /// shape as the last time we recorded it?" A column flip
    /// (`JSONB → BYTEA`), an added index, a renamed FK, or a new
    /// constraint will all flip the hash.
    pub async fn compute_postgres_schema_hash(&self) -> Result<String, Error> {
        // Parent partitioned tables + bookkeeping. Per-type / per-asset
        // child partitions are derivatives of the parent's `PARTITION BY`
        // declaration, so we don't repeat them here.
        const OWNED_TABLES: &[&str] = &[
            "objects",
            "object_edges",
            "object_constraints",
            "object_geo",
            "ousia_meta",
            "sequences",
            #[cfg(feature = "ledger")]
            "ledger_assets",
            #[cfg(feature = "ledger")]
            "ledger_value_objects",
            #[cfg(feature = "ledger")]
            "ledger_transactions",
            #[cfg(feature = "ledger")]
            "ledger_transaction_idempotency_keys",
        ];

        let mut canonical = String::new();
        for table in OWNED_TABLES {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS ( \
                    SELECT 1 FROM pg_class c \
                    JOIN pg_namespace n ON n.oid = c.relnamespace \
                    WHERE n.nspname = 'public' AND c.relname = $1 \
                 )",
            )
            .bind(table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            if !exists {
                continue;
            }

            canonical.push_str("T:");
            canonical.push_str(table);
            canonical.push('\n');

            // Columns — sorted by name so attribute reordering at the
            // catalog level doesn't perturb the hash on its own. The
            // actual on-disk type is what we care about.
            let col_rows = sqlx::query(
                "SELECT a.attname::text, \
                        format_type(a.atttypid, a.atttypmod) AS typ, \
                        a.attnotnull, \
                        a.atthasdef \
                 FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'public' AND c.relname = $1 \
                   AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attname",
            )
            .bind(table)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            for row in col_rows {
                let name: String = row.try_get(0).map_err(|e| Error::Storage(e.to_string()))?;
                let typ: String = row.try_get(1).map_err(|e| Error::Storage(e.to_string()))?;
                let notnull: bool = row.try_get(2).map_err(|e| Error::Storage(e.to_string()))?;
                let hasdef: bool = row.try_get(3).map_err(|e| Error::Storage(e.to_string()))?;
                canonical.push_str(&format!(
                    "  C:{}:{}:nn={}:hd={}\n",
                    name, typ, notnull, hasdef
                ));
            }

            // Indexes via the standard `pg_indexes` view. `indexdef`
            // is PG's own canonical CREATE INDEX text — stable within
            // a Postgres major version. Sorted by name.
            let idx_rows = sqlx::query(
                "SELECT indexname::text, indexdef::text \
                 FROM pg_indexes \
                 WHERE schemaname = 'public' AND tablename = $1 \
                 ORDER BY indexname",
            )
            .bind(table)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            for row in idx_rows {
                let name: String = row.try_get(0).map_err(|e| Error::Storage(e.to_string()))?;
                let def: String = row.try_get(1).map_err(|e| Error::Storage(e.to_string()))?;
                canonical.push_str(&format!("  I:{}:{}\n", name, def));
            }

            // Constraints — PK, FK, CHECK, UNIQUE.
            // `pg_get_constraintdef(oid, true)` produces the canonical
            // clause body (`PRIMARY KEY (...)`, `FOREIGN KEY (...) REFERENCES
            // ... ON DELETE CASCADE`, ...). Sorted by name.
            let con_rows = sqlx::query(
                "SELECT con.conname::text, \
                        pg_get_constraintdef(con.oid, true) AS def \
                 FROM pg_constraint con \
                 JOIN pg_class t ON t.oid = con.conrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 WHERE n.nspname = 'public' AND t.relname = $1 \
                 ORDER BY con.conname",
            )
            .bind(table)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            for row in con_rows {
                let name: String = row.try_get(0).map_err(|e| Error::Storage(e.to_string()))?;
                let def: String = row.try_get(1).map_err(|e| Error::Storage(e.to_string()))?;
                canonical.push_str(&format!("  K:{}:{}\n", name, def));
            }

            // Partition key — `PARTITION BY LIST (type)` etc. NULL for
            // non-partitioned tables; we skip the line in that case.
            let part_row = sqlx::query(
                "SELECT pg_get_partkeydef(c.oid) AS def \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'public' AND c.relname = $1",
            )
            .bind(table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
            let part_def: Option<String> = part_row
                .try_get(0)
                .map_err(|e| Error::Storage(e.to_string()))?;
            if let Some(def) = part_def {
                canonical.push_str(&format!("  P:{}\n", def));
            }
        }

        Ok(blake3::hash(canonical.as_bytes()).to_hex().to_string())
    }

    // ------------------------------------------------------------------ //
    //  ousia_meta helpers (used by composed schema hash in Engine)        //
    // ------------------------------------------------------------------ //

    pub(crate) async fn read_schema_hash_impl(
        &self,
        key_suffix: &str,
    ) -> Result<Option<String>, Error> {
        let key = format!("schema:{}", key_suffix);
        sqlx::query_scalar::<_, String>("SELECT value FROM ousia_meta WHERE key = $1")
            .bind(&key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    pub(crate) async fn upsert_schema_hash_impl(
        &self,
        key_suffix: &str,
        value: &str,
    ) -> Result<(), Error> {
        let key = format!("schema:{}", key_suffix);
        sqlx::query(
            "INSERT INTO ousia_meta (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    // ------------------------------------------------------------------ //
    //  Private schema builders                                            //
    // ------------------------------------------------------------------ //

    async fn create_bookkeeping_tables(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sequences (
                name  TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 1
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

    async fn create_parent_objects(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        // PK `(type, id)` includes the partition key so PG accepts it on a
        // LIST-partitioned parent. Each per-type partition additionally
        // gets a unique index on `id` alone so FKs from edges/constraints/
        // geo can reference it.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.objects (
                id         UUID        NOT NULL,
                type       TEXT        NOT NULL,
                owner      UUID        NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                data       BYTEA       NOT NULL DEFAULT ''::bytea,
                index_meta JSONB       NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (type, id)
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        // Covering indexes on the parent — PG cascades these into every
        // present and future partition automatically.
        for sql in [
            r#"CREATE INDEX IF NOT EXISTS idx_objects_owner_created
                ON public.objects(type, owner, created_at DESC)
                INCLUDE (id, updated_at)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_objects_owner_updated
                ON public.objects(type, owner, updated_at DESC)
                INCLUDE (id, created_at)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_objects_type_created
                ON public.objects(type, created_at DESC)
                INCLUDE (owner, id)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_objects_type_updated
                ON public.objects(type, updated_at DESC)
                INCLUDE (owner, id)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_objects_index_meta
                ON public.objects USING GIN (index_meta jsonb_path_ops)"#,
        ] {
            sqlx::query(sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn create_parent_object_edges(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.object_edges (
                "from"     UUID        NOT NULL,
                "to"       UUID        NOT NULL,
                type       TEXT        NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                data       BYTEA       NOT NULL DEFAULT ''::bytea,
                index_meta JSONB       NOT NULL DEFAULT '{}'::jsonb,
                PRIMARY KEY (type, "from", "to")
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        for sql in [
            r#"CREATE INDEX IF NOT EXISTS idx_object_edges_from_covering
                ON public.object_edges("from", type, created_at DESC)
                INCLUDE ("to", data, index_meta)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_object_edges_to_covering
                ON public.object_edges("to", type, created_at DESC)
                INCLUDE ("from", data, index_meta)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_object_edges_index_meta
                ON public.object_edges USING GIN (index_meta jsonb_path_ops)"#,
        ] {
            sqlx::query(sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        Ok(())
    }

    async fn create_parent_object_constraints(
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.object_constraints (
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

        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS idx_object_constraints_id
                ON public.object_constraints(id)"#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    async fn create_parent_object_geo(tx: &mut Transaction<'_, Postgres>) -> Result<(), Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.object_geo (
                object_id UUID NOT NULL,
                type      TEXT NOT NULL,
                field     TEXT NOT NULL,
                location  geography(Point, 4326) NOT NULL,
                hash      TEXT NOT NULL,
                PRIMARY KEY (type, object_id, field)
            ) PARTITION BY LIST (type)
            "#,
        )
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        for sql in [
            r#"CREATE INDEX IF NOT EXISTS idx_object_geo_gist
                ON public.object_geo USING GIST (location)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_object_geo_type_field
                ON public.object_geo (type, field)"#,
        ] {
            sqlx::query(sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }

        Ok(())
    }

    /// Create `objects_<safe>` partition and add a unique index on `id`
    /// alone so child tables can `FOREIGN KEY (...) REFERENCES
    /// objects_<safe>(id)`. (The parent PK is `(type, id)`; the per-type
    /// partition pins `type` to a single value, so `id` alone is unique
    /// within the partition.)
    async fn create_object_partition(
        tx: &mut Transaction<'_, Postgres>,
        type_name: &str,
    ) -> Result<(), Error> {
        let safe = partition_name_segment(type_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS objects_{safe} \
             PARTITION OF public.objects FOR VALUES IN ('{type_name}')"
        );
        sqlx::query(&sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let idx_sql = format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS objects_{safe}_id_unq ON objects_{safe}(id)"
        );
        sqlx::query(&idx_sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    /// Create `object_constraints_<safe>` partition with FK cascade to
    /// `objects_<safe>(id)`. FK is added via a separate `ALTER TABLE
    /// ADD CONSTRAINT IF NOT EXISTS`-equivalent (PG has no such syntax,
    /// so we check `pg_constraint` first).
    async fn create_constraints_partition(
        tx: &mut Transaction<'_, Postgres>,
        type_name: &str,
    ) -> Result<(), Error> {
        let safe = partition_name_segment(type_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS object_constraints_{safe} \
             PARTITION OF public.object_constraints FOR VALUES IN ('{type_name}')"
        );
        sqlx::query(&sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let fk_name = format!("object_constraints_{safe}_id_fk");
        add_fk_if_missing(
            tx,
            &format!("object_constraints_{safe}"),
            &fk_name,
            &format!(
                "FOREIGN KEY (id) REFERENCES objects_{safe}(id) ON DELETE CASCADE"
            ),
        )
        .await?;

        Ok(())
    }

    async fn create_geo_partition(
        tx: &mut Transaction<'_, Postgres>,
        type_name: &str,
    ) -> Result<(), Error> {
        let safe = partition_name_segment(type_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS object_geo_{safe} \
             PARTITION OF public.object_geo FOR VALUES IN ('{type_name}')"
        );
        sqlx::query(&sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let fk_name = format!("object_geo_{safe}_object_id_fk");
        add_fk_if_missing(
            tx,
            &format!("object_geo_{safe}"),
            &fk_name,
            &format!(
                "FOREIGN KEY (object_id) REFERENCES objects_{safe}(id) ON DELETE CASCADE"
            ),
        )
        .await?;

        Ok(())
    }

    /// Create `object_edges_<safe>` partition. If `from_type` and
    /// `to_type` have matching object partitions, wire FK cascades on
    /// both `from` and `to`. When either side is missing (e.g. an Edge
    /// whose endpoint Object isn't in the manifest yet), the FK is
    /// skipped — partition creation must still succeed so the rest of
    /// init_schema makes progress; a follow-up `init_schema` will add
    /// the FK once the endpoint Object is registered.
    async fn create_edge_partition(
        tx: &mut Transaction<'_, Postgres>,
        type_name: &str,
        from_type: &str,
        to_type: &str,
    ) -> Result<(), Error> {
        let safe = partition_name_segment(type_name);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS object_edges_{safe} \
             PARTITION OF public.object_edges FOR VALUES IN ('{type_name}')"
        );
        sqlx::query(&sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        if !from_type.is_empty() && object_partition_exists(tx, from_type).await? {
            let from_safe = partition_name_segment(from_type);
            let fk_name = format!("object_edges_{safe}_from_fk");
            add_fk_if_missing(
                tx,
                &format!("object_edges_{safe}"),
                &fk_name,
                &format!(
                    "FOREIGN KEY (\"from\") REFERENCES objects_{from_safe}(id) ON DELETE CASCADE"
                ),
            )
            .await?;
        } else if !from_type.is_empty() {
            eprintln!(
                "[ousia warn] Edge '{}' declares from = {} but no objects_{} partition exists; \
                 FK skipped this init_schema",
                type_name,
                from_type,
                partition_name_segment(from_type)
            );
        }

        if !to_type.is_empty() && object_partition_exists(tx, to_type).await? {
            let to_safe = partition_name_segment(to_type);
            let fk_name = format!("object_edges_{safe}_to_fk");
            add_fk_if_missing(
                tx,
                &format!("object_edges_{safe}"),
                &fk_name,
                &format!(
                    "FOREIGN KEY (\"to\") REFERENCES objects_{to_safe}(id) ON DELETE CASCADE"
                ),
            )
            .await?;
        } else if !to_type.is_empty() {
            eprintln!(
                "[ousia warn] Edge '{}' declares to = {} but no objects_{} partition exists; \
                 FK skipped this init_schema",
                type_name,
                to_type,
                partition_name_segment(to_type)
            );
        }

        Ok(())
    }

    /// Drop empty orphaned partitions of `parent_table` whose names
    /// follow the `{parent}_{safe}` convention but aren't in
    /// `known_types`. Non-empty orphans are warned and skipped.
    async fn drop_orphaned_partitions(
        tx: &mut Transaction<'_, Postgres>,
        parent_table: &str,
        known_types: &[&str],
    ) -> Result<(), Error> {
        let known_names: Vec<String> = known_types
            .iter()
            .map(|t| format!("{}_{}", parent_table, partition_name_segment(t)))
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
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {} LIMIT 1",
                partition
            ))
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

    /// Best-effort write of the manifest to `<target>/ousia.json`.
    ///
    /// Path resolution order:
    /// 1. `$CARGO_TARGET_DIR` if explicitly set.
    /// 2. Walk up from CWD looking for the first directory containing
    ///    `Cargo.lock` — that's the workspace root in a dev tree, so
    ///    write `<root>/target/ousia.json`. Without this, `cargo test
    ///    -p some_pkg` lands the file in `some_pkg/target/` rather
    ///    than the shared workspace `target/`.
    /// 3. Fall back to `./target/ousia.json` relative to CWD.
    ///
    /// Failure is logged at `[ousia warn]` and not propagated — this is
    /// a tooling/debugging artifact, never load-bearing.
    fn emit_manifest_json() {
        let mut path = locate_target_dir();
        let _ = std::fs::create_dir_all(&path);
        path.push("ousia.json");
        let body = crate::manifest::render_json();
        if let Err(e) = std::fs::write(&path, body) {
            eprintln!(
                "[ousia warn] could not write {}: {}",
                path.display(),
                e
            );
        }
    }
}

/// See [`PostgresAdapter::emit_manifest_json`] for the resolution
/// strategy. Returns the `target/` directory itself (not the file).
fn locate_target_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        if !dir.is_empty() {
            return std::path::PathBuf::from(dir);
        }
    }

    let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let mut here: Option<&std::path::Path> = Some(cwd.as_path());
    while let Some(dir) = here {
        if dir.join("Cargo.lock").exists() {
            return dir.join("target");
        }
        here = dir.parent();
    }
    cwd.join("target")
}

/// Convert a type name to a safe partition name segment.
/// Lowercases and replaces `-` with `_`.
fn partition_name_segment(type_name: &str) -> String {
    type_name.to_lowercase().replace('-', "_")
}

/// `major.minor` derived from `CARGO_PKG_VERSION` at compile time.
/// Example: `"2.0.4"` → `"2.0"`. Used to tag the composed schema hash
/// in `ousia_meta` so a major version bump trips the migration error.
fn schema_version_prefix() -> String {
    let ver = env!("CARGO_PKG_VERSION");
    let mut parts = ver.splitn(3, '.');
    let major = parts.next().unwrap_or("0");
    let minor = parts.next().unwrap_or("0");
    format!("{}.{}", major, minor)
}

/// Split a stored `"major.minor:hash"` entry into `(major, hash)`.
/// A bare-hash entry (no `:`) is treated as major `"0"` so an upgrade
/// from a legacy schema always reports a major mismatch.
fn parse_schema_entry(entry: &str) -> (&str, &str) {
    match entry.find(':') {
        Some(pos) => {
            let prefix = &entry[..pos];
            let hash = &entry[pos + 1..];
            let major = prefix.split('.').next().unwrap_or("0");
            (major, hash)
        }
        None => ("0", entry),
    }
}

/// PG has no `ADD CONSTRAINT IF NOT EXISTS` — emulate by checking
/// `pg_constraint` first. `definition` is the constraint body without
/// the `CONSTRAINT <name>` prefix, e.g. `"FOREIGN KEY (id) REFERENCES
/// foo(id) ON DELETE CASCADE"`.
async fn add_fk_if_missing(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    constraint_name: &str,
    definition: &str,
) -> Result<(), Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
            SELECT 1 FROM pg_constraint c \
            JOIN pg_class t ON t.oid = c.conrelid \
            WHERE t.relname = $1 AND c.conname = $2 \
         )",
    )
    .bind(table)
    .bind(constraint_name)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;

    if exists {
        return Ok(());
    }

    let sql = format!(
        "ALTER TABLE {} ADD CONSTRAINT {} {}",
        table, constraint_name, definition
    );
    sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

    Ok(())
}

/// True iff a partition `objects_<safe(type_name)>` already exists in
/// `public`. Used to decide whether an edge can FK to it on this init
/// pass.
async fn object_partition_exists(
    tx: &mut Transaction<'_, Postgres>,
    type_name: &str,
) -> Result<bool, Error> {
    let safe = partition_name_segment(type_name);
    let name = format!("objects_{safe}");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
            SELECT 1 FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            WHERE c.relname = $1 AND n.nspname = 'public' \
         )",
    )
    .bind(&name)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(exists)
}
