//! v2-specific schema verification.
//!
//! These tests confirm the v2 plumbing actually does what `plan.md`
//! promises: partitioned tables, per-partition `ON DELETE CASCADE`
//! foreign keys, the linkme `MANIFEST`, the composed schema hash,
//! the `target/ousia.json` artifact, and ledger asset partitions.
//!
//! Every test spins up its own postgres testcontainer so they are
//! isolated and order-independent.

#![cfg(test)]

use std::time::Duration;

use super::{Follow, Place, User};
use ousia::{Engine, Error, Object, ObjectMeta, adapters::postgres::PostgresAdapter};
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

// ─────────────────────────────────────────────────────────────────────
// Shared fixture (mirrors test_postgres::setup_test_db so the two test
// files can run in the same crate without colliding on container names)
// ─────────────────────────────────────────────────────────────────────

async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};

    let postgres = Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_name("imresamu/postgis")
        .with_tag("16-3.6-alpine")
        .start()
        .await
        .expect("Failed to start Postgres");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect");
    (postgres, pool)
}

async fn relations_under(pool: &PgPool, parent: &str) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT c.relname::text \
         FROM pg_class c \
         JOIN pg_inherits i ON i.inhrelid = c.oid \
         JOIN pg_class p ON p.oid = i.inhparent \
         WHERE p.relname = $1 \
         ORDER BY c.relname",
    )
    .bind(parent)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn fk_names_on(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT c.conname::text \
         FROM pg_constraint c \
         JOIN pg_class t ON t.oid = c.conrelid \
         WHERE t.relname = $1 AND c.contype = 'f' \
         ORDER BY c.conname",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap()
}

// ─────────────────────────────────────────────────────────────────────
// 1. Parent partitioned tables + per-type partitions exist
// ─────────────────────────────────────────────────────────────────────

/// `init_schema` must create the four partitioned parents and one
/// partition per type registered in the `MANIFEST` slice (User, Place,
/// Variants, Post, Delivery, Dropoff, etc. defined in `adapter/mod.rs`).
#[tokio::test]
async fn v2_partitions_created_for_every_manifest_type() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    // Object partitions — one per Object in the manifest.
    let objs = relations_under(&pool, "objects").await;
    assert!(
        objs.contains(&"objects_user".to_string()),
        "objects_user missing; got {:?}",
        objs
    );
    assert!(
        objs.contains(&"objects_place".to_string()),
        "objects_place missing; got {:?}",
        objs
    );

    // Edge partitions.
    let edges = relations_under(&pool, "object_edges").await;
    assert!(
        edges.contains(&"object_edges_follow".to_string()),
        "object_edges_follow missing; got {:?}",
        edges
    );

    // Constraints and geo partitions — Place has geo, User has unique.
    let cons = relations_under(&pool, "object_constraints").await;
    assert!(
        cons.contains(&"object_constraints_user".to_string()),
        "object_constraints_user missing; got {:?}",
        cons
    );
    let geos = relations_under(&pool, "object_geo").await;
    assert!(
        geos.contains(&"object_geo_place".to_string()),
        "object_geo_place missing; got {:?}",
        geos
    );
}

/// `init_schema` must be idempotent — re-running over the same
/// database is a no-op and never errors.
#[tokio::test]
async fn v2_init_schema_is_idempotent() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    // Same call again must not error or create duplicates.
    adapter.init_schema().await.unwrap();

    let objs = relations_under(&pool, "objects").await;
    // No duplicate partition entries.
    let unique_count = objs.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(unique_count, objs.len());
}

// ─────────────────────────────────────────────────────────────────────
// 2. FK cascade — deleting an object purges constraints + geo + edges
// ─────────────────────────────────────────────────────────────────────

/// Each per-type partition declares its ON DELETE CASCADE FK back to
/// the matching `objects_<type>(id)`. Confirm the constraints exist
/// by name on the relevant partitions.
#[tokio::test]
async fn v2_fk_constraints_exist_on_partitions() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let cons_fks = fk_names_on(&pool, "object_constraints_user").await;
    assert!(
        cons_fks
            .iter()
            .any(|n| n == "object_constraints_user_id_fk"),
        "object_constraints_user_id_fk missing; got {:?}",
        cons_fks
    );

    let geo_fks = fk_names_on(&pool, "object_geo_place").await;
    assert!(
        geo_fks.iter().any(|n| n == "object_geo_place_object_id_fk"),
        "object_geo_place_object_id_fk missing; got {:?}",
        geo_fks
    );

    let edge_fks = fk_names_on(&pool, "object_edges_follow").await;
    assert!(
        edge_fks.iter().any(|n| n == "object_edges_follow_from_fk"),
        "object_edges_follow_from_fk missing; got {:?}",
        edge_fks
    );
    assert!(
        edge_fks.iter().any(|n| n == "object_edges_follow_to_fk"),
        "object_edges_follow_to_fk missing; got {:?}",
        edge_fks
    );
}

/// Deleting an object must cascade-delete its unique-constraint rows.
/// Regression for the v1 behavior where the application had to clear
/// them manually.
#[tokio::test]
async fn v2_fk_cascade_clears_unique_constraints_on_object_delete() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM object_constraints_user WHERE id = $1")
            .bind(alice.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, 1, "create_object should write a constraint row");

    engine
        .delete_object::<User>(alice.id(), alice.meta().owner)
        .await
        .unwrap();

    let after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM object_constraints_user WHERE id = $1")
            .bind(alice.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after, 0, "FK CASCADE must purge the constraint row");
}

/// Deleting an object must cascade-delete its geo rows.
#[tokio::test]
async fn v2_fk_cascade_clears_geo_on_object_delete() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut p = Place::default();
    p.name = "shop".into();
    p.lat = 6.5244;
    p.lon = 3.3792;
    engine.create_object(&p).await.unwrap();

    let before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM object_geo_place WHERE object_id = $1")
            .bind(p.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, 1);

    engine
        .delete_object::<Place>(p.id(), p.meta().owner)
        .await
        .unwrap();

    let after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM object_geo_place WHERE object_id = $1")
            .bind(p.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after, 0, "FK CASCADE must purge the geo row");
}

/// Deleting a parent must cascade-delete edges where it was the
/// `from` or the `to` endpoint.
#[tokio::test]
async fn v2_fk_cascade_clears_edges_on_endpoint_delete() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@x.com".into();
    engine.create_object(&alice).await.unwrap();
    engine.create_object(&bob).await.unwrap();

    let follow = Follow {
        _meta: ousia::EdgeMeta::new(alice.id(), bob.id()),
        notification: true,
    };
    engine.create_edge(&follow).await.unwrap();

    let before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM object_edges_follow WHERE \"from\" = $1 AND \"to\" = $2",
    )
    .bind(alice.id())
    .bind(bob.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(before, 1);

    // Delete the `to` endpoint — the edge should disappear via CASCADE.
    engine
        .delete_object::<User>(bob.id(), bob.meta().owner)
        .await
        .unwrap();

    let after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM object_edges_follow WHERE \"from\" = $1 AND \"to\" = $2",
    )
    .bind(alice.id())
    .bind(bob.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, 0, "FK CASCADE must purge the edge on endpoint delete");
}

// ─────────────────────────────────────────────────────────────────────
// 3. Manifest artifact + composed schema hash
// ─────────────────────────────────────────────────────────────────────

/// `init_schema` emits `<workspace>/target/ousia.json` — not the
/// per-package `<pkg>/target/`. We resolve the workspace root from the
/// test's CWD (`test_suit/`) by walking up to the first ancestor
/// containing `Cargo.lock`, matching the resolution in
/// `PostgresAdapter::emit_manifest_json`. Also confirms the file is
/// deduplicated: two struct definitions sharing a `type_name` collapse
/// to one entry in the artifact.
#[tokio::test]
async fn v2_init_schema_writes_target_ousia_json() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let cwd = std::env::current_dir().unwrap();
    let mut here: Option<&std::path::Path> = Some(cwd.as_path());
    let workspace_root = loop {
        match here {
            Some(dir) if dir.join("Cargo.lock").exists() => break dir.to_path_buf(),
            Some(dir) => here = dir.parent(),
            None => panic!("no Cargo.lock above {}", cwd.display()),
        }
    };
    let path = workspace_root.join("target").join("ousia.json");
    assert!(
        path.exists(),
        "{} must be written by init_schema",
        path.display()
    );

    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("\"objects\""), "missing objects key: {}", body);
    assert!(body.contains("\"User\""), "missing User: {}", body);
    assert!(body.contains("\"Follow\""), "missing Follow: {}", body);
    // Edge entry carries from/to.
    assert!(
        body.contains("\"from\": \"User\""),
        "missing Follow.from: {}",
        body
    );

    // Two derives in this crate share `type_name = "Post"` (Post + PostNew)
    // and two share `type_name = "User"` (adapter::User + the User
    // defined inside `test_view`). Each must appear exactly once.
    let object_count = |name: &str| body.matches(&format!("\"{}\"", name)).count();
    assert_eq!(object_count("Post"), 1, "Post duplicated: {}", body);
    // "User" is also referenced inside the Follow edge entry (twice as
    // `from`/`to`), so the raw count is 3: 1 in objects[] + 2 in edges[].
    assert_eq!(object_count("User"), 3, "User dedup wrong: {}", body);
}

/// After a successful init, the composed schema hash is stored under
/// `schema:composed` with the current `major.minor:` prefix.
#[tokio::test]
async fn v2_composed_schema_hash_stored_with_version_prefix() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let stored: Option<String> =
        sqlx::query_scalar("SELECT value FROM ousia_meta WHERE key = 'schema:composed'")
            .fetch_optional(&pool)
            .await
            .unwrap();
    let value = stored.expect("schema:composed must be written");

    // Format: "MAJOR.MINOR:HASH". The prefix tracks the ousia crate
    // version (not the test crate's), so verify the shape rather than
    // pinning a specific major here.
    let (prefix, hash) = value
        .split_once(':')
        .unwrap_or_else(|| panic!("composed entry must be `prefix:hash`, got `{}`", value));
    let prefix_parts: Vec<&str> = prefix.split('.').collect();
    assert_eq!(
        prefix_parts.len(),
        2,
        "prefix must be `major.minor`, got `{}`",
        prefix
    );
    assert!(
        prefix_parts.iter().all(|p| p.parse::<u32>().is_ok()),
        "prefix components must be numeric, got `{}`",
        prefix
    );
    assert_eq!(hash.len(), 64, "blake3 hash hex must be 64 chars: `{}`", hash);
    assert!(
        hash.chars().all(|c| c.is_ascii_hexdigit()),
        "hash must be hex, got `{}`",
        hash
    );
}

/// The hash MUST change when the catalog changes. Drop one of our
/// indexes out-of-band and confirm the recomputed catalog hash
/// diverges from the stored one. (We do NOT assert that a follow-up
/// `init_schema` restores the *exact* original hash — PG's catalog can
/// represent the same logical index slightly differently after a drop
/// + recreate, and we don't want to over-promise on byte-for-byte
/// idempotence. What matters is that drift surfaces.)
#[tokio::test]
async fn v2_schema_hash_changes_when_catalog_changes() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let stored: String =
        sqlx::query_scalar("SELECT value FROM ousia_meta WHERE key = 'schema:composed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let (_, original_hash) = stored.split_once(':').unwrap();

    // Out-of-band catalog mutation that `init_schema` did not perform.
    sqlx::query("DROP INDEX idx_objects_owner_created")
        .execute(&pool)
        .await
        .unwrap();

    // Recompute against the new on-disk state.
    let after_drop = adapter.compute_postgres_schema_hash().await.unwrap();
    assert_ne!(
        original_hash, after_drop,
        "catalog hash MUST change when an index disappears — that's the \
         drift this mechanism exists to detect"
    );
}

/// The hash MUST NOT depend on dynamic per-type / per-asset
/// partitions. A developer removing a model the next morning is a
/// totally normal action — `init_schema` would drop the orphaned
/// `objects_<type>` and its `object_constraints_<type>` /
/// `object_geo_<type>` / `object_edges_<edge>` siblings. None of that
/// is structural drift.
#[tokio::test]
async fn v2_schema_hash_ignores_per_type_partitions() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let baseline = adapter.compute_postgres_schema_hash().await.unwrap();

    // Simulate the dev removing the Place model — `init_schema`'s
    // `drop_orphaned_partitions` would do exactly this on next run.
    // Drop in FK-dependency order: dependents first.
    sqlx::query("DROP TABLE IF EXISTS object_geo_place")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE IF EXISTS object_constraints_place")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP TABLE IF EXISTS objects_place")
        .execute(&pool)
        .await
        .unwrap();

    let after_partition_drop = adapter.compute_postgres_schema_hash().await.unwrap();
    assert_eq!(
        baseline, after_partition_drop,
        "removing a per-type partition (dev removed a model) MUST NOT change \
         the schema hash — only structural changes on the parents do"
    );
}

/// Same property for per-asset partitions on `ledger_value_objects`.
/// The asset list is caller-supplied config, not library structure.
#[tokio::test]
async fn v2_schema_hash_ignores_per_asset_partitions() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let baseline = adapter.compute_postgres_schema_hash().await.unwrap();

    use ousia::ledger::adapters::postgres::PostgresSchemaLedgerAdapter;
    adapter
        .init_ledger_schema(&["USD", "NGN", "EUR"])
        .await
        .unwrap();

    let after_assets = adapter.compute_postgres_schema_hash().await.unwrap();
    assert_eq!(
        baseline, after_assets,
        "registering per-asset partitions (deployment config) MUST NOT change \
         the schema hash"
    );
}

/// The hash MUST change when a column type changes. This is the
/// motivating case: a v1 → v2 JSONB → BYTEA flip should not silently
/// pass a structural check.
#[tokio::test]
async fn v2_schema_hash_changes_on_column_type_flip() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let h_bytea = adapter.compute_postgres_schema_hash().await.unwrap();

    // Pretend a hypothetical v1 had `data JSONB`. We have to drop the
    // BYTEA default first because PG can't auto-cast it to jsonb.
    sqlx::query("ALTER TABLE objects ALTER COLUMN data DROP DEFAULT")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE objects ALTER COLUMN data TYPE jsonb USING '{}'::jsonb")
        .execute(&pool)
        .await
        .unwrap();

    let h_jsonb = adapter.compute_postgres_schema_hash().await.unwrap();
    assert_ne!(
        h_bytea, h_jsonb,
        "flipping `objects.data` BYTEA → JSONB MUST flip the catalog hash — \
         that's the kind of structural change we want to detect"
    );
}

/// Tamper with the stored hash to fake a major-version downgrade and
/// confirm `init_schema` refuses to run.
#[tokio::test]
async fn v2_schema_hash_major_mismatch_returns_migration_required() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    // Overwrite with a different major version — simulates an upgrade
    // from a prior breaking release.
    sqlx::query(
        "UPDATE ousia_meta SET value = '0.0:deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef' WHERE key = 'schema:composed'",
    )
    .execute(&pool)
    .await
    .unwrap();

    let err = adapter
        .init_schema()
        .await
        .expect_err("major mismatch must error");
    match err {
        Error::SchemaMigrationRequired(_) => {}
        other => panic!("expected SchemaMigrationRequired, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────
// 4. Ledger asset partitioning
// ─────────────────────────────────────────────────────────────────────

/// `init_ledger_schema(&["USD","NGN"])` creates one per-asset partition
/// of `ledger_value_objects` plus the catch-all `_default`. Calling
/// again with a wider list adds new partitions idempotently.
#[tokio::test]
async fn v2_ledger_init_creates_per_asset_partitions() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    use ousia::ledger::adapters::postgres::PostgresSchemaLedgerAdapter;
    adapter
        .init_ledger_schema(&["USD", "NGN"])
        .await
        .expect("init_ledger_schema with assets");

    let parts = relations_under(&pool, "ledger_value_objects").await;
    assert!(
        parts.contains(&"ledger_value_objects_default".to_string()),
        "default partition missing; got {:?}",
        parts
    );
    assert!(
        parts.contains(&"ledger_value_objects_usd".to_string()),
        "USD partition missing; got {:?}",
        parts
    );
    assert!(
        parts.contains(&"ledger_value_objects_ngn".to_string()),
        "NGN partition missing; got {:?}",
        parts
    );

    // Idempotent + can add new assets later.
    adapter
        .init_ledger_schema(&["USD", "NGN", "EUR"])
        .await
        .expect("idempotent re-init with wider list");
    let parts = relations_under(&pool, "ledger_value_objects").await;
    assert!(parts.contains(&"ledger_value_objects_eur".to_string()));
}
