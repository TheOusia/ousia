//! Shared fixtures and helpers for Ousia benchmarks.
//!
//! # External database setup
//!
//! Set `BENCH_PG_BASE=postgres://user:pass@host` (no trailing slash, no db
//! name). Defaults to `postgres://postgres:postgres@localhost`.
//!
//! Each bench binary drops and recreates its own isolated databases on first
//! run, so previous data never pollutes results.

use ousia::{EdgeMeta, Meta, OusiaDefault, OusiaEdge, OusiaObject, query::ToIndexValue};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};

// ─────────────────────────────────────────────────────────────────────────────
// Domain types used across ALL benchmarks
// ─────────────────────────────────────────────────────────────────────────────

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "BenchUser",
    unique = "username",
    index = "username:search+sort",
    index = "email:search",
    index = "score:search+sort",
    index = "active:search"
)]
pub struct BenchUser {
    pub _meta: Meta,
    pub username: String,
    pub email: String,
    pub display_name: String,
    pub score: i64,
    pub active: bool,
}

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "BenchPost",
    index = "title:search+sort",
    index = "status:search",
    index = "view_count:search+sort",
    index = "tags:search"
)]
pub struct BenchPost {
    pub _meta: Meta,
    pub title: String,
    pub body: String,
    pub status: PostStatus,
    pub view_count: i64,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum PostStatus {
    #[default]
    Draft,
    Published,
    Archived,
}

impl ToIndexValue for PostStatus {
    fn to_index_value(&self) -> ousia::query::IndexValue {
        ousia::query::IndexValue::String(
            match self {
                PostStatus::Draft => "draft",
                PostStatus::Published => "published",
                PostStatus::Archived => "archived",
            }
            .to_string(),
        )
    }
}

#[derive(OusiaEdge, Debug)]
#[ousia(type_name = "BenchFollow", index = "weight:search+sort")]
pub struct BenchFollow {
    pub _meta: EdgeMeta,
    pub weight: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Async Tokio runtime helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt")
}

pub fn mt_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio mt-rt")
}

// ─────────────────────────────────────────────────────────────────────────────
// External database bootstrap
// ─────────────────────────────────────────────────────────────────────────────

fn bench_pg_base() -> String {
    std::env::var("BENCH_PG_BASE")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost".to_string())
}

/// Drop, recreate, and connect to an isolated benchmark database.
///
/// The database is named exactly `name` — pick a distinct name per bench
/// binary to avoid conflicts when binaries run back-to-back.
pub async fn connect_db(name: &str) -> PgPool {
    let base = bench_pg_base();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "bench: cannot connect to Postgres — {e}\n\
                 Set BENCH_PG_BASE=postgres://user:pass@host"
            )
        });

    // Evict existing connections so DROP does not block.
    sqlx::query(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(name)
    .execute(&admin)
    .await
    .ok();

    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\""))
        .execute(&admin)
        .await;

    sqlx::query(&format!("CREATE DATABASE \"{name}\""))
        .execute(&admin)
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE \"{name}\": {e}"));

    admin.close().await;

    PgPoolOptions::new()
        .max_connections(10)
        .connect(&format!("{base}/{name}"))
        .await
        .expect("bench pg pool")
}

/// Connect sea-orm to a database that already exists (e.g. created by
/// [`connect_db`]).  Does NOT drop or recreate anything.
pub async fn connect_orm(name: &str) -> sea_orm::DatabaseConnection {
    let base = bench_pg_base();
    let mut opts = sea_orm::ConnectOptions::new(format!("{base}/{name}"));
    opts.max_connections(10);
    sea_orm::Database::connect(opts)
        .await
        .expect("sea-orm connection")
}

// ─────────────────────────────────────────────────────────────────────────────
// BenchHandle — leak-based lifetime management for criterion
// ─────────────────────────────────────────────────────────────────────────────

/// Keeps a `(Runtime, Ctx)` alive for the entire benchmark binary by leaking
/// the box — criterion benchmarks need `'static` access to shared state.
pub struct BenchHandle<T: 'static> {
    cell: std::sync::OnceLock<&'static (tokio::runtime::Runtime, T)>,
}

impl<T: 'static> BenchHandle<T> {
    pub const fn new() -> Self {
        Self {
            cell: std::sync::OnceLock::new(),
        }
    }

    pub fn get_or_init(
        &'static self,
        f: impl FnOnce() -> (tokio::runtime::Runtime, T),
    ) -> &'static (tokio::runtime::Runtime, T) {
        self.cell.get_or_init(|| Box::leak(Box::new(f())))
    }
}

// SAFETY: Runtime, PgPool, and DatabaseConnection are Send.
// We only hand out shared references after init completes.
unsafe impl<T: Send> Sync for BenchHandle<T> {}

// ─────────────────────────────────────────────────────────────────────────────
// Raw row types — used by raw_sqlx bench variants
// ─────────────────────────────────────────────────────────────────────────────
//
// Every raw_sqlx bench must decode into one of these so the comparison is
// apples-to-apples: ousia returns typed Rust structs, so must raw_sqlx.

#[derive(Debug, sqlx::FromRow)]
pub struct RawUser {
    pub id: uuid::Uuid,
    pub username: String,
    pub email: String,
    pub display_name: String,
    pub score: i64,
    pub active: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Raw post row.  `tags TEXT[]` decodes natively via sqlx's postgres driver.
#[derive(Debug, sqlx::FromRow)]
pub struct RawPost {
    pub id: uuid::Uuid,
    pub owner_id: uuid::Uuid,
    pub title: String,
    pub body: String,
    pub status: String,
    pub view_count: i64,
    pub tags: Vec<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct RawFollow {
    #[sqlx(rename = "from")]
    pub from_id: uuid::Uuid,
    #[sqlx(rename = "to")]
    pub to_id: uuid::Uuid,
    pub weight: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// sea-orm entity definitions  (raw benchmark schema)
// ─────────────────────────────────────────────────────────────────────────────
//
// These match the raw SQL schemas created by `setup_raw_schema` in each bench
// file.  The same entities are used for all three bench binaries because all
// three share the same unified raw schema.

pub mod orm {
    /// `users` table entity.
    pub mod users {
        use sea_orm::entity::prelude::*;

        #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
        #[sea_orm(table_name = "users")]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false)]
            pub id: Uuid,
            pub username: String,
            pub email: String,
            pub display_name: String,
            pub score: i64,
            pub active: bool,
            pub created_at: DateTimeWithTimeZone,
            pub updated_at: DateTimeWithTimeZone,
        }

        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}

        impl ActiveModelBehavior for ActiveModel {}
    }

    /// `follows` edge table entity.
    pub mod follows {
        use sea_orm::entity::prelude::*;

        #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
        #[sea_orm(table_name = "follows")]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false, column_name = "from")]
            pub from_id: Uuid,
            #[sea_orm(primary_key, auto_increment = false, column_name = "to")]
            pub to_id: Uuid,
            pub weight: i64,
        }

        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {
            #[sea_orm(
                belongs_to = "crate::orm::users::Entity",
                from = "Column::FromId", // Use `from_id` here
                to = "crate::orm::users::Column::Id" // Reference user's ID
            )]
            Follower, // Who follows
            #[sea_orm(
                belongs_to = "crate::orm::users::Entity",
                from = "Column::ToId", // Use `to_id` here
                to = "crate::orm::users::Column::Id" // Reference user's ID
            )]
            Followed, // Who is followed
        }

        impl ActiveModelBehavior for ActiveModel {}
    }

    /// Post results via `find_by_statement`.
    ///
    /// `tags TEXT[]` has no native sea-orm entity type, so posts are always
    /// queried with raw SQL through sea-orm's connection, casting the array to
    /// JSON: `to_jsonb(tags) AS tags`.  The `with-json` feature lets sea-orm
    /// decode that as `serde_json::Value`, doing equivalent work to what
    /// raw_sqlx does decoding `Vec<String>` from TEXT[].
    pub mod posts {
        use sea_orm::FromQueryResult;
        use uuid::Uuid;

        #[derive(Debug, FromQueryResult)]
        pub struct OrmPost {
            pub id: Uuid,
            pub owner_id: Uuid,
            pub title: String,
            pub body: String,
            pub status: String,
            pub view_count: i64,
            pub tags: serde_json::Value,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk seeders — fast batch insertion for large-scale benchmarks
// ─────────────────────────────────────────────────────────────────────────────

/// Batch-insert N raw users using `unnest()`. Returns their IDs.
pub async fn seed_raw_users_bulk(pool: &PgPool, n: usize) -> Vec<uuid::Uuid> {
    let ids: Vec<uuid::Uuid> = (0..n).map(|_| uuid::Uuid::now_v7()).collect();
    let usernames: Vec<String> = (0..n).map(|i| format!("user_{i:06}")).collect();
    let emails: Vec<String> = (0..n).map(|i| format!("user_{i:06}@bench.test")).collect();
    let display_names: Vec<String> = (0..n).map(|i| format!("User {i}")).collect();
    let scores: Vec<i64> = (0..n).map(|i| (i as i64) * 7 % 10_000).collect();
    let actives: Vec<bool> = (0..n).map(|i| i % 3 != 0).collect();

    sqlx::query(
        "INSERT INTO users (id, username, email, display_name, score, active) \
         SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[], $5::bigint[], $6::bool[])",
    )
    .bind(&ids)
    .bind(&usernames)
    .bind(&emails)
    .bind(&display_names)
    .bind(&scores)
    .bind(&actives)
    .execute(pool)
    .await
    .unwrap();

    ids
}

/// Batch-insert follows using `unnest()`. Each user follows the next `follows_per` users (circular).
pub async fn seed_raw_follows_bulk(pool: &PgPool, user_ids: &[uuid::Uuid], follows_per: usize) {
    let n = user_ids.len();
    let cap = n * follows_per;
    let mut froms: Vec<uuid::Uuid> = Vec::with_capacity(cap);
    let mut tos: Vec<uuid::Uuid> = Vec::with_capacity(cap);
    let mut weights: Vec<i64> = Vec::with_capacity(cap);

    for i in 0..n {
        for j in 1..=follows_per {
            froms.push(user_ids[i]);
            tos.push(user_ids[(i + j) % n]);
            weights.push(j as i64);
        }
    }

    sqlx::query(
        r#"INSERT INTO follows ("from", "to", weight)
           SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::bigint[])
           ON CONFLICT DO NOTHING"#,
    )
    .bind(&froms)
    .bind(&tos)
    .bind(&weights)
    .execute(pool)
    .await
    .unwrap();
}

/// Batch-insert N ousia `BenchUser` objects directly into `public.objects`.
///
/// Bypasses the engine (no `unique_constraints` entry, no sequence); suitable only for
/// read-only benchmark fixtures.
pub async fn seed_ousia_users_bulk(pool: &PgPool, n: usize) -> Vec<uuid::Uuid> {
    let nil = uuid::Uuid::nil();
    let mut ids: Vec<uuid::Uuid> = Vec::with_capacity(n);
    let mut data_strs: Vec<String> = Vec::with_capacity(n);
    let mut index_meta_strs: Vec<String> = Vec::with_capacity(n);

    for i in 0..n {
        let id = uuid::Uuid::now_v7();
        let username = format!("user_{i:06}");
        let email = format!("user_{i:06}@bench.test");
        let display_name = format!("User {i}");
        let score: i64 = (i as i64) * 7 % 10_000;
        let active = i % 3 != 0;

        ids.push(id);
        data_strs.push(
            serde_json::json!({
                "username": username,
                "email": email,
                "display_name": display_name,
                "score": score,
                "active": active,
            })
            .to_string(),
        );
        index_meta_strs.push(
            serde_json::json!({
                "username": username,
                "email": email,
                "score": score,
                "active": active,
            })
            .to_string(),
        );
    }

    sqlx::query(
        "INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta) \
         SELECT t.id, $2, $3::uuid, now(), now(), t.data::jsonb, t.im::jsonb \
         FROM unnest($1::uuid[], $4::text[], $5::text[]) AS t(id, data, im)",
    )
    .bind(&ids)
    .bind("BenchUser")
    .bind(nil)
    .bind(&data_strs)
    .bind(&index_meta_strs)
    .execute(pool)
    .await
    .unwrap();

    ids
}

/// Batch-insert `BenchFollow` edges directly into `public.edges`.
///
/// Each user follows the next `follows_per` users (circular). Bypasses the engine.
pub async fn seed_ousia_edges_bulk(pool: &PgPool, user_ids: &[uuid::Uuid], follows_per: usize) {
    let n = user_ids.len();
    let cap = n * follows_per;
    let mut froms: Vec<uuid::Uuid> = Vec::with_capacity(cap);
    let mut tos: Vec<uuid::Uuid> = Vec::with_capacity(cap);
    let mut data_strs: Vec<String> = Vec::with_capacity(cap);
    let mut index_meta_strs: Vec<String> = Vec::with_capacity(cap);

    for i in 0..n {
        for j in 1..=follows_per {
            let weight = j as i64;
            froms.push(user_ids[i]);
            tos.push(user_ids[(i + j) % n]);
            data_strs.push(serde_json::json!({ "weight": weight }).to_string());
            index_meta_strs.push(serde_json::json!({ "weight": weight }).to_string());
        }
    }

    sqlx::query(
        r#"INSERT INTO public.edges ("from", "to", type, data, index_meta)
           SELECT t.f, t.t, $3, t.data::jsonb, t.im::jsonb
           FROM unnest($1::uuid[], $2::uuid[], $4::text[], $5::text[]) AS t(f, t, data, im)
           ON CONFLICT DO NOTHING"#,
    )
    .bind(&froms)
    .bind(&tos)
    .bind("BenchFollow")
    .bind(&data_strs)
    .bind(&index_meta_strs)
    .execute(pool)
    .await
    .unwrap();
}
