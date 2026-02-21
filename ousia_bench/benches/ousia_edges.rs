//! Benchmark: Graph edge operations
//!
//! Databases:
//!   ousia_bench_e_ousia  — Ousia edge schema, populated via Engine.
//!   ousia_bench_e_raw    — plain `follows` table; shared by raw sqlx and sea-orm.
//!
//! Set `BENCH_PG_BASE=postgres://user:pass@host` before running.
//!
//! Covers:
//!   query_edges_forward         ousia / raw_sqlx / sea_orm
//!   query_edges_reverse         ousia / raw_sqlx / sea_orm
//!   count_edges                 ousia / raw_sqlx / sea_orm
//!   query_edges_with_filter     ousia / raw_sqlx / sea_orm
//!   preload_forward             ousia / raw_sqlx / sea_orm  (who does pivot follow?)
//!   preload_reverse             ousia / raw_sqlx / sea_orm  (who follows pivot?)
//!   create_edge                 ousia / raw_sqlx / sea_orm

use criterion::{Criterion, criterion_group, criterion_main};
use ousia::{EdgeMeta, EdgeQuery, Engine, ObjectMeta, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchFollow, BenchUser, RawFollow, RawUser, orm};
use sea_orm::{
    ColumnTrait, DbBackend, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter, Statement,
    sea_query::OnConflict,
};
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Extra raw row types for multi-pivot batch benchmarks
// ─────────────────────────────────────────────────────────────────────────────

/// Result of the batch forward-traversal JOIN: pivot_id + full user columns.
#[derive(Debug, sqlx::FromRow)]
struct RawEdgeUser {
    pivot_id: Uuid,
    id: Uuid,
    username: String,
    email: String,
    display_name: String,
    score: i64,
    active: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

/// Result of the batch GROUP BY count query.
#[derive(Debug, sqlx::FromRow)]
struct RawEdgeCount {
    from_id: Uuid,
    count: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    // Ousia side
    engine: Engine,
    ousia_pivot: Uuid,
    ousia_from: Uuid,
    ousia_to: Uuid,
    /// First 1 000 user IDs used as N+1 pivots (full 10k in a loop would be impractical).
    ousia_n1_user_ids: Vec<Uuid>,

    // Raw / sea-orm side (same database)
    raw_pool: PgPool,
    orm_db: sea_orm::DatabaseConnection,
    raw_pivot: Uuid,
    raw_from: Uuid,
    raw_to: Uuid,
    raw_n1_user_ids: Vec<Uuid>,
}

unsafe impl Sync for Ctx {}

static STATE: ousia_bench::BenchHandle<Ctx> = ousia_bench::BenchHandle::new();

fn state() -> &'static (tokio::runtime::Runtime, Ctx) {
    STATE.get_or_init(|| {
        let rt = ousia_bench::mt_rt();
        let ctx = rt.block_on(setup());
        (rt, ctx)
    })
}

macro_rules! run {
    ($e:expr) => {
        state().0.block_on(async { $e })
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Setup
// ─────────────────────────────────────────────────────────────────────────────

async fn setup() -> Ctx {
    let ousia_pool = ousia_bench::connect_db("ousia_bench_e_ousia").await;
    let raw_pool = ousia_bench::connect_db("ousia_bench_e_raw").await;
    let orm_db = ousia_bench::connect_orm("ousia_bench_e_raw").await;

    // --- Ousia side ---
    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    // Clean any data from a previous run before re-seeding.
    sqlx::query("TRUNCATE public.edges, public.objects")
        .execute(&ousia_pool).await.unwrap();
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(ousia_pool.clone())));

    // 10k users, each following the next 10 (100k edges total) — bulk inserted.
    let ousia_user_ids = ousia_bench::seed_ousia_users_bulk(&ousia_pool, 10_000).await;
    ousia_bench::seed_ousia_edges_bulk(&ousia_pool, &ousia_user_ids, 10).await;

    let ousia_pivot = ousia_user_ids[500];
    let ousia_from  = ousia_user_ids[5_000];
    let ousia_to    = ousia_user_ids[9_999];

    // --- Raw / ORM side ---
    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = ousia_bench::seed_raw_users_bulk(&raw_pool, 10_000).await;
    ousia_bench::seed_raw_follows_bulk(&raw_pool, &raw_user_ids, 10).await;

    let raw_pivot = raw_user_ids[500];
    let raw_from  = raw_user_ids[5_000];
    let raw_to    = raw_user_ids[9_999];

    Ctx {
        engine,
        ousia_pivot,
        ousia_from,
        ousia_to,
        ousia_n1_user_ids: ousia_user_ids[..1_000].to_vec(),
        raw_pool,
        orm_db,
        raw_pivot,
        raw_from,
        raw_to,
        raw_n1_user_ids: raw_user_ids[..1_000].to_vec(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw schema
// ─────────────────────────────────────────────────────────────────────────────

async fn setup_raw_schema(pool: &PgPool) {
    // Drop tables from any previous run before recreating.
    sqlx::query("DROP TABLE IF EXISTS follows CASCADE").execute(pool).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS users CASCADE").execute(pool).await.unwrap();

    // Unified users schema with defaults — matches the orm::users entity
    sqlx::query(
        r#"CREATE TABLE users (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            username     TEXT        NOT NULL DEFAULT '',
            email        TEXT        NOT NULL DEFAULT '',
            display_name TEXT        NOT NULL DEFAULT '',
            score        BIGINT      NOT NULL DEFAULT 0,
            active       BOOLEAN     NOT NULL DEFAULT true,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"CREATE TABLE follows (
            "from"  UUID    NOT NULL,
            "to"    UUID    NOT NULL,
            weight  BIGINT  NOT NULL DEFAULT 1,
            PRIMARY KEY ("from", "to")
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    for ddl in [
        r#"CREATE INDEX idx_follows_from ON follows("from")"#,
        r#"CREATE INDEX idx_follows_to   ON follows("to")"#,
        "CREATE INDEX idx_follows_weight ON follows(weight)",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_query_edges(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("query_edges_forward");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_edges(ctx.ousia_pivot, EdgeQuery::default())
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawFollow> = sqlx::query_as(
                    r#"SELECT "from", "to", weight FROM follows WHERE "from" = $1"#,
                )
                .bind(ctx.raw_pivot)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::follows::Model> = orm::follows::Entity::find()
                    .filter(orm::follows::Column::FromId.eq(ctx.raw_pivot))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_reverse_edges(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("query_edges_reverse");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_reverse_edges(ctx.ousia_pivot, EdgeQuery::default())
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawFollow> = sqlx::query_as(
                    r#"SELECT "from", "to", weight FROM follows WHERE "to" = $1"#,
                )
                .bind(ctx.raw_pivot)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::follows::Model> = orm::follows::Entity::find()
                    .filter(orm::follows::Column::ToId.eq(ctx.raw_pivot))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_count_edges(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("count_edges");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: u64 = ctx
                    .engine
                    .count_edges::<BenchFollow>(ctx.ousia_pivot, None)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: i64 =
                    sqlx::query_scalar(r#"SELECT COUNT(*) FROM follows WHERE "from" = $1"#)
                        .bind(ctx.raw_pivot)
                        .fetch_one(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: u64 = orm::follows::Entity::find()
                    .filter(orm::follows::Column::FromId.eq(ctx.raw_pivot))
                    .count(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_edge_filter(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("query_edges_with_filter");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_edges(
                        ctx.ousia_pivot,
                        EdgeQuery::default().where_eq(&BenchFollow::FIELDS.weight, 2_i64),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawFollow> = sqlx::query_as(
                    r#"SELECT "from", "to", weight FROM follows WHERE "from" = $1 AND weight = $2"#,
                )
                .bind(ctx.raw_pivot)
                .bind(2_i64)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::follows::Model> = orm::follows::Entity::find()
                    .filter(orm::follows::Column::FromId.eq(ctx.raw_pivot))
                    .filter(orm::follows::Column::Weight.eq(2_i64))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

/// Forward traversal: "who does pivot follow?" — pivot → BenchFollow → BenchUser
fn bench_preload_forward(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("preload_forward");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .preload_object::<BenchUser>(ctx.ousia_pivot)
                    .edge::<BenchFollow, BenchUser>()
                    .collect()
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawUser> = sqlx::query_as(
                    r#"SELECT u.id, u.username, u.email, u.display_name, u.score, u.active,
                              u.created_at, u.updated_at
                       FROM follows f
                       INNER JOIN users u ON u.id = f."to"
                       WHERE f."from" = $1"#,
                )
                .bind(ctx.raw_pivot)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Model::find_by_statement(
                    Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        r#"SELECT u.id, u.username, u.email, u.display_name, u.score, u.active,
                                  u.created_at, u.updated_at
                           FROM follows f
                           INNER JOIN users u ON u.id = f."to"
                           WHERE f."from" = $1"#,
                        [ctx.raw_pivot.into()],
                    ),
                )
                .all(&ctx.orm_db)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

/// Reverse traversal: "who follows pivot?" — BenchUser → BenchFollow → pivot
fn bench_preload_reverse(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("preload_reverse");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .preload_object::<BenchUser>(ctx.ousia_pivot)
                    .edge::<BenchFollow, BenchUser>()
                    .collect_reverse()
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawUser> = sqlx::query_as(
                    r#"SELECT u.id, u.username, u.email, u.display_name, u.score, u.active,
                              u.created_at, u.updated_at
                       FROM follows f
                       INNER JOIN users u ON u.id = f."from"
                       WHERE f."to" = $1"#,
                )
                .bind(ctx.raw_pivot)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Model::find_by_statement(
                    Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        r#"SELECT u.id, u.username, u.email, u.display_name, u.score, u.active,
                                  u.created_at, u.updated_at
                           FROM follows f
                           INNER JOIN users u ON u.id = f."from"
                           WHERE f."to" = $1"#,
                        [ctx.raw_pivot.into()],
                    ),
                )
                .all(&ctx.orm_db)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_create_edge(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("create_edge");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let edge = BenchFollow {
                    _meta: EdgeMeta::new(ctx.ousia_from, ctx.ousia_to),
                    weight: 1,
                };
                ctx.engine.create_edge(&edge).await.unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                sqlx::query(
                    r#"INSERT INTO follows ("from", "to", weight) VALUES ($1, $2, $3)
               ON CONFLICT ("from", "to") DO UPDATE SET weight = EXCLUDED.weight"#,
                )
                .bind(ctx.raw_from)
                .bind(ctx.raw_to)
                .bind(1_i64)
                .execute(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let model = orm::follows::ActiveModel {
                    from_id: sea_orm::ActiveValue::Set(ctx.raw_from),
                    to_id: sea_orm::ActiveValue::Set(ctx.raw_to),
                    weight: sea_orm::ActiveValue::Set(1_i64),
                };
                orm::follows::Entity::insert(model)
                    .on_conflict(
                        OnConflict::columns([
                            orm::follows::Column::FromId,
                            orm::follows::Column::ToId,
                        ])
                        .update_column(orm::follows::Column::Weight)
                        .to_owned(),
                    )
                    .exec(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

/// Multi-pivot: "for EVERY user, who do they follow?"
///
/// ousia   — 2 queries total (fetch users + one batch JOIN with ANY)
/// raw N+1 — N queries, one JOIN per pivot
/// raw 2q  — 2 queries, manual IN-Rust grouping
fn bench_preload_multi_pivot_forward(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("preload_multi_pivot_forward");

    // ousia: 1 query for all pivots + 1 batch JOIN
    group.bench_function("ousia_batch_2q", |b| {
        b.iter(|| {
            run!({
                let _: Vec<(BenchUser, Vec<BenchUser>)> = ctx
                    .engine
                    .preload_objects::<BenchUser>(Query::default())
                    .edge::<BenchFollow, BenchUser>()
                    .collect()
                    .await
                    .unwrap();
            })
        })
    });

    // raw N+1: one JOIN query per pivot
    group.bench_function("raw_n_plus_1", |b| {
        b.iter(|| {
            run!({
                for &pivot in &ctx.raw_n1_user_ids {
                    let _: Vec<RawUser> = sqlx::query_as(
                        r#"SELECT u.id, u.username, u.email, u.display_name, u.score, u.active,
                                  u.created_at, u.updated_at
                           FROM follows f
                           INNER JOIN users u ON u.id = f."to"
                           WHERE f."from" = $1"#,
                    )
                    .bind(pivot)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
                }
            })
        })
    });

    // raw 2q: proper batch — fetch all users, then one batch JOIN, group in Rust
    group.bench_function("raw_batch_2q", |b| {
        b.iter(|| {
            run!({
                let pivots: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users").fetch_all(&ctx.raw_pool).await.unwrap();

                let pivot_ids: Vec<Uuid> = pivots.iter().map(|u| u.id).collect();
                let rows: Vec<RawEdgeUser> = sqlx::query_as(
                    r#"SELECT f."from" AS pivot_id, u.id, u.username, u.email, u.display_name,
                              u.score, u.active, u.created_at, u.updated_at
                       FROM follows f
                       INNER JOIN users u ON u.id = f."to"
                       WHERE f."from" = ANY($1)"#,
                )
                .bind(&pivot_ids)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();

                use std::collections::HashMap;
                let mut map: HashMap<Uuid, Vec<RawEdgeUser>> = HashMap::new();
                for row in rows {
                    map.entry(row.pivot_id).or_default().push(row);
                }
                let _: Vec<(RawUser, Vec<RawEdgeUser>)> = pivots
                    .into_iter()
                    .map(|u| {
                        let ch = map.remove(&u.id).unwrap_or_default();
                        (u, ch)
                    })
                    .collect();
            })
        })
    });

    group.finish();
}

/// Multi-pivot: count outgoing follows per user.
///
/// ousia   — 2 queries (fetch users + one GROUP BY with ANY)
/// raw N+1 — N COUNT queries, one per pivot
/// raw 2q  — 2 queries, manual zero-filling in Rust
fn bench_preload_multi_pivot_count(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("preload_multi_pivot_count");

    group.bench_function("ousia_batch_2q", |b| {
        b.iter(|| {
            run!({
                let _: Vec<(BenchUser, u64)> = ctx
                    .engine
                    .preload_objects::<BenchUser>(Query::default())
                    .edge::<BenchFollow, BenchUser>()
                    .count()
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_n_plus_1", |b| {
        b.iter(|| {
            run!({
                for &pivot in &ctx.raw_n1_user_ids {
                    let _: i64 =
                        sqlx::query_scalar(r#"SELECT COUNT(*) FROM follows WHERE "from" = $1"#)
                            .bind(pivot)
                            .fetch_one(&ctx.raw_pool)
                            .await
                            .unwrap();
                }
            })
        })
    });

    group.bench_function("raw_batch_2q", |b| {
        b.iter(|| {
            run!({
                let pivots: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users").fetch_all(&ctx.raw_pool).await.unwrap();

                let pivot_ids: Vec<Uuid> = pivots.iter().map(|u| u.id).collect();
                let counts: Vec<RawEdgeCount> = sqlx::query_as(
                    r#"SELECT "from" AS from_id, COUNT(*)::bigint AS count
                       FROM follows
                       WHERE "from" = ANY($1)
                       GROUP BY "from""#,
                )
                .bind(&pivot_ids)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();

                use std::collections::HashMap;
                let count_map: HashMap<Uuid, i64> =
                    counts.into_iter().map(|r| (r.from_id, r.count)).collect();
                let _: Vec<(RawUser, i64)> = pivots
                    .into_iter()
                    .map(|u| {
                        let c = count_map.get(&u.id).copied().unwrap_or(0);
                        (u, c)
                    })
                    .collect();
            })
        })
    });

    group.finish();
}

fn run_all(c: &mut Criterion) {
    bench_query_edges(c);
    bench_reverse_edges(c);
    bench_count_edges(c);
    bench_edge_filter(c);
    bench_preload_forward(c);
    bench_preload_reverse(c);
    bench_preload_multi_pivot_forward(c);
    bench_preload_multi_pivot_count(c);
    bench_create_edge(c);
}

criterion_group! {
    name = ousia_edges;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_edges);
