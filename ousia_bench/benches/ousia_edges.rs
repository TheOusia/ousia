//! Benchmark: Graph edge operations
//!
//! Two completely independent Postgres databases — one for Ousia, one raw.
//! No shared IDs, no shared seeders, no cross-contamination.
//!
//!   • raw_*   — a plain `follows` table with normalised columns, pure sqlx.
//!   • ousia_* — Ousia edge schema, populated entirely through the Engine API.
//!
//! Covers:
//!   - query_edges (forward)
//!   - query_reverse_edges
//!   - count_edges
//!   - query_edges with field filter
//!   - preload_object (edge + object graph traversal)
//!   - create_edge

use criterion::{Criterion, criterion_group, criterion_main};
use ousia::{EdgeMeta, EdgeQuery, Engine, ObjectMeta, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchFollow, BenchUser};
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    _ousia_container: ousia_bench::Container,
    _raw_container: ousia_bench::Container,

    // Ousia side
    engine: Engine,
    ousia_pivot: Uuid, // a user with both forward and reverse edges
    ousia_from: Uuid,  // for create_edge bench
    ousia_to: Uuid,

    // Raw side — its own pool, its own IDs
    raw_pool: PgPool,
    raw_pivot: Uuid,
    raw_from: Uuid,
    raw_to: Uuid,
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
    let (_ousia_container, ousia_pool) = ousia_bench::start_postgres().await;
    let (_raw_container, raw_pool) = ousia_bench::start_postgres().await;

    // --- Ousia side ---
    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(ousia_pool.clone())));

    let ousia_user_ids = seed_ousia_users(&engine, 100).await;
    seed_ousia_follows(&engine, &ousia_user_ids).await;

    let ousia_pivot = ousia_user_ids[10];
    let ousia_from = ousia_user_ids[50];
    let ousia_to = ousia_user_ids[99];

    // --- Raw side ---
    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = seed_raw_users(&raw_pool, 100).await;
    seed_raw_follows(&raw_pool, &raw_user_ids).await;

    let raw_pivot = raw_user_ids[10];
    let raw_from = raw_user_ids[50];
    let raw_to = raw_user_ids[99];

    Ctx {
        _ousia_container,
        _raw_container,
        engine,
        ousia_pivot,
        ousia_from,
        ousia_to,
        raw_pool,
        raw_pivot,
        raw_from,
        raw_to,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw schema
// ─────────────────────────────────────────────────────────────────────────────

async fn setup_raw_schema(pool: &PgPool) {
    sqlx::query(
        r#"CREATE TABLE users (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid()
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
// Raw seeders
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_raw_users(pool: &PgPool, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let id: Uuid = sqlx::query_scalar("INSERT INTO users DEFAULT VALUES RETURNING id")
            .fetch_one(pool)
            .await
            .unwrap();
        ids.push(id);
    }
    ids
}

async fn seed_raw_follows(pool: &PgPool, user_ids: &[Uuid]) {
    let n = user_ids.len();
    for i in 0..n {
        for j in 1..=3_usize {
            let to = user_ids[(i + j) % n];
            sqlx::query(
                r#"INSERT INTO follows ("from", "to", weight) VALUES ($1, $2, $3)
                   ON CONFLICT DO NOTHING"#,
            )
            .bind(user_ids[i])
            .bind(to)
            .bind(j as i64)
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ousia seeders
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_ousia_users(engine: &Engine, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let mut user = BenchUser::default();
        user.username = format!("user_{:06}", i);
        user.email = format!("user_{:06}@bench.test", i);
        user.display_name = format!("User {}", i);
        user.score = (i as i64) * 7 % 10_000;
        user.active = i % 3 != 0;
        engine.create_object(&user).await.unwrap();
        ids.push(user.id());
    }
    ids
}

async fn seed_ousia_follows(engine: &Engine, user_ids: &[Uuid]) {
    let n = user_ids.len();
    for i in 0..n {
        for j in 1..=3_usize {
            let to = user_ids[(i + j) % n];
            let edge = BenchFollow {
                _meta: EdgeMeta::new(user_ids[i], to),
                weight: j as i64,
            };
            engine.create_edge(&edge).await.unwrap();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_query_edges(c: &mut Criterion) {
    let ctx = &state().1;
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
                let _ =
                    sqlx::query(r#"SELECT "from", "to", weight FROM follows WHERE "from" = $1"#)
                        .bind(ctx.raw_pivot)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_reverse_edges(c: &mut Criterion) {
    let ctx = &state().1;
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
                let _ = sqlx::query(r#"SELECT "from", "to", weight FROM follows WHERE "to" = $1"#)
                    .bind(ctx.raw_pivot)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_count_edges(c: &mut Criterion) {
    let ctx = &state().1;
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

    group.finish();
}

fn bench_edge_filter(c: &mut Criterion) {
    let ctx = &state().1;
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
                let _ = sqlx::query(r#"SELECT * FROM follows WHERE "from" = $1 AND weight = $2"#)
                    .bind(ctx.raw_pivot)
                    .bind(2_i64)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_preload(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("preload_object_graph_traverse");

    // Ousia: single preload call (edge traversal + object hydration)
    group.bench_function("ousia_preload", |b| {
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

    // Raw: JOIN follows → users in one query
    group.bench_function("raw_sql_join", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"SELECT u.id FROM follows f
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

    group.finish();
}

fn bench_create_edge(c: &mut Criterion) {
    let ctx = &state().1;
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

    group.finish();
}

fn run_all(c: &mut Criterion) {
    bench_query_edges(c);
    bench_reverse_edges(c);
    bench_count_edges(c);
    bench_edge_filter(c);
    bench_preload(c);
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
