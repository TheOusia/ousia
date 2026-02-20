//! Benchmark: Graph edge operations
//!
//! Covers:
//!   - create_edge
//!   - query_edges (forward)
//!   - query_reverse_edges
//!   - count_edges / count_reverse_edges
//!   - preload_object (edge + object join in one call)
//!   - preload with edge_eq filter
//!   - Raw SQL equivalents for each

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{
    EdgeMeta, EdgeQuery, Engine, ObjectMeta, ObjectOwnership, Query,
    adapters::postgres::PostgresAdapter,
};
use ousia_bench::{BenchFollow, BenchUser, PostStatus, seed_follows, seed_posts, seed_users};
use sqlx::PgPool;
use uuid::Uuid;

struct Ctx {
    _container: ousia_bench::Container,
    pool: PgPool,
    engine: Engine,
    user_ids: Vec<Uuid>,
    // Pick a node that has both forward and reverse edges
    pivot: Uuid,
}
unsafe impl Sync for Ctx {}

static STATE: ousia_bench::BenchHandle<Ctx> = ousia_bench::BenchHandle::new();

fn state() -> &'static (tokio::runtime::Runtime, Ctx) {
    STATE.get_or_init(|| {
        let rt = ousia_bench::mt_rt();
        let ctx = rt.block_on(setup_inner());
        (rt, ctx)
    })
}

macro_rules! run {
    ($e:expr) => {
        state().0.block_on(async { $e })
    };
}

async fn setup_inner() -> Ctx {
    let (_container, pool) = ousia_bench::start_postgres().await;

    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(pool.clone())));

    let user_ids = seed_users(&pool, &engine, 100).await;
    seed_follows(&engine, &user_ids).await;

    // Create a raw follow table for comparison queries
    sqlx::query(
        r#"
            CREATE TABLE IF NOT EXISTS bench_follows (
                "from" UUID NOT NULL,
                "to"   UUID NOT NULL,
                weight BIGINT NOT NULL DEFAULT 1,
                PRIMARY KEY ("from", "to")
            )
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bf_from ON bench_follows(\"from\")")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bf_to ON bench_follows(\"to\")")
        .execute(&pool)
        .await
        .unwrap();

    // Mirror follow data
    for (i, &uid) in user_ids.iter().enumerate() {
        for j in 1..=3_usize {
            let to = user_ids[(i + j) % user_ids.len()];
            sqlx::query(
                r#"INSERT INTO bench_follows ("from", "to", weight)
                       VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"#,
            )
            .bind(uid)
            .bind(to)
            .bind(j as i64)
            .execute(&pool)
            .await
            .unwrap();
        }
    }

    let pivot = user_ids[10];
    Ctx {
        _container,
        pool,
        engine,
        user_ids,
        pivot,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// query_edges (forward)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_query_edges(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_edges_forward");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_edges(ctx.pivot, EdgeQuery::default())
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"SELECT "from", "to", weight FROM bench_follows WHERE "from" = $1"#,
                )
                .bind(ctx.pivot)
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// query_reverse_edges
// ─────────────────────────────────────────────────────────────────────────────

fn bench_reverse_edges(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_edges_reverse");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_reverse_edges(ctx.pivot, EdgeQuery::default())
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"SELECT "from", "to", weight FROM bench_follows WHERE "to" = $1"#,
                )
                .bind(ctx.pivot)
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// count_edges
// ─────────────────────────────────────────────────────────────────────────────

fn bench_count_edges(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("count_edges");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: u64 = ctx
                    .engine
                    .count_edges::<BenchFollow>(ctx.pivot, None)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: i64 =
                    sqlx::query_scalar(r#"SELECT COUNT(*) FROM bench_follows WHERE "from" = $1"#)
                        .bind(ctx.pivot)
                        .fetch_one(&ctx.pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// edge + filter  (weight = 2)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_edge_filter(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_edges_with_filter");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchFollow> = ctx
                    .engine
                    .query_edges(
                        ctx.pivot,
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
                let _ =
                    sqlx::query(r#"SELECT * FROM bench_follows WHERE "from" = $1 AND weight = $2"#)
                        .bind(ctx.pivot)
                        .bind(2_i64)
                        .fetch_all(&ctx.pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// preload_object  (traverse edges -> hydrate target objects)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_preload(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("preload_object_graph_traverse");

    // Ousia: single preload_object call
    group.bench_function("ousia_preload", |b| {
        b.iter(|| {
            run!({
                let _users: Vec<BenchUser> = ctx
                    .engine
                    .preload_object::<BenchUser>(ctx.pivot)
                    .edge::<BenchFollow, BenchUser>()
                    .collect()
                    .await
                    .unwrap();
            })
        })
    });

    // Raw SQL: JOIN edges + objects in one query
    group.bench_function("raw_sql_join", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"
                    SELECT o.id, o.data, o.index_meta, o.type, o.owner,
                           o.created_at, o.updated_at
                    FROM bench_follows f
                    INNER JOIN objects o ON o.id = f."to" AND o.type = 'BenchUser'
                    WHERE f."from" = $1
                    "#,
                )
                .bind(ctx.pivot)
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    // Ousia: manual two-step (query edges then bulk fetch)
    group.bench_function("ousia_two_step", |b| {
        b.iter(|| {
            run!({
                let follows: Vec<BenchFollow> = ctx
                    .engine
                    .query_edges(ctx.pivot, EdgeQuery::default())
                    .await
                    .unwrap();
                let ids: Vec<Uuid> = follows
                    .iter()
                    .map(|f| {
                        use ousia::EdgeMetaTrait;
                        f.to()
                    })
                    .collect();
                let _: Vec<BenchUser> = ctx.engine.fetch_objects(ids).await.unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// create_edge  (insert throughput)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_create_edge(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("create_edge");
    let n = ctx.user_ids.len();

    // Use two users that may not already be connected
    let (from, to) = (ctx.user_ids[50], ctx.user_ids[99]);

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let edge = BenchFollow {
                    _meta: EdgeMeta::new(from, to),
                    weight: 1,
                };
                // upsert semantics — bench the full insert path
                ctx.engine.create_edge(&edge).await.unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx_upsert", |b| {
        b.iter(|| {
            run!({
                sqlx::query(
                    r#"INSERT INTO bench_follows ("from", "to", weight)
                       VALUES ($1, $2, $3)
                       ON CONFLICT ("from","to") DO UPDATE SET weight = EXCLUDED.weight"#,
                )
                .bind(from)
                .bind(to)
                .bind(1_i64)
                .execute(&ctx.pool)
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
