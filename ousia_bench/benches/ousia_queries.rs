//! Benchmark: Query patterns — composite filters, OR conditions,
//! cursor pagination, multi-sort, and full table scans.
//!
//! Each benchmark compares Ousia's typed query builder against equivalent
//! hand-written sqlx queries with the same index coverage.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{Engine, ObjectMeta, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchPost, BenchUser, PostStatus, seed_posts, seed_users};
use sqlx::PgPool;
use uuid::Uuid;

struct Ctx {
    _container: ousia_bench::Container,
    pool: PgPool,
    engine: Engine,
    user_ids: Vec<Uuid>,
    owner_id: Uuid,
    cursor_mid: Uuid, // ID roughly in the middle of the dataset for cursor tests
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

    let user_ids = seed_users(&pool, &engine, 500).await;
    let _ = seed_posts(&pool, &engine, &user_ids[..50], 20).await;

    let owner_id = user_ids[0];
    let cursor_mid = user_ids[250];

    Ctx {
        _container,
        pool,
        engine,
        user_ids,
        owner_id,
        cursor_mid,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// two-field AND filter
// ─────────────────────────────────────────────────────────────────────────────

fn bench_and_filter(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_and_two_fields");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_eq(&BenchUser::FIELDS.active, true)
                            .where_gt(&BenchUser::FIELDS.score, 3000_i64),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    "SELECT * FROM bench_users \
                     WHERE (index_meta->>'active')::boolean = $1 \
                     AND (index_meta->>'score')::bigint > $2",
                )
                .bind(true)
                .bind(3000_i64)
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    // Bonus: same filter on raw normalized table
    group.bench_function("raw_table_normalized", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM bench_users WHERE active = true AND score > $1")
                    .bind(3000_i64)
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// OR conditions
// ─────────────────────────────────────────────────────────────────────────────

fn bench_or_filter(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_or_condition");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_eq(&BenchUser::FIELDS.username, "user_000010")
                            .or_eq(&BenchUser::FIELDS.username, "user_000020")
                            .or_eq(&BenchUser::FIELDS.username, "user_000030"),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    "SELECT * FROM bench_users \
                     WHERE username = ANY($1)",
                )
                .bind(vec!["user_000010", "user_000020", "user_000030"])
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// cursor-based pagination (page 1, page N)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_cursor_pagination(c: &mut Criterion) {
    let ctx = &state().1;
    let rt = &state().0;
    let mut group = c.benchmark_group("cursor_pagination");

    for page_size in [10_u32, 50, 100] {
        // First page (no cursor)
        group.bench_with_input(
            BenchmarkId::new("ousia_page1", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _: Vec<BenchUser> = ctx
                            .engine
                            .query_objects(Query::default().with_limit(ps))
                            .await
                            .unwrap();
                    })
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("raw_page1", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _ = sqlx::query(
                            "SELECT id, type, owner, created_at, updated_at, data, index_meta \
                             FROM objects WHERE type = 'BenchUser' \
                             ORDER BY id DESC LIMIT $1",
                        )
                        .bind(ps as i64)
                        .fetch_all(&ctx.pool)
                        .await
                        .unwrap();
                    })
                })
            },
        );

        // Mid-set page (with cursor)
        group.bench_with_input(
            BenchmarkId::new("ousia_mid_page", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _: Vec<BenchUser> = ctx
                            .engine
                            .query_objects(
                                Query::default().with_cursor(ctx.cursor_mid).with_limit(ps),
                            )
                            .await
                            .unwrap();
                    })
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("raw_mid_page", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _ = sqlx::query(
                            "SELECT id, type, owner, created_at, updated_at, data, index_meta \
                             FROM objects \
                             WHERE type = 'BenchUser' AND id < $1 \
                             ORDER BY id DESC LIMIT $2",
                        )
                        .bind(ctx.cursor_mid)
                        .bind(ps as i64)
                        .fetch_all(&ctx.pool)
                        .await
                        .unwrap();
                    })
                })
            },
        );
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// multi-sort
// ─────────────────────────────────────────────────────────────────────────────

fn bench_multi_sort(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("multi_sort");

    group.bench_function("ousia_two_sort", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .sort_desc(&BenchUser::FIELDS.score)
                            .sort_asc(&BenchUser::FIELDS.username)
                            .with_limit(50),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx_two_sort", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"SELECT id, type, owner, created_at, updated_at, data, index_meta
                     FROM objects WHERE type = 'BenchUser' AND owner > $1
                     ORDER BY (index_meta->>'score')::bigint DESC,
                               (index_meta->>'username')::text ASC
                     LIMIT 50"#,
                )
                .bind(uuid::Uuid::nil())
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// full scan (no filters)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_full_scan(c: &mut Criterion) {
    let ctx = &state().1;
    let rt = &state().0;
    let mut group = c.benchmark_group("full_scan_with_limit");

    for limit in [100_u32, 500] {
        group.bench_with_input(BenchmarkId::new("ousia", limit), &limit, |b, &l| {
            b.iter(|| {
                rt.block_on(async {
                    let _: Vec<BenchUser> = ctx
                        .engine
                        .query_objects(Query::default().with_limit(l))
                        .await
                        .unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("raw_sqlx", limit), &limit, |b, &l| {
            b.iter(|| {
                rt.block_on(async {
                    let _ = sqlx::query(
                        "SELECT id, type, owner, created_at, updated_at, data, index_meta \
                         FROM objects WHERE type = 'BenchUser' \
                         ORDER BY id DESC LIMIT $1",
                    )
                    .bind(l as i64)
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
                })
            })
        });
    }

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// create_object throughput (single insert)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_create_object(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("create_object");

    group.bench_function("ousia_no_unique", |b| {
        b.iter(|| {
            run!({
                let mut post = BenchPost::default();
                post.title = "bench post".to_string();
                ctx.engine.create_object(&post).await.unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx_insert", |b| {
        b.iter(|| run!({
                let id = uuid::Uuid::now_v7();
                let now = chrono::Utc::now();
                sqlx::query(
                    r#"INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
                )
                .bind(id)
                .bind("BenchPost")
                .bind(ousia::system_owner())
                .bind(now)
                .bind(now)
                .bind(serde_json::json!({"title": "bench post", "body": "", "status": "Draft", "view_count": 0, "tags": []}))
                .bind(serde_json::json!({"title": "bench post", "status": "draft", "view_count": 0, "tags": []}))
                .execute(&ctx.pool)
                .await
                .unwrap();
        }))
    });

    group.finish();
}

fn run_all(c: &mut Criterion) {
    bench_and_filter(c);
    bench_or_filter(c);
    bench_cursor_pagination(c);
    bench_multi_sort(c);
    bench_full_scan(c);
    bench_create_object(c);
}

criterion_group! {
    name = ousia_queries;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_queries);
