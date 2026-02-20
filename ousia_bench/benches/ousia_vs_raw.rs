//! Benchmark: Ousia ORM vs optimised raw sqlx queries
//!
//! Covers:
//!   - Single object fetch by PK
//!   - Equality filter query (indexed field)
//!   - Range + sort query with LIMIT
//!   - Owner-scoped fetch (all rows for an owner)
//!   - COUNT aggregate
//!   - IN / bulk fetch
//!
//! All raw-SQL queries use the same indexes that Ousia relies on,
//! so this is a pure serialisation/abstraction overhead comparison.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{Engine, ObjectMeta, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchPost, BenchUser, PostStatus, seed_posts, seed_users};
use sqlx::PgPool;
use uuid::Uuid;
// ─────────────────────────────────────────────────────────────────────────────
// Shared state (one container / engine for the whole binary)
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    _container: ousia_bench::Container,
    pool: PgPool,
    engine: Engine,
    user_ids: Vec<Uuid>,
    post_ids: Vec<Uuid>,
    sample_user_id: Uuid,
    sample_post_id: Uuid,
    owner_id: Uuid,
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
    adapter.init_schema().await.expect("init schema");

    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(pool.clone())));

    // Seed 200 users + 10 posts each = 2 000 posts
    let user_ids = seed_users(&pool, &engine, 200).await;
    let owner_id = user_ids[0];
    let post_ids = seed_posts(&pool, &engine, &user_ids[..20], 10).await;

    let sample_user_id = user_ids[42];
    let sample_post_id = post_ids[7];

    Ctx {
        _container,
        pool,
        engine,
        user_ids,
        post_ids,
        sample_user_id,
        sample_post_id,
        owner_id,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// fetch_by_pk
// ──────────────────────────────────────────
// ───────────────────────────────────

fn bench_fetch_pk(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("fetch_by_pk");

    // Ousia
    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Option<BenchUser> =
                    ctx.engine.fetch_object(ctx.sample_user_id).await.unwrap();
            })
        })
    });

    // Raw sqlx
    group.bench_function("raw_sqlx", |b| {
        b.iter(|| { run!({
                let _row = sqlx::query(
                    "SELECT id, username, email, display_name, score, active, created_at, updated_at \
                     FROM bench_users WHERE id = $1"
                )
                .bind(ctx.sample_user_id)
                .fetch_optional(&ctx.pool)
                .await
                .unwrap();
        }) })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// eq_filter  (username = ?)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_eq_filter(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("eq_filter_indexed");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default().where_eq(&BenchUser::FIELDS.username, "user_000042"),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM bench_users WHERE username = $1")
                    .bind("user_000042")
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// range + sort + limit  (score > N ORDER BY score DESC LIMIT 20)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_range_sort_limit(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("range_sort_limit_20");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_gt(&BenchUser::FIELDS.score, 5000_i64)
                            .sort_desc(&BenchUser::FIELDS.score)
                            .with_limit(20),
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
                    "SELECT * FROM bench_users WHERE score > $1 ORDER BY score DESC LIMIT 20",
                )
                .bind(5000_i64)
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// owner_scan (all posts for one owner)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_owner_scan(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("owner_scan");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchPost> = ctx.engine.fetch_owned_objects(ctx.owner_id).await.unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM bench_posts WHERE owner_id = $1")
                    .bind(ctx.owner_id)
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// count aggregate
// ─────────────────────────────────────────────────────────────────────────────

fn bench_count(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("count_aggregate");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: u64 = ctx
                    .engine
                    .count_objects::<BenchPost>(Some(
                        Query::new(ctx.owner_id)
                            .where_eq(&BenchPost::FIELDS.status, PostStatus::Published),
                    ))
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM bench_posts WHERE owner_id = $1 AND status = $2",
                )
                .bind(ctx.owner_id)
                .bind("published")
                .fetch_one(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// bulk_fetch  (IN clause)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_bulk_fetch(c: &mut Criterion) {
    let ctx = &state().1;
    let rt = &state().0;
    let mut group = c.benchmark_group("bulk_fetch");

    // Take 50 IDs for the bulk test
    for n in [10_usize, 50, 100] {
        let ids: Vec<Uuid> = ctx.user_ids[..n.min(ctx.user_ids.len())].to_vec();

        group.bench_with_input(BenchmarkId::new("ousia", n), &ids, |b, ids| {
            b.iter(|| {
                rt.block_on(async {
                    let _: Vec<BenchUser> = ctx.engine.fetch_objects(ids.clone()).await.unwrap();
                })
            })
        });

        let ids_clone = ids.clone();
        group.bench_with_input(BenchmarkId::new("raw_sqlx", n), &ids_clone, |b, ids| {
            b.iter(|| {
                rt.block_on(async {
                    let _ = sqlx::query("SELECT * FROM bench_users WHERE id = ANY($1)")
                        .bind(ids.as_slice())
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
// join: posts WITH user display_name (raw join vs ousia two-step)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_join(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("join_posts_users");

    // Raw: single JOIN query
    group.bench_function("raw_sql_join", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"
                    SELECT p.id, p.title, p.status, p.view_count,
                           u.username, u.display_name
                    FROM bench_posts p
                    INNER JOIN bench_users u ON u.id = p.owner_id
                    WHERE p.status = 'published'
                    ORDER BY p.view_count DESC
                    LIMIT 20
                    "#,
                )
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    // Ousia: query posts, then fetch owners
    group.bench_function("ousia_two_step", |b| {
        b.iter(|| {
            run!({
                let posts: Vec<BenchPost> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_eq(&BenchPost::FIELDS.status, PostStatus::Published)
                            .sort_desc(&BenchPost::FIELDS.view_count)
                            .with_limit(20),
                    )
                    .await
                    .unwrap();

                let owner_ids: Vec<Uuid> = posts.iter().map(|p| p.owner()).collect();
                let _owners: Vec<BenchUser> = ctx.engine.fetch_objects(owner_ids).await.unwrap();
            })
        })
    });

    // Ousia: preload_object (single traversal)
    group.bench_function("ousia_preload", |b| {
        b.iter(|| {
            run!({
                // Fetch published posts for a known owner via preload
                let _posts: Vec<BenchPost> =
                    ctx.engine.fetch_owned_objects(ctx.owner_id).await.unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// stored procedure / complex view vs Ousia
//
// Raw: a single SQL query that uses a CTE to rank posts by view_count per owner
// Ousia: query + in-memory grouping (the honest apples-to-apples comparison)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_cte_vs_ousia(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("cte_ranked_posts_vs_ousia");

    // Raw CTE (window function: top-3 posts per owner by view_count)
    group.bench_function("raw_sql_cte", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"
                    WITH ranked AS (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY owner_id
                                   ORDER BY view_count DESC
                               ) AS rn
                        FROM bench_posts
                        WHERE status = 'published'
                    )
                    SELECT id, owner_id, title, view_count, rn
                    FROM ranked
                    WHERE rn <= 3
                    ORDER BY owner_id, rn
                    "#,
                )
                .fetch_all(&ctx.pool)
                .await
                .unwrap();
            })
        })
    });

    // Ousia equivalent: query all published posts, group in Rust
    group.bench_function("ousia_rust_grouping", |b| {
        b.iter(|| {
            run!({
                let posts: Vec<BenchPost> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_eq(&BenchPost::FIELDS.status, PostStatus::Published)
                            .sort_desc(&BenchPost::FIELDS.view_count),
                    )
                    .await
                    .unwrap();

                // Group top-3 per owner in Rust
                use std::collections::HashMap;
                let mut grouped: HashMap<Uuid, Vec<&BenchPost>> = HashMap::new();
                for post in &posts {
                    let entry = grouped.entry(post.owner()).or_default();
                    if entry.len() < 3 {
                        entry.push(post);
                    }
                }
                let _result: Vec<_> = grouped.into_values().flatten().collect();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// array contains (tags @> ARRAY['tag_1'])
// ─────────────────────────────────────────────────────────────────────────────

fn bench_array_contains(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("array_contains_tag");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchPost> = ctx
                    .engine
                    .query_objects(
                        Query::default().where_contains(&BenchPost::FIELDS.tags, vec!["tag_1"]),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sql_gin", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM bench_posts WHERE tags @> ARRAY['tag_1']")
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// begins_with  (ILIKE 'user_000%')
// ─────────────────────────────────────────────────────────────────────────────

fn bench_begins_with(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("begins_with_prefix");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(
                        Query::default()
                            .where_begins_with(&BenchUser::FIELDS.username, "user_000")
                            .with_limit(50),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sql_ilike", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM bench_users WHERE username ILIKE $1 LIMIT 50")
                    .bind("user_000%")
                    .fetch_all(&ctx.pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Glue
// ─────────────────────────────────────────────────────────────────────────────

fn run_all(c: &mut Criterion) {
    bench_fetch_pk(c);
    bench_eq_filter(c);
    bench_range_sort_limit(c);
    bench_owner_scan(c);
    bench_count(c);
    bench_bulk_fetch(c);
    bench_join(c);
    bench_cte_vs_ousia(c);
    bench_array_contains(c);
    bench_begins_with(c);
}

criterion_group! {
    name = ousia_vs_raw;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_vs_raw);
