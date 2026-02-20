//! Benchmark: Ousia ORM vs optimised raw sqlx queries
//!
//! Two completely independent Postgres databases are spun up side-by-side.
//! They share no IDs, no schema knowledge, no seeders.
//!
//!   • raw_*   — vanilla normalised Postgres schema, pure sqlx, no Ousia code.
//!   • ousia_* — Ousia schema, populated entirely through the Engine API.
//!
//! We measure the same *logical operation* on each side.
//!
//! Covers:
//!   - Single object fetch by PK
//!   - Equality filter (indexed field)
//!   - Range + sort + LIMIT
//!   - Owner-scoped fetch
//!   - COUNT aggregate
//!   - Bulk fetch (ANY)
//!   - Two-step join (posts → owners)
//!   - CTE window function vs in-memory grouping
//!   - Array contains (GIN index)
//!   - Prefix search (ILIKE)
//!   - Multi-sort

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{Engine, ObjectMeta, ObjectOwnership, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchPost, BenchUser, PostStatus};
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    _ousia_container: ousia_bench::Container,
    _raw_container: ousia_bench::Container,

    // Ousia side — Engine + representative IDs from that database
    engine: Engine,
    ousia_sample_user_id: Uuid,
    ousia_owner_id: Uuid,
    ousia_bulk_user_ids: Vec<Uuid>,

    // Raw side — its own pool + its own IDs, no relation to the Ousia side
    raw_pool: PgPool,
    raw_sample_user_id: Uuid,
    raw_owner_id: Uuid,
    raw_bulk_user_ids: Vec<Uuid>,
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
    // Two fully independent containers — nothing shared.
    let (_ousia_container, ousia_pool) = ousia_bench::start_postgres().await;
    let (_raw_container, raw_pool) = ousia_bench::start_postgres().await;

    // --- Ousia side ---
    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(ousia_pool.clone())));

    let ousia_user_ids = seed_ousia_users(&engine, 200).await;
    seed_ousia_posts(&engine, &ousia_user_ids[..20], 10).await;

    let ousia_sample_user_id = ousia_user_ids[42];
    let ousia_owner_id = ousia_user_ids[0];
    let ousia_bulk_user_ids = ousia_user_ids[..100].to_vec();

    // --- Raw side ---
    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = seed_raw_users(&raw_pool, 200).await;
    seed_raw_posts(&raw_pool, &raw_user_ids[..20], 10).await;

    let raw_sample_user_id = raw_user_ids[42];
    let raw_owner_id = raw_user_ids[0];
    let raw_bulk_user_ids = raw_user_ids[..100].to_vec();

    Ctx {
        _ousia_container,
        _raw_container,
        engine,
        ousia_sample_user_id,
        ousia_owner_id,
        ousia_bulk_user_ids,
        raw_pool,
        raw_sample_user_id,
        raw_owner_id,
        raw_bulk_user_ids,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw schema  (no Ousia types, no Ousia tables)
// ─────────────────────────────────────────────────────────────────────────────

async fn setup_raw_schema(pool: &PgPool) {
    sqlx::query(
        r#"CREATE TABLE users (
            id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            username     TEXT        NOT NULL,
            email        TEXT        NOT NULL,
            display_name TEXT        NOT NULL,
            score        BIGINT      NOT NULL DEFAULT 0,
            active       BOOLEAN     NOT NULL DEFAULT true,
            owner_id     UUID        NOT NULL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    for ddl in [
        "CREATE UNIQUE INDEX idx_users_username ON users(username)",
        "CREATE INDEX idx_users_email    ON users(email)",
        "CREATE INDEX idx_users_score    ON users(score)",
        "CREATE INDEX idx_users_active   ON users(active)",
        "CREATE INDEX idx_users_owner    ON users(owner_id)",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }

    sqlx::query(
        r#"CREATE TABLE posts (
            id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id    UUID        NOT NULL,
            title       TEXT        NOT NULL,
            body        TEXT        NOT NULL,
            status      TEXT        NOT NULL DEFAULT 'draft',
            view_count  BIGINT      NOT NULL DEFAULT 0,
            tags        TEXT[]      NOT NULL DEFAULT '{}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    for ddl in [
        "CREATE INDEX idx_posts_owner      ON posts(owner_id)",
        "CREATE INDEX idx_posts_status     ON posts(status)",
        "CREATE INDEX idx_posts_view_count ON posts(view_count)",
        "CREATE INDEX idx_posts_tags       ON posts USING GIN(tags)",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw seeders  (pure sqlx — no Engine, no Ousia types)
// ─────────────────────────────────────────────────────────────────────────────

/// Returns the generated UUIDs so benchmarks can use representative IDs.
async fn seed_raw_users(pool: &PgPool, n: usize) -> Vec<Uuid> {
    let owner_id = Uuid::now_v7(); // sentinel; mirrors Ousia's system owner concept
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id: Uuid = sqlx::query_scalar(
            r#"INSERT INTO users (username, email, display_name, score, active, owner_id)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING id"#,
        )
        .bind(format!("user_{:06}", i))
        .bind(format!("user_{:06}@bench.test", i))
        .bind(format!("User {}", i))
        .bind((i as i64) * 7 % 10_000)
        .bind(i % 3 != 0)
        .bind(owner_id)
        .fetch_one(pool)
        .await
        .unwrap();
        ids.push(id);
    }
    ids
}

async fn seed_raw_posts(pool: &PgPool, owner_ids: &[Uuid], posts_per_user: usize) {
    for (oi, &owner_id) in owner_ids.iter().enumerate() {
        for p in 0..posts_per_user {
            let status = match p % 3 {
                0 => "draft",
                1 => "published",
                _ => "archived",
            };
            let tags = vec![format!("tag_{}", p % 5), format!("cat_{}", oi % 3)];
            sqlx::query(
                r#"INSERT INTO posts (owner_id, title, body, status, view_count, tags)
                   VALUES ($1, $2, $3, $4, $5, $6)"#,
            )
            .bind(owner_id)
            .bind(format!("Post {} by owner {}", p, oi))
            .bind("Lorem ipsum dolor sit amet".repeat(4))
            .bind(status)
            .bind((p as i64) * 13 % 50_000)
            .bind(&tags)
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Ousia seeders  (Engine only — raw pool never touched)
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

async fn seed_ousia_posts(engine: &Engine, owner_ids: &[Uuid], posts_per_user: usize) {
    for (oi, &owner_id) in owner_ids.iter().enumerate() {
        for p in 0..posts_per_user {
            let mut post = BenchPost::default();
            post.set_owner(owner_id);
            post.title = format!("Post {} by owner {}", p, oi);
            post.body = "Lorem ipsum dolor sit amet".repeat(4);
            post.status = match p % 3 {
                0 => PostStatus::Draft,
                1 => PostStatus::Published,
                _ => PostStatus::Archived,
            };
            post.view_count = (p as i64) * 13 % 50_000;
            post.tags = vec![format!("tag_{}", p % 5), format!("cat_{}", oi % 3)];
            engine.create_object(&post).await.unwrap();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_fetch_pk(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("fetch_by_pk");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Option<BenchUser> = ctx
                    .engine
                    .fetch_object(ctx.ousia_sample_user_id)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM users WHERE id = $1")
                    .bind(ctx.raw_sample_user_id)
                    .fetch_optional(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

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
                let _ = sqlx::query("SELECT * FROM users WHERE username = $1")
                    .bind("user_000042")
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

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
                    "SELECT * FROM users WHERE score > $1 ORDER BY score DESC LIMIT 20",
                )
                .bind(5000_i64)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_owner_scan(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("owner_scan");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchPost> = ctx
                    .engine
                    .fetch_owned_objects(ctx.ousia_owner_id)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM posts WHERE owner_id = $1")
                    .bind(ctx.raw_owner_id)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("count_aggregate");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: u64 = ctx
                    .engine
                    .count_objects::<BenchPost>(Some(
                        Query::new(ctx.ousia_owner_id)
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
                    "SELECT COUNT(*) FROM posts WHERE owner_id = $1 AND status = $2",
                )
                .bind(ctx.raw_owner_id)
                .bind("published")
                .fetch_one(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_bulk_fetch(c: &mut Criterion) {
    let ctx = &state().1;
    let rt = &state().0;
    let mut group = c.benchmark_group("bulk_fetch");

    for n in [10_usize, 50, 100] {
        let ousia_ids = ctx.ousia_bulk_user_ids[..n].to_vec();
        let raw_ids = ctx.raw_bulk_user_ids[..n].to_vec();

        group.bench_with_input(BenchmarkId::new("ousia", n), &ousia_ids, |b, ids| {
            b.iter(|| {
                rt.block_on(async {
                    let _: Vec<BenchUser> = ctx.engine.fetch_objects(ids.clone()).await.unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("raw_sqlx", n), &raw_ids, |b, ids| {
            b.iter(|| {
                rt.block_on(async {
                    let _ = sqlx::query("SELECT * FROM users WHERE id = ANY($1)")
                        .bind(ids.as_slice())
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
                })
            })
        });
    }

    group.finish();
}

fn bench_join(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("join_posts_users");

    group.bench_function("raw_sql_join", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"SELECT p.id, p.title, p.status, p.view_count, u.username, u.display_name
               FROM posts p
               INNER JOIN users u ON u.id = p.owner_id
               WHERE p.status = 'published'
               ORDER BY p.view_count DESC LIMIT 20"#,
                )
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

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
                let _: Vec<BenchUser> = ctx.engine.fetch_objects(owner_ids).await.unwrap();
            })
        })
    });

    group.finish();
}

fn bench_cte_vs_ousia(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("cte_ranked_posts_vs_ousia");

    group.bench_function("raw_sql_cte", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query(
                    r#"WITH ranked AS (
                   SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY owner_id ORDER BY view_count DESC
                   ) AS rn
                   FROM posts WHERE status = 'published'
               )
               SELECT id, owner_id, title, view_count, rn
               FROM ranked WHERE rn <= 3
               ORDER BY owner_id, rn"#,
                )
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

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
                use std::collections::HashMap;
                let mut grouped: HashMap<Uuid, Vec<&BenchPost>> = HashMap::new();
                for post in &posts {
                    let entry = grouped.entry(post.owner()).or_default();
                    if entry.len() < 3 {
                        entry.push(post);
                    }
                }
                let _: Vec<_> = grouped.into_values().flatten().collect();
            })
        })
    });

    group.finish();
}

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
                let _ = sqlx::query("SELECT * FROM posts WHERE tags @> ARRAY['tag_1']")
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

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

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM users WHERE username ILIKE $1 LIMIT 50")
                    .bind("user_000%")
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_multi_sort(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("multi_sort");

    group.bench_function("ousia", |b| {
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

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ =
                    sqlx::query("SELECT * FROM users ORDER BY score DESC, username ASC LIMIT 50")
                        .fetch_all(&ctx.raw_pool)
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
    bench_multi_sort(c);
}

criterion_group! {
    name = ousia_vs_raw;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_vs_raw);
