//! Benchmark: Query patterns — composite filters, OR conditions,
//! cursor pagination, multi-sort, and full table scans.
//!
//! Two completely independent Postgres databases — one for Ousia, one raw.
//! No shared IDs, no shared seeders, no cross-contamination.

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

    // Ousia side
    engine: Engine,
    ousia_cursor_mid: Uuid,

    // Raw side
    raw_pool: PgPool,
    raw_cursor_mid: Uuid,
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
    let engine = Engine::new(Box::new(adapter));

    let ousia_user_ids = seed_ousia_users(&engine, 500).await;
    seed_ousia_posts(&engine, &ousia_user_ids[..50], 20).await;

    let ousia_cursor_mid = ousia_user_ids[250];

    // --- Raw side ---
    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = seed_raw_users(&raw_pool, 500).await;
    seed_raw_posts(&raw_pool, &raw_user_ids[..50], 20).await;

    let raw_cursor_mid = raw_user_ids[250];

    Ctx {
        _ousia_container,
        _raw_container,
        engine,
        ousia_cursor_mid,
        raw_pool,
        raw_cursor_mid,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw schema
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
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    for ddl in [
        "CREATE UNIQUE INDEX idx_users_username ON users(username)",
        "CREATE INDEX idx_users_score  ON users(score)",
        "CREATE INDEX idx_users_active ON users(active)",
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
// Raw seeders
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_raw_users(pool: &PgPool, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id: Uuid = sqlx::query_scalar(
            r#"INSERT INTO users (username, email, display_name, score, active)
               VALUES ($1, $2, $3, $4, $5) RETURNING id"#,
        )
        .bind(format!("user_{:06}", i))
        .bind(format!("user_{:06}@bench.test", i))
        .bind(format!("User {}", i))
        .bind((i as i64) * 7 % 10_000)
        .bind(i % 3 != 0)
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
                let _ = sqlx::query("SELECT * FROM users WHERE active = $1 AND score > $2")
                    .bind(true)
                    .bind(3000_i64)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_or_filter(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("query_or_condition");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let _: Vec<BenchUser> = ctx
                    .engine
                    .query_objects(Query::default().where_contains(
                        &BenchUser::FIELDS.username,
                        vec!["user_000010", "user_000020", "user_000030"],
                    ))
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _ = sqlx::query("SELECT * FROM users WHERE username = ANY($1)")
                    .bind(vec!["user_000010", "user_000020", "user_000030"])
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_cursor_pagination(c: &mut Criterion) {
    let ctx = &state().1;
    let rt = &state().0;
    let mut group = c.benchmark_group("cursor_pagination");

    for page_size in [10_u32, 50, 100] {
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
                        let _ = sqlx::query("SELECT * FROM users ORDER BY id DESC LIMIT $1")
                            .bind(ps as i64)
                            .fetch_all(&ctx.raw_pool)
                            .await
                            .unwrap();
                    })
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ousia_mid_page", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _: Vec<BenchUser> = ctx
                            .engine
                            .query_objects(
                                Query::default()
                                    .with_cursor(ctx.ousia_cursor_mid)
                                    .with_limit(ps),
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
                            "SELECT * FROM users WHERE id < $1 ORDER BY id DESC LIMIT $2",
                        )
                        .bind(ctx.raw_cursor_mid)
                        .bind(ps as i64)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
                    })
                })
            },
        );
    }

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
                    let _ = sqlx::query("SELECT * FROM users ORDER BY id DESC LIMIT $1")
                        .bind(l as i64)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
                })
            })
        });
    }

    group.finish();
}

fn bench_create_object(c: &mut Criterion) {
    let ctx = &state().1;
    let mut group = c.benchmark_group("create_object");

    group.bench_function("ousia", |b| {
        b.iter(|| {
            run!({
                let mut post = BenchPost::default();
                post.title = "bench post".to_string();
                ctx.engine.create_object(&post).await.unwrap();
            })
        })
    });

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let owner_id = Uuid::now_v7();
                sqlx::query(
                    r#"INSERT INTO posts (owner_id, title, body, status, view_count, tags)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
                )
                .bind(owner_id)
                .bind("bench post")
                .bind("")
                .bind("draft")
                .bind(0_i64)
                .bind(Vec::<String>::new())
                .execute(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
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
