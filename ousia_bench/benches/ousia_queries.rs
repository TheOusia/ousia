//! Benchmark: Query patterns — composite filters, OR, cursor pagination,
//! multi-sort, full table scans, and writes.
//!
//! All three variants decode into equivalent Rust types.
//!
//! Databases:
//!   ousia_bench_q_ousia  — Ousia schema
//!   ousia_bench_q_raw    — plain schema; raw_sqlx and sea_orm share it.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{Engine, ObjectMeta, ObjectOwnership, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchPost, BenchUser, PostStatus, RawUser, orm};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    Statement,
};
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    engine: Engine,
    ousia_cursor_mid: Uuid,

    raw_pool: PgPool,
    orm_db: sea_orm::DatabaseConnection,
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
    let ousia_pool = ousia_bench::connect_db("ousia_bench_q_ousia").await;
    let raw_pool = ousia_bench::connect_db("ousia_bench_q_raw").await;
    let orm_db = ousia_bench::connect_orm("ousia_bench_q_raw").await;

    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    // Clean any data from a previous run before re-seeding.
    sqlx::query("TRUNCATE public.edges, public.objects")
        .execute(&ousia_pool).await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // 50k users bulk-seeded; 100 owners × 20 posts = 2 000 posts via engine
    let ousia_user_ids = ousia_bench::seed_ousia_users_bulk(&ousia_pool, 50_000).await;
    seed_ousia_posts(&engine, &ousia_user_ids[..100], 20).await;
    let ousia_cursor_mid = ousia_user_ids[25_000];

    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = ousia_bench::seed_raw_users_bulk(&raw_pool, 50_000).await;
    seed_raw_posts(&raw_pool, &raw_user_ids[..100], 20).await;
    let raw_cursor_mid = raw_user_ids[25_000];

    Ctx { engine, ousia_cursor_mid, raw_pool, orm_db, raw_cursor_mid }
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw schema
// ─────────────────────────────────────────────────────────────────────────────

async fn setup_raw_schema(pool: &PgPool) {
    // Drop tables from any previous run before recreating.
    sqlx::query("DROP TABLE IF EXISTS posts CASCADE").execute(pool).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS users CASCADE").execute(pool).await.unwrap();

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
            title       TEXT        NOT NULL DEFAULT '',
            body        TEXT        NOT NULL DEFAULT '',
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
// Seeders
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_raw_posts(pool: &PgPool, owner_ids: &[Uuid], per_owner: usize) {
    for (oi, &owner_id) in owner_ids.iter().enumerate() {
        for p in 0..per_owner {
            let tags = vec![format!("tag_{}", p % 5), format!("cat_{}", oi % 3)];
            sqlx::query(
                r#"INSERT INTO posts (owner_id, title, body, status, view_count, tags)
                   VALUES ($1, $2, $3, $4, $5, $6)"#,
            )
            .bind(owner_id)
            .bind(format!("Post {p} by owner {oi}"))
            .bind("Lorem ipsum dolor sit amet".repeat(4))
            .bind(match p % 3 { 0 => "draft", 1 => "published", _ => "archived" })
            .bind((p as i64) * 13 % 50_000)
            .bind(&tags)
            .execute(pool)
            .await
            .unwrap();
        }
    }
}

async fn seed_ousia_posts(engine: &Engine, owner_ids: &[Uuid], per_owner: usize) {
    for (oi, &owner_id) in owner_ids.iter().enumerate() {
        for p in 0..per_owner {
            let mut post = BenchPost::default();
            post.set_owner(owner_id);
            post.title = format!("Post {p} by owner {oi}");
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
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE active = $1 AND score > $2")
                        .bind(true)
                        .bind(3000_i64)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Entity::find()
                    .filter(orm::users::Column::Active.eq(true))
                    .filter(orm::users::Column::Score.gt(3000_i64))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_or_filter(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE username = ANY($1)")
                        .bind(vec!["user_000010", "user_000020", "user_000030"])
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Entity::find()
                    .filter(orm::users::Column::Username.is_in([
                        "user_000010",
                        "user_000020",
                        "user_000030",
                    ]))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_cursor_pagination(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                        let _: Vec<RawUser> =
                            sqlx::query_as("SELECT * FROM users ORDER BY id DESC LIMIT $1")
                                .bind(ps as i64)
                                .fetch_all(&ctx.raw_pool)
                                .await
                                .unwrap();
                    })
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("sea_orm_page1", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _: Vec<orm::users::Model> = orm::users::Entity::find()
                            .order_by_desc(orm::users::Column::Id)
                            .limit(ps as u64)
                            .all(&ctx.orm_db)
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
                        let _: Vec<RawUser> = sqlx::query_as(
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

        group.bench_with_input(
            BenchmarkId::new("sea_orm_mid_page", page_size),
            &page_size,
            |b, &ps| {
                b.iter(|| {
                    rt.block_on(async {
                        let _: Vec<orm::users::Model> = orm::users::Entity::find()
                            .filter(orm::users::Column::Id.lt(ctx.raw_cursor_mid))
                            .order_by_desc(orm::users::Column::Id)
                            .limit(ps as u64)
                            .all(&ctx.orm_db)
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
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> = sqlx::query_as(
                    "SELECT * FROM users ORDER BY score DESC, username ASC LIMIT 50",
                )
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Entity::find()
                    .order_by_desc(orm::users::Column::Score)
                    .order_by_asc(orm::users::Column::Username)
                    .limit(50)
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_full_scan(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                    let _: Vec<RawUser> =
                        sqlx::query_as("SELECT * FROM users ORDER BY id DESC LIMIT $1")
                            .bind(l as i64)
                            .fetch_all(&ctx.raw_pool)
                            .await
                            .unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("sea_orm", limit), &limit, |b, &l| {
            b.iter(|| {
                rt.block_on(async {
                    let _: Vec<orm::users::Model> = orm::users::Entity::find()
                        .order_by_desc(orm::users::Column::Id)
                        .limit(l as u64)
                        .all(&ctx.orm_db)
                        .await
                        .unwrap();
                })
            })
        });
    }

    group.finish();
}

fn bench_create_object(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                sqlx::query(
                    r#"INSERT INTO posts (owner_id, title, body, status, view_count, tags)
                       VALUES ($1, $2, $3, $4, $5, $6)"#,
                )
                .bind(Uuid::now_v7())
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

    // sea-orm: insert a post via Statement (tags has DEFAULT '{}' so omit it)
    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                ctx.orm_db
                    .execute(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "INSERT INTO posts (owner_id, title, body, status, view_count) \
                         VALUES ($1, $2, $3, $4, $5)",
                        [
                            Uuid::now_v7().into(),
                            "bench post".into(),
                            "".into(),
                            "draft".into(),
                            0i64.into(),
                        ],
                    ))
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
        .sample_size(20)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_queries);
