//! Benchmark: Ousia ORM vs raw sqlx vs sea-orm
//!
//! All three variants decode into equivalent Rust types so the comparison is
//! fair — no discarding rows as PgRow:
//!   ousia     → typed domain structs (BenchUser, BenchPost, …)
//!   raw_sqlx  → RawUser / RawPost / RawFollow (sqlx::FromRow)
//!   sea_orm   → ORM entity Model / OrmPost (sea_orm::FromQueryResult)
//!
//! Databases:
//!   ousia_bench_vs_ousia  — Ousia schema, populated via Engine.
//!   ousia_bench_vs_raw    — plain schema; raw_sqlx and sea_orm share it.
//!
//! Set `BENCH_PG_BASE=postgres://user:pass@host` before running.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ousia::{Engine, ObjectMeta, ObjectOwnership, Query, adapters::postgres::PostgresAdapter};
use ousia_bench::{BenchPost, BenchUser, PostStatus, RawPost, RawUser, orm};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, Statement,
};
use sqlx::PgPool;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// One-off result types for JOIN / CTE bench groups
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow)]
struct RawJoinRow {
    id: Uuid,
    title: String,
    status: String,
    view_count: i64,
    username: String,
    display_name: String,
}

#[derive(Debug, FromQueryResult)]
struct OrmJoinRow {
    id: Uuid,
    title: String,
    status: String,
    view_count: i64,
    username: String,
    display_name: String,
}

#[derive(Debug, sqlx::FromRow)]
struct RawRankedPost {
    id: Uuid,
    owner_id: Uuid,
    title: String,
    view_count: i64,
    rn: i64,
}

#[derive(Debug, FromQueryResult)]
struct OrmRankedPost {
    id: Uuid,
    owner_id: Uuid,
    title: String,
    view_count: i64,
    rn: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

struct Ctx {
    engine: Engine,
    ousia_sample_user_id: Uuid,
    ousia_owner_id: Uuid,
    ousia_bulk_user_ids: Vec<Uuid>,
    /// The 200 user IDs that own posts — used to scope N+1 / 2q preload benches.
    ousia_post_owner_ids: Vec<Uuid>,

    raw_pool: PgPool,
    orm_db: sea_orm::DatabaseConnection,
    raw_sample_user_id: Uuid,
    raw_owner_id: Uuid,
    raw_bulk_user_ids: Vec<Uuid>,
    /// Mirror of ousia_post_owner_ids for the raw schema.
    raw_post_owner_ids: Vec<Uuid>,
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
    let ousia_pool = ousia_bench::connect_db("ousia_bench_vs_ousia").await;
    let raw_pool = ousia_bench::connect_db("ousia_bench_vs_raw").await;
    let orm_db = ousia_bench::connect_orm("ousia_bench_vs_raw").await;

    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    // Clean any data from a previous run before re-seeding.
    sqlx::query("TRUNCATE public.edges, public.objects")
        .execute(&ousia_pool).await.unwrap();
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(ousia_pool.clone())));

    // 10k users bulk-seeded; 200 post owners × 10 posts = 2 000 posts via engine
    let ousia_user_ids = ousia_bench::seed_ousia_users_bulk(&ousia_pool, 10_000).await;
    seed_ousia_posts(&engine, &ousia_user_ids[..200], 10).await;
    let ousia_sample_user_id = ousia_user_ids[4_200];
    let ousia_owner_id = ousia_user_ids[0];
    let ousia_bulk_user_ids = ousia_user_ids[..1_000].to_vec();
    let ousia_post_owner_ids = ousia_user_ids[..200].to_vec();

    setup_raw_schema(&raw_pool).await;
    let raw_user_ids = ousia_bench::seed_raw_users_bulk(&raw_pool, 10_000).await;
    seed_raw_posts(&raw_pool, &raw_user_ids[..200], 10).await;
    let raw_sample_user_id = raw_user_ids[4_200];
    let raw_owner_id = raw_user_ids[0];
    let raw_bulk_user_ids = raw_user_ids[..1_000].to_vec();
    let raw_post_owner_ids = raw_user_ids[..200].to_vec();

    Ctx {
        engine,
        ousia_sample_user_id,
        ousia_owner_id,
        ousia_bulk_user_ids,
        ousia_post_owner_ids,
        raw_pool,
        orm_db,
        raw_sample_user_id,
        raw_owner_id,
        raw_bulk_user_ids,
        raw_post_owner_ids,
    }
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
        "CREATE INDEX idx_users_email    ON users(email)",
        "CREATE INDEX idx_users_score    ON users(score)",
        "CREATE INDEX idx_users_active   ON users(active)",
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
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// SQL that fetches full post rows from the raw schema for sea-orm Statement
/// queries.  `to_jsonb(tags)` converts TEXT[] → JSONB so sea-orm can decode it
/// as `serde_json::Value` (equivalent work to sqlx decoding `Vec<String>`).
const POST_SELECT: &str =
    "SELECT id, owner_id, title, body, status, view_count, to_jsonb(tags) AS tags FROM posts";

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_fetch_pk(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Option<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE id = $1")
                        .bind(ctx.raw_sample_user_id)
                        .fetch_optional(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Option<orm::users::Model> =
                    orm::users::Entity::find_by_id(ctx.raw_sample_user_id)
                        .one(&ctx.orm_db)
                        .await
                        .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_eq_filter(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE username = $1")
                        .bind("user_000042")
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
                    .filter(orm::users::Column::Username.eq("user_000042"))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_range_sort_limit(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> = sqlx::query_as(
                    "SELECT * FROM users WHERE score > $1 ORDER BY score DESC LIMIT 20",
                )
                .bind(5000_i64)
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
                    .filter(orm::users::Column::Score.gt(5000_i64))
                    .order_by_desc(orm::users::Column::Score)
                    .limit(20)
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_owner_scan(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Vec<RawPost> =
                    sqlx::query_as("SELECT * FROM posts WHERE owner_id = $1")
                        .bind(ctx.raw_owner_id)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let sql = format!("{POST_SELECT} WHERE owner_id = $1");
                let _: Vec<orm::posts::OrmPost> =
                    orm::posts::OrmPost::find_by_statement(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        &sql,
                        [ctx.raw_owner_id.into()],
                    ))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_count(c: &mut Criterion) {
    let (rt, ctx) = state();
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

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let row = ctx
                    .orm_db
                    .query_one(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT COUNT(*)::bigint AS c FROM posts WHERE owner_id = $1 AND status = $2",
                        [ctx.raw_owner_id.into(), "published".into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                let _: i64 = row.try_get("", "c").unwrap();
            })
        })
    });

    group.finish();
}

fn bench_bulk_fetch(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                    let _: Vec<RawUser> = sqlx::query_as("SELECT * FROM users WHERE id = ANY($1)")
                        .bind(ids.as_slice())
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("sea_orm", n), &raw_ids, |b, ids| {
            b.iter(|| {
                rt.block_on(async {
                    let _: Vec<orm::users::Model> = orm::users::Entity::find()
                        .filter(orm::users::Column::Id.is_in(ids.clone()))
                        .all(&ctx.orm_db)
                        .await
                        .unwrap();
                })
            })
        });
    }

    group.finish();
}

fn bench_join(c: &mut Criterion) {
    let (rt, ctx) = state();
    // Ousia's data model is graph-first — ownership is an attribute, not a
    // FK to JOIN on.  This group shows raw JOIN performance; ousia covers this
    // access pattern via `preload_object` (graph traversal) in ousia_edges.
    let mut group = c.benchmark_group("join_posts_users");

    const JOIN_SQL: &str = r#"
        SELECT p.id, p.title, p.status, p.view_count, u.username, u.display_name
        FROM posts p
        INNER JOIN users u ON u.id = p.owner_id
        WHERE p.status = 'published'
        ORDER BY p.view_count DESC LIMIT 20"#;

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawJoinRow> = sqlx::query_as(JOIN_SQL)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<OrmJoinRow> = OrmJoinRow::find_by_statement(
                    Statement::from_sql_and_values(DbBackend::Postgres, JOIN_SQL, []),
                )
                .all(&ctx.orm_db)
                .await
                .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_cte_vs_ousia(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("cte_ranked_posts");

    const CTE_SQL: &str = r#"
        WITH ranked AS (
            SELECT id, owner_id, title, view_count,
                   ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY view_count DESC) AS rn
            FROM posts WHERE status = 'published'
        )
        SELECT id, owner_id, title, view_count, rn
        FROM ranked WHERE rn <= 3
        ORDER BY owner_id, rn"#;

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawRankedPost> = sqlx::query_as(CTE_SQL)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<OrmRankedPost> = OrmRankedPost::find_by_statement(
                    Statement::from_sql_and_values(DbBackend::Postgres, CTE_SQL, []),
                )
                .all(&ctx.orm_db)
                .await
                .unwrap();
            })
        })
    });

    // Ousia equivalent: fetch all published posts, group top-3 per owner in Rust.
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
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("array_contains_tag");

    const GIN_SQL: &str = concat!(
        "SELECT id, owner_id, title, body, status, view_count, to_jsonb(tags) AS tags ",
        "FROM posts WHERE tags @> ARRAY['tag_1']"
    );

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

    group.bench_function("raw_sqlx", |b| {
        b.iter(|| {
            run!({
                let _: Vec<RawPost> =
                    sqlx::query_as("SELECT * FROM posts WHERE tags @> ARRAY['tag_1']")
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::posts::OrmPost> =
                    orm::posts::OrmPost::find_by_statement(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        GIN_SQL,
                        [],
                    ))
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

    group.finish();
}

fn bench_begins_with(c: &mut Criterion) {
    let (rt, ctx) = state();
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
                let _: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE username ILIKE $1 LIMIT 50")
                        .bind("user_000%")
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
            })
        })
    });

    // sea-orm generates LIKE (case-sensitive); differences negligible at bench scale
    group.bench_function("sea_orm", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _: Vec<orm::users::Model> = orm::users::Entity::find()
                    .filter(orm::users::Column::Username.starts_with("user_000"))
                    .limit(50)
                    .all(&ctx.orm_db)
                    .await
                    .unwrap();
            })
        })
    });

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

// ─────────────────────────────────────────────────────────────────────────────
// Glue
// ─────────────────────────────────────────────────────────────────────────────

/// Multi-pivot ownership: fetch posts for known post-owning users.
///
/// ousia       — 2 queries: all 10k users + batch WHERE owner = ANY(all user IDs)
/// raw N+1     — 201 queries: fetch 200 post-owners by ID, then 1 query per owner
/// raw 2q      — 2 queries: fetch 200 post-owners by ID + batch posts, group in Rust
///
/// N+1 is scoped to the 200 known post-owners to keep bench time practical.
fn bench_preload_owned_batch(c: &mut Criterion) {
    let (rt, ctx) = state();
    let mut group = c.benchmark_group("preload_owned_batch");

    // ousia: 2 queries total, results already grouped by owner
    group.bench_function("ousia_batch_2q", |b| {
        b.iter(|| {
            run!({
                let _: Vec<(BenchUser, Vec<BenchPost>)> = ctx
                    .engine
                    .preload_objects::<BenchUser>(Query::default())
                    .preload::<BenchPost>()
                    .collect()
                    .await
                    .unwrap();
            })
        })
    });

    // raw N+1: for each post owner, fetch their posts (200 owners × 1 query)
    group.bench_function("raw_n_plus_1", |b| {
        b.iter(|| {
            run!({
                let owners: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE id = ANY($1)")
                        .bind(ctx.raw_post_owner_ids.as_slice())
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();
                for owner in &owners {
                    let _: Vec<RawPost> =
                        sqlx::query_as("SELECT * FROM posts WHERE owner_id = $1")
                            .bind(owner.id)
                            .fetch_all(&ctx.raw_pool)
                            .await
                            .unwrap();
                }
            })
        })
    });

    // raw 2q: fetch post-owners + one batch owned-posts query, group in Rust
    group.bench_function("raw_batch_2q", |b| {
        b.iter(|| {
            run!({
                let owners: Vec<RawUser> =
                    sqlx::query_as("SELECT * FROM users WHERE id = ANY($1)")
                        .bind(ctx.raw_post_owner_ids.as_slice())
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();

                let owner_ids: Vec<Uuid> = owners.iter().map(|u| u.id).collect();
                let posts: Vec<RawPost> =
                    sqlx::query_as("SELECT * FROM posts WHERE owner_id = ANY($1)")
                        .bind(&owner_ids)
                        .fetch_all(&ctx.raw_pool)
                        .await
                        .unwrap();

                use std::collections::HashMap;
                let mut map: HashMap<Uuid, Vec<RawPost>> = HashMap::new();
                for post in posts {
                    map.entry(post.owner_id).or_default().push(post);
                }
                let _: Vec<(RawUser, Vec<RawPost>)> = owners
                    .into_iter()
                    .filter_map(|u| {
                        let ps = map.remove(&u.id)?;
                        Some((u, ps))
                    })
                    .collect();
            })
        })
    });

    group.finish();
}

fn run_all(c: &mut Criterion) {
    bench_fetch_pk(c);
    bench_eq_filter(c);
    bench_range_sort_limit(c);
    bench_owner_scan(c);
    bench_count(c);
    bench_bulk_fetch(c);
    bench_preload_owned_batch(c);
    bench_join(c);
    bench_cte_vs_ousia(c);
    bench_array_contains(c);
    bench_begins_with(c);
    bench_multi_sort(c);
}

criterion_group! {
    name = ousia_vs_raw;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_vs_raw);
