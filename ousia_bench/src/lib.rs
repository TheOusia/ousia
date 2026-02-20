//! Shared fixtures and helpers for Ousia benchmarks.

use ousia::{EdgeMeta, Meta, OusiaDefault, OusiaEdge, OusiaObject, filter, query::ToIndexValue};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

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
// Async Tokio runtime helper for criterion
// ─────────────────────────────────────────────────────────────────────────────

/// Returns a single-threaded tokio runtime for use inside criterion `iter`.
pub fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio rt")
}

/// Returns a multi-threaded tokio runtime (for concurrency benches).
pub fn mt_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio mt-rt")
}

// ─────────────────────────────────────────────────────────────────────────────
// Database bootstrap
// ─────────────────────────────────────────────────────────────────────────────

pub type Container = testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>;

/// Spins up a throw-away Postgres container and returns `(container, pool)`.
/// The container MUST be kept alive for the lifetime of the benchmark binary —
/// store it in your `Ctx` struct (prefixed `_container`) so it drops last.
///
/// Use [`BenchHandle`] to ensure the container never outlives its runtime.
pub async fn start_postgres() -> (Container, PgPool) {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};
    use testcontainers_modules::postgres::Postgres;

    let container = Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("ousia_bench")
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres container");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!(
        "postgres://postgres:postgres@localhost:{}/ousia_bench",
        port
    );

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .expect("pg pool");

    (container, pool)
}

/// Seed `n` users into both the Ousia object store and a raw `bench_users`
/// table (used by the raw-SQL benchmarks).
pub async fn seed_users(pool: &PgPool, engine: &ousia::Engine, n: usize) -> Vec<uuid::Uuid> {
    use ousia::ObjectMeta;

    // Ensure raw table exists
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bench_users (
            id UUID PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            display_name TEXT NOT NULL,
            score BIGINT NOT NULL DEFAULT 0,
            active BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await
    .unwrap();

    // Ensure indexes mirror what Ousia provides
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_users_username ON bench_users(username)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_users_email ON bench_users(email)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_users_score ON bench_users(score)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_users_active ON bench_users(active)")
        .execute(pool)
        .await
        .unwrap();

    let mut ids = Vec::with_capacity(n);

    for i in 0..n {
        let mut user = BenchUser::default();
        user.username = format!("user_{:06}", i);
        user.email = format!("user_{:06}@bench.test", i);
        user.display_name = format!("User {}", i);
        user.score = (i as i64) * 7 % 10_000;
        user.active = i % 3 != 0;

        // Insert into Ousia
        if let Err(err) = engine.create_object(&user).await {
            if err.is_unique_constraint_violation() {
                eprintln!(
                    "Existing user = {:#?}",
                    engine
                        .find_object::<BenchUser>(&[filter!(
                            &BenchUser::FIELDS.username,
                            user.username
                        )])
                        .await
                );
            }
        }

        // Mirror into raw table
        sqlx::query(
            r#"
            INSERT INTO bench_users (id, username, email, display_name, score, active)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(user.id())
        .bind(&user.username)
        .bind(&user.email)
        .bind(&user.display_name)
        .bind(user.score)
        .bind(user.active)
        .execute(pool)
        .await
        .unwrap();

        ids.push(user.id());
    }

    ids
}

/// Seed posts owned by the given user IDs, and mirror to a raw `bench_posts` table.
pub async fn seed_posts(
    pool: &PgPool,
    engine: &ousia::Engine,
    owner_ids: &[uuid::Uuid],
    posts_per_user: usize,
) -> Vec<uuid::Uuid> {
    use ousia::ObjectMeta;
    use ousia::ObjectOwnership;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS bench_posts (
            id UUID PRIMARY KEY,
            owner_id UUID NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft',
            view_count BIGINT NOT NULL DEFAULT 0,
            tags TEXT[] NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_posts_owner ON bench_posts(owner_id)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_posts_status ON bench_posts(status)")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_bench_posts_tags ON bench_posts USING GIN(tags)")
        .execute(pool)
        .await
        .unwrap();

    let mut ids = Vec::new();
    let statuses = ["draft", "published", "archived"];

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

            sqlx::query(
                r#"
                INSERT INTO bench_posts (id, owner_id, title, body, status, view_count, tags)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO NOTHING
                "#,
            )
            .bind(post.id())
            .bind(owner_id)
            .bind(&post.title)
            .bind(&post.body)
            .bind(statuses[p % 3])
            .bind(post.view_count)
            .bind(&post.tags)
            .execute(pool)
            .await
            .unwrap();

            ids.push(post.id());
        }
    }

    ids
}

/// Seed follow edges between users.
pub async fn seed_follows(engine: &ousia::Engine, user_ids: &[uuid::Uuid]) {
    let n = user_ids.len();
    for i in 0..n {
        // Each user follows the next 3 users (wrap-around)
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
// BenchHandle — leak-based lifetime management for criterion + testcontainers
// ─────────────────────────────────────────────────────────────────────────────

/// Keeps a `(Runtime, Ctx)` alive for the entire benchmark binary by leaking
/// the box — ensuring the container's async destructor is never invoked after
/// the runtime has shut down.
///
/// Docker-side cleanup is handled by testcontainers' Ryuk reaper sidecar.
///
/// ```rust,ignore
/// static STATE: ousia_bench::BenchHandle<MyCxt> = ousia_bench::BenchHandle::new();
///
/// fn state() -> &'static (tokio::runtime::Runtime, MyCxt) {
///     STATE.get_or_init(|| {
///         let rt = ousia_bench::mt_rt();
///         let ctx = rt.block_on(async { /* ... */ });
///         (rt, ctx)
///     })
/// }
/// ```
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

// SAFETY: Runtime, PgPool, and Container are Send.
// We only hand out shared references after init completes.
unsafe impl<T: Send> Sync for BenchHandle<T> {}
