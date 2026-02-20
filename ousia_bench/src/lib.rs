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
