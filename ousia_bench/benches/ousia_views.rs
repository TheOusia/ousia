//! Benchmark: View system serialisation overhead
//!
//! Measures:
//!   - Deserialise a full object (from ObjectRecord) — baseline
//!   - Generate a public view  (_public())
//!   - Generate an admin view  (_admin())
//!   - Manually construct the equivalent struct (zero-cost baseline)
//!   - serde_json::to_value on a raw struct
//!
//! These are all in-memory benchmarks; no DB round-trips.

use criterion::{Criterion, criterion_group, criterion_main};
use ousia::{Meta, ObjectMeta, OusiaDefault, OusiaObject};
use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// A richly-annotated type that exercises the view system
// ─────────────────────────────────────────────────────────────────────────────

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
pub struct ProfileUser {
    #[ousia_meta(view(public = "id,created_at"))]
    #[ousia_meta(view(admin = "id,owner,created_at,updated_at"))]
    pub _meta: Meta,

    #[ousia(view(public))]
    #[ousia(view(admin))]
    pub username: String,

    #[ousia(view(public))]
    #[ousia(view(admin))]
    pub display_name: String,

    #[ousia(view(admin))]
    pub email: String,

    #[ousia(view(admin))]
    pub role: String,

    // Private — never serialised
    #[ousia(private)]
    pub password_hash: String,
    #[ousia(private)]
    pub totp_secret: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Equivalent hand-rolled structs (zero-cost baseline)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct PublicView {
    id: uuid::Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    username: String,
    display_name: String,
}

#[derive(Debug, Serialize)]
struct AdminView {
    id: uuid::Uuid,
    owner: uuid::Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    username: String,
    display_name: String,
    email: String,
    role: String,
}

fn make_user() -> ProfileUser {
    let mut u = ProfileUser::default();
    u.username = "alice_benchmark".to_string();
    u.display_name = "Alice Benchmark".to_string();
    u.email = "alice@bench.test".to_string();
    u.role = "member".to_string();
    u.password_hash = "$2b$10$abc123".to_string();
    u.totp_secret = "JBSWY3DPEHPK3PXP".to_string();
    u
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_views(c: &mut Criterion) {
    let user = make_user();
    let mut group = c.benchmark_group("view_serialisation");

    // Full internal serialise (what the DB write path uses)
    group.bench_function("__serialize_internal", |b| {
        use ousia::object::ObjectInternal;
        b.iter(|| {
            let _ = user.__serialize_internal();
        })
    });

    // Default Serialize (omits private + no view selection)
    group.bench_function("serde_json_to_value_default", |b| {
        b.iter(|| {
            let _ = serde_json::to_value(&user).unwrap();
        })
    });

    // Public view struct generation
    group.bench_function("ousia_public_view", |b| {
        b.iter(|| {
            let _ = user._public();
        })
    });

    // Admin view struct generation
    group.bench_function("ousia_admin_view", |b| {
        b.iter(|| {
            let _ = user._admin();
        })
    });

    // Hand-rolled public view (zero-cost baseline)
    group.bench_function("manual_public_view", |b| {
        b.iter(|| {
            let _ = PublicView {
                id: user.id(),
                created_at: user.created_at(),
                username: user.username.clone(),
                display_name: user.display_name.clone(),
            };
        })
    });

    // Hand-rolled admin view
    group.bench_function("manual_admin_view", |b| {
        b.iter(|| {
            let _ = AdminView {
                id: user.id(),
                owner: user.owner(),
                created_at: user.created_at(),
                updated_at: user.updated_at(),
                username: user.username.clone(),
                display_name: user.display_name.clone(),
                email: user.email.clone(),
                role: user.role.clone(),
            };
        })
    });

    // Serialize the public view to JSON (what an API handler would do)
    group.bench_function("ousia_public_view_to_json", |b| {
        b.iter(|| {
            let v = user._public();
            let _ = serde_json::to_string(&v).unwrap();
        })
    });

    group.bench_function("manual_public_view_to_json", |b| {
        b.iter(|| {
            let v = PublicView {
                id: user.id(),
                created_at: user.created_at(),
                username: user.username.clone(),
                display_name: user.display_name.clone(),
            };
            let _ = serde_json::to_string(&v).unwrap();
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// ObjectRecord round-trip  (what a fetch from DB looks like)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_record_roundtrip(c: &mut Criterion) {
    use ousia::ObjectRecord;
    use ousia::object::ObjectInternal;

    let user = make_user();
    let record = ObjectRecord::from_object(&user);
    let mut group = c.benchmark_group("object_record_roundtrip");

    group.bench_function("from_object", |b| {
        b.iter(|| {
            let _ = ObjectRecord::from_object(&user);
        })
    });

    // group.bench_function("to_object", |b| {
    //     b.iter(|| {
    //         let _: ProfileUser = record.to_object().unwrap();
    //     })
    // });

    // Full round-trip (serialize + deserialize)
    group.bench_function("full_roundtrip", |b| {
        b.iter(|| {
            let rec = ObjectRecord::from_object(&user);
            let _: ProfileUser = rec.to_object().unwrap();
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// index_meta generation
// ─────────────────────────────────────────────────────────────────────────────

fn bench_index_meta(c: &mut Criterion) {
    use ousia::Object;

    let user = make_user();
    let mut group = c.benchmark_group("index_meta_generation");

    group.bench_function("index_meta", |b| {
        b.iter(|| {
            let _ = user.index_meta();
        })
    });

    group.bench_function("index_meta_to_json", |b| {
        b.iter(|| {
            let meta = user.index_meta();
            let _ = serde_json::to_value(&meta).unwrap();
        })
    });

    group.finish();
}

criterion_group! {
    name = ousia_views;
    config = Criterion::default()
        .sample_size(200)
        .measurement_time(std::time::Duration::from_secs(3));
    targets = bench_views, bench_record_roundtrip, bench_index_meta
}
criterion_main!(ousia_views);
