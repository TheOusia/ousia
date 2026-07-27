//! Benchmark: 2-hop batch traversal (ousia 2.1.0) vs. the previous-version
//! idiom (ousia 2.0.2) and hand-written equivalents.
//!
//! Models the motivating case from `plan_2.1.md`:
//! `Highlight -[HighlightItem]-> Item -[ItemModifier]-> Modifier`.
//!
//! Databases:
//!   ousia_bench_2h_ousia — Ousia schema, populated via Engine (guarantees
//!                          correct msgpack encoding — no raw-bytes seeding).
//!   ousia_bench_2h_raw   — plain schema; raw_sqlx and sea_orm share it.
//!
//! Covers, all for "for these N highlights, give me every item with its
//! modifiers" — one page's worth of pivots, all in ONE benchmark call:
//!
//!   ousia_v2_1_1query   — NEW: `batch_edge().then_edge().collect_with_target()`,
//!                         exactly 1 SQL query.
//!   ousia_v2_0_2query   — PREVIOUS VERSION (2.0.2): the only batch-traversal
//!                         API available before 2.1 — two sequential
//!                         `query_edges_with_targets_batch` calls, deduping
//!                         item ids and joining the results in Rust. This is
//!                         the direct "before" baseline for the 2.1.0 feature.
//!   raw_sqlx_1query     — hand-written double-JOIN, single query, floor
//!                         reference for what ousia's new query costs.
//!   raw_sqlx_2query     — hand-written two queries + Rust grouping, floor
//!                         reference for what the 2.0.2 idiom costs.
//!   sea_orm_1query      — sea_orm has no native multi-hop batch join either,
//!                         so this is the same escape hatch a sea_orm user
//!                         would reach for: `find_by_statement` on the same
//!                         double-JOIN SQL as raw_sqlx_1query.
//!
//! A literal per-pivot N+1 baseline is intentionally NOT included: the real
//! predecessor to 2.1.0 is the 2.0.2 batch idiom (ousia_v2_0_2query) — that
//! was already the recommended pattern, per plan_2.1.md's "Immediate fix"
//! section. A true N+1 loop only restates that N+1 is worse than 2 queries,
//! which the existing `ousia_edges` bench already demonstrates.
//!
//! ## Two overlap scenarios — read this before trusting a single number
//!
//! The 1-query JOIN and the 2-query dedup-then-join idiom do NOT scale the
//! same way with how much the highlight→item edge set *reuses* items:
//!
//! - `low_overlap`  — each highlight's items are (almost) unique to it
//!   (`N_ITEMS_LOW_OVERLAP` ≈ total highlight-item edges). This is the real
//!   mealgro shape: items belong to one store, highlights rarely share them.
//!   Here the 1-query JOIN wins — it does the same amount of work either way.
//! - `high_overlap` — items are heavily reused across highlights (~10×
//!   here). The 2-query idiom's Rust-side `dedup()` shrinks the second hop
//!   to one row per *unique* item; the 1-query JOIN instead re-executes the
//!   second hop once per *(highlight, item)* pair, redoing the same
//!   item→modifier lookup up to 10× per item. The 2-query idiom wins here.
//!
//! Both are included so this file doesn't quietly cherry-pick the flattering
//! shape — see the results write-up in `progress_2.1.md` / the PR/commit
//! description for actual numbers and which shape matches production.

use criterion::{Criterion, criterion_group, criterion_main};
use ousia::{
    EdgeMeta, EdgeQuery, Engine, Meta, ObjectEdge, ObjectMeta, OusiaDefault, OusiaEdge,
    OusiaObject, adapters::postgres::PostgresAdapter,
};
use sea_orm::{DbBackend, FromQueryResult, Statement};
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────────
// Ousia domain types (bench-local — not shared with other bench binaries)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(type_name = "BenchHighlight", index = "name:search")]
struct BenchHighlight {
    _meta: Meta,
    name: String,
}

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(type_name = "BenchItem", index = "name:search")]
struct BenchItem {
    _meta: Meta,
    name: String,
    price_cents: i64,
}

#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(type_name = "BenchModifier", index = "name:search")]
struct BenchModifier {
    _meta: Meta,
    name: String,
    price_delta_cents: i64,
}

#[derive(OusiaEdge, Debug)]
#[ousia(
    type_name = "BenchHighlightItem",
    from = BenchHighlight,
    to = BenchItem,
    index = "position:search"
)]
struct BenchHighlightItem {
    _meta: EdgeMeta,
    position: i64,
}

#[derive(OusiaEdge, Debug)]
#[ousia(
    type_name = "BenchItemModifier",
    from = BenchItem,
    to = BenchModifier,
    index = "is_required:search"
)]
struct BenchItemModifier {
    _meta: EdgeMeta,
    is_required: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Raw row types (must decode to typed structs — no bare PgRow)
// ─────────────────────────────────────────────────────────────────────────────

/// Flat row for the single hand-written 2-hop JOIN (`raw_sqlx_1query`).
/// One row per (highlight, item, modifier); an item with zero modifiers
/// produces one row with NULL modifier columns (LEFT JOIN).
#[derive(Debug, Clone, sqlx::FromRow)]
struct RawTwoHopRow {
    highlight_id: Uuid,
    item_id: Uuid,
    item_name: String,
    item_price_cents: i64,
    modifier_id: Option<Uuid>,
    modifier_name: Option<String>,
    modifier_price_delta_cents: Option<i64>,
}

/// Same shape, decoded via sea_orm's `FromQueryResult` (`sea_orm_1query`).
#[derive(Debug, FromQueryResult)]
struct OrmTwoHopRow {
    highlight_id: Uuid,
    item_id: Uuid,
    item_name: String,
    item_price_cents: i64,
    modifier_id: Option<Uuid>,
    modifier_name: Option<String>,
    modifier_price_delta_cents: Option<i64>,
}

/// Hop-1 row for the hand-written 2-query idiom (`raw_sqlx_2query`).
#[derive(Debug, sqlx::FromRow)]
struct RawHop1Row {
    highlight_id: Uuid,
    item_id: Uuid,
    item_name: String,
    item_price_cents: i64,
}

/// Hop-2 row for the hand-written 2-query idiom (`raw_sqlx_2query`).
#[derive(Debug, sqlx::FromRow)]
struct RawHop2Row {
    item_id: Uuid,
    modifier_id: Uuid,
    modifier_name: String,
    modifier_price_delta_cents: i64,
}

/// Group flat 2-hop rows into `highlight -> [(item, [modifiers])]`, matching
/// the nested shape `ousia`'s `collect_with_target()` returns. Shared by
/// `raw_sqlx_1query` and `sea_orm_1query` since both decode to the same
/// column shape.
#[allow(clippy::type_complexity)]
fn group_two_hop_rows(
    rows: impl IntoIterator<
        Item = (
            Uuid,
            Uuid,
            String,
            i64,
            Option<Uuid>,
            Option<String>,
            Option<i64>,
        ),
    >,
) -> HashMap<Uuid, Vec<(Uuid, String, i64, Vec<(Uuid, String, i64)>)>> {
    let mut out: HashMap<Uuid, Vec<(Uuid, String, i64, Vec<(Uuid, String, i64)>)>> =
        HashMap::new();
    let mut slot: HashMap<(Uuid, Uuid), usize> = HashMap::new();
    for (hid, iid, iname, iprice, mid, mname, mdelta) in rows {
        let entry = out.entry(hid).or_default();
        let idx = match slot.get(&(hid, iid)) {
            Some(&i) => i,
            None => {
                entry.push((iid, iname, iprice, Vec::new()));
                let i = entry.len() - 1;
                slot.insert((hid, iid), i);
                i
            }
        };
        if let (Some(mid), Some(mname), Some(mdelta)) = (mid, mname, mdelta) {
            entry[idx].3.push((mid, mname, mdelta));
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared state
// ─────────────────────────────────────────────────────────────────────────────

const N_HIGHLIGHTS: usize = 300;
const N_MODIFIERS: usize = 100;
const ITEMS_PER_HIGHLIGHT: usize = 5;
const MODIFIERS_PER_ITEM: usize = 2;

/// `low_overlap`: items are (almost) unique per highlight — the realistic
/// mealgro shape. `N_HIGHLIGHTS * ITEMS_PER_HIGHLIGHT` gives zero reuse.
const N_ITEMS_LOW_OVERLAP: usize = N_HIGHLIGHTS * ITEMS_PER_HIGHLIGHT;
/// `high_overlap`: a small shared item pool — each item reused ~10× across
/// highlights, the adversarial case for a single flat JOIN.
const N_ITEMS_HIGH_OVERLAP: usize = 150;

struct Ctx {
    engine: Engine,
    ousia_highlight_ids: Vec<Uuid>,

    raw_pool: PgPool,
    orm_db: sea_orm::DatabaseConnection,
    raw_highlight_ids: Vec<Uuid>,
}

unsafe impl Sync for Ctx {}

static STATE_LOW: ousia_bench::BenchHandle<Ctx> = ousia_bench::BenchHandle::new();
static STATE_HIGH: ousia_bench::BenchHandle<Ctx> = ousia_bench::BenchHandle::new();

fn state_low() -> &'static (tokio::runtime::Runtime, Ctx) {
    STATE_LOW.get_or_init(|| {
        let rt = ousia_bench::mt_rt();
        let ctx = rt.block_on(setup(
            "ousia_bench_2h_lo_ousia",
            "ousia_bench_2h_lo_raw",
            N_ITEMS_LOW_OVERLAP,
        ));
        (rt, ctx)
    })
}

fn state_high() -> &'static (tokio::runtime::Runtime, Ctx) {
    STATE_HIGH.get_or_init(|| {
        let rt = ousia_bench::mt_rt();
        let ctx = rt.block_on(setup(
            "ousia_bench_2h_hi_ousia",
            "ousia_bench_2h_hi_raw",
            N_ITEMS_HIGH_OVERLAP,
        ));
        (rt, ctx)
    })
}

macro_rules! run_on {
    ($state:expr, $e:expr) => {
        $state.0.block_on(async { $e })
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Setup
// ─────────────────────────────────────────────────────────────────────────────

async fn setup(ousia_db: &str, raw_db: &str, n_items: usize) -> Ctx {
    let ousia_pool = ousia_bench::connect_db(ousia_db).await;
    let raw_pool = ousia_bench::connect_db(raw_db).await;
    let orm_db = ousia_bench::connect_orm(raw_db).await;

    // --- Ousia side: seed via the Engine so `data`/`index_meta` encoding is
    // guaranteed correct (msgpack `data`, JSONB `index_meta`) — bulk raw
    // inserts are only safe when they match that encoding exactly, and this
    // is setup cost, not measured, so the extra round trips are free. ---
    let adapter = PostgresAdapter::from_pool(ousia_pool.clone());
    adapter.init_schema().await.expect("ousia schema");
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(ousia_pool.clone())));

    let mut item_ids = Vec::with_capacity(n_items);
    for i in 0..n_items {
        let mut item = BenchItem::default();
        item.name = format!("item_{i:04}");
        item.price_cents = (i as i64) * 137 % 5000;
        engine.create_object(&item).await.unwrap();
        item_ids.push(item.id());
    }

    let mut modifier_ids = Vec::with_capacity(N_MODIFIERS);
    for i in 0..N_MODIFIERS {
        let mut m = BenchModifier::default();
        m.name = format!("modifier_{i:04}");
        m.price_delta_cents = (i as i64) * 53 % 1000;
        engine.create_object(&m).await.unwrap();
        modifier_ids.push(m.id());
    }

    let mut ousia_highlight_ids = Vec::with_capacity(N_HIGHLIGHTS);
    for i in 0..N_HIGHLIGHTS {
        let mut h = BenchHighlight::default();
        h.name = format!("highlight_{i:04}");
        engine.create_object(&h).await.unwrap();
        ousia_highlight_ids.push(h.id());
    }

    // Highlight -[position j]-> item[(hi*ITEMS_PER_HIGHLIGHT+j) % n_items].
    // With n_items == N_HIGHLIGHTS*ITEMS_PER_HIGHLIGHT (low_overlap) every
    // item is hit exactly once; with a small n_items (high_overlap) items
    // are heavily reused across highlights.
    for (hi, &hid) in ousia_highlight_ids.iter().enumerate() {
        for j in 0..ITEMS_PER_HIGHLIGHT {
            let iid = item_ids[(hi * ITEMS_PER_HIGHLIGHT + j) % n_items];
            engine
                .create_edge(&BenchHighlightItem {
                    _meta: EdgeMeta::new(hid, iid),
                    position: j as i64,
                })
                .await
                .unwrap();
        }
    }

    // Item -> modifier: seeded once per item (an item's modifiers are its
    // own, independent of which highlights reference it).
    for (ii, &iid) in item_ids.iter().enumerate() {
        for j in 0..MODIFIERS_PER_ITEM {
            let mid = modifier_ids[(ii * MODIFIERS_PER_ITEM + j) % N_MODIFIERS];
            engine
                .create_edge(&BenchItemModifier {
                    _meta: EdgeMeta::new(iid, mid),
                    is_required: j == 0,
                })
                .await
                .unwrap();
        }
    }

    // --- Raw / sea_orm side: same shape, bulk `unnest()` insert. ---
    setup_raw_schema(&raw_pool).await;
    let raw_highlight_ids = seed_raw_bulk(&raw_pool, n_items).await;

    // Freshly created partitions have no stats until analyzed — don't rely
    // on autovacuum's timing (naptime default 60s can easily lose the race
    // against a benchmark that starts querying seconds after seeding,
    // leaving the planner guessing on a table it thinks has 0 rows).
    sqlx::query("ANALYZE").execute(&ousia_pool).await.unwrap();
    sqlx::query("ANALYZE").execute(&raw_pool).await.unwrap();

    Ctx {
        engine,
        ousia_highlight_ids,
        raw_pool,
        orm_db,
        raw_highlight_ids,
    }
}

async fn setup_raw_schema(pool: &PgPool) {
    for ddl in [
        "DROP TABLE IF EXISTS item_modifiers CASCADE",
        "DROP TABLE IF EXISTS highlight_items CASCADE",
        "DROP TABLE IF EXISTS modifiers CASCADE",
        "DROP TABLE IF EXISTS items CASCADE",
        "DROP TABLE IF EXISTS highlights CASCADE",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }

    sqlx::query(
        "CREATE TABLE highlights (
            id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL DEFAULT ''
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE items (
            id           UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
            name         TEXT   NOT NULL DEFAULT '',
            price_cents  BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE TABLE modifiers (
            id                 UUID   PRIMARY KEY DEFAULT gen_random_uuid(),
            name               TEXT   NOT NULL DEFAULT '',
            price_delta_cents  BIGINT NOT NULL DEFAULT 0
        )",
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"CREATE TABLE highlight_items (
            highlight_id UUID   NOT NULL,
            item_id      UUID   NOT NULL,
            position     BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (highlight_id, item_id)
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        r#"CREATE TABLE item_modifiers (
            item_id      UUID    NOT NULL,
            modifier_id  UUID    NOT NULL,
            is_required  BOOLEAN NOT NULL DEFAULT false,
            PRIMARY KEY (item_id, modifier_id)
        )"#,
    )
    .execute(pool)
    .await
    .unwrap();

    for ddl in [
        "CREATE INDEX idx_hi_highlight ON highlight_items(highlight_id)",
        "CREATE INDEX idx_hi_item ON highlight_items(item_id)",
        "CREATE INDEX idx_im_item ON item_modifiers(item_id)",
    ] {
        sqlx::query(ddl).execute(pool).await.unwrap();
    }
}

async fn seed_raw_bulk(pool: &PgPool, n_items: usize) -> Vec<Uuid> {
    let item_ids: Vec<Uuid> = (0..n_items).map(|_| Uuid::now_v7()).collect();
    let item_names: Vec<String> = (0..n_items).map(|i| format!("item_{i:04}")).collect();
    let item_prices: Vec<i64> = (0..n_items).map(|i| (i as i64) * 137 % 5000).collect();
    sqlx::query(
        "INSERT INTO items (id, name, price_cents) \
         SELECT * FROM unnest($1::uuid[], $2::text[], $3::bigint[])",
    )
    .bind(&item_ids)
    .bind(&item_names)
    .bind(&item_prices)
    .execute(pool)
    .await
    .unwrap();

    let modifier_ids: Vec<Uuid> = (0..N_MODIFIERS).map(|_| Uuid::now_v7()).collect();
    let modifier_names: Vec<String> =
        (0..N_MODIFIERS).map(|i| format!("modifier_{i:04}")).collect();
    let modifier_deltas: Vec<i64> = (0..N_MODIFIERS).map(|i| (i as i64) * 53 % 1000).collect();
    sqlx::query(
        "INSERT INTO modifiers (id, name, price_delta_cents) \
         SELECT * FROM unnest($1::uuid[], $2::text[], $3::bigint[])",
    )
    .bind(&modifier_ids)
    .bind(&modifier_names)
    .bind(&modifier_deltas)
    .execute(pool)
    .await
    .unwrap();

    let highlight_ids: Vec<Uuid> = (0..N_HIGHLIGHTS).map(|_| Uuid::now_v7()).collect();
    let highlight_names: Vec<String> =
        (0..N_HIGHLIGHTS).map(|i| format!("highlight_{i:04}")).collect();
    sqlx::query(
        "INSERT INTO highlights (id, name) SELECT * FROM unnest($1::uuid[], $2::text[])",
    )
    .bind(&highlight_ids)
    .bind(&highlight_names)
    .execute(pool)
    .await
    .unwrap();

    let hi_cap = N_HIGHLIGHTS * ITEMS_PER_HIGHLIGHT;
    let mut hi_highlight: Vec<Uuid> = Vec::with_capacity(hi_cap);
    let mut hi_item: Vec<Uuid> = Vec::with_capacity(hi_cap);
    let mut hi_position: Vec<i64> = Vec::with_capacity(hi_cap);
    for (hi, &hid) in highlight_ids.iter().enumerate() {
        for j in 0..ITEMS_PER_HIGHLIGHT {
            hi_highlight.push(hid);
            hi_item.push(item_ids[(hi * ITEMS_PER_HIGHLIGHT + j) % n_items]);
            hi_position.push(j as i64);
        }
    }
    sqlx::query(
        "INSERT INTO highlight_items (highlight_id, item_id, position) \
         SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::bigint[])",
    )
    .bind(&hi_highlight)
    .bind(&hi_item)
    .bind(&hi_position)
    .execute(pool)
    .await
    .unwrap();

    let im_cap = n_items * MODIFIERS_PER_ITEM;
    let mut im_item: Vec<Uuid> = Vec::with_capacity(im_cap);
    let mut im_modifier: Vec<Uuid> = Vec::with_capacity(im_cap);
    let mut im_required: Vec<bool> = Vec::with_capacity(im_cap);
    for (ii, &iid) in item_ids.iter().enumerate() {
        for j in 0..MODIFIERS_PER_ITEM {
            im_item.push(iid);
            im_modifier.push(modifier_ids[(ii * MODIFIERS_PER_ITEM + j) % N_MODIFIERS]);
            im_required.push(j == 0);
        }
    }
    sqlx::query(
        "INSERT INTO item_modifiers (item_id, modifier_id, is_required) \
         SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::bool[])",
    )
    .bind(&im_item)
    .bind(&im_modifier)
    .bind(&im_required)
    .execute(pool)
    .await
    .unwrap();

    highlight_ids
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL shared by raw_sqlx_1query and sea_orm_1query
// ─────────────────────────────────────────────────────────────────────────────

const TWO_HOP_SQL: &str = r#"
    SELECT
        hi.highlight_id AS highlight_id,
        i.id AS item_id, i.name AS item_name, i.price_cents AS item_price_cents,
        m.id AS modifier_id, m.name AS modifier_name,
        m.price_delta_cents AS modifier_price_delta_cents
    FROM highlight_items hi
    JOIN items i ON hi.item_id = i.id
    LEFT JOIN item_modifiers im ON im.item_id = i.id
    LEFT JOIN modifiers m ON im.modifier_id = m.id
    WHERE hi.highlight_id = ANY($1)
"#;

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks
// ─────────────────────────────────────────────────────────────────────────────

fn bench_two_hop_group(
    c: &mut Criterion,
    group_name: &str,
    state: &'static (tokio::runtime::Runtime, Ctx),
) {
    let (rt, ctx) = state;
    let mut group = c.benchmark_group(group_name);

    // NEW (2.1.0): one query, native chained traversal.
    group.bench_function("ousia_v2_1_1query", |b| {
        b.iter(|| {
            run_on!(state, {
                let _: ousia::TwoHopMap<BenchItem, BenchItemModifier, BenchModifier> = ctx
                    .engine
                    .batch_edge::<BenchHighlightItem, BenchHighlight, BenchItem>(
                        &ctx.ousia_highlight_ids,
                    )
                    .then_edge::<BenchItemModifier, BenchModifier>()
                    .collect_with_target()
                    .await
                    .unwrap();
            })
        })
    });

    // PREVIOUS VERSION (2.0.2): two sequential batch calls, deduped and
    // joined by hand in Rust — the idiom `plan_2.1.md` describes as the best
    // available before 2.1.0 shipped.
    group.bench_function("ousia_v2_0_2query", |b| {
        b.iter(|| {
            run_on!(state, {
                let hop1: HashMap<Uuid, Vec<ObjectEdge<BenchHighlightItem, BenchItem>>> = ctx
                    .engine
                    .query_edges_with_targets_batch::<BenchHighlightItem, BenchItem>(
                        &ctx.ousia_highlight_ids,
                        &[],
                        EdgeQuery::default(),
                    )
                    .await
                    .unwrap();

                let mut item_ids: Vec<Uuid> = hop1
                    .values()
                    .flatten()
                    .map(|oe| oe.object().id())
                    .collect();
                item_ids.sort_unstable();
                item_ids.dedup();

                let hop2: HashMap<Uuid, Vec<ObjectEdge<BenchItemModifier, BenchModifier>>> = ctx
                    .engine
                    .query_edges_with_targets_batch::<BenchItemModifier, BenchModifier>(
                        &item_ids,
                        &[],
                        EdgeQuery::default(),
                    )
                    .await
                    .unwrap();

                let empty: Vec<ObjectEdge<BenchItemModifier, BenchModifier>> = Vec::new();
                let _: HashMap<Uuid, Vec<(&BenchItem, &Vec<ObjectEdge<BenchItemModifier, BenchModifier>>)>> =
                    hop1.iter()
                        .map(|(hid, items)| {
                            let mapped: Vec<(&BenchItem, &Vec<ObjectEdge<BenchItemModifier, BenchModifier>>)> =
                                items
                                    .iter()
                                    .map(|oe| {
                                        let item = oe.object();
                                        let leaves =
                                            hop2.get(&item.id()).unwrap_or(&empty);
                                        (item, leaves)
                                    })
                                    .collect();
                            (*hid, mapped)
                        })
                        .collect();
            })
        })
    });

    // Hand-written single JOIN — floor reference for ousia_v2_1_1query.
    group.bench_function("raw_sqlx_1query", |b| {
        b.iter(|| {
            run_on!(state, {
                let rows: Vec<RawTwoHopRow> = sqlx::query_as(TWO_HOP_SQL)
                    .bind(&ctx.raw_highlight_ids)
                    .fetch_all(&ctx.raw_pool)
                    .await
                    .unwrap();
                let _ = group_two_hop_rows(rows.into_iter().map(|r| {
                    (
                        r.highlight_id,
                        r.item_id,
                        r.item_name,
                        r.item_price_cents,
                        r.modifier_id,
                        r.modifier_name,
                        r.modifier_price_delta_cents,
                    )
                }));
            })
        })
    });

    // Hand-written two queries + Rust grouping — floor reference for
    // ousia_v2_0_2query.
    group.bench_function("raw_sqlx_2query", |b| {
        b.iter(|| {
            run_on!(state, {
                let hop1: Vec<RawHop1Row> = sqlx::query_as(
                    "SELECT hi.highlight_id, i.id AS item_id, i.name AS item_name, \
                            i.price_cents AS item_price_cents \
                     FROM highlight_items hi JOIN items i ON hi.item_id = i.id \
                     WHERE hi.highlight_id = ANY($1)",
                )
                .bind(&ctx.raw_highlight_ids)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();

                let mut item_ids: Vec<Uuid> = hop1.iter().map(|r| r.item_id).collect();
                item_ids.sort_unstable();
                item_ids.dedup();

                let hop2: Vec<RawHop2Row> = sqlx::query_as(
                    "SELECT im.item_id, m.id AS modifier_id, m.name AS modifier_name, \
                            m.price_delta_cents AS modifier_price_delta_cents \
                     FROM item_modifiers im JOIN modifiers m ON im.modifier_id = m.id \
                     WHERE im.item_id = ANY($1)",
                )
                .bind(&item_ids)
                .fetch_all(&ctx.raw_pool)
                .await
                .unwrap();

                let mut hop2_map: HashMap<Uuid, Vec<&RawHop2Row>> = HashMap::new();
                for r in &hop2 {
                    hop2_map.entry(r.item_id).or_default().push(r);
                }
                let empty: Vec<&RawHop2Row> = Vec::new();
                let mut by_highlight: HashMap<Uuid, Vec<(&RawHop1Row, &Vec<&RawHop2Row>)>> =
                    HashMap::new();
                for r in &hop1 {
                    let leaves = hop2_map.get(&r.item_id).unwrap_or(&empty);
                    by_highlight.entry(r.highlight_id).or_default().push((r, leaves));
                }
                let _ = by_highlight;
            })
        })
    });

    // sea_orm has no native multi-hop batch join — `find_by_statement` on
    // the same double-JOIN SQL is the idiomatic escape hatch, same as
    // `bench_preload_forward` in `ousia_edges.rs`.
    group.bench_function("sea_orm_1query", |b| {
        b.iter(|| {
            rt.block_on(async {
                let rows: Vec<OrmTwoHopRow> = OrmTwoHopRow::find_by_statement(
                    Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        TWO_HOP_SQL,
                        [ctx.raw_highlight_ids.clone().into()],
                    ),
                )
                .all(&ctx.orm_db)
                .await
                .unwrap();
                let _ = group_two_hop_rows(rows.into_iter().map(|r| {
                    (
                        r.highlight_id,
                        r.item_id,
                        r.item_name,
                        r.item_price_cents,
                        r.modifier_id,
                        r.modifier_name,
                        r.modifier_price_delta_cents,
                    )
                }));
            })
        })
    });

    group.finish();
}

fn run_all(c: &mut Criterion) {
    bench_two_hop_group(c, "batch_two_hop_traversal_low_overlap", state_low());
    bench_two_hop_group(c, "batch_two_hop_traversal_high_overlap", state_high());
}

criterion_group! {
    name = ousia_two_hop;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = run_all
}
criterion_main!(ousia_two_hop);
