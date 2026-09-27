use super::*;

#[tokio::test]
async fn test_query_all_index_value_variants() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let uid_a = uuid::Uuid::now_v7();
    let uid_b = uuid::Uuid::now_v7();
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let t1 = t0 + chrono::Duration::days(1);
    let t2 = t0 + chrono::Duration::days(2);

    for (name, count, price, active, uid, ts, tags, scores) in [
        (
            "alpha",
            1i64,
            1.5f64,
            true,
            uid_a,
            t0,
            vec!["red", "blue"],
            vec![10i64, 20],
        ),
        (
            "bravo",
            2,
            2.5,
            false,
            uid_b,
            t1,
            vec!["green"],
            vec![20, 30],
        ),
        (
            "charlie",
            3,
            3.5,
            true,
            uid_a,
            t2,
            vec!["blue", "green"],
            vec![10, 30],
        ),
    ] {
        let mut v = Variants::default();
        v.name = name.into();
        v.count = count;
        v.price = price;
        v.active = active;
        v.uid = uid;
        v.occurred_at = EventTime(ts);
        v.tags = tags.into_iter().map(String::from).collect();
        v.scores = scores;
        engine.create_object(&v).await.unwrap();
    }

    // String (IndexValue::String)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.name, "alpha"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.name, "alpha"))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "String where_ne");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_begins_with(&Variants::FIELDS.name, "br"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_begins_with");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.name, "ar"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_contains 'ar' → 'charlie' only");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_not_contains(&Variants::FIELDS.name, "a"))
        .await
        .unwrap();
    assert_eq!(r.len(), 0, "String where_not_contains 'a' → none");

    // Int (IndexValue::Int)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Int where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_ne");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.count, 1i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gte(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_gte");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lt(&Variants::FIELDS.count, 3i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_lt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_lte");

    // Float (IndexValue::Float) — values chosen so equality is exact in IEEE 754
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.price, 1.5f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Float where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.price, 2.0f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Float where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.price, 2.5f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Float where_lte");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.price, vec![1.5f64, 3.5]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Float where_in");

    // Bool (IndexValue::Bool)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.active, true))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Bool where_eq true");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.active, true))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Bool where_ne true");

    // Uuid (IndexValue::Uuid)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.uid, uid_a))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Uuid where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.uid, uid_a))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Uuid where_ne");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.uid, vec![uid_b]))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Vec<Uuid> where_in");
    let r: Vec<Variants> = engine
        .query_objects(
            Query::default().where_in(&Variants::FIELDS.uid, vec![uid_a, uid_b, uuid::Uuid::now_v7()]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 3, "Vec<Uuid> where_in, all");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.uid, vec![uid_a]))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Vec<Uuid> where_not_in");

    // Timestamp (IndexValue::Timestamp)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.occurred_at, EventTime(t0)))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Timestamp where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.occurred_at, EventTime(t0)))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Timestamp where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.occurred_at, EventTime(t1)))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Timestamp where_lte");

    // Array of String (IndexValue::Array(Vec<IndexValueInner::String>))
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.tags, vec!["blue"]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<String> where_contains [blue]");
    let r: Vec<Variants> = engine
        .query_objects(
            Query::default().where_contains_all(&Variants::FIELDS.tags, vec!["blue", "green"]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Array<String> where_contains_all [blue,green]");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_not_contains(&Variants::FIELDS.tags, vec!["red"]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<String> where_not_contains [red]");

    // Array of Int (IndexValue::Array(Vec<IndexValueInner::Int>))
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.scores, vec![10i64]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<Int> where_contains [10]");
    let r: Vec<Variants> = engine
        .query_objects(
            Query::default().where_contains_all(&Variants::FIELDS.scores, vec![10i64, 30]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Array<Int> where_contains_all [10,30]");
}

/// Regression: `where_eq(field, false)` and `where_ne(field, true)` must
/// return the same set for boolean fields. Before the fix, a row whose
/// `index_meta` was missing the field entirely (e.g. legacy data written
/// before the index was declared) satisfied `NOT @>` but not `@>`,
/// producing asymmetric results.
#[tokio::test]
async fn test_query_ne_requires_key_existence() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut explicit_false = Variants::default();
    explicit_false.name = "explicit-false".into();
    explicit_false.active = false;
    engine.create_object(&explicit_false).await.unwrap();

    let mut explicit_true = Variants::default();
    explicit_true.name = "explicit-true".into();
    explicit_true.active = true;
    engine.create_object(&explicit_true).await.unwrap();

    // Inject a legacy row whose index_meta is missing `active` entirely —
    // simulates data written before the `active` index existed.
    let legacy_id = uuid::Uuid::now_v7();
    let now = chrono::Utc::now();
    sqlx::query(
        r#"INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
           VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
    )
    .bind(legacy_id)
    .bind(Variants::TYPE)
    .bind(system_owner())
    .bind(now)
    .bind(now)
    // v2: `data` is BYTEA / msgpack. Encode the legacy payload as a
    // msgpack map matching the struct's field order.
    .bind({
        use rmp_serde::Serializer;
        use serde::Serialize as _;
        let mut buf = Vec::new();
        let mut ser = Serializer::new(&mut buf).with_struct_map();
        let payload = serde_json::json!({
            "id": legacy_id,
            "owner": system_owner(),
            "created_at": now,
            "updated_at": now,
            "name": "legacy",
            "count": 0,
            "price": 0.0,
            "active": false,
            "uid": uuid::Uuid::nil(),
            "occurred_at": chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
            "tags": [],
            "scores": []
        });
        payload.serialize(&mut ser).unwrap();
        buf
    })
    .bind(serde_json::json!({"name": "legacy"}))
    .execute(&pool)
    .await
    .unwrap();

    let eq_false: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.active, false))
        .await
        .unwrap();
    let ne_true: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.active, true))
        .await
        .unwrap();

    assert_eq!(
        eq_false.len(),
        ne_true.len(),
        "where_eq(false) and where_ne(true) must agree on the same data"
    );
    assert_eq!(
        eq_false.len(),
        1,
        "only the explicit `active=false` row should match"
    );
    assert_eq!(eq_false[0].id(), explicit_false.id());
}

// ── Negated comparisons & random sort ───────────────────────────────────────


#[tokio::test]
async fn test_query_not_begins_with() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("rust-a", 1, &[]), ("rust-b", 2, &[]), ("go-c", 3, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_begins_with(&Variants::FIELDS.name, "rust"))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["go-c"]);
}

#[tokio::test]
async fn test_query_not_in_strings_and_ints() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("a", 1, &[]), ("b", 2, &[]), ("c", 3, &[]), ("d", 4, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.name, vec!["a", "b"]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["c", "d"]);

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.count, vec![1i64, 4]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b", "c"]);

    // AND-ed with a range filter: count >= 2 AND name NOT IN [c]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_gte(&Variants::FIELDS.count, 2i64)
                .where_not_in(&Variants::FIELDS.name, vec!["c"]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b", "d"]);

    // OR form: name = a OR count NOT IN [1, 2, 3]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_eq(&Variants::FIELDS.name, "a")
                .or_not_in(&Variants::FIELDS.count, vec![1i64, 2, 3]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "d"]);
}

#[tokio::test]
async fn test_query_in_strings_and_ints() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("a", 1, &[]), ("b", 2, &[]), ("c", 3, &[]), ("d", 4, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.name, vec!["a", "b", "zz"]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "b"]);

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.count, vec![1i64, 4]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "d"]);

    // AND-ed with a range filter: count >= 2 AND name IN [a, b, c]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_gte(&Variants::FIELDS.count, 2i64)
                .where_in(&Variants::FIELDS.name, vec!["a", "b", "c"]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b", "c"]);

    // OR form: name = a OR count IN [3, 4]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_eq(&Variants::FIELDS.name, "a")
                .or_in(&Variants::FIELDS.count, vec![3i64, 4]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "c", "d"]);

    // a single value is equality
    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.name, "c"))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["c"]);
}

/// "In the empty set" is false for every row. Dropping the filter instead
/// would return the whole table (e.g. a feed for a user who follows nobody).
#[tokio::test]
async fn test_query_in_empty_list_matches_nothing() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("a", 1, &[]), ("b", 2, &[]), ("c", 3, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.name, Vec::<String>::new()))
        .await
        .unwrap();
    assert!(rows.is_empty(), "where_in([]) must match nothing: {:?}", sorted_names(&rows));

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_in(&Variants::FIELDS.uid, Vec::<uuid::Uuid>::new()))
        .await
        .unwrap();
    assert!(rows.is_empty(), "where_in(Vec<Uuid>[]) must match nothing");

    let n = engine
        .count_objects::<Variants>(Some(
            Query::default().where_in(&Variants::FIELDS.count, Vec::<i64>::new()),
        ))
        .await
        .unwrap();
    assert_eq!(n, 0);

    // parameters after the FALSE still line up
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_in(&Variants::FIELDS.name, Vec::<String>::new())
                .or_eq(&Variants::FIELDS.name, "b")
                .where_gte(&Variants::FIELDS.count, 1i64),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b"]);

    // the negation still keeps everything
    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.name, Vec::<String>::new()))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "b", "c"]);
}

#[tokio::test]
async fn test_query_not_contains_all() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(
        &engine,
        &[("xy", 1, &["x", "y"]), ("xyz", 2, &["x", "y", "z"]), ("x", 3, &["x"]), ("yz", 4, &["y", "z"])],
    )
    .await;

    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default().where_not_contains_all(&Variants::FIELDS.tags, vec!["x", "y"]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["x", "yz"]);
}

// ============================================================
// Composite indexes: ousia's queries use them
// ============================================================
//
// Index use is checked from the stats counters after running the engine's
// own query, so these tests see the SQL ousia actually sends.

/// A one-connection pool on the same database, so the stats flush in
/// `index_stats` happens on the backend that ran the engine's query.
async fn single_connection(pool: &PgPool) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap()
}

/// `(idx_scan, idx_tup_read)` for `index`, after flushing this backend's
/// pending stats.
async fn index_stats(single: &PgPool, index: &str) -> (i64, i64) {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(single)
        .await
        .unwrap();
    sqlx::query_as(
        "SELECT idx_scan, idx_tup_read FROM pg_stat_user_indexes WHERE indexrelname = $1",
    )
    .bind(index)
    .fetch_one(single)
    .await
    .unwrap()
}

/// Two quiet actors with five events each, written through the engine, then
/// 4 000 events from 40 busy actors (100 each) spread over the hour around
/// them, then `ANALYZE`. Busy events are written directly, with an empty
/// `data` map: they decode with default fields, their `index_meta` is real. Walking the type in `created_at` order to find the
/// quiet actors' events reads half the busy rows first, and a `created_at`
/// cursor among the quiet events still leaves half of them. A third of the
/// busy events are `open`, so an equality on `state` is not selective either.
async fn seed_feed(engine: &Engine, pool: &PgPool) -> (uuid::Uuid, uuid::Uuid) {
    let (a, b) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
    for i in 0..10i64 {
        let mut e = FeedEvent::default();
        e.actor = if i % 2 == 0 { a } else { b };
        e.score = 1_000_000 + i;
        e.location_key = "quiet".into();
        e.state = if i < 6 { "open".into() } else { "closed".into() };
        e.region = "quiet".into();
        e.tier = if i < 4 { 7 } else { 8 };
        engine.create_object(&e).await.unwrap();
    }
    sqlx::raw_sql(
        "INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta) \
         SELECT gen_random_uuid(), 'FeedEvent', gen_random_uuid(), \
                now() + (g - 2000) * interval '1 second', \
                now() + (g - 2000) * interval '1 second', \
                '\\x80'::bytea, \
                jsonb_build_object( \
                  'actor', busy_actor(g % 40), \
                  'score', g % 1000, \
                  'location_key', 'lk' || (g % 50), \
                  'state', (ARRAY['open', 'closed', 'archived'])[g % 3 + 1], \
                  'region', 'r' || (g % 20), \
                  'tier', g % 10) \
         FROM generate_series(1, 4000) g; \
         ANALYZE objects_feedevent;",
    )
    .execute(pool)
    .await
    .unwrap();
    (a, b)
}

/// The uuid text `seed_feed` gives busy actor `n` (0–39).
fn busy_actor(n: u32) -> uuid::Uuid {
    uuid::Uuid::parse_str(&format!("00000000-0000-0000-0000-{n:012}")).unwrap()
}

async fn setup_feed() -> (ContainerAsync<Postgres>, PgPool, PgPool, Engine, uuid::Uuid, uuid::Uuid) {
    let (r, pool) = setup_test_db().await;
    PostgresAdapter::from_pool(pool.clone()).init_schema().await.unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION busy_actor(n int) RETURNS text IMMUTABLE LANGUAGE sql \
         AS $$ SELECT '00000000-0000-0000-0000-' || lpad(n::text, 12, '0') $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    let single = single_connection(&pool).await;
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(single.clone())));
    let (a, b) = seed_feed(&engine, &pool).await;
    (r, pool, single, engine, a, b)
}

/// Run `query` through the engine and assert Postgres scanned `index` for it.
/// Returns the rows and how many index entries were read.
async fn query_via_index(
    engine: &Engine,
    single: &PgPool,
    index: &str,
    query: Query,
) -> (Vec<FeedEvent>, i64) {
    let (scans, read) = index_stats(single, index).await;
    let rows: Vec<FeedEvent> = engine.query_objects(query).await.unwrap();
    let (scans_after, read_after) = index_stats(single, index).await;
    assert!(scans_after > scans, "the engine's query did not use {index}");
    (rows, read_after - read)
}

fn assert_newest_first(rows: &[FeedEvent]) {
    assert!(
        rows.windows(2).all(|w| w[0].created_at() >= w[1].created_at()),
        "not sorted by created_at desc"
    );
}

fn ids(rows: &[FeedEvent]) -> Vec<uuid::Uuid> {
    rows.iter().map(|e| e.id()).collect()
}

const ACTOR_INDEX: &str = "objects_feedevent_actor_created_at_desc_idx";

#[tokio::test]
async fn test_composite_index_serves_where_in_sorted_by_created_at() {
    let (_r, _pool, single, engine, a, b) = setup_feed().await;

    let (rows, _) = query_via_index(
        &engine,
        &single,
        ACTOR_INDEX,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.actor, vec![a, b])
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21),
    )
    .await;
    assert_eq!(rows.len(), 10);
    assert!(rows.iter().all(|e| e.actor == a || e.actor == b));
    assert_newest_first(&rows);

    // A feed page: an equality on another field (a GIN `@>`) and a
    // `created_at` cursor on top of the `where_in`.
    let cursor = rows[3].created_at();
    let (page, _) = query_via_index(
        &engine,
        &single,
        ACTOR_INDEX,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.actor, vec![a, b])
            .where_eq(&FeedEvent::FIELDS.state, "open")
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21)
            .where_lt(&FeedEvent::FIELDS.created_at, cursor),
    )
    .await;
    let expected: Vec<uuid::Uuid> = rows
        .iter()
        .filter(|e| e.created_at() < cursor && e.state == "open")
        .map(|e| e.id())
        .collect();
    assert!(!expected.is_empty());
    assert_eq!(ids(&page), expected);
}

#[tokio::test]
async fn test_fan_in_reads_about_limit_index_entries_per_value() {
    let (_r, _pool, single, engine, _, _) = setup_feed().await;

    // Five actors with 100 events each: fetching all of them and sorting
    // would read 500 index entries; one ordered read per actor stops at 5.
    let actors: Vec<uuid::Uuid> = (0..5).map(busy_actor).collect();
    let (rows, read) = query_via_index(
        &engine,
        &single,
        ACTOR_INDEX,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.actor, actors.clone())
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(5),
    )
    .await;
    assert_eq!(rows.len(), 5);
    assert_newest_first(&rows);
    assert!(read <= 5 * (5 + 1), "read {read} index entries for 5 actors × limit 5");

    // Same answer as the unlimited query cut to 5.
    let all: Vec<FeedEvent> = engine
        .query_objects(
            Query::wide()
                .where_in(&FeedEvent::FIELDS.actor, actors)
                .sort_desc(&FeedEvent::FIELDS.created_at),
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 500);
    assert_eq!(ids(&rows), ids(&all[..5]));
}

#[tokio::test]
async fn test_fan_in_pages_exactly_through_ties_and_duplicate_values() {
    let (r, pool) = setup_test_db().await;
    let _r = r;
    PostgresAdapter::from_pool(pool.clone()).init_schema().await.unwrap();
    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(pool.clone())));

    // Three actors, 7 events each, many sharing a timestamp across actors
    // and within one actor, so the `id` tie-break decides the order.
    let actors: Vec<uuid::Uuid> = (0..3).map(|_| uuid::Uuid::now_v7()).collect();
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_800_000_000, 0).unwrap();
    let mut all = Vec::new();
    for i in 0..21i64 {
        let mut e = FeedEvent::default();
        e.actor = actors[(i % 3) as usize];
        e._meta.created_at = t0 + chrono::Duration::seconds(i / 4);
        engine.create_object(&e).await.unwrap();
        all.push((e.created_at(), e.id()));
    }
    all.sort_by(|x, y| y.cmp(x));
    let expected: Vec<uuid::Uuid> = all.iter().map(|(_, id)| *id).collect();

    // Page 4 at a time, repeating a value in the list.
    let values = vec![actors[0], actors[1], actors[2], actors[0]];
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let mut q = Query::wide()
            .where_in(&FeedEvent::FIELDS.actor, values.clone())
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(4);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let page: Vec<FeedEvent> = engine.query_objects(q).await.unwrap();
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 4);
        cursor = page.last().map(|e| e.id());
        seen.extend(ids(&page));
    }
    assert_eq!(seen, expected);

    // Ascending, too (the index is read backwards).
    let asc: Vec<FeedEvent> = engine
        .query_objects(
            Query::wide()
                .where_in(&FeedEvent::FIELDS.actor, values)
                .sort_asc(&FeedEvent::FIELDS.created_at)
                .with_limit(5),
        )
        .await
        .unwrap();
    let mut oldest: Vec<(chrono::DateTime<chrono::Utc>, uuid::Uuid)> = all.clone();
    oldest.sort_by(|x, y| x.0.cmp(&y.0).then(y.1.cmp(&x.1)));
    assert_eq!(ids(&asc), oldest[..5].iter().map(|(_, id)| *id).collect::<Vec<_>>());
}

#[tokio::test]
async fn test_where_eq_on_a_leading_field_uses_the_composite_index() {
    let (_r, _pool, single, engine, a, _) = setup_feed().await;

    let (rows, _) = query_via_index(
        &engine,
        &single,
        ACTOR_INDEX,
        Query::wide()
            .where_eq(&FeedEvent::FIELDS.actor, a)
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(3),
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|e| e.actor == a));
    assert_newest_first(&rows);

    // An actor with 100 events: the newest 3 come off the index in order.
    let (rows, read) = query_via_index(
        &engine,
        &single,
        ACTOR_INDEX,
        Query::wide()
            .where_eq(&FeedEvent::FIELDS.actor, busy_actor(7))
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(3),
    )
    .await;
    assert_eq!(rows.len(), 3);
    assert_newest_first(&rows);
    assert!(read < 100, "read all {read} of the actor's index entries for limit 3");

    // `state` is second in its index and `location_key` isn't pinned, so
    // this stays on the GIN index — and still answers correctly.
    let open: Vec<FeedEvent> = engine
        .query_objects(Query::wide().where_eq(&FeedEvent::FIELDS.state, "open"))
        .await
        .unwrap();
    assert_eq!(open.len(), 6 + 1333, "6 quiet events + every third busy one");
    let counted = engine
        .count_objects::<FeedEvent>(Some(Query::wide().where_eq(&FeedEvent::FIELDS.actor, a)))
        .await
        .unwrap();
    assert_eq!(counted, 5);
}

#[tokio::test]
async fn test_composite_index_on_an_int_field_matches_the_bigint_cast() {
    let (_r, pool, single, engine, _, _) = setup_feed().await;
    let index = "objects_feedevent_score_created_at_desc_idx";

    let (rows, _) = query_via_index(
        &engine,
        &single,
        index,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.score, vec![1_000_001i64, 1_000_002])
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21),
    )
    .await;
    assert_eq!(rows.len(), 2);

    // A range comparison is not fanned in; the plain query builds the same
    // `::bigint` expression the index holds.
    let plan: String = sqlx::query_scalar::<_, String>(
        "EXPLAIN SELECT o.id FROM objects o \
         WHERE o.type = 'FeedEvent' AND o.owner > '00000000-0000-0000-0000-000000000000' \
           AND (o.index_meta->>'score')::bigint > $1 \
         ORDER BY o.created_at DESC, o.id DESC LIMIT 21",
    )
    .bind(999_999i64)
    .fetch_all(&pool)
    .await
    .unwrap()
    .join("\n");
    assert!(plan.contains(index), "plan does not use {index}:\n{plan}");
    let (rows, _) = query_via_index(
        &engine,
        &single,
        index,
        Query::wide()
            .where_gt(&FeedEvent::FIELDS.score, 999_999i64)
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21),
    )
    .await;
    assert_eq!(rows.len(), 10);
    assert_newest_first(&rows);
}

#[tokio::test]
async fn test_composite_index_over_two_index_meta_fields() {
    let (_r, _pool, single, engine, _, _) = setup_feed().await;

    let (rows, _) = query_via_index(
        &engine,
        &single,
        "objects_feedevent_location_key_state_idx",
        Query::wide()
            .where_in(&FeedEvent::FIELDS.location_key, vec!["quiet"])
            .where_in(&FeedEvent::FIELDS.state, vec!["open"]),
    )
    .await;
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|e| e.location_key == "quiet" && e.state == "open"));
}

#[tokio::test]
async fn test_composite_index_over_two_index_meta_fields_and_created_at() {
    let (_r, _pool, single, engine, _, _) = setup_feed().await;
    let index = "objects_feedevent_region_tier_created_at_desc_idx";

    let all: Vec<FeedEvent> = engine
        .query_objects(
            Query::wide()
                .where_in(&FeedEvent::FIELDS.region, vec!["quiet"])
                .sort_desc(&FeedEvent::FIELDS.created_at),
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 10);
    let cursor = all[2].created_at();

    // Every element used: `region` fanned in over two values, `tier` pinned
    // by an equality, `created_at` as the order and the cursor.
    let (page, read) = query_via_index(
        &engine,
        &single,
        index,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.region, vec!["quiet", "r1"])
            .where_eq(&FeedEvent::FIELDS.tier, 7i64)
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21)
            .where_lt(&FeedEvent::FIELDS.created_at, cursor),
    )
    .await;
    let expected: Vec<uuid::Uuid> = all
        .iter()
        .filter(|e| e.tier == 7 && e.created_at() < cursor)
        .map(|e| e.id())
        .collect();
    assert!(!expected.is_empty());
    // No `r1` event has tier 7.
    assert_eq!(ids(&page), expected);
    assert!(read <= 2 * 22, "read {read} index entries");

    // The leading element alone is enough to seek on.
    let (rows, _) = query_via_index(
        &engine,
        &single,
        index,
        Query::wide()
            .where_in(&FeedEvent::FIELDS.region, vec!["quiet"])
            .sort_desc(&FeedEvent::FIELDS.created_at)
            .with_limit(21),
    )
    .await;
    assert_eq!(rows.len(), 10);
    assert_newest_first(&rows);
}
