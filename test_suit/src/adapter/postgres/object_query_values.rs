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
