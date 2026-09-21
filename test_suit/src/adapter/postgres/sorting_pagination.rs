use super::*;

#[tokio::test]
async fn test_query_sort_random() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let names: Vec<String> = (0..20).map(|i| format!("v{i:02}")).collect();
    let rows: Vec<(&str, i64, &[&str])> =
        names.iter().enumerate().map(|(i, n)| (n.as_str(), i as i64, &[][..])).collect();
    seed_variants(&engine, &rows).await;

    let mut orders = std::collections::HashSet::new();
    for _ in 0..5 {
        let rows: Vec<Variants> = engine
            .query_objects(Query::default().sort_random())
            .await
            .unwrap();
        assert_eq!(rows.len(), 20);
        orders.insert(rows.iter().map(|v| v.name.clone()).collect::<Vec<_>>());
    }
    assert!(orders.len() > 1, "5 random orderings of 20 rows were identical");

    // composes with filters and limit
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_lt(&Variants::FIELDS.count, 10i64)
                .sort_random()
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|v| v.count < 10));
}

/// Walk `query` page by page (cursor = last id of the previous page) and
/// return the ids in visit order.
async fn paged_ids<T: Object>(engine: &Engine, query: impl Fn() -> Query, page: u32) -> Vec<uuid::Uuid> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let mut q = query().with_limit(page);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let rows: Vec<T> = engine.query_objects(q).await.unwrap();
        out.extend(rows.iter().map(|r| r.id()));
        match rows.last() {
            Some(last) if rows.len() == page as usize => cursor = Some(last.id()),
            _ => return out,
        }
    }
}

#[tokio::test]
async fn test_sort_orders_numbers_and_timestamps_by_value() {
    use chrono::{Duration, DurationRound};
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Text order would be 10, 100, 2, 9 and -1.5, 10.25, 2.5. Timestamps are
    // stored with 0, 3 or 6 fractional digits, whose text order is wrong too.
    let t0 = chrono::Utc::now().duration_trunc(Duration::seconds(1)).unwrap();
    for (name, count, price, at) in [
        ("a", 10i64, 10.25, t0 + Duration::microseconds(500_001)),
        ("b", 2, -1.5, t0),
        ("c", 100, 2.5, t0 + Duration::seconds(1)),
        ("d", 9, 9.0, t0 + Duration::milliseconds(500)),
    ] {
        let mut v = Variants::default();
        v.name = name.into();
        v.count = count;
        v.price = price;
        v.occurred_at = EventTime(at);
        engine.create_object(&v).await.unwrap();
    }
    let names = |rows: Vec<Variants>| rows.into_iter().map(|v| v.name).collect::<Vec<_>>();
    let asc = |f| Query::default().sort_asc(f);

    assert_eq!(names(engine.query_objects(asc(&Variants::FIELDS.count)).await.unwrap()), ["b", "d", "a", "c"]);
    assert_eq!(names(engine.query_objects(asc(&Variants::FIELDS.price)).await.unwrap()), ["b", "c", "d", "a"]);
    assert_eq!(names(engine.query_objects(asc(&Variants::FIELDS.occurred_at)).await.unwrap()), ["b", "d", "a", "c"]);
    assert_eq!(
        names(engine.query_objects(Query::default().sort_desc(&Variants::FIELDS.count)).await.unwrap()),
        ["c", "a", "d", "b"]
    );
}

#[tokio::test]
async fn test_edge_sort_orders_numbers_by_value() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = Hub::default();
    hub.name = "hub".into();
    engine.create_object(&hub).await.unwrap();
    for position in [10i64, 2, 100, 9] {
        let spoke = Spoke::default();
        engine.create_object(&spoke).await.unwrap();
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub.id(), spoke.id()),
                position,
            })
            .await
            .unwrap();
    }
    let positions = |edges: Vec<HubSpoke>| edges.into_iter().map(|e| e.position).collect::<Vec<_>>();

    let edges = engine
        .query_edges::<HubSpoke>(hub.id(), EdgeQuery::default().sort_asc(&HubSpoke::FIELDS.position))
        .await
        .unwrap();
    assert_eq!(positions(edges), [2, 9, 10, 100]);

    let edges = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_sort_desc(&HubSpoke::FIELDS.position)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(positions(edges), [100, 10, 9, 2]);
}

#[tokio::test]
async fn test_cursor_pagination_follows_field_sorts() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    // duplicate names (ties), and one row whose index_meta lacks `name` (sorts as NULL)
    let mut missing = Variants::default();
    missing.name = "zz".into();
    let mut record = ObjectRecord::from_object(&missing);
    record.index_meta = serde_json::json!({});
    adapter.insert_object(record).await.unwrap();
    let engine = Engine::new(Box::new(adapter));
    for (name, count) in [("c", 1i64), ("a", 2), ("b", 3), ("a", 4), ("c", 5), ("b", 6), ("a", 7)] {
        let mut v = Variants::default();
        v.name = name.into();
        v.count = count;
        engine.create_object(&v).await.unwrap();
    }

    let cases: Vec<(&str, Box<dyn Fn() -> Query>)> = vec![
        ("name asc", Box::new(|| Query::default().sort_asc(&Variants::FIELDS.name))),
        ("name desc", Box::new(|| Query::default().sort_desc(&Variants::FIELDS.name))),
        ("created_at asc", Box::new(|| Query::default().sort_asc(&Variants::FIELDS.created_at))),
        ("created_at desc", Box::new(|| Query::default().sort_desc(&Variants::FIELDS.created_at))),
        (
            "name asc, count desc",
            Box::new(|| {
                Query::default()
                    .sort_asc(&Variants::FIELDS.name)
                    .sort_desc(&Variants::FIELDS.count)
            }),
        ),
        (
            "filtered, name desc",
            Box::new(|| {
                Query::default()
                    .where_gt(&Variants::FIELDS.count, 1i64)
                    .sort_desc(&Variants::FIELDS.name)
            }),
        ),
    ];
    for (label, query) in cases {
        let full: Vec<uuid::Uuid> = engine
            .query_objects::<Variants>(query())
            .await
            .unwrap()
            .iter()
            .map(|v| v.id())
            .collect();
        for page in [1, 2, 3] {
            let paged = paged_ids::<Variants>(&engine, &query, page).await;
            assert_eq!(paged, full, "{label}, page size {page}");
        }
    }
}

#[tokio::test]
async fn test_cursor_pagination_follows_distance_order() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // pairs at identical distances, so distance alone doesn't give a total order
    for (i, (lat, lon)) in [(6.50, 3.30), (6.50, 3.30), (6.60, 3.40), (6.60, 3.40), (6.70, 3.50), (6.45, 3.35)]
        .into_iter()
        .enumerate()
    {
        let mut p = Place::default();
        p.name = format!("p{i}");
        p.lat = lat;
        p.lon = lon;
        engine.create_object(&p).await.unwrap();
    }

    for ascending in [true, false] {
        let query = || Query::default().order_by_distance(&Place::FIELDS.location, 3.3, 6.5, ascending);
        let full: Vec<uuid::Uuid> = engine
            .query_objects::<Place>(query())
            .await
            .unwrap()
            .iter()
            .map(|p| p.id())
            .collect();
        assert_eq!(full.len(), 6);
        for page in [1, 2, 4] {
            let paged = paged_ids::<Place>(&engine, query, page).await;
            assert_eq!(paged, full, "ascending={ascending}, page size {page}");
        }

        let with_distance: Vec<uuid::Uuid> = engine
            .query_objects_with_distance::<Place>(query().with_limit(2).with_cursor(full[1]))
            .await
            .unwrap()
            .iter()
            .map(|(p, _)| p.id())
            .collect();
        assert_eq!(with_distance, full[2..4], "with_distance, ascending={ascending}");
    }
}

#[tokio::test]
async fn test_sort_random_ignores_cursor() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = Hub::default();
    hub.name = "hub".into();
    engine.create_object(&hub).await.unwrap();
    for (position, name) in [(1i64, "a"), (2, "b"), (3, "c")] {
        let mut spoke = Spoke::default();
        spoke.name = name.into();
        engine.create_object(&spoke).await.unwrap();
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub.id(), spoke.id()),
                position,
            })
            .await
            .unwrap();
    }
    // a nil cursor is below every v7 id, so if applied it would return nothing
    let cursor = uuid::Uuid::nil();
    let applied: Vec<Spoke> = engine
        .query_objects(Query::default().with_cursor(cursor))
        .await
        .unwrap();
    assert!(applied.is_empty());

    let spokes: Vec<Spoke> = engine
        .query_objects(Query::default().with_cursor(cursor).sort_random())
        .await
        .unwrap();
    assert_eq!(spokes.len(), 3);

    let edges = engine
        .query_edges::<HubSpoke>(hub.id(), EdgeQuery::default().sort_random().with_cursor(cursor))
        .await
        .unwrap();
    assert_eq!(edges.len(), 3);

    let via_edge = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_sort_random()
        .with_cursor(cursor)
        .collect()
        .await
        .unwrap();
    assert_eq!(via_edge.len(), 3);

    let via_target = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .sort_random()
        .with_cursor(cursor)
        .collect_with_target()
        .await
        .unwrap();
    assert_eq!(via_target.len(), 3);
}

#[tokio::test]
async fn test_edge_negated_filters_and_random_sort() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = Hub::default();
    hub.name = "hub".into();
    engine.create_object(&hub).await.unwrap();
    for (position, name) in [(1i64, "s-a"), (2, "s-b"), (3, "t-c"), (4, "t-d")] {
        let mut spoke = Spoke::default();
        spoke.name = name.into();
        engine.create_object(&spoke).await.unwrap();
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub.id(), spoke.id()),
                position,
            })
            .await
            .unwrap();
    }
    let positions = |edges: &[HubSpoke]| {
        let mut p: Vec<i64> = edges.iter().map(|e| e.position).collect();
        p.sort();
        p
    };

    // EdgeQuery
    let edges: Vec<HubSpoke> = engine
        .query_edges(
            hub.id(),
            EdgeQuery::default().where_not_in(&HubSpoke::FIELDS.position, vec![1i64, 2]),
        )
        .await
        .unwrap();
    assert_eq!(positions(&edges), [3, 4]);

    let edges: Vec<HubSpoke> = engine
        .query_edges(hub.id(), EdgeQuery::default().sort_random())
        .await
        .unwrap();
    assert_eq!(positions(&edges), [1, 2, 3, 4]);

    // EdgeQueryContext: edge-side and target-side negations together
    let spokes: Vec<Spoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_not_in(&HubSpoke::FIELDS.position, vec![1i64])
        .where_not_begins_with(&Spoke::FIELDS.name, "t-")
        .collect()
        .await
        .unwrap();
    assert_eq!(spokes.len(), 1);
    assert_eq!(spokes[0].name, "s-b");

    let edges: Vec<HubSpoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_sort_random()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(positions(&edges), [1, 2, 3, 4]);

    let spokes: Vec<Spoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .sort_random()
        .collect()
        .await
        .unwrap();
    assert_eq!(spokes.len(), 4);
}
