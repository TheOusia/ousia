use super::*;

// ============================================================
// Section 14: Sorting & cursor pagination
// ============================================================

async fn seed_variants(engine: &Engine, rows: &[(&str, i64, f64)]) {
    for (name, count, price) in rows {
        let mut v = Variants::default();
        v.name = (*name).into();
        v.count = *count;
        v.price = *price;
        engine.create_object(&v).await.unwrap();
    }
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
async fn test_sort_orders_numbers_by_value() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // text order would be 10, 100, 2, 9 and -1.5, 10.25, 2.5
    seed_variants(&engine, &[("a", 10, 10.25), ("b", 2, -1.5), ("c", 100, 2.5), ("d", 9, 9.0)]).await;
    let names = |rows: Vec<Variants>| rows.into_iter().map(|v| v.name).collect::<Vec<_>>();

    let by = |q| async { names(engine.query_objects(q).await.unwrap()) };
    assert_eq!(by(Query::default().sort_asc(&Variants::FIELDS.count)).await, ["b", "d", "a", "c"]);
    assert_eq!(by(Query::default().sort_desc(&Variants::FIELDS.count)).await, ["c", "a", "d", "b"]);
    assert_eq!(by(Query::default().sort_asc(&Variants::FIELDS.price)).await, ["b", "c", "d", "a"]);
    assert_eq!(by(Query::default().sort_asc(&Variants::FIELDS.name)).await, ["a", "b", "c", "d"]);
}

#[tokio::test]
async fn test_cursor_pagination_follows_field_sorts() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    // one row whose index_meta lacks the sorted fields (sorts as NULL)
    let mut missing = Variants::default();
    missing.name = "zz".into();
    let mut record = ObjectRecord::from_object(&missing);
    record.index_meta = serde_json::json!({});
    adapter.insert_object(record).await.unwrap();
    let engine = Engine::new(Box::new(adapter));
    // duplicate names give ties
    seed_variants(
        &engine,
        &[("c", 1, 1.0), ("a", 2, 2.0), ("b", 30, 3.0), ("a", 4, 4.0), ("c", 5, 5.0), ("b", 6, 6.0), ("a", 7, 7.0)],
    )
    .await;

    let cases: Vec<(&str, Box<dyn Fn() -> Query>)> = vec![
        ("name asc", Box::new(|| Query::default().sort_asc(&Variants::FIELDS.name))),
        ("name desc", Box::new(|| Query::default().sort_desc(&Variants::FIELDS.name))),
        ("count asc", Box::new(|| Query::default().sort_asc(&Variants::FIELDS.count))),
        ("created_at desc", Box::new(|| Query::default().sort_desc(&Variants::FIELDS.created_at))),
        (
            "name asc, count desc",
            Box::new(|| {
                Query::default()
                    .sort_asc(&Variants::FIELDS.name)
                    .sort_desc(&Variants::FIELDS.count)
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
        assert_eq!(full.len(), 8, "{label}");
        for page in [1, 2, 3] {
            assert_eq!(paged_ids::<Variants>(&engine, &query, page).await, full, "{label}, page {page}");
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
            assert_eq!(paged_ids::<Place>(&engine, query, page).await, full, "ascending={ascending}, page {page}");
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
async fn test_edge_sort_and_cursor_follow_position() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = User::default();
    hub.username = "hub".into();
    engine.create_object(&hub).await.unwrap();
    for (i, position) in [10i64, 2, 100, 9, 2].into_iter().enumerate() {
        let mut u = User::default();
        u.username = format!("u{i}");
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Ranked {
                _meta: EdgeMeta::new(hub.id(), u.id()),
                position,
            })
            .await
            .unwrap();
    }
    let positions = |edges: &[Ranked]| edges.iter().map(|e| e.position).collect::<Vec<_>>();

    let asc = || EdgeQuery::default().sort_asc(&Ranked::FIELDS.position);
    let all: Vec<Ranked> = engine.query_edges(hub.id(), asc()).await.unwrap();
    assert_eq!(positions(&all), [2, 2, 9, 10, 100]);

    let desc: Vec<Ranked> = engine
        .preload_object::<User>(hub.id())
        .edge::<Ranked, User>()
        .edge_sort_desc(&Ranked::FIELDS.position)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(positions(&desc), [100, 10, 9, 2, 2]);

    // cursor pages walk the same order, including the tie at 2
    let mut paged: Vec<Ranked> = Vec::new();
    let mut cursor = None;
    loop {
        let mut q = asc().with_limit(2);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let page: Vec<Ranked> = engine.query_edges(hub.id(), q).await.unwrap();
        let done = page.len() < 2;
        cursor = page.last().map(|e| e.to());
        paged.extend(page);
        if done {
            break;
        }
    }
    assert_eq!(
        paged.iter().map(|e| e.to()).collect::<Vec<_>>(),
        all.iter().map(|e| e.to()).collect::<Vec<_>>()
    );
}

/// With no explicit sort, edge pages walk newest-created first, independent of
/// id order (the largest-id follower's edge is created first).
#[tokio::test]
async fn test_query_reverse_edges_cursor_orders_by_created_at_not_by_id() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut target = User::default();
    target.username = "cursor_order_target".into();
    engine.create_object(&target).await.unwrap();

    let mut followers = Vec::new();
    for i in 0..3 {
        let mut u = User::default();
        u.username = format!("cursor_order_follower{i}");
        engine.create_object(&u).await.unwrap();
        followers.push(u.id());
    }
    let max_id_follower = *followers.iter().max().unwrap();
    let mut creation_order = vec![max_id_follower];
    creation_order.extend(followers.iter().copied().filter(|id| *id != max_id_follower));
    for follower_id in &creation_order {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(*follower_id, target.id()),
                notification: true,
            })
            .await
            .unwrap();
    }

    let mut visited = Vec::new();
    let mut cursor: Option<uuid::Uuid> = None;
    loop {
        let mut q = EdgeQuery::default().with_limit(1);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let page: Vec<Follow> = engine.query_reverse_edges(target.id(), q).await.unwrap();
        let Some(edge) = page.into_iter().next() else { break };
        assert!(!visited.contains(&edge.from()), "an edge was returned twice");
        visited.push(edge.from());
        cursor = Some(edge.from());
        assert!(visited.len() <= 3, "pagination did not terminate");
    }
    creation_order.reverse();
    assert_eq!(visited, creation_order);
}
