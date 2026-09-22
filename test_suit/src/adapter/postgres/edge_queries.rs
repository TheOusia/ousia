use super::*;

// ============================================================
// Section 7: Edge Queries & Filters
// ============================================================

#[tokio::test]
async fn test_query_edges_forward_and_reverse() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@x.com".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(michael.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    // Forward: alice → [bob]
    let fwd: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(fwd.len(), 1);
    assert_eq!(fwd[0].to(), bob.id());

    // Forward: bob → []
    let fwd: Vec<Follow> = engine
        .query_edges(bob.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(fwd.len(), 0);

    // Reverse: bob ← [alice, michael]
    let rev: Vec<Follow> = engine
        .query_reverse_edges(bob.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(rev.len(), 2);

    // count_edges / count_reverse_edges
    assert_eq!(
        engine.count_edges::<Follow>(bob.id(), None).await.unwrap(),
        0
    );
    assert_eq!(
        engine
            .count_reverse_edges::<Follow>(bob.id(), None)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn test_edge_filter_on_edge_query_context() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    for (i, notif) in [(0, true), (1, false), (2, true)] {
        let mut u = User::default();
        u.username = format!("ef{}", i);
        u.email = format!("ef{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    // edge_eq: only notification=true edges
    let notified: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, true)
        .collect()
        .await
        .unwrap();
    assert_eq!(notified.len(), 2, "edge_eq notification=true");

    // edge_eq: only notification=false edges
    let silent: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, false)
        .collect()
        .await
        .unwrap();
    assert_eq!(silent.len(), 1, "edge_eq notification=false");

    // edge_ne: all edges where notification != true
    let not_notified: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_ne(&Follow::FIELDS.notification, true)
        .collect()
        .await
        .unwrap();
    assert_eq!(not_notified.len(), 1, "edge_ne notification=true");
}

#[tokio::test]
async fn test_edge_query_filter_and_sort() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for (i, notif) in [(0, true), (1, false), (2, true)] {
        let mut u = User::default();
        u.username = format!("eq{}", i);
        u.email = format!("eq{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    // EdgeQuery::where_eq via query_edges
    let filtered: Vec<Follow> = engine
        .query_edges(
            pivot.id(),
            EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.len(), 2, "EdgeQuery::where_eq");

    // EdgeQuery::where_ne
    let filtered: Vec<Follow> = engine
        .query_edges(
            pivot.id(),
            EdgeQuery::default().where_ne(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1, "EdgeQuery::where_ne");
}

#[tokio::test]
async fn test_count_edges_with_plan() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "cpivot".into();
    pivot.email = "cpivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    for (i, notif) in [(0, true), (1, false), (2, true), (3, true)] {
        let mut u = User::default();
        u.username = format!("ct{}", i);
        u.email = format!("ct{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    let total = engine
        .count_edges::<Follow>(pivot.id(), None)
        .await
        .unwrap();
    assert_eq!(total, 4);

    let notif_count = engine
        .count_edges::<Follow>(
            pivot.id(),
            Some(EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true)),
        )
        .await
        .unwrap();
    assert_eq!(notif_count, 3);
}

#[tokio::test]
async fn test_edge_timestamp_filters() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = User::default();
    hub.username = "hub".into();
    engine.create_object(&hub).await.unwrap();
    let mut mark = None;
    for position in 0..4i64 {
        if position == 2 {
            mark = Some(chrono::Utc::now());
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let mut u = User::default();
        u.username = format!("u{position}");
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Ranked { _meta: EdgeMeta::new(hub.id(), u.id()), position })
            .await
            .unwrap();
    }
    let mark = mark.unwrap();
    let positions = |edges: Vec<Ranked>| {
        let mut p: Vec<i64> = edges.into_iter().map(|e| e.position).collect();
        p.sort();
        p
    };

    let edges: Vec<Ranked> = engine
        .query_edges(hub.id(), EdgeQuery::default().where_gt(&Ranked::FIELDS.created_at, mark))
        .await
        .unwrap();
    assert_eq!(positions(edges), [2, 3]);

    let edges = engine
        .preload_object::<User>(hub.id())
        .edge::<Ranked, User>()
        .edge_lt(&Ranked::FIELDS.created_at, mark)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(positions(edges), [0, 1]);
}
