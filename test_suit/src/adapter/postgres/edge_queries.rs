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

/// Regression test for a bug where `EdgeQuery::with_cursor` filtered on the raw
/// `from`/`to` column with no `ORDER BY` tying the two together, so pagination
/// order was whatever Postgres felt like returning — which, for a freshly
/// inserted small table, ordinarily lines up with insertion (`created_at`)
/// order by coincidence, silently masking the bug in exactly this kind of
/// test. This one deliberately breaks that coincidence: the follower with the
/// numerically largest id gets its edge created *first* (so it's
/// chronologically oldest despite having the largest id), which fails under
/// the old id-based cursor and passes under the fixed created_at-based one.
#[tokio::test]
async fn test_query_reverse_edges_cursor_orders_by_created_at_not_by_id() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut target = User::default();
    target.username = "cursor_order_target".into();
    target.email = "cursor_order_target@x.com".into();
    engine.create_object(&target).await.unwrap();

    let mut followers = Vec::new();
    for i in 0..3 {
        let mut u = User::default();
        u.username = format!("cursor_order_follower{i}");
        u.email = format!("cursor_order_follower{i}@x.com");
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

    // Walk every page (limit 1, so each page boundary exercises the cursor)
    // and record `from` in the order returned.
    let mut visited = Vec::new();
    let mut cursor: Option<uuid::Uuid> = None;
    loop {
        let mut q = EdgeQuery::default().with_limit(1);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let page: Vec<Follow> = engine.query_reverse_edges(target.id(), q).await.unwrap();
        let Some(edge) = page.into_iter().next() else {
            break;
        };
        let from = edge.from();
        assert!(
            !visited.contains(&from),
            "follower {from:?} was returned on more than one page"
        );
        visited.push(from);
        cursor = Some(from);
        assert!(
            visited.len() <= 3,
            "pagination did not terminate after visiting every follower"
        );
    }

    let mut expected_newest_first = creation_order.clone();
    expected_newest_first.reverse();
    assert_eq!(
        visited, expected_newest_first,
        "default (no explicit sort) cursor pagination should walk edges newest-created-first, \
         not in id order"
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
