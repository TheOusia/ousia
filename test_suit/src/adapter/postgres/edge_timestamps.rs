use super::*;

// ============================================================
// Section 6: Edge Timestamps
// ============================================================

#[tokio::test]
async fn test_edge_timestamps_populated_on_create() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

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

    let edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();

    // Timestamps must be non-zero and close to now.
    let now = chrono::Utc::now();
    let diff_created = (now - edge.created_at()).num_seconds().abs();
    let diff_updated = (now - edge.updated_at()).num_seconds().abs();
    assert!(diff_created < 60, "created_at should be recent");
    assert!(diff_updated < 60, "updated_at should be recent");
    assert_eq!(
        edge.created_at(),
        edge.updated_at(),
        "on first insert created_at == updated_at"
    );
}

#[tokio::test]
async fn test_edge_sort_desc_by_created_at() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut u = User::default();
        u.username = format!("t{}", i);
        u.email = format!("t{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: true,
            })
            .await
            .unwrap();
        targets.push(u);
    }

    // Sort desc by edge created_at → newest edge first (t2 was created last).
    let edges: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_sort_desc(&Follow::FIELDS.created_at)
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(edges.len(), 3);
    assert_eq!(
        edges[0].to(),
        targets[2].id(),
        "newest edge should be first"
    );
    assert_eq!(edges[2].to(), targets[0].id(), "oldest edge should be last");
}

#[tokio::test]
async fn test_edge_sort_asc_by_created_at() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut u = User::default();
        u.username = format!("asc{}", i);
        u.email = format!("asc{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: false,
            })
            .await
            .unwrap();
        targets.push(u);
    }

    let edges: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_sort_asc(&Follow::FIELDS.created_at)
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(edges.len(), 3);
    assert_eq!(
        edges[0].to(),
        targets[0].id(),
        "oldest edge should be first"
    );
    assert_eq!(edges[2].to(), targets[2].id(), "newest edge should be last");
}
