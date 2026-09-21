use super::*;

// ============================================================
// Section 8: Edge Traversal — collect variants
// ============================================================

#[tokio::test]
async fn test_query_context_get() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qcg_alice".into();
    alice.email = "qcg_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let found = engine
        .preload_object::<User>(alice.id())
        .get()
        .await
        .unwrap();
    assert_eq!(found.unwrap().username, "qcg_alice");

    let missing = engine
        .preload_object::<User>(uuid::Uuid::now_v7())
        .get()
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_edge_query_context_paginate() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "eqp_alice".into();
    alice.email = "eqp_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut targets = Vec::new();
    for name in ["eqp_t1", "eqp_t2", "eqp_t3"] {
        let mut u = User::default();
        u.username = name.into();
        u.email = format!("{name}@x.com");
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
    }
    // Edges are returned newest-`to`-first by default cursor ordering, so
    // insert in a known order and page from the most recent.
    for &to in &targets {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(alice.id(), to),
                notification: true,
            })
            .await
            .unwrap();
    }

    let all: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // paginate(None) must behave identically to no pagination at all.
    let unpaginated: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .paginate(None::<uuid::Uuid>)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(unpaginated.len(), 3);

    // paginate(Some(cursor)) filters to `to < cursor` (same contract as
    // `with_cursor`) — use the max `to` so exactly one row (itself) drops.
    let cursor_to = all.iter().map(|e| e.to()).max().unwrap();
    let page: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .paginate(Some(cursor_to))
        .collect_edges()
        .await
        .unwrap();
    assert!(page.iter().all(|e| e.to() < cursor_to));
    assert_eq!(page.len(), all.len() - 1);
}

#[tokio::test]
async fn test_collect_forward_and_reverse() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    // Alice is following bob (forward)
    let following: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();
    assert_eq!(following.len(), 1);
    assert_eq!(following[0].username, "bob");

    // Charlie follows alice (alice's followers = reverse)
    let followers: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_reverse()
        .await
        .unwrap();
    assert_eq!(followers.len(), 1);
    assert_eq!(followers[0].username, "charlie");

    // collect_edges (forward)
    let edges: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].from(), alice.id());
    assert_eq!(edges[0].to(), bob.id());

    // collect_reverse_edges
    let rev_edges: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_reverse_edges()
        .await
        .unwrap();
    assert_eq!(rev_edges.len(), 1);
    assert_eq!(rev_edges[0].from(), charlie.id());
}

#[tokio::test]
async fn test_collect_with_target_and_reverse() {
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

    // Forward: collect (edge, target) pairs
    let pairs = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].edge().from(), alice.id());
    assert_eq!(pairs[0].object().username, "bob");

    // Reverse: collect (edge, source) pairs from bob's perspective
    let rev_pairs = engine
        .preload_object::<User>(bob.id())
        .edge::<Follow, User>()
        .collect_reverse_with_target()
        .await
        .unwrap();
    assert_eq!(rev_pairs.len(), 1);
    assert_eq!(rev_pairs[0].edge().from(), alice.id());
    assert_eq!(rev_pairs[0].object().username, "alice");
}

#[tokio::test]
async fn test_collect_both() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (following, followers) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both()
        .await
        .unwrap();
    assert_eq!(following.len(), 1);
    assert_eq!(following[0].username, "bob");
    assert_eq!(followers.len(), 1);
    assert_eq!(followers[0].username, "charlie");
}

#[tokio::test]
async fn test_collect_both_with_target() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (fwd, rev) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both_with_target()
        .await
        .unwrap();

    assert_eq!(fwd.len(), 1);
    assert_eq!(fwd[0].edge().from(), alice.id());
    assert_eq!(fwd[0].object().username, "bob");

    assert_eq!(rev.len(), 1);
    assert_eq!(rev[0].edge().from(), charlie.id());
    assert_eq!(rev[0].object().username, "charlie");
}

#[tokio::test]
async fn test_collect_both_edges() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (fwd_edges, rev_edges): (Vec<Follow>, Vec<Follow>) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both_edges()
        .await
        .unwrap();

    assert_eq!(fwd_edges.len(), 1);
    assert_eq!(fwd_edges[0].from(), alice.id());
    assert_eq!(fwd_edges[0].to(), bob.id());
    assert!(fwd_edges[0].notification);

    assert_eq!(rev_edges.len(), 1);
    assert_eq!(rev_edges[0].from(), charlie.id());
    assert_eq!(rev_edges[0].to(), alice.id());
    assert!(!rev_edges[0].notification);
}

#[tokio::test]
async fn test_edge_traversal_with_object_filter() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

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
            _meta: EdgeMeta::new(pivot.id(), alice.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(pivot.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    // Filter the target objects: only return User where username == "alice"
    let results: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .where_eq(&User::FIELDS.username, "alice")
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].username, "alice");
}

#[tokio::test]
async fn test_edge_traversal_with_limit_and_cursor() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut target_ids = Vec::new();
    for i in 0..5 {
        let mut u = User::default();
        u.username = format!("t{}", i);
        u.email = format!("t{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        target_ids.push(u.id());
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: true,
            })
            .await
            .unwrap();
    }

    // with_limit
    let limited: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .with_limit(3)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(limited.len(), 3);
}
