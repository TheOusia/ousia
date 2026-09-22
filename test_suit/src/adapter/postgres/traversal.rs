use super::*;

// ============================================================
// Section 8: Edge Traversal — collect variants
// ============================================================

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

// ── Traversal ordering & cursor paging ──────────────────────────────────────

fn named(name: &str) -> Variants {
    let mut v = Variants::default();
    v.name = name.into();
    v
}

/// hub → 7 targets, and 7 sources → one target; duplicate names and positions.
async fn traversal_fixture(engine: &Engine) -> (uuid::Uuid, uuid::Uuid) {
    let (hub, target) = (named("hub"), named("target"));
    engine.create_object(&hub).await.unwrap();
    engine.create_object(&target).await.unwrap();
    for (name, position) in [("c", 3i64), ("a", 1), ("b", 2), ("a", 2), ("c", 1), ("b", 3), ("a", 1)] {
        let (to, from) = (named(name), named(name));
        engine.create_object(&to).await.unwrap();
        engine.create_object(&from).await.unwrap();
        for (a, b) in [(hub.id(), to.id()), (from.id(), target.id())] {
            engine
                .create_edge(&Ranked { _meta: EdgeMeta::new(a, b), position })
                .await
                .unwrap();
        }
    }
    (hub.id(), target.id())
}

/// Pages of `page` via `run(cursor, limit)`, cursor = last id, until exhausted.
async fn walk<F, Fut>(run: F, page: u32) -> Vec<uuid::Uuid>
where
    F: Fn(Option<uuid::Uuid>, Option<u32>) -> Fut,
    Fut: std::future::Future<Output = Vec<uuid::Uuid>>,
{
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let ids = run(cursor, Some(page)).await;
        out.extend(&ids);
        if ids.len() < page as usize {
            return out;
        }
        cursor = ids.last().copied();
    }
}

#[tokio::test]
async fn test_traversal_sorts_by_target_fields() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));
    let (hub, target) = traversal_fixture(&engine).await;

    let names: Vec<String> = engine
        .preload_object::<Variants>(hub)
        .edge::<Ranked, Variants>()
        .sort_asc(&Variants::FIELDS.name)
        .collect()
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name)
        .collect();
    assert_eq!(names, ["a", "a", "a", "b", "b", "c", "c"]);

    let names: Vec<String> = engine
        .preload_object::<Variants>(target)
        .edge::<Ranked, Variants>()
        .sort_desc(&Variants::FIELDS.name)
        .collect_reverse_with_target()
        .await
        .unwrap()
        .into_iter()
        .map(|oe| oe.object().name.clone())
        .collect();
    assert_eq!(names, ["c", "c", "b", "b", "a", "a", "a"]);
}

#[tokio::test]
async fn test_traversal_cursor_pages_follow_the_order() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));
    let (hub, target) = traversal_fixture(&engine).await;

    #[derive(Clone, Copy, Debug)]
    enum Order {
        Default,
        EdgeAsc,
        TargetAscEdgeDesc,
    }
    for order in [Order::Default, Order::EdgeAsc, Order::TargetAscEdgeDesc] {
        let ordered = |pivot: uuid::Uuid| {
            let q = engine.preload_object::<Variants>(pivot).edge::<Ranked, Variants>();
            match order {
                Order::Default => q,
                Order::EdgeAsc => q.edge_sort_asc(&Ranked::FIELDS.position),
                Order::TargetAscEdgeDesc => q
                    .sort_asc(&Variants::FIELDS.name)
                    .edge_sort_desc(&Ranked::FIELDS.position),
            }
        };
        let forward = |cursor: Option<uuid::Uuid>, limit: Option<u32>| {
            let mut q = ordered(hub);
            if let Some(c) = cursor {
                q = q.with_cursor(c);
            }
            if let Some(l) = limit {
                q = q.with_limit(l);
            }
            async move { q.collect().await.unwrap().iter().map(|v| v.id()).collect::<Vec<_>>() }
        };
        let reverse = |cursor: Option<uuid::Uuid>, limit: Option<u32>| {
            let mut q = ordered(target);
            if let Some(c) = cursor {
                q = q.with_cursor(c);
            }
            if let Some(l) = limit {
                q = q.with_limit(l);
            }
            async move {
                q.collect_reverse_with_target()
                    .await
                    .unwrap()
                    .iter()
                    .map(|oe| oe.object().id())
                    .collect::<Vec<_>>()
            }
        };

        let full_fwd = forward(None, None).await;
        let full_rev = reverse(None, None).await;
        assert_eq!((full_fwd.len(), full_rev.len()), (7, 7), "{order:?}");
        for page in [1, 2, 3] {
            assert_eq!(walk(&forward, page).await, full_fwd, "forward {order:?}, page {page}");
            assert_eq!(walk(&reverse, page).await, full_rev, "reverse {order:?}, page {page}");
        }
    }
}
