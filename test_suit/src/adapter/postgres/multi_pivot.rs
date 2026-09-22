use super::*;

// ============================================================
// Section 9: Multi-Pivot Preload API
// ============================================================

#[tokio::test]
async fn test_multi_pivot_collect_following() {
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
            _meta: EdgeMeta::new(bob.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();

    let results: Vec<(User, Vec<User>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_e.1.len(), 1);
    assert_eq!(alice_e.1[0].username, "bob");

    let charlie_e = results
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert!(charlie_e.1.is_empty());
}

#[tokio::test]
async fn test_multi_pivot_collect_reverse_followers() {
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

    let results: Vec<(User, Vec<User>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse()
        .await
        .unwrap();

    let bob_e = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_e.1.len(), 2);

    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert!(alice_e.1.is_empty());
}

#[tokio::test]
async fn test_multi_pivot_collect_edges_and_reverse_edges() {
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

    // collect_edges
    let fwd: Vec<(User, Vec<Follow>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    let alice_entry = fwd.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert!(alice_entry.1[0].notification);

    // collect_reverse_edges
    let rev: Vec<(User, Vec<Follow>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse_edges()
        .await
        .unwrap();
    let bob_entry = rev.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].from(), alice.id());
}

#[tokio::test]
async fn test_multi_pivot_collect_with_target_and_reverse() {
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

    // Forward
    let fwd = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();
    let alice_entry = fwd.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert_eq!(alice_entry.1[0].object().username, "bob");

    // Reverse
    let rev = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse_with_target()
        .await
        .unwrap();
    let bob_entry = rev.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].object().username, "alice");
}

#[tokio::test]
async fn test_multi_pivot_count_and_count_reverse() {
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
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), charlie.id()),
            notification: true,
        })
        .await
        .unwrap();

    let counts: Vec<(User, u64)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .count()
        .await
        .unwrap();

    let alice_c = counts.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_c.1, 2);
    let charlie_c = counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_c.1, 0);

    let rev_counts: Vec<(User, u64)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .count_reverse()
        .await
        .unwrap();

    let charlie_rc = rev_counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_rc.1, 2);
    let alice_rc = rev_counts
        .iter()
        .find(|(u, _)| u.username == "alice")
        .unwrap();
    assert_eq!(alice_rc.1, 0);
}

#[tokio::test]
async fn test_multi_pivot_owned() {
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

    for title in ["Post 1", "Post 2"] {
        let mut p = Post::default();
        p.set_owner(alice.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(bob.id());
    p.title = "Bob Post".into();
    engine.create_object(&p).await.unwrap();

    let results: Vec<(User, Vec<Post>)> = engine
        .preload_objects::<User>(Query::default())
        .preload::<Post>()
        .collect()
        .await
        .unwrap();

    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_e.1.len(), 2);
    let bob_e = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_e.1.len(), 1);
}
