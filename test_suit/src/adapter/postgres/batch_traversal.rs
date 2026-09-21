use super::*;

// ============================================================
// Section 14: Batch edge/ownership traversal (arbitrary id sets)
// ============================================================

#[tokio::test]
async fn test_query_edges_batch_groups_by_from() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qeb_alice".into();
    alice.email = "qeb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "qeb_bob".into();
    bob.email = "qeb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut t1 = User::default();
    t1.username = "qeb_t1".into();
    t1.email = "qeb_t1@x.com".into();
    engine.create_object(&t1).await.unwrap();

    let mut t2 = User::default();
    t2.username = "qeb_t2".into();
    t2.email = "qeb_t2@x.com".into();
    engine.create_object(&t2).await.unwrap();

    let mut t3 = User::default();
    t3.username = "qeb_t3".into();
    t3.email = "qeb_t3@x.com".into();
    engine.create_object(&t3).await.unwrap();

    // alice → t1, t2 (2 edges); bob → t3 (1 edge)
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), t1.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), t2.id()),
            notification: false,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), t3.id()),
            notification: true,
        })
        .await
        .unwrap();

    let no_edges_id = uuid::Uuid::now_v7();

    let grouped = engine
        .query_edges_batch::<Follow>(&[alice.id(), bob.id(), no_edges_id], EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(grouped.get(&alice.id()).unwrap().len(), 2);
    assert_eq!(grouped.get(&bob.id()).unwrap().len(), 1);
    assert!(grouped.get(&no_edges_id).is_none());

    // With an EdgeQuery filter applied.
    let filtered = engine
        .query_edges_batch::<Follow>(
            &[alice.id(), bob.id()],
            EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.get(&alice.id()).unwrap().len(), 1);
    assert_eq!(filtered.get(&bob.id()).unwrap().len(), 1);
}

#[tokio::test]
async fn test_query_edges_with_targets_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qewtb_alice".into();
    alice.email = "qewtb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "qewtb_bob".into();
    bob.email = "qewtb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut u1 = User::default();
    u1.username = "qewtb_u1".into();
    u1.email = "qewtb_u1@x.com".into();
    engine.create_object(&u1).await.unwrap();

    let mut u2 = User::default();
    u2.username = "qewtb_u2".into();
    u2.email = "qewtb_u2@x.com".into();
    engine.create_object(&u2).await.unwrap();

    let mut u3 = User::default();
    u3.username = "qewtb_u3".into();
    u3.email = "qewtb_u3@x.com".into();
    engine.create_object(&u3).await.unwrap();

    // alice → u1, u2; bob → u3
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), u1.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), u2.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), u3.id()),
            notification: true,
        })
        .await
        .unwrap();

    let grouped = engine
        .query_edges_with_targets_batch::<Follow, User>(
            &[alice.id(), bob.id()],
            &[],
            EdgeQuery::default(),
        )
        .await
        .unwrap();

    let alice_pairs = grouped.get(&alice.id()).unwrap();
    assert_eq!(alice_pairs.len(), 2);
    let mut alice_usernames: Vec<&str> = alice_pairs
        .iter()
        .map(|p| p.object().username.as_str())
        .collect();
    alice_usernames.sort();
    assert_eq!(alice_usernames, vec!["qewtb_u1", "qewtb_u2"]);
    assert!(alice_pairs.iter().all(|p| p.edge().from() == alice.id()));

    let bob_pairs = grouped.get(&bob.id()).unwrap();
    assert_eq!(bob_pairs.len(), 1);
    assert_eq!(bob_pairs[0].object().username, "qewtb_u3");

    // With an object filter restricting the joined target.
    let filtered = engine
        .query_edges_with_targets_batch::<Follow, User>(
            &[alice.id(), bob.id()],
            &[filter!(&User::FIELDS.username, "qewtb_u1")],
            EdgeQuery::default(),
        )
        .await
        .unwrap();
    let alice_filtered = filtered.get(&alice.id()).unwrap();
    assert_eq!(alice_filtered.len(), 1);
    assert_eq!(alice_filtered[0].object().username, "qewtb_u1");
    assert!(filtered.get(&bob.id()).is_none());
}

#[tokio::test]
async fn test_count_reverse_edges_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "creb_alice".into();
    alice.email = "creb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "creb_bob".into();
    bob.email = "creb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "creb_charlie".into();
    charlie.email = "creb_charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    // charlie ← alice, bob (2 reverse edges); bob ← alice (1 reverse edge);
    // alice has none.
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: true,
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
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let counts = engine
        .count_reverse_edges_batch::<Follow>(
            &[charlie.id(), bob.id(), alice.id()],
            EdgeQuery::default(),
        )
        .await
        .unwrap();

    assert_eq!(counts.get(&charlie.id()).copied(), Some(2));
    assert_eq!(counts.get(&bob.id()).copied(), Some(1));
    // No reverse edges → the id is absent from the map entirely (GROUP BY
    // produces no row), not present with a value of 0.
    assert!(counts.get(&alice.id()).is_none());
}

#[tokio::test]
async fn test_fetch_owned_objects_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner1 = User::default();
    owner1.username = "foob_owner1".into();
    owner1.email = "foob_owner1@x.com".into();
    engine.create_object(&owner1).await.unwrap();

    let mut owner2 = User::default();
    owner2.username = "foob_owner2".into();
    owner2.email = "foob_owner2@x.com".into();
    engine.create_object(&owner2).await.unwrap();

    for title in ["Post A", "Post B"] {
        let mut p = Post::default();
        p.set_owner(owner1.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(owner2.id());
    p.title = "Post C".into();
    engine.create_object(&p).await.unwrap();

    let no_posts_owner = uuid::Uuid::now_v7();

    let grouped = engine
        .fetch_owned_objects_batch::<Post>(&[owner1.id(), owner2.id(), no_posts_owner])
        .await
        .unwrap();

    assert_eq!(grouped.get(&owner1.id()).unwrap().len(), 2);
    assert_eq!(grouped.get(&owner2.id()).unwrap().len(), 1);
    assert!(grouped.get(&no_posts_owner).is_none());
}
