use super::*;

// ============================================================
// Section 5: Edge CRUD
// ============================================================

#[tokio::test]
async fn test_insert_and_fetch_edge() {
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

    let follow = Follow {
        _meta: EdgeMeta::new(alice.id(), bob.id()),
        notification: true,
    };
    engine.create_edge(&follow).await.unwrap();

    let fetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();
    assert!(fetched.is_some());
    assert!(fetched.unwrap().notification);
}

#[tokio::test]
async fn test_fetch_missing_edge_returns_none() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let result = engine
        .fetch_edge::<Follow>(uuid::Uuid::now_v7(), uuid::Uuid::now_v7())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_insert_edge_upsert_updates_data() {
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
            notification: false,
        })
        .await
        .unwrap();

    // Re-insert with notification=true — upsert should update.
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let fetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    assert!(fetched.notification, "upsert should update edge data");
}

#[tokio::test]
async fn test_delete_edge() {
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

    engine
        .delete_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();

    let gone: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert!(gone.is_empty());
}

#[tokio::test]
async fn test_update_edge_data_only() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "ue_alice".into();
    alice.email = "ue_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "ue_bob".into();
    bob.email = "ue_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    let mut edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    edge.notification = true;
    engine.update_edge(&mut edge, None).await.unwrap();

    let refetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        refetched.notification,
        "update_edge should persist data changes"
    );
    assert_eq!(
        refetched.to(),
        bob.id(),
        "no retarget requested — `to` unchanged"
    );
}

#[tokio::test]
async fn test_update_edge_retargets_to() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "uer_alice".into();
    alice.email = "uer_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "uer_bob".into();
    bob.email = "uer_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "uer_charlie".into();
    charlie.email = "uer_charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let mut edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    engine
        .update_edge(&mut edge, Some(charlie.id()))
        .await
        .unwrap();

    // Old (from, bob) key is gone — retargeting moves the edge, doesn't duplicate it.
    let old = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();
    assert!(old.is_none());

    let retargeted = engine
        .fetch_edge::<Follow>(alice.id(), charlie.id())
        .await
        .unwrap();
    assert!(retargeted.is_some());
    assert!(retargeted.unwrap().notification);
}

#[tokio::test]
async fn test_delete_object_edge_removes_all_forward_edges() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "doe_alice".into();
    alice.email = "doe_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut targets = Vec::new();
    for name in ["doe_t1", "doe_t2", "doe_t3"] {
        let mut u = User::default();
        u.username = name.into();
        u.email = format!("{name}@x.com");
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
    }

    for &to in &targets {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(alice.id(), to),
                notification: true,
            })
            .await
            .unwrap();
    }

    // A second user's edges must survive — this only touches `alice`'s.
    let mut dave = User::default();
    dave.username = "doe_dave".into();
    dave.email = "doe_dave@x.com".into();
    engine.create_object(&dave).await.unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(dave.id(), targets[0]),
            notification: true,
        })
        .await
        .unwrap();

    engine
        .delete_object_edge::<Follow>(alice.id())
        .await
        .unwrap();

    let alice_edges: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert!(alice_edges.is_empty());

    let dave_edges: Vec<Follow> = engine
        .query_edges(dave.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(
        dave_edges.len(),
        1,
        "delete_object_edge must not touch other owners' edges"
    );
}
