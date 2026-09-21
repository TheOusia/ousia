use super::*;

// ============================================================
// Section 3: Object Ownership & Bulk Operations
// ============================================================

#[tokio::test]
async fn test_transfer_ownership_success() {
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

    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".into();
    engine.create_object(&post).await.unwrap();

    let transferred: Post = engine
        .transfer_object(post.id(), alice.id(), bob.id())
        .await
        .unwrap();
    assert_eq!(transferred.owner(), bob.id());
}

#[tokio::test]
async fn test_transfer_ownership_wrong_owner_fails() {
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

    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".into();
    engine.create_object(&post).await.unwrap();

    // bob doesn't own the post → NotFound
    let result: Result<Post, Error> = engine
        .transfer_object(post.id(), bob.id(), alice.id())
        .await;
    assert!(matches!(result, Err(Error::NotFound)));
}

fn slot_for(owner: uuid::Uuid, label: &str) -> Slot {
    let mut slot = Slot::default();
    slot.set_owner(owner);
    slot.label = label.into();
    slot
}

#[tokio::test]
async fn test_transfer_rekeys_owner_unique_constraint() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let (alice, bob) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
    let slot = slot_for(alice, "a");
    engine.create_object(&slot).await.unwrap();

    let moved: Slot = engine
        .transfer_object(slot.id(), alice, bob)
        .await
        .unwrap();
    assert_eq!(moved.owner(), bob);

    // bob's owner hash was registered by the transfer
    let err = engine
        .create_object(&slot_for(bob, "b2"))
        .await
        .unwrap_err();
    assert_eq!(err, Error::UniqueConstraintViolation("owner".into()));

    // alice's stale owner hash was released by the transfer
    engine.create_object(&slot_for(alice, "a2")).await.unwrap();
}

#[tokio::test]
async fn test_transfer_into_taken_owner_unique_is_rejected() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let (alice, bob) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
    let alice_slot = slot_for(alice, "a");
    engine.create_object(&alice_slot).await.unwrap();
    engine.create_object(&slot_for(bob, "b")).await.unwrap();

    let err = engine
        .transfer_object::<Slot>(alice_slot.id(), alice, bob)
        .await
        .unwrap_err();
    assert_eq!(err, Error::UniqueConstraintViolation("owner".into()));

    // nothing moved, and alice still holds her slot's hash
    let still: Slot = engine.fetch_object(alice_slot.id()).await.unwrap().unwrap();
    assert_eq!(still.owner(), alice);
    let err = engine
        .create_object(&slot_for(alice, "a2"))
        .await
        .unwrap_err();
    assert_eq!(err, Error::UniqueConstraintViolation("owner".into()));
}

#[tokio::test]
async fn test_transfer_owner_unique_wrong_owner_leaves_no_hash() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let (alice, bob, carol) = (
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
    );
    let slot = slot_for(alice, "a");
    engine.create_object(&slot).await.unwrap();

    let result = engine.transfer_object::<Slot>(slot.id(), bob, carol).await;
    assert!(matches!(result, Err(Error::NotFound)));

    // no hash was reserved for carol by the failed transfer
    engine.create_object(&slot_for(carol, "c")).await.unwrap();
}

#[tokio::test]
async fn test_create_unique_conflict_leaves_no_object_row() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let alice = uuid::Uuid::now_v7();
    engine.create_object(&slot_for(alice, "a")).await.unwrap();
    let dup = slot_for(alice, "dup");
    let err = engine.create_object(&dup).await.unwrap_err();
    assert_eq!(err, Error::UniqueConstraintViolation("owner".into()));

    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM objects WHERE id = $1")
        .bind(dup.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

#[tokio::test]
async fn test_update_unique_conflict_is_all_or_nothing() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    engine.create_object(&alice).await.unwrap();
    let mut bob = User::default();
    bob.username = "bob".into();
    bob.display_name = "Bob".into();
    engine.create_object(&bob).await.unwrap();

    bob.username = "alice".into();
    bob.display_name = "changed".into();
    let err = engine.update_object(&mut bob).await.unwrap_err();
    assert_eq!(err, Error::UniqueConstraintViolation("username".into()));

    let stored: User = engine.fetch_object(bob.id()).await.unwrap().unwrap();
    assert_eq!(stored.username, "bob");
    assert_eq!(stored.display_name, "Bob");
    // bob's own key survived the failed update
    let mut other = User::default();
    other.username = "bob".into();
    assert!(engine.create_object(&other).await.is_err());
}

#[tokio::test]
async fn test_update_missing_object_is_not_found() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    assert_eq!(
        adapter.update_object(ObjectRecord::from_object(&Post::default())).await,
        Err(Error::NotFound)
    );
    let engine = Engine::new(Box::new(adapter));

    let mut ghost = User::default();
    ghost.username = "ghost".into();
    assert_eq!(engine.update_object(&mut ghost).await, Err(Error::NotFound));
    let mut post = Post::default();
    assert_eq!(engine.update_object(&mut post).await, Err(Error::NotFound));
    // nothing was reserved for the missing object
    engine.create_object(&ghost).await.unwrap();
}

#[tokio::test]
async fn test_transfer_atomic_refuses_stale_snapshot() {
    use ousia::Unique;
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let (alice, bob) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
    let slot = slot_for(alice, "a");
    adapter
        .create_object_atomic(ObjectRecord::from_object(&slot), slot.derive_unique_hashes(), vec![])
        .await
        .unwrap();

    let bob_keys = slot_for(bob, "a").derive_unique_hashes();
    let stale = rmp_serde::to_vec_named(&std::collections::BTreeMap::from([("label", "old")])).unwrap();
    let res = adapter
        .transfer_object_atomic(Slot::TYPE, slot.id(), alice, bob, stale, bob_keys)
        .await
        .unwrap();
    assert!(res.is_none(), "stale snapshot must not transfer");
    let rec = adapter.fetch_object(Slot::TYPE, slot.id()).await.unwrap().unwrap();
    assert_eq!(rec.owner, alice);

    let res = adapter
        .transfer_object_atomic(Slot::TYPE, slot.id(), bob, alice, rec.data.clone(), vec![])
        .await;
    assert_eq!(res.unwrap_err(), Error::NotFound);
}

#[tokio::test]
async fn test_concurrent_writers_cannot_share_unique_value() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let owner = uuid::Uuid::now_v7();
    let creates: Vec<_> = (0..8)
        .map(|i| {
            let engine = engine.clone();
            tokio::spawn(
                async move { engine.create_object(&slot_for(owner, &format!("c{i}"))).await },
            )
        })
        .collect();
    let mut ok = 0;
    for h in creates {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 1);

    // two different slots racing into the same new owner
    let target = uuid::Uuid::now_v7();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let from = uuid::Uuid::now_v7();
        let slot = slot_for(from, "s");
        engine.create_object(&slot).await.unwrap();
        ids.push((slot.id(), from));
    }
    let transfers: Vec<_> = ids
        .into_iter()
        .map(|(id, from)| {
            let engine = engine.clone();
            tokio::spawn(async move { engine.transfer_object::<Slot>(id, from, target).await })
        })
        .collect();
    let mut results = Vec::new();
    for h in transfers {
        results.push(h.await.unwrap());
    }
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert!(results
        .iter()
        .any(|r| matches!(r, Err(Error::UniqueConstraintViolation(f)) if f == "owner")));
}

#[tokio::test]
async fn test_delete_bulk_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut ids = Vec::new();
    for i in 0..5 {
        let mut u = User::default();
        u.username = format!("bulk{}", i);
        u.email = format!("bulk{}@x.com", i);
        ids.push(u.id());
        engine.create_object(&u).await.unwrap();
    }

    let deleted = engine
        .delete_objects::<User>(ids[..3].to_vec(), system_owner())
        .await
        .unwrap();
    assert_eq!(deleted, 3);

    assert_eq!(engine.count_objects::<User>(None).await.unwrap(), 2);
}

#[tokio::test]
async fn test_delete_owned_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "downer".into();
    owner.email = "downer@x.com".into();
    engine.create_object(&owner).await.unwrap();

    for i in 0..4 {
        let mut p = Post::default();
        p.set_owner(owner.id());
        p.title = format!("P{}", i);
        engine.create_object(&p).await.unwrap();
    }

    let deleted = engine
        .delete_owned_objects::<Post>(owner.id())
        .await
        .unwrap();
    assert_eq!(deleted, 4);

    let count = engine
        .count_objects::<Post>(Some(Query::new(owner.id())))
        .await
        .unwrap();
    assert_eq!(count, 0);
}
