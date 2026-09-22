use super::*;

// ============================================================
// Section 15: Atomic writes (object row + unique keys + geo rows)
// ============================================================

fn slot_for(owner: uuid::Uuid, label: &str) -> Slot {
    let mut slot = Slot::default();
    slot.set_owner(owner);
    slot.label = label.into();
    slot
}

async fn side_rows(pool: &PgPool, id: uuid::Uuid) -> (i64, i64) {
    let unique: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM unique_constraints WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    let geo: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    (unique, geo)
}

#[tokio::test]
async fn test_update_missing_object_reserves_no_unique_keys() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut ghost = User::default();
    ghost.username = "ghost".into();
    // 1.x behaviour: updating a missing object is not an error
    engine.update_object(&mut ghost).await.unwrap();
    assert_eq!(side_rows(&pool, ghost.id()).await.0, 0);
    // so the username is still free
    let mut real = User::default();
    real.username = "ghost".into();
    engine.create_object(&real).await.unwrap();
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
    assert_eq!((stored.username.as_str(), stored.display_name.as_str()), ("bob", "Bob"));
    let mut other = User::default();
    other.username = "bob".into();
    assert!(engine.create_object(&other).await.is_err(), "bob's key must survive");
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
    let still: Slot = engine.fetch_object(alice_slot.id()).await.unwrap().unwrap();
    assert_eq!(still.owner(), alice);

    // a successful transfer moves the key: bob's slot gone, so alice's can move
    let carol = uuid::Uuid::now_v7();
    let moved: Slot = engine.transfer_object(alice_slot.id(), alice, carol).await.unwrap();
    assert_eq!(moved.owner(), carol);
    engine.create_object(&slot_for(alice, "a2")).await.unwrap();
    assert!(engine.create_object(&slot_for(carol, "c2")).await.is_err());
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

    let stale = serde_json::json!({ "label": "old" });
    let res = adapter
        .transfer_object_atomic(Slot::TYPE, slot.id(), alice, bob, stale, slot_for(bob, "a").derive_unique_hashes())
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
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let engine = engine.clone();
            tokio::spawn(async move { engine.create_object(&slot_for(owner, &format!("c{i}"))).await })
        })
        .collect();
    let mut ok = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 1);
}

#[tokio::test]
async fn test_deletes_remove_unique_keys_and_geo_rows() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // single delete: unique keys freed
    let owner = uuid::Uuid::now_v7();
    let slot = slot_for(owner, "s");
    engine.create_object(&slot).await.unwrap();
    assert_eq!(side_rows(&pool, slot.id()).await.0, 1);
    let deleted: Option<Slot> = engine.delete_object(slot.id(), owner).await.unwrap();
    assert!(deleted.is_some());
    assert_eq!(side_rows(&pool, slot.id()).await, (0, 0));
    engine.create_object(&slot_for(owner, "s2")).await.unwrap();

    // geo rows go with the object
    let mut place = Place::default();
    place.set_owner(owner);
    place.lat = 6.5;
    place.lon = 3.3;
    engine.create_object(&place).await.unwrap();
    assert_eq!(side_rows(&pool, place.id()).await.1, 1);
    engine.delete_object::<Place>(place.id(), owner).await.unwrap();
    assert_eq!(side_rows(&pool, place.id()).await, (0, 0));

    // bulk delete only touches the owner's rows
    let other = uuid::Uuid::now_v7();
    let mut mine = Vec::new();
    for name in ["m1", "m2"] {
        let mut u = User::default();
        u.set_owner(owner);
        u.username = name.into();
        engine.create_object(&u).await.unwrap();
        mine.push(u.id());
    }
    let mut theirs = User::default();
    theirs.set_owner(other);
    theirs.username = "t1".into();
    engine.create_object(&theirs).await.unwrap();
    let mut ids = mine.clone();
    ids.push(theirs.id());
    assert_eq!(engine.delete_objects::<User>(ids, owner).await.unwrap(), 2);
    for id in &mine {
        assert_eq!(side_rows(&pool, *id).await.0, 0);
    }
    assert_eq!(side_rows(&pool, theirs.id()).await.0, 1, "another owner's key must stay");

    // delete everything an owner has
    assert_eq!(engine.delete_owned_objects::<Slot>(owner).await.unwrap(), 1);
    engine.create_object(&slot_for(owner, "s3")).await.unwrap();
    let mut again = User::default();
    again.username = "m1".into();
    engine.create_object(&again).await.unwrap();
}
