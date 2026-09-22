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
