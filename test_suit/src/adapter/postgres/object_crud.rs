use super::*;

// ============================================================
// Section 1: Object CRUD
// ============================================================

#[tokio::test]
async fn test_insert_and_fetch_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let mut user = User::default();
    user.username = "alice".into();
    user.email = "alice@example.com".into();
    adapter
        .insert_object(ObjectRecord::from_object(&user))
        .await
        .unwrap();

    let fetched = adapter.fetch_object(User::TYPE, user.id()).await.unwrap();
    assert!(fetched.is_some());
    let fetched: User = fetched.unwrap().to_object().unwrap();
    assert_eq!(fetched.id(), user.id());
    assert_eq!(fetched.username, "alice");
}

#[tokio::test]
async fn test_fetch_missing_object_returns_none() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let result = adapter
        .fetch_object(User::TYPE, uuid::Uuid::now_v7())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_update_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.username = "bob".into();
    user.email = "bob@example.com".into();
    engine.create_object(&user).await.unwrap();

    user.display_name = "Robert".into();
    engine.update_object(&mut user).await.unwrap();

    let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
    assert_eq!(fetched.unwrap().display_name, "Robert");
}

#[tokio::test]
async fn test_delete_object_returns_deleted() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.username = "charlie".into();
    user.email = "charlie@example.com".into();
    engine.create_object(&user).await.unwrap();

    let deleted: Option<User> = engine.delete_object(user.id(), user.owner()).await.unwrap();
    assert!(deleted.is_some());

    let gone: Option<User> = engine.fetch_object(user.id()).await.unwrap();
    assert!(gone.is_none());
}

#[tokio::test]
async fn test_delete_missing_object_returns_none() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let result: Option<User> = engine
        .delete_object(uuid::Uuid::now_v7(), system_owner())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_bulk_fetch_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut ids = Vec::new();
    for i in 0..4 {
        let mut user = User::default();
        user.username = format!("bulk{}", i);
        user.email = format!("bulk{}@example.com", i);
        ids.push(user.id());
        engine.create_object(&user).await.unwrap();
    }

    let fetched: Vec<User> = engine.fetch_objects(ids.clone()).await.unwrap();
    assert_eq!(fetched.len(), 4);

    // Fetching with a missing ID still returns the ones that exist.
    let mut partial = ids[..2].to_vec();
    partial.push(uuid::Uuid::now_v7());
    let fetched_partial: Vec<User> = engine.fetch_objects(partial).await.unwrap();
    assert_eq!(fetched_partial.len(), 2);
}

#[tokio::test]
async fn test_fetch_owned_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "owner".into();
    owner.email = "owner@example.com".into();
    engine.create_object(&owner).await.unwrap();

    let mut post = Post::default();
    post.set_owner(owner.id());
    post.title = "The Post".into();
    engine.create_object(&post).await.unwrap();

    let found: Option<Post> = engine.fetch_owned_object(owner.id()).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "The Post");

    // Wrong owner → None.
    let none: Option<Post> = engine
        .fetch_owned_object(uuid::Uuid::now_v7())
        .await
        .unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn test_fetch_owned_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "poster".into();
    owner.email = "poster@example.com".into();
    engine.create_object(&owner).await.unwrap();

    for i in 0..3 {
        let mut post = Post::default();
        post.set_owner(owner.id());
        post.title = format!("Post {}", i);
        engine.create_object(&post).await.unwrap();
    }

    let posts: Vec<Post> = engine.fetch_owned_objects(owner.id()).await.unwrap();
    assert_eq!(posts.len(), 3);
}
