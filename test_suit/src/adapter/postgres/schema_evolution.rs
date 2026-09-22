use super::*;

// ============================================================
// Section 12: Schema Evolution / Default Fields
// ============================================================

#[tokio::test]
async fn test_default_field_backward_compatible() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "owner".into();
    owner.email = "owner@x.com".into();
    engine.create_object(&owner).await.unwrap();

    // Write a Post (old schema without rating field).
    let mut post = Post::default();
    post.set_owner(owner.id());
    post.title = "Old Post".into();
    engine.create_object(&post).await.unwrap();

    // Read it back as PostNew (new schema with default rating=10).
    let found: Option<PostNew> = engine.fetch_owned_object(owner.id()).await.unwrap();
    assert!(found.is_some());
    assert_eq!(
        found.unwrap().rating,
        10,
        "missing field should get default"
    );
}

#[tokio::test]
async fn test_default_field_value_roundtrip() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "owner2".into();
    owner.email = "owner2@x.com".into();
    engine.create_object(&owner).await.unwrap();

    // Write with explicit rating.
    let mut post = PostNew::default();
    post.set_owner(owner.id());
    post.title = "New Post".into();
    post.rating = 42;
    engine.create_object(&post).await.unwrap();

    let found: Option<PostNew> = engine.fetch_owned_object(owner.id()).await.unwrap();
    assert_eq!(found.unwrap().rating, 42);

    // Update rating.
    post.rating = 100;
    engine.update_object(&mut post).await.unwrap();

    let updated: Option<PostNew> = engine.fetch_owned_object(owner.id()).await.unwrap();
    assert_eq!(updated.unwrap().rating, 100);
}
