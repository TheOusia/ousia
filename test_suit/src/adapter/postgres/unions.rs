use super::*;

// ============================================================
// Section 4: Union Types
// ============================================================

#[tokio::test]
async fn test_fetch_union_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let result = adapter
        .fetch_union_object(User::TYPE, Post::TYPE, alice.id())
        .await
        .unwrap();
    let union: Union<User, Post> = result.unwrap().into();
    assert!(union.is_first());
}

#[tokio::test]
async fn test_fetch_union_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let mut post = Post::default();
    post.title = "Hello".into();
    adapter
        .insert_object(ObjectRecord::from_object(&post))
        .await
        .unwrap();

    let results = adapter
        .fetch_union_objects(User::TYPE, Post::TYPE, vec![alice.id(), post.id()])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);

    let unions: Vec<Union<User, Post>> = results.into_iter().map(Into::into).collect();
    assert!(unions.iter().any(|u| u.is_first()));
    assert!(unions.iter().any(|u| u.is_second()));
}

#[tokio::test]
async fn test_fetch_owned_union_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let mut post = Post::default();
    post.title = "Owned".into();
    adapter
        .insert_object(ObjectRecord::from_object(&post))
        .await
        .unwrap();

    let results = adapter
        .fetch_owned_union_objects(User::TYPE, Post::TYPE, system_owner())
        .await
        .unwrap();
    assert!(!results.is_empty());

    let unions: Vec<Union<User, Post>> = results.into_iter().map(Into::into).collect();
    assert!(unions.iter().any(|u| u.is_first()));
}
