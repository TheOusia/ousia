#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use super::*;
#[cfg(test)]
use ousia::adapters::Adapter;
#[cfg(test)]
use ousia::{
    EdgeMeta, EdgeMetaTrait, EdgeQuery, Engine, Error, Meta, Object, ObjectMeta, ObjectOwnership,
    Query, Union,
    adapters::{ObjectRecord, sqlite::SqliteAdapter},
    filter, system_owner,
};

#[tokio::test]
async fn test_adapter_insert() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();

    if let Err(err) = adapter.init_schema().await {
        panic!("Error: {:#?}", err);
    }

    let user = User::default();
    if let Err(err) = adapter
        .insert_object(ObjectRecord::from_object(&user))
        .await
    {
        panic!("Error: {:#?}", err);
    };
}

#[tokio::test]
async fn test_adapter_get() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();

    if let Err(err) = adapter.init_schema().await {
        panic!("Error: {:#?}", err);
    }

    let mut user = User::default();
    user.username = "test_user".to_string();
    if let Err(err) = adapter
        .insert_object(ObjectRecord::from_object(&user))
        .await
    {
        panic!("Error: {:#?}", err);
    };

    let user_result = adapter
        .fetch_object(user.type_name(), user.id())
        .await
        .unwrap();
    assert!(user_result.is_some());

    let _user: User = user_result.unwrap().to_object().unwrap();
    assert_eq!(_user.id(), user.id());
    assert_eq!(_user.username, user.username);
}

#[tokio::test]
async fn test_adapter_update() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();

    if let Err(err) = adapter.init_schema().await {
        panic!("Error: {:#?}", err);
    }

    let mut user = User::default();
    user.username = "test_user".to_string();
    if let Err(err) = adapter
        .insert_object(ObjectRecord::from_object(&user))
        .await
    {
        panic!("Error: {:#?}", err);
    } else {
        let user_result = adapter
            .fetch_object(user.type_name(), user.id())
            .await
            .unwrap();
        assert!(user_result.is_some());

        let _user: User = user_result.unwrap().to_object().unwrap();
        assert_eq!(_user.id(), user.id());
        assert_eq!(_user.username, user.username);
    }

    user.username = "new_username".to_string();
    if let Err(err) = adapter
        .update_object(ObjectRecord::from_object(&user))
        .await
    {
        panic!("Error: {:#?}", err);
    } else {
        let user_result = adapter
            .fetch_object(user.type_name(), user.id())
            .await
            .unwrap();
        assert!(user_result.is_some());

        let _user: User = user_result.unwrap().to_object().unwrap();
        assert_eq!(_user.id(), user.id());
        assert_eq!(_user.username, user.username);
    }
}

#[tokio::test]
async fn test_adapter_query() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();

    if let Err(err) = adapter.init_schema().await {
        panic!("Error: {:#?}", err);
    }

    let mut user = User::default();
    user.username = "test_user".to_string();
    user.email = "test@gmail.com".to_string();
    if let Err(err) = adapter
        .insert_object(ObjectRecord::from_object(&user))
        .await
    {
        panic!("Error: {:#?}", err);
    }
    let user_result = adapter
        .fetch_object(user.type_name(), user.id())
        .await
        .unwrap();
    assert!(user_result.is_some());

    let users = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_eq(&User::FIELDS.email, "efedua.bell@gmail.com"),
        )
        .await
        .unwrap();
    assert_eq!(users.len(), 0);

    let users = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_eq(&User::FIELDS.email, "test@gmail.com"),
        )
        .await
        .unwrap();
    assert_eq!(users.len(), 1);

    let mut post_1 = Post::default();
    post_1.set_owner(user.id());

    adapter
        .insert_object(ObjectRecord::from_object(&post_1))
        .await
        .unwrap();

    post_1.status = PostStatus::Published;
    adapter
        .update_object(ObjectRecord::from_object(&post_1))
        .await
        .unwrap();

    let _post: Post = adapter
        .fetch_object(post_1.type_name(), post_1.id())
        .await
        .unwrap()
        .expect("Post not found")
        .to_object()
        .unwrap();
    assert_eq!(_post.id(), post_1.id());

    let posts = adapter
        .query_objects(
            Post::TYPE,
            Query::new(user.id()).where_eq(&Post::FIELDS.status, PostStatus::Published),
        )
        .await
        .unwrap();
    assert_eq!(posts.len(), 1);

    let posts = adapter
        .query_objects(
            Post::TYPE,
            Query::new(user.id()).where_eq(&Post::FIELDS.status, PostStatus::Draft),
        )
        .await
        .unwrap();
    assert_eq!(posts.len(), 0);

    let mut post_with_tag = Post::default();
    post_with_tag.set_owner(user.id());
    post_with_tag.tags = vec!["tag1".to_string(), "tag2".to_string()];

    adapter
        .insert_object(ObjectRecord::from_object(&post_with_tag))
        .await
        .unwrap();

    // adapter.insert_object()
    let posts = adapter
        .query_objects(
            Post::TYPE,
            Query::new(user.id()).where_contains(&Post::FIELDS.tags, vec!["tag1"]),
        )
        .await
        .unwrap();
    assert_eq!(posts.len(), 1);
}

#[test]
fn test_object_ownership_is_system_owned() {
    let user = User::default();
    assert!(user.is_system_owned());
}

#[test]
fn test_object_ownership_not_system_owned() {
    let user = User {
        _meta: Meta::new_with_owner(uuid::Uuid::now_v7()),
        username: "johndoe".to_string(),
        email: "john.doe@example.com".to_string(),
        display_name: "John Doe".to_string(),
        balance: Wallet::default(),
    };
    assert!(!user.is_system_owned());
}

#[test]
fn test_index_meta() {
    let mut user = User::default();
    user.username = "John Doe".to_string();

    assert_eq!(
        user.index_meta()
            .meta()
            .get("username")
            .map(|ik| ik.as_string().unwrap()),
        Some("John Doe")
    );
}

#[test]
fn test_query_fields() {
    assert_eq!(User::FIELDS.username.name, "username");
    assert_eq!(User::FIELDS.email.name, "email");
}

#[tokio::test]
async fn test_engine_create_and_fetch() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.display_name = "Alice".to_string();
    user.email = "alice@example.com".to_string();

    // Create
    engine.create_object(&user).await.unwrap();

    // Fetch
    let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
    assert!(fetched.is_some());
    let fetched = fetched.unwrap();
    assert_eq!(fetched.display_name, "Alice");
    assert_eq!(fetched.email, "alice@example.com");
}

#[tokio::test]
async fn test_engine_update() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.display_name = "Bob".to_string();
    user.email = "bob@example.com".to_string();

    engine.create_object(&user).await.unwrap();

    // Update
    user.display_name = "Robert".to_string();
    engine.update_object(&mut user).await.unwrap();

    // Verify
    let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
    assert_eq!(fetched.unwrap().display_name, "Robert");
}

#[tokio::test]
async fn test_engine_delete() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let mut user = User::default();
    user.display_name = "Charlie".to_string();
    user.email = "charlie@example.com".to_string();

    engine.create_object(&user).await.unwrap();

    // Delete
    let deleted: Option<User> = engine.delete_object(user.id(), user.owner()).await.unwrap();
    assert!(deleted.is_some());

    // Verify deleted
    let fetched: Option<User> = engine.fetch_object(user.id()).await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn test_engine_query() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create multiple users
    let mut alice = User::default();
    alice.display_name = "Alice".to_string();
    alice.username = "alice".to_string();
    alice.email = "alice@example.com".to_string();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut bob = User::default();
    bob.display_name = "Bob".to_string();
    bob.username = "bob".to_string();
    bob.email = "bob@example.com".to_string();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut charlie = User::default();
    charlie.display_name = "Charlie".to_string();
    charlie.username = "charlie".to_string();
    charlie.email = "charlie@example.com".to_string();

    engine.create_object(&alice).await.unwrap();
    engine.create_object(&bob).await.unwrap();
    engine.create_object(&charlie).await.unwrap();

    // Query by name
    let users: Vec<User> = engine
        .query_objects(Query::default().where_eq(&User::FIELDS.username, "alice"))
        .await
        .unwrap();

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].username, "alice");

    // Query with cursor
    let users: Vec<User> = engine
        .query_objects(Query::default().with_cursor(charlie.id()))
        .await
        .unwrap();

    assert_eq!(users.len(), 2);
    assert_eq!(users.get(0).unwrap().username, "bob");
}

#[tokio::test]
async fn test_engine_ownership() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create owner
    let mut owner = User::default();
    owner.display_name = "Owner".to_string();
    owner.email = "owner@example.com".to_string();
    engine.create_object(&owner).await.unwrap();

    // Create owned post
    let mut post = Post::default();
    post.set_owner(owner.id());
    post.title = "My First Post".to_string();
    post.content = "Hello, world!".to_string();
    engine.create_object(&post).await.unwrap();

    // Verify ownership
    assert!(post.is_owned_by(&owner));

    // Fetch owned objects
    let posts: Vec<Post> = engine.fetch_owned_objects(owner.id()).await.unwrap();
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0].title, "My First Post");
}

#[tokio::test]
async fn test_engine_transfer_ownership() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create two users
    let mut alice = User::default();
    alice.display_name = "Alice".to_string();
    alice.username = "alice".to_string();
    alice.email = "alice@example.com".to_string();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.display_name = "Bob".to_string();
    bob.username = "bob".to_string();
    bob.email = "bob@example.com".to_string();
    engine.create_object(&bob).await.unwrap();

    // Create post owned by Alice
    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".to_string();
    post.content = "Original content".to_string();
    engine.create_object(&post).await.unwrap();

    // Transfer to Bob
    let transferred: Post = engine
        .transfer_object(post.id(), alice.id(), bob.id())
        .await
        .unwrap();

    assert_eq!(transferred.owner(), bob.id());
}

#[tokio::test]
async fn test_engine_edges() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create two users
    let mut alice = User::default();
    alice.display_name = "Alice".to_string();
    alice.username = "alice".to_string();
    alice.email = "alice@example.com".to_string();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.display_name = "Bob".to_string();
    bob.username = "bob".to_string();
    bob.email = "bob@example.com".to_string();
    engine.create_object(&bob).await.unwrap();

    // Create follow edge: Alice follows Bob
    let follow = Follow {
        _meta: EdgeMeta::new(alice.id(), bob.id()),
        notification: true,
    };
    engine.create_edge(&follow).await.unwrap();

    // Query edges
    let follows: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(follows.len(), 1);
    assert_eq!(follows[0].from(), alice.id());
    assert_eq!(follows[0].to(), bob.id());
    assert!(follows[0].notification);

    // Delete edge
    engine
        .delete_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();

    // Verify deleted
    let follows: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(follows.len(), 0);
}

#[tokio::test]
async fn test_engine_count_objects() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create multiple users
    for i in 0..5 {
        let mut user = User::default();
        user.username = format!("User{}", i);
        user.email = format!("user{}@example.com", i);
        engine.create_object(&user).await.unwrap();
    }

    // Count all users
    let count: u64 = engine.count_objects::<User>(None).await.unwrap();
    assert_eq!(count, 5);

    // Count with filter
    let count: u64 = engine
        .count_objects::<User>(Some(
            Query::default().where_eq(&User::FIELDS.username, "User0"),
        ))
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_engine_bulk_fetch() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create multiple users
    let mut ids = Vec::new();
    for i in 0..3 {
        let mut user = User::default();
        user.username = format!("User{}", i);
        user.email = format!("user{}@example.com", i);
        ids.push(user.id());
        engine.create_object(&user).await.unwrap();
    }

    // Fetch in bulk
    let users: Vec<User> = engine.fetch_objects(ids).await.unwrap();
    assert_eq!(users.len(), 3);
}

#[tokio::test]
async fn test_engine_complex_query() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create owner
    let mut owner = User::default();
    owner.username = "Owner".to_string();
    owner.email = "owner@example.com".to_string();
    engine.create_object(&owner).await.unwrap();

    let mut created_posts: Vec<Post> = vec![];
    // Create multiple posts
    for i in 0..10 {
        let mut post = Post::default();
        post.set_owner(owner.id());
        post.title = format!("Post {}", i);
        post.content = format!("Content {}", i);
        engine.create_object(&post).await.unwrap();
        created_posts.push(post);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Query with limit
    let posts: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).with_limit(5))
        .await
        .unwrap();
    assert_eq!(posts.len(), 5);

    // Query with offset
    let posts: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id())
                .with_cursor(created_posts[4].id())
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(posts.len(), 3, "Expected 3 posts but got {}", posts.len());
}

#[tokio::test]
async fn test_engine_query_custom_field() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create owner
    let mut owner = User::default();
    owner.username = "Owner".to_string();
    owner.email = "owner@example.com".to_string();
    owner.balance = Wallet { inner: 200 };
    engine.create_object(&owner).await.unwrap();

    let obj = engine
        .find_object::<User>(&[filter!(&User::FIELDS.balance, 200)])
        .await
        .unwrap();

    assert!(obj.is_some())
}

#[tokio::test]
async fn test_transfer_wrong_owner_fails() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    // Create users
    let mut alice = User::default();
    alice.display_name = "Alice".to_string();
    alice.username = "alice".to_string();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.display_name = "Bob".to_string();
    bob.username = "bob".to_string();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.display_name = "Charlie".to_string();
    charlie.username = "charlie".to_string();
    engine.create_object(&charlie).await.unwrap();

    // Create object owned by Alice
    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".to_string();
    engine.create_object(&post).await.unwrap();

    // Try to transfer from Bob to Charlie (should fail - Bob doesn't own it)
    let result: Result<Post, Error> = engine
        .transfer_object(post.id(), bob.id(), charlie.id())
        .await;

    assert!(matches!(result, Err(Error::NotFound)));
}

#[tokio::test]
async fn test_fetch_union_object() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.display_name = "Alice".to_string();
    alice.username = "alice".to_string();
    alice.email = "alice@example.com".to_string();
    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let result = adapter
        .fetch_union_object(User::TYPE, Post::TYPE, alice.id())
        .await;
    let Ok(result) = result else {
        panic!("Failed to fetch union object {:?}", result.unwrap_err());
    };

    let union: Union<User, Post> = result.unwrap().into();
    assert!(union.is_first());
}

#[tokio::test]
async fn test_fetch_union_objects() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();

    let mut post = Post::default();
    post.title = "Hello".into();
    post.content = "World".into();

    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();
    adapter
        .insert_object(ObjectRecord::from_object(&post))
        .await
        .unwrap();

    let result = adapter
        .fetch_union_objects(User::TYPE, Post::TYPE, vec![alice.id(), post.id()])
        .await
        .unwrap();

    assert_eq!(result.len(), 2);

    let unions: Vec<Union<User, Post>> = result.into_iter().map(Into::into).collect();

    assert!(unions.iter().any(|u| u.is_first()));
    assert!(unions.iter().any(|u| u.is_second()));
}

#[tokio::test]
async fn test_fetch_owned_union_object() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();

    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let result = adapter
        .fetch_owned_union_object(User::TYPE, Post::TYPE, system_owner())
        .await
        .unwrap()
        .unwrap();

    let union: Union<User, Post> = result.into();

    assert!(union.is_first());
}

#[tokio::test]
async fn test_fetch_owned_union_objects() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();

    let mut post = Post::default();
    post.title = "Owned Post".into();
    post.content = "Content".into();

    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();
    adapter
        .insert_object(ObjectRecord::from_object(&post))
        .await
        .unwrap();

    let result = adapter
        .fetch_owned_union_objects(User::TYPE, Post::TYPE, system_owner())
        .await
        .unwrap();

    assert!(!result.is_empty());

    let unions: Vec<Union<User, Post>> = result.into_iter().map(Into::into).collect();

    // At least one User must exist
    assert!(unions.iter().any(|u| u.is_first()));
}

#[tokio::test]
async fn test_reverse_edges() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@example.com".into();
    michael.display_name = "Michael".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    bob.display_name = "Bob".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge::<Follow>(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge::<Follow>(&Follow {
            _meta: EdgeMeta::new(michael.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    let alice_following = engine
        .query_edges::<Follow>(alice.id(), EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(alice_following.len(), 1);

    let michael_following = engine
        .query_edges::<Follow>(michael.id(), EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(michael_following.len(), 1);

    let bob_following = engine
        .query_edges::<Follow>(bob.id(), EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(bob_following.len(), 0);

    let bob_followers = engine
        .query_reverse_edges::<Follow>(bob.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(bob_followers.len(), 2);

    let bob_following_count = engine.count_edges::<Follow>(bob.id(), None).await.unwrap();
    assert_eq!(bob_following_count, 0);

    let bob_followers_count = engine
        .count_reverse_edges::<Follow>(bob.id(), None)
        .await
        .unwrap();
    assert_eq!(bob_followers_count, 2);
}

#[tokio::test]
async fn test_unique_object() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "alice".into();
    michael.email = "michael@example.com".into();
    michael.display_name = "Michael".into();
    let err = engine.create_object(&michael).await.unwrap_err();
    assert_eq!(
        err,
        Error::UniqueConstraintViolation(String::from("username"))
    );

    use ousia::{Meta, OusiaDefault, OusiaObject};
    #[derive(OusiaObject, OusiaDefault, Debug)]
    #[ousia(
        unique = "username+email",
        index = "email:search",
        index = "username:search+sort"
    )]
    pub struct CompositeUser {
        _meta: Meta,

        pub username: String,
        pub email: String,
        pub display_name: String,
    }

    let mut alice = CompositeUser::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = CompositeUser::default();
    michael.username = "alice".into();
    michael.email = "michael@example.com".into();
    michael.display_name = "Michael".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = CompositeUser::default();
    bob.username = "alice".into();
    bob.email = "alice@example.com".into();
    bob.display_name = "Bob".into();
    let err = engine.create_object(&bob).await.unwrap_err();

    assert_eq!(
        err,
        Error::UniqueConstraintViolation(String::from("username+email"))
    );
}

#[tokio::test]
async fn test_sequence() {
    let adapter = SqliteAdapter::new_memory().await.unwrap();
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let value = engine.counter_value("my-key".to_string()).await;
    assert_eq!(value, 1);

    let value = engine.counter_next_value("my-key".to_string()).await;
    assert_eq!(value, 2);

    let value = engine.counter_value("my-key".to_string()).await;
    assert_eq!(value, 2);
}
