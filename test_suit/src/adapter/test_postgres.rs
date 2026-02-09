#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use super::*;
#[cfg(test)]
use ousia::{
    EdgeMeta, EdgeMetaTrait, EdgeQuery, Engine, Error, Meta, Object, ObjectMeta, ObjectOwnership,
    Query, Union,
    adapters::{ObjectRecord, postgres::PostgresAdapter},
    filter, system_owner,
};
#[cfg(test)]
use sqlx::PgPool;
#[cfg(test)]
use testcontainers::ContainerAsync;
#[cfg(test)]
use testcontainers_modules::postgres::Postgres;

#[cfg(test)]
use ousia::adapters::Adapter;

#[cfg(test)]
pub(crate) async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};

    let postgres = match Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_tag("16-alpine")
        .start()
        .await
    {
        Ok(postgres) => postgres,
        Err(err) => panic!("Failed to start Postgres: {}", err),
    };
    // Give DB time to start
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
    {
        Ok(pool) => pool,
        Err(err) => panic!("Failed to connect to Postgres: {}", err),
    };

    (postgres, pool)
}

#[tokio::test]
async fn test_adapter_insert() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);

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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);

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

    let user_result = adapter.fetch_object(user.id()).await.unwrap();
    assert!(user_result.is_some());

    let _user: User = user_result.unwrap().to_object().unwrap();
    assert_eq!(_user.id(), user.id());
    assert_eq!(_user.username, user.username);
}

#[tokio::test]
async fn test_adapter_update() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);

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
        let user_result = adapter.fetch_object(user.id()).await.unwrap();
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
        let user_result = adapter.fetch_object(user.id()).await.unwrap();
        assert!(user_result.is_some());

        let _user: User = user_result.unwrap().to_object().unwrap();
        assert_eq!(_user.id(), user.id());
        assert_eq!(_user.username, user.username);
    }
}

#[tokio::test]
async fn test_adapter_query() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);

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
    let user_result = adapter.fetch_object(user.id()).await.unwrap();
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
        .fetch_object(post_1.id())
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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

#[cfg(feature = "ledger")]
mod ledger_tests {
    use super::*;
    use ousia::adapters::postgres::PostgresAdapter;
    use ousia::ledger::{
        Asset, Balance, LedgerAdapter, LedgerSystem, Money, MoneyError, Transaction,
    };
    use sqlx::postgres::PgPoolOptions;
    use std::sync::Arc;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_full_transaction_flow() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        // Create asset
        let asset = Asset::new("USD", 10_000);
        adapter.create_asset(asset.clone()).await.unwrap();

        // Create two users
        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();

        // Mint money to Alice
        adapter
            .mint_value_objects(asset.id, alice, 50_000, "initial".to_string())
            .await
            .unwrap();

        // Check Alice's balance
        let balance = adapter.get_balance(asset.id, alice).await.unwrap();
        assert_eq!(balance.available, 50_000);
        assert_eq!(balance.reserved, 0);

        // Transfer to Bob
        let to_burn = adapter
            .select_for_burn(asset.id, alice, 20_000)
            .await
            .unwrap();
        let burn_ids: Vec<Uuid> = to_burn.iter().map(|vo| vo.id).collect();
        adapter
            .burn_value_objects(burn_ids, "transfer".to_string())
            .await
            .unwrap();

        adapter
            .mint_value_objects(asset.id, bob, 20_000, "transfer".to_string())
            .await
            .unwrap();

        // Check final balances
        let alice_balance = adapter.get_balance(asset.id, alice).await.unwrap();
        let bob_balance = adapter.get_balance(asset.id, bob).await.unwrap();

        assert_eq!(alice_balance.available, 30_000);
        assert_eq!(bob_balance.available, 20_000);
    }

    #[tokio::test]
    async fn test_idempotency() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let asset = Asset::new("EUR", 10_000);
        adapter.create_asset(asset.clone()).await.unwrap();

        let user = uuid::Uuid::now_v7();
        let key = "unique-key-123";

        // First mint
        let tx_id1 = adapter
            .mint_idempotent(key, asset.id, user, 10_000, "deposit".to_string())
            .await
            .unwrap();

        // Second mint with same key - should return same transaction
        let tx_id2 = adapter
            .mint_idempotent(key, asset.id, user, 10_000, "deposit".to_string())
            .await
            .unwrap();

        assert_eq!(tx_id1, tx_id2);

        // Balance should only reflect one mint
        let balance = adapter.get_balance(asset.id, user).await.unwrap();
        assert_eq!(balance.available, 10_000);
    }

    #[tokio::test]
    async fn test_transaction_reversion() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let asset = Asset::new("GBP", 10_000);
        adapter.create_asset(asset.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();

        // Mint to Alice
        adapter
            .mint_value_objects(asset.id, alice, 30_000, "initial".to_string())
            .await
            .unwrap();

        // Transfer to Bob
        let to_burn = adapter
            .select_for_burn(asset.id, alice, 15_000)
            .await
            .unwrap();
        let burn_ids: Vec<Uuid> = to_burn.iter().map(|vo| vo.id).collect();
        adapter
            .burn_value_objects(burn_ids, "transfer".to_string())
            .await
            .unwrap();

        adapter
            .mint_value_objects(asset.id, bob, 15_000, "transfer".to_string())
            .await
            .unwrap();

        // Record the transaction
        let tx = Transaction::new(
            asset.id,
            Some(alice),
            Some(bob),
            15_000,
            15_000,
            "transfer".to_string(),
        );
        let tx_id = adapter.record_transaction(tx).await.unwrap();

        // Revert it
        let _revert_tx_id = adapter
            .revert_transaction(tx_id, "mistake".to_string())
            .await
            .unwrap();

        // Check balances are back to original
        let alice_balance = adapter.get_balance(asset.id, alice).await.unwrap();
        let bob_balance = adapter.get_balance(asset.id, bob).await.unwrap();

        assert_eq!(alice_balance.available, 30_000);
        assert_eq!(bob_balance.available, 0);
    }

    #[tokio::test]
    async fn test_create_assets() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Create assets
        let usd = Asset::new("USD", 100_00);
        let ngn = Asset::new("NGN", 1_000_00);

        system.adapter().create_asset(usd.clone()).await.unwrap();
        system.adapter().create_asset(ngn.clone()).await.unwrap();

        // Verify assets can be retrieved
        let retrieved_usd = system.adapter().get_asset("USD").await.unwrap();
        let retrieved_ngn = system.adapter().get_asset("NGN").await.unwrap();

        assert_eq!(retrieved_usd.code, "USD");
        assert_eq!(retrieved_usd.unit, 100_00);
        assert_eq!(retrieved_ngn.code, "NGN");
        assert_eq!(retrieved_ngn.unit, 1_000_00);
    }

    #[tokio::test]
    async fn test_mint_money() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Create asset
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();

        // Mint $500 to Alice
        let mint_tx = Money::mint(
            "USD",
            alice,
            500_00,
            "Initial deposit".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        assert_eq!(mint_tx.amount, 500_00);

        // Check Alice's balance
        let alice_balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        assert_eq!(alice_balance.available, 500_00);
        assert_eq!(alice_balance.reserved, 0);
        assert_eq!(alice_balance.total, 500_00);
    }

    #[tokio::test]
    async fn test_transfer_money() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();

        // Mint to Alice
        Money::mint(
            "USD",
            alice,
            500_00,
            "Initial deposit".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        // Transfer from Alice to Bob
        let alice_money = Money::new(system.clone(), "USD", alice);
        let slice = alice_money.slice(200_00).unwrap();

        slice
            .transfer_to(bob, "Payment for services".to_string())
            .await
            .unwrap();

        // Check balances
        let alice_balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        let bob_balance = Balance::get("USD", bob, system.clone()).await.unwrap();

        assert_eq!(alice_balance.available, 300_00);
        assert_eq!(bob_balance.available, 200_00);
    }

    #[tokio::test]
    async fn test_transfer_insufficient_funds() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();

        // Mint small amount to Alice
        Money::mint("USD", alice, 50_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Try to transfer more than Alice has
        let alice_money = Money::new(system.clone(), "USD", alice);
        let slice = alice_money.slice(100_00).unwrap();

        let result = slice.transfer_to(bob, "payment".to_string()).await;

        assert!(matches!(result, Err(MoneyError::InsufficientFunds)));
    }

    #[tokio::test]
    async fn test_reserve_money() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let bob = uuid::Uuid::now_v7();
        let marketplace = uuid::Uuid::now_v7();

        // Mint to Bob
        Money::mint("USD", bob, 200_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Reserve money
        let reserve_tx = Money::reserve(
            "USD",
            bob,
            marketplace,
            100_00,
            "Escrow for order #123".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        assert_eq!(reserve_tx.amount, 100_00);

        // Check balance
        let bob_balance = Balance::get("USD", bob, system.clone()).await.unwrap();
        assert_eq!(bob_balance.available, 100_00);
        assert_eq!(bob_balance.reserved, 100_00);
        assert_eq!(bob_balance.total, 200_00);
    }

    #[tokio::test]
    async fn test_idempotent_minting() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let charlie = uuid::Uuid::now_v7();
        let idempotency_key = "webhook-123-retry-1";

        // First mint
        let tx1 = Money::mint_idempotent(
            idempotency_key.to_string(),
            "USD",
            charlie,
            75_00,
            "Webhook deposit".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        // Retry with same key (simulating webhook retry)
        let tx2 = Money::mint_idempotent(
            idempotency_key.to_string(),
            "USD",
            charlie,
            75_00,
            "Webhook deposit".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        // Should return the same transaction
        assert_eq!(tx1.transaction_id, tx2.transaction_id);

        // Balance should only reflect one mint
        let charlie_balance = Balance::get("USD", charlie, system.clone()).await.unwrap();
        assert_eq!(charlie_balance.available, 75_00);
    }

    #[tokio::test]
    async fn test_burn_money() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();

        // Mint to Alice
        Money::mint("USD", alice, 300_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Burn some money
        let alice_money = Money::new(system.clone(), "USD", alice);
        let slice = alice_money.slice(50_00).unwrap();

        slice.burn("Fee deduction".to_string()).await.unwrap();

        // Check balance
        let alice_balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        assert_eq!(alice_balance.available, 250_00);
    }

    #[tokio::test]
    async fn test_multiple_transfers() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();
        let charlie = uuid::Uuid::now_v7();

        // Mint to Alice
        Money::mint("USD", alice, 1000_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Alice transfers to Bob
        let alice_money = Money::new(system.clone(), "USD", alice);
        let slice = alice_money.slice(300_00).unwrap();
        slice
            .transfer_to(bob, "payment 1".to_string())
            .await
            .unwrap();

        // Bob transfers to Charlie
        let bob_money = Money::new(system.clone(), "USD", bob);
        let slice = bob_money.slice(150_00).unwrap();
        slice
            .transfer_to(charlie, "payment 2".to_string())
            .await
            .unwrap();

        // Alice transfers to Charlie
        let alice_money = Money::new(system.clone(), "USD", alice);
        let slice = alice_money.slice(200_00).unwrap();
        slice
            .transfer_to(charlie, "payment 3".to_string())
            .await
            .unwrap();

        // Check final balances
        let alice_balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        let bob_balance = Balance::get("USD", bob, system.clone()).await.unwrap();
        let charlie_balance = Balance::get("USD", charlie, system.clone()).await.unwrap();

        assert_eq!(alice_balance.available, 500_00); // 1000 - 300 - 200
        assert_eq!(bob_balance.available, 150_00); // 300 - 150
        assert_eq!(charlie_balance.available, 350_00); // 150 + 200
    }

    #[tokio::test]
    async fn test_invalid_amount() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();

        // Try to mint negative amount
        let result =
            Money::mint("USD", alice, -100_00, "invalid".to_string(), system.clone()).await;

        assert!(matches!(result, Err(MoneyError::InvalidAmount)));

        // Try to mint zero
        let result = Money::mint("USD", alice, 0, "invalid".to_string(), system.clone()).await;

        assert!(matches!(result, Err(MoneyError::InvalidAmount)));
    }

    #[tokio::test]
    async fn test_asset_not_found() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        let alice = uuid::Uuid::now_v7();

        // Try to mint with non-existent asset
        let result =
            Money::mint("INVALID", alice, 100_00, "test".to_string(), system.clone()).await;

        assert!(matches!(result, Err(MoneyError::AssetNotFound(_))));
    }

    #[tokio::test]
    async fn test_concurrent_transfers() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Setup
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();
        let bob = uuid::Uuid::now_v7();
        let charlie = uuid::Uuid::now_v7();

        // Mint to Alice
        Money::mint("USD", alice, 1000_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Spawn concurrent transfers
        let system1 = system.clone();
        let system2 = system.clone();

        let handle1 = tokio::spawn(async move {
            let alice_money = Money::new(system1.clone(), "USD", alice);
            let slice = alice_money.slice(200_00).unwrap();
            slice.transfer_to(bob, "concurrent 1".to_string()).await
        });

        let handle2 = tokio::spawn(async move {
            let alice_money = Money::new(system2.clone(), "USD", alice);
            let slice = alice_money.slice(300_00).unwrap();
            slice.transfer_to(charlie, "concurrent 2".to_string()).await
        });

        // Both should succeed
        handle1.await.unwrap().unwrap();
        handle2.await.unwrap().unwrap();

        // Check final balances
        let alice_balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        let bob_balance = Balance::get("USD", bob, system.clone()).await.unwrap();
        let charlie_balance = Balance::get("USD", charlie, system.clone()).await.unwrap();

        assert_eq!(alice_balance.available, 500_00); // 1000 - 200 - 300
        assert_eq!(bob_balance.available, 200_00);
        assert_eq!(charlie_balance.available, 300_00);
    }

    #[tokio::test]
    async fn test_fragmentation() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        // Create asset with small unit size
        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let alice = uuid::Uuid::now_v7();

        // Mint amount larger than unit - should fragment
        Money::mint(
            "USD",
            alice,
            500_00, // 5x the unit size
            "deposit".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        let balance = Balance::get("USD", alice, system.clone()).await.unwrap();
        assert_eq!(balance.available, 500_00);

        // The adapter should have created 5 ValueObjects of 100_00 each
        // We can't directly check this without exposing internal methods,
        // but the balance should be correct
    }

    #[tokio::test]
    async fn test_balance_with_mixed_states() {
        let (_container, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::from_pool(pool);
        adapter.init_schema().await.unwrap();

        let system = Arc::new(LedgerSystem::new(Box::new(adapter)));

        let usd = Asset::new("USD", 100_00);
        system.adapter().create_asset(usd.clone()).await.unwrap();

        let bob = uuid::Uuid::now_v7();
        let marketplace = uuid::Uuid::now_v7();

        // Mint to Bob
        Money::mint("USD", bob, 500_00, "deposit".to_string(), system.clone())
            .await
            .unwrap();

        // Reserve some
        Money::reserve(
            "USD",
            bob,
            marketplace,
            200_00,
            "escrow".to_string(),
            system.clone(),
        )
        .await
        .unwrap();

        // Burn some
        let bob_money = Money::new(system.clone(), "USD", bob);
        let slice = bob_money.slice(50_00).unwrap();
        slice.burn("fee".to_string()).await.unwrap();

        // Check balance
        let balance = Balance::get("USD", bob, system.clone()).await.unwrap();
        assert_eq!(balance.available, 250_00); // 500 - 200 (reserved) - 50 (burned)
        assert_eq!(balance.reserved, 200_00);
        assert_eq!(balance.total, 450_00); // 250 + 200
    }
}
