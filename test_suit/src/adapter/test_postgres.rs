use ousia::{
    EdgeMeta, Meta, Object, ObjectMeta, ObjectOwnership, OusiaDefault, OusiaEdge, OusiaObject,
    Query,
    adapters::{ObjectRecord, postgres::PostgresAdapter},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

#[cfg(test)]
use ousia::adapters::Adapter;
use ulid::Ulid;

/// Example: Blog Post object
#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "Post",
    index = "title:search+sort",
    index = "status:search"
)]
pub struct Post {
    _meta: Meta,

    pub title: String,
    pub content: String,
    pub status: PostStatus,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub tags: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PostStatus {
    Draft,
    Published,
    Archived,
}

impl Default for PostStatus {
    fn default() -> Self {
        PostStatus::Draft
    }
}

// Implement ToIndexValue for custom enum
impl ousia::query::ToIndexValue for PostStatus {
    fn to_index_value(&self) -> ousia::query::IndexValue {
        let s = match self {
            PostStatus::Draft => "draft",
            PostStatus::Published => "published",
            PostStatus::Archived => "archived",
        };
        ousia::query::IndexValue::String(s.to_string())
    }
}

/// Example: User object
#[derive(OusiaObject, OusiaDefault, Debug, Clone)]
#[ousia(
    type_name = "User",
    index = "email:search",
    index = "username:search+sort"
)]
pub struct User {
    _meta: Meta,

    pub username: String,
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, OusiaEdge)]
#[ousia(type_name = "Follow", index = "notification:search")]
struct Follow {
    _meta: EdgeMeta,
    notification: bool,
}

impl Default for Follow {
    fn default() -> Self {
        Self {
            _meta: EdgeMeta::new(Ulid::nil(), Ulid::nil()),
            notification: false,
        }
    }
}

pub(crate) async fn postgres_test_client() -> (ContainerAsync<Postgres>, PostgresAdapter) {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::new(pool);
    if let Err(err) = adapter.init_schema().await {
        panic!("Error: {:#?}", err);
    }

    (_resource, adapter)
}

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

mod postgres_adpater_test {
    use super::*;

    #[tokio::test]
    async fn test_adapter_insert() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);

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
        let adapter = PostgresAdapter::new(pool);

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
        let adapter = PostgresAdapter::new(pool);

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
        let adapter = PostgresAdapter::new(pool);

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
    }
}

mod engine_test {
    use ousia::{EdgeMetaTrait as _, EdgeQuery, Engine, Error};

    use super::*;

    #[test]
    fn test_object_ownership_is_system_owned() {
        let user = User::default();
        assert!(user.is_system_owned());
    }

    #[test]
    fn test_object_ownership_not_system_owned() {
        let user = User {
            _meta: Meta::new_with_owner(Ulid::new()),
            username: "johndoe".to_string(),
            email: "john.doe@example.com".to_string(),
            display_name: "John Doe".to_string(),
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
        let (_resource, adapter) = postgres_test_client().await;
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
        let adapter = PostgresAdapter::new(pool);
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
        let adapter = PostgresAdapter::new(pool);
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
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create multiple users
        let mut alice = User::default();
        alice.display_name = "Alice".to_string();
        alice.username = "alice".to_string();
        alice.email = "alice@example.com".to_string();

        let mut bob = User::default();
        bob.display_name = "Bob".to_string();
        bob.email = "bob@example.com".to_string();

        engine.create_object(&alice).await.unwrap();
        engine.create_object(&bob).await.unwrap();

        // Query by name
        let users: Vec<User> = engine
            .query_objects(Query::default().where_eq(&User::FIELDS.username, "alice"))
            .await
            .unwrap();

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].username, "alice");
    }

    #[tokio::test]
    async fn test_engine_ownership() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
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
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create two users
        let mut alice = User::default();
        alice.display_name = "Alice".to_string();
        alice.email = "alice@example.com".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.display_name = "Bob".to_string();
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
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create two users
        let mut alice = User::default();
        alice.display_name = "Alice".to_string();
        alice.email = "alice@example.com".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.display_name = "Bob".to_string();
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
        let adapter = PostgresAdapter::new(pool);
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
        let adapter = PostgresAdapter::new(pool);
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
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create owner
        let mut owner = User::default();
        owner.username = "Owner".to_string();
        owner.email = "owner@example.com".to_string();
        engine.create_object(&owner).await.unwrap();

        // Create multiple posts
        for i in 0..10 {
            let mut post = Post::default();
            post.set_owner(owner.id());
            post.title = format!("Post {}", i);
            post.content = format!("Content {}", i);
            engine.create_object(&post).await.unwrap();
        }

        // Query with limit
        let posts: Vec<Post> = engine
            .query_objects(Query::new(owner.id()).with_limit(5))
            .await
            .unwrap();
        assert_eq!(posts.len(), 5);

        // Query with offset
        let posts: Vec<Post> = engine
            .query_objects(Query::new(owner.id()).with_offset(5).with_limit(3))
            .await
            .unwrap();
        assert_eq!(posts.len(), 3);
    }

    #[tokio::test]
    async fn test_transfer_wrong_owner_fails() {
        let (_resource, pool) = setup_test_db().await;
        let adapter = PostgresAdapter::new(pool);
        adapter.init_schema().await.unwrap();

        let engine = Engine::new(Box::new(adapter));

        // Create users
        let mut alice = User::default();
        alice.display_name = "Alice".to_string();
        engine.create_object(&alice).await.unwrap();

        let mut bob = User::default();
        bob.display_name = "Bob".to_string();
        engine.create_object(&bob).await.unwrap();

        let mut charlie = User::default();
        charlie.display_name = "Charlie".to_string();
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
}
