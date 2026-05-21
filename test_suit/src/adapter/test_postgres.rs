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
async fn setup_test_db() -> (ContainerAsync<Postgres>, PgPool) {
    use sqlx::postgres::PgPoolOptions;
    use testcontainers::{ImageExt, runners::AsyncRunner as _};

    // ousia's init_schema enables the postgis extension unconditionally, so the
    // test container must ship with postgis pre-installed. postgis/postgis is a
    // drop-in for the official postgres image (same env vars, same port).
    let postgres = match Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_name("postgis/postgis")
        .with_tag("16-3.4-alpine")
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
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    // Users (all system-owned): alice=50, bob=100, charlie=150
    let mut alice = User::default();
    alice.username = "alice".to_string();
    alice.email = "alice@example.com".to_string();
    alice.balance = Wallet { inner: 50 };
    adapter
        .insert_object(ObjectRecord::from_object(&alice))
        .await
        .unwrap();

    let mut bob = User::default();
    bob.username = "bob".to_string();
    bob.email = "bob@example.com".to_string();
    bob.balance = Wallet { inner: 100 };
    adapter
        .insert_object(ObjectRecord::from_object(&bob))
        .await
        .unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".to_string();
    charlie.email = "charlie@example.com".to_string();
    charlie.balance = Wallet { inner: 150 };
    adapter
        .insert_object(ObjectRecord::from_object(&charlie))
        .await
        .unwrap();

    // Posts owned by alice
    // post_a: "Alpha post",     Draft,     ["rust","orm"]
    // post_b: "Beta showcase",  Published, ["rust","async"]
    // post_c: "Gamma post review", Draft,  ["async","testing"]
    // post_d: "Delta summary",  Archived,  ["testing"]
    let owner_id = alice.id();

    let mut post_a = Post::default();
    post_a.set_owner(owner_id);
    post_a.title = "Alpha post".to_string();
    post_a.status = PostStatus::Draft;
    post_a.tags = vec!["rust".to_string(), "orm".to_string()];
    adapter
        .insert_object(ObjectRecord::from_object(&post_a))
        .await
        .unwrap();

    let mut post_b = Post::default();
    post_b.set_owner(owner_id);
    post_b.title = "Beta showcase".to_string();
    post_b.status = PostStatus::Published;
    post_b.tags = vec!["rust".to_string(), "async".to_string()];
    adapter
        .insert_object(ObjectRecord::from_object(&post_b))
        .await
        .unwrap();

    let mut post_c = Post::default();
    post_c.set_owner(owner_id);
    post_c.title = "Gamma post review".to_string();
    post_c.status = PostStatus::Draft;
    post_c.tags = vec!["async".to_string(), "testing".to_string()];
    adapter
        .insert_object(ObjectRecord::from_object(&post_c))
        .await
        .unwrap();

    let mut post_d = Post::default();
    post_d.set_owner(owner_id);
    post_d.title = "Delta summary".to_string();
    post_d.status = PostStatus::Archived;
    post_d.tags = vec!["testing".to_string()];
    adapter
        .insert_object(ObjectRecord::from_object(&post_d))
        .await
        .unwrap();

    // ── String: Equal / NotEqual ──────────────────────────────────────────────
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_eq(&Post::FIELDS.title, "Alpha post"),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "where_eq(title): {:#?}", result);

    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_ne(&Post::FIELDS.title, "Alpha post"),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 3, "where_ne(title): {:#?}", result);

    // ── String: BeginsWith ────────────────────────────────────────────────────
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_begins_with(&Post::FIELDS.title, "Alpha"),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "where_begins_with(title): {:#?}", result);

    // ── String: Contains / NotContains ────────────────────────────────────────
    // "post" is in "Alpha post" and "Gamma post review"
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::wide().where_contains(&Post::FIELDS.title, "post"),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_contains(title): {:#?}", result);

    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::wide().where_not_contains(&Post::FIELDS.title, "post"),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_not_contains(title): {:#?}", result);

    // ── Enum: Equal / NotEqual ────────────────────────────────────────────────
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_eq(&Post::FIELDS.status, PostStatus::Published),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1, "where_eq(status=Published): {:#?}", result);

    // Draft×2 + Published×1 + Archived×1 → NotEqual(Draft) = 2
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_ne(&Post::FIELDS.status, PostStatus::Draft),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_ne(status=Draft): {:#?}", result);

    // ── Array: Contains / ContainsAll / NotContains ───────────────────────────
    // "rust" in post_a(["rust","orm"]) and post_b(["rust","async"])
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_contains(&Post::FIELDS.tags, vec!["rust"]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        2,
        "where_contains(tags=[rust]): {:#?}",
        result
    );

    // ContainsAll with single element behaves the same as Contains
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_contains_all(&Post::FIELDS.tags, vec!["rust"]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        2,
        "where_contains_all(tags=[rust]): {:#?}",
        result
    );

    // "testing" in post_c and post_d → not_contains gives post_a and post_b = 2
    let result = adapter
        .query_objects(
            Post::TYPE,
            Query::new(owner_id).where_not_contains(&Post::FIELDS.tags, vec!["testing"]),
        )
        .await
        .unwrap();
    assert_eq!(
        result.len(),
        2,
        "where_not_contains(tags=[testing]): {:#?}",
        result
    );

    // ── Int: GreaterThan / GreaterThanOrEqual / LessThan / LessThanOrEqual ────
    // alice=50, bob=100, charlie=150 (3 system-owned users total)
    let result = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_gt(&User::FIELDS.balance, Wallet { inner: 50 }),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_gt(balance>50): {:#?}", result);

    let result = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_gte(&User::FIELDS.balance, Wallet { inner: 100 }),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_gte(balance>=100): {:#?}", result);

    let result = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_lt(&User::FIELDS.balance, Wallet { inner: 150 }),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_lt(balance<150): {:#?}", result);

    let result = adapter
        .query_objects(
            User::TYPE,
            Query::default().where_lte(&User::FIELDS.balance, Wallet { inner: 100 }),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 2, "where_lte(balance<=100): {:#?}", result);
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
async fn test_engine_query_sort() {
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
        .query_objects(Query::default().sort_desc(&User::FIELDS.username))
        .await
        .unwrap();

    assert_eq!(users.len(), 3);
    assert_eq!(&users[0].username, "charlie");
    assert_eq!(&users[1].username, "bob");
    assert_eq!(&users[2].username, "alice");
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

#[tokio::test]
async fn test_sequence() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(adapter));

    let value = engine.counter_value("my-key".to_string()).await;
    assert_eq!(value, 1);

    let value = engine.counter_next_value("my-key".to_string()).await;
    assert_eq!(value, 2);

    let value = engine.counter_value("my-key".to_string()).await;
    assert_eq!(value, 2);
}

// ============================================================
// Preload API — Single Pivot (QueryContext / EdgeQueryContext)
// ============================================================

#[tokio::test]
async fn test_preload_object_get() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    alice.display_name = "Alice".into();
    engine.create_object(&alice).await.unwrap();

    let found: Option<User> = engine.preload_object(alice.id()).get().await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().username, "alice");

    let missing: Option<User> = engine
        .preload_object(uuid::Uuid::now_v7())
        .get()
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_preload_single_pivot_following() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();

    let following: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();

    assert_eq!(following.len(), 2);
    let ids: std::collections::HashSet<_> = following.iter().map(|u| u.id()).collect();
    assert!(ids.contains(&bob.id()));
    assert!(ids.contains(&charlie.id()));

    let bobs_following: Vec<User> = engine
        .preload_object::<User>(bob.id())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();
    assert!(bobs_following.is_empty());
}

#[tokio::test]
async fn test_preload_single_pivot_followers() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@example.com".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(michael.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    let followers: Vec<User> = engine
        .preload_object::<User>(bob.id())
        .edge::<Follow, User>()
        .collect_reverse()
        .await
        .unwrap();

    assert_eq!(followers.len(), 2);
    let ids: std::collections::HashSet<_> = followers.iter().map(|u| u.id()).collect();
    assert!(ids.contains(&alice.id()));
    assert!(ids.contains(&michael.id()));
}

#[tokio::test]
async fn test_preload_single_pivot_collect_edges() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let edges: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].from(), alice.id());
    assert_eq!(edges[0].to(), bob.id());
    assert!(edges[0].notification);
}

#[tokio::test]
async fn test_fetch_edge() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();

    assert!(edge.is_some());
    assert!(edge.unwrap().notification);
}

#[tokio::test]
async fn test_preload_single_pivot_collect_with_target() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let pairs = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();

    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].edge().from(), alice.id());
    assert_eq!(pairs[0].edge().to(), bob.id());
    assert!(pairs[0].edge().notification);
    assert_eq!(pairs[0].object().username, "bob");
}

#[tokio::test]
async fn test_preload_single_pivot_collect_both() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (following, followers) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both()
        .await
        .unwrap();

    assert_eq!(following.len(), 1);
    assert_eq!(following[0].username, "bob");
    assert_eq!(followers.len(), 1);
    assert_eq!(followers[0].username, "charlie");
}

#[tokio::test]
async fn test_preload_single_pivot_collect_both_with_target() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (fwd_pairs, rev_pairs) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both_with_target()
        .await
        .unwrap();

    assert_eq!(fwd_pairs.len(), 1);
    assert_eq!(fwd_pairs[0].edge().from(), alice.id());
    assert_eq!(fwd_pairs[0].object().username, "bob");

    assert_eq!(rev_pairs.len(), 1);
    assert_eq!(rev_pairs[0].edge().from(), charlie.id());
    assert_eq!(rev_pairs[0].object().username, "charlie");
}

#[tokio::test]
async fn test_preload_single_pivot_collect_both_edges() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(charlie.id(), alice.id()),
            notification: false,
        })
        .await
        .unwrap();

    let (fwd_edges, rev_edges): (Vec<Follow>, Vec<Follow>) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both_edges()
        .await
        .unwrap();

    assert_eq!(fwd_edges.len(), 1);
    assert_eq!(fwd_edges[0].from(), alice.id());
    assert_eq!(fwd_edges[0].to(), bob.id());
    assert!(fwd_edges[0].notification);

    assert_eq!(rev_edges.len(), 1);
    assert_eq!(rev_edges[0].from(), charlie.id());
    assert_eq!(rev_edges[0].to(), alice.id());
    assert!(!rev_edges[0].notification);
}

#[tokio::test]
async fn test_preload_single_pivot_edge_filter() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();

    let notified: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, true)
        .collect()
        .await
        .unwrap();

    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].username, "bob");

    let silent: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, false)
        .collect()
        .await
        .unwrap();

    assert_eq!(silent.len(), 1);
    assert_eq!(silent[0].username, "charlie");
}

// ============================================================
// Preload API — Multi-Pivot (MultiPreloadContext)
// ============================================================

#[tokio::test]
async fn test_preload_multi_pivot_following() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();

    let results: Vec<(User, Vec<User>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);

    let alice_entry = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert_eq!(alice_entry.1[0].username, "bob");

    let bob_entry = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].username, "charlie");

    let charlie_entry = results
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert!(charlie_entry.1.is_empty());
}

#[tokio::test]
async fn test_preload_multi_pivot_followers() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@example.com".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(michael.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    let results: Vec<(User, Vec<User>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse()
        .await
        .unwrap();

    assert_eq!(results.len(), 3);

    let bob_entry = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 2);
    let follower_names: std::collections::HashSet<_> =
        bob_entry.1.iter().map(|u| u.username.as_str()).collect();
    assert!(follower_names.contains("alice"));
    assert!(follower_names.contains("michael"));

    let alice_entry = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert!(alice_entry.1.is_empty());
}

#[tokio::test]
async fn test_preload_multi_pivot_collect_edges() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let results: Vec<(User, Vec<Follow>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);

    let alice_entry = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert_eq!(alice_entry.1[0].from(), alice.id());
    assert_eq!(alice_entry.1[0].to(), bob.id());
    assert!(alice_entry.1[0].notification);

    let bob_entry = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert!(bob_entry.1.is_empty());
}

#[tokio::test]
async fn test_preload_multi_pivot_collect_with_target() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let results = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);

    let alice_entry = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert_eq!(alice_entry.1[0].edge().from(), alice.id());
    assert_eq!(alice_entry.1[0].edge().to(), bob.id());
    assert!(alice_entry.1[0].edge().notification);
    assert_eq!(alice_entry.1[0].object().username, "bob");

    let bob_entry = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert!(bob_entry.1.is_empty());
}

#[tokio::test]
async fn test_preload_multi_pivot_count() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), charlie.id()),
            notification: true,
        })
        .await
        .unwrap();

    let counts: Vec<(User, u64)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .count()
        .await
        .unwrap();

    assert_eq!(counts.len(), 3);

    let alice_count = counts.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_count.1, 2);

    let bob_count = counts.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_count.1, 1);

    let charlie_count = counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_count.1, 0);
}

#[tokio::test]
async fn test_preload_multi_pivot_count_reverse() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@example.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: false,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), charlie.id()),
            notification: true,
        })
        .await
        .unwrap();

    let counts: Vec<(User, u64)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .count_reverse()
        .await
        .unwrap();

    assert_eq!(counts.len(), 3);

    let alice_count = counts.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_count.1, 0);

    let bob_count = counts.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_count.1, 1);

    let charlie_count = counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_count.1, 2);
}

#[tokio::test]
async fn test_preload_multi_pivot_owned() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut post1 = Post::default();
    post1.set_owner(alice.id());
    post1.title = "Alice Post 1".into();
    engine.create_object(&post1).await.unwrap();

    let mut post2 = Post::default();
    post2.set_owner(alice.id());
    post2.title = "Alice Post 2".into();
    engine.create_object(&post2).await.unwrap();

    let mut post3 = Post::default();
    post3.set_owner(bob.id());
    post3.title = "Bob Post".into();
    engine.create_object(&post3).await.unwrap();

    let results: Vec<(User, Vec<Post>)> = engine
        .preload_objects::<User>(Query::default())
        .preload::<Post>()
        .collect()
        .await
        .unwrap();

    assert_eq!(results.len(), 2);

    let alice_entry = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 2);
    let alice_post_titles: std::collections::HashSet<_> =
        alice_entry.1.iter().map(|p| p.title.as_str()).collect();
    assert!(alice_post_titles.contains("Alice Post 1"));
    assert!(alice_post_titles.contains("Alice Post 2"));

    let bob_entry = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].title, "Bob Post");
}

// ============================================================
// Engine — Bulk Delete & Utility Methods
// ============================================================

#[tokio::test]
async fn test_delete_bulk_objects() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut ids = Vec::new();
    for i in 0..5 {
        let mut user = User::default();
        user.username = format!("bulk{}", i);
        user.email = format!("bulk{}@example.com", i);
        ids.push(user.id());
        engine.create_object(&user).await.unwrap();
    }

    let count_before: u64 = engine.count_objects::<User>(None).await.unwrap();
    assert_eq!(count_before, 5);

    let deleted = engine
        .delete_objects::<User>(ids[..3].to_vec(), system_owner())
        .await
        .unwrap();
    assert_eq!(deleted, 3);

    let remaining: u64 = engine.count_objects::<User>(None).await.unwrap();
    assert_eq!(remaining, 2);
}

#[tokio::test]
async fn test_delete_owned_objects() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "owner".into();
    owner.email = "owner@example.com".into();
    engine.create_object(&owner).await.unwrap();

    for i in 0..4 {
        let mut post = Post::default();
        post.set_owner(owner.id());
        post.title = format!("Post {}", i);
        engine.create_object(&post).await.unwrap();
    }

    let count_before: u64 = engine
        .count_objects::<Post>(Some(Query::new(owner.id())))
        .await
        .unwrap();
    assert_eq!(count_before, 4);

    let deleted = engine
        .delete_owned_objects::<Post>(owner.id())
        .await
        .unwrap();
    assert_eq!(deleted, 4);

    let count_after: u64 = engine
        .count_objects::<Post>(Some(Query::new(owner.id())))
        .await
        .unwrap();
    assert_eq!(count_after, 0);
}

#[tokio::test]
async fn test_find_object_with_owner() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "finder".into();
    owner.email = "finder@example.com".into();
    engine.create_object(&owner).await.unwrap();

    let mut published = Post::default();
    published.set_owner(owner.id());
    published.title = "Published Post".into();
    published.status = PostStatus::Published;
    engine.create_object(&published).await.unwrap();

    let mut draft = Post::default();
    draft.set_owner(owner.id());
    draft.title = "Draft Post".into();
    engine.create_object(&draft).await.unwrap();

    let found: Option<Post> = engine
        .find_object_with_owner(
            owner.id(),
            &[filter!(&Post::FIELDS.status, PostStatus::Published)],
        )
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "Published Post");

    let other_owner_id = uuid::Uuid::now_v7();
    let missing: Option<Post> = engine
        .find_object_with_owner(
            other_owner_id,
            &[filter!(&Post::FIELDS.status, PostStatus::Published)],
        )
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_fetch_owned_object() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@example.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".into();
    engine.create_object(&post).await.unwrap();

    let found: Option<Post> = engine.fetch_owned_object(alice.id()).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "Alice's Post");

    let none: Option<Post> = engine.fetch_owned_object(bob.id()).await.unwrap();
    assert!(none.is_none());
}

#[tokio::test]
async fn test_default_field() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut post = Post::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".into();
    engine.create_object(&post).await.unwrap();

    let found: Option<Post> = engine.fetch_owned_object(alice.id()).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "Alice's Post");

    let found_new: Option<PostNew> = engine.fetch_owned_object(alice.id()).await.unwrap();
    assert!(found_new.is_some());
    assert_eq!(found_new.unwrap().rating, 10);
}

#[tokio::test]
async fn test_default_field_has_value() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@example.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut post = PostNew::default();
    post.set_owner(alice.id());
    post.title = "Alice's Post".into();
    // post.rating = 13;
    engine.create_object(&post).await.unwrap();

    let found: Option<PostNew> = engine.fetch_owned_object(alice.id()).await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().rating, 10);

    post.rating = 13;
    engine.update_object(&mut post).await.unwrap();

    let updated: Option<PostNew> = engine.fetch_owned_object(alice.id()).await.unwrap();
    assert!(updated.is_some());
    assert_eq!(updated.unwrap().rating, 13);
}

// ============================================================
// Geo — derive metadata, schema, write/update/delete, queries
// ============================================================

#[test]
fn test_geo_derive_metadata() {
    use ousia::query::IndexKind;

    let kinds = Place::FIELDS.location.kinds;
    assert_eq!(
        kinds.len(),
        1,
        "Place.location should have a single Geo kind"
    );
    match kinds[0] {
        IndexKind::Geo {
            lat_field,
            lon_field,
        } => {
            assert_eq!(lat_field, "lat");
            assert_eq!(lon_field, "lon");
        }
        other => panic!("expected IndexKind::Geo, got {:?}", other),
    }
    assert_eq!(Place::FIELDS.location.name, "location");
    assert!(Place::HAS_GEO_FIELDS);
    assert!(!User::HAS_GEO_FIELDS);

    // Multi-geo
    assert!(Delivery::HAS_GEO_FIELDS);
    assert_eq!(Delivery::FIELDS.pickup.name, "pickup");
    assert_eq!(Delivery::FIELDS.dropoff.name, "dropoff");
}

#[tokio::test]
async fn test_geo_schema_and_crud() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let postgis_enabled: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(postgis_enabled, "postgis extension missing");

    let has_hash: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'object_geo' AND column_name = 'hash')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_hash, "object_geo.hash column missing");

    let has_gist: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = 'idx_object_geo_gist')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(has_gist, "object_geo GIST index missing");

    let engine = Engine::new(Box::new(adapter));

    let mut place = Place::default();
    place.name = "Eiffel".into();
    place.lat = 48.8584;
    place.lon = 2.2945;
    engine.create_object(&place).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    let (lon_val, lat_val): (f64, f64) = sqlx::query_as(
        "SELECT ST_X(location::geometry), ST_Y(location::geometry) \
         FROM object_geo WHERE object_id = $1",
    )
    .bind(place.id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!((lon_val - 2.2945).abs() < 1e-6);
    assert!((lat_val - 48.8584).abs() < 1e-6);

    let _: Option<Place> = engine
        .delete_object(place.id(), system_owner())
        .await
        .unwrap();

    let count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count_after, 0, "delete_object should cascade to object_geo");
}

#[tokio::test]
async fn test_geo_update_diff() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut place = Place::default();
    place.name = "Original".into();
    place.lat = 10.0;
    place.lon = 20.0;
    engine.create_object(&place).await.unwrap();

    let hash_before: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();

    // Scalar-only mutation — geo row should not change.
    place.name = "Renamed".into();
    engine.update_object(&mut place).await.unwrap();

    let hash_after_scalar: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        hash_before, hash_after_scalar,
        "geo row hash changed on scalar-only update"
    );

    // Mutating lat changes the hash and the stored location.
    place.lat = 11.0;
    engine.update_object(&mut place).await.unwrap();

    let hash_after_geo: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(
        hash_before, hash_after_geo,
        "geo row hash did not change on lat update"
    );

    let lat_val: f64 =
        sqlx::query_scalar("SELECT ST_Y(location::geometry) FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!((lat_val - 11.0).abs() < 1e-6);
}

#[tokio::test]
async fn test_geo_query_within() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Seed places at known distances from (0, 0).
    let mut near = Place::default();
    near.name = "near".into();
    near.lat = 0.0;
    near.lon = 0.0;
    engine.create_object(&near).await.unwrap();

    let mut mid = Place::default();
    mid.name = "mid".into();
    mid.lat = 0.01; // ~1.11 km north
    mid.lon = 0.0;
    engine.create_object(&mid).await.unwrap();

    let mut far = Place::default();
    far.name = "far".into();
    far.lat = 1.0; // ~111 km north
    far.lon = 0.0;
    engine.create_object(&far).await.unwrap();

    // Within 5 km — should match near + mid only.
    let results: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            5_000.0,
        ))
        .await
        .unwrap();
    let names: std::collections::HashSet<&str> = results.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains("near"), "missing 'near': {:?}", names);
    assert!(names.contains("mid"), "missing 'mid': {:?}", names);
    assert!(
        !names.contains("far"),
        "'far' should be outside 5km: {:?}",
        names
    );

    // Count also flows through the geo path.
    let count = engine
        .count_objects::<Place>(Some(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            5_000.0,
        )))
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_geo_multi_field() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Lagos (3.3792, 6.5244) → Abuja (8.6753, 9.0820)
    let mut d = Delivery::default();
    d.pickup_lat = 6.5244;
    d.pickup_lon = 3.3792;
    d.dropoff_lat = 9.0820;
    d.dropoff_lon = 8.6753;
    engine.create_object(&d).await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(d.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "Delivery should write two object_geo rows");

    // 50 km around Lagos on the pickup field → hits.
    let near_lagos_pickup: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.pickup,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(near_lagos_pickup.len(), 1);

    // Same point but filtered on the dropoff field → miss (Abuja is hundreds of km away).
    let near_lagos_dropoff: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.dropoff,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(near_lagos_dropoff.len(), 0);

    // Abuja point on the dropoff field → hit.
    let near_abuja_dropoff: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.dropoff,
            8.6753,
            9.0820,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(near_abuja_dropoff.len(), 1);
}

// ──────────────────────────────────────────────────────────────────────────
// Geo helper extensions: bbox / order_by_distance / collect_with_distance /
// multiple geo filters.
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
fn make_place(name: &str, lat: f64, lon: f64) -> Place {
    let mut p = Place::default();
    p.name = name.to_string();
    p.lat = lat;
    p.lon = lon;
    p
}

/// Approximate great-circle distance in meters between two WGS84 points
/// (haversine). Used as an oracle for ordering / distance assertions.
#[cfg(test)]
fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let r = 6_371_008.8_f64; // WGS84 mean radius — matches PostGIS sphere closely enough
    let to_rad = std::f64::consts::PI / 180.0;
    let dphi = (lat2 - lat1) * to_rad;
    let dlam = (lon2 - lon1) * to_rad;
    let phi1 = lat1 * to_rad;
    let phi2 = lat2 * to_rad;
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

#[tokio::test]
async fn test_geo_bbox_basic() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // 3x3 grid spaced 1° apart, centered on (0,0). Only the center sits
    // strictly inside the small bbox (-0.5..0.5, -0.5..0.5).
    for (i, lat) in [-1.0, 0.0, 1.0].iter().enumerate() {
        for (j, lon) in [-1.0, 0.0, 1.0].iter().enumerate() {
            engine
                .create_object(&make_place(&format!("p{}{}", i, j), *lat, *lon))
                .await
                .unwrap();
        }
    }

    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_in_bbox(
            &Place::FIELDS.location,
            -0.5,
            -0.5,
            0.5,
            0.5,
        ))
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].lat, 0.0);
    assert_eq!(hits[0].lon, 0.0);

    // Tiny bbox at a corner of the grid: should pick up only the single corner point.
    let corner: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_in_bbox(
            &Place::FIELDS.location,
            -1.5,
            -1.5,
            -0.5,
            -0.5,
        ))
        .await
        .unwrap();
    assert_eq!(corner.len(), 1);
    assert_eq!(corner[0].lat, -1.0);
    assert_eq!(corner[0].lon, -1.0);
}

#[tokio::test]
async fn test_geo_order_by_distance_asc_and_desc() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // 5 places with increasing distance from (0,0).
    let pts: [(&str, f64, f64); 5] = [
        ("a", 0.01, 0.01),
        ("b", 0.05, 0.05),
        ("c", 0.10, 0.10),
        ("d", 0.50, 0.50),
        ("e", 1.00, 1.00),
    ];
    for (n, lat, lon) in pts.iter() {
        engine.create_object(&make_place(n, *lat, *lon)).await.unwrap();
    }

    let asc: Vec<Place> = engine
        .query_objects::<Place>(Query::default().order_by_distance(
            &Place::FIELDS.location,
            0.0,
            0.0,
            true,
        ))
        .await
        .unwrap();
    let asc_names: Vec<&str> = asc.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(asc_names, vec!["a", "b", "c", "d", "e"]);

    let desc_limit2: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                .order_by_distance(&Place::FIELDS.location, 0.0, 0.0, false)
                .with_limit(2),
        )
        .await
        .unwrap();
    let desc_names: Vec<&str> = desc_limit2.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(desc_names, vec!["e", "d"]);
}

#[tokio::test]
async fn test_geo_collect_with_distance() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let pts: [(&str, f64, f64); 4] = [
        ("near", 0.001, 0.001),
        ("mid", 0.05, 0.05),
        ("far", 0.2, 0.2),
        ("farther", 1.0, 1.0),
    ];
    for (n, lat, lon) in pts.iter() {
        engine.create_object(&make_place(n, *lat, *lon)).await.unwrap();
    }

    let results: Vec<(Place, f64)> = engine
        .query_objects_with_distance::<Place>(Query::default().order_by_distance(
            &Place::FIELDS.location,
            0.0,
            0.0,
            true,
        ))
        .await
        .unwrap();

    assert_eq!(results.len(), 4);
    // Distances non-decreasing.
    for w in results.windows(2) {
        assert!(w[0].1 <= w[1].1, "distances must be non-decreasing");
    }
    // Each distance must be within 1% of the haversine oracle (allows for
    // PostGIS geodesic vs. spherical haversine drift).
    for (p, d) in &results {
        let oracle = haversine_m(0.0, 0.0, p.lon, p.lat);
        let rel = (d - oracle).abs() / oracle.max(1.0);
        assert!(rel < 0.01, "distance {} too far from oracle {}", d, oracle);
    }
}

#[tokio::test]
async fn test_geo_collect_with_distance_requires_order() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let err = engine
        .query_objects_with_distance::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            10_000.0,
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidQuery(_)),
        "expected InvalidQuery, got {:?}",
        err,
    );
}

#[tokio::test]
async fn test_geo_multiple_within_filters_and() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Deliveries with various pickup/dropoff combinations relative to two anchor points.
    // Anchor A = (0, 0); Anchor B = (1, 1). Only `match_both` should fall inside
    // both 50km radii.
    let mut both = Delivery::default();
    both.pickup_lat = 0.001;
    both.pickup_lon = 0.001;
    both.dropoff_lat = 1.001;
    both.dropoff_lon = 1.001;
    engine.create_object(&both).await.unwrap();

    let mut pickup_only = Delivery::default();
    pickup_only.pickup_lat = 0.001;
    pickup_only.pickup_lon = 0.001;
    pickup_only.dropoff_lat = 5.0;
    pickup_only.dropoff_lon = 5.0;
    engine.create_object(&pickup_only).await.unwrap();

    let mut dropoff_only = Delivery::default();
    dropoff_only.pickup_lat = 5.0;
    dropoff_only.pickup_lon = 5.0;
    dropoff_only.dropoff_lat = 1.001;
    dropoff_only.dropoff_lon = 1.001;
    engine.create_object(&dropoff_only).await.unwrap();

    let hits: Vec<Delivery> = engine
        .query_objects::<Delivery>(
            Query::default()
                .where_geo_within(&Delivery::FIELDS.pickup, 0.0, 0.0, 50_000.0)
                .where_geo_within(&Delivery::FIELDS.dropoff, 1.0, 1.0, 50_000.0),
        )
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id(), both.id());
}

#[tokio::test]
async fn test_geo_mixed_within_and_bbox_same_field() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Two points: one inside both the radius AND the bbox; one inside only the radius.
    engine.create_object(&make_place("in_both", 0.001, 0.001)).await.unwrap();
    engine.create_object(&make_place("only_radius", 0.4, 0.0)).await.unwrap();
    engine.create_object(&make_place("outside", 5.0, 5.0)).await.unwrap();

    let hits: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                // Radius ~50km — includes in_both and only_radius.
                .where_geo_within(&Place::FIELDS.location, 0.0, 0.0, 50_000.0)
                // Tiny bbox — includes only in_both.
                .where_geo_in_bbox(&Place::FIELDS.location, -0.1, -0.1, 0.1, 0.1),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].name, "in_both");
}

#[tokio::test]
async fn test_geo_order_on_different_field_than_filter() {
    let (_resource, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // 3 deliveries with different pickup distances from (0,0) but
    // dropoffs ordered differently relative to (10, 10).
    let mut d1 = Delivery::default();
    d1.pickup_lat = 0.01;
    d1.pickup_lon = 0.01; // pickup close to (0,0)
    d1.dropoff_lat = 9.0;
    d1.dropoff_lon = 9.0; // dropoff ~157km from (10,10)
    engine.create_object(&d1).await.unwrap();

    let mut d2 = Delivery::default();
    d2.pickup_lat = 0.05;
    d2.pickup_lon = 0.05; // pickup farther
    d2.dropoff_lat = 9.9;
    d2.dropoff_lon = 9.9; // dropoff closest to (10,10)
    engine.create_object(&d2).await.unwrap();

    let mut d3 = Delivery::default();
    d3.pickup_lat = 0.1;
    d3.pickup_lon = 0.1; // pickup farthest
    d3.dropoff_lat = 9.5;
    d3.dropoff_lon = 9.5; // dropoff middle
    engine.create_object(&d3).await.unwrap();

    // Filter on pickup, order by dropoff distance to (10,10).
    let results: Vec<Delivery> = engine
        .query_objects::<Delivery>(
            Query::default()
                .where_geo_within(&Delivery::FIELDS.pickup, 0.0, 0.0, 50_000.0)
                .order_by_distance(&Delivery::FIELDS.dropoff, 10.0, 10.0, true),
        )
        .await
        .unwrap();

    // All three pickups fall inside 50km — but order is by dropoff distance to (10,10):
    // d2 (~15km) < d3 (~78km) < d1 (~157km).
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id(), d2.id());
    assert_eq!(results[1].id(), d3.id());
    assert_eq!(results[2].id(), d1.id());
}

