#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use super::*;
#[cfg(test)]
use ousia::{
    EdgeMeta, EdgeMetaTrait, EdgeQuery, Engine, Error, Object, ObjectMeta, ObjectOwnership, Query,
    Union,
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

    let postgres = Postgres::default()
        .with_password("postgres")
        .with_user("postgres")
        .with_db_name("postgres")
        .with_name("imresamu/postgis")
        .with_tag("16-3.6-alpine")
        .start()
        .await
        .expect("Failed to start Postgres");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to Postgres");

    (postgres, pool)
}

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

// ============================================================
// Section 2: Object Queries — Filter Variants
// ============================================================

#[tokio::test]
async fn test_query_string_filters() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let owner = {
        let mut u = User::default();
        u.username = "qowner".into();
        u.email = "qowner@example.com".into();
        engine.create_object(&u).await.unwrap();
        u
    };

    for (title, status) in [
        ("Alpha post", PostStatus::Draft),
        ("Beta showcase", PostStatus::Published),
        ("Gamma post review", PostStatus::Draft),
        ("Delta summary", PostStatus::Archived),
    ] {
        let mut p = Post::default();
        p.set_owner(owner.id());
        p.title = title.into();
        p.status = status;
        engine.create_object(&p).await.unwrap();
    }

    // where_eq
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_eq(&Post::FIELDS.title, "Alpha post"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "where_eq");

    // where_ne
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_ne(&Post::FIELDS.title, "Alpha post"))
        .await
        .unwrap();
    assert_eq!(r.len(), 3, "where_ne");

    // where_begins_with
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_begins_with(&Post::FIELDS.title, "Alpha"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "where_begins_with");

    // where_contains — "post" in "Alpha post" and "Gamma post review"
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_contains(&Post::FIELDS.title, "post"))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "where_contains");

    // where_not_contains
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_not_contains(&Post::FIELDS.title, "post"))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "where_not_contains");

    // Enum equality / inequality
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_eq(&Post::FIELDS.status, PostStatus::Published))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "where_eq enum");

    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_ne(&Post::FIELDS.status, PostStatus::Draft))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "where_ne enum");
}

#[tokio::test]
async fn test_query_numeric_filters() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (username, email, balance) in [
        ("alice", "a@x.com", 50i64),
        ("bob", "b@x.com", 100),
        ("charlie", "c@x.com", 150),
    ] {
        let mut u = User::default();
        u.username = username.into();
        u.email = email.into();
        u.balance = Wallet { inner: balance };
        engine.create_object(&u).await.unwrap();
    }

    let gt: Vec<User> = engine
        .query_objects(Query::default().where_gt(&User::FIELDS.balance, Wallet { inner: 50 }))
        .await
        .unwrap();
    assert_eq!(gt.len(), 2, "where_gt");

    let gte: Vec<User> = engine
        .query_objects(Query::default().where_gte(&User::FIELDS.balance, Wallet { inner: 100 }))
        .await
        .unwrap();
    assert_eq!(gte.len(), 2, "where_gte");

    let lt: Vec<User> = engine
        .query_objects(Query::default().where_lt(&User::FIELDS.balance, Wallet { inner: 150 }))
        .await
        .unwrap();
    assert_eq!(lt.len(), 2, "where_lt");

    let lte: Vec<User> = engine
        .query_objects(Query::default().where_lte(&User::FIELDS.balance, Wallet { inner: 100 }))
        .await
        .unwrap();
    assert_eq!(lte.len(), 2, "where_lte");
}

#[tokio::test]
async fn test_query_array_filters() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "tagger".into();
    owner.email = "tagger@example.com".into();
    engine.create_object(&owner).await.unwrap();

    for (title, tags) in [
        ("A", vec!["rust", "orm"]),
        ("B", vec!["rust", "async"]),
        ("C", vec!["async", "testing"]),
        ("D", vec!["testing"]),
    ] {
        let mut p = Post::default();
        p.set_owner(owner.id());
        p.title = title.into();
        p.tags = tags.into_iter().map(String::from).collect();
        engine.create_object(&p).await.unwrap();
    }

    // contains any of [rust]
    let r: Vec<Post> = engine
        .query_objects(Query::new(owner.id()).where_contains(&Post::FIELDS.tags, vec!["rust"]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "contains [rust]");

    // contains_all [rust, async] — only B
    let r: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id()).where_contains_all(&Post::FIELDS.tags, vec!["rust", "async"]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "contains_all [rust,async]");

    // not_contains [testing] — A and B
    let r: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id()).where_not_contains(&Post::FIELDS.tags, vec!["testing"]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "not_contains [testing]");
}

#[tokio::test]
async fn test_query_or_filters() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "orowner".into();
    owner.email = "orowner@example.com".into();
    engine.create_object(&owner).await.unwrap();

    for title in ["Alpha", "Beta", "Gamma", "Delta"] {
        let mut p = Post::default();
        p.set_owner(owner.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }

    // or_eq: title == "Alpha" OR title == "Delta" → 2
    let r: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id())
                .where_eq(&Post::FIELDS.title, "Alpha")
                .or_eq(&Post::FIELDS.title, "Delta"),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "or_eq");

    // or_begins_with: title starts with "Al" OR "Be" → 2
    let r: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id())
                .where_begins_with(&Post::FIELDS.title, "Al")
                .or_begins_with(&Post::FIELDS.title, "Be"),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "or_begins_with");

    // or_contains: "pha" OR "elt" → Alpha + Delta = 2
    let r: Vec<Post> = engine
        .query_objects(
            Query::new(owner.id())
                .where_contains(&Post::FIELDS.title, "pha")
                .or_contains(&Post::FIELDS.title, "elt"),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "or_contains");
}

#[tokio::test]
async fn test_query_sort_and_pagination() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut created_ids = Vec::new();
    for name in ["alpha", "beta", "charlie", "delta", "echo"] {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut u = User::default();
        u.username = name.into();
        u.email = format!("{}@x.com", name);
        engine.create_object(&u).await.unwrap();
        created_ids.push(u.id());
    }

    // sort_desc by username → z-first
    let r: Vec<User> = engine
        .query_objects(Query::default().sort_desc(&User::FIELDS.username))
        .await
        .unwrap();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].username, "echo");
    assert_eq!(r[4].username, "alpha");

    // sort_asc by username → a-first
    let r: Vec<User> = engine
        .query_objects(Query::default().sort_asc(&User::FIELDS.username))
        .await
        .unwrap();
    assert_eq!(r[0].username, "alpha");

    // sort_asc by created_at — insertion order
    let r: Vec<User> = engine
        .query_objects(Query::default().sort_asc(&User::FIELDS.created_at))
        .await
        .unwrap();
    assert_eq!(r[0].username, "alpha");
    assert_eq!(r[4].username, "echo");

    // with_limit
    let r: Vec<User> = engine
        .query_objects(
            Query::default()
                .sort_asc(&User::FIELDS.username)
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r[2].username, "charlie");

    // cursor pagination: cursor at charlie → older rows (alpha, beta)
    let cursor_id = created_ids[2]; // charlie
    let r: Vec<User> = engine
        .query_objects(Query::default().with_cursor(cursor_id))
        .await
        .unwrap();
    assert_eq!(r.len(), 2);
}

#[tokio::test]
async fn test_query_wide() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // System-owned
    let mut u1 = User::default();
    u1.username = "sys1".into();
    u1.email = "sys1@x.com".into();
    engine.create_object(&u1).await.unwrap();

    // User-owned post
    let mut post = Post::default();
    post.set_owner(u1.id());
    post.title = "wide-test".into();
    engine.create_object(&post).await.unwrap();

    // Query::wide() should cross owner boundaries for Post
    let r: Vec<Post> = engine
        .query_objects(Query::wide().where_eq(&Post::FIELDS.title, "wide-test"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
}

#[tokio::test]
async fn test_find_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut u = User::default();
    u.username = "finder".into();
    u.email = "finder@example.com".into();
    u.balance = Wallet { inner: 999 };
    engine.create_object(&u).await.unwrap();

    let found: Option<User> = engine
        .find_object(&[filter!(&User::FIELDS.balance, 999)])
        .await
        .unwrap();
    assert!(found.is_some());

    let missing: Option<User> = engine
        .find_object(&[filter!(&User::FIELDS.balance, 0)])
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_find_object_with_owner() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner = User::default();
    owner.username = "po".into();
    owner.email = "po@x.com".into();
    engine.create_object(&owner).await.unwrap();

    let mut published = Post::default();
    published.set_owner(owner.id());
    published.status = PostStatus::Published;
    published.title = "Pub".into();
    engine.create_object(&published).await.unwrap();

    let found: Option<Post> = engine
        .find_object_with_owner(
            owner.id(),
            &[filter!(&Post::FIELDS.status, PostStatus::Published)],
        )
        .await
        .unwrap();
    assert!(found.is_some());

    // Wrong owner → None
    let missing: Option<Post> = engine
        .find_object_with_owner(
            uuid::Uuid::now_v7(),
            &[filter!(&Post::FIELDS.status, PostStatus::Published)],
        )
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_count_objects() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for i in 0..5 {
        let mut u = User::default();
        u.username = format!("cu{}", i);
        u.email = format!("cu{}@x.com", i);
        engine.create_object(&u).await.unwrap();
    }

    // All
    assert_eq!(engine.count_objects::<User>(None).await.unwrap(), 5);

    // Filtered
    let count = engine
        .count_objects::<User>(Some(
            Query::default().where_eq(&User::FIELDS.username, "cu0"),
        ))
        .await
        .unwrap();
    assert_eq!(count, 1);

    // No match
    let count = engine
        .count_objects::<User>(Some(
            Query::default().where_eq(&User::FIELDS.username, "ghost"),
        ))
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_count_objects_wide_query_spans_all_owners() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner_a = User::default();
    owner_a.username = "wide-owner-a".into();
    owner_a.email = "wide-a@x.com".into();
    engine.create_object(&owner_a).await.unwrap();

    let mut owner_b = User::default();
    owner_b.username = "wide-owner-b".into();
    owner_b.email = "wide-b@x.com".into();
    engine.create_object(&owner_b).await.unwrap();

    for (owner, n) in [(owner_a.id(), 2), (owner_b.id(), 3)] {
        for i in 0..n {
            let mut p = Post::default();
            p.set_owner(owner);
            p.title = format!("wide post {}", i);
            p.status = PostStatus::Published;
            engine.create_object(&p).await.unwrap();
        }
    }

    let count = engine
        .count_objects::<Post>(Some(
            Query::wide().where_eq(&Post::FIELDS.status, PostStatus::Published),
        ))
        .await
        .unwrap();
    assert_eq!(
        count, 5,
        "wide count must span every owner, not just match a nil owner"
    );
}

#[tokio::test]
async fn test_query_all_index_value_variants() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let uid_a = uuid::Uuid::now_v7();
    let uid_b = uuid::Uuid::now_v7();
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let t1 = t0 + chrono::Duration::days(1);
    let t2 = t0 + chrono::Duration::days(2);

    for (name, count, price, active, uid, ts, tags, scores) in [
        (
            "alpha",
            1i64,
            1.5f64,
            true,
            uid_a,
            t0,
            vec!["red", "blue"],
            vec![10i64, 20],
        ),
        (
            "bravo",
            2,
            2.5,
            false,
            uid_b,
            t1,
            vec!["green"],
            vec![20, 30],
        ),
        (
            "charlie",
            3,
            3.5,
            true,
            uid_a,
            t2,
            vec!["blue", "green"],
            vec![10, 30],
        ),
    ] {
        let mut v = Variants::default();
        v.name = name.into();
        v.count = count;
        v.price = price;
        v.active = active;
        v.uid = uid;
        v.occurred_at = EventTime(ts);
        v.tags = tags.into_iter().map(String::from).collect();
        v.scores = scores;
        engine.create_object(&v).await.unwrap();
    }

    // String (IndexValue::String)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.name, "alpha"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.name, "alpha"))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "String where_ne");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_begins_with(&Variants::FIELDS.name, "br"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_begins_with");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.name, "ar"))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "String where_contains 'ar' → 'charlie' only");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_not_contains(&Variants::FIELDS.name, "a"))
        .await
        .unwrap();
    assert_eq!(r.len(), 0, "String where_not_contains 'a' → none");

    // Int (IndexValue::Int)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Int where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_ne");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.count, 1i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gte(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_gte");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lt(&Variants::FIELDS.count, 3i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_lt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.count, 2i64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Int where_lte");

    // Float (IndexValue::Float) — values chosen so equality is exact in IEEE 754
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.price, 1.5f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Float where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.price, 2.0f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Float where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.price, 2.5f64))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Float where_lte");

    // Bool (IndexValue::Bool)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.active, true))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Bool where_eq true");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.active, true))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Bool where_ne true");

    // Uuid (IndexValue::Uuid)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.uid, uid_a))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Uuid where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.uid, uid_a))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Uuid where_ne");

    // Timestamp (IndexValue::Timestamp)
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.occurred_at, EventTime(t0)))
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Timestamp where_eq");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_gt(&Variants::FIELDS.occurred_at, EventTime(t0)))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Timestamp where_gt");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_lte(&Variants::FIELDS.occurred_at, EventTime(t1)))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Timestamp where_lte");

    // Array of String (IndexValue::Array(Vec<IndexValueInner::String>))
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.tags, vec!["blue"]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<String> where_contains [blue]");
    let r: Vec<Variants> = engine
        .query_objects(
            Query::default().where_contains_all(&Variants::FIELDS.tags, vec!["blue", "green"]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Array<String> where_contains_all [blue,green]");
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_not_contains(&Variants::FIELDS.tags, vec!["red"]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<String> where_not_contains [red]");

    // Array of Int (IndexValue::Array(Vec<IndexValueInner::Int>))
    let r: Vec<Variants> = engine
        .query_objects(Query::default().where_contains(&Variants::FIELDS.scores, vec![10i64]))
        .await
        .unwrap();
    assert_eq!(r.len(), 2, "Array<Int> where_contains [10]");
    let r: Vec<Variants> = engine
        .query_objects(
            Query::default().where_contains_all(&Variants::FIELDS.scores, vec![10i64, 30]),
        )
        .await
        .unwrap();
    assert_eq!(r.len(), 1, "Array<Int> where_contains_all [10,30]");
}

/// Regression: `where_eq(field, false)` and `where_ne(field, true)` must
/// return the same set for boolean fields. Before the fix, a row whose
/// `index_meta` was missing the field entirely (e.g. legacy data written
/// before the index was declared) satisfied `NOT @>` but not `@>`,
/// producing asymmetric results.
#[tokio::test]
async fn test_query_ne_requires_key_existence() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut explicit_false = Variants::default();
    explicit_false.name = "explicit-false".into();
    explicit_false.active = false;
    engine.create_object(&explicit_false).await.unwrap();

    let mut explicit_true = Variants::default();
    explicit_true.name = "explicit-true".into();
    explicit_true.active = true;
    engine.create_object(&explicit_true).await.unwrap();

    // Inject a legacy row whose index_meta is missing `active` entirely —
    // simulates data written before the `active` index existed.
    let legacy_id = uuid::Uuid::now_v7();
    let now = chrono::Utc::now();
    sqlx::query(
        r#"INSERT INTO public.objects (id, type, owner, created_at, updated_at, data, index_meta)
           VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
    )
    .bind(legacy_id)
    .bind(Variants::TYPE)
    .bind(system_owner())
    .bind(now)
    .bind(now)
    // v2: `data` is BYTEA / msgpack. Encode the legacy payload as a
    // msgpack map matching the struct's field order.
    .bind({
        use rmp_serde::Serializer;
        use serde::Serialize as _;
        let mut buf = Vec::new();
        let mut ser = Serializer::new(&mut buf).with_struct_map();
        let payload = serde_json::json!({
            "id": legacy_id,
            "owner": system_owner(),
            "created_at": now,
            "updated_at": now,
            "name": "legacy",
            "count": 0,
            "price": 0.0,
            "active": false,
            "uid": uuid::Uuid::nil(),
            "occurred_at": chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap(),
            "tags": [],
            "scores": []
        });
        payload.serialize(&mut ser).unwrap();
        buf
    })
    .bind(serde_json::json!({"name": "legacy"}))
    .execute(&pool)
    .await
    .unwrap();

    let eq_false: Vec<Variants> = engine
        .query_objects(Query::default().where_eq(&Variants::FIELDS.active, false))
        .await
        .unwrap();
    let ne_true: Vec<Variants> = engine
        .query_objects(Query::default().where_ne(&Variants::FIELDS.active, true))
        .await
        .unwrap();

    assert_eq!(
        eq_false.len(),
        ne_true.len(),
        "where_eq(false) and where_ne(true) must agree on the same data"
    );
    assert_eq!(
        eq_false.len(),
        1,
        "only the explicit `active=false` row should match"
    );
    assert_eq!(eq_false[0].id(), explicit_false.id());
}

// ── Negated comparisons & random sort ───────────────────────────────────────

#[cfg(test)]
async fn seed_variants(engine: &Engine, rows: &[(&str, i64, &[&str])]) {
    for (name, count, tags) in rows {
        let mut v = Variants::default();
        v.name = (*name).into();
        v.count = *count;
        v.tags = tags.iter().map(|t| t.to_string()).collect();
        engine.create_object(&v).await.unwrap();
    }
}

#[cfg(test)]
fn sorted_names(rows: &[Variants]) -> Vec<String> {
    let mut names: Vec<String> = rows.iter().map(|v| v.name.clone()).collect();
    names.sort();
    names
}

#[tokio::test]
async fn test_query_not_begins_with() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("rust-a", 1, &[]), ("rust-b", 2, &[]), ("go-c", 3, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_begins_with(&Variants::FIELDS.name, "rust"))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["go-c"]);
}

#[tokio::test]
async fn test_query_not_in_strings_and_ints() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(&engine, &[("a", 1, &[]), ("b", 2, &[]), ("c", 3, &[]), ("d", 4, &[])]).await;

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.name, vec!["a", "b"]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["c", "d"]);

    let rows: Vec<Variants> = engine
        .query_objects(Query::default().where_not_in(&Variants::FIELDS.count, vec![1i64, 4]))
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b", "c"]);

    // AND-ed with a range filter: count >= 2 AND name NOT IN [c]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_gte(&Variants::FIELDS.count, 2i64)
                .where_not_in(&Variants::FIELDS.name, vec!["c"]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["b", "d"]);

    // OR form: name = a OR count NOT IN [1, 2, 3]
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_eq(&Variants::FIELDS.name, "a")
                .or_not_in(&Variants::FIELDS.count, vec![1i64, 2, 3]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["a", "d"]);
}

#[tokio::test]
async fn test_query_not_contains_all() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    seed_variants(
        &engine,
        &[("xy", 1, &["x", "y"]), ("xyz", 2, &["x", "y", "z"]), ("x", 3, &["x"]), ("yz", 4, &["y", "z"])],
    )
    .await;

    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default().where_not_contains_all(&Variants::FIELDS.tags, vec!["x", "y"]),
        )
        .await
        .unwrap();
    assert_eq!(sorted_names(&rows), ["x", "yz"]);
}

#[tokio::test]
async fn test_query_sort_random() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let names: Vec<String> = (0..20).map(|i| format!("v{i:02}")).collect();
    let rows: Vec<(&str, i64, &[&str])> =
        names.iter().enumerate().map(|(i, n)| (n.as_str(), i as i64, &[][..])).collect();
    seed_variants(&engine, &rows).await;

    let mut orders = std::collections::HashSet::new();
    for _ in 0..5 {
        let rows: Vec<Variants> = engine
            .query_objects(Query::default().sort_random())
            .await
            .unwrap();
        assert_eq!(rows.len(), 20);
        orders.insert(rows.iter().map(|v| v.name.clone()).collect::<Vec<_>>());
    }
    assert!(orders.len() > 1, "5 random orderings of 20 rows were identical");

    // composes with filters and limit
    let rows: Vec<Variants> = engine
        .query_objects(
            Query::default()
                .where_lt(&Variants::FIELDS.count, 10i64)
                .sort_random()
                .with_limit(3),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|v| v.count < 10));
}

#[tokio::test]
async fn test_edge_negated_filters_and_random_sort() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hub = Hub::default();
    hub.name = "hub".into();
    engine.create_object(&hub).await.unwrap();
    for (position, name) in [(1i64, "s-a"), (2, "s-b"), (3, "t-c"), (4, "t-d")] {
        let mut spoke = Spoke::default();
        spoke.name = name.into();
        engine.create_object(&spoke).await.unwrap();
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub.id(), spoke.id()),
                position,
            })
            .await
            .unwrap();
    }
    let positions = |edges: &[HubSpoke]| {
        let mut p: Vec<i64> = edges.iter().map(|e| e.position).collect();
        p.sort();
        p
    };

    // EdgeQuery
    let edges: Vec<HubSpoke> = engine
        .query_edges(
            hub.id(),
            EdgeQuery::default().where_not_in(&HubSpoke::FIELDS.position, vec![1i64, 2]),
        )
        .await
        .unwrap();
    assert_eq!(positions(&edges), [3, 4]);

    let edges: Vec<HubSpoke> = engine
        .query_edges(hub.id(), EdgeQuery::default().sort_random())
        .await
        .unwrap();
    assert_eq!(positions(&edges), [1, 2, 3, 4]);

    // EdgeQueryContext: edge-side and target-side negations together
    let spokes: Vec<Spoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_not_in(&HubSpoke::FIELDS.position, vec![1i64])
        .where_not_begins_with(&Spoke::FIELDS.name, "t-")
        .collect()
        .await
        .unwrap();
    assert_eq!(spokes.len(), 1);
    assert_eq!(spokes[0].name, "s-b");

    let edges: Vec<HubSpoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .edge_sort_random()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(positions(&edges), [1, 2, 3, 4]);

    let spokes: Vec<Spoke> = engine
        .preload_object::<Hub>(hub.id())
        .edge::<HubSpoke, Spoke>()
        .sort_random()
        .collect()
        .await
        .unwrap();
    assert_eq!(spokes.len(), 4);
}

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

#[cfg(test)]
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

#[tokio::test]
async fn test_engine_fetch_owned_union_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let owner = uuid::Uuid::now_v7();

    // Nothing owned yet — the singular O2O union fetch must return None,
    // not an error, and not silently pick up an unrelated owner's row.
    let none: Option<Union<User, Post>> = engine
        .fetch_owned_union_object::<User, Post>(owner)
        .await
        .unwrap();
    assert!(none.is_none());

    let mut post = Post::default();
    post.set_owner(owner);
    post.title = "Owned singular".into();
    engine.create_object(&post).await.unwrap();

    let found: Union<User, Post> = engine
        .fetch_owned_union_object::<User, Post>(owner)
        .await
        .unwrap()
        .unwrap();
    assert!(found.is_second());
}

// ============================================================
// Section 5: Edge CRUD
// ============================================================

#[tokio::test]
async fn test_insert_and_fetch_edge() {
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

    let follow = Follow {
        _meta: EdgeMeta::new(alice.id(), bob.id()),
        notification: true,
    };
    engine.create_edge(&follow).await.unwrap();

    let fetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();
    assert!(fetched.is_some());
    assert!(fetched.unwrap().notification);
}

#[tokio::test]
async fn test_fetch_missing_edge_returns_none() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let result = engine
        .fetch_edge::<Follow>(uuid::Uuid::now_v7(), uuid::Uuid::now_v7())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_insert_edge_upsert_updates_data() {
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

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    // Re-insert with notification=true — upsert should update.
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let fetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    assert!(fetched.notification, "upsert should update edge data");
}

#[tokio::test]
async fn test_delete_edge() {
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

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    engine
        .delete_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();

    let gone: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert!(gone.is_empty());
}

#[tokio::test]
async fn test_update_edge_data_only() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "ue_alice".into();
    alice.email = "ue_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "ue_bob".into();
    bob.email = "ue_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: false,
        })
        .await
        .unwrap();

    let mut edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    edge.notification = true;
    engine.update_edge(&mut edge, None).await.unwrap();

    let refetched = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    assert!(
        refetched.notification,
        "update_edge should persist data changes"
    );
    assert_eq!(
        refetched.to(),
        bob.id(),
        "no retarget requested — `to` unchanged"
    );
}

#[tokio::test]
async fn test_update_edge_retargets_to() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "uer_alice".into();
    alice.email = "uer_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "uer_bob".into();
    bob.email = "uer_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "uer_charlie".into();
    charlie.email = "uer_charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let mut edge = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap()
        .unwrap();
    engine
        .update_edge(&mut edge, Some(charlie.id()))
        .await
        .unwrap();

    // Old (from, bob) key is gone — retargeting moves the edge, doesn't duplicate it.
    let old = engine
        .fetch_edge::<Follow>(alice.id(), bob.id())
        .await
        .unwrap();
    assert!(old.is_none());

    let retargeted = engine
        .fetch_edge::<Follow>(alice.id(), charlie.id())
        .await
        .unwrap();
    assert!(retargeted.is_some());
    assert!(retargeted.unwrap().notification);
}

#[tokio::test]
async fn test_delete_object_edge_removes_all_forward_edges() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "doe_alice".into();
    alice.email = "doe_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut targets = Vec::new();
    for name in ["doe_t1", "doe_t2", "doe_t3"] {
        let mut u = User::default();
        u.username = name.into();
        u.email = format!("{name}@x.com");
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
    }

    for &to in &targets {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(alice.id(), to),
                notification: true,
            })
            .await
            .unwrap();
    }

    // A second user's edges must survive — this only touches `alice`'s.
    let mut dave = User::default();
    dave.username = "doe_dave".into();
    dave.email = "doe_dave@x.com".into();
    engine.create_object(&dave).await.unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(dave.id(), targets[0]),
            notification: true,
        })
        .await
        .unwrap();

    engine
        .delete_object_edge::<Follow>(alice.id())
        .await
        .unwrap();

    let alice_edges: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert!(alice_edges.is_empty());

    let dave_edges: Vec<Follow> = engine
        .query_edges(dave.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(
        dave_edges.len(),
        1,
        "delete_object_edge must not touch other owners' edges"
    );
}

// ============================================================
// Section 6: Edge Timestamps
// ============================================================

#[tokio::test]
async fn test_edge_timestamps_populated_on_create() {
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
        .unwrap()
        .unwrap();

    // Timestamps must be non-zero and close to now.
    let now = chrono::Utc::now();
    let diff_created = (now - edge.created_at()).num_seconds().abs();
    let diff_updated = (now - edge.updated_at()).num_seconds().abs();
    assert!(diff_created < 60, "created_at should be recent");
    assert!(diff_updated < 60, "updated_at should be recent");
    assert_eq!(
        edge.created_at(),
        edge.updated_at(),
        "on first insert created_at == updated_at"
    );
}

#[tokio::test]
async fn test_edge_sort_desc_by_created_at() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut u = User::default();
        u.username = format!("t{}", i);
        u.email = format!("t{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: true,
            })
            .await
            .unwrap();
        targets.push(u);
    }

    // Sort desc by edge created_at → newest edge first (t2 was created last).
    let edges: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_sort_desc(&Follow::FIELDS.created_at)
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(edges.len(), 3);
    assert_eq!(
        edges[0].to(),
        targets[2].id(),
        "newest edge should be first"
    );
    assert_eq!(edges[2].to(), targets[0].id(), "oldest edge should be last");
}

#[tokio::test]
async fn test_edge_sort_asc_by_created_at() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for i in 0..3 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut u = User::default();
        u.username = format!("asc{}", i);
        u.email = format!("asc{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: false,
            })
            .await
            .unwrap();
        targets.push(u);
    }

    let edges: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_sort_asc(&Follow::FIELDS.created_at)
        .collect_edges()
        .await
        .unwrap();

    assert_eq!(edges.len(), 3);
    assert_eq!(
        edges[0].to(),
        targets[0].id(),
        "oldest edge should be first"
    );
    assert_eq!(edges[2].to(), targets[2].id(), "newest edge should be last");
}

// ============================================================
// Section 7: Edge Queries & Filters
// ============================================================

#[tokio::test]
async fn test_query_edges_forward_and_reverse() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@x.com".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@x.com".into();
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

    // Forward: alice → [bob]
    let fwd: Vec<Follow> = engine
        .query_edges(alice.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(fwd.len(), 1);
    assert_eq!(fwd[0].to(), bob.id());

    // Forward: bob → []
    let fwd: Vec<Follow> = engine
        .query_edges(bob.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(fwd.len(), 0);

    // Reverse: bob ← [alice, michael]
    let rev: Vec<Follow> = engine
        .query_reverse_edges(bob.id(), EdgeQuery::default())
        .await
        .unwrap();
    assert_eq!(rev.len(), 2);

    // count_edges / count_reverse_edges
    assert_eq!(
        engine.count_edges::<Follow>(bob.id(), None).await.unwrap(),
        0
    );
    assert_eq!(
        engine
            .count_reverse_edges::<Follow>(bob.id(), None)
            .await
            .unwrap(),
        2
    );
}

/// Regression test for a bug where `EdgeQuery::with_cursor` filtered on the raw
/// `from`/`to` column with no `ORDER BY` tying the two together, so pagination
/// order was whatever Postgres felt like returning — which, for a freshly
/// inserted small table, ordinarily lines up with insertion (`created_at`)
/// order by coincidence, silently masking the bug in exactly this kind of
/// test. This one deliberately breaks that coincidence: the follower with the
/// numerically largest id gets its edge created *first* (so it's
/// chronologically oldest despite having the largest id), which fails under
/// the old id-based cursor and passes under the fixed created_at-based one.
#[tokio::test]
async fn test_query_reverse_edges_cursor_orders_by_created_at_not_by_id() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut target = User::default();
    target.username = "cursor_order_target".into();
    target.email = "cursor_order_target@x.com".into();
    engine.create_object(&target).await.unwrap();

    let mut followers = Vec::new();
    for i in 0..3 {
        let mut u = User::default();
        u.username = format!("cursor_order_follower{i}");
        u.email = format!("cursor_order_follower{i}@x.com");
        engine.create_object(&u).await.unwrap();
        followers.push(u.id());
    }

    let max_id_follower = *followers.iter().max().unwrap();
    let mut creation_order = vec![max_id_follower];
    creation_order.extend(followers.iter().copied().filter(|id| *id != max_id_follower));

    for follower_id in &creation_order {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(*follower_id, target.id()),
                notification: true,
            })
            .await
            .unwrap();
    }

    // Walk every page (limit 1, so each page boundary exercises the cursor)
    // and record `from` in the order returned.
    let mut visited = Vec::new();
    let mut cursor: Option<uuid::Uuid> = None;
    loop {
        let mut q = EdgeQuery::default().with_limit(1);
        if let Some(c) = cursor {
            q = q.with_cursor(c);
        }
        let page: Vec<Follow> = engine.query_reverse_edges(target.id(), q).await.unwrap();
        let Some(edge) = page.into_iter().next() else {
            break;
        };
        let from = edge.from();
        assert!(
            !visited.contains(&from),
            "follower {from:?} was returned on more than one page"
        );
        visited.push(from);
        cursor = Some(from);
        assert!(
            visited.len() <= 3,
            "pagination did not terminate after visiting every follower"
        );
    }

    let mut expected_newest_first = creation_order.clone();
    expected_newest_first.reverse();
    assert_eq!(
        visited, expected_newest_first,
        "default (no explicit sort) cursor pagination should walk edges newest-created-first, \
         not in id order"
    );
}

#[tokio::test]
async fn test_edge_filter_on_edge_query_context() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    for (i, notif) in [(0, true), (1, false), (2, true)] {
        let mut u = User::default();
        u.username = format!("ef{}", i);
        u.email = format!("ef{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    // edge_eq: only notification=true edges
    let notified: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, true)
        .collect()
        .await
        .unwrap();
    assert_eq!(notified.len(), 2, "edge_eq notification=true");

    // edge_eq: only notification=false edges
    let silent: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_eq(&Follow::FIELDS.notification, false)
        .collect()
        .await
        .unwrap();
    assert_eq!(silent.len(), 1, "edge_eq notification=false");

    // edge_ne: all edges where notification != true
    let not_notified: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .edge_ne(&Follow::FIELDS.notification, true)
        .collect()
        .await
        .unwrap();
    assert_eq!(not_notified.len(), 1, "edge_ne notification=true");
}

#[tokio::test]
async fn test_edge_query_filter_and_sort() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut targets = Vec::new();
    for (i, notif) in [(0, true), (1, false), (2, true)] {
        let mut u = User::default();
        u.username = format!("eq{}", i);
        u.email = format!("eq{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    // EdgeQuery::where_eq via query_edges
    let filtered: Vec<Follow> = engine
        .query_edges(
            pivot.id(),
            EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.len(), 2, "EdgeQuery::where_eq");

    // EdgeQuery::where_ne
    let filtered: Vec<Follow> = engine
        .query_edges(
            pivot.id(),
            EdgeQuery::default().where_ne(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1, "EdgeQuery::where_ne");
}

#[tokio::test]
async fn test_count_edges_with_plan() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "cpivot".into();
    pivot.email = "cpivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    for (i, notif) in [(0, true), (1, false), (2, true), (3, true)] {
        let mut u = User::default();
        u.username = format!("ct{}", i);
        u.email = format!("ct{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: notif,
            })
            .await
            .unwrap();
    }

    let total = engine
        .count_edges::<Follow>(pivot.id(), None)
        .await
        .unwrap();
    assert_eq!(total, 4);

    let notif_count = engine
        .count_edges::<Follow>(
            pivot.id(),
            Some(EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true)),
        )
        .await
        .unwrap();
    assert_eq!(notif_count, 3);
}

// ============================================================
// Section 8: Edge Traversal — collect variants
// ============================================================

#[tokio::test]
async fn test_query_context_get() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qcg_alice".into();
    alice.email = "qcg_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let found = engine
        .preload_object::<User>(alice.id())
        .get()
        .await
        .unwrap();
    assert_eq!(found.unwrap().username, "qcg_alice");

    let missing = engine
        .preload_object::<User>(uuid::Uuid::now_v7())
        .get()
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_edge_query_context_paginate() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "eqp_alice".into();
    alice.email = "eqp_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut targets = Vec::new();
    for name in ["eqp_t1", "eqp_t2", "eqp_t3"] {
        let mut u = User::default();
        u.username = name.into();
        u.email = format!("{name}@x.com");
        engine.create_object(&u).await.unwrap();
        targets.push(u.id());
    }
    // Edges are returned newest-`to`-first by default cursor ordering, so
    // insert in a known order and page from the most recent.
    for &to in &targets {
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(alice.id(), to),
                notification: true,
            })
            .await
            .unwrap();
    }

    let all: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // paginate(None) must behave identically to no pagination at all.
    let unpaginated: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .paginate(None::<uuid::Uuid>)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(unpaginated.len(), 3);

    // paginate(Some(cursor)) filters to `to < cursor` (same contract as
    // `with_cursor`) — use the max `to` so exactly one row (itself) drops.
    let cursor_to = all.iter().map(|e| e.to()).max().unwrap();
    let page: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .paginate(Some(cursor_to))
        .collect_edges()
        .await
        .unwrap();
    assert!(page.iter().all(|e| e.to() < cursor_to));
    assert_eq!(page.len(), all.len() - 1);
}

#[tokio::test]
async fn test_collect_forward_and_reverse() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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

    // Alice is following bob (forward)
    let following: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect()
        .await
        .unwrap();
    assert_eq!(following.len(), 1);
    assert_eq!(following[0].username, "bob");

    // Charlie follows alice (alice's followers = reverse)
    let followers: Vec<User> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_reverse()
        .await
        .unwrap();
    assert_eq!(followers.len(), 1);
    assert_eq!(followers[0].username, "charlie");

    // collect_edges (forward)
    let edges: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].from(), alice.id());
    assert_eq!(edges[0].to(), bob.id());

    // collect_reverse_edges
    let rev_edges: Vec<Follow> = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_reverse_edges()
        .await
        .unwrap();
    assert_eq!(rev_edges.len(), 1);
    assert_eq!(rev_edges[0].from(), charlie.id());
}

#[tokio::test]
async fn test_collect_with_target_and_reverse() {
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

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    // Forward: collect (edge, target) pairs
    let pairs = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].edge().from(), alice.id());
    assert_eq!(pairs[0].object().username, "bob");

    // Reverse: collect (edge, source) pairs from bob's perspective
    let rev_pairs = engine
        .preload_object::<User>(bob.id())
        .edge::<Follow, User>()
        .collect_reverse_with_target()
        .await
        .unwrap();
    assert_eq!(rev_pairs.len(), 1);
    assert_eq!(rev_pairs[0].edge().from(), alice.id());
    assert_eq!(rev_pairs[0].object().username, "alice");
}

#[tokio::test]
async fn test_collect_both() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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
async fn test_collect_both_with_target() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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

    let (fwd, rev) = engine
        .preload_object::<User>(alice.id())
        .edge::<Follow, User>()
        .collect_both_with_target()
        .await
        .unwrap();

    assert_eq!(fwd.len(), 1);
    assert_eq!(fwd[0].edge().from(), alice.id());
    assert_eq!(fwd[0].object().username, "bob");

    assert_eq!(rev.len(), 1);
    assert_eq!(rev[0].edge().from(), charlie.id());
    assert_eq!(rev[0].object().username, "charlie");
}

#[tokio::test]
async fn test_collect_both_edges() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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
async fn test_edge_traversal_with_object_filter() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(pivot.id(), alice.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(pivot.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    // Filter the target objects: only return User where username == "alice"
    let results: Vec<User> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .where_eq(&User::FIELDS.username, "alice")
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].username, "alice");
}

#[tokio::test]
async fn test_edge_traversal_with_limit_and_cursor() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut pivot = User::default();
    pivot.username = "pivot".into();
    pivot.email = "pivot@x.com".into();
    engine.create_object(&pivot).await.unwrap();

    let mut target_ids = Vec::new();
    for i in 0..5 {
        let mut u = User::default();
        u.username = format!("t{}", i);
        u.email = format!("t{}@x.com", i);
        engine.create_object(&u).await.unwrap();
        target_ids.push(u.id());
        engine
            .create_edge(&Follow {
                _meta: EdgeMeta::new(pivot.id(), u.id()),
                notification: true,
            })
            .await
            .unwrap();
    }

    // with_limit
    let limited: Vec<Follow> = engine
        .preload_object::<User>(pivot.id())
        .edge::<Follow, User>()
        .with_limit(3)
        .collect_edges()
        .await
        .unwrap();
    assert_eq!(limited.len(), 3);
}

// ============================================================
// Section 9: Multi-Pivot Preload API
// ============================================================

#[tokio::test]
async fn test_multi_pivot_collect_following() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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
    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_e.1.len(), 1);
    assert_eq!(alice_e.1[0].username, "bob");

    let charlie_e = results
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert!(charlie_e.1.is_empty());
}

#[tokio::test]
async fn test_multi_pivot_collect_reverse_followers() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut michael = User::default();
    michael.username = "michael".into();
    michael.email = "michael@x.com".into();
    engine.create_object(&michael).await.unwrap();

    let mut bob = User::default();
    bob.username = "bob".into();
    bob.email = "bob@x.com".into();
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

    let bob_e = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_e.1.len(), 2);

    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert!(alice_e.1.is_empty());
}

#[tokio::test]
async fn test_multi_pivot_collect_edges_and_reverse_edges() {
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

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    // collect_edges
    let fwd: Vec<(User, Vec<Follow>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_edges()
        .await
        .unwrap();
    let alice_entry = fwd.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert!(alice_entry.1[0].notification);

    // collect_reverse_edges
    let rev: Vec<(User, Vec<Follow>)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse_edges()
        .await
        .unwrap();
    let bob_entry = rev.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].from(), alice.id());
}

#[tokio::test]
async fn test_multi_pivot_collect_with_target_and_reverse() {
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

    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    // Forward
    let fwd = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_with_target()
        .await
        .unwrap();
    let alice_entry = fwd.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_entry.1.len(), 1);
    assert_eq!(alice_entry.1[0].object().username, "bob");

    // Reverse
    let rev = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .collect_reverse_with_target()
        .await
        .unwrap();
    let bob_entry = rev.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_entry.1.len(), 1);
    assert_eq!(bob_entry.1[0].object().username, "alice");
}

#[tokio::test]
async fn test_multi_pivot_count_and_count_reverse() {
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

    let mut charlie = User::default();
    charlie.username = "charlie".into();
    charlie.email = "charlie@x.com".into();
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

    let alice_c = counts.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_c.1, 2);
    let charlie_c = counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_c.1, 0);

    let rev_counts: Vec<(User, u64)> = engine
        .preload_objects::<User>(Query::default())
        .edge::<Follow, User>()
        .count_reverse()
        .await
        .unwrap();

    let charlie_rc = rev_counts
        .iter()
        .find(|(u, _)| u.username == "charlie")
        .unwrap();
    assert_eq!(charlie_rc.1, 2);
    let alice_rc = rev_counts
        .iter()
        .find(|(u, _)| u.username == "alice")
        .unwrap();
    assert_eq!(alice_rc.1, 0);
}

#[tokio::test]
async fn test_multi_pivot_owned() {
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

    for title in ["Post 1", "Post 2"] {
        let mut p = Post::default();
        p.set_owner(alice.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(bob.id());
    p.title = "Bob Post".into();
    engine.create_object(&p).await.unwrap();

    let results: Vec<(User, Vec<Post>)> = engine
        .preload_objects::<User>(Query::default())
        .preload::<Post>()
        .collect()
        .await
        .unwrap();

    let alice_e = results.iter().find(|(u, _)| u.username == "alice").unwrap();
    assert_eq!(alice_e.1.len(), 2);
    let bob_e = results.iter().find(|(u, _)| u.username == "bob").unwrap();
    assert_eq!(bob_e.1.len(), 1);
}

// ============================================================
// Section 10: Unique Constraints
// ============================================================

#[tokio::test]
async fn test_unique_single_field_conflict() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut dupe = User::default();
    dupe.username = "alice".into(); // same username
    dupe.email = "other@x.com".into();
    let err = engine.create_object(&dupe).await.unwrap_err();
    assert_eq!(
        err,
        Error::UniqueConstraintViolation("username".to_string())
    );
}

#[tokio::test]
async fn test_unique_composite_field_conflict() {
    use ousia::{Meta, OusiaDefault, OusiaObject};

    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    #[derive(OusiaObject, OusiaDefault, Debug)]
    #[ousia(
        type_name = "ComboUser",
        unique = "username+email",
        index = "username:search",
        index = "email:search"
    )]
    pub struct ComboUser {
        _meta: Meta,
        pub username: String,
        pub email: String,
    }

    let mut alice = ComboUser::default();
    alice.username = "alice".into();
    alice.email = "alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    // Different email — no conflict.
    let mut alice2 = ComboUser::default();
    alice2.username = "alice".into();
    alice2.email = "other@x.com".into();
    engine.create_object(&alice2).await.unwrap();

    // Same username+email → conflict.
    let mut dupe = ComboUser::default();
    dupe.username = "alice".into();
    dupe.email = "alice@x.com".into();
    let err = engine.create_object(&dupe).await.unwrap_err();
    assert_eq!(
        err,
        Error::UniqueConstraintViolation("username+email".to_string())
    );
}

// ============================================================
// Section 11: Sequences
// ============================================================

#[tokio::test]
async fn test_sequence_value_and_next() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    // Initial value is None.
    assert_eq!(engine.counter_value("seq-test".into()).await, None);

    // next → 1.
    assert_eq!(engine.counter_next_value("seq-test".into()).await, 1);

    // value is now 2.
    assert_eq!(engine.counter_value("seq-test".into()).await, Some(1));

    // next → 1.
    assert_eq!(engine.counter_next_value("seq-test".into()).await, 2);

    // value is now 2.
    assert_eq!(engine.counter_value("seq-test".into()).await, Some(2));

    // next → 3.
    assert_eq!(engine.counter_next_value("seq-test".into()).await, 3);

    // Independent keys don't interfere.
    assert_eq!(engine.counter_value("other-seq".into()).await, None);
}

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

// ============================================================
// Section 13: Geo — derive, schema, CRUD, queries
// ============================================================

#[test]
fn test_geo_derive_metadata() {
    use ousia::query::IndexKind;

    let kinds = Place::FIELDS.location.kinds;
    assert_eq!(kinds.len(), 1);
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
    assert!(Delivery::HAS_GEO_FIELDS);
}

#[tokio::test]
async fn test_geo_schema_and_crud() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let postgis_enabled: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'postgis')")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(postgis_enabled);

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

    // Delete cascades to object_geo.
    let _: Option<Place> = engine
        .delete_object(place.id(), system_owner())
        .await
        .unwrap();
    let gone: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(gone, 0);
}

#[tokio::test]
async fn test_geo_update_diff() {
    let (_r, pool) = setup_test_db().await;
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

    // Scalar-only update: geo row should not change.
    place.name = "Renamed".into();
    engine.update_object(&mut place).await.unwrap();
    let hash_scalar: String =
        sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
            .bind(place.id())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        hash_before, hash_scalar,
        "scalar update must not change geo row"
    );

    // Lat mutation: hash must change.
    place.lat = 11.0;
    engine.update_object(&mut place).await.unwrap();
    let hash_geo: String = sqlx::query_scalar("SELECT hash FROM object_geo WHERE object_id = $1")
        .bind(place.id())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_ne!(hash_before, hash_geo, "geo update must change hash");
}

#[tokio::test]
async fn test_geo_query_within() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (name, lat, lon) in [("near", 0.0, 0.0), ("mid", 0.01, 0.0), ("far", 1.0, 0.0)] {
        let mut p = Place::default();
        p.name = name.into();
        p.lat = lat;
        p.lon = lon;
        engine.create_object(&p).await.unwrap();
    }

    // Within 5 km — near + mid only.
    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            5_000.0,
        ))
        .await
        .unwrap();
    let names: std::collections::HashSet<&str> = hits.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains("near"));
    assert!(names.contains("mid"));
    assert!(!names.contains("far"));

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
async fn test_geo_query_within_empty() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut p = Place::default();
    p.name = "somewhere".into();
    p.lat = 50.0;
    p.lon = 50.0;
    engine.create_object(&p).await.unwrap();

    // Radius at (0,0) — no match.
    let hits: Vec<Place> = engine
        .query_objects::<Place>(Query::default().where_geo_within(
            &Place::FIELDS.location,
            0.0,
            0.0,
            100.0,
        ))
        .await
        .unwrap();
    assert!(hits.is_empty());
}

#[cfg(test)]
fn make_place(name: &str, lat: f64, lon: f64) -> Place {
    let mut p = Place::default();
    p.name = name.to_string();
    p.lat = lat;
    p.lon = lon;
    p
}

#[cfg(test)]
fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let r = 6_371_008.8_f64;
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
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

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

    // Corner bbox.
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
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (n, lat, lon) in [
        ("a", 0.01, 0.01),
        ("b", 0.05, 0.05),
        ("c", 0.10, 0.10),
        ("d", 0.50, 0.50),
        ("e", 1.00, 1.00),
    ] {
        engine
            .create_object(&make_place(n, lat, lon))
            .await
            .unwrap();
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

    let desc: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                .order_by_distance(&Place::FIELDS.location, 0.0, 0.0, false)
                .with_limit(2),
        )
        .await
        .unwrap();
    let desc_names: Vec<&str> = desc.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(desc_names, vec!["e", "d"]);
}

#[tokio::test]
async fn test_geo_collect_with_distance() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    for (n, lat, lon) in [
        ("near", 0.001, 0.001),
        ("mid", 0.05, 0.05),
        ("far", 0.2, 0.2),
        ("farther", 1.0, 1.0),
    ] {
        engine
            .create_object(&make_place(n, lat, lon))
            .await
            .unwrap();
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
    for w in results.windows(2) {
        assert!(w[0].1 <= w[1].1, "distances must be non-decreasing");
    }
    for (p, d) in &results {
        let oracle = haversine_m(0.0, 0.0, p.lon, p.lat);
        let rel = (d - oracle).abs() / oracle.max(1.0);
        assert!(rel < 0.01, "distance {} too far from oracle {}", d, oracle);
    }
}

#[tokio::test]
async fn test_geo_collect_with_distance_requires_order() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
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
    assert!(matches!(err, Error::InvalidQuery(_)));
}

#[tokio::test]
async fn test_geo_multi_field_delivery() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

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

    let near_pickup: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.pickup,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(near_pickup.len(), 1);

    let not_dropoff: Vec<Delivery> = engine
        .query_objects::<Delivery>(Query::default().where_geo_within(
            &Delivery::FIELDS.dropoff,
            3.3792,
            6.5244,
            50_000.0,
        ))
        .await
        .unwrap();
    assert_eq!(not_dropoff.len(), 0);
}

#[tokio::test]
async fn test_geo_multiple_within_filters_and() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

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
async fn test_geo_mixed_within_and_bbox() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    engine
        .create_object(&make_place("in_both", 0.001, 0.001))
        .await
        .unwrap();
    engine
        .create_object(&make_place("only_radius", 0.4, 0.0))
        .await
        .unwrap();
    engine
        .create_object(&make_place("outside", 5.0, 5.0))
        .await
        .unwrap();

    let hits: Vec<Place> = engine
        .query_objects::<Place>(
            Query::default()
                .where_geo_within(&Place::FIELDS.location, 0.0, 0.0, 50_000.0)
                .where_geo_in_bbox(&Place::FIELDS.location, -0.1, -0.1, 0.1, 0.1),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].name, "in_both");
}

#[tokio::test]
async fn test_geo_order_on_different_field_than_filter() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut d1 = Delivery::default();
    d1.pickup_lat = 0.01;
    d1.pickup_lon = 0.01;
    d1.dropoff_lat = 9.0;
    d1.dropoff_lon = 9.0;
    engine.create_object(&d1).await.unwrap();

    let mut d2 = Delivery::default();
    d2.pickup_lat = 0.05;
    d2.pickup_lon = 0.05;
    d2.dropoff_lat = 9.9;
    d2.dropoff_lon = 9.9;
    engine.create_object(&d2).await.unwrap();

    let mut d3 = Delivery::default();
    d3.pickup_lat = 0.1;
    d3.pickup_lon = 0.1;
    d3.dropoff_lat = 9.5;
    d3.dropoff_lon = 9.5;
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
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id(), d2.id()); // closest dropoff to (10,10)
    assert_eq!(results[2].id(), d1.id()); // farthest dropoff
}

// ============================================================
// Section 14: Batch edge/ownership traversal (arbitrary id sets)
// ============================================================

#[tokio::test]
async fn test_query_edges_batch_groups_by_from() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qeb_alice".into();
    alice.email = "qeb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "qeb_bob".into();
    bob.email = "qeb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut t1 = User::default();
    t1.username = "qeb_t1".into();
    t1.email = "qeb_t1@x.com".into();
    engine.create_object(&t1).await.unwrap();

    let mut t2 = User::default();
    t2.username = "qeb_t2".into();
    t2.email = "qeb_t2@x.com".into();
    engine.create_object(&t2).await.unwrap();

    let mut t3 = User::default();
    t3.username = "qeb_t3".into();
    t3.email = "qeb_t3@x.com".into();
    engine.create_object(&t3).await.unwrap();

    // alice → t1, t2 (2 edges); bob → t3 (1 edge)
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), t1.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), t2.id()),
            notification: false,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), t3.id()),
            notification: true,
        })
        .await
        .unwrap();

    let no_edges_id = uuid::Uuid::now_v7();

    let grouped = engine
        .query_edges_batch::<Follow>(&[alice.id(), bob.id(), no_edges_id], EdgeQuery::default())
        .await
        .unwrap();

    assert_eq!(grouped.get(&alice.id()).unwrap().len(), 2);
    assert_eq!(grouped.get(&bob.id()).unwrap().len(), 1);
    assert!(grouped.get(&no_edges_id).is_none());

    // With an EdgeQuery filter applied.
    let filtered = engine
        .query_edges_batch::<Follow>(
            &[alice.id(), bob.id()],
            EdgeQuery::default().where_eq(&Follow::FIELDS.notification, true),
        )
        .await
        .unwrap();
    assert_eq!(filtered.get(&alice.id()).unwrap().len(), 1);
    assert_eq!(filtered.get(&bob.id()).unwrap().len(), 1);
}

#[tokio::test]
async fn test_query_edges_with_targets_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "qewtb_alice".into();
    alice.email = "qewtb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "qewtb_bob".into();
    bob.email = "qewtb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut u1 = User::default();
    u1.username = "qewtb_u1".into();
    u1.email = "qewtb_u1@x.com".into();
    engine.create_object(&u1).await.unwrap();

    let mut u2 = User::default();
    u2.username = "qewtb_u2".into();
    u2.email = "qewtb_u2@x.com".into();
    engine.create_object(&u2).await.unwrap();

    let mut u3 = User::default();
    u3.username = "qewtb_u3".into();
    u3.email = "qewtb_u3@x.com".into();
    engine.create_object(&u3).await.unwrap();

    // alice → u1, u2; bob → u3
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), u1.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), u2.id()),
            notification: true,
        })
        .await
        .unwrap();
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(bob.id(), u3.id()),
            notification: true,
        })
        .await
        .unwrap();

    let grouped = engine
        .query_edges_with_targets_batch::<Follow, User>(
            &[alice.id(), bob.id()],
            &[],
            EdgeQuery::default(),
        )
        .await
        .unwrap();

    let alice_pairs = grouped.get(&alice.id()).unwrap();
    assert_eq!(alice_pairs.len(), 2);
    let mut alice_usernames: Vec<&str> = alice_pairs
        .iter()
        .map(|p| p.object().username.as_str())
        .collect();
    alice_usernames.sort();
    assert_eq!(alice_usernames, vec!["qewtb_u1", "qewtb_u2"]);
    assert!(alice_pairs.iter().all(|p| p.edge().from() == alice.id()));

    let bob_pairs = grouped.get(&bob.id()).unwrap();
    assert_eq!(bob_pairs.len(), 1);
    assert_eq!(bob_pairs[0].object().username, "qewtb_u3");

    // With an object filter restricting the joined target.
    let filtered = engine
        .query_edges_with_targets_batch::<Follow, User>(
            &[alice.id(), bob.id()],
            &[filter!(&User::FIELDS.username, "qewtb_u1")],
            EdgeQuery::default(),
        )
        .await
        .unwrap();
    let alice_filtered = filtered.get(&alice.id()).unwrap();
    assert_eq!(alice_filtered.len(), 1);
    assert_eq!(alice_filtered[0].object().username, "qewtb_u1");
    assert!(filtered.get(&bob.id()).is_none());
}

#[tokio::test]
async fn test_count_reverse_edges_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut alice = User::default();
    alice.username = "creb_alice".into();
    alice.email = "creb_alice@x.com".into();
    engine.create_object(&alice).await.unwrap();

    let mut bob = User::default();
    bob.username = "creb_bob".into();
    bob.email = "creb_bob@x.com".into();
    engine.create_object(&bob).await.unwrap();

    let mut charlie = User::default();
    charlie.username = "creb_charlie".into();
    charlie.email = "creb_charlie@x.com".into();
    engine.create_object(&charlie).await.unwrap();

    // charlie ← alice, bob (2 reverse edges); bob ← alice (1 reverse edge);
    // alice has none.
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), charlie.id()),
            notification: true,
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
    engine
        .create_edge(&Follow {
            _meta: EdgeMeta::new(alice.id(), bob.id()),
            notification: true,
        })
        .await
        .unwrap();

    let counts = engine
        .count_reverse_edges_batch::<Follow>(
            &[charlie.id(), bob.id(), alice.id()],
            EdgeQuery::default(),
        )
        .await
        .unwrap();

    assert_eq!(counts.get(&charlie.id()).copied(), Some(2));
    assert_eq!(counts.get(&bob.id()).copied(), Some(1));
    // No reverse edges → the id is absent from the map entirely (GROUP BY
    // produces no row), not present with a value of 0.
    assert!(counts.get(&alice.id()).is_none());
}

#[tokio::test]
async fn test_fetch_owned_objects_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner1 = User::default();
    owner1.username = "foob_owner1".into();
    owner1.email = "foob_owner1@x.com".into();
    engine.create_object(&owner1).await.unwrap();

    let mut owner2 = User::default();
    owner2.username = "foob_owner2".into();
    owner2.email = "foob_owner2@x.com".into();
    engine.create_object(&owner2).await.unwrap();

    for title in ["Post A", "Post B"] {
        let mut p = Post::default();
        p.set_owner(owner1.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(owner2.id());
    p.title = "Post C".into();
    engine.create_object(&p).await.unwrap();

    let no_posts_owner = uuid::Uuid::now_v7();

    let grouped = engine
        .fetch_owned_objects_batch::<Post>(&[owner1.id(), owner2.id(), no_posts_owner])
        .await
        .unwrap();

    assert_eq!(grouped.get(&owner1.id()).unwrap().len(), 2);
    assert_eq!(grouped.get(&owner2.id()).unwrap().len(), 1);
    assert!(grouped.get(&no_posts_owner).is_none());
}

// ============================================================
// Section 15: Two-hop batch traversal + batch owned-object count (2.1.0)
// ============================================================

/// Ids for the shared 2-hop fixture graph:
///
/// hub1 -[pos 1]-> s1 -> l1(req), l2(!req), l3(req)
/// hub1 -[pos 2]-> s2 -> (no leaves)
/// hub2 -[pos 1]-> s3 -> l4(req)
/// hub3 -> (no spokes)
#[cfg(test)]
struct TwoHopIds {
    hub1: uuid::Uuid,
    hub2: uuid::Uuid,
    hub3: uuid::Uuid,
    s1: uuid::Uuid,
    s2: uuid::Uuid,
    s3: uuid::Uuid,
    l1: uuid::Uuid,
    l2: uuid::Uuid,
    l3: uuid::Uuid,
    l4: uuid::Uuid,
}

#[cfg(test)]
async fn setup_two_hop_fixture() -> (ContainerAsync<Postgres>, Engine, TwoHopIds) {
    let (container, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut hubs = Vec::new();
    for name in ["hub1", "hub2", "hub3"] {
        let mut h = Hub::default();
        h.name = name.into();
        engine.create_object(&h).await.unwrap();
        hubs.push(h.id());
    }
    let mut spokes = Vec::new();
    for name in ["s1", "s2", "s3"] {
        let mut s = Spoke::default();
        s.name = name.into();
        engine.create_object(&s).await.unwrap();
        spokes.push(s.id());
    }
    let mut leaves = Vec::new();
    for name in ["l1", "l2", "l3", "l4"] {
        let mut l = Leaf::default();
        l.name = name.into();
        engine.create_object(&l).await.unwrap();
        leaves.push(l.id());
    }
    let ids = TwoHopIds {
        hub1: hubs[0],
        hub2: hubs[1],
        hub3: hubs[2],
        s1: spokes[0],
        s2: spokes[1],
        s3: spokes[2],
        l1: leaves[0],
        l2: leaves[1],
        l3: leaves[2],
        l4: leaves[3],
    };

    for (hub, spoke, position) in [
        (ids.hub1, ids.s1, 1i64),
        (ids.hub1, ids.s2, 2),
        (ids.hub2, ids.s3, 1),
    ] {
        engine
            .create_edge(&HubSpoke {
                _meta: EdgeMeta::new(hub, spoke),
                position,
            })
            .await
            .unwrap();
    }
    for (spoke, leaf, required) in [
        (ids.s1, ids.l1, true),
        (ids.s1, ids.l2, false),
        (ids.s1, ids.l3, true),
        (ids.s3, ids.l4, true),
    ] {
        engine
            .create_edge(&SpokeLeaf {
                _meta: EdgeMeta::new(spoke, leaf),
                required,
            })
            .await
            .unwrap();
    }

    (container, engine, ids)
}

#[tokio::test]
async fn test_two_hop_collect_with_target_full_chain() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let unknown_id = uuid::Uuid::now_v7();
    let by_hub = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2, ids.hub3, unknown_id])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();

    // Every pivot id appears — zero-edge and unknown ids map to empty Vecs.
    assert_eq!(by_hub.len(), 4);
    assert!(by_hub.get(&ids.hub3).unwrap().is_empty());
    assert!(by_hub.get(&unknown_id).unwrap().is_empty());

    // hub1: two spokes; s1 carries 3 leaves with edge metadata, s2 none (LEFT JOIN).
    let hub1_children = by_hub.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 2);
    let (s1, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1.name, "s1");
    assert_eq!(s1_leaves.len(), 3);
    let mut leaf_ids: Vec<uuid::Uuid> = s1_leaves.iter().map(|oe| oe.object().id()).collect();
    leaf_ids.sort();
    let mut expected = vec![ids.l1, ids.l2, ids.l3];
    expected.sort();
    assert_eq!(leaf_ids, expected);
    // Edge metadata survives the join.
    let l2_edge = s1_leaves
        .iter()
        .find(|oe| oe.object().id() == ids.l2)
        .unwrap()
        .edge();
    assert!(!l2_edge.required);
    assert!(
        s1_leaves
            .iter()
            .filter(|oe| oe.object().id() != ids.l2)
            .all(|oe| oe.edge().required)
    );
    let (_, s2_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s2)
        .unwrap();
    assert!(s2_leaves.is_empty());

    // hub2: one spoke with exactly its own leaf — no cross-contamination.
    let hub2_children = by_hub.get(&ids.hub2).unwrap();
    assert_eq!(hub2_children.len(), 1);
    let (s3, s3_leaves) = &hub2_children[0];
    assert_eq!(s3.id(), ids.s3);
    assert_eq!(s3_leaves.len(), 1);
    assert_eq!(s3_leaves[0].object().id(), ids.l4);
}

#[tokio::test]
async fn test_two_hop_collect_drops_edge_metadata() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let by_hub = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect()
        .await
        .unwrap();

    let hub1_children = by_hub.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 2);
    let (_, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 3);
    let (_, s2_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s2)
        .unwrap();
    assert!(s2_leaves.is_empty());
    assert_eq!(by_hub.get(&ids.hub2).unwrap().len(), 1);
}

/// `batch_edge` (raw ids) and `preload_objects(query).edge()` must be the
/// same code path into `then_edge` — identical results on identical data.
#[tokio::test]
async fn test_two_hop_batch_edge_vs_preload_objects_parity() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;

    let from_ids = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1, ids.hub2, ids.hub3])
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let from_query = engine
        .preload_objects::<Hub>(Query::default())
        .edge::<HubSpoke, Spoke>()
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();

    // Same keys, same per-hub spoke sets, same per-spoke leaf sets.
    let shape = |m: &std::collections::HashMap<
        uuid::Uuid,
        Vec<(Spoke, Vec<ousia::ObjectEdge<SpokeLeaf, Leaf>>)>,
    >| {
        let mut out: Vec<(uuid::Uuid, Vec<(uuid::Uuid, Vec<uuid::Uuid>)>)> = m
            .iter()
            .map(|(hub, children)| {
                let mut children: Vec<(uuid::Uuid, Vec<uuid::Uuid>)> = children
                    .iter()
                    .map(|(s, leaves)| {
                        let mut leaf_ids: Vec<uuid::Uuid> =
                            leaves.iter().map(|oe| oe.object().id()).collect();
                        leaf_ids.sort();
                        (s.id(), leaf_ids)
                    })
                    .collect();
                children.sort();
                (*hub, children)
            })
            .collect();
        out.sort();
        out
    };
    assert_eq!(shape(&from_ids), shape(&from_query));
}

#[tokio::test]
async fn test_two_hop_filter_passthrough() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;
    let all_hubs = [ids.hub1, ids.hub2, ids.hub3];

    // Hop-2 edge filter: only required leaves survive.
    let required_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .then_edge::<SpokeLeaf, Leaf>()
        .with_edge_query(EdgeQuery::default().where_eq(&SpokeLeaf::FIELDS.required, true))
        .collect_with_target()
        .await
        .unwrap();
    let (_, s1_leaves) = required_only
        .get(&ids.hub1)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    let mut leaf_ids: Vec<uuid::Uuid> = s1_leaves.iter().map(|oe| oe.object().id()).collect();
    leaf_ids.sort();
    let mut expected = vec![ids.l1, ids.l3];
    expected.sort();
    assert_eq!(leaf_ids, expected);

    // Hop-2 object filter: an excluded leaf drops its edge with it — the
    // spoke stays, with an empty leaf Vec (not a stranded half-pair).
    let l2_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .then_edge::<SpokeLeaf, Leaf>()
        .obj_eq(&Leaf::FIELDS.name, "l2")
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = l2_only.get(&ids.hub1).unwrap();
    let (_, s1_leaves) = hub1_children
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 1);
    assert_eq!(s1_leaves[0].object().id(), ids.l2);
    let (_, s3_leaves) = l2_only
        .get(&ids.hub2)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s3)
        .unwrap();
    assert!(s3_leaves.is_empty());

    // Hop-1 edge filter: position=1 keeps s1/s3, drops s2 entirely.
    let pos1 = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .with_edge_query(EdgeQuery::default().where_eq(&HubSpoke::FIELDS.position, 1i64))
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = pos1.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 1);
    assert_eq!(hub1_children[0].0.id(), ids.s1);
    assert_eq!(pos1.get(&ids.hub2).unwrap().len(), 1);

    // Hop-1 object filter: only the named spoke survives; hub2 goes empty.
    let s1_only = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&all_hubs)
        .obj_eq(&Spoke::FIELDS.name, "s1")
        .then_edge::<SpokeLeaf, Leaf>()
        .collect_with_target()
        .await
        .unwrap();
    let hub1_children = s1_only.get(&ids.hub1).unwrap();
    assert_eq!(hub1_children.len(), 1);
    assert_eq!(hub1_children[0].0.id(), ids.s1);
    assert!(s1_only.get(&ids.hub2).unwrap().is_empty());
}

/// Per-hop LIMIT is deliberately unsupported on the 2-hop path for 2.1.0 —
/// Postgres can't LIMIT one leg of a flat JOIN; a lateral-join variant is
/// deferred until a concrete need shows up (see plan_2.1.md, "SQL shape").
/// `EdgeQuery::limit`/`cursor` are ignored by
/// `query_two_hop_edges_with_targets_batch`. This test exists to document
/// that decision; enable it if lateral-join limits ever land.
#[tokio::test]
#[ignore = "per-hop LIMIT unsupported on the 2-hop path (deferred to a lateral-join follow-up)"]
async fn test_two_hop_per_hop_limit() {
    let (_r, engine, ids) = setup_two_hop_fixture().await;
    let limited = engine
        .batch_edge::<HubSpoke, Hub, Spoke>(&[ids.hub1])
        .then_edge::<SpokeLeaf, Leaf>()
        .with_edge_query(EdgeQuery::default().with_limit(1))
        .collect_with_target()
        .await
        .unwrap();
    let (_, s1_leaves) = limited
        .get(&ids.hub1)
        .unwrap()
        .iter()
        .find(|(s, _)| s.id() == ids.s1)
        .unwrap();
    assert_eq!(s1_leaves.len(), 1); // would require JOIN LATERAL (... LIMIT 1)
}

#[tokio::test]
async fn test_count_owned_objects_batch() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let mut owner1 = User::default();
    owner1.username = "coob_owner1".into();
    owner1.email = "coob_owner1@x.com".into();
    engine.create_object(&owner1).await.unwrap();

    let mut owner2 = User::default();
    owner2.username = "coob_owner2".into();
    owner2.email = "coob_owner2@x.com".into();
    engine.create_object(&owner2).await.unwrap();

    let mut owner3 = User::default();
    owner3.username = "coob_owner3".into();
    owner3.email = "coob_owner3@x.com".into();
    engine.create_object(&owner3).await.unwrap();

    for title in ["Count A", "Count B", "Count C"] {
        let mut p = Post::default();
        p.set_owner(owner1.id());
        p.title = title.into();
        engine.create_object(&p).await.unwrap();
    }
    let mut p = Post::default();
    p.set_owner(owner2.id());
    p.title = "Count D".into();
    engine.create_object(&p).await.unwrap();

    let absent_owner = uuid::Uuid::now_v7();

    let counts = engine
        .count_owned_objects_batch::<Post>(&[owner1.id(), owner2.id(), owner3.id(), absent_owner])
        .await
        .unwrap();

    assert_eq!(counts.get(&owner1.id()).copied(), Some(3));
    assert_eq!(counts.get(&owner2.id()).copied(), Some(1));
    // Zero objects → absent from the map entirely (GROUP BY produces no
    // row), matching count_reverse_edges_batch's convention.
    assert!(counts.get(&owner3.id()).is_none());
    assert!(counts.get(&absent_owner).is_none());
}

// ============================================================
// Section 16: Field-name drift detection at startup
// ============================================================
//
// `PostgresAdapter::check_field_drift` (called from `init_schema`) samples
// one stored row per registered type and warns — doesn't fail — when a
// key present in stored data no longer matches any field on the current
// struct (the field was renamed or removed, so its old value will be
// dropped on the next save). Named-key encoding means field identity is
// the field's name, so this is the safety net for renames the compiler
// can't catch.

#[tokio::test]
async fn test_check_field_drift_detects_renamed_field_on_object() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    // Simulate a row written before `avatar` was renamed/removed from
    // `User` — insert directly, bypassing the engine (which only knows
    // the struct's *current* field set).
    let mut legacy = std::collections::BTreeMap::new();
    legacy.insert("username", serde_json::json!("alice"));
    legacy.insert("avatar", serde_json::json!("http://example.com/a.png"));
    let data = rmp_serde::to_vec_named(&legacy).unwrap();
    sqlx::query(
        "INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta) \
         VALUES ($1, 'User', $2, now(), now(), $3, '{}'::jsonb)",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(uuid::Uuid::nil())
    .bind(data)
    .execute(&pool)
    .await
    .unwrap();

    let warnings = adapter.check_field_drift().await.unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("object:User") && w.contains("`avatar`")),
        "expected an avatar drift warning, got {:?}",
        warnings
    );
}

#[tokio::test]
async fn test_check_field_drift_detects_renamed_field_on_edge() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(pool.clone())));
    let mut alice = User::default();
    alice.username = "alice_drift_edge_test".into();
    let mut bob = User::default();
    bob.username = "bob_drift_edge_test".into();
    engine.create_object(&alice).await.unwrap();
    engine.create_object(&bob).await.unwrap();

    let mut legacy = std::collections::BTreeMap::new();
    legacy.insert("notification", serde_json::json!(true));
    legacy.insert("legacy_field", serde_json::json!(42));
    let data = rmp_serde::to_vec_named(&legacy).unwrap();
    sqlx::query(
        "INSERT INTO object_edges (\"from\", \"to\", type, created_at, updated_at, data, index_meta) \
         VALUES ($1, $2, 'Follow', now(), now(), $3, '{}'::jsonb)",
    )
    .bind(alice.id())
    .bind(bob.id())
    .bind(data)
    .execute(&pool)
    .await
    .unwrap();

    let warnings = adapter.check_field_drift().await.unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("edge:Follow") && w.contains("`legacy_field`")),
        "expected a legacy_field drift warning, got {:?}",
        warnings
    );
}

#[tokio::test]
async fn test_check_field_drift_no_warnings_on_matching_data() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let engine = Engine::new(Box::new(PostgresAdapter::from_pool(pool.clone())));
    let mut user = User::default();
    user.username = "no_drift_test_user".into();
    engine.create_object(&user).await.unwrap();

    let warnings = adapter.check_field_drift().await.unwrap();
    assert!(
        warnings.iter().all(|w| !w.contains("object:User")),
        "expected no User drift warnings, got {:?}",
        warnings
    );
}

#[tokio::test]
async fn test_check_field_drift_clean_db_no_rows() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    // No rows in any table yet — must not be mistaken for drift.
    adapter.init_schema().await.unwrap();
    let warnings = adapter.check_field_drift().await.unwrap();
    assert!(warnings.is_empty(), "got {:?}", warnings);
}

/// Manifest smoke test — every `OusiaObject`/`OusiaEdge` derive in this test
/// crate must register an entry in `ousia::MANIFEST` at link time. No DB.
#[test]
fn test_manifest_registers_objects_and_edges() {
    use ousia::manifest;

    let objects = manifest::object_types_sorted();
    // The test types defined in `adapter/mod.rs` (User, Place, Dropoff, etc.).
    assert!(
        objects.contains(&"User"),
        "expected User in manifest, got {:?}",
        objects
    );
    assert!(
        objects.contains(&"Place"),
        "expected Place in manifest, got {:?}",
        objects
    );

    let edges = manifest::edge_entries_sorted();
    let follow = edges
        .iter()
        .find(|e| e.type_name == "Follow")
        .expect("Follow edge must register in MANIFEST");
    assert_eq!(follow.from_type, Some("User"));
    assert_eq!(follow.to_type, Some("User"));

    // render_json should produce non-empty, parseable-looking output.
    let json = manifest::render_json();
    assert!(json.contains("\"objects\""));
    assert!(json.contains("\"User\""));
    assert!(json.contains("\"edges\""));
    assert!(json.contains("\"Follow\""));
}
