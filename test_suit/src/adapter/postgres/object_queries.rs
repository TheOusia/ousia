use super::*;

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

    for owner in [uuid::Uuid::now_v7(), uuid::Uuid::now_v7(), system_owner()] {
        for title in ["wide", "wide", "other"] {
            let mut post = Post::default();
            post.set_owner(owner);
            post.title = title.into();
            engine.create_object(&post).await.unwrap();
        }
    }

    assert_eq!(engine.count_objects::<Post>(Some(Query::wide())).await.unwrap(), 9);
    let wide_titled = Query::wide().where_eq(&Post::FIELDS.title, "wide");
    assert_eq!(engine.count_objects::<Post>(Some(wide_titled)).await.unwrap(), 6);
    // counts match what query_objects returns for the same query
    let rows: Vec<Post> = engine
        .query_objects(Query::wide().where_eq(&Post::FIELDS.title, "wide"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 6);
}

#[tokio::test]
async fn test_timestamp_filters_read_the_real_columns() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool);
    adapter.init_schema().await.unwrap();
    let engine = Engine::new(Box::new(adapter));

    let owner = uuid::Uuid::now_v7();
    let mut ids = Vec::new();
    let mut marks = Vec::new();
    for title in ["p0", "p1", "p2"] {
        marks.push(chrono::Utc::now());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let mut post = Post::default();
        post.set_owner(owner);
        post.title = title.into();
        engine.create_object(&post).await.unwrap();
        ids.push(post.id());
    }
    let titles = |rows: Vec<Post>| {
        let mut t: Vec<String> = rows.into_iter().map(|p| p.title).collect();
        t.sort();
        t
    };

    let q = Query::new(owner).where_gt(&Post::FIELDS.created_at, marks[1]);
    assert_eq!(titles(engine.query_objects(q).await.unwrap()), ["p1", "p2"]);
    let q = Query::new(owner).where_lt(&Post::FIELDS.created_at, marks[1]);
    assert_eq!(titles(engine.query_objects(q).await.unwrap()), ["p0"]);

    // string values are compared as timestamps, and patterns match the column's text
    let q = Query::new(owner).where_gt(&Post::FIELDS.created_at, marks[1].to_rfc3339());
    assert_eq!(titles(engine.query_objects(q).await.unwrap()), ["p1", "p2"]);
    let today = marks[0].format("%Y-%m-%d").to_string();
    let q = Query::new(owner).where_begins_with(&Post::FIELDS.created_at, today.as_str());
    assert_eq!(titles(engine.query_objects(q).await.unwrap()).len(), 3);
    let bad = engine
        .query_objects::<Post>(Query::new(owner).where_gt(&Post::FIELDS.created_at, 5i64))
        .await;
    assert!(matches!(bad, Err(Error::Storage(_))), "a non-timestamp value is an error: {bad:?}");

    // equality against the timestamp as stored (microsecond precision)
    let stored: Post = engine.fetch_object(ids[2]).await.unwrap().unwrap();
    let q = Query::new(owner).where_eq(&Post::FIELDS.created_at, stored.created_at());
    assert_eq!(titles(engine.query_objects(q).await.unwrap()), ["p2"]);

    // transfer_object bumps the updated_at column, not the index_meta copy
    let before = chrono::Utc::now();
    let other = uuid::Uuid::now_v7();
    engine.transfer_object::<Post>(ids[0], owner, other).await.unwrap();
    let q = Query::new(other).where_gt(&Post::FIELDS.updated_at, before);
    assert_eq!(titles(engine.query_objects(q).await.unwrap()), ["p0"]);
    let n = engine
        .count_objects::<Post>(Some(Query::wide().where_gt(&Post::FIELDS.updated_at, before)))
        .await
        .unwrap();
    assert_eq!(n, 1);
}
