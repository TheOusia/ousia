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

/// The partitions `init_schema` left for an object type `RetiredEntry` and an
/// edge `RetiredLink` (RetiredEntry → RetiredEntry), both since removed from
/// the manifest: the same DDL `create_*_partition` emits, FKs included.
const RETIRED_TYPE_DDL: &str = "\
    CREATE TABLE objects_retiredentry PARTITION OF objects FOR VALUES IN ('RetiredEntry'); \
    CREATE UNIQUE INDEX objects_retiredentry_id_unq ON objects_retiredentry(id); \
    CREATE TABLE object_constraints_retiredentry \
      PARTITION OF object_constraints FOR VALUES IN ('RetiredEntry'); \
    ALTER TABLE object_constraints_retiredentry ADD CONSTRAINT object_constraints_retiredentry_id_fk \
      FOREIGN KEY (id) REFERENCES objects_retiredentry(id) ON DELETE CASCADE; \
    CREATE TABLE object_geo_retiredentry PARTITION OF object_geo FOR VALUES IN ('RetiredEntry'); \
    ALTER TABLE object_geo_retiredentry ADD CONSTRAINT object_geo_retiredentry_object_id_fk \
      FOREIGN KEY (object_id) REFERENCES objects_retiredentry(id) ON DELETE CASCADE; \
    CREATE TABLE object_edges_retiredlink PARTITION OF object_edges FOR VALUES IN ('RetiredLink'); \
    ALTER TABLE object_edges_retiredlink ADD CONSTRAINT object_edges_retiredlink_from_fk \
      FOREIGN KEY (\"from\") REFERENCES objects_retiredentry(id) ON DELETE CASCADE; \
    ALTER TABLE object_edges_retiredlink ADD CONSTRAINT object_edges_retiredlink_to_fk \
      FOREIGN KEY (\"to\") REFERENCES objects_retiredentry(id) ON DELETE CASCADE;";

async fn table_exists(pool: &PgPool, name: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_removed_types_empty_partitions_are_dropped_despite_their_fks() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    sqlx::raw_sql(RETIRED_TYPE_DDL).execute(&pool).await.unwrap();

    adapter.init_schema().await.unwrap();
    for t in [
        "objects_retiredentry",
        "object_constraints_retiredentry",
        "object_geo_retiredentry",
        "object_edges_retiredlink",
    ] {
        assert!(!table_exists(&pool, t).await, "{t} not dropped");
    }
    // And the next start is clean too.
    adapter.init_schema().await.unwrap();
}

#[tokio::test]
async fn test_orphan_partition_still_referenced_is_kept_and_startup_succeeds() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    sqlx::raw_sql(RETIRED_TYPE_DDL).execute(&pool).await.unwrap();
    // A table ousia doesn't manage, still pointing at the retired partition.
    sqlx::raw_sql(
        "CREATE TABLE retired_notes (entry uuid REFERENCES objects_retiredentry(id))",
    )
    .execute(&pool)
    .await
    .unwrap();

    adapter.init_schema().await.unwrap();
    assert!(table_exists(&pool, "objects_retiredentry").await, "dropped under a live FK");
    assert!(table_exists(&pool, "retired_notes").await);
    // Its own dependents carry no data and are gone.
    assert!(!table_exists(&pool, "object_constraints_retiredentry").await);
    assert!(!table_exists(&pool, "object_geo_retiredentry").await);
    assert!(!table_exists(&pool, "object_edges_retiredlink").await);
}
