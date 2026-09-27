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

// ============================================================
// Composite indexes (`#[ousia(composite_index = "...")]`)
// ============================================================

/// `(name, definition, oid)` of each index on `table` that ousia built from a
/// `composite_index` declaration, by name. The oid shows whether an index was
/// rebuilt.
async fn composite_indexes_on(pool: &PgPool, table: &str) -> Vec<(String, String, i64)> {
    sqlx::query_as(
        "SELECT i.relname::text, pg_get_indexdef(i.oid), i.oid::bigint \
         FROM pg_index ix \
         JOIN pg_class i ON i.oid = ix.indexrelid \
         JOIN pg_class t ON t.oid = ix.indrelid \
         WHERE t.relname = $1 \
           AND obj_description(i.oid, 'pg_class') LIKE 'ousia composite_index: %' \
         ORDER BY 1",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn index_names_on(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar("SELECT indexname::text FROM pg_indexes WHERE tablename = $1 ORDER BY 1")
        .bind(table)
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_composite_indexes_are_built_on_the_declaring_partition_only() {
    let (_r, pool) = setup_test_db().await;
    PostgresAdapter::from_pool(pool.clone()).init_schema().await.unwrap();

    let defs: Vec<(String, String)> = composite_indexes_on(&pool, "objects_feedevent")
        .await
        .into_iter()
        .map(|(name, def, _)| (name, def))
        .collect();
    assert_eq!(
        defs,
        vec![
            (
                "objects_feedevent_actor_created_at_desc_idx".to_string(),
                "CREATE INDEX objects_feedevent_actor_created_at_desc_idx ON public.objects_feedevent \
                 USING btree (((index_meta ->> 'actor'::text)), created_at DESC)"
                    .to_string(),
            ),
            (
                "objects_feedevent_location_key_state_idx".to_string(),
                "CREATE INDEX objects_feedevent_location_key_state_idx ON public.objects_feedevent \
                 USING btree (((index_meta ->> 'location_key'::text)), ((index_meta ->> 'state'::text)))"
                    .to_string(),
            ),
            (
                "objects_feedevent_region_tier_created_at_desc_idx".to_string(),
                "CREATE INDEX objects_feedevent_region_tier_created_at_desc_idx ON public.objects_feedevent \
                 USING btree (((index_meta ->> 'region'::text)), (((index_meta ->> 'tier'::text))::bigint), created_at DESC)"
                    .to_string(),
            ),
            (
                "objects_feedevent_score_created_at_desc_idx".to_string(),
                "CREATE INDEX objects_feedevent_score_created_at_desc_idx ON public.objects_feedevent \
                 USING btree ((((index_meta ->> 'score'::text))::bigint), created_at DESC)"
                    .to_string(),
            ),
        ]
    );

    // Not cascaded onto the parent or any sibling partition.
    let elsewhere: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes \
         WHERE tablename <> 'objects_feedevent' \
           AND (indexdef LIKE '%''actor''%' OR indexdef LIKE '%''score''%' \
                OR indexdef LIKE '%''location_key''%')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(elsewhere, 0, "composite index leaked outside objects_feedevent");
    assert!(composite_indexes_on(&pool, "objects_post").await.is_empty());
    assert!(composite_indexes_on(&pool, "objects").await.is_empty());
}

#[tokio::test]
async fn test_init_schema_twice_leaves_composite_indexes_and_hash_alone() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let before = composite_indexes_on(&pool, "objects_feedevent").await;
    let hash_before = adapter.compute_postgres_schema_hash().await.unwrap();

    adapter.init_schema().await.unwrap();
    assert_eq!(composite_indexes_on(&pool, "objects_feedevent").await, before, "rebuilt");
    assert_eq!(adapter.compute_postgres_schema_hash().await.unwrap(), hash_before);
    let stored: String =
        sqlx::query_scalar("SELECT value FROM ousia_meta WHERE key = 'schema:composed'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(stored.ends_with(&hash_before), "stored hash {stored} != {hash_before}");
}

#[tokio::test]
async fn test_undeclared_composite_index_is_dropped_but_hand_made_ones_stay() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    // What an earlier build with `composite_index = "state"` would have left.
    sqlx::raw_sql(
        "CREATE INDEX objects_feedevent_state_idx ON objects_feedevent ((index_meta->>'state')); \
         COMMENT ON INDEX objects_feedevent_state_idx IS \
           'ousia composite_index: (index_meta->>''state'')'; \
         CREATE INDEX objects_feedevent_by_hand ON objects_feedevent (owner, updated_at);",
    )
    .execute(&pool)
    .await
    .unwrap();

    adapter.init_schema().await.unwrap();
    let names = index_names_on(&pool, "objects_feedevent").await;
    assert!(!names.contains(&"objects_feedevent_state_idx".to_string()), "{names:?}");
    assert!(names.contains(&"objects_feedevent_by_hand".to_string()), "{names:?}");
    assert_eq!(composite_indexes_on(&pool, "objects_feedevent").await.len(), 4);
}

#[tokio::test]
async fn test_changed_or_invalid_composite_index_is_rebuilt() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();
    let before = composite_indexes_on(&pool, "objects_feedevent").await;

    // `score` indexed as text, as if the field had been a String: same name,
    // different definition.
    sqlx::raw_sql(
        "DROP INDEX objects_feedevent_score_created_at_desc_idx; \
         CREATE INDEX objects_feedevent_score_created_at_desc_idx \
           ON objects_feedevent ((index_meta->>'score'), created_at DESC); \
         COMMENT ON INDEX objects_feedevent_score_created_at_desc_idx IS \
           'ousia composite_index: (index_meta->>''score''), created_at DESC';",
    )
    .execute(&pool)
    .await
    .unwrap();
    // A concurrent build that died part way leaves the index INVALID.
    sqlx::query(
        "UPDATE pg_index SET indisvalid = false \
         WHERE indexrelid = 'objects_feedevent_actor_created_at_desc_idx'::regclass",
    )
    .execute(&pool)
    .await
    .unwrap();

    adapter.init_schema().await.unwrap();
    let after = composite_indexes_on(&pool, "objects_feedevent").await;
    for ((name, def, oid), (_, def_before, oid_before)) in after.iter().zip(&before) {
        assert_eq!(def, def_before, "{name} definition not restored");
        let tampered = [
            "objects_feedevent_actor_created_at_desc_idx",
            "objects_feedevent_score_created_at_desc_idx",
        ];
        if tampered.contains(&name.as_str()) {
            assert_ne!(oid, oid_before, "{name} not rebuilt");
        } else {
            assert_eq!(oid, oid_before, "{name} rebuilt needlessly");
        }
    }
    let invalid: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_index WHERE indrelid = 'objects_feedevent'::regclass \
         AND NOT indisvalid",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(invalid, 0);
}

#[tokio::test]
async fn test_long_composite_index_names_stay_unique_and_idempotent() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let before = composite_indexes_on(&pool, "objects_longindexnames").await;
    assert_eq!(before.len(), 2, "{before:?}");
    assert_ne!(before[0].0, before[1].0);
    for (name, def, _) in &before {
        assert_eq!(name.len(), 63, "{name}");
        assert!(def.starts_with(&format!("CREATE INDEX {name} ON ")), "{def}");
    }

    adapter.init_schema().await.unwrap();
    assert_eq!(composite_indexes_on(&pool, "objects_longindexnames").await, before);
}

#[tokio::test]
async fn test_composite_index_builds_leave_the_pools_statement_timeout_alone() {
    let (_r, pool) = setup_test_db().await;
    // The build session lifts statement_timeout; the pool must not inherit it.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with((*pool.connect_options()).clone().options([("statement_timeout", "5s")]))
        .await
        .unwrap();
    PostgresAdapter::from_pool(pool.clone()).init_schema().await.unwrap();
    assert_eq!(composite_indexes_on(&pool, "objects_feedevent").await.len(), 4);

    let mut timeouts = Vec::new();
    let mut held = Vec::new();
    for _ in 0..2 {
        let mut conn = pool.acquire().await.unwrap();
        let t: String = sqlx::query_scalar("SHOW statement_timeout")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        timeouts.push(t);
        held.push(conn);
    }
    assert_eq!(timeouts, vec!["5s", "5s"]);
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
