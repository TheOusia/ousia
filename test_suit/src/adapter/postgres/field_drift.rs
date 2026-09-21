use super::*;

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
async fn test_rename_alias_reads_old_key_and_rewrites_on_save() {
    let (_r, pool) = setup_test_db().await;
    let adapter = PostgresAdapter::from_pool(pool.clone());
    adapter.init_schema().await.unwrap();

    let id = uuid::Uuid::now_v7();
    let mut legacy = std::collections::BTreeMap::new();
    legacy.insert("handle", "alice");
    sqlx::query(
        "INSERT INTO objects (id, type, owner, created_at, updated_at, data, index_meta) \
         VALUES ($1, 'Profile', $2, now(), now(), $3, '{}'::jsonb)",
    )
    .bind(id)
    .bind(uuid::Uuid::nil())
    .bind(rmp_serde::to_vec_named(&legacy).unwrap())
    .execute(&pool)
    .await
    .unwrap();

    // an aliased key is not drift: it is read, not lost
    let warnings = adapter.check_field_drift().await.unwrap();
    assert!(
        !warnings.iter().any(|w| w.contains("object:Profile")),
        "unexpected drift warning: {warnings:?}"
    );

    let engine = Engine::new(Box::new(adapter));
    let mut profile: Profile = engine.fetch_object(id).await.unwrap().unwrap();
    assert_eq!(profile.display, "alice");

    // saving re-encodes under the current name
    engine.update_object(&mut profile).await.unwrap();
    let data: Vec<u8> = sqlx::query_scalar("SELECT data FROM objects WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let stored: std::collections::BTreeMap<String, String> = rmp_serde::from_slice(&data).unwrap();
    assert_eq!(stored.get("display").map(String::as_str), Some("alice"));
    assert!(!stored.contains_key("handle"));
}

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
