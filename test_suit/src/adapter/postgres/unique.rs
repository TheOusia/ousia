use super::*;

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
