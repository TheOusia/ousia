use super::*;

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
