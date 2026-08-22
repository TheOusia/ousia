use chrono::{Days, Utc};
// ledger/tests/integration_tests.rs
use ousia_ledger::{
    Asset, Balance, LedgerContext, LedgerSystem, Money, MoneyError, adapters::MemoryAdapter,
};
use std::sync::Arc;
use uuid::Uuid;

fn setup() -> (Arc<LedgerSystem>, LedgerContext, Uuid) {
    let adapter = Box::new(MemoryAdapter::new());
    let system = Arc::new(LedgerSystem::new(adapter));
    let ctx = LedgerContext::new(system.adapter_arc());
    let user = Uuid::now_v7();

    (system, ctx, user)
}

async fn create_usd_asset(system: &LedgerSystem) -> Asset {
    let usd = Asset::new("USD", 10_00, 2);
    system.adapter().create_asset(usd.clone()).await.unwrap();
    usd
}

#[tokio::test]
async fn test_mint_creates_balance() {
    let (system, ctx, user) = setup();
    let _ = create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "initial deposit".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 100_00);
    assert_eq!(balance.reserved, 0);
    assert_eq!(balance.total, 100_00);
}

#[tokio::test]
async fn test_mint_and_reserve() {
    let (system, ctx, user) = setup();
    let _ = create_usd_asset(&system).await;

    let oid = Uuid::now_v7();

    Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            100_00,
            format!("checkout-deposit:{}", oid),
            "unique-key".to_string(),
        )
        .await?;

        tx.reserve(
            "USD",
            user,
            oid,
            100_00,
            format!("checkout-reserve:{}", oid),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(user_balance.available, 0);
    assert_eq!(user_balance.reserved, 0);
    assert_eq!(user_balance.total, 0);

    let authority_balance = Balance::get("USD", oid, &ctx).await.unwrap();
    assert_eq!(authority_balance.available, 0);
    assert_eq!(authority_balance.reserved, 100_00);
    assert_eq!(authority_balance.total, 100_00);
}

#[tokio::test]
async fn test_simple_transfer() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    let _ = create_usd_asset(&system).await;

    // Mint initial balance
    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    // Transfer
    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 60_00).await?;
        let slice = money.slice(60_00)?;
        slice.transfer_to(merchant, "payment".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 40_00);
    assert_eq!(merchant_balance.available, 60_00);
}

#[tokio::test]
async fn test_transfer_with_change() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    // Mint $100
    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    // Transfer $60 (should lock $100, return $40 as change)
    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 60_00).await?;
        let slice = money.slice(60_00)?;
        slice.transfer_to(merchant, "payment".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(user_balance.available, 40_00);
}

#[tokio::test]
async fn test_multiple_slices_from_money() {
    let (system, ctx, user) = setup();
    let merchant1 = Uuid::now_v7();
    let merchant2 = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let slice1 = money.slice(60_00)?;
        let slice2 = money.slice(40_00)?;

        slice1
            .transfer_to(merchant1, "payment1".to_string())
            .await?;
        slice2
            .transfer_to(merchant2, "payment2".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let m1_balance = Balance::get("USD", merchant1, &ctx).await.unwrap();
    let m2_balance = Balance::get("USD", merchant2, &ctx).await.unwrap();
    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();

    assert_eq!(m1_balance.available, 60_00);
    assert_eq!(m2_balance.available, 40_00);
    assert_eq!(user_balance.available, 0);
}

#[tokio::test]
async fn test_slice_can_be_split() {
    let (system, ctx, user) = setup();
    let merchant1 = Uuid::now_v7();
    let merchant2 = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let mut slice = money.slice(100_00)?;

        let payment1 = slice.slice(60_00)?;
        let payment2 = slice.slice(40_00)?;

        payment1
            .transfer_to(merchant1, "payment1".to_string())
            .await?;
        payment2
            .transfer_to(merchant2, "payment2".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let m1_balance = Balance::get("USD", merchant1, &ctx).await.unwrap();
    let m2_balance = Balance::get("USD", merchant2, &ctx).await.unwrap();

    assert_eq!(m1_balance.available, 60_00);
    assert_eq!(m2_balance.available, 40_00);
}

#[tokio::test]
async fn test_burn_operation() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 60_00).await?;
        let slice = money.slice(60_00)?;
        slice.burn("fee".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 40_00);
}

#[tokio::test]
async fn test_reserve_operation() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 60_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let authority_balance = Balance::get("USD", authority, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 40_00);
    assert_eq!(authority_balance.reserved, 60_00);
}

#[tokio::test]
async fn test_settle_operation() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    let receiver = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 60_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.settle("USD", authority, receiver, 60_00, "settle".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let authority_balance = Balance::get("USD", authority, &ctx).await.unwrap();
    let receiver_balance = Balance::get("USD", receiver, &ctx).await.unwrap();

    assert_eq!(authority_balance.reserved, 0);
    assert_eq!(authority_balance.available, 0);
    assert_eq!(receiver_balance.available, 60_00);
}

#[tokio::test]
async fn test_settle_with_change() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    let receiver = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 60_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    // Settle only $40 — $20 should remain reserved for authority
    Money::atomic(&ctx, |tx| async move {
        tx.settle(
            "USD",
            authority,
            receiver,
            40_00,
            "partial_settle".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let authority_balance = Balance::get("USD", authority, &ctx).await.unwrap();
    let receiver_balance = Balance::get("USD", receiver, &ctx).await.unwrap();

    assert_eq!(authority_balance.reserved, 20_00);
    assert_eq!(receiver_balance.available, 40_00);
    assert_eq!(
        authority_balance.reserved + receiver_balance.available,
        60_00
    );
}

#[tokio::test]
async fn test_settle_insufficient_reserved() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    let receiver = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 40_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        tx.settle("USD", authority, receiver, 60_00, "settle".to_string())
            .await?;
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::InsufficientFunds)));
}

#[tokio::test]
async fn test_insufficient_funds() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 50_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let slice = money.slice(100_00)?;
        slice
            .transfer_to(Uuid::now_v7(), "payment".to_string())
            .await?;
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::InsufficientFunds)));
}

#[tokio::test]
async fn test_unconsumed_slice_error() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let _slice = money.slice(100_00)?; // Created but not consumed!
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::UnconsumedSlice)));
}

#[tokio::test]
async fn test_money_not_sliced_error() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        let _money = tx.money("USD", user, 100_00).await?; // Created but never sliced!
        Ok(())
    })
    .await;

    assert!(matches!(
        result,
        Err(MoneyError::Storage(ref msg)) if msg.contains("never sliced")
    ));
}

#[tokio::test]
async fn test_over_slice_error() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let _slice1 = money.slice(60_00)?;
        let _slice2 = money.slice(50_00)?; // 60 + 50 > 100
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::InvalidAmount)));
}

#[tokio::test]
async fn test_concurrent_transfers_double_spend_protection() {
    let (system, ctx, user) = setup();
    let merchant1 = Uuid::now_v7();
    let merchant2 = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    // Clone ctx for each task — both share the same underlying Arc<dyn LedgerAdapter>
    let ctx1 = ctx.clone();
    let ctx2 = ctx.clone();

    let handle1 = tokio::spawn(async move {
        Money::atomic(&ctx1, |tx| async move {
            let money = tx.money("USD", user, 100_00).await?;
            let slice = money.slice(100_00)?;
            slice.transfer_to(merchant1, "payment1".to_string()).await?;
            Ok(())
        })
        .await
    });

    let handle2 = tokio::spawn(async move {
        Money::atomic(&ctx2, |tx| async move {
            let money = tx.money("USD", user, 100_00).await?;
            let slice = money.slice(100_00)?;
            slice.transfer_to(merchant2, "payment2".to_string()).await?;
            Ok(())
        })
        .await
    });

    let (result1, result2) = tokio::join!(handle1, handle2);
    let result1 = result1.unwrap(); // unwrap JoinError, keep MoneyError
    let result2 = result2.unwrap();

    // Under true concurrency we don't know which wins — assert exactly one of each
    let outcomes = [&result1, &result2];
    let succeeded = outcomes.iter().filter(|r| r.is_ok()).count();
    let failed = outcomes
        .iter()
        .filter(|r| matches!(r, Err(MoneyError::InsufficientFunds)))
        .count();

    assert_eq!(succeeded, 1, "exactly one transfer should succeed");
    assert_eq!(
        failed, 1,
        "exactly one transfer should hit InsufficientFunds"
    );

    // The winner's merchant should have the full balance
    let total_received = Balance::get("USD", merchant1, &ctx)
        .await
        .unwrap()
        .available
        + Balance::get("USD", merchant2, &ctx)
            .await
            .unwrap()
            .available;

    assert_eq!(
        total_received, 100_00,
        "exactly $100 should have moved, no more"
    );
}

#[tokio::test]
async fn test_asset_decimals_conversion() {
    let usd = Asset::new("USD", 10_00, 2);
    assert_eq!(usd.decimals, 2);
    assert_eq!(usd.to_internal(100.50), 10050);
    assert_eq!(usd.to_display(10050), 100.50);
}

#[tokio::test]
async fn test_fragmentation() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await; // unit = 10_000 ($100)

    // Mint $250 should create 3 fragments: $100, $100, $50
    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 250_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 250_00);
}

#[tokio::test]
async fn test_complex_multi_recipient_payment() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    let platform = Uuid::now_v7();
    let charity = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    // Split payment: 60% merchant, 30% platform, 10% charity
    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let mut slice = money.slice(100_00)?;

        let merchant_payment = slice.slice(60_00)?;
        let platform_fee = slice.slice(30_00)?;
        let charity_donation = slice.slice(10_00)?;

        merchant_payment
            .transfer_to(merchant, "purchase".to_string())
            .await?;
        platform_fee
            .transfer_to(platform, "platform_fee".to_string())
            .await?;
        charity_donation
            .transfer_to(charity, "donation".to_string())
            .await?;

        Ok(())
    })
    .await
    .unwrap();

    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();
    let platform_balance = Balance::get("USD", platform, &ctx).await.unwrap();
    let charity_balance = Balance::get("USD", charity, &ctx).await.unwrap();
    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();

    assert_eq!(merchant_balance.available, 60_00);
    assert_eq!(platform_balance.available, 30_00);
    assert_eq!(charity_balance.available, 10_00);
    assert_eq!(user_balance.available, 0);
}

#[tokio::test]
async fn test_rollback_on_error() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let initial_balance = Balance::get("USD", user, &ctx).await.unwrap();

    // Transaction that fails mid-way
    let result = Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let mut slice = money.slice(100_00)?;

        let payment1 = slice.slice(60_00)?;
        payment1
            .transfer_to(merchant, "payment".to_string())
            .await?;

        // Intentionally fail
        return Err(MoneyError::Storage("simulated error".to_string()));
    })
    .await;

    assert!(result.is_err());

    // Balance should be unchanged
    let final_balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(final_balance.available, initial_balance.available);

    // Merchant should have received nothing
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();
    assert_eq!(merchant_balance.available, 0);
}

#[tokio::test]
async fn test_multiple_assets() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    let ngn = Asset::new("NGN", 500_000, 2); // ₦5,000 unit
    system.adapter().create_asset(ngn).await.unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "usd_deposit".to_string())
            .await?;
        tx.mint("NGN", user, 50_000_00, "ngn_deposit".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let usd_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let ngn_balance = Balance::get("NGN", user, &ctx).await.unwrap();

    assert_eq!(usd_balance.available, 100_00);
    assert_eq!(ngn_balance.available, 50_000_00);
}

#[tokio::test]
async fn test_fetch_transactions() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 60_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let authority_balance = Balance::get("USD", authority, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 40_00);
    assert_eq!(authority_balance.reserved, 60_00);

    let transactions = system
        .adapter()
        .get_transactions_for_owner(
            user,
            &[
                Utc::now().checked_sub_days(Days::new(1)).unwrap(),
                Utc::now(),
            ],
        )
        .await
        .unwrap();

    assert_eq!(transactions.len(), 2);
}

// ── Fragmentation & Consolidation Tests ──────────────────────────────────────
//
// These tests verify the smart fragmentation behaviour introduced in:
//   - fragment_amount_smart (unit as soft floor, max_fragments as hard cap)
//   - mint_internal_tx_with_max_fragments (change consolidation via burned_count)

/// Small amount below unit: $5 with unit=$10 → 1 fragment, balance correct.
#[tokio::test]
async fn test_fragmentation_small_amount_below_unit() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await; // unit = 10_00 ($10)

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 5_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 5_00);
}

/// Exact unit boundary: amount == unit → 1 fragment.
#[tokio::test]
async fn test_fragmentation_exact_unit() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await; // unit = 10_00

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 10_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 10_00);
}

/// Multiple fragments: $25 with unit=$10 → total still $25.
#[tokio::test]
async fn test_fragmentation_multiple_fragments_correct_total() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await; // unit = 10_00

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 25_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 25_00);
}

/// Value is conserved through a transfer with odd change amount.
/// Mint $100, spend $37 → user $63, merchant $37, total $100.
#[tokio::test]
async fn test_fragmentation_value_preserved_through_transfer() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 37_00).await?;
        let slice = money.slice(37_00)?;
        slice.transfer_to(merchant, "payment".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 63_00);
    assert_eq!(merchant_balance.available, 37_00);
    assert_eq!(user_balance.available + merchant_balance.available, 100_00);
}

// ── change consolidation via burned_count ─────────────────────────────────────

/// Repeated small spends consolidate change correctly each time.
/// After 10 x $1 spends from a $500 balance, total is still conserved.
#[tokio::test]
async fn test_change_consolidation_repeated_small_spends() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 500_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    for _ in 0..10 {
        Money::atomic(&ctx, |tx| async move {
            let money = tx.money("USD", user, 1_00).await?;
            let slice = money.slice(1_00)?;
            slice.transfer_to(merchant, "payment".to_string()).await?;
            Ok(())
        })
        .await
        .unwrap();
    }

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 490_00);
    assert_eq!(merchant_balance.available, 10_00);
    assert_eq!(user_balance.available + merchant_balance.available, 500_00);
}

/// Full spend (change = 0) works correctly — no phantom change VO minted.
#[tokio::test]
async fn test_full_spend_no_change() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 100_00).await?;
        let slice = money.slice(100_00)?;
        slice.transfer_to(merchant, "payment".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 0);
    assert_eq!(merchant_balance.available, 100_00);

    // Re-mint after full drain should work cleanly
    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 50_00, "re-deposit".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(user_balance.available, 50_00);
}

/// Burn operation: change consolidation applies on the sender side.
/// Mint $100, burn $60 → $40 remains.
#[tokio::test]
async fn test_change_consolidation_after_burn() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 60_00).await?;
        let slice = money.slice(60_00)?;
        slice.burn("fee".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 40_00);
}

/// Reserve operation: change consolidation applies on the sender side.
/// Mint $100, reserve $60 → sender $40 available, authority $60 reserved.
#[tokio::test]
async fn test_change_consolidation_after_reserve() {
    let (system, ctx, user) = setup();
    let authority = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.reserve("USD", user, authority, 60_00, "escrow".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let authority_balance = Balance::get("USD", authority, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 40_00);
    assert_eq!(authority_balance.reserved, 60_00);
    assert_eq!(user_balance.available + authority_balance.reserved, 100_00);
}

/// Total supply is conserved across a complex 3-way split with change.
/// Mint $1000, split $900 three ways → user $100, recipients get their amounts.
#[tokio::test]
async fn test_value_conservation_complex_split_with_change() {
    let (system, ctx, user) = setup();
    let r1 = Uuid::now_v7();
    let r2 = Uuid::now_v7();
    let r3 = Uuid::now_v7();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 1_000_00, "deposit".to_string())
            .await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 900_00).await?;
        let mut slice = money.slice(900_00)?;

        let p1 = slice.slice(500_00)?;
        let p2 = slice.slice(250_00)?;
        let p3 = slice.slice(150_00)?;

        p1.transfer_to(r1, "payment1".to_string()).await?;
        p2.transfer_to(r2, "payment2".to_string()).await?;
        p3.transfer_to(r3, "payment3".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let r1_balance = Balance::get("USD", r1, &ctx).await.unwrap();
    let r2_balance = Balance::get("USD", r2, &ctx).await.unwrap();
    let r3_balance = Balance::get("USD", r3, &ctx).await.unwrap();

    assert_eq!(user_balance.available, 100_00);
    assert_eq!(r1_balance.available, 500_00);
    assert_eq!(r2_balance.available, 250_00);
    assert_eq!(r3_balance.available, 150_00);

    let total =
        user_balance.available + r1_balance.available + r2_balance.available + r3_balance.available;
    assert_eq!(total, 1_000_00);
}

/// Interleaved mints and spends: balance is always correct, no value leaks.
#[tokio::test]
async fn test_interleaved_mints_and_spends_balance_integrity() {
    let (system, ctx, user) = setup();
    let merchant = Uuid::now_v7();
    create_usd_asset(&system).await;

    let ops: &[(u64, u64)] = &[
        (200_00, 50_00),
        (100_00, 75_00),
        (300_00, 100_00),
        (50_00, 25_00),
    ];

    let mut expected_user: i64 = 0;
    let mut expected_merchant: i64 = 0;

    for &(mint_amount, spend_amount) in ops {
        Money::atomic(&ctx, |tx| async move {
            tx.mint("USD", user, mint_amount, "deposit".to_string())
                .await?;
            Ok(())
        })
        .await
        .unwrap();

        Money::atomic(&ctx, |tx| async move {
            let money = tx.money("USD", user, spend_amount).await?;
            let slice = money.slice(spend_amount)?;
            slice.transfer_to(merchant, "payment".to_string()).await?;
            Ok(())
        })
        .await
        .unwrap();

        expected_user += mint_amount as i64 - spend_amount as i64;
        expected_merchant += spend_amount as i64;
    }

    let user_balance = Balance::get("USD", user, &ctx).await.unwrap();
    let merchant_balance = Balance::get("USD", merchant, &ctx).await.unwrap();

    assert_eq!(user_balance.available, expected_user as u64);
    assert_eq!(merchant_balance.available, expected_merchant as u64);
}

// ── Idempotent mint/burn (MemoryAdapter) ──────────────────────────────────────
//
// `test_mint_and_reserve` above already exercises the happy path for
// `mint_idempotent`, but nothing here previously covered the duplicate-key
// rejection path or `burn_idempotent` at all — the equivalent gap that let
// a Postgres-only bug (idempotency-key row inserted before the transaction
// row it FK-references) ship unnoticed. `MemoryAdapter` has no real foreign
// keys so it never hit that specific bug, but it had its own: the duplicate
// error carried the new (never-stored) transaction's id instead of the
// original one's, and it stored the idempotency key differently from the
// Postgres adapter (raw key vs. hash) — covered below.

#[tokio::test]
async fn test_mint_idempotent_duplicate_key_rejected_and_not_double_minted() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            100_00,
            "deposit".to_string(),
            "order-456".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            100_00,
            "deposit retry".to_string(),
            "order-456".to_string(),
        )
        .await?;
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::DuplicateIdempotencyKey(_))));

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(balance.available, 100_00);
}

#[tokio::test]
async fn test_burn_idempotent_duplicate_key_rejected_and_not_double_burned() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    })
    .await
    .unwrap();

    Money::atomic(&ctx, |tx| async move {
        tx.burn_idempotent(
            "USD",
            user,
            30_00,
            "fee".to_string(),
            "fee-001".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        tx.burn_idempotent(
            "USD",
            user,
            30_00,
            "fee retry".to_string(),
            "fee-001".to_string(),
        )
        .await?;
        Ok(())
    })
    .await;

    assert!(matches!(result, Err(MoneyError::DuplicateIdempotencyKey(_))));

    let balance = Balance::get("USD", user, &ctx).await.unwrap();
    assert_eq!(
        balance.available, 70_00,
        "only the first burn should have taken effect"
    );
}

/// The duplicate-key error used to carry the id of the new, never-persisted
/// transaction instead of the transaction that actually owns the key.
#[tokio::test]
async fn test_duplicate_idempotency_key_error_references_original_transaction() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            50_00,
            "deposit".to_string(),
            "order-789".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let original = system
        .adapter()
        .get_transaction_by_idempotency_key("order-789")
        .await
        .unwrap();

    let result = Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            50_00,
            "deposit retry".to_string(),
            "order-789".to_string(),
        )
        .await?;
        Ok(())
    })
    .await;

    match result {
        Err(MoneyError::DuplicateIdempotencyKey(id)) => {
            assert_eq!(
                id, original.id,
                "error should reference the original transaction, not a phantom one"
            );
        }
        other => panic!("expected DuplicateIdempotencyKey, got {other:?}"),
    }
}

/// `get_transaction(id)` on the Postgres adapter used to query a nonexistent
/// `assets` table (should have been `ledger_assets`) and fail unconditionally.
/// `MemoryAdapter` never had that bug, but this keeps both adapters covered
/// with the same lookup-by-id path.
#[tokio::test]
async fn test_get_transaction_by_id() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            25_00,
            "deposit".to_string(),
            "lookup-1".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let by_key = system
        .adapter()
        .get_transaction_by_idempotency_key("lookup-1")
        .await
        .unwrap();

    let by_id = system.adapter().get_transaction(by_key.id).await.unwrap();
    assert_eq!(by_id.id, by_key.id);
    assert_eq!(by_id.code, "USD");
    assert_eq!(by_id.minted_amount, 25_00);
}

/// The Postgres adapter only ever persists the blake3 hash of an idempotency
/// key (`ledger_transaction_idempotency_keys.key`), never the raw value —
/// `MemoryAdapter` used to store and return the raw key instead, so the two
/// adapters disagreed on what `Transaction.idempotency_key` contains for the
/// same logical operation. It should be the hash on both.
#[tokio::test]
async fn test_memory_adapter_idempotency_key_is_hashed_like_postgres() {
    let (system, ctx, user) = setup();
    create_usd_asset(&system).await;

    Money::atomic(&ctx, |tx| async move {
        tx.mint_idempotent(
            "USD",
            user,
            10_00,
            "deposit".to_string(),
            "raw-key-should-not-round-trip".to_string(),
        )
        .await?;
        Ok(())
    })
    .await
    .unwrap();

    let stored = system
        .adapter()
        .get_transaction_by_idempotency_key("raw-key-should-not-round-trip")
        .await
        .unwrap();

    let key = stored
        .idempotency_key
        .expect("idempotency_key should be set");
    assert_ne!(
        key, "raw-key-should-not-round-trip",
        "stored idempotency_key must be the hash, not the raw key"
    );
    assert_eq!(key.len(), 64, "blake3 hex digest is 64 chars");
}

// ── Asset decimal conversion ──────────────────────────────────────────────────

/// `to_internal` used to truncate `display_amount * 10^decimals` straight to
/// `u64` — for values where that multiplication isn't exactly representable in
/// f64 (e.g. `19.99 * 100 == 1998.9999999999998`), truncation silently lost a
/// cent. It must round to the nearest integer instead.
#[tokio::test]
async fn test_asset_to_internal_rounds_against_float_error() {
    let usd = Asset::new("USD", 10_00, 2);
    assert_eq!(usd.to_internal(19.99), 1999);
    assert_eq!(usd.to_internal(0.29), 29);
}

// ── Account registry ─────────────────────────────────────────────────────

mod account_registry {
    use super::{create_usd_asset, setup};
    use ousia_ledger::{Account, AccountQuery, Money};
    use uuid::Uuid;

    /// The registry is descriptive, not a gate. Money must move for an
    /// owner nobody ever registered — every balance that existed before
    /// the registry did depends on this.
    #[tokio::test]
    async fn an_unregistered_owner_still_holds_and_moves_money() {
        let (system, ctx, user) = setup();
        let _ = create_usd_asset(&system).await;

        Money::atomic(&ctx, |tx| async move {
            tx.mint("USD", user, 100_00, "deposit".to_string()).await
        })
        .await
        .unwrap();

        assert_eq!(ctx.balance("USD", user).await.unwrap().available, 100_00);
        assert!(
            ctx.account(user).await.unwrap().is_none(),
            "an unregistered owner simply has no description"
        );
    }

    #[tokio::test]
    async fn register_then_resolve_by_owner_and_by_key() {
        let (_system, ctx, _user) = setup();
        let owner = Uuid::now_v7();

        let registered = ctx
            .register_account(&Account::new(
                owner,
                "partner:fastlink",
                "Fast Link",
                "partner",
            ))
            .await
            .unwrap();
        assert_eq!(registered.owner, owner);
        assert!(!registered.is_archived());

        assert_eq!(ctx.account(owner).await.unwrap().unwrap().key, "partner:fastlink");
        assert_eq!(
            ctx.account_by_key("partner:fastlink")
                .await
                .unwrap()
                .unwrap()
                .owner,
            owner
        );
        assert!(ctx.account_by_key("partner:nobody").await.unwrap().is_none());
    }

    /// Registration runs at every application start, so it has to be a
    /// true upsert — and must not keep resetting the account's age.
    #[tokio::test]
    async fn re_registering_updates_in_place_and_keeps_created_at() {
        let (_system, ctx, _user) = setup();
        let owner = Uuid::now_v7();

        let first = ctx
            .register_account(&Account::new(owner, "partner:fastlink", "Fastlink", "partner"))
            .await
            .unwrap();

        let second = ctx
            .register_account(
                &Account::new(owner, "partner:fastlink", "Fast Link NG", "partner")
                    .with_metadata(serde_json::json!({ "account_ref": "MG-4471" })),
            )
            .await
            .unwrap();

        assert_eq!(second.label, "Fast Link NG");
        assert_eq!(second.metadata["account_ref"], "MG-4471");
        assert_eq!(second.created_at, first.created_at);
        assert_eq!(ctx.accounts(&AccountQuery::new()).await.unwrap().len(), 1);
    }

    /// One key, one account, for the life of the ledger — otherwise the
    /// key is worthless as a durable identity.
    #[tokio::test]
    async fn a_key_cannot_be_stolen_by_another_owner() {
        let (_system, ctx, _user) = setup();

        ctx.register_account(&Account::new(
            Uuid::now_v7(),
            "mealgro-platform",
            "Platform",
            "system",
        ))
        .await
        .unwrap();

        let clash = ctx
            .register_account(&Account::new(
                Uuid::now_v7(),
                "mealgro-platform",
                "Platform (copy)",
                "system",
            ))
            .await;

        assert!(clash.is_err(), "a second owner must not claim a live key");
    }

    #[tokio::test]
    async fn listing_filters_by_kind_and_hides_archived_by_default() {
        let (_system, ctx, _user) = setup();
        let platform = Uuid::now_v7();
        let partner_a = Uuid::now_v7();
        let partner_b = Uuid::now_v7();

        ctx.register_account(&Account::new(platform, "mealgro-platform", "Platform", "system"))
            .await
            .unwrap();
        ctx.register_account(&Account::new(partner_a, "partner:a", "A", "partner"))
            .await
            .unwrap();
        ctx.register_account(&Account::new(partner_b, "partner:b", "B", "partner"))
            .await
            .unwrap();

        assert_eq!(ctx.accounts(&AccountQuery::new()).await.unwrap().len(), 3);
        assert_eq!(
            ctx.accounts(&AccountQuery::new().of_kind("partner"))
                .await
                .unwrap()
                .len(),
            2
        );

        ctx.archive_account(partner_b).await.unwrap();
        assert_eq!(
            ctx.accounts(&AccountQuery::new().of_kind("partner"))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            ctx.accounts(&AccountQuery::new().of_kind("partner").including_archived())
                .await
                .unwrap()
                .len(),
            2
        );

        // Archiving never hides an account from a direct lookup — a
        // retired account's money still has to be explainable.
        assert!(ctx.account(partner_b).await.unwrap().unwrap().is_archived());
        assert!(ctx.account_by_key("partner:b").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn archive_and_unarchive_are_idempotent() {
        let (_system, ctx, _user) = setup();
        let owner = Uuid::now_v7();
        ctx.register_account(&Account::new(owner, "partner:a", "A", "partner"))
            .await
            .unwrap();

        ctx.archive_account(owner).await.unwrap();
        let first = ctx.account(owner).await.unwrap().unwrap().archived_at;
        ctx.archive_account(owner).await.unwrap();
        assert_eq!(
            ctx.account(owner).await.unwrap().unwrap().archived_at,
            first,
            "re-archiving must not move the retirement date"
        );

        ctx.unarchive_account(owner).await.unwrap();
        ctx.unarchive_account(owner).await.unwrap();
        assert!(!ctx.account(owner).await.unwrap().unwrap().is_archived());
    }

    /// A boot-time re-register must not silently resurrect an account
    /// someone deliberately retired.
    #[tokio::test]
    async fn re_registering_does_not_resurrect_an_archived_account() {
        let (_system, ctx, _user) = setup();
        let owner = Uuid::now_v7();
        ctx.register_account(&Account::new(owner, "partner:a", "A", "partner"))
            .await
            .unwrap();
        ctx.archive_account(owner).await.unwrap();

        ctx.register_account(&Account::new(owner, "partner:a", "A", "partner"))
            .await
            .unwrap();

        assert!(ctx.account(owner).await.unwrap().unwrap().is_archived());
    }

    /// The listing this whole type exists for: every internal account and
    /// what it holds, in one call. Impossible before — every ledger read
    /// was keyed *by* owner, so there was no way to enumerate owners.
    #[tokio::test]
    async fn account_balances_lists_accounts_with_their_money() {
        let (system, ctx, _user) = setup();
        let _ = create_usd_asset(&system).await;

        let partner_a = Uuid::now_v7();
        let partner_b = Uuid::now_v7();
        ctx.register_account(&Account::new(partner_a, "partner:a", "A", "partner"))
            .await
            .unwrap();
        ctx.register_account(&Account::new(partner_b, "partner:b", "B", "partner"))
            .await
            .unwrap();

        Money::atomic(&ctx, |tx| async move {
            tx.mint("USD", partner_a, 700_00, "owed".to_string()).await
        })
        .await
        .unwrap();

        let listed = ctx
            .account_balances("USD", &AccountQuery::new().of_kind("partner"))
            .await
            .unwrap();

        assert_eq!(listed.len(), 2, "an account with no money is still an account");

        let a = listed.iter().find(|r| r.account.owner == partner_a).unwrap();
        let b = listed.iter().find(|r| r.account.owner == partner_b).unwrap();
        assert_eq!(a.balance.available, 700_00);
        assert_eq!(b.balance.available, 0);
    }

    #[tokio::test]
    async fn account_balances_reports_reserved_separately() {
        let (system, ctx, _user) = setup();
        let _ = create_usd_asset(&system).await;
        let partner = Uuid::now_v7();
        let payout = Uuid::now_v7();
        ctx.register_account(&Account::new(partner, "partner:a", "A", "partner"))
            .await
            .unwrap();

        Money::atomic(&ctx, |tx| async move {
            tx.mint("USD", partner, 700_00, "owed".to_string()).await?;
            tx.reserve("USD", partner, payout, 200_00, "payout".to_string())
                .await
        })
        .await
        .unwrap();

        let listed = ctx
            .account_balances("USD", &AccountQuery::new().of_kind("partner"))
            .await
            .unwrap();
        let a = &listed[0];
        assert_eq!(a.balance.available, 500_00);
        assert_eq!(a.balance.total, 500_00);
    }

    #[tokio::test]
    async fn listing_paginates() {
        let (_system, ctx, _user) = setup();
        for i in 0..5 {
            ctx.register_account(&Account::new(
                Uuid::now_v7(),
                format!("partner:{i}"),
                format!("Partner {i}"),
                "partner",
            ))
            .await
            .unwrap();
        }

        assert_eq!(ctx.accounts(&AccountQuery::new().limit(2)).await.unwrap().len(), 2);
        assert_eq!(
            ctx.accounts(&AccountQuery::new().limit(2).offset(4))
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
