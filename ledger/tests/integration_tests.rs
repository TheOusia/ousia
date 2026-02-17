// ledger/tests/integration_tests.rs
use ledger::{
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
        .get_transactions_for_owner(user)
        .await
        .unwrap();

    println!("{:#?}", transactions);
}
