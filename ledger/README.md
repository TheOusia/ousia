# Ledger

A high-performance, double-entry ledger system for Rust with two-phase execution, value object fragmentation, and a fluent payment-splitting API.

Designed to be used standalone or embedded in [Ousia](../README.md) via the `ledger` feature.

---

## Table of Contents

- [Architecture](#architecture)
- [Installation](#installation)
- [Setup](#setup)
- [Assets](#assets)
- [Minting and Burning](#minting-and-burning)
- [Transfers](#transfers)
- [The Slice API](#the-slice-api)
- [Payment Splits](#payment-splits)
- [Reserve (Escrow)](#reserve-escrow)
- [Balances](#balances)
- [Transactions](#transactions)
- [Value Objects and Fragmentation](#value-objects-and-fragmentation)
- [Error Handling](#error-handling)
- [Implementing a Production Adapter](#implementing-a-production-adapter)
- [Running Tests](#running-tests)

---

## Architecture

### Two-Phase Execution

Every money operation goes through two phases:

```
Phase 1 — Planning (pure memory, no DB locks)
  ├── Build ExecutionPlan (list of Operations)
  ├── Validate all slices are consumed
  └── Calculate required locks

Phase 2 — Execution (single DB transaction, microsecond locks)
  ├── BEGIN TRANSACTION
  ├── SELECT FOR UPDATE on required value objects
  ├── Execute all operations
  ├── Burn locked value objects
  ├── Mint change (over-selection handled automatically)
  └── COMMIT  (or ROLLBACK on any error)
```

Key properties:

- **No early locking** — DB locks are held only during execution, not during business logic
- **Double-spend protection** — atomic execution with SELECT FOR UPDATE
- **Automatic change** — if you select $100 worth of value objects but only spend $60, $40 is minted back as change
- **Rollback on any failure** — a planning error or a partial `Err` in the closure leaves balances unchanged

### Value Objects

Money exists as immutable `ValueObject` fragments. Each fragment has an `amount`, an `owner`, an `asset`, and a `state` (`Alive`, `Reserved`, or `Burned`).

```
State transitions:
  Alive ──→ Reserved ──→ Alive
  Alive ──→ Burned
  Reserved ──→ Burned
  Burned ──→ (terminal — no transitions)
```

Fragmentation keeps individual value objects at or below the asset's configured `unit` size, preventing unbounded accumulation.

---

## Installation

```toml
ledger = "1.0"
```

## \*\* If using ledger as standalone, the MemoryAdapter is for testing and shouldn't be used in Production as it doesn't persist data. You should implement XLedgerAdapter where X corresponds to the database you're using.

```rust
pub struct PostgresAdapter {
    pub(crate) pool: PgPool,
}

impl PostgresLedgerAdapter for PostgresAdapter
where
    PostgresAdapter: Send + Sync,
{
    fn get_pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }
}
```

## Setup

```rust
use ledger::{adapters::MemoryAdapter, Asset, LedgerContext, LedgerSystem, Money};
use std::sync::Arc;

// 1. Create a system with an adapter
let adapter = Box::new(MemoryAdapter::new());
let system  = Arc::new(LedgerSystem::new(adapter));
let ctx     = LedgerContext::new(system.adapter_arc());

// 2. Register assets before any money operations
let usd = Asset::new("USD", 10_000, 2);  // unit = $100, 2 decimal places
system.adapter().create_asset(usd).await?;
```

In production you implement `LedgerAdapter` for your database (see [Implementing a Production Adapter](#implementing-a-production-adapter)).

---

## Assets

Assets describe a currency or token and control fragmentation and display precision.

```rust
// Fiat — unit sized to practical transaction amounts
let usd = Asset::new("USD", 10_000, 2);   // unit = $100.00, display as X.XX
let ngn = Asset::new("NGN", 500_000, 2);  // unit = ₦5,000.00

// Crypto — unit sized to typical on-chain amounts
let eth = Asset::new("ETH", 10_000_000_000_000_000, 18); // unit = 0.01 ETH
let btc = Asset::new("BTC", 10_000_000, 8);              // unit = 0.1 BTC
```

**`unit`** — maximum amount per `ValueObject`. Large balances are split into multiple fragments, each at most `unit` in size. Smaller units mean more fragments; larger units mean fewer, bigger objects.

**`decimals`** — display conversion only, does not affect internal amounts.

```rust
let usd = Asset::new("USD", 10_000, 2);

// Display conversion helpers
let internal = usd.to_internal(100.50); // → 10050 (internal units)
let display  = usd.to_display(10050);   // → 100.50 (f64)
```

All internal amounts are `u64`. The convention in tests and examples is to write them with underscores for readability: `100_00` means 100 dollars (10000 cents).

---

## Minting and Burning

Use `Money::atomic` for all state-changing operations. The closure receives a `TransactionContext` and returns `Result<(), MoneyError>`. The plan is only executed if the closure returns `Ok(())` and all slices are consumed.

```rust
// Mint (create money from nothing — e.g., a deposit webhook)
Money::atomic(&ctx, |tx| async move {
    tx.mint("USD", user_id, 500_00, "deposit".to_string()).await?;
    Ok(())
}).await?;

// Burn via a slice (destroy money — e.g., a fee deduction)
Money::atomic(&ctx, |tx| async move {
    let money = tx.money("USD", user_id, 50_00).await?;
    let slice = money.slice(50_00)?;
    slice.burn("platform_fee".to_string()).await?;
    Ok(())
}).await?;

// Direct burn (without the slice API)
Money::atomic(&ctx, |tx| async move {
    tx.burn("USD", user_id, 50_00, "platform_fee".to_string()).await?;
    Ok(())
}).await?;
```

---

## Transfers

```rust
Money::atomic(&ctx, |tx| async move {
    // Select money from sender (validates sufficient balance in the planning phase)
    let money = tx.money("USD", sender_id, 200_00).await?;

    // Slice the exact amount to transfer
    let slice = money.slice(200_00)?;

    // Transfer to recipient
    slice.transfer_to(recipient_id, "payment".to_string()).await?;

    Ok(())
}).await?;
```

`tx.money(asset, owner, amount)` checks the current balance in the planning phase and returns `InsufficientFunds` early if the sender can't cover `amount`. The actual value objects are only locked and burned during execution.

---

## The Slice API

A `Money` represents funds you intend to spend from a particular owner. A `MoneySlice` is a portion of that money earmarked for a specific operation.

**Rules enforced at commit time:**

1. Every `Money` created via `tx.money()` must have its full amount sliced out — leaving money unsliced returns `Storage("never sliced")`.
2. Every `MoneySlice` must be consumed (via `transfer_to` or `burn`) — an unconsumed slice returns `UnconsumedSlice`.
3. You cannot slice more than the remaining amount on a `Money` — returns `InvalidAmount`.

```rust
let money = tx.money("USD", user_id, 100_00).await?;

// Slice from Money
let slice_a = money.slice(60_00)?;   // 60 taken, 40 remaining
let slice_b = money.slice(40_00)?;   // 40 taken, 0 remaining ✓

// Sub-slice from an existing slice
let mut big_slice = money.slice(100_00)?;
let part1 = big_slice.slice(70_00)?;  // big_slice now holds 30
let part2 = big_slice.slice(30_00)?;  // big_slice now holds 0

// Consume each slice
part1.transfer_to(merchant, "sale".to_string()).await?;
part2.transfer_to(platform, "fee".to_string()).await?;
big_slice.burn("remainder".to_string()).await?;  // consume leftover (0 is valid)
```

---

## Payment Splits

The slice API makes multi-party payment splits ergonomic and correct:

```rust
Money::atomic(&ctx, |tx| async move {
    let money = tx.money("USD", buyer_id, 100_00).await?;
    let mut slice = money.slice(100_00)?;

    // 70/20/10 split
    let seller_cut   = slice.slice(70_00)?;
    let platform_fee = slice.slice(20_00)?;
    let charity      = slice.slice(10_00)?;

    seller_cut.transfer_to(seller_id,   "sale".to_string()).await?;
    platform_fee.transfer_to(platform_id, "fee".to_string()).await?;
    charity.transfer_to(charity_id,     "donation".to_string()).await?;

    Ok(())
}).await?;
```

All three transfers are executed in a single atomic database transaction. Either all succeed or none do.

---

## Reserve (Escrow)

Reserving moves funds into a `Reserved` state under a designated authority. The funds are locked against the original owner (reducing their `available` balance) but credited as `reserved` to the authority.

```rust
// Reserve $200 from buyer, held by marketplace escrow
Money::atomic(&ctx, |tx| async move {
    tx.reserve(
        "USD",
        buyer_id,
        marketplace_id,   // authority
        200_00,
        "order_escrow".to_string(),
    ).await?;
    Ok(())
}).await?;

// Balance after:
// buyer.available = original - 200_00
// marketplace.reserved = 200_00
```

Releasing a reservation (e.g., after order completion) is handled by your application — burn the reserved amount or transfer it onward.

---

## Balances

```rust
use ledger::Balance;

let balance = Balance::get("USD", user_id, &ctx).await?;

println!("Available: {}", balance.available);  // spendable
println!("Reserved:  {}", balance.reserved);   // locked in escrow
println!("Total:     {}", balance.total);      // available + reserved
```

You can also query balance inside a transaction:

```rust
Money::atomic(&ctx, |tx| async move {
    let balance = tx.get_balance("USD", user_id).await?;
    if balance.available < 100_00 {
        return Err(MoneyError::InsufficientFunds);
    }
    // ...
    Ok(())
}).await?;
```

---

## Transactions

Every operation records a `Transaction` with `sender`, `receiver`, `burned_amount`, `minted_amount`, and `metadata`.

```rust
let tx = system.adapter().get_transaction(tx_id).await?;
println!("From:    {:?}", tx.sender);
println!("To:      {:?}", tx.receiver);
println!("Burned:  {}", tx.burned_amount);
println!("Minted:  {}", tx.minted_amount);
println!("Note:    {}", tx.metadata);
println!("Time:    {}", tx.created_at);
```

---

## Value Objects and Fragmentation

Behind the scenes, a balance of $500 with a `unit` of $100 is stored as five `ValueObject` rows, each with `amount = 100_00`. When you spend $200, the system selects two (or more) value objects that cover the amount, burns them, executes the transfers, and mints the change back.

This fragmentation prevents any single value object from growing unboundedly and keeps lock contention low — you're locking small, discrete fragments rather than a single mutable balance row.

```rust
// Asset with unit = 100_00 ($100)
// Mint $250 → creates three fragments: $100 + $100 + $50
tx.mint("USD", user_id, 250_00, "deposit".to_string()).await?;
```

You never interact with `ValueObject` directly in normal usage. The adapter handles selection, locking, and change minting transparently.

---

## Error Handling

```rust
use ledger::MoneyError;

match result {
    Err(MoneyError::InsufficientFunds)           => { /* not enough spendable balance */ }
    Err(MoneyError::AssetNotFound(code))         => { /* unknown asset code */ }
    Err(MoneyError::InvalidAmount)               => { /* zero or overflowing amount */ }
    Err(MoneyError::UnconsumedSlice)             => { /* slice created but not consumed */ }
    Err(MoneyError::Storage(msg))                => { /* DB or logic error, see msg */ }
    Err(MoneyError::DuplicateIdempotencyKey(id)) => { /* key already used */ }
    Err(MoneyError::TransactionNotFound)         => { /* tx_id not in store */ }
    Err(MoneyError::Conflict(msg))               => { /* concurrent modification */ }
    Ok(())                                       => { /* success */ }
}
```

The `Storage("Money created but never sliced")` variant fires when `tx.money()` is called but `.slice()` is never invoked before the closure returns. Always slice (and consume) every `Money` you create.

---

## Implementing a Production Adapter

The `MemoryAdapter` is provided for testing. For production, implement `LedgerAdapter` for your database:

```rust
use ledger::{Asset, Balance, ExecutionPlan, LedgerAdapter, MoneyError, Transaction};
use async_trait::async_trait;
use uuid::Uuid;

pub struct PostgresLedgerAdapter {
    pool: sqlx::PgPool,
}

#[async_trait]
impl LedgerAdapter for PostgresLedgerAdapter {
    /// Core method — called once per atomic block.
    ///
    /// `locks` contains (asset_id, owner_id, amount) for every owner whose
    /// value objects need to be locked. Use SELECT FOR UPDATE here.
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, u64)],
    ) -> Result<(), MoneyError> {
        let mut tx = self.pool.begin().await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;

        // 1. Lock value objects
        for (asset_id, owner_id, amount) in locks {
            sqlx::query(
                "SELECT id FROM value_objects
                 WHERE asset = $1 AND owner = $2 AND state = 'alive'
                 ORDER BY amount ASC
                 FOR UPDATE"
            )
            .bind(asset_id)
            .bind(owner_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| MoneyError::Storage(e.to_string()))?;
        }

        // 2. Execute operations from plan.operations()
        for op in plan.operations() {
            // match on Operation::Mint, Burn, Transfer, Reserve, RecordTransaction
            // and apply to database
        }

        // 3. Commit
        tx.commit().await.map_err(|e| MoneyError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn begin_transaction(&self)    -> Result<(), MoneyError> { Ok(()) } // handled inside execute_plan
    async fn commit_transaction(&self)   -> Result<(), MoneyError> { Ok(()) }
    async fn rollback_transaction(&self) -> Result<(), MoneyError> { Ok(()) }

    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError> {
        // SELECT SUM(amount) FROM value_objects WHERE ...
        todo!()
    }

    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
        todo!()
    }

    async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError> {
        todo!()
    }

    async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError> {
        todo!()
    }
}
```

The critical contract is `execute_plan`: it receives the complete `ExecutionPlan` and the lock requirements. Lock, execute, handle change, commit — all in one DB transaction.

---

## Running Tests

```bash
# Unit + integration tests (in-memory adapter)
cargo test

# With verbose output
cargo test -- --nocapture
```

### Test Coverage

**Core operations:** mint, transfer, transfer with change, multiple slices, sub-slices, burn, reserve

**Error paths:** insufficient funds, unconsumed slice, money never sliced, over-slice, double-spend

**Advanced:** asset decimal conversion, fragmentation, multi-recipient payment splits, rollback on error, multiple assets, concurrent transfers

---

## License

MIT
