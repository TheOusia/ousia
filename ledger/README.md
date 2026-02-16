# Ledger

A high-performance, double-entry ledger system with two-phase execution for maximum throughput.

## Architecture

### Two-Phase Execution

1. **Planning Phase** (Pure Memory)
   - No database locks
   - Build execution plan
   - Validate slices consumed

2. **Execution Phase** (Single Transaction)
   - BEGIN TRANSACTION
   - Lock only what's needed
   - Execute all operations
   - Handle change automatically
   - COMMIT

### Key Features

- ✅ **No early locking** - Locks held for microseconds only
- ✅ **Double-spend protection** - Atomic execution with validation
- ✅ **Value object pattern** - Immutable money fragments
- ✅ **Automatic change handling** - Over-selection handled transparently
- ✅ **Multi-asset support** - USD, NGN, ETH, BTC, etc.
- ✅ **Decimal precision** - Configurable per asset (2 for fiat, 18 for ETH)
- ✅ **Fragmentation** - Configurable unit size per asset

## Running Tests

```bash
cargo test
```

## Test Coverage

### Core Functionality

- ✅ Mint creates balance
- ✅ Simple transfer
- ✅ Transfer with automatic change
- ✅ Multiple slices from money
- ✅ Slice splitting
- ✅ Burn operation
- ✅ Reserve operation

### Error Handling

- ✅ Insufficient funds
- ✅ Unconsumed slice detection
- ✅ Money not sliced detection
- ✅ Over-slice detection
- ✅ Double-spend protection
- ✅ Transaction rollback

### Advanced

- ✅ Asset decimals conversion
- ✅ Fragmentation behavior
- ✅ Complex multi-recipient payments
- ✅ Multiple assets
- ✅ Concurrent operations

## Usage Example

```rust
use ledger::{adapters::MemoryAdapter, LedgerContext, LedgerSystem, Money, Asset};
use std::sync::Arc;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let adapter = Box::new(MemoryAdapter::new());
    let system = Arc::new(LedgerSystem::new(adapter));
    let ctx = LedgerContext::new(system.adapter_arc());

    // Create asset
    let usd = Asset::fiat("USD");
    system.adapter().create_asset(usd).await?;

    let user = Uuid::now_v7();
    let merchant = Uuid::now_v7();

    // Mint initial balance
    Money::atomic(&ctx, |tx| async move {
        tx.mint("USD", user, 100_00, "deposit".to_string()).await?;
        Ok(())
    }).await?;

    // Make payment
    Money::atomic(&ctx, |tx| async move {
        let money = tx.money("USD", user, 60_00).await?;
        let slice = money.slice(60_00)?;
        slice.transfer_to(merchant, "payment".to_string()).await?;
        Ok(())
    }).await?;

    Ok(())
}
```

## Asset Configuration

```rust
// Fiat: unit based on practical purchasing power
let usd = Asset::new("USD", 1_000, 2);    // $10 units, 2 decimals
let ngn = Asset::new("NGN", 500_000, 2);  // ₦5,000 units, 2 decimals

// Crypto: unit based on transaction patterns
let eth = Asset::new("ETH", 10_000_000_000_000_000, 18); // 0.01 ETH units
let btc = Asset::new("BTC", 10_000_000, 8);              // 0.1 BTC units
```

## Implementation Notes

### In-Memory Adapter

The `MemoryAdapter` is provided for testing. For production, implement `LedgerAdapter` for your database:

```rust
#[async_trait]
impl LedgerAdapter for PostgresAdapter {
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, i64)],
    ) -> Result<(), MoneyError> {
        // 1. SELECT FOR UPDATE to lock amounts
        // 2. Execute operations
        // 3. Burn locked VOs
        // 4. Mint change
        Ok(())
    }
    // ... other methods
}
```

### Key Design Decisions

1. **i64 vs u64**: Currently uses `i64` for PostgreSQL compatibility. Consider changing to `u64` for type safety.

2. **Unit vs Decimals**:
   - `decimals`: Display conversion (2 for USD, 18 for ETH)
   - `unit`: Max per ValueObject (practical transaction size)

3. **No Early Locking**: Planning phase is pure memory. Execution phase locks for microseconds only.

4. **Automatic Cleanup**: Change and leftover handling is automatic in `execute_plan`.

## License

MIT
