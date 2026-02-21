// ledger/src/money.rs
use super::{Balance, LedgerAdapter, MoneyError, Transaction};
use metrics::{counter, histogram};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum Operation {
    Mint {
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        metadata: String,
        idempotency_key: Option<String>,
    },
    Burn {
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        metadata: String,
        idempotency_key: Option<String>,
    },
    Transfer {
        asset_id: Uuid,
        from: Uuid,
        to: Uuid,
        amount: u64,
        metadata: String,
    },
    Reserve {
        asset_id: Uuid,
        from: Uuid,
        for_authority: Uuid,
        amount: u64,
        metadata: String,
    },
    Settle {
        asset_id: Uuid,
        authority: Uuid,
        receiver: Uuid,
        amount: u64,
        metadata: String,
    },
    RecordTransaction {
        transaction: Transaction,
    },
}

#[derive(Clone)]
pub struct ExecutionPlan {
    operations: Vec<Operation>,
}

impl ExecutionPlan {
    fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    fn add(&mut self, op: Operation) {
        self.operations.push(op);
    }

    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    pub fn calculate_locks(&self) -> Vec<(Uuid, Uuid, u64)> {
        use std::collections::HashMap;
        let mut locks: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        for op in &self.operations {
            match op {
                Operation::Burn {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    *locks.entry((*asset_id, *owner)).or_insert(0) += amount;
                }
                Operation::Transfer {
                    asset_id,
                    from,
                    amount,
                    ..
                } => {
                    *locks.entry((*asset_id, *from)).or_insert(0) += amount;
                }
                Operation::Reserve {
                    asset_id,
                    from,
                    amount,
                    ..
                } => {
                    *locks.entry((*asset_id, *from)).or_insert(0) += amount;
                }
                _ => {}
            }
        }

        locks
            .into_iter()
            .map(|((asset, owner), amount)| (asset, owner, amount))
            .collect()
    }
}

#[derive(Clone)]
pub struct LedgerContext {
    adapter: Arc<dyn LedgerAdapter>,
}

impl LedgerContext {
    pub fn new(adapter: Arc<dyn LedgerAdapter>) -> Self {
        Self { adapter }
    }

    pub fn adapter(&self) -> &dyn LedgerAdapter {
        self.adapter.as_ref()
    }
}

struct MoneyState {
    amount: u64,
    sliced_amount: u64,
}

impl MoneyState {
    fn remaining(&self) -> u64 {
        self.amount - self.sliced_amount
    }
}

struct SliceState {
    id: Uuid,
    amount: u64,
    consumed: bool,
}

#[derive(Clone)]
pub struct TransactionContext {
    ctx: Arc<LedgerContext>,
    money_states: Arc<Mutex<Vec<MoneyState>>>,
    slice_states: Arc<Mutex<Vec<SliceState>>>,
    plan: Arc<Mutex<ExecutionPlan>>,
}

impl TransactionContext {
    fn new(adapter: Arc<dyn LedgerAdapter>) -> Self {
        Self {
            ctx: Arc::new(LedgerContext::new(adapter)),
            money_states: Arc::new(Mutex::new(Vec::new())),
            slice_states: Arc::new(Mutex::new(Vec::new())),
            plan: Arc::new(Mutex::new(ExecutionPlan::new())),
        }
    }

    pub async fn get_balance(&self, asset: &str, owner: Uuid) -> Result<Balance, MoneyError> {
        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;
        adapter.get_balance(asset_obj.id, owner).await
    }

    pub async fn money(
        &self,
        asset: impl Into<String>,
        owner: Uuid,
        amount: u64,
    ) -> Result<Money, MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let asset_code = asset.into();
        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(&asset_code).await?;

        let balance = adapter.get_balance(asset_obj.id, owner).await?;
        if balance.available < amount {
            return Err(MoneyError::InsufficientFunds);
        }

        let state = MoneyState {
            amount,
            sliced_amount: 0,
        };

        let state_id = {
            let mut states = self.money_states.lock().unwrap();
            states.push(state);
            states.len() - 1
        };

        Ok(Money {
            state_id,
            asset_code,
            owner,
            money_states: Arc::clone(&self.money_states),
            slice_states: Arc::clone(&self.slice_states),
            plan: Arc::clone(&self.plan),
            ctx: Arc::clone(&self.ctx),
        })
    }

    pub async fn mint(
        &self,
        asset: &str,
        owner: Uuid,
        amount: u64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Mint {
            asset_id: asset_obj.id,
            owner,
            amount,
            metadata: metadata.clone(),
            idempotency_key: None,
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                None,
                Some(owner),
                0,
                amount,
                metadata,
                None,
            ),
        });

        Ok(())
    }

    pub async fn mint_idempotent(
        &self,
        asset: &str,
        owner: Uuid,
        amount: u64,
        metadata: String,
        idempotency_key: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Mint {
            asset_id: asset_obj.id,
            owner,
            amount,
            metadata: metadata.clone(),
            idempotency_key: Some(idempotency_key.clone()),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                None,
                Some(owner),
                0,
                amount,
                metadata,
                Some(idempotency_key),
            ),
        });

        Ok(())
    }

    pub async fn burn(
        &self,
        asset: &str,
        owner: Uuid,
        amount: u64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Burn {
            asset_id: asset_obj.id,
            owner,
            amount,
            metadata: metadata.clone(),
            idempotency_key: None,
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                Some(owner),
                None,
                amount,
                0,
                metadata,
                None,
            ),
        });

        Ok(())
    }

    pub async fn burn_idempotent(
        &self,
        asset: &str,
        owner: Uuid,
        amount: u64,
        metadata: String,
        idempotency_key: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Burn {
            asset_id: asset_obj.id,
            owner,
            amount,
            metadata: metadata.clone(),
            idempotency_key: Some(idempotency_key.clone()),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                Some(owner),
                None,
                amount,
                0,
                metadata,
                Some(idempotency_key),
            ),
        });

        Ok(())
    }

    pub async fn reserve(
        &self,
        asset: &str,
        from: Uuid,
        for_authority: Uuid,
        amount: u64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Reserve {
            asset_id: asset_obj.id,
            from,
            for_authority,
            amount,
            metadata: metadata.clone(),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                Some(from),
                Some(for_authority),
                amount,
                amount,
                metadata,
                None,
            ),
        });

        Ok(())
    }

    pub async fn settle(
        &self,
        asset: &str,
        authority: Uuid,
        receiver: Uuid,
        amount: u64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let adapter = self.ctx.adapter();
        let asset_obj = adapter.get_asset(asset).await?;

        // Advisory pre-flight — the real guard is the adapter's inline lock during execute_plan
        let balance = adapter.get_balance(asset_obj.id, authority).await?;
        if balance.reserved < amount {
            return Err(MoneyError::InsufficientFunds);
        }

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Settle {
            asset_id: asset_obj.id,
            authority,
            receiver,
            amount,
            metadata: metadata.clone(),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset_obj.id,
                asset_obj.code,
                Some(authority),
                Some(receiver),
                amount,
                amount,
                metadata,
                None,
            ),
        });

        Ok(())
    }

    fn validate(&self) -> Result<(), MoneyError> {
        let states = self.money_states.lock().unwrap();
        for state in states.iter() {
            if state.sliced_amount == 0 {
                return Err(MoneyError::Storage(
                    "Money created but never sliced".to_string(),
                ));
            }
            if state.sliced_amount > state.amount {
                return Err(MoneyError::InvalidAmount);
            }
        }

        let slices = self.slice_states.lock().unwrap();
        let unconsumed: Vec<_> = slices
            .iter()
            .filter(|s| !s.consumed && s.amount > 0)
            .collect();

        if !unconsumed.is_empty() {
            return Err(MoneyError::UnconsumedSlice);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Money {
    state_id: usize,
    asset_code: String,
    owner: Uuid,
    money_states: Arc<Mutex<Vec<MoneyState>>>,
    slice_states: Arc<Mutex<Vec<SliceState>>>,
    plan: Arc<Mutex<ExecutionPlan>>,
    ctx: Arc<LedgerContext>,
}

impl Money {
    pub async fn atomic<F, Fut>(ledger_ctx: &LedgerContext, f: F) -> Result<(), MoneyError>
    where
        F: FnOnce(TransactionContext) -> Fut,
        Fut: std::future::Future<Output = Result<(), MoneyError>>,
    {
        let tx_ctx = TransactionContext::new(Arc::clone(&ledger_ctx.adapter));

        // Run the planning closure (pure memory, no DB writes)
        if let Err(e) = f(tx_ctx.clone()).await {
            return Err(e);
        }

        // Validate slice accounting
        tx_ctx.validate()?;

        let plan = tx_ctx.plan.lock().unwrap().clone();
        let locks = plan.calculate_locks();

        // Metrics (unchanged)
        for operation in &plan.operations {
            if let Operation::RecordTransaction { transaction } = operation {
                histogram!("ledger.transaction.amount", "asset" => transaction.code.clone())
                    .record(if transaction.burned_amount > 0 {
                        transaction.burned_amount as f64
                    } else {
                        transaction.minted_amount as f64
                    });
            }
        }

        // execute_plan owns BEGIN/COMMIT/ROLLBACK — no wrapper needed here
        let result = ledger_ctx.adapter().execute_plan(&plan, &locks).await;

        counter!("ledger.transactions.total",
            "status" => if result.is_ok() { "success" } else { "failed" }
        )
        .increment(1);

        result
    }

    pub fn slice(&self, amount: u64) -> Result<MoneySlice, MoneyError> {
        if amount == 0 {
            return Err(MoneyError::InvalidAmount);
        }

        let mut states = self.money_states.lock().unwrap();
        let state = &mut states[self.state_id];

        if state.remaining() < amount {
            return Err(MoneyError::InvalidAmount);
        }

        state.sliced_amount += amount;
        drop(states);

        let slice_id = Uuid::now_v7();
        let mut slices = self.slice_states.lock().unwrap();
        slices.push(SliceState {
            id: slice_id,
            amount,
            consumed: false,
        });
        drop(slices);

        Ok(MoneySlice {
            id: slice_id,
            state_id: self.state_id,
            asset_code: self.asset_code.clone(),
            owner: self.owner,
            amount,
            consumed: false,
            money_states: Arc::clone(&self.money_states),
            slice_states: Arc::clone(&self.slice_states),
            plan: Arc::clone(&self.plan),
            ctx: Arc::clone(&self.ctx),
        })
    }
}

pub struct MoneySlice {
    id: Uuid,
    state_id: usize,
    asset_code: String,
    owner: Uuid,
    amount: u64,
    consumed: bool,
    money_states: Arc<Mutex<Vec<MoneyState>>>,
    slice_states: Arc<Mutex<Vec<SliceState>>>,
    plan: Arc<Mutex<ExecutionPlan>>,
    ctx: Arc<LedgerContext>,
}

impl MoneySlice {
    pub fn slice(&mut self, amount: u64) -> Result<MoneySlice, MoneyError> {
        if amount == 0 || amount > self.amount {
            return Err(MoneyError::InvalidAmount);
        }

        self.amount -= amount;

        let mut slices = self.slice_states.lock().unwrap();
        if let Some(slice) = slices.iter_mut().find(|s| s.id == self.id) {
            slice.amount = self.amount;
        }
        drop(slices);

        let slice_id = Uuid::now_v7();
        let mut slices = self.slice_states.lock().unwrap();
        slices.push(SliceState {
            id: slice_id,
            amount,
            consumed: false,
        });
        drop(slices);

        Ok(MoneySlice {
            id: slice_id,
            state_id: self.state_id,
            asset_code: self.asset_code.clone(),
            owner: self.owner,
            amount,
            consumed: false,
            money_states: Arc::clone(&self.money_states),
            slice_states: Arc::clone(&self.slice_states),
            plan: Arc::clone(&self.plan),
            ctx: Arc::clone(&self.ctx),
        })
    }

    pub async fn transfer_to(
        mut self,
        recipient: Uuid,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if self.consumed {
            return Err(MoneyError::UnconsumedSlice);
        }

        let adapter = self.ctx.adapter();
        let asset = adapter.get_asset(&self.asset_code).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Transfer {
            asset_id: asset.id,
            from: self.owner,
            to: recipient,
            amount: self.amount,
            metadata: metadata.clone(),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset.id,
                asset.code,
                Some(self.owner),
                Some(recipient),
                self.amount,
                self.amount,
                metadata,
                None,
            ),
        });

        self.consumed = true;
        let mut slices = self.slice_states.lock().unwrap();
        if let Some(slice) = slices.iter_mut().find(|s| s.id == self.id) {
            slice.consumed = true;
        }

        Ok(())
    }

    pub async fn burn(mut self, metadata: String) -> Result<(), MoneyError> {
        if self.consumed {
            return Err(MoneyError::UnconsumedSlice);
        }

        let adapter = self.ctx.adapter();
        let asset = adapter.get_asset(&self.asset_code).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Burn {
            asset_id: asset.id,
            owner: self.owner,
            amount: self.amount,
            metadata: metadata.clone(),
            idempotency_key: None,
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset.id,
                asset.code,
                Some(self.owner),
                None,
                self.amount,
                0,
                metadata,
                None,
            ),
        });

        self.consumed = true;
        let mut slices = self.slice_states.lock().unwrap();
        if let Some(slice) = slices.iter_mut().find(|s| s.id == self.id) {
            slice.consumed = true;
        }

        Ok(())
    }

    pub async fn burn_idempotent(
        mut self,
        metadata: String,
        idempotency_key: String,
    ) -> Result<(), MoneyError> {
        if self.consumed {
            return Err(MoneyError::UnconsumedSlice);
        }

        let adapter = self.ctx.adapter();
        let asset = adapter.get_asset(&self.asset_code).await?;

        let mut plan = self.plan.lock().unwrap();
        plan.add(Operation::Burn {
            asset_id: asset.id,
            owner: self.owner,
            amount: self.amount,
            metadata: metadata.clone(),
            idempotency_key: Some(idempotency_key.clone()),
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset.id,
                asset.code,
                Some(self.owner),
                None,
                self.amount,
                0,
                metadata,
                Some(idempotency_key),
            ),
        });

        self.consumed = true;
        let mut slices = self.slice_states.lock().unwrap();
        if let Some(slice) = slices.iter_mut().find(|s| s.id == self.id) {
            slice.consumed = true;
        }

        Ok(())
    }

    pub fn is_consumed(&self) -> bool {
        self.consumed
    }
}

impl Balance {
    pub async fn get(
        asset_code: impl Into<String>,
        owner: Uuid,
        ctx: &LedgerContext,
    ) -> Result<Balance, MoneyError> {
        let adapter = ctx.adapter();
        let asset = adapter.get_asset(&asset_code.into()).await?;
        adapter.get_balance(asset.id, owner).await
    }
}
