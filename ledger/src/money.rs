// ledger/src/money.rs
use super::{Asset, Balance, LedgerAdapter, MoneyError, Transaction, ValueObject};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum Operation {
    Mint {
        asset_id: Uuid,
        owner: Uuid,
        amount: i64,
        metadata: String,
    },
    Burn {
        asset_id: Uuid,
        owner: Uuid,
        amount: i64,
        metadata: String,
    },
    Transfer {
        asset_id: Uuid,
        from: Uuid,
        to: Uuid,
        amount: i64,
        metadata: String,
    },
    Reserve {
        asset_id: Uuid,
        from: Uuid,
        for_authority: Uuid,
        amount: i64,
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

    pub fn calculate_locks(&self) -> Vec<(Uuid, Uuid, i64)> {
        use std::collections::HashMap;
        let mut locks: HashMap<(Uuid, Uuid), i64> = HashMap::new();

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
    asset_id: Uuid,
    asset_code: String,
    owner: Uuid,
    amount: i64,
    sliced_amount: i64,
}

impl MoneyState {
    fn remaining(&self) -> i64 {
        self.amount - self.sliced_amount
    }
}

struct SliceState {
    id: Uuid,
    amount: i64,
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
        amount: i64,
    ) -> Result<Money, MoneyError> {
        if amount <= 0 {
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
            asset_id: asset_obj.id,
            asset_code: asset_code.clone(),
            owner,
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
        amount: i64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount <= 0 {
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
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(asset_obj.id, None, Some(owner), 0, amount, metadata),
        });

        Ok(())
    }

    pub async fn burn(
        &self,
        asset: &str,
        owner: Uuid,
        amount: i64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount <= 0 {
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
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(asset_obj.id, Some(owner), None, amount, 0, metadata),
        });

        Ok(())
    }

    pub async fn reserve(
        &self,
        asset: &str,
        from: Uuid,
        for_authority: Uuid,
        amount: i64,
        metadata: String,
    ) -> Result<(), MoneyError> {
        if amount <= 0 {
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
                Some(from),
                Some(for_authority),
                amount,
                amount,
                metadata,
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

        let result = f(tx_ctx.clone()).await;
        if let Err(e) = result {
            return Err(e);
        }

        tx_ctx.validate()?;

        let plan = tx_ctx.plan.lock().unwrap().clone();
        let locks = plan.calculate_locks();

        ledger_ctx.adapter().begin_transaction().await?;

        let execution_result = ledger_ctx.adapter().execute_plan(&plan, &locks).await;

        match execution_result {
            Ok(_) => {
                ledger_ctx.adapter().commit_transaction().await?;
                Ok(())
            }
            Err(e) => {
                ledger_ctx.adapter().rollback_transaction().await?;
                Err(e)
            }
        }
    }

    pub fn slice(&self, amount: i64) -> Result<MoneySlice, MoneyError> {
        if amount <= 0 {
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
    amount: i64,
    consumed: bool,
    money_states: Arc<Mutex<Vec<MoneyState>>>,
    slice_states: Arc<Mutex<Vec<SliceState>>>,
    plan: Arc<Mutex<ExecutionPlan>>,
    ctx: Arc<LedgerContext>,
}

impl MoneySlice {
    pub fn slice(&mut self, amount: i64) -> Result<MoneySlice, MoneyError> {
        if amount <= 0 || amount > self.amount {
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
                Some(self.owner),
                Some(recipient),
                self.amount,
                self.amount,
                metadata,
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
        });

        plan.add(Operation::RecordTransaction {
            transaction: Transaction::new(
                asset.id,
                Some(self.owner),
                None,
                self.amount,
                0,
                metadata,
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

impl Drop for MoneySlice {
    fn drop(&mut self) {
        if !self.consumed && self.amount > 0 {
            #[cfg(not(test))]
            panic!("MoneySlice dropped without being consumed");

            #[cfg(test)]
            println!("WARNING: MoneySlice dropped without being consumed");
        }
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
