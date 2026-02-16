// ledger/src/adapters/memory.rs
use crate::{
    Asset, Balance, ExecutionPlan, LedgerAdapter, MoneyError, Operation, Transaction, ValueObject,
    ValueObjectState,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct MemoryStore {
    assets: Arc<Mutex<HashMap<String, Asset>>>,
    value_objects: Arc<Mutex<HashMap<Uuid, ValueObject>>>,
    transactions: Arc<Mutex<HashMap<Uuid, Transaction>>>,
    in_transaction: Arc<Mutex<bool>>,
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            assets: Arc::new(Mutex::new(HashMap::new())),
            value_objects: Arc::new(Mutex::new(HashMap::new())),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            in_transaction: Arc::new(Mutex::new(false)),
        }
    }

    fn fragment_amount(
        &self,
        amount: u64,
        unit: u64,
        asset_id: Uuid,
        owner: Uuid,
        state: ValueObjectState,
        reserved_for: Option<Uuid>,
    ) -> Vec<ValueObject> {
        let mut fragments = Vec::new();
        let mut remaining = amount;

        while remaining > 0 {
            let chunk = remaining.min(unit);

            let mut vo = if state == ValueObjectState::Reserved {
                ValueObject::new_reserved(asset_id, owner, chunk, reserved_for.unwrap())
            } else {
                ValueObject::new_alive(asset_id, owner, chunk)
            };

            if state == ValueObjectState::Burned {
                vo.state = ValueObjectState::Burned;
            }

            fragments.push(vo);
            remaining -= chunk;
        }

        fragments
    }
}

pub struct MemoryAdapter {
    store: MemoryStore,
}

impl MemoryAdapter {
    pub fn new() -> Self {
        Self {
            store: MemoryStore::new(),
        }
    }
}

#[async_trait]
impl LedgerAdapter for MemoryAdapter {
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, u64)],
    ) -> Result<(), MoneyError> {
        // Step 1: Lock required amounts (SELECT FOR UPDATE simulation)
        let mut locked: HashMap<(Uuid, Uuid), Vec<ValueObject>> = HashMap::new();

        for (asset_id, owner, amount) in locks {
            let vos = self.select_for_burn_internal(*asset_id, *owner, *amount)?;

            let total: u64 = vos.iter().map(|vo| vo.amount).sum();
            if total < *amount {
                return Err(MoneyError::InsufficientFunds);
            }

            locked.insert((*asset_id, *owner), vos);
        }

        // Step 2: Track what's been used from each lock
        let mut used: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        // Step 3: Execute operations
        for op in plan.operations() {
            match op {
                Operation::Mint {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    self.mint_internal(*asset_id, *owner, *amount)?;
                }
                Operation::Burn {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *owner)).or_insert(0) += amount;
                }
                Operation::Transfer {
                    asset_id,
                    from,
                    to,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *from)).or_insert(0) += amount;
                    self.mint_internal(*asset_id, *to, *amount)?;
                }
                Operation::Reserve {
                    asset_id,
                    from,
                    for_authority,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *from)).or_insert(0) += amount;
                    self.mint_reserved_internal(
                        *asset_id,
                        *for_authority,
                        *amount,
                        *for_authority,
                    )?;
                }
                Operation::RecordTransaction { transaction } => {
                    let mut txs = self.store.transactions.lock().unwrap();
                    txs.insert(transaction.id, transaction.clone());
                }
            }
        }

        // Step 4: Burn all locked VOs and mint change
        for ((asset_id, owner), vos) in locked.iter() {
            let total_locked: u64 = vos.iter().map(|vo| vo.amount).sum();
            let total_used = used.get(&(*asset_id, *owner)).copied().unwrap_or(0);

            // Burn all locked VOs
            for vo in vos {
                let mut value_objects = self.store.value_objects.lock().unwrap();
                if let Some(stored_vo) = value_objects.get_mut(&vo.id) {
                    stored_vo.state = ValueObjectState::Burned;
                }
            }

            // Mint change if any
            let change = total_locked - total_used;
            if change > 0 {
                self.mint_internal(*asset_id, *owner, change)?;
            }
        }

        Ok(())
    }

    async fn begin_transaction(&self) -> Result<(), MoneyError> {
        let mut in_tx = self.store.in_transaction.lock().unwrap();
        if *in_tx {
            return Err(MoneyError::Storage("Already in transaction".to_string()));
        }
        *in_tx = true;
        Ok(())
    }

    async fn commit_transaction(&self) -> Result<(), MoneyError> {
        let mut in_tx = self.store.in_transaction.lock().unwrap();
        if !*in_tx {
            return Err(MoneyError::Storage("Not in transaction".to_string()));
        }
        *in_tx = false;
        Ok(())
    }

    async fn rollback_transaction(&self) -> Result<(), MoneyError> {
        let mut in_tx = self.store.in_transaction.lock().unwrap();
        *in_tx = false;
        Ok(())
    }

    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError> {
        let vos = self.store.value_objects.lock().unwrap();

        let alive_sum: u64 = vos
            .values()
            .filter(|vo| vo.asset == asset_id && vo.owner == owner && vo.state.is_alive())
            .map(|vo| vo.amount)
            .sum();

        let reserved_sum: u64 = vos
            .values()
            .filter(|vo| vo.asset == asset_id && vo.owner == owner && vo.state.is_reserved())
            .map(|vo| vo.amount)
            .sum();

        Ok(Balance::from_value_objects(
            owner,
            asset_id,
            alive_sum,
            reserved_sum,
        ))
    }

    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
        let txs = self.store.transactions.lock().unwrap();
        txs.get(&tx_id)
            .cloned()
            .ok_or(MoneyError::TransactionNotFound)
    }

    async fn get_asset(&self, code: &str) -> Result<Asset, MoneyError> {
        let assets = self.store.assets.lock().unwrap();
        assets
            .get(code)
            .cloned()
            .ok_or_else(|| MoneyError::AssetNotFound(code.to_string()))
    }

    async fn create_asset(&self, asset: Asset) -> Result<(), MoneyError> {
        let mut assets = self.store.assets.lock().unwrap();
        assets.insert(asset.code.clone(), asset);
        Ok(())
    }
}

impl MemoryAdapter {
    fn select_for_burn_internal(
        &self,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
    ) -> Result<Vec<ValueObject>, MoneyError> {
        let vos = self.store.value_objects.lock().unwrap();

        let mut available: Vec<ValueObject> = vos
            .values()
            .filter(|vo| vo.asset == asset_id && vo.owner == owner && vo.state.is_alive())
            .cloned()
            .collect();

        available.sort_by_key(|vo| vo.amount);

        let mut selected = Vec::new();
        let mut total = 0u64;

        for vo in available {
            selected.push(vo.clone());
            total += vo.amount;
            if total >= amount {
                break;
            }
        }

        Ok(selected)
    }

    fn mint_internal(&self, asset_id: Uuid, owner: Uuid, amount: u64) -> Result<(), MoneyError> {
        let assets = self.store.assets.lock().unwrap();
        let asset = assets
            .values()
            .find(|a| a.id == asset_id)
            .ok_or(MoneyError::AssetNotFound(asset_id.to_string()))?;

        let fragments = self.store.fragment_amount(
            amount,
            asset.unit,
            asset_id,
            owner,
            ValueObjectState::Alive,
            None,
        );

        let mut vos = self.store.value_objects.lock().unwrap();
        for fragment in fragments {
            vos.insert(fragment.id, fragment);
        }

        Ok(())
    }

    fn mint_reserved_internal(
        &self,
        asset_id: Uuid,
        owner: Uuid,
        amount: u64,
        authority: Uuid,
    ) -> Result<(), MoneyError> {
        let assets = self.store.assets.lock().unwrap();
        let asset = assets
            .values()
            .find(|a| a.id == asset_id)
            .ok_or(MoneyError::AssetNotFound(asset_id.to_string()))?;

        let fragments = self.store.fragment_amount(
            amount,
            asset.unit,
            asset_id,
            owner,
            ValueObjectState::Reserved,
            Some(authority),
        );

        let mut vos = self.store.value_objects.lock().unwrap();
        for fragment in fragments {
            vos.insert(fragment.id, fragment);
        }

        Ok(())
    }
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}
