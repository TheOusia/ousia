// ledger/src/adapters/memory.rs
use crate::{
    Asset, Balance, ExecutionPlan, LedgerAdapter, MoneyError, Operation, Transaction, ValueObject,
    ValueObjectState,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct MemoryStore {
    assets: Arc<Mutex<HashMap<String, Asset>>>,
    value_objects: Arc<Mutex<HashMap<Uuid, ValueObject>>>,
    transactions: Arc<Mutex<HashMap<Uuid, Transaction>>>,
    idempotency_keys: Arc<Mutex<HashMap<String, Uuid>>>, // hash -> transaction_id
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            assets: Arc::new(Mutex::new(HashMap::new())),
            value_objects: Arc::new(Mutex::new(HashMap::new())),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            idempotency_keys: Arc::new(Mutex::new(HashMap::new())),
        }
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
        // Hold the mutex for the ENTIRE operation — this is the MemoryAdapter's
        // equivalent of BEGIN/SELECT FOR UPDATE/COMMIT. No other task can enter
        // execute_plan while we hold it.
        let mut value_objects = self.store.value_objects.lock().unwrap();
        let assets = self.store.assets.lock().unwrap();
        let mut transactions = self.store.transactions.lock().unwrap();

        // ── Phase 1: Select & verify under lock ───────────────────────────────
        // HashMap<(asset_id, owner) -> (selected_vo_ids, total_locked)>
        let mut locked: HashMap<(Uuid, Uuid), (Vec<Uuid>, u64)> = HashMap::new();

        for (asset_id, owner, required) in locks {
            let mut candidates: Vec<(Uuid, u64)> = value_objects
                .values()
                .filter(|vo| vo.asset == *asset_id && vo.owner == *owner && vo.state.is_alive())
                .map(|vo| (vo.id, vo.amount))
                .collect();

            // Smallest-first selection (matches Postgres ORDER BY amount ASC)
            candidates.sort_by_key(|(_, amt)| *amt);

            let mut ids = Vec::new();
            let mut total = 0u64;

            for (id, amt) in candidates {
                ids.push(id);
                total += amt;
                if total >= *required {
                    break;
                }
            }

            // Checked while holding the mutex — this is the real double-spend guard
            if total < *required {
                return Err(MoneyError::InsufficientFunds);
            }

            locked.insert((*asset_id, *owner), (ids, total));
        }

        // ── Phase 2: Execute operations ───────────────────────────────────────
        // Track how much of each locked pool is actually consumed
        let mut used: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        for op in plan.operations() {
            match op {
                Operation::Mint {
                    asset_id,
                    owner,
                    amount,
                    ..
                } => {
                    let asset = assets
                        .values()
                        .find(|a| a.id == *asset_id)
                        .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

                    let mut remaining = *amount;
                    while remaining > 0 {
                        let chunk = remaining.min(asset.unit);
                        let vo = ValueObject::new_alive(*asset_id, *owner, chunk);
                        value_objects.insert(vo.id, vo);
                        remaining -= chunk;
                    }
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

                    let asset = assets
                        .values()
                        .find(|a| a.id == *asset_id)
                        .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

                    let mut remaining = *amount;
                    while remaining > 0 {
                        let chunk = remaining.min(asset.unit);
                        let vo = ValueObject::new_alive(*asset_id, *to, chunk);
                        value_objects.insert(vo.id, vo);
                        remaining -= chunk;
                    }
                }

                Operation::Reserve {
                    asset_id,
                    from,
                    for_authority,
                    amount,
                    ..
                } => {
                    *used.entry((*asset_id, *from)).or_insert(0) += amount;

                    let asset = assets
                        .values()
                        .find(|a| a.id == *asset_id)
                        .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

                    let mut remaining = *amount;
                    while remaining > 0 {
                        let chunk = remaining.min(asset.unit);
                        let vo = ValueObject::new_reserved(
                            *asset_id,
                            *for_authority,
                            chunk,
                            *for_authority,
                        );
                        value_objects.insert(vo.id, vo);
                        remaining -= chunk;
                    }
                }

                Operation::Settle {
                    asset_id,
                    authority,
                    receiver,
                    amount,
                    ..
                } => {
                    // Select reserved VOs owned by authority, smallest-first
                    let mut candidates: Vec<(Uuid, u64)> = value_objects
                        .values()
                        .filter(|vo| {
                            vo.asset == *asset_id
                                && vo.owner == *authority
                                && vo.state.is_reserved()
                        })
                        .map(|vo| (vo.id, vo.amount))
                        .collect();
                    candidates.sort_by_key(|(_, amt)| *amt);

                    let mut ids_to_burn = Vec::new();
                    let mut total_reserved = 0u64;
                    for (id, amt) in candidates {
                        ids_to_burn.push(id);
                        total_reserved += amt;
                        if total_reserved >= *amount {
                            break;
                        }
                    }

                    if total_reserved < *amount {
                        return Err(MoneyError::InsufficientFunds);
                    }

                    // Burn the selected reserved VOs
                    for id in &ids_to_burn {
                        if let Some(vo) = value_objects.get_mut(id) {
                            vo.state = ValueObjectState::Burned;
                        }
                    }

                    let asset = assets
                        .values()
                        .find(|a| a.id == *asset_id)
                        .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

                    // Return change as reserved VOs for authority
                    let change = total_reserved - *amount;
                    if change > 0 {
                        let mut remaining = change;
                        while remaining > 0 {
                            let chunk = remaining.min(asset.unit);
                            let vo =
                                ValueObject::new_reserved(*asset_id, *authority, chunk, *authority);
                            value_objects.insert(vo.id, vo);
                            remaining -= chunk;
                        }
                    }

                    // Mint alive VOs for receiver
                    let mut remaining = *amount;
                    while remaining > 0 {
                        let chunk = remaining.min(asset.unit);
                        let vo = ValueObject::new_alive(*asset_id, *receiver, chunk);
                        value_objects.insert(vo.id, vo);
                        remaining -= chunk;
                    }
                }

                Operation::RecordTransaction { transaction } => {
                    if let Some(ref raw_key) = transaction.idempotency_key {
                        let hash = crate::hash_idempotency_key(raw_key);

                        let mut keys = self.store.idempotency_keys.lock().unwrap();

                        // Check for duplicate while holding the mutex — atomic with the insert
                        if keys.contains_key(&hash) {
                            return Err(MoneyError::DuplicateIdempotencyKey(transaction.id));
                        }

                        keys.insert(hash, transaction.id);
                    }

                    transactions.insert(transaction.id, transaction.clone());
                }
            }
        }

        // ── Phase 3: Burn locked VOs, mint change ─────────────────────────────
        for ((asset_id, owner), (ids, total_locked)) in &locked {
            let total_used = used.get(&(*asset_id, *owner)).copied().unwrap_or(0);

            // Burn every selected VO
            for id in ids {
                if let Some(vo) = value_objects.get_mut(id) {
                    vo.state = ValueObjectState::Burned;
                }
            }

            // Mint change if we locked more than we spent
            let change = total_locked - total_used;
            if change > 0 {
                let asset = assets
                    .values()
                    .find(|a| a.id == *asset_id)
                    .ok_or_else(|| MoneyError::AssetNotFound(asset_id.to_string()))?;

                let mut remaining = change;
                while remaining > 0 {
                    let chunk = remaining.min(asset.unit);
                    let vo = ValueObject::new_alive(*asset_id, *owner, chunk);
                    value_objects.insert(vo.id, vo);
                    remaining -= chunk;
                }
            }
        }

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

    async fn check_idempotency_key(&self, key: &str) -> Result<(), MoneyError> {
        let hash = crate::hash_idempotency_key(key);
        let keys = self.store.idempotency_keys.lock().unwrap();
        if keys.contains_key(&hash) {
            // Return the transaction id that consumed this key
            let tx_id = *keys.get(&hash).unwrap();
            return Err(MoneyError::DuplicateIdempotencyKey(tx_id));
        }
        Ok(())
    }

    async fn get_transaction_by_idempotency_key(
        &self,
        key: &str,
    ) -> Result<Transaction, MoneyError> {
        let hash = crate::hash_idempotency_key(key);

        let tx_id = {
            let keys = self.store.idempotency_keys.lock().unwrap();
            *keys.get(&hash).ok_or(MoneyError::TransactionNotFound)?
        };

        let txs = self.store.transactions.lock().unwrap();
        txs.get(&tx_id)
            .cloned()
            .ok_or(MoneyError::TransactionNotFound)
    }

    async fn get_transaction(&self, tx_id: Uuid) -> Result<Transaction, MoneyError> {
        let txs = self.store.transactions.lock().unwrap();
        txs.get(&tx_id)
            .cloned()
            .ok_or(MoneyError::TransactionNotFound)
    }

    async fn get_transactions_for_owner(
        &self,
        owner: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError> {
        let txs = self.store.transactions.lock().unwrap();
        Ok(txs
            .values()
            .filter(|tx| {
                ((tx.sender.is_some() && tx.sender.unwrap() == owner)
                    || (tx.receiver.is_some() && tx.receiver.unwrap() == owner))
                    && tx.created_at.timestamp() >= timespan[0].timestamp()
                    && tx.created_at.timestamp() <= timespan[1].timestamp()
            })
            .cloned()
            .collect::<Vec<_>>())
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

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}
