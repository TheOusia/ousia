// ledger/src/adapters/memory.rs
use crate::{
    Account, AccountBalance, AccountQuery, Asset, Balance, ExecutionPlan, Holding, LedgerAdapter,
    MoneyError, Operation, Transaction, ValueObject, ValueObjectState,
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
    accounts: Arc<Mutex<HashMap<Uuid, Account>>>,        // owner -> account
}

impl MemoryStore {
    fn new() -> Self {
        Self {
            assets: Arc::new(Mutex::new(HashMap::new())),
            value_objects: Arc::new(Mutex::new(HashMap::new())),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            idempotency_keys: Arc::new(Mutex::new(HashMap::new())),
            accounts: Arc::new(Mutex::new(HashMap::new())),
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
        // Hold the mutexes for the ENTIRE operation — this is the MemoryAdapter's
        // equivalent of BEGIN/SELECT FOR UPDATE. No other task can enter
        // execute_plan while we hold them.
        //
        // Phases 0-3 below operate on LOCAL clones, not the guards directly:
        // an error returned partway through (e.g. Phase 1's InsufficientFunds,
        // or Phase 2's DuplicateIdempotencyKey — which can be discovered only
        // *after* Phase 0 already minted new value objects for the same plan)
        // must leave the shared store completely untouched. Writing straight
        // into the guarded maps and returning early on error — the previous
        // behaviour — committed whatever had mutated so far with no rollback,
        // silently double-minting on a duplicate idempotency key. The clones
        // are written back to the guards only once every phase has succeeded,
        // which is this adapter's equivalent of COMMIT; on any early return
        // they're simply dropped and the guards (hence the shared store)
        // never see the partial work.
        let mut value_objects_guard = self.store.value_objects.lock().unwrap();
        let assets = self.store.assets.lock().unwrap();
        let mut transactions_guard = self.store.transactions.lock().unwrap();
        let mut idempotency_keys_guard = self.store.idempotency_keys.lock().unwrap();

        let mut value_objects = value_objects_guard.clone();
        let mut transactions = transactions_guard.clone();
        let mut idempotency_keys = idempotency_keys_guard.clone();

        // ── Phase 0: Apply mints ──────────────────────────────────────────────
        // Mints create new value; they don't compete for existing VOs. Running
        // them first means Phase 1's selection sees them as ordinary alive
        // rows, so a reserve/transfer in the same atomic block can be funded
        // by an in-plan mint without any special-case Phase 3 accounting.
        for op in plan.operations() {
            if let Operation::Mint {
                asset_id,
                owner,
                amount,
                ..
            } = op
            {
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
        }

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
        // Track how much of each locked pool is actually consumed.
        // Mints already applied in Phase 0.
        let mut used: HashMap<(Uuid, Uuid), u64> = HashMap::new();

        for op in plan.operations() {
            match op {
                Operation::Mint { .. } => {}

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
                    let mut transaction = transaction.clone();

                    if let Some(ref raw_key) = transaction.idempotency_key {
                        let hash = crate::hash_idempotency_key(raw_key);

                        // Checked against the local staging map — still race-free,
                        // since the real `idempotency_keys` mutex has been held
                        // (via `idempotency_keys_guard`) since the top of this call.
                        if let Some(existing_id) = idempotency_keys.get(&hash) {
                            return Err(MoneyError::DuplicateIdempotencyKey(*existing_id));
                        }

                        idempotency_keys.insert(hash.clone(), transaction.id);
                        // Match the Postgres adapter, which only ever persists the
                        // hash (`ledger_transaction_idempotency_keys.key`), never the
                        // raw key — otherwise the two adapters return different
                        // `idempotency_key` content for the same stored transaction.
                        transaction.idempotency_key = Some(hash);
                    }

                    transactions.insert(transaction.id, transaction);
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

        // Every phase succeeded — commit the staged state into the shared store.
        *value_objects_guard = value_objects;
        *transactions_guard = transactions;
        *idempotency_keys_guard = idempotency_keys;

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

    async fn get_holdings(&self, owner: Uuid) -> Result<Vec<Holding>, MoneyError> {
        let vos = self.store.value_objects.lock().unwrap();
        let assets = self.store.assets.lock().unwrap();

        let mut alive: HashMap<Uuid, u64> = HashMap::new();
        let mut reserved: HashMap<Uuid, u64> = HashMap::new();

        for vo in vos.values().filter(|vo| vo.owner == owner) {
            if vo.state.is_alive() {
                *alive.entry(vo.asset).or_insert(0) += vo.amount;
            } else if vo.state.is_reserved() {
                *reserved.entry(vo.asset).or_insert(0) += vo.amount;
            }
        }

        let mut asset_ids: std::collections::HashSet<Uuid> =
            alive.keys().chain(reserved.keys()).copied().collect();
        asset_ids.retain(|id| alive.get(id).copied().unwrap_or(0) + reserved.get(id).copied().unwrap_or(0) > 0);

        let holdings = asset_ids
            .into_iter()
            .filter_map(|asset_id| {
                let asset = assets.values().find(|a| a.id == asset_id)?.clone();
                let balance = Balance::from_value_objects(
                    owner,
                    asset_id,
                    alive.get(&asset_id).copied().unwrap_or(0),
                    reserved.get(&asset_id).copied().unwrap_or(0),
                );
                Some(Holding::new(asset, balance))
            })
            .collect();

        Ok(holdings)
    }

    async fn get_transactions_for_asset(
        &self,
        asset_id: Uuid,
        timespan: &[DateTime<Utc>; 2],
    ) -> Result<Vec<Transaction>, MoneyError> {
        let txs = self.store.transactions.lock().unwrap();
        Ok(txs
            .values()
            .filter(|tx| {
                tx.asset == asset_id
                    && tx.created_at.timestamp() >= timespan[0].timestamp()
                    && tx.created_at.timestamp() <= timespan[1].timestamp()
            })
            .cloned()
            .collect())
    }

    // ------------------------------------------------------------------ //
    //  Account registry                                                   //
    // ------------------------------------------------------------------ //

    async fn register_account(&self, account: &Account) -> Result<Account, MoneyError> {
        let mut accounts = self.store.accounts.lock().unwrap();

        // Mirror the Postgres UNIQUE on `key`: one key belongs to exactly
        // one owner for the life of the ledger.
        if accounts
            .values()
            .any(|existing| existing.key == account.key && existing.owner != account.owner)
        {
            return Err(MoneyError::Storage(format!(
                "account key '{}' is already registered to a different owner",
                account.key
            )));
        }

        let now = Utc::now();
        let stored = Account {
            // Preserve the original registration time on re-register.
            created_at: accounts
                .get(&account.owner)
                .map(|existing| existing.created_at)
                .unwrap_or(now),
            updated_at: now,
            // An upsert never resurrects an archived account — that is
            // `unarchive_account`'s job, and silently un-retiring one on a
            // boot-time re-register would be a surprise.
            archived_at: accounts
                .get(&account.owner)
                .and_then(|existing| existing.archived_at),
            ..account.clone()
        };
        accounts.insert(account.owner, stored.clone());
        Ok(stored)
    }

    async fn get_account(&self, owner: Uuid) -> Result<Option<Account>, MoneyError> {
        Ok(self.store.accounts.lock().unwrap().get(&owner).cloned())
    }

    async fn get_account_by_key(&self, key: &str) -> Result<Option<Account>, MoneyError> {
        Ok(self
            .store
            .accounts
            .lock()
            .unwrap()
            .values()
            .find(|account| account.key == key)
            .cloned())
    }

    async fn list_accounts(&self, query: &AccountQuery) -> Result<Vec<Account>, MoneyError> {
        let accounts = self.store.accounts.lock().unwrap();
        let mut matched: Vec<Account> = accounts
            .values()
            .filter(|account| match &query.kind {
                Some(kind) => &account.kind == kind,
                None => true,
            })
            .filter(|account| query.include_archived || !account.is_archived())
            .cloned()
            .collect();

        matched.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.owner.cmp(&b.owner))
        });

        Ok(matched
            .into_iter()
            .skip(query.effective_offset() as usize)
            .take(query.effective_limit() as usize)
            .collect())
    }

    async fn list_account_balances(
        &self,
        asset_id: Uuid,
        query: &AccountQuery,
    ) -> Result<Vec<AccountBalance>, MoneyError> {
        let matched = self.list_accounts(query).await?;
        let value_objects = self.store.value_objects.lock().unwrap();

        Ok(matched
            .into_iter()
            .map(|account| {
                let (alive, reserved) = value_objects
                    .values()
                    .filter(|vo| vo.owner == account.owner && vo.asset == asset_id)
                    .fold((0u64, 0u64), |(alive, reserved), vo| match vo.state {
                        ValueObjectState::Alive => (alive + vo.amount, reserved),
                        ValueObjectState::Reserved => (alive, reserved + vo.amount),
                        ValueObjectState::Burned => (alive, reserved),
                    });
                let balance =
                    Balance::from_value_objects(account.owner, asset_id, alive, reserved);
                AccountBalance { account, balance }
            })
            .collect())
    }

    async fn archive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        let mut accounts = self.store.accounts.lock().unwrap();
        if let Some(account) = accounts.get_mut(&owner) {
            if account.archived_at.is_none() {
                account.archived_at = Some(Utc::now());
                account.updated_at = Utc::now();
            }
        }
        Ok(())
    }

    async fn unarchive_account(&self, owner: Uuid) -> Result<(), MoneyError> {
        let mut accounts = self.store.accounts.lock().unwrap();
        if let Some(account) = accounts.get_mut(&owner) {
            if account.archived_at.is_some() {
                account.archived_at = None;
                account.updated_at = Utc::now();
            }
        }
        Ok(())
    }
}

impl Default for MemoryAdapter {
    fn default() -> Self {
        Self::new()
    }
}
