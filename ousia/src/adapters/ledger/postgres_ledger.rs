use ledger::{Asset, Balance, ExecutionPlan, LedgerAdapter, MoneyError, Transaction};
use uuid::Uuid;

use crate::adapters::postgres::PostgresAdapter;

impl PostgresAdapter {
    pub async fn init_ledger_schema(&self) -> Result<(), MoneyError> {
        todo!()
    }
}

#[async_trait::async_trait]
impl LedgerAdapter for PostgresAdapter {
    async fn execute_plan(
        &self,
        plan: &ExecutionPlan,
        locks: &[(Uuid, Uuid, u64)],
    ) -> Result<(), MoneyError> {
        todo!()
    }

    // === TRANSACTION CONTROL ===

    async fn begin_transaction(&self) -> Result<(), MoneyError> {
        todo!()
    }
    async fn commit_transaction(&self) -> Result<(), MoneyError> {
        todo!()
    }
    async fn rollback_transaction(&self) -> Result<(), MoneyError> {
        todo!()
    }

    // === READ OPERATIONS ===

    async fn get_balance(&self, asset_id: Uuid, owner: Uuid) -> Result<Balance, MoneyError> {
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
