use ledger::adapters::postgres::PostgresLedgerAdapter;

use super::PostgresAdapter;

impl PostgresLedgerAdapter for PostgresAdapter
where
    PostgresAdapter: Send + Sync,
{
    fn get_pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }
}
