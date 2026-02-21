mod adapter_impl;
mod helper;
mod traversal_impl;
mod unique_impl;

#[cfg(feature = "ledger")]
mod ledger_impl;

use sqlx::PgPool;

use crate::adapters::Error;

/// PostgreSQL adapter using a unified JSON storage model
///
/// Schema:
/// ```sql
/// CREATE TABLE public.objects (
///     id uuid PRIMARY KEY,
///     type TEXT NOT NULL,
///     owner uuid NOT NULL,
///     created_at TIMESTAMPTZ NOT NULL,
///     updated_at TIMESTAMPTZ NOT NULL,
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL
/// );
///
/// -- type is always bound; owner on scoped queries; id DESC for default cursor pagination
/// CREATE INDEX idx_objects_type_owner ON objects(type, owner, id DESC);
/// -- composite date indexes: planner uses these when user sorts by created_at / updated_at
/// CREATE INDEX idx_objects_type_owner_created ON objects(type, owner, created_at DESC);
/// CREATE INDEX idx_objects_type_owner_updated ON objects(type, owner, updated_at DESC);
/// -- GIN index for index_meta search/filter operations
/// CREATE INDEX idx_objects_index_meta ON public.objects USING GIN (index_meta);
/// ```
pub struct PostgresAdapter {
    pub(crate) pool: PgPool,
}

impl PostgresAdapter {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the database schema
    pub async fn init_schema(&self) -> Result<(), Error> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| Error::Storage(err.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.objects (
                id uuid PRIMARY KEY,
                type TEXT NOT NULL,
                owner uuid NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                data JSONB NOT NULL,
                index_meta JSONB NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner
                ON objects(type, owner, id DESC)
                INCLUDE (created_at, updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_created
                ON objects(type, owner, created_at DESC)
                INCLUDE (id, updated_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_type_owner_updated
                ON objects(type, owner, updated_at DESC)
                INCLUDE (id, created_at);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_objects_index_meta
                ON public.objects USING GIN (index_meta jsonb_path_ops);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS public.edges (
                "from" uuid NOT NULL,
                "to" uuid NOT NULL,
                type TEXT NOT NULL,
                data JSONB NOT NULL,
                index_meta JSONB NOT NULL
            );
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edges_key ON public.edges("from", "to", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_from_key ON public.edges("from", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_to_key ON public.edges("to", type);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_edges_index_meta
                ON public.edges USING GIN (index_meta jsonb_path_ops);
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE TABLE IF NOT EXISTS unique_constraints (
                        id UUID NOT NULL,
                        type TEXT NOT NULL,
                        key TEXT NOT NULL UNIQUE,
                        field TEXT NOT NULL,
                        PRIMARY KEY (type, key)
                    )
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE INDEX IF NOT EXISTS idx_unique_id
                    ON unique_constraints(id)
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
                    CREATE INDEX IF NOT EXISTS idx_unique_type_key
                    ON unique_constraints(type, key)
                    "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sequences (
                name TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 1
            )
            "#,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        #[cfg(feature = "ledger")]
        {
            use ledger::adapters::postgres::PostgresSchemaLedgerAdapter;

            self.init_ledger_schema().await.map_err(|me| match me {
                ledger::MoneyError::Storage(e) => Error::Storage(e),
                _ => Error::Storage(me.to_string()),
            })?;
        }
        Ok(())
    }
}
