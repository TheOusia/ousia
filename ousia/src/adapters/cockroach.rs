use chrono::Utc;
use sqlx::{
    PgPool, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::{Query as PgQuery, QueryScalar},
};
use uuid::Uuid;

use crate::{
    adapters::{Adapter, EdgeQuery, EdgeRecord, Error, ObjectRecord, Query},
    query::{Cursor, IndexValue, IndexValueInner, QueryFilter},
};

/// CockroachDB adapter using a unified JSON storage model
///
/// CockroachDB is PostgreSQL-compatible but with distributed SQL capabilities.
/// This adapter leverages CockroachDB-specific features like:
/// - Automatic UUID generation with gen_random_uuid()
/// - JSONB indexing with inverted indexes
/// - Distributed transactions
///
/// Schema:
/// ```sql
/// CREATE TABLE public.objects (
///     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
///     type TEXT NOT NULL,
///     owner UUID NOT NULL,
///     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
///     updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL,
///     INDEX idx_objects_type_owner (type, owner),
///     INDEX idx_objects_owner (owner),
///     INDEX idx_objects_created_at (created_at),
///     INDEX idx_objects_updated_at (updated_at),
///     INVERTED INDEX idx_objects_index_meta (index_meta)
/// );
///
/// CREATE TABLE public.edges (
///     "from" UUID NOT NULL,
///     "to" UUID NOT NULL,
///     type TEXT NOT NULL,
///     data JSONB NOT NULL,
///     index_meta JSONB NOT NULL,
///     PRIMARY KEY ("from", "to", type),
///     INDEX idx_edges_from_type ("from", type),
///     INDEX idx_edges_to_type ("to", type),
///     INVERTED INDEX idx_edges_index_meta (index_meta)
/// );
/// ```
pub struct CockroachAdapter {
    pub(crate) pool: PgPool,
}
