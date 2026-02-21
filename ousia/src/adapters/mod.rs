#[cfg(feature = "cockroach")]
pub mod cockroach;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod query;
pub mod record;

#[cfg(feature = "ledger")]
use std::sync::Arc;

use async_trait::async_trait;
pub use query::*;
pub use record::*;
use uuid::Uuid;

use crate::{Object, edge::query::EdgeQuery, error::Error, query::QueryFilter};

/// -----------------------------
/// Adapter contract
/// -----------------------------

#[async_trait]
pub trait UniqueAdapter {
    async fn insert_unique_hashes(
        &self,
        type_name: &str,
        object_id: Uuid,
        hashes: Vec<(String, &str)>,
    ) -> Result<(), Error>;

    async fn delete_unique(&self, hash: &str) -> Result<(), Error>;
    async fn delete_unique_hashes(&self, hashes: Vec<String>) -> Result<(), Error>;

    async fn get_hashes_for_object(&self, object_id: Uuid) -> Result<Vec<String>, Error>;
}

#[async_trait]
pub trait EdgeTraversal {
    async fn fetch_object_from_edge_traversal_internal(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error>;

    async fn fetch_object_from_edge_reverse_traversal_internal(
        &self,
        edge_type_name: &str,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error>;

    // ── Batch traversal (multiple pivots, single query each) ─────────────────

    /// Forward JOIN for multiple source IDs — edges WHERE "from" = ANY(ids) + target objects.
    async fn query_edges_with_targets_batch(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        from_ids: &[Uuid],
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error>;

    /// Reverse JOIN for multiple target IDs — edges WHERE "to" = ANY(ids) + source objects.
    async fn query_reverse_edges_with_sources_batch(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        to_ids: &[Uuid],
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error>;

    /// Forward edge records only for multiple sources — no object JOIN.
    async fn query_edges_batch(
        &self,
        edge_type: &'static str,
        from_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error>;

    /// Reverse edge records only for multiple targets — no object JOIN.
    async fn query_reverse_edges_batch(
        &self,
        edge_type: &'static str,
        to_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error>;

    /// Single pivot, both directions in one UNION query, with objects.
    /// Returns (forward_results, reverse_results).
    async fn query_edges_both_directions_with_objects(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        pivot: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<(Vec<(EdgeRecord, ObjectRecord)>, Vec<(EdgeRecord, ObjectRecord)>), Error>;

    /// Single pivot, both directions in one UNION query, edges only.
    /// Returns (forward_edges, reverse_edges).
    async fn query_edges_both_directions(
        &self,
        edge_type: &'static str,
        pivot: Uuid,
        plan: EdgeQuery,
    ) -> Result<(Vec<EdgeRecord>, Vec<EdgeRecord>), Error>;

    /// Count forward edges per source in one GROUP BY query — Vec<(from_id, count)>.
    async fn count_edges_batch(
        &self,
        edge_type: &'static str,
        from_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<(Uuid, u64)>, Error>;

    /// Count reverse edges per target in one GROUP BY query — Vec<(to_id, count)>.
    async fn count_reverse_edges_batch(
        &self,
        edge_type: &'static str,
        to_ids: &[Uuid],
        plan: EdgeQuery,
    ) -> Result<Vec<(Uuid, u64)>, Error>;
}

#[async_trait]
pub trait Adapter: UniqueAdapter + EdgeTraversal + Send + Sync + 'static {
    /* ---------------- OBJECTS ---------------- */
    async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error>;
    async fn fetch_object(
        &self,
        type_name: &'static str,
        id: Uuid,
    ) -> Result<Option<ObjectRecord>, Error>;
    async fn fetch_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error>;
    async fn update_object(&self, record: ObjectRecord) -> Result<(), Error>;

    /// Explicit ownership transfer
    async fn transfer_object(
        &self,
        type_name: &'static str,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<ObjectRecord, Error>;

    async fn delete_object(
        &self,
        type_name: &'static str,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error>;

    async fn delete_bulk_objects(
        &self,
        type_name: &'static str,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error>;

    async fn delete_owned_objects(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<u64, Error>;

    /* ---------------- QUERIES ---------------- */
    /// Fetch ALL objects matching `plan`. Filters by owner.
    async fn find_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
        filters: &[QueryFilter],
    ) -> Result<Option<ObjectRecord>, Error>;

    async fn query_objects(
        &self,
        type_name: &'static str,
        plan: Query,
    ) -> Result<Vec<ObjectRecord>, Error>;

    async fn count_objects(
        &self,
        type_name: &'static str,
        plan: Option<Query>,
    ) -> Result<u64, Error>;

    /// Fetch ALL objects owned by `owner`
    async fn fetch_owned_objects(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error>;

    /// Batch-fetch owned children for multiple parents in one query.
    /// Each returned ObjectRecord's `.owner` field is the parent ID — use it to group.
    async fn fetch_owned_objects_batch(
        &self,
        type_name: &'static str,
        owner_ids: &[Uuid],
    ) -> Result<Vec<ObjectRecord>, Error>;

    /// Fetch a SINGLE owned object (O2O)
    async fn fetch_owned_object(
        &self,
        type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error>;

    // ==================== Union Operations ====================
    async fn fetch_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        id: Uuid,
    ) -> Result<Option<ObjectRecord>, Error>;

    async fn fetch_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        id: Vec<Uuid>,
    ) -> Result<Vec<ObjectRecord>, Error>;

    async fn fetch_owned_union_object(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Option<ObjectRecord>, Error>;

    async fn fetch_owned_union_objects(
        &self,
        a_type_name: &'static str,
        b_type_name: &'static str,
        owner: Uuid,
    ) -> Result<Vec<ObjectRecord>, Error>;

    /* ---------------- EDGES ---------------- */
    async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error>;
    async fn update_edge(
        &self,
        record: EdgeRecord,
        old_to: Uuid,
        to: Option<Uuid>,
    ) -> Result<(), Error>;
    async fn delete_edge(&self, type_name: &'static str, from: Uuid, to: Uuid)
    -> Result<(), Error>;

    async fn delete_object_edge(&self, type_name: &'static str, from: Uuid) -> Result<(), Error>;

    async fn query_edges(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error>;

    async fn query_reverse_edges(
        &self,
        type_name: &'static str,
        owner_reverse: Uuid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error>;

    /// Single JOIN query: edges WHERE "from" = owner + their target objects.
    async fn query_edges_with_targets(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        owner: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error>;

    /// Single JOIN query: edges WHERE "to" = owner + their source objects.
    async fn query_reverse_edges_with_sources(
        &self,
        edge_type: &'static str,
        obj_type: &'static str,
        owner: Uuid,
        obj_filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<(EdgeRecord, ObjectRecord)>, Error>;

    async fn count_edges(
        &self,
        type_name: &'static str,
        owner: Uuid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error>;

    async fn count_reverse_edges(
        &self,
        type_name: &'static str,
        to: Uuid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error>;

    /* ---------------- SEQUENCE ---------------- */
    async fn sequence_value(&self, sq: String) -> u64;
    async fn sequence_next_value(&self, sq: String) -> u64;

    /* ---------------- LEDGER ---------------- */
    #[cfg(feature = "ledger")]
    fn ledger_adapter(&self) -> Option<Arc<dyn ledger::LedgerAdapter>> {
        None // default — adapters opt in
    }
}

impl dyn Adapter {
    pub fn preload_object<'a, T: Object>(&'a self, id: Uuid) -> QueryContext<'a, T> {
        QueryContext::new(self, id)
    }

    pub fn preload_objects<'a, P: Object>(&'a self, query: Query) -> MultiPreloadContext<'a, P> {
        MultiPreloadContext::new(self, query)
    }
}
