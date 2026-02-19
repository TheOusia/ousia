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
pub(crate) trait UniqueAdapter {
    async fn insert_unique(
        &self,
        type_name: &str,
        object_id: Uuid,
        hash: &str,
        field: &str,
    ) -> Result<(), Error>;

    async fn insert_unique_hashes(
        &self,
        type_name: &str,
        object_id: Uuid,
        hashes: Vec<(String, &str)>,
    ) -> Result<(), Error>;

    async fn delete_unique(&self, hash: &str) -> Result<(), Error>;
    async fn delete_unique_hashes(&self, hashes: Vec<String>) -> Result<(), Error>;

    async fn delete_all_for_object(&self, object_id: Uuid) -> Result<(), Error>;

    async fn get_hashes_for_object(&self, object_id: Uuid) -> Result<Vec<String>, Error>;
}

#[async_trait]
pub(crate) trait EdgeTraversal {
    async fn fetch_object_from_edge_traversal_internal(
        &self,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error>;

    async fn fetch_object_from_edge_reverse_traversal_internal(
        &self,
        type_name: &str,
        owner: Uuid,
        filters: &[QueryFilter],
        plan: EdgeQuery,
    ) -> Result<Vec<ObjectRecord>, Error>;
}

#[allow(private_bounds)]
#[async_trait]
pub trait Adapter: UniqueAdapter + Send + Sync + 'static {
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
}
