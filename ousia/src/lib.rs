pub mod adapters;
pub mod edge;
pub mod error;
pub mod object;
pub mod query;

pub use crate::adapters::{Adapter, EdgeRecord, ObjectRecord, Query, QueryContext};
pub use crate::edge::meta::*;
pub use crate::edge::query::EdgeQuery;
pub use crate::edge::traits::*;
pub use crate::error::Error;
pub use crate::object::*;
use crate::query::QueryFilter;
use chrono::Utc;
pub use query::IndexQuery;
use ulid::Ulid;

#[cfg(feature = "derive")]
pub use ousia_derive::*;

/// The Engine is the primary interface for interacting with domain objects and edges.
/// It abstracts away storage details and provides a type-safe API.
pub struct Engine {
    adapter: Box<dyn Adapter>,
}

impl Engine {
    pub fn new(adapter: Box<dyn Adapter>) -> Self {
        Self { adapter }
    }

    // ==================== Object CRUD ====================

    /// Create a new object in storage
    pub async fn create_object<T: Object>(&self, obj: &T) -> Result<(), Error> {
        self.adapter
            .insert_object(ObjectRecord::from_object(obj))
            .await
    }

    /// Fetch an object by ID
    pub async fn fetch_object<T: Object>(&self, id: Ulid) -> Result<Option<T>, Error> {
        let val = self.adapter.fetch_object(id).await?;
        match val {
            Some(record) => record.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Fetch multiple objects by IDs
    pub async fn fetch_objects<T: Object>(&self, ids: Vec<Ulid>) -> Result<Vec<T>, Error> {
        let records = self.adapter.fetch_bulk_objects(ids).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Update an existing object
    pub async fn update_object<T: Object>(&self, obj: &mut T) -> Result<(), Error> {
        let meta = obj.meta_mut();
        meta.updated_at = Utc::now();

        self.adapter
            .update_object(ObjectRecord::from_object(obj))
            .await
    }

    /// Delete an object
    pub async fn delete_object<T: Object>(
        &self,
        id: Ulid,
        owner: Ulid,
    ) -> Result<Option<T>, Error> {
        let record = self.adapter.delete_object(id, owner).await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Transfer ownership of an object
    pub async fn transfer_object<T: Object>(
        &self,
        id: Ulid,
        from_owner: Ulid,
        to_owner: Ulid,
    ) -> Result<T, Error> {
        let record = self
            .adapter
            .transfer_object(id, from_owner, to_owner)
            .await?;
        record.to_object()
    }

    // ==================== Object Queries ====================

    /// Query objects with filters
    pub async fn find_object<T: Object>(
        &self,
        filters: &[QueryFilter],
    ) -> Result<Option<T>, Error> {
        let record = self
            .adapter
            .find_object(T::TYPE, *SYSTEM_OWNER, filters)
            .await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    pub async fn find_object_with_owner<T: Object>(
        &self,
        owner: Ulid,
        filters: &[QueryFilter],
    ) -> Result<Option<T>, Error> {
        let record = self.adapter.find_object(T::TYPE, owner, filters).await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    pub async fn query_objects<T: Object>(&self, query: Query) -> Result<Vec<T>, Error> {
        let records = self.adapter.query_objects(T::TYPE, query).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Count objects matching query
    pub async fn count_objects<T: Object>(&self, query: Option<Query>) -> Result<u64, Error> {
        self.adapter.count_objects(T::TYPE, query).await
    }

    /// Fetch all objects owned by a specific owner
    pub async fn fetch_owned_objects<T: Object>(&self, owner: Ulid) -> Result<Vec<T>, Error> {
        let records = self.adapter.fetch_owned_objects(T::TYPE, owner).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Fetch a single owned object (for one-to-one relationships)
    pub async fn fetch_owned_object<T: Object>(&self, owner: Ulid) -> Result<Option<T>, Error> {
        let record = self.adapter.fetch_owned_object(T::TYPE, owner).await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    // ==================== Edge Operations ====================

    /// Create a new edge
    pub async fn create_edge<E: Edge>(&self, edge: &E) -> Result<(), Error> {
        self.adapter.insert_edge(EdgeRecord::from_edge(edge)).await
    }

    /// Delete an edge
    pub async fn delete_edge<E: Edge>(&self, from: Ulid, to: Ulid) -> Result<(), Error> {
        self.adapter.delete_edge(E::TYPE, from, to).await
    }

    /// Query edges
    pub async fn query_edges<E: Edge>(
        &self,
        from: Ulid,
        query: EdgeQuery,
    ) -> Result<Vec<E>, Error> {
        let records = self.adapter.query_edges(E::TYPE, from, query).await?;
        records.into_iter().map(|r| r.to_edge()).collect()
    }

    /// Count edges
    pub async fn count_edges<E: Edge>(
        &self,
        from: Ulid,
        query: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        self.adapter.count_edges(E::TYPE, from, query).await
    }

    // ==================== Advanced Query API ====================

    /// Start a query context for complex traversals
    pub async fn preload_object<'a, T: Object>(&'a self, id: Ulid) -> QueryContext<'a, T> {
        self.adapter.preload_object(id).await
    }
}
