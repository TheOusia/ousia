pub mod adapters;
pub mod edge;
pub mod error;
pub mod object;
pub mod query;

#[cfg(feature = "ledger")]
pub use ledger;

use std::sync::Arc;

pub use crate::adapters::{Adapter, EdgeRecord, ObjectRecord, Query, QueryContext};
pub use crate::edge::meta::*;
pub use crate::edge::query::EdgeQuery;
pub use crate::edge::traits::*;
pub use crate::error::Error;
pub use crate::object::*;
use crate::query::QueryFilter;
use chrono::Utc;
pub use query::IndexQuery;
use uuid::Uuid;

#[cfg(feature = "derive")]
pub use ousia_derive::*;

pub struct ReplicaConfig {
    pub url: String,
}

/// The Engine is the primary interface for interacting with domain objects and edges.
/// It abstracts away storage details and provides a type-safe API.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Ousia>,
}

pub struct Ousia {
    adapter: Box<dyn Adapter>,
}

impl Engine {
    pub fn new(adapter: Box<dyn Adapter>) -> Self {
        Self {
            inner: Arc::new(Ousia { adapter: adapter }),
        }
    }

    // ==================== Object CRUD ====================
    /// Create a new object in storage
    pub async fn create_object<T: Object>(&self, obj: &T) -> Result<(), Error> {
        if !T::HAS_UNIQUE_FIELDS {
            self.inner
                .adapter
                .insert_object(ObjectRecord::from_object(obj))
                .await?;
        } else {
            let unique_hashes = obj.derive_unique_hashes();

            self.inner
                .adapter
                .insert_unique_hashes(obj.id(), obj.type_name(), unique_hashes)
                .await?;
            self.inner
                .adapter
                .insert_object(ObjectRecord::from_object(obj))
                .await?;
        }

        Ok(())
    }

    /// Fetch an object by ID
    pub async fn fetch_object<T: Object>(&self, id: Uuid) -> Result<Option<T>, Error> {
        let val = self.inner.adapter.fetch_object(id).await?;
        match val {
            Some(record) => record.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Fetch multiple objects by IDs
    pub async fn fetch_objects<T: Object>(&self, ids: Vec<Uuid>) -> Result<Vec<T>, Error> {
        let records = self.inner.adapter.fetch_bulk_objects(ids).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Update an existing object
    pub async fn update_object<T: Object>(&self, obj: &mut T) -> Result<(), Error> {
        let meta = obj.meta_mut();
        meta.updated_at = Utc::now();

        if !T::HAS_UNIQUE_FIELDS {
            // No unique fields, just update the object
            self.inner
                .adapter
                .update_object(ObjectRecord::from_object(obj))
                .await?;
        } else {
            let object_id = obj.id();
            let type_name = obj.type_name();

            // Get current hashes from database
            let old_hashes = self.inner.adapter.get_hashes_for_object(object_id).await?;

            // Get new hashes from the updated object
            let new_hashes = obj.derive_unique_hashes();

            // Determine which hashes to add and remove
            let hashes_to_add: Vec<_> = new_hashes
                .iter()
                .filter(|(hash, _)| !old_hashes.contains(hash))
                .cloned()
                .collect();

            let hashes_to_remove: Vec<_> = old_hashes
                .iter()
                .filter(|hash| !new_hashes.iter().any(|(h, _)| h == *hash))
                .cloned()
                .collect();

            // If nothing changed in unique fields, skip uniqueness operations
            if hashes_to_add.is_empty() && hashes_to_remove.is_empty() {
                // Just update the object
                self.inner
                    .adapter
                    .update_object(ObjectRecord::from_object(obj))
                    .await?;
            } else {
                // Try to insert new hashes (will fail if already taken)
                if !hashes_to_add.is_empty() {
                    self.inner
                        .adapter
                        .insert_unique_hashes(
                            object_id,
                            type_name,
                            hashes_to_add.iter().cloned().collect(),
                        )
                        .await?;
                }

                // Update the object
                match self
                    .inner
                    .adapter
                    .update_object(ObjectRecord::from_object(obj))
                    .await
                {
                    Ok(_) => (),
                    Err(err) => {
                        // Rollback the insertion of new hashes
                        if !hashes_to_add.is_empty() {
                            let hashes = hashes_to_add
                                .into_iter()
                                .map(|(hash, _)| hash)
                                .collect::<Vec<String>>();
                            self.inner.adapter.delete_unique_hashes(hashes).await?;
                        }
                        return Err(err);
                    }
                }

                // Clean up old hashes (only after successful update)
                if !hashes_to_remove.is_empty() {
                    for hash in hashes_to_remove {
                        self.inner.adapter.delete_unique(&hash).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Delete an object
    pub async fn delete_object<T: Object>(
        &self,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<T>, Error> {
        let record = self.inner.adapter.delete_object(id, owner).await?;

        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Transfer ownership of an object
    pub async fn transfer_object<T: Object>(
        &self,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<T, Error> {
        let record = self
            .inner
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
            .inner
            .adapter
            .find_object(T::TYPE, SYSTEM_OWNER, filters)
            .await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    pub async fn find_object_with_owner<T: Object>(
        &self,
        owner: Uuid,
        filters: &[QueryFilter],
    ) -> Result<Option<T>, Error> {
        let record = self
            .inner
            .adapter
            .find_object(T::TYPE, owner, filters)
            .await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    pub async fn query_objects<T: Object>(&self, query: Query) -> Result<Vec<T>, Error> {
        let records = self.inner.adapter.query_objects(T::TYPE, query).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Count objects matching query
    pub async fn count_objects<T: Object>(&self, query: Option<Query>) -> Result<u64, Error> {
        self.inner.adapter.count_objects(T::TYPE, query).await
    }

    /// Fetch all objects owned by a specific owner
    pub async fn fetch_owned_objects<T: Object>(&self, owner: Uuid) -> Result<Vec<T>, Error> {
        let records = self
            .inner
            .adapter
            .fetch_owned_objects(T::TYPE, owner)
            .await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Fetch a single owned object (for one-to-one relationships)
    pub async fn fetch_owned_object<T: Object>(&self, owner: Uuid) -> Result<Option<T>, Error> {
        let record = self
            .inner
            .adapter
            .fetch_owned_object(T::TYPE, owner)
            .await?;
        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    // ==================== Union Operations ====================
    /// Fetch an union by ID
    pub async fn fetch_union_object<A: Object, B: Object>(
        &self,
        id: Uuid,
    ) -> Result<Option<Union<A, B>>, Error> {
        let record = self
            .inner
            .adapter
            .fetch_union_object(A::TYPE, B::TYPE, id)
            .await?;
        match record {
            Some(r) => Ok(Some(r.into())),
            None => Ok(None),
        }
    }

    pub async fn fetch_union_objects<A: Object, B: Object>(
        &self,
        id: Vec<Uuid>,
    ) -> Result<Vec<Union<A, B>>, Error> {
        let records = self
            .inner
            .adapter
            .fetch_union_objects(A::TYPE, B::TYPE, id)
            .await?;
        records.into_iter().map(|r| Ok(r.into())).collect()
    }

    pub async fn fetch_owned_union_object<A: Object, B: Object>(
        &self,
        owner: Uuid,
    ) -> Result<Option<Union<A, B>>, Error> {
        let record = self
            .inner
            .adapter
            .fetch_owned_union_object(A::TYPE, B::TYPE, owner)
            .await?;
        match record {
            Some(r) => Ok(Some(r.into())),
            None => Ok(None),
        }
    }

    pub async fn fetch_owned_union_objects<A: Object, B: Object>(
        &self,
        owner: Uuid,
    ) -> Result<Vec<Union<A, B>>, Error> {
        let records = self
            .inner
            .adapter
            .fetch_owned_union_objects(A::TYPE, B::TYPE, owner)
            .await?;
        records.into_iter().map(|r| Ok(r.into())).collect()
    }

    // ==================== Edge Operations ====================

    /// Create a new edge
    pub async fn create_edge<E: Edge>(&self, edge: &E) -> Result<(), Error> {
        self.inner
            .adapter
            .insert_edge(EdgeRecord::from_edge(edge))
            .await
    }

    /// Update an edge
    pub async fn update_edge<E: Edge>(&self, edge: &mut E, to: Option<Uuid>) -> Result<(), Error> {
        let old_link_id = edge.to();
        if let Some(to) = to {
            edge.meta_mut().to = to;
        }

        let _ = self
            .inner
            .adapter
            .update_edge(EdgeRecord::from_edge(edge), old_link_id, to)
            .await?;

        Ok(())
    }

    /// Delete an edge
    pub async fn delete_edge<E: Edge>(&self, from: Uuid, to: Uuid) -> Result<(), Error> {
        self.inner.adapter.delete_edge(E::TYPE, from, to).await
    }

    /// Delete all edge of an object
    pub async fn delete_object_edge<E: Edge>(&self, from: Uuid) -> Result<(), Error> {
        self.inner.adapter.delete_object_edge(E::TYPE, from).await
    }

    /// Query edges
    pub async fn query_edges<E: Edge>(
        &self,
        from: Uuid,
        query: EdgeQuery,
    ) -> Result<Vec<E>, Error> {
        let records = self.inner.adapter.query_edges(E::TYPE, from, query).await?;
        records.into_iter().map(|r| r.to_edge()).collect()
    }

    /// Query reverse edges
    pub async fn query_reverse_edges<E: Edge>(
        &self,
        to: Uuid,
        query: EdgeQuery,
    ) -> Result<Vec<E>, Error> {
        let records = self
            .inner
            .adapter
            .query_reverse_edges(E::TYPE, to, query)
            .await?;
        records.into_iter().map(|r| r.to_edge()).collect()
    }

    /// Count edges
    pub async fn count_edges<E: Edge>(
        &self,
        from: Uuid,
        query: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        self.inner.adapter.count_edges(E::TYPE, from, query).await
    }

    /// Count reverse edges
    pub async fn count_reverse_edges<E: Edge>(
        &self,
        to: Uuid,
        query: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        self.inner
            .adapter
            .count_reverse_edges(E::TYPE, to, query)
            .await
    }

    // ==================== Sequence ====================
    pub async fn counter_value(&self, key: String) -> u64 {
        self.inner.adapter.sequence_value(key).await
    }

    pub async fn counter_next_value(&self, key: String) -> u64 {
        self.inner.adapter.sequence_next_value(key).await
    }

    // ==================== Advanced Query API ====================

    /// Start a query context for complex traversals
    pub fn preload_object<'a, T: Object>(&'a self, id: Uuid) -> QueryContext<'a, T> {
        self.inner.adapter.preload_object(id)
    }
}
