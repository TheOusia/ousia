//! # Ousia
//!
//! *οὐσία — Ancient Greek for "essence" or "substance".*
//!
//! Ousia is a Postgres-native ORM for Rust that ships with a built-in
//! **double-entry ledger** as a first-class primitive. It is designed for
//! applications where data, relationships, and money all need to move
//! together — atomically, correctly, and without ceremony.
//!
//! ## What's inside
//!
//! ### Graph-relational ORM
//! Model your domain as entities connected by typed edges. Relations are
//! not just foreign keys — they are traversable, queryable graph
//! connections backed by Postgres.
//!
//! ### Double-entry ledger
//! Every monetary operation — mint, burn, transfer, reserve — is a
//! double-entry transaction. Nothing is deleted. Everything is auditable.
//! The ledger is ACID-safe and lives in the same Postgres connection as
//! your application data.
//!
//! ```rust,ignore
//! Money::atomic(&ctx, |tx| async move {
//!     // Lock $60 from user, mint $60 to merchant atomically.
//!     let money = tx.money("USD", user, 60_00).await?;
//!     let slice = money.slice(60_00)?;
//!     slice.transfer_to(merchant, "payment".to_string()).await?;
//!     Ok(())
//! })
//! .await?;
//! ```
//!
//! ### Smart fragmentation
//! Balances are stored as **value objects** — discrete fragments of value.
//! The fragmentation engine uses your asset's natural denomination as a
//! soft preferred chunk size, with a hard fragment cap (`max_fragments`)
//! that automatically scales chunk size up when needed. Every spend is a
//! consolidation opportunity: change is minted back into at most
//! `burned_count` fragments, so active accounts stay lean over time
//! without any background compaction job.
//!
//! ### FIFO aging
//! Value objects are selected oldest-first on every spend. Burned rows
//! naturally age to the back of the live index and become eligible for
//! cold-storage archival, keeping your hot dataset small.
//!
//! ### Atomic money operations
//! The `Money` API enforces correct usage at the type level:
//! - **Mint** — create value out of thin air (deposits, issuance)
//! - **Burn** — destroy value permanently (fees, redemptions)
//! - **Transfer** — move value between owners atomically
//! - **Reserve** — escrow value for a future authority
//! - **Slice** — partition a money handle before spending
//!
//! Unconsumed slices, over-slicing, and double-spend are all caught
//! before hitting the database.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use ousia::{Engine, adapters::postgres::PostgresAdapter};
//!
//! let adapter = PostgresAdapter::from_pool(pool);
//! adapter.init_schema().await?;
//!
//! let engine = Engine::new(Box::new(adapter));
//! let ctx = engine.ledger_ctx();
//! ```
//!
//! ## Feature flags
//!
//! | Flag       | Default | Description                        |
//! |------------|---------|------------------------------------|
//! | `postgres` | ✓       | PostgreSQL adapter via sqlx         |
//! | `cockroach` | ✓       | CockroachDB adapter via sqlx         |
//! | `sqlite`   |         | SQLite adapter (in-memory or file)  |
//!
//! ## Ousia
//!
//! *Ousia* (οὐσία) is Aristotle's term for the fundamental substance of
//! a thing — what it is at its core. The name reflects the library's
//! ambition: to be the essential data substrate of a Rust application,
//! handling entities, relationships, and money in one coherent layer.
//!

pub mod adapters;
pub mod edge;
pub mod error;
pub mod object;
pub mod query;

#[cfg(feature = "ledger")]
pub use ledger;
use metrics::histogram;

use std::sync::Arc;
use std::time::Instant;

pub use crate::adapters::{
    Adapter, EdgeRecord, MultiEdgeContext, MultiOwnedContext, MultiPreloadContext, ObjectRecord,
    Query, QueryContext,
};
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
    #[cfg(feature = "ledger")]
    ledger: Option<Arc<dyn ledger::LedgerAdapter>>,
}

impl Engine {
    pub fn new(adapter: Box<dyn Adapter>) -> Self {
        #[cfg(feature = "ledger")]
        let ledger = adapter.ledger_adapter();

        Self {
            inner: Arc::new(Ousia {
                adapter: adapter,
                #[cfg(feature = "ledger")]
                ledger,
            }),
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
                .insert_unique_hashes(obj.type_name(), obj.id(), unique_hashes)
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
        let val = self.inner.adapter.fetch_object(T::TYPE, id).await?;
        match val {
            Some(record) => record.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Fetch multiple objects by IDs
    pub async fn fetch_objects<T: Object>(&self, ids: Vec<Uuid>) -> Result<Vec<T>, Error> {
        let records = self.inner.adapter.fetch_bulk_objects(T::TYPE, ids).await?;
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
                            type_name,
                            object_id,
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
        let record = self.inner.adapter.delete_object(T::TYPE, id, owner).await?;

        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    pub async fn delete_objects<T: Object>(
        &self,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        let record = self
            .inner
            .adapter
            .delete_bulk_objects(T::TYPE, ids, owner)
            .await?;

        Ok(record)
    }

    pub async fn delete_owned_objects<T: Object>(&self, owner: Uuid) -> Result<u64, Error> {
        let record = self
            .inner
            .adapter
            .delete_owned_objects(T::TYPE, owner)
            .await?;

        Ok(record)
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
            .transfer_object(T::TYPE, id, from_owner, to_owner)
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
        let start = Instant::now();
        let records = self.inner.adapter.query_objects(T::TYPE, query).await?;
        histogram!("ousia.query.duration_ms",
            "type" => T::TYPE
        )
        .record(start.elapsed().as_millis() as f64);
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
        let start = Instant::now();
        let records = self.inner.adapter.query_edges(E::TYPE, from, query).await?;
        histogram!("ousia.query_edges.duration_ms",
            "type" => E::TYPE
        )
        .record(start.elapsed().as_millis() as f64);
        records.into_iter().map(|r| r.to_edge()).collect()
    }

    /// Query reverse edges
    pub async fn query_reverse_edges<E: Edge>(
        &self,
        to: Uuid,
        query: EdgeQuery,
    ) -> Result<Vec<E>, Error> {
        let start = Instant::now();
        let records = self
            .inner
            .adapter
            .query_reverse_edges(E::TYPE, to, query)
            .await?;
        histogram!("ousia.query_edges.duration_ms",
            "type" => E::TYPE
        )
        .record(start.elapsed().as_millis() as f64);
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

    /// Start a single-pivot query context for edge traversals.
    pub fn preload_object<'a, T: Object>(&'a self, id: Uuid) -> QueryContext<'a, T> {
        self.inner.adapter.preload_object(id)
    }

    /// Start a multi-pivot query context. Fetches parents first, then batch-joins edges/children.
    /// All terminal methods execute exactly 2 queries — never N+1.
    pub fn preload_objects<'a, P: Object>(
        &'a self,
        query: Query,
    ) -> MultiPreloadContext<'a, P> {
        self.inner.adapter.preload_objects(query)
    }

    #[cfg(feature = "ledger")]
    pub fn ledger(&self) -> &Arc<dyn ledger::LedgerAdapter> {
        let ledger = self
            .inner
            .ledger
            .as_ref()
            .expect("This adapter does not support the ledger. Use PostgresAdapter.");

        ledger
    }

    #[cfg(feature = "ledger")]
    pub fn ledger_ctx(&self) -> ledger::LedgerContext {
        let arc = self
            .inner
            .ledger
            .as_ref()
            .expect("This adapter does not support the ledger. Use PostgresAdapter.");

        ledger::LedgerContext::new(Arc::clone(arc))
    }
}
