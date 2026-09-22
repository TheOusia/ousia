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
    /// Create a new object in storage. On adapters that support it, the row,
    /// its unique keys and geo points are written in one transaction.
    pub async fn create_object<T: Object>(&self, obj: &T) -> Result<(), Error> {
        let unique = if T::HAS_UNIQUE_FIELDS { obj.derive_unique_hashes() } else { vec![] };
        let geo = if T::HAS_GEO_FIELDS { obj.geo_points() } else { vec![] };
        match self
            .inner
            .adapter
            .create_object_atomic(ObjectRecord::from_object(obj), unique, geo)
            .await
        {
            Err(Error::Unsupported(_)) => self.create_object_sequential(obj).await,
            other => other,
        }
    }

    async fn create_object_sequential<T: Object>(&self, obj: &T) -> Result<(), Error> {
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

        if T::HAS_GEO_FIELDS {
            let points = obj.geo_points();
            if !points.is_empty() {
                self.inner
                    .adapter
                    .upsert_geo_points(obj.type_name(), obj.id(), points)
                    .await?;
            }
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

    /// Update an existing object. On adapters that support it, the row, its
    /// unique keys and geo points are updated in one transaction.
    pub async fn update_object<T: Object>(&self, obj: &mut T) -> Result<(), Error> {
        obj.meta_mut().updated_at = Utc::now();
        let unique = T::HAS_UNIQUE_FIELDS.then(|| obj.derive_unique_hashes());
        let geo = T::HAS_GEO_FIELDS.then(|| obj.geo_points());
        match self
            .inner
            .adapter
            .update_object_atomic(ObjectRecord::from_object(obj), unique, geo)
            .await
        {
            Err(Error::Unsupported(_)) => self.update_object_sequential(obj).await,
            other => other,
        }
    }

    async fn update_object_sequential<T: Object>(&self, obj: &mut T) -> Result<(), Error> {

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

        if T::HAS_GEO_FIELDS {
            let object_id = obj.id();
            let new_points = obj.geo_points();
            let old_hashes: std::collections::HashMap<String, String> = self
                .inner
                .adapter
                .get_geo_hashes(object_id)
                .await?
                .into_iter()
                .collect();

            let new_fields: std::collections::HashSet<&str> =
                new_points.iter().map(|p| p.field).collect();

            // Points whose (field, hash) differs from the stored row — these
            // need to be UPSERTed. Points whose hash is unchanged are skipped.
            let points_to_upsert: Vec<crate::query::GeoPoint> = new_points
                .iter()
                .filter(|p| old_hashes.get(p.field).map(|h| h != &p.hash).unwrap_or(true))
                .cloned()
                .collect();

            // Fields that existed before but are no longer produced by the
            // object — these rows must be deleted.
            let fields_to_delete: Vec<String> = old_hashes
                .keys()
                .filter(|f| !new_fields.contains(f.as_str()))
                .cloned()
                .collect();

            if !points_to_upsert.is_empty() {
                self.inner
                    .adapter
                    .upsert_geo_points(obj.type_name(), object_id, points_to_upsert)
                    .await?;
            }
            if !fields_to_delete.is_empty() {
                self.inner
                    .adapter
                    .delete_geo_fields(object_id, fields_to_delete)
                    .await?;
            }
        }

        Ok(())
    }

    /// Delete an object, with its unique keys and geo rows (in one transaction
    /// on adapters that support it).
    pub async fn delete_object<T: Object>(
        &self,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<T>, Error> {
        match self
            .inner
            .adapter
            .delete_object_atomic(T::TYPE, id, owner, T::HAS_UNIQUE_FIELDS, T::HAS_GEO_FIELDS)
            .await
        {
            Err(Error::Unsupported(_)) => self.delete_object_sequential(id, owner).await,
            Err(e) => Err(e),
            Ok(record) => record.map(|r| r.to_object()).transpose(),
        }
    }

    async fn delete_object_sequential<T: Object>(
        &self,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<T>, Error> {
        let record = self.inner.adapter.delete_object(T::TYPE, id, owner).await?;

        if record.is_some() {
            if T::HAS_UNIQUE_FIELDS {
                self.inner.adapter.delete_unique_for_object(id).await?;
            }
            if T::HAS_GEO_FIELDS {
                self.inner.adapter.delete_geo_for_object(id).await?;
            }
        }

        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Delete `ids` owned by `owner`, with their unique keys and geo rows (in
    /// one transaction on adapters that support it). Returns the rows deleted.
    pub async fn delete_objects<T: Object>(
        &self,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        match self
            .inner
            .adapter
            .delete_objects_atomic(T::TYPE, owner, Some(ids.clone()), T::HAS_UNIQUE_FIELDS, T::HAS_GEO_FIELDS)
            .await
        {
            Err(Error::Unsupported(_)) => self.delete_objects_sequential::<T>(ids, owner).await,
            other => other,
        }
    }

    async fn delete_objects_sequential<T: Object>(
        &self,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        let cleanup_ids = if T::HAS_UNIQUE_FIELDS || T::HAS_GEO_FIELDS {
            ids.clone()
        } else {
            Vec::new()
        };
        // Unique cleanup runs BEFORE the object delete so the join in
        // `delete_unique_for_objects` can resolve which ids actually belong to
        // (type, owner) and avoid touching unrelated hashes.
        if T::HAS_UNIQUE_FIELDS {
            self.inner
                .adapter
                .delete_unique_for_objects(T::TYPE, owner, cleanup_ids.clone())
                .await?;
        }
        let record = self
            .inner
            .adapter
            .delete_bulk_objects(T::TYPE, ids, owner)
            .await?;
        if T::HAS_GEO_FIELDS {
            for id in cleanup_ids {
                self.inner.adapter.delete_geo_for_object(id).await?;
            }
        }

        Ok(record)
    }

    /// Delete every `T` owned by `owner`, with their unique keys and geo rows
    /// (in one transaction on adapters that support it).
    pub async fn delete_owned_objects<T: Object>(&self, owner: Uuid) -> Result<u64, Error> {
        match self
            .inner
            .adapter
            .delete_objects_atomic(T::TYPE, owner, None, T::HAS_UNIQUE_FIELDS, T::HAS_GEO_FIELDS)
            .await
        {
            Err(Error::Unsupported(_)) => self.delete_owned_objects_sequential::<T>(owner).await,
            other => other,
        }
    }

    async fn delete_owned_objects_sequential<T: Object>(&self, owner: Uuid) -> Result<u64, Error> {
        // Geo cleanup needs ids; capture them before the rows disappear.
        let geo_cleanup_ids: Vec<Uuid> = if T::HAS_GEO_FIELDS {
            self.fetch_owned_objects::<T>(owner)
                .await?
                .iter()
                .map(|o| o.id())
                .collect()
        } else {
            Vec::new()
        };
        // Unique cleanup must precede the object delete — the join resolves ids
        // through `objects`, so it has to run while those rows still exist.
        if T::HAS_UNIQUE_FIELDS {
            self.inner
                .adapter
                .delete_unique_for_owned(T::TYPE, owner)
                .await?;
        }
        let record = self
            .inner
            .adapter
            .delete_owned_objects(T::TYPE, owner)
            .await?;

        for id in geo_cleanup_ids {
            self.inner.adapter.delete_geo_for_object(id).await?;
        }

        Ok(record)
    }

    /// Transfer ownership of an object. Unique constraints that include
    /// `owner` are re-keyed (in the same transaction on adapters that support it).
    pub async fn transfer_object<T: Object>(
        &self,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<T, Error> {
        if !T::HAS_UNIQUE_FIELDS {
            return self.transfer_object_sequential(id, from_owner, to_owner).await;
        }
        // Unique keys are derived from a snapshot of the object; the adapter
        // only commits if the stored data is unchanged, else we re-read.
        const ATTEMPTS: usize = 3;
        for _ in 0..ATTEMPTS {
            let record = match self.inner.adapter.fetch_object(T::TYPE, id).await? {
                Some(record) if record.owner == from_owner => record,
                _ => return Err(Error::NotFound),
            };
            let snapshot = record.data.clone();
            let mut obj: T = record.to_object()?;
            obj.meta_mut().owner = to_owner;
            let unique = obj.derive_unique_hashes();
            match self
                .inner
                .adapter
                .transfer_object_atomic(T::TYPE, id, from_owner, to_owner, snapshot, unique)
                .await
            {
                Err(Error::Unsupported(_)) => {
                    return self.transfer_object_sequential(id, from_owner, to_owner).await;
                }
                Err(e) => return Err(e),
                Ok(Some(record)) => return record.to_object(),
                Ok(None) => continue,
            }
        }
        Err(Error::Storage(format!(
            "transfer_object: {} {id} changed concurrently {ATTEMPTS} times; giving up",
            T::TYPE
        )))
    }

    async fn transfer_object_sequential<T: Object>(
        &self,
        id: Uuid,
        from_owner: Uuid,
        to_owner: Uuid,
    ) -> Result<T, Error> {
        // Fast path: nothing to migrate in the side-table.
        if !T::HAS_UNIQUE_FIELDS {
            let record = self
                .inner
                .adapter
                .transfer_object(T::TYPE, id, from_owner, to_owner)
                .await?;
            return record.to_object();
        }

        // Re-deriving hashes requires the current object state. We fetch by id
        // and validate ownership ourselves so a wrong-owner caller behaves the
        // same as the adapter SQL (returns NotFound, no side-effects).
        let current = self
            .inner
            .adapter
            .fetch_object(T::TYPE, id)
            .await?
            .ok_or(Error::NotFound)?;
        if current.owner != from_owner {
            return Err(Error::NotFound);
        }

        let from_obj: T = current.to_object()?;
        let from_hashes: Vec<String> = from_obj
            .derive_unique_hashes()
            .into_iter()
            .map(|(h, _)| h)
            .collect();

        let mut to_obj = from_obj;
        to_obj.meta_mut().owner = to_owner;
        let to_hashes = to_obj.derive_unique_hashes();

        // Diff: only hashes that involve `owner` will differ; the rest are
        // unchanged and skip the side-table entirely.
        let hashes_to_add: Vec<(String, &'static str)> = to_hashes
            .iter()
            .filter(|(h, _)| !from_hashes.contains(h))
            .cloned()
            .collect();
        let hashes_to_remove: Vec<String> = from_hashes
            .into_iter()
            .filter(|h| !to_hashes.iter().any(|(nh, _)| nh == h))
            .collect();

        // Insert new hashes first — if the destination owner already has a
        // conflicting object, this aborts before any owner mutation lands.
        if !hashes_to_add.is_empty() {
            self.inner
                .adapter
                .insert_unique_hashes(T::TYPE, id, hashes_to_add.clone())
                .await?;
        }

        match self
            .inner
            .adapter
            .transfer_object(T::TYPE, id, from_owner, to_owner)
            .await
        {
            Ok(record) => {
                if !hashes_to_remove.is_empty() {
                    self.inner
                        .adapter
                        .delete_unique_hashes(hashes_to_remove)
                        .await?;
                }
                record.to_object()
            }
            Err(err) => {
                // Rollback the optimistic hash insert so we don't leave an
                // orphan claim for the destination owner.
                if !hashes_to_add.is_empty() {
                    let rollback: Vec<String> =
                        hashes_to_add.into_iter().map(|(h, _)| h).collect();
                    let _ = self.inner.adapter.delete_unique_hashes(rollback).await;
                }
                Err(err)
            }
        }
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

    /// Query objects and return each result paired with its distance (meters)
    /// from the point set by `query.order_by_distance(...)`. Requires
    /// `geo_order` to be set on the query — otherwise returns
    /// `Error::InvalidQuery`. Postgres-only (other adapters return
    /// `Error::Unsupported`).
    pub async fn query_objects_with_distance<T: Object>(
        &self,
        query: Query,
    ) -> Result<Vec<(T, f64)>, Error> {
        let start = Instant::now();
        let pairs = self
            .inner
            .adapter
            .query_objects_with_distance(T::TYPE, query)
            .await?;
        histogram!("ousia.query.duration_ms",
            "type" => T::TYPE
        )
        .record(start.elapsed().as_millis() as f64);
        pairs
            .into_iter()
            .map(|(r, d)| r.to_object::<T>().map(|t| (t, d)))
            .collect()
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

    /// Fetch a known edge
    pub async fn fetch_edge<E: Edge>(&self, from: Uuid, to: Uuid) -> Result<Option<E>, Error> {
        let edge_record = self.inner.adapter.fetch_edge(E::TYPE, from, to).await?;
        let Some(edge_record) = edge_record else {
            return Ok(None);
        };
        edge_record.to_edge().map(|edge| Some(edge))
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
    pub fn preload_objects<'a, P: Object>(&'a self, query: Query) -> MultiPreloadContext<'a, P> {
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
