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
use crate::query::{IndexKind, QueryFilter};
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

    /// Insert a new object into storage.
    ///
    /// If `T` declares `unique = "..."` fields in its `OusiaObject` derive, the
    /// unique hashes are written atomically before the object row is inserted.
    /// Returns `Error::UniqueConstraintViolation` when a unique field value is
    /// already taken by another object of the same type. The caller can either
    /// surface that error to the user or fetch the existing object with
    /// `find_object` / `find_object_with_owner`.
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

    /// Fetch a single object by its UUID. Returns `None` if not found.
    pub async fn fetch_object<T: Object>(&self, id: Uuid) -> Result<Option<T>, Error> {
        let val = self.inner.adapter.fetch_object(T::TYPE, id).await?;
        match val {
            Some(record) => record.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Fetch multiple objects of type `T` by their UUIDs in a single query.
    ///
    /// On Postgres/CockroachDB this issues one `WHERE id = ANY($1)` query,
    /// binding all IDs as a typed array. Order of results is not guaranteed.
    pub async fn fetch_objects<T: Object>(&self, ids: Vec<Uuid>) -> Result<Vec<T>, Error> {
        let records = self.inner.adapter.fetch_bulk_objects(T::TYPE, ids).await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Fetch objects of multiple types in a single UNION ALL query.
    ///
    /// Each entry in `pairs` is `(type_name, ids)`. Returns raw [`ObjectRecord`]s so
    /// the caller can group by `record.type_name` and deserialize each group with
    /// `record.to_object::<T>()`.
    pub async fn fetch_objects_batch(
        &self,
        pairs: Vec<(&'static str, Vec<Uuid>)>,
    ) -> Result<Vec<ObjectRecord>, Error> {
        self.inner.adapter.fetch_objects_batch(pairs).await
    }

    /// Persist changes to an existing object.
    ///
    /// Sets `updated_at` to now before writing. If `T` has unique fields and
    /// any of them changed, the new hashes are inserted first (failing with
    /// `UniqueConstraintViolation` if taken), and the old hashes are removed
    /// only after the object row is successfully updated. If the object update
    /// fails the new hashes are rolled back automatically.
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

    /// Delete an object by ID and owner.
    ///
    /// Removes any associated unique hashes first, then deletes the object
    /// row. Returns the deleted object, or `None` if no object with that
    /// ID and owner was found.
    pub async fn delete_object<T: Object>(
        &self,
        id: Uuid,
        owner: Uuid,
    ) -> Result<Option<T>, Error> {
        if T::HAS_UNIQUE_FIELDS {
            let hashes = self.inner.adapter.get_hashes_for_object(id).await?;
            if !hashes.is_empty() {
                self.inner.adapter.delete_unique_hashes(hashes).await?;
            }
        }

        let record = self.inner.adapter.delete_object(T::TYPE, id, owner).await?;

        match record {
            Some(r) => r.to_object().map(Some),
            None => Ok(None),
        }
    }

    /// Delete multiple objects of type `T` in bulk.
    ///
    /// Cleans up unique hashes for the given IDs before deleting the rows.
    /// Returns the number of rows actually deleted.
    pub async fn delete_objects<T: Object>(
        &self,
        ids: Vec<Uuid>,
        owner: Uuid,
    ) -> Result<u64, Error> {
        if T::HAS_UNIQUE_FIELDS && !ids.is_empty() {
            let hashes = self
                .inner
                .adapter
                .get_hashes_for_objects(ids.clone())
                .await?;
            if !hashes.is_empty() {
                self.inner.adapter.delete_unique_hashes(hashes).await?;
            }
        }

        self.inner
            .adapter
            .delete_bulk_objects(T::TYPE, ids, owner)
            .await
    }

    /// Delete all objects of type `T` owned by `owner`.
    ///
    /// Useful for cascade-style cleanup when an owner entity is removed.
    /// Returns the number of rows deleted.
    pub async fn delete_owned_objects<T: Object>(&self, owner: Uuid) -> Result<u64, Error> {
        let record = self
            .inner
            .adapter
            .delete_owned_objects(T::TYPE, owner)
            .await?;

        Ok(record)
    }

    /// Transfer ownership of an object from one owner to another.
    ///
    /// Returns the updated object. Errors if `from_owner` does not currently
    /// own the object.
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

    /// Find the first system-owned object of type `T` matching `filters`.
    ///
    /// Equivalent to `find_object_with_owner` with `system_owner()` as the
    /// owner. Returns `None` when no match is found.
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

    /// Find the first object of type `T` owned by `owner` matching `filters`.
    ///
    /// Returns `None` when no matching object is found.
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

    /// Execute a paginated query for objects of type `T`.
    ///
    /// Build the query with [`Query::new`] for user-owned objects,
    /// [`Query::default`] for system-owned objects, or [`Query::wide`] for a
    /// cross-owner scan. Chain `where_*`, `sort_*`, `with_limit`, and
    /// `with_cursor` calls to refine the query before passing it here.
    pub async fn query_objects<T: Object>(&self, query: Query) -> Result<Vec<T>, Error> {
        let start = Instant::now();
        let records = self.inner.adapter.query_objects(T::TYPE, query).await?;
        histogram!("ousia.query.duration_ms",
            "type" => T::TYPE
        )
        .record(start.elapsed().as_millis() as f64);
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Count objects of type `T` matching the query without fetching row data.
    ///
    /// Pass `None` to count all objects of the type visible to the query's
    /// owner partition (or across all partitions for `Query::wide`).
    pub async fn count_objects<T: Object>(&self, query: Option<Query>) -> Result<u64, Error> {
        self.inner.adapter.count_objects(T::TYPE, query).await
    }

    /// Fetch all objects of type `T` owned by `owner` without pagination.
    ///
    /// Use this for small, bounded collections (e.g. settings, profile data).
    /// For large collections prefer `query_objects` with a `with_limit` and
    /// `with_cursor` for pagination.
    pub async fn fetch_owned_objects<T: Object>(&self, owner: Uuid) -> Result<Vec<T>, Error> {
        let records = self
            .inner
            .adapter
            .fetch_owned_objects(T::TYPE, owner)
            .await?;
        records.into_iter().map(|r| r.to_object()).collect()
    }

    /// Fetch the single object of type `T` owned by `owner`.
    ///
    /// Designed for one-to-one ownership relationships where each owner holds
    /// exactly one child of type `T`. Returns `None` if none exists.
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

    /// Fetch an object that is either type `A` or type `B` by its UUID.
    ///
    /// Issues a single UNION ALL query that checks both type tables. Returns
    /// `None` if neither type has a row with the given ID.
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

    /// Bulk-fetch objects that are either type `A` or type `B` by their UUIDs.
    ///
    /// Issues a single UNION ALL query across both type tables, binding all
    /// IDs at once.
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

    /// Fetch the single `A`-or-`B` object owned by `owner`.
    ///
    /// Intended for one-to-one ownership where the child can be one of two
    /// types. Returns `None` if neither type has a row owned by `owner`.
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

    /// Fetch all `A`-or-`B` objects owned by `owner`.
    ///
    /// Issues a single UNION ALL query across both type tables filtered by
    /// owner.
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

    /// Insert a new edge of type `E` into storage.
    pub async fn create_edge<E: Edge>(&self, edge: &E) -> Result<(), Error> {
        self.inner
            .adapter
            .insert_edge(EdgeRecord::from_edge(edge))
            .await
    }

    /// Update an edge's data fields and optionally retarget it to a new `to` UUID.
    ///
    /// If `to` is `Some(new_id)`, the edge's target is atomically changed from
    /// the current `to` to `new_id`. Pass `None` to update data fields only.
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

    /// Delete the edge of type `E` between `from` and `to`.
    pub async fn delete_edge<E: Edge>(&self, from: Uuid, to: Uuid) -> Result<(), Error> {
        self.inner.adapter.delete_edge(E::TYPE, from, to).await
    }

    /// Delete all edge of an object
    pub async fn delete_object_edge<E: Edge>(&self, from: Uuid) -> Result<(), Error> {
        self.inner.adapter.delete_object_edge(E::TYPE, from).await
    }

    /// Fetch the edge of type `E` between `from` and `to`.
    ///
    /// Returns `None` if no such edge exists.
    pub async fn fetch_edge<E: Edge>(&self, from: Uuid, to: Uuid) -> Result<Option<E>, Error> {
        let edge_record = self.inner.adapter.fetch_edge(E::TYPE, from, to).await?;
        let Some(edge_record) = edge_record else {
            return Ok(None);
        };
        edge_record.to_edge().map(|edge| Some(edge))
    }

    /// Query forward edges of type `E` originating from `from`.
    ///
    /// Returns edges where `e."from" = from`. Use an [`EdgeQuery`] to filter
    /// and sort by the edge's own indexed fields.
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

    /// Query reverse edges of type `E` pointing into `to`.
    ///
    /// Returns edges where `e."to" = to`. Use an [`EdgeQuery`] to filter and
    /// sort by the edge's own indexed fields.
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

    /// Count forward edges of type `E` originating from `from` without fetching data.
    ///
    /// Pass `None` for `query` to count all edges from `from`.
    pub async fn count_edges<E: Edge>(
        &self,
        from: Uuid,
        query: Option<EdgeQuery>,
    ) -> Result<u64, Error> {
        self.inner.adapter.count_edges(E::TYPE, from, query).await
    }

    /// Count reverse edges of type `E` pointing into `to` without fetching data.
    ///
    /// Pass `None` for `query` to count all reverse edges into `to`.
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

    /// Return the current value of the named counter.
    ///
    /// If the counter does not yet exist it is created and initialised to 1,
    /// and 1 is returned. Does not advance the counter.
    pub async fn counter_value(&self, key: String) -> u64 {
        self.inner.adapter.sequence_value(key).await
    }

    /// Atomically increment the named counter and return its new value.
    ///
    /// If the counter does not yet exist it is created, incremented to 1, and
    /// 1 is returned.
    pub async fn counter_next_value(&self, key: String) -> u64 {
        self.inner.adapter.sequence_next_value(key).await
    }

    // ==================== Advanced Query API ====================

    /// Start a single-pivot preload context for object `id` of type `T`.
    ///
    /// Returns a [`QueryContext`] that lets you fetch `T` and traverse its
    /// edges or owned children in separate chained calls. Each terminal method
    /// (`collect`, `collect_reverse`, `collect_both`, etc.) issues its own
    /// query — use `preload_objects` when you need to avoid N+1 queries across
    /// multiple parents.
    pub fn preload_object<'a, T: Object>(&'a self, id: Uuid) -> QueryContext<'a, T> {
        self.inner.adapter.preload_object(id)
    }

    /// Start a multi-pivot query context. Fetches parents first, then batch-joins edges/children.
    /// All terminal methods execute exactly 2 queries — never N+1.
    pub fn preload_objects<'a, P: Object>(&'a self, query: Query) -> MultiPreloadContext<'a, P> {
        self.inner.adapter.preload_objects(query)
    }

    // ==================== Schema Drift Detection ====================

    /// Compare the current schema hash for `T` against the stored hash in `ousia_meta`.
    ///
    /// Stored format: `"major.minor:hash"` (e.g. `"1.2:abc123..."`).
    ///
    /// - First call: records the current version prefix and hash.
    /// - Major version change (`x` differs): returns [`Error::SchemaMigrationRequired`].
    ///   The stored value is **not** updated — the operator must reconcile manually.
    /// - Minor version change or hash drift (same major, different hash): warns via
    ///   `eprintln!` and updates the stored value.
    /// - Exact match: no-op.
    ///
    /// The hash is derived from `T::TYPE` and the sorted list of `#[ousia(index)]`
    /// declarations. It does **not** change on a patch version bump unless the schema
    /// structure itself changed.
    pub async fn check_schema<T: Object + IndexQuery>(&self) -> Result<(), Error> {
        let current_hash = compute_schema_hash::<T>();
        let current_prefix = schema_version_prefix();
        let current_entry = format!("{}:{}", current_prefix, current_hash);

        let stored = self.inner.adapter.read_schema_hash(T::TYPE).await?;

        match stored {
            None => {
                self.inner
                    .adapter
                    .upsert_schema_hash(T::TYPE, &current_entry)
                    .await?;
            }
            Some(ref s) if s == &current_entry => {}
            Some(stored_entry) => {
                // Parse stored entry — support both old (bare hash) and new (prefix:hash) format.
                let (stored_major, stored_hash) = parse_schema_entry(&stored_entry);
                let current_major = current_prefix
                    .split('.')
                    .next()
                    .unwrap_or("0");

                if stored_major != current_major {
                    return Err(Error::SchemaMigrationRequired(format!(
                        "type '{}': stored schema version '{}' is incompatible with current \
                         version prefix '{}'. Run a manual migration then update ousia_meta.",
                        T::TYPE, stored_major, current_major
                    )));
                }

                // Same major — warn about hash/minor drift and update.
                eprintln!(
                    "[ousia warn] Schema drift detected for '{}': \
                     index structure changed (stored: {}…, current: {}…). \
                     Rows written before this change will not appear in queries on new indexes.",
                    T::TYPE,
                    &stored_hash[..stored_hash.len().min(12)],
                    &current_hash[..current_hash.len().min(12)],
                );
                self.inner
                    .adapter
                    .upsert_schema_hash(T::TYPE, &current_entry)
                    .await?;
            }
        }

        Ok(())
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

/// Compute a stable blake3 hash of a type's index structure.
/// Input: `"TypeName:field1:Kind1+Kind2,field2:Kind1"` (fields and kinds sorted).
fn compute_schema_hash<T: Object + IndexQuery>() -> String {
    let mut fields: Vec<_> = T::indexed_fields()
        .iter()
        .map(|f| {
            let mut kinds: Vec<&str> = f
                .kinds
                .iter()
                .map(|k| match k {
                    IndexKind::Search => "Search",
                    IndexKind::Sort => "Sort",
                })
                .collect();
            kinds.sort_unstable();
            (f.name, kinds.join("+"))
        })
        .collect();
    fields.sort_unstable_by_key(|(name, _)| *name);

    let canonical = fields
        .iter()
        .map(|(name, kinds)| format!("{}:{}", name, kinds))
        .collect::<Vec<_>>()
        .join(",");

    let input = format!("{}:{}", T::TYPE, canonical);
    blake3::hash(input.as_bytes()).to_hex().to_string()
}

/// Returns the `"major.minor"` prefix derived from `CARGO_PKG_VERSION` at compile time.
/// For version `"1.2.4"` this returns `"1.2"`.
fn schema_version_prefix() -> String {
    let ver = env!("CARGO_PKG_VERSION");
    let mut parts = ver.splitn(3, '.');
    let major = parts.next().unwrap_or("0");
    let minor = parts.next().unwrap_or("0");
    format!("{}.{}", major, minor)
}

/// Parse a stored schema entry into `(major, hash)`.
/// Handles both old bare-hash format and new `"major.minor:hash"` format.
fn parse_schema_entry(entry: &str) -> (&str, &str) {
    match entry.find(':') {
        Some(pos) => {
            // New format: "major.minor:hash" — extract just the major component.
            let prefix = &entry[..pos];
            let hash = &entry[pos + 1..];
            let major = prefix.split('.').next().unwrap_or("0");
            (major, hash)
        }
        // Old format: bare hash — treat as version "0" so major change is detected.
        None => ("0", entry),
    }
}
