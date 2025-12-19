#[cfg(feature = "postgres")]
pub mod postgres;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    Object,
    edge::{Edge, query::EdgeQuery},
    error::Error,
    object::SYSTEM_OWNER,
    query::{
        Comparison, Cursor, IndexField, Operator, QueryFilter, QueryMode, QuerySearch, QuerySort,
        ToIndexValue,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ObjectRecord {
    pub id: Ulid,
    pub type_name: String,
    pub owner: Ulid,
    pub data: serde_json::Value,
    pub index_meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ObjectRecord {
    pub(crate) fn to_object<T: Object>(self) -> Result<T, Error> {
        if self.type_name != T::TYPE {
            return Err(Error::TypeMismatch);
        }

        let mut val = serde_json::from_value::<T>(self.data)
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let meta = val.meta_mut();
        meta.id = self.id;
        meta.owner = self.owner;
        meta.created_at = self.created_at;
        meta.updated_at = self.updated_at;
        Ok(val)
    }

    pub(crate) fn from_object<'a, T: Object>(obj: &'a T) -> Self {
        let meta = obj.meta();
        Self {
            id: meta.id,
            type_name: obj.type_name().to_string(),
            owner: meta.owner,
            index_meta: serde_json::to_value(obj.index_meta())
                .expect("Failed to serialize index_meta"),
            data: serde_json::to_value(obj).expect("Failed to serialize object"),
            created_at: meta.created_at,
            updated_at: meta.updated_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EdgeRecord {
    pub type_name: String,
    pub from: Ulid,
    pub to: Ulid,
    pub data: serde_json::Value,
    pub index_meta: serde_json::Value,
}

impl EdgeRecord {
    pub fn to_edge<E: Edge>(self) -> Result<E, Error> {
        if self.type_name != E::TYPE {
            return Err(Error::TypeMismatch);
        }

        let mut val = serde_json::from_value::<E>(self.data)
            .map_err(|e| Error::Deserialize(e.to_string()))?;
        let meta = val.meta_mut();
        meta.to = self.to;
        meta.from = self.from;
        Ok(val)
    }

    pub fn from_edge<'a, E: Edge>(edge: &'a E) -> Self {
        let meta = edge.meta();
        Self {
            to: meta.to,
            from: meta.from,
            type_name: edge.type_name().to_string(),
            data: serde_json::to_value(edge).expect("Failed to serialize edge"),
            index_meta: serde_json::to_value(edge.index_meta())
                .expect("Failed to serialize index meta"),
        }
    }
}

/// -----------------------------
/// Object Query Plan (storage contract)
/// -----------------------------

#[derive(Debug)]
pub struct Query {
    pub owner: Ulid, // enforced, never optional
    pub filters: Vec<QueryFilter>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            owner: *SYSTEM_OWNER,
            filters: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}

impl Query {
    pub fn new(owner: Ulid) -> Self {
        Self {
            owner,
            filters: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    pub fn filter(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
        mode: QueryMode,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode,
        });
        consumed_self
    }

    pub fn where_eq(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }
}

pub struct QueryContext<'a, T> {
    root: Ulid,
    adapter: &'a dyn Adapter,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T: Object> QueryContext<'a, T> {
    pub(crate) fn new(adapter: &'a dyn Adapter, root: Ulid) -> Self {
        Self {
            root,
            adapter,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn get(&self) -> Result<Option<T>, Error> {
        let val = self.adapter.fetch_object(self.root).await?;
        match val {
            Some(o) => o.to_object().map(|o| Some(o)),
            None => Ok(None),
        }
    }

    pub fn edge<E: Edge, O: Object>(self) -> EdgeQueryContext<'a, E, O> {
        EdgeQueryContext::new(self.adapter, self.root)
    }
}
/// ==========================
/// Edge Query Context
/// ==========================

pub struct EdgeQueryContext<'a, E: Edge, O: crate::Object> {
    owner: Ulid,
    filters: Vec<QueryFilter>,
    edge_filters: Vec<QueryFilter>,
    limit: Option<u32>,
    offset: Option<u32>,
    adapter: &'a dyn Adapter,
    _marker: std::marker::PhantomData<(E, O)>,
}

impl<'a, E: Edge, O: Object> EdgeQueryContext<'a, E, O> {
    pub(crate) fn new(adapter: &'a dyn Adapter, owner: Ulid) -> Self {
        Self {
            adapter,
            owner,
            filters: vec![],
            edge_filters: vec![],
            limit: None,
            offset: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Filter on the target objects (not the edges themselves)
    pub fn filter(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
        mode: QueryMode,
    ) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode,
        });
        self
    }

    /// Filter on the target objects with equality comparison
    pub fn where_eq(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter on the edge properties
    pub fn edge_filter(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
        mode: QueryMode,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode,
        });
        self
    }

    /// Set limit for pagination
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset for pagination
    pub fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Collect the target objects (traverse the edges and return the destinations)
    pub async fn collect(self) -> Result<Vec<O>, Error> {
        // First, query the edges
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.offset {
            edge_query.offset = Some(offset);
        }

        let edge_records = self
            .adapter
            .query_edges(E::TYPE, self.owner, edge_query)
            .await?;

        // Extract the 'to' IDs from edges
        let target_ids: Vec<Ulid> = edge_records.iter().map(|e| e.to).collect();

        if target_ids.is_empty() {
            return Ok(vec![]);
        }

        // Fetch the target objects
        let object_records = self.adapter.fetch_bulk_objects(target_ids).await?;

        // Convert records to domain objects and apply object filters
        let mut objects: Vec<O> = object_records
            .into_iter()
            .filter_map(|r| r.to_object().ok())
            .collect();

        // Apply in-memory filtering if needed (for object filters)
        // This is a simple implementation - a more efficient approach would push filters to SQL
        if !self.filters.is_empty() {
            objects.retain(|obj| self.matches_filters(obj));
        }

        Ok(objects)
    }

    /// Collect the edges themselves (not the target objects)
    pub async fn collect_edges(self) -> Result<Vec<E>, Error> {
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters;
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.offset {
            edge_query.offset = Some(offset);
        }

        let edge_records = self
            .adapter
            .query_edges(E::TYPE, self.owner, edge_query)
            .await?;

        edge_records
            .into_iter()
            .map(|r| r.to_edge())
            .collect::<Result<Vec<E>, Error>>()
    }

    /// Paginate using a cursor
    pub fn paginate(mut self, cursor: Option<impl Into<Cursor>>) -> Self {
        if let Some(cursor) = cursor {
            let _cursor: Cursor = cursor.into();
            // TODO: Implement cursor-based pagination
            // This would typically involve adding a filter like "id > cursor.last_id"
        }
        self
    }

    /// Helper to check if an object matches the filters
    fn matches_filters(&self, obj: &O) -> bool {
        let index_meta = obj.index_meta();
        let meta = index_meta.meta();

        for filter in &self.filters {
            if let Some(value) = meta.get(filter.field.name) {
                if !self.value_matches(value, &filter.value, &filter.mode) {
                    return false;
                }
            } else {
                return false; // Field not found means no match
            }
        }

        true
    }

    /// Helper to compare index values based on query mode
    fn value_matches(
        &self,
        actual: &crate::query::IndexValue,
        expected: &crate::query::IndexValue,
        mode: &QueryMode,
    ) -> bool {
        if let QueryMode::Search(search) = mode {
            match search.comparison {
                Comparison::Equal => actual == expected,
                Comparison::NotEqual => actual != expected,
                Comparison::Contains => {
                    if let (
                        crate::query::IndexValue::String(a),
                        crate::query::IndexValue::String(e),
                    ) = (actual, expected)
                    {
                        a.contains(e)
                    } else {
                        false
                    }
                }
                Comparison::BeginsWith => {
                    if let (
                        crate::query::IndexValue::String(a),
                        crate::query::IndexValue::String(e),
                    ) = (actual, expected)
                    {
                        a.starts_with(e)
                    } else {
                        false
                    }
                }
                Comparison::GreaterThan => match (actual, expected) {
                    (crate::query::IndexValue::Int(a), crate::query::IndexValue::Int(e)) => a > e,
                    (crate::query::IndexValue::Float(a), crate::query::IndexValue::Float(e)) => {
                        a > e
                    }
                    (
                        crate::query::IndexValue::Timestamp(a),
                        crate::query::IndexValue::Timestamp(e),
                    ) => a > e,
                    _ => false,
                },
                Comparison::LessThan => match (actual, expected) {
                    (crate::query::IndexValue::Int(a), crate::query::IndexValue::Int(e)) => a < e,
                    (crate::query::IndexValue::Float(a), crate::query::IndexValue::Float(e)) => {
                        a < e
                    }
                    (
                        crate::query::IndexValue::Timestamp(a),
                        crate::query::IndexValue::Timestamp(e),
                    ) => a < e,
                    _ => false,
                },
                Comparison::GreaterThanOrEqual => match (actual, expected) {
                    (crate::query::IndexValue::Int(a), crate::query::IndexValue::Int(e)) => a >= e,
                    (crate::query::IndexValue::Float(a), crate::query::IndexValue::Float(e)) => {
                        a >= e
                    }
                    (
                        crate::query::IndexValue::Timestamp(a),
                        crate::query::IndexValue::Timestamp(e),
                    ) => a >= e,
                    _ => false,
                },
                Comparison::LessThanOrEqual => match (actual, expected) {
                    (crate::query::IndexValue::Int(a), crate::query::IndexValue::Int(e)) => a <= e,
                    (crate::query::IndexValue::Float(a), crate::query::IndexValue::Float(e)) => {
                        a <= e
                    }
                    (
                        crate::query::IndexValue::Timestamp(a),
                        crate::query::IndexValue::Timestamp(e),
                    ) => a <= e,
                    _ => false,
                },
            }
        } else {
            true // Sort mode doesn't affect filtering
        }
    }
}
/// -----------------------------
/// Adapter contract
/// -----------------------------

#[async_trait]
pub(crate) trait Adapter: Send + Sync + 'static {
    /* ---------------- OBJECTS ---------------- */
    async fn insert_object(&self, record: ObjectRecord) -> Result<(), Error>;
    async fn fetch_object(&self, id: Ulid) -> Result<Option<ObjectRecord>, Error>;
    async fn fetch_bulk_objects(&self, ids: Vec<Ulid>) -> Result<Vec<ObjectRecord>, Error>;
    async fn update_object(&self, record: ObjectRecord) -> Result<(), Error>;

    /// Explicit ownership transfer
    async fn transfer_object(
        &self,
        id: Ulid,
        from_owner: Ulid,
        to_owner: Ulid,
    ) -> Result<ObjectRecord, Error>;

    async fn delete_object(&self, id: Ulid, owner: Ulid) -> Result<Option<ObjectRecord>, Error>;

    /* ---------------- QUERIES ---------------- */
    /// Fetch ALL objects matching `plan`. Filters by owner.
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
        owner: Ulid,
    ) -> Result<Vec<ObjectRecord>, Error>;

    /// Fetch a SINGLE owned object (O2O)
    async fn fetch_owned_object(
        &self,
        type_name: &'static str,
        owner: Ulid,
    ) -> Result<Option<ObjectRecord>, Error>;

    /* ---------------- EDGES ---------------- */
    async fn insert_edge(&self, record: EdgeRecord) -> Result<(), Error>;
    async fn delete_edge(&self, type_name: &'static str, from: Ulid, to: Ulid)
    -> Result<(), Error>;

    async fn query_edges(
        &self,
        type_name: &'static str,
        owner: Ulid,
        plan: EdgeQuery,
    ) -> Result<Vec<EdgeRecord>, Error>;

    async fn count_edges(
        &self,
        type_name: &'static str,
        owner: Ulid,
        plan: Option<EdgeQuery>,
    ) -> Result<u64, Error>;
}

impl dyn Adapter {
    pub async fn preload_object<'a, T: Object>(&'a self, id: Ulid) -> QueryContext<'a, T> {
        QueryContext::new(self, id)
    }
}
