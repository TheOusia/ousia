pub mod error;

#[cfg(feature = "postgres")]
mod postgres;

use async_trait::async_trait;
use ulid::Ulid;

use crate::{
    Object,
    engine::adapters::error::AdapterError,
    object::{
        SYSTEM_OWNER,
        query::{IndexValue, ToIndexValue},
    },
};

/// -----------------------------
/// Adapter contract
/// -----------------------------

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn fetch_by_id<T: Object>(&self, id: Ulid) -> Option<T>;
    async fn insert<T: Object>(&self, obj: &T) -> Result<(), AdapterError>;
    async fn update<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError>;
    async fn query<T: Object>(&self, plan: QueryPlan) -> Vec<T>;
}

/// -----------------------------
/// Query Plan (storage contract)
/// -----------------------------

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub owner: Ulid, // enforced, never optional
    pub filters: Vec<QueryFilter>,
    pub sort: Vec<QuerySort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

impl Default for QueryPlan {
    fn default() -> Self {
        Self {
            owner: *SYSTEM_OWNER,
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}

impl QueryPlan {
    pub fn new(owner: Ulid) -> Self {
        Self {
            owner,
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    pub fn with_filter(self, field: Field, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field: field.name,
            value: value.to_index_value(),
        });
        consumed_self
    }

    pub fn with_sort(self, sort: QuerySort) -> Self {
        let mut consumed_self = self;
        consumed_self.sort.push(sort);
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

#[derive(Debug, Clone)]
pub struct QueryFilter {
    pub field: &'static str,
    pub value: IndexValue,
}

#[derive(Debug, Clone)]
pub struct QuerySort {
    pub field: &'static str,
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: &'static str,
}

impl Field {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }
}
