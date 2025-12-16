pub mod error;
mod postgres;

use async_trait::async_trait;
use ulid::Ulid;

use crate::{Object, engine::adapters::error::AdapterError, object::query::IndexValue};

/// -----------------------------
/// Adapter contract
/// -----------------------------

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn fetch_by_id<T: Object>(&self, id: Ulid) -> Option<T>;

    async fn insert<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError>;

    async fn update<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError>;

    async fn query<T: Object>(&self, plan: QueryPlan) -> Vec<T>;
}

/// -----------------------------
/// Query Plan (storage contract)
/// -----------------------------

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub object_type: &'static str,
    pub owner: Ulid, // enforced, never optional
    pub filters: Vec<QueryFilter>,
    pub sort: Vec<QuerySort>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
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
