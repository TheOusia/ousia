use std::marker::PhantomData;

use async_trait::async_trait;
use ulid::Ulid;

use crate::{
    engine::adapters::{Adapter, QueryFilter, QueryPlan, QuerySort, error::AdapterError},
    object::{
        query::{IndexKind, IndexValue, ObjectQuery, ToIndexValue},
        traits::Object,
    },
};

/// -----------------------------
/// Engine
/// -----------------------------
pub struct Engine<A: Adapter> {
    adapter: A,
}

impl<A: Adapter> Engine<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    pub fn query<T>(&self, owner: Ulid) -> ObjectQueryBuilder<'_, A, T>
    where
        T: Object + ObjectQuery,
    {
        ObjectQueryBuilder::new(&self.adapter, owner)
    }

    pub async fn fetch_by_id<T: Object>(&self, id: Ulid) -> Option<T> {
        self.adapter.fetch_by_id(id).await
    }

    pub async fn insert<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError> {
        self.adapter.insert(obj).await
    }

    pub async fn update<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError> {
        self.adapter.update(obj).await
    }
}

/// -----------------------------
/// Query Builder (engine-level)
/// -----------------------------

pub struct ObjectQueryBuilder<'a, A, T>
where
    A: Adapter,
    T: Object + ObjectQuery,
{
    adapter: &'a A,
    owner: Ulid,
    filters: Vec<QueryFilter>,
    sort: Vec<QuerySort>,
    limit: Option<u32>,
    offset: Option<u32>,
    _marker: PhantomData<T>,
}

impl<'a, A, T> ObjectQueryBuilder<'a, A, T>
where
    A: Adapter,
    T: Object + ObjectQuery,
{
    fn new(adapter: &'a A, owner: Ulid) -> Self {
        Self {
            adapter,
            owner,
            filters: Vec::new(),
            sort: Vec::new(),
            limit: None,
            offset: None,
            _marker: PhantomData,
        }
    }

    pub fn filter<V: ToIndexValue>(mut self, field: &'static str, value: V) -> Self {
        let allowed = T::indexed_fields()
            .iter()
            .any(|f| f.name == field && f.kinds.contains(&IndexKind::Search));

        assert!(
            allowed,
            "Field `{}` is not searchable for `{}`",
            field,
            T::TYPE
        );

        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
        });

        self
    }

    pub fn sort(mut self, field: &'static str, ascending: bool) -> Self {
        let allowed = T::indexed_fields()
            .iter()
            .any(|f| f.name == field && f.kinds.contains(&IndexKind::Sort));

        assert!(
            allowed,
            "Field `{}` is not sortable for `{}`",
            field,
            T::TYPE
        );

        self.sort.push(QuerySort { field, ascending });
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    pub async fn fetch(self) -> Vec<T> {
        let plan = QueryPlan {
            object_type: T::TYPE,
            owner: self.owner,
            filters: self.filters,
            sort: self.sort,
            limit: self.limit,
            offset: self.offset,
        };

        self.adapter.query::<T>(plan).await
    }
}
