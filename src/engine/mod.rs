pub mod adapters;

use async_trait::async_trait;
use std::marker::PhantomData;
use ulid::Ulid;

use crate::{
    engine::adapters::{Adapter, QueryFilter, QueryPlan, QuerySort, error::AdapterError},
    object::{
        query::{IndexKind, IndexValue, ObjectQuery, ToIndexValue},
        traits::Object,
    },
};

/// Main engine coordinating storage operations
pub struct Engine<A: Adapter> {
    adapter: A,
}

impl<A: Adapter> Engine<A> {
    pub fn new(adapter: A) -> Self {
        Self { adapter }
    }

    /// Start a query for objects of type T owned by the given owner
    pub fn query<T>(&self, owner: Ulid) -> ObjectQueryBuilder<'_, A, T>
    where
        T: Object + ObjectQuery,
    {
        ObjectQueryBuilder::new(&self.adapter, owner)
    }

    /// Fetch a single object by ID
    pub async fn fetch_by_id<T: Object>(&self, id: Ulid) -> Option<T> {
        self.adapter.fetch_by_id(id).await
    }

    /// Fetch multiple objects by their IDs
    pub async fn fetch_by_ids<T: Object>(&self, ids: &[Ulid]) -> Vec<T> {
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(obj) = self.adapter.fetch_by_id(*id).await {
                results.push(obj);
            }
        }
        results
    }

    /// Insert a new object
    pub async fn insert<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError> {
        self.adapter.insert(obj).await
    }

    /// Insert multiple objects in a batch
    pub async fn insert_batch<T: Object>(&self, objects: &mut [T]) -> Result<(), AdapterError> {
        for obj in objects.iter_mut() {
            self.adapter.insert(obj).await?;
        }
        Ok(())
    }

    /// Update an existing object
    pub async fn update<T: Object>(&self, obj: &mut T) -> Result<(), AdapterError> {
        self.adapter.update(obj).await
    }

    /// Update multiple objects in a batch
    pub async fn update_batch<T: Object>(&self, objects: &mut [T]) -> Result<(), AdapterError> {
        for obj in objects.iter_mut() {
            self.adapter.update(obj).await?;
        }
        Ok(())
    }

    /// Delete an object (soft delete by updating a status field could be implemented)
    pub async fn delete<T: Object>(&self, id: Ulid) -> Result<(), AdapterError> {
        // Note: This would require adding a delete method to the Adapter trait
        // For now, this is a placeholder
        todo!("Implement delete in Adapter trait")
    }

    /// Check if an object exists
    pub async fn exists<T: Object>(&self, id: Ulid) -> bool {
        self.fetch_by_id::<T>(id).await.is_some()
    }

    /// Count objects matching a query
    pub async fn count<T>(&self, owner: Ulid) -> ObjectCountBuilder<'_, A, T>
    where
        T: Object + ObjectQuery,
    {
        ObjectCountBuilder::new(&self.adapter, owner)
    }
}

/// Fluent query builder for fetching objects
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

    /// Add a filter condition (AND semantics)
    pub fn filter<V: ToIndexValue>(mut self, field: &'static str, value: V) -> Self {
        let allowed = T::indexed_fields()
            .iter()
            .any(|f| f.name == field && f.kinds.contains(&IndexKind::Search));

        assert!(
            allowed,
            "Field `{}` is not searchable for type `{}`",
            field,
            T::TYPE
        );

        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
        });

        self
    }

    /// Add multiple filters at once
    pub fn filters(mut self, filters: Vec<(&'static str, IndexValue)>) -> Self {
        for (field, value) in filters {
            let allowed = T::indexed_fields()
                .iter()
                .any(|f| f.name == field && f.kinds.contains(&IndexKind::Search));

            assert!(
                allowed,
                "Field `{}` is not searchable for type `{}`",
                field,
                T::TYPE
            );

            self.filters.push(QueryFilter { field, value });
        }
        self
    }

    /// Add a sort clause
    pub fn sort(mut self, field: &'static str, ascending: bool) -> Self {
        let allowed = T::indexed_fields()
            .iter()
            .any(|f| f.name == field && f.kinds.contains(&IndexKind::Sort));

        assert!(
            allowed,
            "Field `{}` is not sortable for type `{}`",
            field,
            T::TYPE
        );

        self.sort.push(QuerySort { field, ascending });
        self
    }

    /// Sort ascending
    pub fn sort_asc(self, field: &'static str) -> Self {
        self.sort(field, true)
    }

    /// Sort descending
    pub fn sort_desc(self, field: &'static str) -> Self {
        self.sort(field, false)
    }

    /// Limit the number of results
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset for pagination
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Paginate results (convenience method)
    pub fn page(self, page: u32, page_size: u32) -> Self {
        let offset = page * page_size;
        self.limit(page_size).offset(offset)
    }

    /// Execute the query and fetch results
    pub async fn fetch(self) -> Vec<T> {
        let plan = QueryPlan {
            owner: self.owner,
            filters: self.filters,
            sort: self.sort,
            limit: self.limit,
            offset: self.offset,
        };

        self.adapter.query::<T>(plan).await
    }

    /// Fetch the first result, if any
    pub async fn fetch_one(self) -> Option<T> {
        let plan = QueryPlan {
            owner: self.owner,
            filters: self.filters,
            sort: self.sort,
            limit: Some(1),
            offset: self.offset,
        };

        self.adapter.query::<T>(plan).await.into_iter().next()
    }

    /// Check if any results exist
    pub async fn exists(self) -> bool {
        self.limit(1).fetch_one().await.is_some()
    }
}

/// Query builder for counting objects
pub struct ObjectCountBuilder<'a, A, T>
where
    A: Adapter,
    T: Object + ObjectQuery,
{
    adapter: &'a A,
    owner: Ulid,
    filters: Vec<QueryFilter>,
    _marker: PhantomData<T>,
}

impl<'a, A, T> ObjectCountBuilder<'a, A, T>
where
    A: Adapter,
    T: Object + ObjectQuery,
{
    fn new(adapter: &'a A, owner: Ulid) -> Self {
        Self {
            adapter,
            owner,
            filters: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Add a filter condition
    pub fn filter<V: ToIndexValue>(mut self, field: &'static str, value: V) -> Self {
        let allowed = T::indexed_fields()
            .iter()
            .any(|f| f.name == field && f.kinds.contains(&IndexKind::Search));

        assert!(
            allowed,
            "Field `{}` is not searchable for type `{}`",
            field,
            T::TYPE
        );

        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
        });

        self
    }

    /// Execute the count query
    pub async fn execute(self) -> usize {
        let plan = QueryPlan {
            owner: self.owner,
            filters: self.filters,
            sort: Vec::new(),
            limit: None,
            offset: None,
        };

        // Note: This is inefficient - a proper implementation would add
        // a count method to the Adapter trait
        self.adapter.query::<T>(plan).await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Example usage patterns
    #[test]
    fn test_query_builder_api() {
        // This demonstrates the fluent API
        // Actual execution would require a real adapter
    }
}
