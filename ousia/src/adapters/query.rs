use super::Adapter;
use uuid::Uuid;

use crate::{
    Object,
    edge::{
        Edge,
        query::{EdgeQuery, ObjectEdge},
    },
    error::Error,
    query::{
        Comparison, Cursor, IndexField, Operator, QueryFilter, QueryMode, QuerySearch, QuerySort,
        ToIndexValue,
    },
    system_owner,
};

#[derive(Debug, Clone)]
pub(crate) enum TraversalDirection {
    /// Forward: edges where e."from" = owner  →  fetch e."to" objects
    Forward,
    /// Reverse: edges where e."to" = owner  →  fetch e."from" objects
    Reverse,
}

/// -----------------------------
/// Object Query Plan (storage contract)
/// -----------------------------

#[derive(Debug, Clone)]
pub struct Query {
    pub owner: Uuid, // enforced, never optional
    pub filters: Vec<QueryFilter>,
    pub limit: Option<u32>,
    pub cursor: Option<Cursor>,
}

impl Default for Query {
    /// Use this for objects owned by system
    /// For Global search see `Query::wide`
    fn default() -> Self {
        Self {
            owner: system_owner(),
            filters: Vec::new(),
            limit: None,
            cursor: None,
        }
    }
}

impl Query {
    pub fn new(owner: Uuid) -> Self {
        Self {
            owner,
            filters: Vec::new(),
            limit: None,
            cursor: None,
        }
    }

    /// Global search.
    /// For optimized search use `Query::default` or `Query::new(owner)` if owner is known
    pub fn wide() -> Self {
        Self {
            owner: Uuid::nil(),
            filters: Vec::new(),
            limit: None,
            cursor: None,
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

    // Equality
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

    // Not Equal
    pub fn where_ne(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Greater Than
    pub fn where_gt(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Greater Than or Equal
    pub fn where_gte(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Less Than
    pub fn where_lt(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Less Than or Equal
    pub fn where_lte(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Contains
    pub fn where_contains(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Contains All
    pub fn where_contains_all(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Begins With (for strings)
    pub fn where_begins_with(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    // Sorting
    pub fn sort_asc(self, field: &'static IndexField) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(), // Dummy value for sort
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        consumed_self
    }

    pub fn sort_desc(self, field: &'static IndexField) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(), // Dummy value for sort
            mode: QueryMode::Sort(QuerySort { ascending: false }),
        });
        consumed_self
    }

    // OR operator variants
    pub fn or_eq(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_ne(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_gt(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_gte(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_lt(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_lte(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_contains(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_contains_all(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn or_begins_with(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_cursor(mut self, cursor: Uuid) -> Self {
        self.cursor = Some(Cursor { last_id: cursor });
        self
    }
}

#[macro_export]
macro_rules! filter {
    ($field:expr, $value:expr) => {{
        use $crate::query::ToIndexValue;
        $crate::query::QueryFilter {
            field: $field,
            value: $value.to_index_value(),
            mode: $crate::query::QueryMode::Search($crate::query::QuerySearch {
                comparison: $crate::query::Comparison::Equal,
                operator: $crate::query::Operator::default(),
            }),
        }
    }};
}

pub struct QueryContext<'a, T> {
    root: Uuid,
    adapter: &'a dyn Adapter,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T: Object> QueryContext<'a, T> {
    pub(crate) fn new(adapter: &'a dyn Adapter, root: Uuid) -> Self {
        Self {
            root,
            adapter,
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn get(&self) -> Result<Option<T>, Error> {
        let val = self.adapter.fetch_object(T::TYPE, self.root).await?;
        match val {
            Some(o) => o.to_object().map(|o| Some(o)),
            None => Ok(None),
        }
    }

    pub fn edge<E: Edge, O: Object>(self) -> EdgeQueryContext<'a, E, O> {
        EdgeQueryContext::new(self.adapter, self.root)
    }

    pub fn preload<C: Object>(self) -> OwnedContext<'a, C> {
        OwnedContext::new(self.adapter, self.root)
    }
}

/// ==========================
/// Edge Query Context
/// ==========================
pub struct EdgeQueryContext<'a, E: Edge, O: crate::Object> {
    owner: Uuid,
    filters: Vec<QueryFilter>,
    edge_filters: Vec<QueryFilter>,
    limit: Option<u32>,
    cursor: Option<Cursor>,
    adapter: &'a dyn Adapter,
    _marker: std::marker::PhantomData<(E, O)>,
}

impl<'a, E: Edge, O: Object> EdgeQueryContext<'a, E, O> {
    pub(crate) fn new(adapter: &'a dyn Adapter, owner: Uuid) -> Self {
        Self {
            adapter,
            owner,
            filters: vec![],
            edge_filters: vec![],
            limit: None,
            cursor: None,
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

    /// Filter target objects where field equals value
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

    /// Filter target objects where field does not equal value
    pub fn where_ne(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field is greater than value
    pub fn where_gt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field is greater than or equal to value
    pub fn where_gte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field is less than value
    pub fn where_lt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field is less than or equal to value
    pub fn where_lte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field contains value
    pub fn where_contains(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::default(),
            }),
        });
        self
    }

    pub fn where_contains_all(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter target objects where field begins with value (prefix search)
    pub fn where_begins_with(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::default(),
            }),
        });
        self
    }

    // OR variants for target objects
    pub fn or_eq(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_ne(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_gt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_gte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_lt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_lte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_contains(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_contains_all(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn or_begins_with(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::Or,
            }),
        });
        self
    }

    // ============================================================
    // EDGE FILTERS (filters on the edge/pivot properties)
    // ============================================================

    /// Generic filter on edge properties
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

    /// Filter edges where field equals value
    pub fn edge_eq(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field does not equal value
    pub fn edge_ne(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field is greater than value
    pub fn edge_gt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field is greater than or equal to value
    pub fn edge_gte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field is less than value
    pub fn edge_lt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field is less than or equal to value
    pub fn edge_lte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field contains value
    pub fn edge_contains(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::default(),
            }),
        });
        self
    }

    pub fn edge_contains_all(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Filter edges where field begins with value (prefix search)
    pub fn edge_begins_with(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::default(),
            }),
        });
        self
    }

    // OR variants for edges
    pub fn edge_or_eq(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_ne(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_gt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThan,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_gte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::GreaterThanOrEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_lt(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThan,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_lte(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::LessThanOrEqual,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_contains(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Contains,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_contains_all(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::ContainsAll,
                operator: Operator::Or,
            }),
        });
        self
    }

    pub fn edge_or_begins_with(
        mut self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::BeginsWith,
                operator: Operator::Or,
            }),
        });
        self
    }

    // ============================================================
    // SORTING
    // ============================================================

    /// Sort target objects by field in ascending order
    pub fn sort_asc(mut self, field: &'static IndexField) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        self
    }

    /// Sort target objects by field in descending order
    pub fn sort_desc(mut self, field: &'static IndexField) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: false }),
        });
        self
    }

    /// Sort edges by field in ascending order
    pub fn edge_sort_asc(mut self, field: &'static IndexField) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        self
    }

    /// Sort edges by field in descending order
    pub fn edge_sort_desc(mut self, field: &'static IndexField) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: field.name.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: false }),
        });
        self
    }

    /// Set limit for pagination
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set cursor for pagination
    pub fn with_cursor(mut self, cursor: Uuid) -> Self {
        self.cursor = Some(Cursor { last_id: cursor });
        self
    }

    /// Collect the target objects (traverse the edges and return the destinations)
    pub async fn collect(&self) -> Result<Vec<O>, Error> {
        // First, query the edges
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(cusor) = self.cursor {
            edge_query.cursor = Some(cusor);
        }

        let objects = self
            .adapter
            .fetch_object_from_edge_traversal_internal(
                E::TYPE,
                O::TYPE,
                self.owner,
                &self.filters,
                edge_query,
            )
            .await?;

        objects
            .into_iter()
            .map(|or| or.to_object())
            .collect::<Result<Vec<O>, Error>>()
    }

    /// Collect the target objects (traverse the edges and return the destinations)
    pub async fn collect_reverse(&self) -> Result<Vec<O>, Error> {
        // First, query the edges
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(cusor) = self.cursor {
            edge_query.cursor = Some(cusor);
        }

        let objects = self
            .adapter
            .fetch_object_from_edge_reverse_traversal_internal(
                E::TYPE,
                O::TYPE,
                self.owner,
                &self.filters,
                edge_query,
            )
            .await?;

        objects
            .into_iter()
            .map(|or| or.to_object())
            .collect::<Result<Vec<O>, Error>>()
    }

    /// Collect the edges themselves (not the target objects)
    pub async fn collect_edges(&self) -> Result<Vec<E>, Error> {
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.cursor {
            edge_query.cursor = Some(offset);
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

    /// Collect the edges themselves (not the target objects)
    pub async fn collect_reverse_edges(&self) -> Result<Vec<E>, Error> {
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.cursor {
            edge_query.cursor = Some(offset);
        }

        let edge_records = self
            .adapter
            .query_reverse_edges(E::TYPE, self.owner, edge_query)
            .await?;

        edge_records
            .into_iter()
            .map(|r| r.to_edge())
            .collect::<Result<Vec<E>, Error>>()
    }

    /// Collect edges with their forward targets in a single JOIN query.
    pub async fn collect_with_target(&self) -> Result<Vec<ObjectEdge<E, O>>, Error> {
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.cursor {
            edge_query.cursor = Some(offset);
        }

        self.adapter
            .query_edges_with_targets(E::TYPE, O::TYPE, self.owner, &self.filters, edge_query)
            .await?
            .into_iter()
            .map(|(er, or)| Ok(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<O>()?)))
            .collect()
    }

    /// Collect edges with their reverse sources in a single JOIN query.
    pub async fn collect_reverse_with_target(&self) -> Result<Vec<ObjectEdge<E, O>>, Error> {
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(offset) = self.cursor {
            edge_query.cursor = Some(offset);
        }

        self.adapter
            .query_reverse_edges_with_sources(
                E::TYPE, O::TYPE, self.owner, &self.filters, edge_query,
            )
            .await?
            .into_iter()
            .map(|(er, or)| Ok(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<O>()?)))
            .collect()
    }

    /// Paginate using a cursor
    pub fn paginate(mut self, cursor: Option<impl Into<Cursor>>) -> Self {
        if let Some(cursor) = cursor {
            let _cursor: Cursor = cursor.into();
            self.cursor = Some(_cursor);
        }
        self
    }

    fn build_edge_query(&self) -> EdgeQuery {
        let mut eq = EdgeQuery::default();
        eq.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            eq.limit = Some(limit);
        }
        if let Some(cursor) = self.cursor {
            eq.cursor = Some(cursor);
        }
        eq
    }

    /// Collect both forward and reverse objects in one UNION query.
    /// Returns (following, followers) — forward traversal first, reverse second.
    pub async fn collect_both(&self) -> Result<(Vec<O>, Vec<O>), Error> {
        let edge_query = self.build_edge_query();
        let (fwd, rev) = self
            .adapter
            .query_edges_both_directions_with_objects(
                E::TYPE,
                O::TYPE,
                self.owner,
                &self.filters,
                edge_query,
            )
            .await?;

        let fwd = fwd
            .into_iter()
            .map(|(_, or)| or.to_object::<O>())
            .collect::<Result<Vec<O>, Error>>()?;
        let rev = rev
            .into_iter()
            .map(|(_, or)| or.to_object::<O>())
            .collect::<Result<Vec<O>, Error>>()?;
        Ok((fwd, rev))
    }

    /// Collect both directions with edge+object pairs in one UNION query.
    pub async fn collect_both_with_target(
        &self,
    ) -> Result<(Vec<ObjectEdge<E, O>>, Vec<ObjectEdge<E, O>>), Error> {
        let edge_query = self.build_edge_query();
        let (fwd, rev) = self
            .adapter
            .query_edges_both_directions_with_objects(
                E::TYPE,
                O::TYPE,
                self.owner,
                &self.filters,
                edge_query,
            )
            .await?;

        let fwd = fwd
            .into_iter()
            .map(|(er, or)| Ok(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<O>()?)))
            .collect::<Result<Vec<_>, Error>>()?;
        let rev = rev
            .into_iter()
            .map(|(er, or)| Ok(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<O>()?)))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok((fwd, rev))
    }

    /// Collect edges in both directions in one UNION query.
    /// Returns (forward_edges, reverse_edges).
    pub async fn collect_both_edges(&self) -> Result<(Vec<E>, Vec<E>), Error> {
        let edge_query = self.build_edge_query();
        let (fwd, rev) = self
            .adapter
            .query_edges_both_directions(E::TYPE, self.owner, edge_query)
            .await?;

        let fwd = fwd
            .into_iter()
            .map(|r| r.to_edge::<E>())
            .collect::<Result<Vec<E>, Error>>()?;
        let rev = rev
            .into_iter()
            .map(|r| r.to_edge::<E>())
            .collect::<Result<Vec<E>, Error>>()?;
        Ok((fwd, rev))
    }
}

// ============================================================
// Multi-Pivot Preload API
// ============================================================

/// Entry point for multi-pivot queries.
/// Created via `Engine::preload_objects::<P>(query)` or `adapter.preload_objects(query)`.
pub struct MultiPreloadContext<'a, P: Object> {
    adapter: &'a dyn Adapter,
    query: Query,
    _marker: std::marker::PhantomData<P>,
}

impl<'a, P: Object> MultiPreloadContext<'a, P> {
    pub(crate) fn new(adapter: &'a dyn Adapter, query: Query) -> Self {
        Self {
            adapter,
            query,
            _marker: std::marker::PhantomData,
        }
    }

    /// Traverse typed edges from each parent. Configurable with edge/object filters.
    /// Call `.collect()`, `.collect_reverse()`, `.count()`, etc. on the returned context.
    pub fn edge<E: Edge, C: Object>(self) -> MultiEdgeContext<'a, E, P, C> {
        MultiEdgeContext::new(self.adapter, self.query)
    }

    /// Fetch ownership-children for each parent. Parent IDs become owner IDs on children.
    pub fn preload<C: Object>(self) -> MultiOwnedContext<'a, P, C> {
        MultiOwnedContext::new(self.adapter, self.query)
    }
}

/// Multi-pivot edge context: executes exactly 2 queries — one for parents, one batch join.
pub struct MultiEdgeContext<'a, E: Edge, P: Object, C: Object> {
    adapter: &'a dyn Adapter,
    parent_query: Query,
    edge_query: EdgeQuery,
    obj_filters: Vec<QueryFilter>,
    _marker: std::marker::PhantomData<(E, P, C)>,
}

impl<'a, E: Edge, P: Object, C: Object> MultiEdgeContext<'a, E, P, C> {
    pub(crate) fn new(adapter: &'a dyn Adapter, parent_query: Query) -> Self {
        Self {
            adapter,
            parent_query,
            edge_query: EdgeQuery::default(),
            obj_filters: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Apply a complete EdgeQuery (filters + limit + cursor) for edge traversal.
    pub fn with_edge_query(mut self, edge_query: EdgeQuery) -> Self {
        self.edge_query = edge_query;
        self
    }

    /// Filter the connected objects (not the edges).
    pub fn obj_eq(mut self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        self.obj_filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::Equal,
                operator: Operator::default(),
            }),
        });
        self
    }

    /// Forward: edges WHERE "from" IN parent_ids → joined target objects.
    /// Returns Vec<(P, Vec<C>)> — exactly 2 queries.
    pub async fn collect(self) -> Result<Vec<(P, Vec<C>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let pairs = self
            .adapter
            .query_edges_with_targets_batch(
                E::TYPE,
                C::TYPE,
                &parent_ids,
                &self.obj_filters,
                self.edge_query,
            )
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<C>> =
            std::collections::HashMap::new();
        for (er, or) in pairs {
            grouped.entry(er.from).or_default().push(or.to_object::<C>()?);
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let children = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, children))
            })
            .collect()
    }

    /// Reverse: edges WHERE "to" IN parent_ids → joined source objects.
    /// Returns Vec<(P, Vec<C>)> — exactly 2 queries.
    pub async fn collect_reverse(self) -> Result<Vec<(P, Vec<C>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let pairs = self
            .adapter
            .query_reverse_edges_with_sources_batch(
                E::TYPE,
                C::TYPE,
                &parent_ids,
                &self.obj_filters,
                self.edge_query,
            )
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<C>> =
            std::collections::HashMap::new();
        for (er, or) in pairs {
            grouped.entry(er.to).or_default().push(or.to_object::<C>()?);
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let children = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, children))
            })
            .collect()
    }

    /// Forward edges only — no object JOIN.
    /// Returns Vec<(P, Vec<E>)> — exactly 2 queries.
    pub async fn collect_edges(self) -> Result<Vec<(P, Vec<E>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let edge_records = self
            .adapter
            .query_edges_batch(E::TYPE, &parent_ids, self.edge_query)
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<E>> =
            std::collections::HashMap::new();
        for er in edge_records {
            grouped.entry(er.from).or_default().push(er.to_edge::<E>()?);
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let edges = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, edges))
            })
            .collect()
    }

    /// Reverse edges only — no object JOIN.
    /// Returns Vec<(P, Vec<E>)> — exactly 2 queries.
    pub async fn collect_reverse_edges(self) -> Result<Vec<(P, Vec<E>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let edge_records = self
            .adapter
            .query_reverse_edges_batch(E::TYPE, &parent_ids, self.edge_query)
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<E>> =
            std::collections::HashMap::new();
        for er in edge_records {
            grouped.entry(er.to).or_default().push(er.to_edge::<E>()?);
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let edges = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, edges))
            })
            .collect()
    }

    /// Forward join: edges + target objects per parent.
    /// Returns Vec<(P, Vec<ObjectEdge<E, C>>)> — exactly 2 queries.
    pub async fn collect_with_target(self) -> Result<Vec<(P, Vec<ObjectEdge<E, C>>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let pairs = self
            .adapter
            .query_edges_with_targets_batch(
                E::TYPE,
                C::TYPE,
                &parent_ids,
                &self.obj_filters,
                self.edge_query,
            )
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<ObjectEdge<E, C>>> =
            std::collections::HashMap::new();
        for (er, or) in pairs {
            let from = er.from;
            grouped
                .entry(from)
                .or_default()
                .push(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<C>()?));
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let items = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, items))
            })
            .collect()
    }

    /// Reverse join: edges + source objects per parent.
    /// Returns Vec<(P, Vec<ObjectEdge<E, C>>)> — exactly 2 queries.
    pub async fn collect_reverse_with_target(
        self,
    ) -> Result<Vec<(P, Vec<ObjectEdge<E, C>>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let pairs = self
            .adapter
            .query_reverse_edges_with_sources_batch(
                E::TYPE,
                C::TYPE,
                &parent_ids,
                &self.obj_filters,
                self.edge_query,
            )
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<ObjectEdge<E, C>>> =
            std::collections::HashMap::new();
        for (er, or) in pairs {
            let to = er.to;
            grouped
                .entry(to)
                .or_default()
                .push(ObjectEdge::new(er.to_edge::<E>()?, or.to_object::<C>()?));
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let items = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, items))
            })
            .collect()
    }

    /// Forward edge count per parent — GROUP BY, exactly 2 queries.
    /// Returns Vec<(P, u64)>.
    pub async fn count(self) -> Result<Vec<(P, u64)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let counts = self
            .adapter
            .count_edges_batch(E::TYPE, &parent_ids, self.edge_query)
            .await?;

        let count_map: std::collections::HashMap<Uuid, u64> = counts.into_iter().collect();

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let n = count_map.get(&p.meta().id()).copied().unwrap_or(0);
                Ok((p, n))
            })
            .collect()
    }

    /// Reverse edge count per parent — GROUP BY, exactly 2 queries.
    /// Returns Vec<(P, u64)>.
    pub async fn count_reverse(self) -> Result<Vec<(P, u64)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let counts = self
            .adapter
            .count_reverse_edges_batch(E::TYPE, &parent_ids, self.edge_query)
            .await?;

        let count_map: std::collections::HashMap<Uuid, u64> = counts.into_iter().collect();

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let n = count_map.get(&p.meta().id()).copied().unwrap_or(0);
                Ok((p, n))
            })
            .collect()
    }
}

/// Single-pivot ownership context: fetch owned children for one parent.
/// Mirrors `MultiOwnedContext` but for a single known parent ID.
pub struct OwnedContext<'a, C: Object> {
    adapter: &'a dyn Adapter,
    owner: Uuid,
    _marker: std::marker::PhantomData<C>,
}

impl<'a, C: Object> OwnedContext<'a, C> {
    pub(crate) fn new(adapter: &'a dyn Adapter, owner: Uuid) -> Self {
        Self {
            adapter,
            owner,
            _marker: std::marker::PhantomData,
        }
    }

    /// Fetch all objects owned by this parent.
    pub async fn collect(self) -> Result<Vec<C>, Error> {
        let records = self.adapter.fetch_owned_objects(C::TYPE, self.owner).await?;
        records
            .into_iter()
            .map(|or| or.to_object())
            .collect::<Result<Vec<C>, Error>>()
    }
}

/// Multi-pivot ownership context: fetch owned children for each parent.
/// Executes exactly 2 queries — one for parents, one batch ownership fetch.
pub struct MultiOwnedContext<'a, P: Object, C: Object> {
    adapter: &'a dyn Adapter,
    parent_query: Query,
    _marker: std::marker::PhantomData<(P, C)>,
}

impl<'a, P: Object, C: Object> MultiOwnedContext<'a, P, C> {
    pub(crate) fn new(adapter: &'a dyn Adapter, parent_query: Query) -> Self {
        Self {
            adapter,
            parent_query,
            _marker: std::marker::PhantomData,
        }
    }

    /// Fetch all children owned by each parent.
    /// Returns Vec<(P, Vec<C>)> — exactly 2 queries.
    pub async fn collect(self) -> Result<Vec<(P, Vec<C>)>, Error> {
        let parents = self.adapter.query_objects(P::TYPE, self.parent_query).await?;
        if parents.is_empty() {
            return Ok(Vec::new());
        }
        let owner_ids: Vec<Uuid> = parents.iter().map(|p| p.id).collect();

        let children = self
            .adapter
            .fetch_owned_objects_batch(C::TYPE, &owner_ids)
            .await?;

        let mut grouped: std::collections::HashMap<Uuid, Vec<C>> =
            std::collections::HashMap::new();
        for cr in children {
            let owner = cr.owner;
            grouped.entry(owner).or_default().push(cr.to_object::<C>()?);
        }

        parents
            .into_iter()
            .map(|pr| {
                let p = pr.to_object::<P>()?;
                let children = grouped.remove(&p.meta().id()).unwrap_or_default();
                Ok((p, children))
            })
            .collect()
    }
}
