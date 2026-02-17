use super::Adapter;
use uuid::Uuid;

use crate::{
    Object,
    edge::{Edge, query::EdgeQuery},
    error::Error,
    query::{
        Comparison, Cursor, IndexField, Operator, QueryFilter, QueryMode, QuerySearch, QuerySort,
        ToIndexValue,
    },
    system_owner,
};

/// -----------------------------
/// Object Query Plan (storage contract)
/// -----------------------------

#[derive(Debug)]
pub struct Query {
    pub owner: Uuid, // enforced, never optional
    pub filters: Vec<QueryFilter>,
    pub limit: Option<u32>,
    pub cursor: Option<Cursor>,
}

impl Default for Query {
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
            value: true.to_index_value(), // Dummy value for sort
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        consumed_self
    }

    pub fn sort_desc(self, field: &'static IndexField) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: true.to_index_value(), // Dummy value for sort
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
            value: true.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        self
    }

    /// Sort target objects by field in descending order
    pub fn sort_desc(mut self, field: &'static IndexField) -> Self {
        self.filters.push(QueryFilter {
            field,
            value: true.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: false }),
        });
        self
    }

    /// Sort edges by field in ascending order
    pub fn edge_sort_asc(mut self, field: &'static IndexField) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: true.to_index_value(),
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        self
    }

    /// Sort edges by field in descending order
    pub fn edge_sort_desc(mut self, field: &'static IndexField) -> Self {
        self.edge_filters.push(QueryFilter {
            field,
            value: true.to_index_value(),
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
    pub async fn collect(self) -> Result<Vec<O>, Error> {
        // First, query the edges
        let mut edge_query = EdgeQuery::default();
        edge_query.filters = self.edge_filters.clone();
        if let Some(limit) = self.limit {
            edge_query.limit = Some(limit);
        }
        if let Some(cusor) = self.cursor {
            edge_query.cursor = Some(cusor);
        }

        let edge_records = self
            .adapter
            .query_edges(E::TYPE, self.owner, edge_query)
            .await?;

        // Extract the 'to' IDs from edges
        let target_ids: Vec<Uuid> = edge_records.iter().map(|e| e.to).collect();

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

    /// Paginate using a cursor
    pub fn paginate(mut self, cursor: Option<impl Into<Cursor>>) -> Self {
        if let Some(cursor) = cursor {
            let _cursor: Cursor = cursor.into();
            self.cursor = Some(_cursor);
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
                    use crate::query::IndexValue;
                    match (actual, expected) {
                        (IndexValue::String(a), IndexValue::String(e)) => a.contains(e),
                        (IndexValue::Int(a), IndexValue::Int(e)) => a == e,
                        (IndexValue::Float(a), IndexValue::Float(e)) => a == e,
                        (IndexValue::Bool(a), IndexValue::Bool(e)) => a == e,
                        (IndexValue::Uuid(a), IndexValue::Uuid(e)) => a == e,
                        (IndexValue::Timestamp(a), IndexValue::Timestamp(e)) => a == e,
                        (IndexValue::Array(a), IndexValue::Array(e)) => {
                            e.iter().any(|inner| a.contains(inner))
                        }
                        _ => false, // Handle other cases or mismatched types
                    }
                }
                Comparison::ContainsAll => {
                    use crate::query::IndexValue;
                    match (actual, expected) {
                        (IndexValue::String(a), IndexValue::String(e)) => a == e,
                        (IndexValue::Int(a), IndexValue::Int(e)) => a == e,
                        (IndexValue::Float(a), IndexValue::Float(e)) => a == e,
                        (IndexValue::Bool(a), IndexValue::Bool(e)) => a == e,
                        (IndexValue::Uuid(a), IndexValue::Uuid(e)) => a == e,
                        (IndexValue::Timestamp(a), IndexValue::Timestamp(e)) => a == e,
                        (IndexValue::Array(a), IndexValue::Array(e)) => {
                            e.iter().all(|inner| a.contains(inner))
                        }
                        _ => false, // Handle other cases or mismatched types
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
