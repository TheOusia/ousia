use uuid::Uuid;

use crate::query::{
    Comparison, Cursor, IndexField, Operator, QueryFilter, QueryMode, QuerySearch, QuerySort,
    ToIndexValue, SORT_RANDOM_FIELD,
};

/// Builder for edge queries. Filters apply to the edge's own indexed fields,
/// not to the source or target object.
///
/// All filter and sort methods consume `self` and return `Self`:
///
/// ```rust,ignore
/// let eq = EdgeQuery::default()
///     .where_eq(Follow::FIELDS.status, "active")
///     .sort_desc(Follow::FIELDS.created_at)
///     .with_limit(50);
/// ```
///
/// Fields must be declared with `index = "field:search"` or
/// `index = "field:sort"` in the edge's `OusiaEdge` derive macro. Use
/// `E::FIELDS.field_name` to obtain the required `&'static IndexField`.
///
/// When you need to filter both edges and their target objects in a single
/// query, use the `edge_*` methods on [`EdgeQueryContext`] instead of building
/// a bare `EdgeQuery`.
#[derive(Debug, Clone)]
pub struct EdgeQuery {
    pub filters: Vec<QueryFilter>,
    pub limit: Option<u32>,
    pub cursor: Option<Cursor>,
}

impl Default for EdgeQuery {
    /// Returns an empty `EdgeQuery` with no filters, no limit, and no cursor.
    fn default() -> Self {
        Self {
            filters: Vec::new(),
            limit: None,
            cursor: None,
        }
    }
}

pub struct ObjectEdge<E: super::Edge, O: crate::Object> {
    edge: E,
    object: O,
}

impl<E: super::Edge, O: crate::Object> ObjectEdge<E, O> {
    pub fn new(edge: E, object: O) -> Self {
        Self { edge, object }
    }

    pub fn edge(&self) -> &E {
        &self.edge
    }

    pub fn object(&self) -> &O {
        &self.object
    }

    pub fn into_parts(self) -> (E, O) {
        (self.edge, self.object)
    }
}

impl EdgeQuery {
    /// Low-level filter insertion. Prefer the typed `where_*` helpers when possible.
    ///
    /// Appends a [`QueryFilter`] with the given field, value, and mode. Useful
    /// for constructing filters programmatically or passing a pre-built
    /// [`QueryMode`].
    pub fn with_filter(
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

    /// Filter edges where `field = value`.
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

    /// Filter edges where `field != value`.
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

    /// Filter edges where `field > value`.
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

    /// Filter edges where `field >= value`.
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

    /// Filter edges where `field < value`.
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

    /// Filter edges where `field <= value`.
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

    /// Filter edges where `field` contains `value`.
    ///
    /// For string fields: `ILIKE '%value%'` with GIN trigram acceleration.
    /// For array fields: overlap check (`field && value`) using the GIN index.
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

    /// Filter edges where a string field starts with `value` (`ILIKE 'value%'`).
    ///
    /// Uses the GIN-backed prefix extraction index when available.
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

    /// Filter edges where a string field does NOT start with `value`.
    ///
    /// No GIN support for negated prefix matches; performs a full edge table
    /// scan for the owner. Pair with other indexed filters to limit row count.
    pub fn where_not_begins_with(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotBeginsWith,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    /// Filter edges where `field` does NOT contain `value`.
    ///
    /// GIN indexes cannot accelerate NOT LIKE or exclusion array checks; this
    /// performs a full edge table scan. Combine with other indexed filters to
    /// reduce the scanned row count.
    pub fn where_not_contains(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotContains,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    /// Filter edges where an array field does NOT contain all of the provided values.
    ///
    /// Negation of a GIN `@>` check; GIN indexes do not accelerate this, so a
    /// full edge table scan is performed.
    pub fn where_not_contains_all(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotContainsAll,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    /// Filter edges where a scalar field is NOT IN the supplied array.
    ///
    /// On Postgres/CockroachDB the value is bound as a typed array parameter.
    /// Uses a B-tree index on the field when one exists; otherwise a full scan.
    pub fn where_not_in(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotIn,
                operator: Operator::default(),
            }),
        });
        consumed_self
    }

    /// Sort results by `field` in ascending order (`ORDER BY field ASC`).
    ///
    /// The field must be declared with `index = "field:sort"`.
    pub fn sort_asc(self, field: &'static IndexField) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: true.to_index_value(), // Dummy value for sort
            mode: QueryMode::Sort(QuerySort { ascending: true }),
        });
        consumed_self
    }

    /// Sort results by `field` in descending order (`ORDER BY field DESC`).
    ///
    /// The field must be declared with `index = "field:sort"`.
    pub fn sort_desc(self, field: &'static IndexField) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: true.to_index_value(), // Dummy value for sort
            mode: QueryMode::Sort(QuerySort { ascending: false }),
        });
        consumed_self
    }

    /// Sort results randomly (`ORDER BY RANDOM()`).
    ///
    /// Requires a full edge table scan. Never use on large result sets without
    /// pairing with a `with_limit` call.
    pub fn sort_random(self) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field: &SORT_RANDOM_FIELD,
            value: true.to_index_value(),
            mode: QueryMode::SortRandom,
        });
        consumed_self
    }

    // OR operator variants — same semantics as the `where_*` methods but the
    // condition is joined to the previous filter with OR instead of AND.

    /// Add an OR `field = value` condition.
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

    /// Add an OR `field != value` condition.
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

    /// Add an OR `field > value` condition.
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

    /// Add an OR `field >= value` condition.
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

    /// Add an OR `field < value` condition.
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

    /// Add an OR `field <= value` condition.
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

    /// Add an OR contains condition. See [`EdgeQuery::where_contains`] for semantics.
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

    /// Add an OR begins-with condition. See [`EdgeQuery::where_begins_with`] for semantics.
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

    /// Add an OR not-begins-with condition. See [`EdgeQuery::where_not_begins_with`] for semantics.
    pub fn or_not_begins_with(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotBeginsWith,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    /// Add an OR not-contains condition. See [`EdgeQuery::where_not_contains`] for semantics.
    pub fn or_not_contains(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotContains,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    /// Add an OR not-contains-all condition. See [`EdgeQuery::where_not_contains_all`] for semantics.
    pub fn or_not_contains_all(
        self,
        field: &'static IndexField,
        value: impl ToIndexValue,
    ) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotContainsAll,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    /// Add an OR not-in condition. See [`EdgeQuery::where_not_in`] for semantics.
    pub fn or_not_in(self, field: &'static IndexField, value: impl ToIndexValue) -> Self {
        let mut consumed_self = self;
        consumed_self.filters.push(QueryFilter {
            field,
            value: value.to_index_value(),
            mode: QueryMode::Search(QuerySearch {
                comparison: Comparison::NotIn,
                operator: Operator::Or,
            }),
        });
        consumed_self
    }

    /// Cap the number of edges returned by this query.
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Continue pagination from the edge identified by this UUID (exclusive).
    ///
    /// Pass the `id` of the last edge returned by the previous page. The next
    /// page starts immediately after that edge in the current sort order.
    pub fn with_cursor(mut self, cursor: Uuid) -> Self {
        self.cursor = Some(Cursor { last_id: cursor });
        self
    }
}
