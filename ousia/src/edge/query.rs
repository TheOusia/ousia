use uuid::Uuid;

use crate::query::{
    Comparison, Cursor, IndexField, Operator, QueryFilter, QueryMode, QuerySearch, QuerySort,
    ToIndexValue,
};

/// -----------------------------
/// Edge Query Plan (storage contract)
/// -----------------------------

#[derive(Debug)]
pub struct EdgeQuery {
    pub filters: Vec<QueryFilter>,
    pub limit: Option<u32>,
    pub cursor: Option<Cursor>,
}

impl Default for EdgeQuery {
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

    // Contains (for strings)
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
