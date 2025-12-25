use ulid::Ulid;

use crate::query::{Cursor, IndexField, QueryFilter, QueryMode, ToIndexValue};

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
            // sort: Vec::new(),
            // search: Vec::new(),
            limit: None,
            cursor: None,
        }
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

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_cursor(mut self, cursor: Ulid) -> Self {
        self.cursor = Some(Cursor { last_id: cursor });
        self
    }
}
