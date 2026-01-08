use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexMeta(pub BTreeMap<String, IndexValue>);

impl IndexMeta {
    pub fn meta(&self) -> &BTreeMap<String, IndexValue> {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum IndexValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Timestamp(chrono::DateTime<chrono::Utc>),
}

impl IndexValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            IndexValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            IndexValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            IndexValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            IndexValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            IndexValue::Timestamp(t) => Some(*t),
            _ => None,
        }
    }
}

// Helper trait to convert types to IndexValue
pub trait ToIndexValue {
    fn to_index_value(&self) -> IndexValue;
}

impl ToIndexValue for String {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::String(self.clone())
    }
}

impl ToIndexValue for &str {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::String(self.to_string())
    }
}

impl ToIndexValue for i64 {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Int(*self)
    }
}

impl ToIndexValue for i32 {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Int(*self as i64)
    }
}

impl ToIndexValue for f64 {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Float(*self)
    }
}

impl ToIndexValue for f32 {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Float(*self as f64)
    }
}

impl ToIndexValue for bool {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Bool(*self)
    }
}

impl ToIndexValue for chrono::DateTime<chrono::Utc> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Timestamp(*self)
    }
}

impl ToIndexValue for Ulid {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::String(self.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Search, // equality + adapter-defined text matching
    Sort,   // ordered comparison
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexField {
    pub name: &'static str,
    pub kinds: &'static [IndexKind],
}

pub trait IndexQuery {
    fn indexed_fields() -> &'static [IndexField];
}

#[derive(Debug, Clone)]
pub struct QueryFilter {
    pub field: &'static IndexField,
    pub value: IndexValue,
    pub mode: QueryMode,
}

#[derive(Debug, Clone)]
pub enum QueryMode {
    Search(QuerySearch),
    Sort(QuerySort),
}

impl QueryMode {
    pub fn as_search(&self) -> Option<&QuerySearch> {
        match self {
            QueryMode::Search(search) => Some(search),
            _ => None,
        }
    }

    pub fn as_sort(&self) -> Option<&QuerySort> {
        match self {
            QueryMode::Sort(sort) => Some(sort),
            _ => None,
        }
    }

    pub fn search(comp: Comparison, op: Option<Operator>) -> Self {
        QueryMode::Search(QuerySearch {
            comparison: comp,
            operator: op.unwrap_or_default(),
        })
    }

    /// Search using default comparison '=' and 'AND' operator
    pub fn search_default() -> Self {
        QueryMode::Search(QuerySearch {
            comparison: Comparison::Equal,
            operator: Operator::And,
        })
    }

    pub fn sort(asc: bool) -> Self {
        QueryMode::Sort(QuerySort { ascending: asc })
    }

    /// Sort using AND operator
    pub fn sort_default() -> Self {
        QueryMode::Sort(QuerySort { ascending: true })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySearch {
    pub comparison: Comparison,
    pub operator: Operator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySort {
    pub ascending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Comparison {
    Equal,
    BeginsWith,
    Contains,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    NotEqual,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Operator {
    #[default]
    And,
    Or,
}

/// Pagination cursor
#[derive(Debug, Clone, Copy)]
pub struct Cursor {
    pub last_id: Ulid,
}

impl Into<Cursor> for Ulid {
    fn into(self) -> Cursor {
        Cursor { last_id: self }
    }
}
