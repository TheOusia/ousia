use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexMeta {
    pub values: BTreeMap<String, IndexValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Search,     // equality + adapter-defined text matching
    Sort,       // ordered comparison
    SearchSort, // both
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexField {
    pub name: &'static str,
    pub kinds: &'static [IndexKind],
}

pub trait ObjectQuery {
    fn indexed_fields() -> &'static [IndexField];
}
