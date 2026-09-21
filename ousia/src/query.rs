use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexMeta(pub BTreeMap<String, IndexValue>);

impl IndexMeta {
    pub fn meta(&self) -> &BTreeMap<String, IndexValue> {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum IndexValueInner {
    String(String),
    Int(i64),
    Float(f64),
}

impl IndexValueInner {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            IndexValueInner::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            IndexValueInner::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            IndexValueInner::Float(f) => Some(*f),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum IndexValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Uuid(Uuid),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Array(Vec<IndexValueInner>),
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

    pub fn as_array(&self) -> Option<Vec<IndexValueInner>> {
        match self {
            IndexValue::Array(a) => Some(a.clone()),
            _ => None,
        }
    }
}

// Helper trait to convert types to IndexValue
pub trait ToIndexValue {
    /// How `sort_asc` / `sort_desc` order this field. The default (`Json`)
    /// orders numbers numerically and strings as text. A custom type whose
    /// `to_index_value` returns `IndexValue::Timestamp` must set `Timestamp`,
    /// since timestamps are stored as text with a varying number of digits.
    const SORT_AS: SortAs = SortAs::Json;

    fn to_index_value(&self) -> IndexValue;
}

/// How a sorted field is compared in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortAs {
    /// By the stored JSON value: numbers by value, strings as text.
    Json,
    /// As a point in time.
    Timestamp,
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
    const SORT_AS: SortAs = SortAs::Timestamp;

    fn to_index_value(&self) -> IndexValue {
        IndexValue::Timestamp(*self)
    }
}

impl ToIndexValue for IndexValueInner {
    fn to_index_value(&self) -> IndexValue {
        match self {
            IndexValueInner::String(s) => IndexValue::String(s.clone()),
            IndexValueInner::Int(i) => IndexValue::Int(*i),
            IndexValueInner::Float(f) => IndexValue::Float(*f),
        }
    }
}

impl ToIndexValue for Vec<IndexValueInner> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Array(self.clone())
    }
}

impl ToIndexValue for Vec<String> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Array(
            self.iter()
                .map(|s| IndexValueInner::String(s.clone()))
                .collect(),
        )
    }
}

impl ToIndexValue for Vec<&str> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Array(
            self.iter()
                .map(|s| IndexValueInner::String(s.to_string()))
                .collect(),
        )
    }
}

impl ToIndexValue for Vec<i64> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Array(self.iter().map(|i| IndexValueInner::Int(*i)).collect())
    }
}

impl ToIndexValue for Vec<f64> {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Array(self.iter().map(|f| IndexValueInner::Float(*f)).collect())
    }
}

impl ToIndexValue for Uuid {
    fn to_index_value(&self) -> IndexValue {
        IndexValue::Uuid(self.clone())
    }
}

impl<T: ToIndexValue + Default> ToIndexValue for Option<T> {
    fn to_index_value(&self) -> IndexValue {
        match self {
            Some(val) => val.to_index_value(),
            None => T::default().to_index_value(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Search, // equality + adapter-defined text matching
    Sort,   // ordered comparison
    /// Spatial index. The virtual field name lives on `IndexField.name`; the
    /// `lat_field` / `lon_field` here are the struct field names that source
    /// the coordinates at write time.
    Geo {
        lat_field: &'static str,
        lon_field: &'static str,
    },
}

/// A single geo point produced by an object at write time.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoPoint {
    pub field: &'static str,
    pub lon: f64,
    pub lat: f64,
    pub hash: String,
}

/// Geo spatial filter attached to a `Query`. Each variant targets a single
/// geo-indexed field by name. Multiple filters can be combined on a `Query`
/// (Postgres emits one aliased `object_geo` JOIN per filter).
#[derive(Debug, Clone, PartialEq)]
pub enum GeoFilter {
    /// `ST_DWithin` — match objects whose geo point is within `radius_m`
    /// meters of (`lon`, `lat`).
    Within {
        field: String,
        lon: f64,
        lat: f64,
        radius_m: f64,
    },
    /// `ST_Within(geometry, ST_MakeEnvelope(...))` — match objects whose
    /// geo point falls inside the axis-aligned bounding box. Does NOT
    /// handle antimeridian crossing (max_lon < min_lon returns empty).
    InBbox {
        field: String,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    },
}

impl GeoFilter {
    /// The geo field name this filter targets.
    pub fn field(&self) -> &str {
        match self {
            GeoFilter::Within { field, .. } | GeoFilter::InBbox { field, .. } => field,
        }
    }
}

/// Distance-based ordering: `ORDER BY <field>.location <-> ST_MakePoint(lon, lat)`.
/// Doubles as the distance anchor for `collect_with_distance()`.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoOrder {
    pub field: String,
    pub lon: f64,
    pub lat: f64,
    pub ascending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexField {
    pub name: &'static str,
    pub kinds: &'static [IndexKind],
    pub sort_as: SortAs,
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
    /// `ORDER BY RANDOM()` — full sort of the matching rows; not cursor-paginatable.
    SortRandom,
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

    pub fn is_random_sort(&self) -> bool {
        matches!(self, QueryMode::SortRandom)
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

    pub fn sort_random() -> Self {
        QueryMode::SortRandom
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
    NotBeginsWith,
    Contains,
    NotContains,
    ContainsAll,
    NotContainsAll,
    /// Scalar field is not any of the supplied array values.
    NotIn,
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

/// Placeholder field for `QueryFilter::random_sort`; never rendered into SQL.
pub static SORT_RANDOM_FIELD: IndexField = IndexField {
    name: "__random__",
    kinds: &[],
    sort_as: SortAs::Json,
};

impl QueryFilter {
    pub(crate) fn search(
        field: &'static IndexField,
        value: IndexValue,
        comparison: Comparison,
        operator: Operator,
    ) -> Self {
        Self {
            field,
            value,
            mode: QueryMode::Search(QuerySearch { comparison, operator }),
        }
    }

    pub fn random_sort() -> Self {
        Self {
            field: &SORT_RANDOM_FIELD,
            value: IndexValue::Bool(false),
            mode: QueryMode::SortRandom,
        }
    }
}

/// A cursor is a keyset boundary, and `ORDER BY RANDOM()` has no order to page
/// through, so under a random sort the cursor is dropped with a warning.
pub(crate) fn cursor_unless_random(
    cursor: Option<Cursor>,
    filter_sets: &[&[QueryFilter]],
) -> Option<Cursor> {
    let random = filter_sets.iter().flat_map(|f| f.iter()).any(|f| f.mode.is_random_sort());
    if cursor.is_some() && random {
        eprintln!("[ousia warn] sort_random ignores the cursor; running the query without it");
        return None;
    }
    cursor
}

/// Pagination cursor
#[derive(Debug, Clone, Copy)]
pub struct Cursor {
    pub last_id: Uuid,
}

impl Into<Cursor> for Uuid {
    fn into(self) -> Cursor {
        Cursor { last_id: self }
    }
}
