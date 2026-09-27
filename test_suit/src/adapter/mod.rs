#[cfg(test)]
mod postgres;
pub mod test_v2_schema;

use ousia::{EdgeMeta, Meta, OusiaDefault, OusiaEdge, OusiaObject, query::ToIndexValue};
use serde::{Deserialize, Serialize};

/// Example: Blog Post object
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Post",
    index = "title:search+sort",
    index = "status:search",
    index = "tags:search"
)]
pub struct Post {
    _meta: Meta,

    pub title: String,
    pub content: String,
    pub status: PostStatus,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub tags: Vec<String>,
}

#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Post",
    index = "title:search+sort",
    index = "status:search",
    index = "tags:search"
)]
pub struct PostNew {
    _meta: Meta,

    // Tags 0-4 deliberately match `Post`'s — this is the exact schema-
    // evolution scenario: data written as `Post` (tags 0-4 only) must
    // still decode correctly as `PostNew`, with `rating` (tag 5, absent
    // from old data) filled in via its default.
    pub title: String,
    pub content: String,
    pub status: PostStatus,
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    pub tags: Vec<String>,
    #[ousia(default = 10)]
    pub rating: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub enum PostStatus {
    #[default]
    Draft,
    Published,
    Archived,
}

// Implement ToIndexValue for custom enum
impl ousia::query::ToIndexValue for PostStatus {
    fn to_index_value(&self) -> ousia::query::IndexValue {
        let s = match self {
            PostStatus::Draft => "draft",
            PostStatus::Published => "published",
            PostStatus::Archived => "archived",
        };
        ousia::query::IndexValue::String(s.to_string())
    }
}

/// Example: User object
#[derive(Debug, Serialize, Deserialize)]
pub struct Wallet {
    inner: i64,
}

impl Default for Wallet {
    fn default() -> Self {
        Self { inner: 0 }
    }
}

impl ToIndexValue for Wallet {
    fn to_index_value(&self) -> ousia::query::IndexValue {
        ousia::query::IndexValue::Int(self.inner)
    }
}

#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "User",
    unique = "username",
    index = "email:search+sort",
    index = "username:search+sort",
    index = "balance:search"
)]
pub struct User {
    _meta: Meta,

    pub username: String,
    pub email: String,
    pub display_name: String,
    pub balance: Wallet,
}

#[derive(Debug, OusiaEdge)]
#[ousia(
    type_name = "Follow",
    from = User,
    to = User,
    index = "notification:search"
)]
struct Follow {
    _meta: EdgeMeta,
    notification: bool,
}

/// 3-level chain fixtures for the 2-hop batch traversal tests
/// (Hub -[HubSpoke]-> Spoke -[SpokeLeaf]-> Leaf) — mirrors the motivating
/// Highlight -[HighlightItem]-> Item -[ItemModifier]-> Modifier shape.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(type_name = "Hub", index = "name:search")]
pub struct Hub {
    _meta: Meta,

    pub name: String,
}

#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(type_name = "Spoke", index = "name:search")]
pub struct Spoke {
    _meta: Meta,

    pub name: String,
}

#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(type_name = "Leaf", index = "name:search")]
pub struct Leaf {
    _meta: Meta,

    pub name: String,
}

#[derive(Debug, OusiaEdge)]
#[ousia(
    type_name = "HubSpoke",
    from = Hub,
    to = Spoke,
    index = "position:search"
)]
pub struct HubSpoke {
    _meta: EdgeMeta,
    pub position: i64,
}

#[derive(Debug, OusiaEdge)]
#[ousia(
    type_name = "SpokeLeaf",
    from = Spoke,
    to = Leaf,
    index = "required:search"
)]
pub struct SpokeLeaf {
    _meta: EdgeMeta,
    pub required: bool,
}

/// Test object with a single geo index. The virtual field name `"location"`
/// does NOT exist as a struct field — only the referenced `lat` / `lon` do.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Place",
    index = "name:search",
    index = "location:geo(lat, lon)"
)]
pub struct Place {
    _meta: Meta,

    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

/// Newtype wrapper so a `chrono::DateTime<Utc>` field can derive `Default`
/// (the underlying type doesn't impl Default).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct EventTime(pub chrono::DateTime<chrono::Utc>);

impl Default for EventTime {
    fn default() -> Self {
        Self(chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap())
    }
}

impl ToIndexValue for EventTime {
    const SORT_AS: ousia::query::SortAs = ousia::query::SortAs::Timestamp;

    fn to_index_value(&self) -> ousia::query::IndexValue {
        ousia::query::IndexValue::Timestamp(self.0)
    }
}

/// Object that exercises every `IndexValue` variant via a single indexed
/// field per variant. Used by `test_query_all_index_value_variants`.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Variants",
    index = "name:search+sort",
    index = "count:search+sort",
    index = "price:search+sort",
    index = "active:search",
    index = "uid:search",
    index = "occurred_at:search+sort",
    index = "tags:search",
    index = "scores:search"
)]
pub struct Variants {
    _meta: Meta,

    pub name: String,
    pub count: i64,
    pub price: f64,
    pub active: bool,
    pub uid: uuid::Uuid,
    pub occurred_at: EventTime,
    pub tags: Vec<String>,
    pub scores: Vec<i64>,
}

/// Test object with two geo indexes on the same struct.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "Delivery",
    index = "pickup:geo(pickup_lat, pickup_lon)",
    index = "dropoff:geo(dropoff_lat, dropoff_lon)"
)]
pub struct Delivery {
    _meta: Meta,

    pub pickup_lat: f64,
    pub pickup_lon: f64,

    pub dropoff_lat: f64,
    pub dropoff_lon: f64,
}

/// At most one per owner — exercises unique constraints keyed on `owner`.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(type_name = "Slot", unique = "owner")]
pub struct Slot {
    _meta: Meta,

    pub label: String,
}

/// `display` was previously stored under the key `handle`.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(type_name = "Profile")]
pub struct Profile {
    _meta: Meta,

    #[ousia(rename = "handle")]
    pub display: String,
}

/// Composite-index fixture in the shape of an activity feed: an `index_meta`
/// field + a native column (uuid-as-text and int variants), two `index_meta`
/// fields together, and two `index_meta` fields (text + int) + a native column.
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "FeedEvent",
    index = "actor:search",
    index = "score:search",
    index = "location_key:search",
    index = "state:search",
    composite_index = "actor, created_at desc",
    composite_index = "score, created_at DESC",
    index = "region:search",
    index = "tier:search",
    composite_index = "location_key, state",
    composite_index = "region, tier, created_at desc"
)]
pub struct FeedEvent {
    _meta: Meta,

    pub actor: uuid::Uuid,
    pub score: i64,
    pub location_key: String,
    pub state: String,
    pub region: String,
    pub tier: i64,
}

/// Composite indexes whose generated names run past Postgres' 63-byte
/// identifier limit and share their first 63 bytes. Only its schema is used.
#[allow(dead_code)]
#[derive(OusiaObject, OusiaDefault, Debug)]
#[ousia(
    type_name = "LongIndexNames",
    index = "a_rather_long_field_name_for_testing:search",
    index = "another_rather_long_field_name_one:search",
    index = "another_rather_long_field_name_two:search",
    composite_index = "a_rather_long_field_name_for_testing, another_rather_long_field_name_one",
    composite_index = "a_rather_long_field_name_for_testing, another_rather_long_field_name_two"
)]
pub struct LongIndexNames {
    _meta: Meta,

    pub a_rather_long_field_name_for_testing: String,
    pub another_rather_long_field_name_one: String,
    pub another_rather_long_field_name_two: String,
}
