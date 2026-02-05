use ousia::{EdgeMeta, Meta, OusiaDefault, OusiaEdge, OusiaObject, query::ToIndexValue};
use serde::{Deserialize, Serialize};

pub mod test_cockroach;
pub mod test_postgres;
pub mod test_sqlite;

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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum PostStatus {
    Draft,
    Published,
    Archived,
}

impl Default for PostStatus {
    fn default() -> Self {
        PostStatus::Draft
    }
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
    index = "email:search",
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

#[derive(Debug, OusiaEdge, OusiaDefault)]
#[ousia(type_name = "Follow", index = "notification:search")]
struct Follow {
    _meta: EdgeMeta,
    notification: bool,
}
