use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{edge::meta::EdgeMeta, query::IndexMeta};

///
/// Derive macro is expected to produce
/// const FIELDS: &'static TypeNameIndexes {field_name: crate::query::IndexField,...}
pub trait Edge: Serialize + for<'de> Deserialize<'de> + Sized + Send + Sync + 'static {
    /// Edge logical type (e.g. "Follow", "Member", "Like")
    const TYPE: &'static str;

    /// Logical type of the object the edge originates from. Wired by
    /// `#[ousia(from = SomeObject)]` on the derive; used by `init_schema` to
    /// add a per-partition `FOREIGN KEY (from) REFERENCES objects_<from_type>(id)
    /// ON DELETE CASCADE`.
    const FROM_TYPE: &'static str;

    /// Logical type of the object the edge points to. Wired by
    /// `#[ousia(to = SomeObject)]` on the derive; used identically to
    /// [`Self::FROM_TYPE`] for the `(to)` foreign key.
    const TO_TYPE: &'static str;

    /// Object type name helper
    fn type_name(&self) -> &'static str {
        Self::TYPE
    }

    fn meta(&self) -> &EdgeMeta;

    fn meta_mut(&mut self) -> &mut EdgeMeta;

    /// Indexable fields
    fn index_meta(&self) -> IndexMeta;
}

pub trait EdgeMetaTrait {
    fn from(&self) -> Uuid;
    fn to(&self) -> Uuid;
    fn created_at(&self) -> DateTime<Utc>;
    fn updated_at(&self) -> DateTime<Utc>;
}

impl<E> EdgeMetaTrait for E
where
    E: Edge,
{
    fn from(&self) -> uuid::Uuid {
        self.meta().from()
    }

    fn to(&self) -> uuid::Uuid {
        self.meta().to()
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.meta().created_at()
    }

    fn updated_at(&self) -> DateTime<Utc> {
        self.meta().updated_at()
    }
}
