use serde::{Deserialize, Serialize};

use crate::{
    object::Meta,
    query::{GeoPoint, IndexMeta},
};

/// Internal trait for engine operations
/// This trait is NOT part of the public API and should only be used
/// by the Ousia engine for persistence operations.
#[doc(hidden)]
pub trait ObjectInternal {
    /// Serialize the object's non-meta fields (including private fields)
    /// to MessagePack bytes, ready to land in the `BYTEA data` column.
    /// Bypasses the public Serialize view system which hides private fields.
    fn __serialize_internal(&self) -> Vec<u8>;
}

/// Provides unique fields for an object.
pub trait Unique {
    const HAS_UNIQUE_FIELDS: bool;
    fn derive_unique_hashes(&self) -> Vec<(String, &'static str)>;
}

///
/// Derive macro is expected to produce
/// const FIELDS: &'static TypeNameIndexes {field_name: crate::query::IndexField,...}
///
pub trait Object:
    ObjectInternal + Unique + Serialize + for<'de> Deserialize<'de> + Sized + Send + Sync + 'static
{
    /// Object type name
    const TYPE: &'static str;

    /// Object type name helper
    fn type_name(&self) -> &'static str {
        Self::TYPE
    }

    /// Object metadata (id, owner, created_at, updated_at)
    fn meta(&self) -> &Meta;

    /// Mutable object metadata (id, owner, created_at, updated_at)
    fn meta_mut(&mut self) -> &mut Meta;

    // Derived, non-meta indexes only
    fn index_meta(&self) -> IndexMeta;

    /// True iff this object declares at least one `IndexKind::Geo` index.
    const HAS_GEO_FIELDS: bool = false;

    /// Geo points sourced from this object's lat/lon fields, one per declared
    /// `geo(...)` index. Default is empty for types without geo indexes.
    fn geo_points(&self) -> Vec<GeoPoint> {
        Vec::new()
    }
}

pub trait ObjectMeta {
    fn id(&self) -> uuid::Uuid;
    fn owner(&self) -> uuid::Uuid;
    fn created_at(&self) -> chrono::DateTime<chrono::Utc>;
    fn updated_at(&self) -> chrono::DateTime<chrono::Utc>;
}

impl<T> ObjectMeta for T
where
    T: Object,
{
    fn id(&self) -> uuid::Uuid {
        self.meta().id()
    }

    fn owner(&self) -> uuid::Uuid {
        self.meta().owner()
    }

    fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.meta().created_at()
    }

    fn updated_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.meta().updated_at()
    }
}

pub trait ObjectType {
    fn type_name(&self) -> &'static str;
}

impl<T: Object> ObjectType for T
where
    T: Object,
{
    fn type_name(&self) -> &'static str {
        T::TYPE
    }
}

pub trait ObjectOwnership {
    fn is_system_owned(&self) -> bool;

    fn is_owned_by<O: Object>(&self, owner: &O) -> bool;

    fn set_owner(&mut self, owner: uuid::Uuid);
}

impl<T: Object> ObjectOwnership for T {
    fn is_system_owned(&self) -> bool {
        self.meta().owner() == super::SYSTEM_OWNER
    }

    fn is_owned_by<O: Object>(&self, object: &O) -> bool {
        self.meta().owner() == object.meta().id()
    }

    fn set_owner(&mut self, owner: uuid::Uuid) {
        self.meta_mut().owner = owner;
    }
}

pub enum Union<A: Object, B: Object> {
    First(A),
    Second(B),
}

impl<A: Object, B: Object> Union<A, B> {
    pub fn is_first(&self) -> bool {
        match self {
            Self::First(_) => true,
            Self::Second(_) => false,
        }
    }

    pub fn is_second(&self) -> bool {
        match self {
            Self::First(_) => false,
            Self::Second(_) => true,
        }
    }

    pub fn as_first(self) -> Option<A> {
        match self {
            Self::First(a) => Some(a),
            Self::Second(_) => None,
        }
    }

    pub fn as_second(self) -> Option<B> {
        match self {
            Self::First(_) => None,
            Self::Second(b) => Some(b),
        }
    }
}
