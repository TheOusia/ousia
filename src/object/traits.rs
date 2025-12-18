use serde::{Deserialize, Serialize};

use crate::{object::meta::Meta, query::IndexMeta};

pub trait Object: Serialize + for<'de> Deserialize<'de> + Sized + Send + Sync + 'static {
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
}

pub trait ObjectMeta {
    fn id(&self) -> ulid::Ulid;
    fn owner(&self) -> ulid::Ulid;
    fn created_at(&self) -> chrono::DateTime<chrono::Utc>;
    fn updated_at(&self) -> chrono::DateTime<chrono::Utc>;
}

impl<T> ObjectMeta for T
where
    T: Object,
{
    fn id(&self) -> ulid::Ulid {
        self.meta().id()
    }

    fn owner(&self) -> ulid::Ulid {
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
}

impl<T: Object> ObjectOwnership for T {
    fn is_system_owned(&self) -> bool {
        self.meta().owner() == *super::SYSTEM_OWNER
    }

    fn is_owned_by<O: Object>(&self, object: &O) -> bool {
        self.meta().owner() == object.meta().id()
    }
}
