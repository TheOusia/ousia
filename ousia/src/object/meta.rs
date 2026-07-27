use serde::{Deserialize, Serialize};

use crate::object::SYSTEM_OWNER;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Meta {
    pub id: uuid::Uuid,
    pub owner: uuid::Uuid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            owner: SYSTEM_OWNER,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

impl Meta {
    pub fn new_with_owner(owner: uuid::Uuid) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            owner,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    /// Cheap, syscall-free placeholder used only by derive-macro-generated
    /// `Deserialize` impls while decoding a stored object. `ObjectRecord::to_object`
    /// unconditionally overwrites every field immediately after a successful
    /// decode, so these values are never observed — unlike `default()`/
    /// `new_with_owner()`, this skips `Uuid::now_v7()` (clock read + CSPRNG)
    /// and `Utc::now()` (clock read), which cost real, measured time on
    /// every single row decoded and would otherwise be pure waste.
    ///
    /// Not for general use — construct real objects with `default()` or
    /// `new_with_owner()`.
    #[doc(hidden)]
    pub fn __deserialize_placeholder() -> Self {
        Self {
            id: uuid::Uuid::nil(),
            owner: uuid::Uuid::nil(),
            created_at: deserialize_placeholder_time(),
            updated_at: deserialize_placeholder_time(),
        }
    }
}

#[doc(hidden)]
pub fn deserialize_placeholder_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(0, 0).expect("epoch is always a valid timestamp")
}

impl Meta {
    pub fn id(&self) -> uuid::Uuid {
        self.id
    }

    pub fn owner(&self) -> uuid::Uuid {
        self.owner
    }

    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.updated_at
    }
}
