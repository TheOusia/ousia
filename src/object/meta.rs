use serde::{Deserialize, Serialize};

use crate::object::SYSTEM_OWNER;

#[derive(Serialize, Deserialize)]
pub struct Meta {
    pub id: ulid::Ulid,

    #[serde(skip_serializing)]
    pub owner: ulid::Ulid,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            id: ulid::Ulid::new(),
            owner: *SYSTEM_OWNER,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

impl Meta {
    pub fn new_with_owner(owner: ulid::Ulid) -> Self {
        Self {
            id: ulid::Ulid::new(),
            owner,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }
}

impl Meta {
    pub fn id(&self) -> ulid::Ulid {
        self.id
    }

    pub fn owner(&self) -> ulid::Ulid {
        self.owner
    }

    pub fn created_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.updated_at
    }
}
