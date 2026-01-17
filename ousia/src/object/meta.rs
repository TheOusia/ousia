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
