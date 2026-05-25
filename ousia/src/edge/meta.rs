use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::edge::EdgeMetaTrait;

fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeMeta {
    pub from: Uuid,
    pub to: Uuid,
    #[serde(default = "utc_now")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "utc_now")]
    pub updated_at: DateTime<Utc>,
}

impl EdgeMeta {
    pub fn new(from: Uuid, to: Uuid) -> Self {
        let now = Utc::now();
        Self {
            from,
            to,
            created_at: now,
            updated_at: now,
        }
    }
}

impl EdgeMetaTrait for EdgeMeta {
    fn from(&self) -> Uuid {
        self.from
    }

    fn to(&self) -> Uuid {
        self.to
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}
