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

    /// Cheap, syscall-free placeholder used only by derive-macro-generated
    /// `Deserialize` impls while decoding a stored edge. `EdgeRecord::to_edge`
    /// unconditionally overwrites every field immediately after a successful
    /// decode, so these values are never observed — unlike `new()`, this
    /// skips two `Uuid::now_v7()` calls (clock read + CSPRNG each) and
    /// `Utc::now()` (clock read), which cost real, measured time on every
    /// single row decoded and would otherwise be pure waste.
    ///
    /// Not for general use — construct real edges with `new()`.
    #[doc(hidden)]
    pub fn __deserialize_placeholder() -> Self {
        Self {
            from: Uuid::nil(),
            to: Uuid::nil(),
            created_at: crate::object::meta::deserialize_placeholder_time(),
            updated_at: crate::object::meta::deserialize_placeholder_time(),
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
