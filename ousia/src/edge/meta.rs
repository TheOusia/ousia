use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::edge::EdgeMetaTrait;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeMeta {
    pub from: Uuid,
    pub to: Uuid,
}

impl EdgeMeta {
    pub fn new(from: Uuid, to: Uuid) -> Self {
        Self { from, to }
    }
}

impl EdgeMetaTrait for EdgeMeta {
    fn from(&self) -> Uuid {
        self.from
    }

    fn to(&self) -> Uuid {
        self.to
    }
}
