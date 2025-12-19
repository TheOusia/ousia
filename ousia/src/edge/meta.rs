use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::edge::EdgeMetaTrait;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EdgeMeta {
    pub from: Ulid,
    pub to: Ulid,
}

impl EdgeMeta {
    pub fn new(from: Ulid, to: Ulid) -> Self {
        Self { from, to }
    }
}

impl EdgeMetaTrait for EdgeMeta {
    fn from(&self) -> Ulid {
        self.from
    }

    fn to(&self) -> Ulid {
        self.to
    }
}
