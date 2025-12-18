use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub trait EdgeMetaTrait {
    /// Edge owner (always = From.id)
    fn from(&self) -> Ulid;

    /// Edge target (points to To.id)
    fn to(&self) -> Ulid;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct EdgeMeta {
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
