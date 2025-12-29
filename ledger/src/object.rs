// ousia/src/ledger/value_object.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueObjectState {
    Alive,
    Reserved,
    Burned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueObject {
    pub id: Ulid,
    pub asset_id: Ulid,
    pub owner: Ulid,
    pub amount: i64,
    pub state: ValueObjectState,
    pub reserved_for: Option<Ulid>, // Authority that can activate/release reserved funds
    pub created_at: DateTime<Utc>,
}

impl ValueObject {
    pub fn new_alive(asset_id: Ulid, owner: Ulid, amount: i64) -> Self {
        Self {
            id: Ulid::new(),
            asset_id,
            owner,
            amount,
            state: ValueObjectState::Alive,
            reserved_for: None,
            created_at: Utc::now(),
        }
    }

    pub fn new_reserved(asset_id: Ulid, owner: Ulid, amount: i64, reserved_for: Ulid) -> Self {
        Self {
            id: Ulid::new(),
            asset_id,
            owner,
            amount,
            state: ValueObjectState::Reserved,
            reserved_for: Some(reserved_for),
            created_at: Utc::now(),
        }
    }

    pub fn is_alive(&self) -> bool {
        matches!(self.state, ValueObjectState::Alive)
    }

    pub fn is_reserved(&self) -> bool {
        matches!(self.state, ValueObjectState::Reserved)
    }

    pub fn is_burned(&self) -> bool {
        matches!(self.state, ValueObjectState::Burned)
    }
}
