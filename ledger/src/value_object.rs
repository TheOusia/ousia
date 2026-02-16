// ledger/src/value_object.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueObjectState {
    Alive,
    Reserved,
    Burned,
}

impl ValueObjectState {
    pub fn can_transition_to(&self, target: ValueObjectState) -> bool {
        match (self, target) {
            (s1, s2) if s1 == &s2 => true,
            (ValueObjectState::Alive, ValueObjectState::Reserved) => true,
            (ValueObjectState::Alive, ValueObjectState::Burned) => true,
            (ValueObjectState::Reserved, ValueObjectState::Alive) => true,
            (ValueObjectState::Reserved, ValueObjectState::Burned) => true,
            (ValueObjectState::Burned, _) => false,
            _ => false,
        }
    }

    pub fn is_alive(&self) -> bool {
        matches!(self, ValueObjectState::Alive)
    }

    pub fn is_reserved(&self) -> bool {
        matches!(self, ValueObjectState::Reserved)
    }

    pub fn is_burned(&self) -> bool {
        matches!(self, ValueObjectState::Burned)
    }

    pub fn is_spendable(&self) -> bool {
        self.is_alive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueObject {
    pub id: Uuid,
    pub asset: Uuid,
    pub owner: Uuid,
    pub amount: i64,
    pub state: ValueObjectState,
    pub reserved_for: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

impl ValueObject {
    pub fn new_alive(asset_id: Uuid, owner: Uuid, amount: i64) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            asset: asset_id,
            owner,
            amount,
            state: ValueObjectState::Alive,
            reserved_for: None,
            created_at: Utc::now(),
        }
    }

    pub fn new_reserved(asset_id: Uuid, owner: Uuid, amount: i64, reserved_for: Uuid) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            asset: asset_id,
            owner,
            amount,
            state: ValueObjectState::Reserved,
            reserved_for: Some(reserved_for),
            created_at: Utc::now(),
        }
    }
}
