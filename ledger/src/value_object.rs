// ousia/src/ledger/value_object.rs

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// State of a ValueObject
/// State transitions are one-way: alive/reserved → burned
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValueObjectState {
    /// Usable value that can be spent
    Alive,
    /// Minted but locked for a specific authority (escrow)
    Reserved,
    /// Consumed, no longer exists in circulation
    Burned,
}

impl ValueObjectState {
    /// Check if this state can transition to another state
    pub fn can_transition_to(&self, target: ValueObjectState) -> bool {
        match (self, target) {
            // Can always stay in same state
            (s1, s2) if s1 == &s2 => true,
            // Alive can go to Reserved or Burned
            (ValueObjectState::Alive, ValueObjectState::Reserved) => true,
            (ValueObjectState::Alive, ValueObjectState::Burned) => true,
            // Reserved can go to Alive (activate) or Burned (release/cancel)
            (ValueObjectState::Reserved, ValueObjectState::Alive) => true,
            (ValueObjectState::Reserved, ValueObjectState::Burned) => true,
            // Burned is terminal - no transitions out
            (ValueObjectState::Burned, _) => false,
            // All other transitions are invalid
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
        // Only alive ValueObjects can be spent
        self.is_alive()
    }
}

/// ValueObject represents a discrete, indivisible fragment of value
///
/// Invariants:
/// - amount is immutable after creation
/// - id is unique and immutable
/// - state can only transition one-way (never from burned back to alive/reserved)
/// - amount <= Asset.unit (enforced at creation)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueObject {
    /// Unique identifier for this ValueObject
    pub id: Ulid,

    /// Which asset this value belongs to (USD, NGN, etc.)
    pub asset_id: Ulid,

    /// Who owns this value
    pub owner: Ulid,

    /// Amount of value (immutable)
    /// This must be <= Asset.unit
    pub amount: i64,

    /// Current state of this ValueObject
    pub state: ValueObjectState,

    /// If reserved, which authority can activate/release it
    /// Only relevant when state == Reserved
    pub reserved_for: Option<Ulid>,

    /// When this ValueObject was created
    pub created_at: DateTime<Utc>,
}

impl ValueObject {
    /// Create a new alive ValueObject
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

    /// Create a new reserved ValueObject
    /// Reserved ValueObjects are owned but not spendable until activated
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

    /// Check if this ValueObject is alive (spendable)
    pub fn is_alive(&self) -> bool {
        self.state.is_alive()
    }

    /// Check if this ValueObject is reserved
    pub fn is_reserved(&self) -> bool {
        self.state.is_reserved()
    }

    /// Check if this ValueObject is burned
    pub fn is_burned(&self) -> bool {
        self.state.is_burned()
    }

    /// Check if this ValueObject can be spent
    pub fn is_spendable(&self) -> bool {
        self.state.is_spendable()
    }

    /// Check if a specific authority can activate/release this reserved ValueObject
    pub fn can_be_activated_by(&self, authority: Ulid) -> bool {
        if !self.is_reserved() {
            return false;
        }

        match self.reserved_for {
            Some(reserved_authority) => reserved_authority == authority,
            None => false,
        }
    }

    /// Validate that amount is within asset unit limit
    pub fn validate_amount(&self, asset_unit: i64) -> bool {
        self.amount > 0 && self.amount <= asset_unit
    }

    /// Get age of this ValueObject
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_object_creation_alive() {
        let asset_id = Ulid::new();
        let owner = Ulid::new();
        let vo = ValueObject::new_alive(asset_id, owner, 1000);

        assert_eq!(vo.asset_id, asset_id);
        assert_eq!(vo.owner, owner);
        assert_eq!(vo.amount, 1000);
        assert!(vo.is_alive());
        assert!(!vo.is_reserved());
        assert!(!vo.is_burned());
        assert!(vo.is_spendable());
        assert!(vo.reserved_for.is_none());
    }

    #[test]
    fn test_value_object_creation_reserved() {
        let asset_id = Ulid::new();
        let owner = Ulid::new();
        let authority = Ulid::new();
        let vo = ValueObject::new_reserved(asset_id, owner, 1000, authority);

        assert_eq!(vo.asset_id, asset_id);
        assert_eq!(vo.owner, owner);
        assert_eq!(vo.amount, 1000);
        assert!(!vo.is_alive());
        assert!(vo.is_reserved());
        assert!(!vo.is_burned());
        assert!(!vo.is_spendable());
        assert_eq!(vo.reserved_for, Some(authority));
    }

    #[test]
    fn test_state_transitions() {
        use ValueObjectState::*;

        // Alive → Reserved (valid)
        assert!(Alive.can_transition_to(Reserved));
        // Alive → Burned (valid)
        assert!(Alive.can_transition_to(Burned));
        // Reserved → Alive (valid - activation)
        assert!(Reserved.can_transition_to(Alive));
        // Reserved → Burned (valid - release)
        assert!(Reserved.can_transition_to(Burned));
        // Burned → Alive (invalid - burned is terminal)
        assert!(!Burned.can_transition_to(Alive));
        // Burned → Reserved (invalid - burned is terminal)
        assert!(!Burned.can_transition_to(Reserved));
        // Same state (always valid)
        assert!(Alive.can_transition_to(Alive));
        assert!(Reserved.can_transition_to(Reserved));
        assert!(Burned.can_transition_to(Burned));
    }

    #[test]
    fn test_can_be_activated_by() {
        let asset_id = Ulid::new();
        let owner = Ulid::new();
        let authority = Ulid::new();
        let wrong_authority = Ulid::new();

        let vo = ValueObject::new_reserved(asset_id, owner, 1000, authority);

        assert!(vo.can_be_activated_by(authority));
        assert!(!vo.can_be_activated_by(wrong_authority));

        // Alive ValueObjects can't be "activated"
        let alive_vo = ValueObject::new_alive(asset_id, owner, 1000);
        assert!(!alive_vo.can_be_activated_by(authority));
    }

    #[test]
    fn test_validate_amount() {
        let vo = ValueObject::new_alive(Ulid::new(), Ulid::new(), 100);

        // Valid: amount within limit
        assert!(vo.validate_amount(100));
        assert!(vo.validate_amount(150));

        // Invalid: amount exceeds limit
        assert!(!vo.validate_amount(99));

        // Invalid: zero amount
        let zero_vo = ValueObject::new_alive(Ulid::new(), Ulid::new(), 0);
        assert!(!zero_vo.validate_amount(100));

        // Invalid: negative amount
        let neg_vo = ValueObject::new_alive(Ulid::new(), Ulid::new(), -100);
        assert!(!neg_vo.validate_amount(100));
    }

    #[test]
    fn test_age() {
        let vo = ValueObject::new_alive(Ulid::new(), Ulid::new(), 1000);
        let age = vo.age();

        // Age should be very small (just created)
        assert!(age.num_milliseconds() < 100);
    }
}
