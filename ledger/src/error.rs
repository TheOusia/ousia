// ousia/src/ledger/error.rs
use std::fmt;

#[derive(Debug)]
pub enum MoneyError {
    InsufficientFunds,
    AssetNotFound(String),
    InvalidAmount,
    UnconsumedSlice,
    ReservationNotFound,
    InvalidAuthority,
    TransactionNotFound,
    DuplicateIdempotencyKey(uuid::Uuid),
    Storage(String),
    Conflict(String),
}

impl fmt::Display for MoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientFunds => write!(f, "Insufficient funds"),
            Self::AssetNotFound(code) => write!(f, "Asset not found: {}", code),
            Self::InvalidAmount => write!(f, "Invalid amount"),
            Self::UnconsumedSlice => write!(f, "Not all slices were consumed"),
            Self::ReservationNotFound => write!(f, "Reservation not found"),
            Self::InvalidAuthority => write!(f, "Invalid authority"),
            Self::TransactionNotFound => write!(f, "Transaction not found"),
            Self::DuplicateIdempotencyKey(id) => {
                write!(f, "Duplicate idempotency key: {}", id)
            }
            Self::Storage(msg) => write!(f, "Storage error: {}", msg),
            Self::Conflict(msg) => write!(f, "Conflict: {}", msg),
        }
    }
}

impl std::error::Error for MoneyError {}
