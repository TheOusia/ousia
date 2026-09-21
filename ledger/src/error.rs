// ledger/src/error.rs
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
    /// Asset code rejected: must be 1–42 chars of `[A-Za-z0-9_-]`, unique after case/`-` folding.
    InvalidAssetCode(String),
    Storage(String),
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
            Self::DuplicateIdempotencyKey(id) => write!(f, "Duplicate idempotency key: {}", id),
            Self::InvalidAssetCode(msg) => write!(f, "Invalid asset code: {}", msg),
            Self::Storage(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for MoneyError {}

impl MoneyError {
    pub fn is_duplicate_idempotent_key(&self) -> bool {
        match self {
            MoneyError::DuplicateIdempotencyKey(_) => true,
            _ => false,
        }
    }
}
