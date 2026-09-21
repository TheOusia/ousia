// ledger/src/asset.rs
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::MoneyError;

/// Longest code whose Postgres partition name, `ledger_value_objects_<code>`,
/// fits the 63-byte identifier limit.
pub const MAX_ASSET_CODE_LEN: usize = 63 - "ledger_value_objects_".len();

/// Asset codes are 1-42 ASCII letters and case-insensitive (`usd` is `USD`).
/// Returns the canonical upper-case form.
pub fn normalize_asset_code(code: &str) -> Result<String, MoneyError> {
    let valid = !code.is_empty()
        && code.len() <= MAX_ASSET_CODE_LEN
        && code.bytes().all(|b| b.is_ascii_alphabetic());
    if !valid {
        return Err(MoneyError::InvalidAssetCode(format!(
            "{code:?}: use 1-{MAX_ASSET_CODE_LEN} letters A-Z (e.g. MGPOINT, not MG-POINT)"
        )));
    }
    Ok(code.to_ascii_uppercase())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub code: String,
    pub unit: u64,
    pub decimals: u8,
}

impl Asset {
    pub fn new(code: &str, unit: u64, decimals: u8) -> Self {
        Self {
            id: uuid::Uuid::now_v7(),
            code: code.to_ascii_uppercase(),
            unit,
            decimals,
        }
    }

    pub fn to_internal(&self, display_amount: f64) -> u64 {
        // `display_amount * 10^decimals` isn't always exactly representable in
        // f64 (e.g. 19.99 * 100 == 1998.9999999999998) — truncating with a bare
        // `as u64` would silently lose a cent. Round to the nearest integer first.
        (display_amount * 10_f64.powi(self.decimals as i32)).round() as u64
    }

    pub fn to_display(&self, internal_amount: u64) -> f64 {
        internal_amount as f64 / 10_f64.powi(self.decimals as i32)
    }
}
