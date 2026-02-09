pub mod meta;
pub mod traits;

pub use meta::*;
pub use traits::*;

use uuid::Uuid;
/// SYSTEM_OWNER represents the root/system authority.
/// It must never be assigned to user-generated objects.
pub const SYSTEM_OWNER: Uuid = Uuid::from_bytes([
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
]);

pub fn system_owner() -> Uuid {
    SYSTEM_OWNER
}

pub fn derive_unique_hash(type_name: &str, field_name: &str, value: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(type_name.as_bytes());
    hasher.update(b"::");
    hasher.update(field_name.as_bytes());
    hasher.update(b"::");
    hasher.update(value.as_bytes());
    hasher.finalize().to_hex().to_string()
}
