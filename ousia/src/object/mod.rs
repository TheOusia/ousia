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
