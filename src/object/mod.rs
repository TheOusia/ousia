pub mod meta;
pub mod traits;

use once_cell::sync::Lazy;

/// SYSTEM_OWNER represents the root/system authority.
/// It must never be assigned to user-generated objects.
pub static SYSTEM_OWNER: Lazy<ulid::Ulid> = Lazy::new(|| {
    match ulid::Ulid::from_string(
        &std::env::var("OUSIA_SYSTEM_ID").unwrap_or("00000000000000000000000000".to_string()),
    ) {
        Ok(id) => id,
        Err(err) => panic!("{:?}", err),
    }
});
