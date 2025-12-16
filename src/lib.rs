mod engine;
mod ledger;
mod object;

pub use crate::object::meta::*;
pub use crate::object::traits::*;

#[cfg(feature = "derive")]
pub use ousia_derive::*;

pub struct Ousia {}

#[cfg(test)]
mod test {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(OusiaObject, OusiaDefault, Serialize, Deserialize)]
    #[ousia(type_name = "User", index = "name:search")]
    struct User {
        #[serde(skip_serializing)]
        _meta: Meta,
        name: String,
    }

    #[test]
    fn test_object_ownership() {
        let user = User::default();
        assert!(!user.is_system_owned());
    }

    #[test]
    fn test_index_meta() {
        let mut user = User::default();
        user.name = "John Doe".to_string();

        assert_eq!(
            user.index_meta()
                .values
                .get("name")
                .map(|ik| ik.as_string().unwrap()),
            Some("John Doe")
        );
    }
}
