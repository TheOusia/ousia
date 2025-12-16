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
    use crate::engine::adapters::Field;

    use super::*;

    #[derive(OusiaObject, OusiaDefault, Clone)]
    #[ousia(type_name = "User", index = "name:search")]
    struct User {
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
                .meta()
                .get("name")
                .map(|ik| ik.as_string().unwrap()),
            Some("John Doe")
        );
    }

    #[test]
    fn test_query() {
        let mut user = User::default();
        user.name = "John Doe".to_string();

        assert_eq!(User::INDEXES.name, Field { name: "name" });
    }
}
