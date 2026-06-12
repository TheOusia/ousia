use std::fmt::Display;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Serialize(String),
    Deserialize(String),
    Storage(String),
    UniqueConstraintViolation(String),
    Unsupported(String),
    InvalidQuery(String),
    /// The composed schema hash stored in `ousia_meta` is from a
    /// different *major* schema version. Manual migration required.
    SchemaMigrationRequired(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => write!(f, "Not found"),
            Error::Serialize(err) => write!(f, "Serialization error: {}", err),
            Error::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            Error::Storage(err) => write!(f, "Storage error: {}", err),
            Error::UniqueConstraintViolation(field) => {
                write!(f, "Unique constraint violation on field: {}", field)
            }
            Error::Unsupported(msg) => write!(f, "Unsupported operation: {}", msg),
            Error::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            Error::SchemaMigrationRequired(msg) => {
                write!(f, "Schema migration required: {}", msg)
            }
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn is_unique_constraint_violation(&self) -> bool {
        match self {
            Error::UniqueConstraintViolation(_) => true,
            _ => false,
        }
    }
}
