use std::fmt::Display;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Serialize(String),
    Deserialize(String),
    Storage(String),
    UniqueConstraintViolation(String),
    /// The stored schema version's major component differs from the current library version.
    /// Manual reconciliation is required before the engine can be used.
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

    pub fn is_schema_migration_required(&self) -> bool {
        matches!(self, Error::SchemaMigrationRequired(_))
    }
}
