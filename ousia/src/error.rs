use std::fmt::Display;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    Serialize(String),
    Deserialize(String),
    Storage(String),
    TypeMismatch,
    UniqueConstraintViolation(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => write!(f, "Not found"),
            Error::Serialize(err) => write!(f, "Serialization error: {}", err),
            Error::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            Error::Storage(err) => write!(f, "Storage error: {}", err),
            Error::TypeMismatch => write!(f, "Type mismatch"),
            Error::UniqueConstraintViolation(field) => {
                write!(f, "Unique constraint violation on field: {}", field)
            }
        }
    }
}

impl std::error::Error for Error {}
