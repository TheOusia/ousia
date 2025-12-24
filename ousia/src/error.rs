use std::fmt::Display;

#[derive(Debug)]
pub enum Error {
    NotFound,
    Conflict,
    Serialize(String),
    Deserialize(String),
    Storage(String),
    TypeMismatch,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => write!(f, "Not found"),
            Error::Conflict => write!(f, "Conflict"),
            Error::Serialize(err) => write!(f, "Serialization error: {}", err),
            Error::Deserialize(err) => write!(f, "Deserialization error: {}", err),
            Error::Storage(err) => write!(f, "Storage error: {}", err),
            Error::TypeMismatch => write!(f, "Type mismatch"),
        }
    }
}

impl std::error::Error for Error {}
