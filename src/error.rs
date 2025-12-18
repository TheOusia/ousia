#[derive(Debug)]
pub enum Error {
    NotFound,
    Conflict,
    Serialize(String),
    Deserialize(String),
    Storage(String),
    TypeMismatch,
}
