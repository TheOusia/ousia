#[derive(Debug)]
pub enum AdapterError {
    NotFound,
    Conflict,
    Serialization(String),
    Storage(String),
}
