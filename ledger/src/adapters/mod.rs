// ledger/src/adapters/mod.rs
pub mod memory;
pub mod postgres;
pub use memory::MemoryAdapter;
