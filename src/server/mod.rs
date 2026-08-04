pub mod query_engine;
pub mod sql_api;
pub mod ingest_api;
pub mod agent_api;
pub mod agent_store;
pub mod metadata_api;
pub mod metadata_store;
pub mod table_provider;
pub mod compaction;
pub mod db;

#[cfg(test)]
mod test_util;

// Re-exports for the server wiring layer (main.rs / later tasks).
pub use agent_api::AgentApiHandler;
pub use ingest_api::IngestApiHandler;
pub use metadata_api::MetadataApiHandler;
