// iedb/src/agent/mod.rs
pub mod model;
pub mod buffer;
pub mod wal;
pub mod flush;
pub mod write;
pub mod query;

// Agent client 占位（后续 Task 实现）
#[allow(unused)]
use std::sync::Arc;

#[allow(unused)]
pub struct AgentClient {
    // Task 7 实现
}
