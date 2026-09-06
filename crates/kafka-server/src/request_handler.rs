//! Re-export the handlers module for convenience.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala

pub use crate::handlers::{build_empty_response, build_response_frame, dispatch};
