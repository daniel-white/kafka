//! Kafka record module: on-disk record batch format (magic 2+).
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/`.
//!
//! MIGRATION_SOURCE: (new) — workspace crate root

pub mod base_records;
pub mod compression_type;
pub mod errors;
pub mod header;
pub mod memory_records;
pub mod record;
pub mod record_batch;
pub mod timestamp_type;

pub use record_batch::DefaultRecordBatch;
pub use record_batch::RecordBatchHeader;
pub use compression_type::{CompressionError, CompressionType};
