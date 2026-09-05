//! Error types for the kafka-record crate.
//!
//! Specific record-format errors stay in this crate; they can be converted
//! from the shared [`kafka_errors::KafkaError`] via `#[from]`.

use kafka_errors::KafkaError;
use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid record size: expected {expected} bytes, but only {actual} remaining")]
    InvalidRecordSize { expected: usize, actual: usize },

    #[error("Invalid number of record headers: {0}")]
    InvalidHeaderCount(i32),

    #[error("Invalid negative header key size {0}")]
    InvalidHeaderKeySize(i32),

    #[error("Record batch size {size} is smaller than the minimum allowed overhead {overhead}")]
    BatchTooSmall { size: i32, overhead: i32 },

    #[error("Invalid magic value {magic}, expected {expected}")]
    InvalidMagic { magic: i8, expected: i8 },

    #[error(transparent)]
    Kafka(#[from] KafkaError),
}
