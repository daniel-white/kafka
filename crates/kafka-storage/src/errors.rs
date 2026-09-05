//! Storage-layer error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Index file is corrupt: {message}")]
    CorruptIndex { message: String },

    #[error("Index offset overflow: offset delta exceeds 32-bit range")]
    OffsetOverflow,

    #[error("Invalid relative offset: {0}")]
    InvalidRelativeOffset(i32),

    #[error("Offset {offset} not found in index; range is [{low}, {high}]")]
    OffsetNotFound {
        offset: i64,
        low: i64,
        high: i64,
    },

    #[error("Timestamp {timestamp} not found in time index")]
    TimestampNotFound { timestamp: i64 },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
