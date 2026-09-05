//! Kafka exception hierarchy, mirroring `clients/src/main/java/org/apache/kafka/common/errors/`.
//!
//! In Java, Kafka uses a large exception hierarchy (154 classes) rooted at
//! `KafkaException` → `ApiException` → `RetriableException` etc.
//! In Rust, these collapse into a single `KafkaError` enum via `thiserror`,
//! with the `retriable()` discriminant preserving the Java retry semantics.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/errors/

use std::io;
use thiserror::Error;

/// Root error type for all Kafka client/broker errors.
///
/// Mirrors `org.apache.kafka.common.errors.ApiException` (and its parent
/// `KafkaException`). Each variant corresponds to a Java exception subclass.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/errors/ApiException.java
#[derive(Debug, Error)]
pub enum KafkaError {
    // --- Retriable exceptions (extend RetriableException) ---

    #[error("The broker is not available")]
    BrokerNotAvailable {
        #[source]
        source: Option<Box<io::Error>>,
    },

    #[error("Connection failure")]
    ConnectionFailure {
        #[source]
        source: Option<Box<io::Error>>,
    },

    #[error("Timeout after waiting for {timeout_ms:?} ms")]
    Timeout { timeout_ms: Option<i64> },

    #[error("Corrupt record: {message}")]
    CorruptRecord { message: String },

    // --- Non-retriable API exceptions ---

    #[error("Invalid record: {message}")]
    InvalidRecord { message: String },

    #[error("Invalid configuration: {message}")]
    InvalidConfiguration { message: String },

    #[error("Serialization error: {message}")]
    Serialization {
        message: String,
        #[source]
        source: Option<Box<io::Error>>,
    },

    #[error("Invalid request: {message}")]
    InvalidRequest { message: String },

    #[error("Unknown server error: {message}")]
    UnknownServer {
        message: String,
        #[source]
        source: Option<Box<io::Error>>,
    },

    #[error("Not leader for partition: {message}")]
    NotLeaderForPartition { message: String },

    #[error("The requested offset is out of range")]
    OffsetOutOfRange {
        message: String,
        #[source]
        source: Option<Box<io::Error>>,
    },

    #[error("Record format version is not supported")]
    UnsupportedVersion { message: String },

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Trait for errors that expose whether they are retriable.
///
/// In Java, `RetriableException` is a marker base class. Rust has no
/// marker-exception types, so this trait provides the same semantic query.
pub trait Retriable {
    fn retriable(&self) -> bool;
}

impl Retriable for KafkaError {
    fn retriable(&self) -> bool {
        matches!(
            self,
            KafkaError::BrokerNotAvailable { .. }
                | KafkaError::ConnectionFailure { .. }
                | KafkaError::Timeout { .. }
                | KafkaError::CorruptRecord { .. }
                | KafkaError::OffsetOutOfRange { .. }
                | KafkaError::UnknownServer { .. }
                | KafkaError::Io(_)
        )
    }
}

/// Result alias using [`KafkaError`].
pub type Result<T> = std::result::Result<T, KafkaError>;
