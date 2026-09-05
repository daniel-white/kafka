//! Error types for the coordinator runtime.
//!
//! Mirrors Java's `Deserializer.UnknownRecordTypeException` and
//! `Deserializer.UnknownRecordVersionException`, plus general coordinator errors.
//!
//! MIGRATION_SOURCE: coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/Deserializer.java

use thiserror::Error;

/// Errors that can occur during coordinator record deserialization or processing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CoordinatorError {
    /// An unknown record type was encountered during deserialization.
    ///
    /// Mirrors `Deserializer.UnknownRecordTypeException`.
    #[error("Found an unknown record type {unknown_type}")]
    UnknownRecordType { unknown_type: i16 },

    /// An unknown record version was encountered.
    ///
    /// Mirrors `Deserializer.UnknownRecordVersionException`.
    #[error("Found an unknown record version {unknown_version} for record type {record_type}")]
    UnknownRecordVersion {
        unknown_version: i16,
        record_type: i16,
    },

    /// A coordinator operation failed.
    #[error("Coordinator error: {message}")]
    Coordinator { message: String },
}
