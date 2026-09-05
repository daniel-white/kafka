//! Error types for the metadata layer.
//!
//! Mirrors `MetadataParseException` and related exceptions.
//!
//! MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/MetadataException.java

use std::io;
use thiserror::Error;

/// Errors that can occur during metadata record serialization/deserialization.
#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("Unknown metadata record type for API key {0}")]
    UnknownRecordType(i16),

    #[error("Error while reading {0}: {1}")]
    ReadError(String, String),

    #[error("Invalid frame version: expected 1, got {0} (only version 1 is supported)")]
    InvalidFrameVersion(u32),

    #[error("API key {0} is not a valid metadata record")]
    InvalidApiKey(i16),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}
