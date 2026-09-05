//! ApiError: wraps an Errors enum and an optional message for API responses.
//!
//! Mirrors Java's `ApiError` class.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java

use getset::Getters;
use kafka_protocol::errors::Errors;

/// An error with an optional message, wrapping a protocol `Errors` enum.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
#[derive(Debug, Clone, PartialEq, Eq, Getters)]
pub struct ApiError {
    #[get = "pub"]
    error: Errors,
    #[get = "pub"]
    message: Option<String>,
}

impl ApiError {
    /// Sentinel: no error.
    ///
    /// Mirrors Java's `ApiError.NONE`.
    pub const NONE: ApiError = ApiError {
        error: Errors::None,
        message: None,
    };

    /// Create an ApiError from an Errors variant (message defaults to the error's message).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn from_error(error: Errors) -> Self {
        let message = if error.message().is_empty() {
            None
        } else {
            Some(error.message().to_string())
        };
        ApiError { error, message }
    }

    /// Create an ApiError from an Errors variant and an explicit message.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn new(error: Errors, message: Option<String>) -> Self {
        ApiError { error, message }
    }

    /// Create an ApiError from a short error code and optional message.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn from_code(code: i16, message: Option<String>) -> Self {
        ApiError {
            error: Errors::for_code(code),
            message,
        }
    }

    /// Check if this error is a specific Errors variant.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn is(&self, error: Errors) -> bool {
        self.error == error
    }

    /// Returns true if this is not `Errors::None`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn is_failure(&self) -> bool {
        !self.is_success()
    }

    /// Returns true if this is `Errors::None`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn is_success(&self) -> bool {
        self.is(Errors::None)
    }

    /// Return the message, or the error's default message if not set.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/ApiError.java
    pub fn message_with_fallback(&self) -> String {
        self.message
            .clone()
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| self.error.message().to_string())
    }
}

impl Default for ApiError {
    fn default() -> Self {
        ApiError::NONE
    }
}
