//! BaseRecords marker type.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/BaseRecords.java`.

/// Base type for all log record container types (MemoryRecords, FileRecords, etc.).
///
/// In Java this is an abstract class with no fields; in Rust it is a unit struct
/// used as a type marker / base for extension.
pub struct BaseRecords;
