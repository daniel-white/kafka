//! Kafka compression codecs: gzip, snappy, lz4, zstd.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/compress/`.
//!
//! Uses enum dispatch (no `dyn Trait`) per project convention.

pub mod errors;
pub mod gzip;
pub mod snappy;
pub mod lz4;
pub mod zstd_codec;
pub mod compression;
