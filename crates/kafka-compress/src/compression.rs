//! Compression dispatch using enum (no `dyn Trait`).
//!
//! Mirrors `org.apache.kafka.common.Compression` interface with
//! `GzipCompression`, `SnappyCompression`, `Lz4Compression`, `ZstdCompression`,
//! `NoCompression` implementations.

use crate::errors::CompressError;
use kafka_record::CompressionType;

pub struct Compression;

impl Compression {
    /// Compress data using the given compression type.
    ///
    /// Mirrors `Compression.of(CompressionType).wrapForOutput(...)`.
    pub fn compress(
        data: &[u8],
        compression_type: CompressionType,
        level: i32,
    ) -> Result<Vec<u8>, CompressError> {
        match compression_type {
            CompressionType::NONE => Ok(data.to_vec()),
            CompressionType::GZIP => crate::gzip::compress(data, level),
            CompressionType::SNAPPY => crate::snappy::compress(data),
            CompressionType::LZ4 => crate::lz4::compress(data, level),
            CompressionType::ZSTD => crate::zstd_codec::compress(data, level),
        }
    }

    /// Decompress data using the given compression type.
    /// For LZ4, the uncompressed size must be provided (Kafka's LZ4 format
    /// stores it in the block header, which requires separate parsing).
    pub fn decompress(
        data: &[u8],
        compression_type: CompressionType,
        _uncompressed_size_hint: Option<usize>,
    ) -> Result<Vec<u8>, CompressError> {
        match compression_type {
            CompressionType::NONE => Ok(data.to_vec()),
            CompressionType::GZIP => crate::gzip::decompress(data),
            CompressionType::SNAPPY => crate::snappy::decompress(data),
            CompressionType::LZ4 => crate::lz4::decompress(data),
            CompressionType::ZSTD => crate::zstd_codec::decompress(data),
        }
    }

    /// Check if a compression type supports levels.
    pub fn supports_levels(compression_type: CompressionType) -> bool {
        matches!(compression_type, CompressionType::GZIP | CompressionType::LZ4 | CompressionType::ZSTD)
    }

    /// Get the default level for a compression type.
    pub fn default_level(compression_type: CompressionType) -> i32 {
        match compression_type {
            CompressionType::NONE => 0,
            CompressionType::GZIP => crate::gzip::GZIP_DEFAULT_LEVEL,
            CompressionType::SNAPPY => 0,
            CompressionType::LZ4 => crate::lz4::LZ4_DEFAULT_LEVEL,
            CompressionType::ZSTD => crate::zstd_codec::ZSTD_DEFAULT_LEVEL,
        }
    }
}
