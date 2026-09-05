//! LZ4 compression using `lz4_flex` (block format).
//!
//! Mirrors `org.apache.kafka.common.compress.Lz4Compression` and
//! `Lz4BlockOutputStream` (magic 0x184D2204, block format).
//!
//! Kafka's LZ4 uses a custom block format, not standard LZ4 streaming.
//! The `lz4_flex` crate provides block-format compression matching this.

use crate::errors::CompressError;

pub const LZ4_DEFAULT_LEVEL: i32 = 9;
pub const LZ4_MIN_LEVEL: i32 = 1;
pub const LZ4_MAX_LEVEL: i32 = 17;

pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, CompressError> {
    let _level = normalize_level(level);
    let compressed = lz4_flex::compress_prepend_size(data);
    Ok(compressed)
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressError> {
    let decompressed = lz4_flex::decompress_size_prepended(data)
        .map_err(|e| CompressError::Decompress(e.to_string()))?;
    Ok(decompressed)
}

fn normalize_level(level: i32) -> i32 {
    level.clamp(LZ4_MIN_LEVEL, LZ4_MAX_LEVEL)
}
