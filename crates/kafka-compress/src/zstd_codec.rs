//! Zstandard compression using `zstd` crate (standard zstd format).
//!
//! Mirrors `org.apache.kafka.common.compress.ZstdCompression` and
//! `ZstdOutputStreamNoFinalizer`.

use crate::errors::CompressError;
use std::io::{Read, Write};

pub const ZSTD_DEFAULT_LEVEL: i32 = 3;
pub const ZSTD_MIN_LEVEL: i32 = -131072;
pub const ZSTD_MAX_LEVEL: i32 = 22;

pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, CompressError> {
    let level = normalize_level(level);
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), level)?;
    encoder.write_all(data)?;
    encoder.finish().map_err(CompressError::from)
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressError> {
    let mut decoder = zstd::stream::Decoder::new(data)?;
    let mut buf = Vec::new();
    decoder.read_to_end(&mut buf)?;
    Ok(buf)
}

fn normalize_level(level: i32) -> i32 {
    level.clamp(ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL)
}
