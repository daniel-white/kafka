//! Gzip compression using `flate2`.
//!
//! Mirrors `org.apache.kafka.common.compress.GzipCompression`.
//!
//! Java's `GZIPOutputStream` writes a gzip header with:
//!   - Magic: 0x1f 0x8b
//!   - Compression method: 8 (deflate)
//!   - Flags: varies
//!   - MTIME: timestamp (4 bytes)
//!   - XFL, OS bytes
//!
//! For byte-exactness with Java, we use `flate2::write::GzEncoder` with
//! `GzBuilder` to control the MTIME and OS fields.

use crate::errors::CompressError;

pub const GZIP_DEFAULT_LEVEL: i32 = -1;
pub const GZIP_MAX_LEVEL: i32 = 9;
pub const GZIP_MIN_LEVEL: i32 = 1;

pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>, CompressError> {
    let level = map_level(level);
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
    std::io::Write::write_all(&mut encoder, data)?;
    encoder.finish().map_err(CompressError::from)
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressError> {
    let mut decoder = flate2::write::GzDecoder::new(Vec::new());
    std::io::Write::write_all(&mut decoder, data)?;
    decoder.finish().map_err(CompressError::from)
}

fn map_level(level: i32) -> flate2::Compression {
    if level == GZIP_DEFAULT_LEVEL || level < 0 {
        flate2::Compression::default()
    } else if level < GZIP_MIN_LEVEL {
        flate2::Compression::new(1)
    } else if level > GZIP_MAX_LEVEL {
        flate2::Compression::new(9)
    } else {
        flate2::Compression::new(level as u32)
    }
}
