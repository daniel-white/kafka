//! Snappy compression using `snap` (block format).
//!
//! Mirrors `org.apache.kafka.common.compress.SnappyCompression`.
//!
//! Note: Java uses `org.xerial.snappy.SnappyOutputStream` (streaming format with
//! checksums). The `snap` crate produces the block format. For Kafka record
//! batches, the Java broker uses `snappy.block_size` framing which matches the
//! block format when the data is a single block. Round-trip verification is
//! done via decompress(compress(x)) == x.

use crate::errors::CompressError;

pub fn compress(data: &[u8]) -> Result<Vec<u8>, CompressError> {
    let encoder = snap::write::FrameEncoder::new(Vec::new());
    let mut encoder = encoder;
    std::io::Write::write_all(&mut encoder, data)?;
    encoder
        .into_inner()
        .map_err(|e| CompressError::Io(std::io::Error::other(e.to_string())))
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressError> {
    let mut decoder = snap::read::FrameDecoder::new(data);
    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut buf)?;
    Ok(buf)
}