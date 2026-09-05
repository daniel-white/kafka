//! Compression error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompressError {
    #[error("Compression error: {0}")]
    Compress(String),

    #[error("Decompression error: {0}")]
    Decompress(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
