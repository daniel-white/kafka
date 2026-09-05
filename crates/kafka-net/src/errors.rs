//! Network-layer error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("Invalid receive: size = {size}, max = {max}")]
    InvalidReceive { size: i32, max: i32 },

    #[error("EOF while reading from channel")]
    Eof,

    #[error("Invalid header version: {0}")]
    InvalidHeaderVersion(i16),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
