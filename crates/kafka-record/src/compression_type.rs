//! Compression type enum for record batches.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/CompressionType.java`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub const NONE: CompressionType = CompressionType::None;
    pub const GZIP: CompressionType = CompressionType::Gzip;
    pub const SNAPPY: CompressionType = CompressionType::Snappy;
    pub const LZ4: CompressionType = CompressionType::Lz4;
    pub const ZSTD: CompressionType = CompressionType::Zstd;

    pub const fn id(&self) -> i8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Gzip => 1,
            CompressionType::Snappy => 2,
            CompressionType::Lz4 => 3,
            CompressionType::Zstd => 4,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        }
    }

    pub const fn default_level(&self) -> i32 {
        match self {
            CompressionType::None => -1,
            CompressionType::Gzip => -1,
            CompressionType::Snappy => -1,
            CompressionType::Lz4 => 9,
            CompressionType::Zstd => 3,
        }
    }

    pub const fn max_level(&self) -> i32 {
        match self {
            CompressionType::None => -1,
            CompressionType::Gzip => 9,
            CompressionType::Snappy => -1,
            CompressionType::Lz4 => 17,
            CompressionType::Zstd => 22,
        }
    }

    pub const fn min_level(&self) -> i32 {
        match self {
            CompressionType::None => -1,
            CompressionType::Gzip => 1,
            CompressionType::Snappy => -1,
            CompressionType::Lz4 => 1,
            CompressionType::Zstd => -131072,
        }
    }

    pub fn for_id(id: i32) -> Result<Self, CompressionError> {
        match id {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Snappy),
            3 => Ok(CompressionType::Lz4),
            4 => Ok(CompressionType::Zstd),
            _ => Err(CompressionError::UnknownId(id)),
        }
    }

    pub fn for_name(name: &str) -> Result<Self, CompressionError> {
        match name {
            "none" => Ok(CompressionType::None),
            "gzip" => Ok(CompressionType::Gzip),
            "snappy" => Ok(CompressionType::Snappy),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            _ => Err(CompressionError::UnknownName(name.to_string())),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, CompressionType::None)
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("Unknown compression type id: {0}")]
    UnknownId(i32),

    #[error("Unknown compression name: {0}")]
    UnknownName(String),
}
