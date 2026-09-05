//! 128-bit UUID mirroring Java's `org.apache.kafka.common.Uuid`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java

use getset::CopyGetters;
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid as InnerUuid;

const BASE64_URL_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// A 128-bit UUID, stored as most-significant and least-significant 64-bit halves.
///
/// Fields are private; access via generated `getset` accessors, mirroring Java's
/// private-field + getter pattern.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
#[derive(CopyGetters, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Uuid {
    #[get_copy = "pub"]
    most_significant_bits: i64,
    #[get_copy = "pub"]
    least_significant_bits: i64,
}

impl Uuid {
    /// A reserved UUID. Will never be returned by `random_uuid`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub const ONE_UUID: Uuid = Uuid {
        most_significant_bits: 0,
        least_significant_bits: 1,
    };

    /// A UUID for the metadata topic in KRaft mode.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub const METADATA_TOPIC_ID: Uuid = Uuid::ONE_UUID;

    /// A UUID that represents a null or empty UUID.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub const ZERO_UUID: Uuid = Uuid {
        most_significant_bits: 0,
        least_significant_bits: 0,
    };

    /// Constructs a 128-bit UUID from most and least significant bits.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub const fn new(most_sig_bits: i64, least_sig_bits: i64) -> Self {
        Uuid {
            most_significant_bits: most_sig_bits,
            least_significant_bits: least_sig_bits,
        }
    }

    /// Returns the most significant bits of the UUID's 128-bit value.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub fn get_most_significant_bits(&self) -> i64 {
        self.most_significant_bits
    }

    /// Returns the least significant bits of the UUID's 128-bit value.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub fn get_least_significant_bits(&self) -> i64 {
        self.least_significant_bits
    }

    /// Creates a UUID from a base64 URL string encoding.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub fn from_string(s: &str) -> Result<Self, UuidParseError> {
        if s.len() > 24 {
            return Err(UuidParseError::TooLong(s[0..24].to_string()));
        }
        let decoded =
            decode_base64_url(s).ok_or_else(|| UuidParseError::InvalidBase64(s.to_string()))?;
        if decoded.len() != 16 {
            return Err(UuidParseError::WrongLength(decoded.len()));
        }
        let most = i64::from_be_bytes(decoded[0..8].try_into().unwrap());
        let least = i64::from_be_bytes(decoded[8..16].try_into().unwrap());
        Ok(Uuid::new(most, least))
    }

    /// Generate a type 4 (pseudo-random) UUID.
    ///
    /// Uses the `uuid` crate's `new_v4()` for CSPRNG-based generation,
    /// matching Java's `java.util.UUID.randomUUID()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    pub fn random_uuid() -> Self {
        let inner = InnerUuid::new_v4();
        let (most, least) = inner.as_u64_pair();
        Uuid::new(most as i64, least as i64)
    }
}

impl fmt::Display for Uuid {
    /// Returns the base64 URL string encoding (no padding), matching Java's `toString()`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/Uuid.java
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.most_significant_bits.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.least_significant_bits.to_be_bytes());
        f.write_str(&encode_base64_url(&bytes))
    }
}

impl PartialOrd for Uuid {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Uuid {
    fn cmp(&self, other: &Self) -> Ordering {
        self.most_significant_bits
            .cmp(&other.most_significant_bits)
            .then(self.least_significant_bits.cmp(&other.least_significant_bits))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum UuidParseError {
    #[error("Input string with prefix `{0}` is too long")]
    TooLong(String),
    #[error("Input string `{0}` is not valid base64 URL")]
    InvalidBase64(String),
    #[error("Decoded to {0} bytes, expected 16")]
    WrongLength(usize),
}

fn encode_base64_url(input: &[u8]) -> String {
    let mut out = String::with_capacity((input.len() * 4).div_ceil(3));
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i];
        let remaining = input.len() - i;
        if remaining == 1 {
            out.push(BASE64_URL_CHARS[(b0 >> 2) as usize] as char);
            out.push(BASE64_URL_CHARS[((b0 & 0x03) << 4) as usize] as char);
        } else if remaining == 2 {
            let b1 = input[i + 1];
            out.push(BASE64_URL_CHARS[(b0 >> 2) as usize] as char);
            out.push(BASE64_URL_CHARS[((b0 & 0x03) << 4 | (b1 >> 4)) as usize] as char);
            out.push(BASE64_URL_CHARS[((b1 & 0x0F) << 2) as usize] as char);
        } else {
            let b1 = input[i + 1];
            let b2 = input[i + 2];
            out.push(BASE64_URL_CHARS[(b0 >> 2) as usize] as char);
            out.push(BASE64_URL_CHARS[((b0 & 0x03) << 4 | (b1 >> 4)) as usize] as char);
            out.push(BASE64_URL_CHARS[((b1 & 0x0F) << 2 | (b2 >> 6)) as usize] as char);
            out.push(BASE64_URL_CHARS[(b2 & 0x3F) as usize] as char);
        }
        i += 3;
    }
    out
}

fn decode_base64_url(input: &str) -> Option<Vec<u8>> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity((bytes.len() * 3).div_ceil(4));
    let mut i = 0;
    while i < bytes.len() {
        let mut chunk = [0u8; 4];
        let mut n = 0;
        for j in 0..4 {
            if i + j < bytes.len() {
                chunk[j] = decode_base64_url_char(bytes[i + j])?;
                n += 1;
            } else {
                chunk[j] = 0;
            }
        }
        out.push((chunk[0] << 2) | (chunk[1] >> 4));
        if n >= 3 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if n >= 4 {
            out.push((chunk[2] << 6) | chunk[3]);
        }
        i += 4;
    }
    Some(out)
}

fn decode_base64_url_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}
