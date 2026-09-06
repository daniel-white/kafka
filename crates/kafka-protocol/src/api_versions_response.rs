//! ApiVersions response messages, implementing Writable and Readable traits.
//!
//! Wire format from ApiVersionsResponse.json:
//!   v0-v2 (non-flexible): ErrorCode, ApiKeys[] (ApiKey+MinVer+MaxVer), [ThrottleTimeMs(v1+)]
//!   v3+ (flexible): same fields but with compact encoding + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::reader::Reader;
use crate::writer::Writer;
use crate::errors::ProtocolError;
use crate::{MessageContext, Readable, Writable};
use getset::{CopyGetters, Getters};

// ── ApiVersion entry ───────────────────────────────────────────────────────

/// A single API version entry in the ApiVersions response.
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct ApiVersionEntry {
    #[get_copy = "pub"]
    api_key: i16,
    #[get_copy = "pub"]
    min_version: i16,
    #[get_copy = "pub"]
    max_version: i16,
}

impl ApiVersionEntry {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        ApiVersionEntry {
            api_key,
            min_version,
            max_version,
        }
    }
}

impl Writable for ApiVersionEntry {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 3;
        // ApiKey: INT16 (always)
        w.write_short(self.api_key);
        // MinVersion: INT16 (always)
        w.write_short(self.min_version);
        // MaxVersion: INT16 (always)
        w.write_short(self.max_version);
        // tagged_fields (flexible only, v3+)
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for ApiVersionEntry {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let api_key = r.read_short()?;
        let min_version = r.read_short()?;
        let max_version = r.read_short()?;
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(ApiVersionEntry {
            api_key,
            min_version,
            max_version,
        })
    }
}

// ── Top-level ApiVersionsResponse ─────────────────────────────────────────

/// Full ApiVersionsResponse message.
///
/// This response always uses a non-flexible response header (header version 0),
/// even when the request used a flexible header. Tagged fields are only supported
/// in the body (for v3+), not in the header.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
#[derive(Debug, Clone, Default)]
pub struct ApiVersionsResponse {
    error_code: i16,
    api_keys: Vec<ApiVersionEntry>,
    throttle_time_ms: i32,
    // Tagged fields (v3+): SupportedFeatures, FinalizedFeaturesEpoch, etc.
    // Omitted for simplicity — brokers may not return these.
}

impl ApiVersionsResponse {
    pub fn new(error_code: i16, api_keys: Vec<ApiVersionEntry>, throttle_time_ms: i32) -> Self {
        ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms,
        }
    }

    /// Build an ApiVersionsResponse from a list of (api_key, min, max) tuples.
    pub fn from_entries(entries: &[(i16, i16, i16)]) -> Self {
        let api_keys: Vec<ApiVersionEntry> = entries
            .iter()
            .map(|&(key, min, max)| ApiVersionEntry::new(key, min, max))
            .collect();
        ApiVersionsResponse::new(0, api_keys, 0)
    }
}

impl Writable for ApiVersionsResponse {
    fn write<W: Writer>(&self, w: &mut W, ctx: &MessageContext) {
        let flexible = ctx.api_version() >= 3;

        // ErrorCode: INT16 (always)
        w.write_short(self.error_code);

        // ApiKeys: ARRAY/COMPACT_ARRAY
        w.write_array_count(self.api_keys.len(), flexible);
        for entry in &self.api_keys {
            entry.write(w, ctx);
        }

        // ThrottleTimeMs: INT32 (v1+, ignorable)
        if ctx.api_version() >= 1 {
            w.write_int(self.throttle_time_ms);
        }

        // Top-level tagged_fields (v3+ only)
        // Note: SupportedFeatures, FinalizedFeaturesEpoch, etc. are tagged fields
        // that we don't populate.
        if flexible {
            w.write_empty_tagged_fields();
        }
    }
}

impl Readable for ApiVersionsResponse {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let error_code = r.read_short()?;
        let count = r.read_array_count(flexible)?;
        let mut api_keys = Vec::with_capacity(count);
        for _ in 0..count {
            api_keys.push(ApiVersionEntry::read(r, ctx)?);
        }
        let throttle_time_ms = if ctx.api_version() >= 1 {
            r.read_int()?
        } else {
            0
        };
        if flexible {
            r.skip_tagged_fields()?;
        }
        Ok(ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms,
        })
    }
}

// Re-export for convenience
pub use ApiVersionEntry as Entry;
