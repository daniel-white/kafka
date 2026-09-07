//! ApiVersions request and response messages, implementing Writable and Readable traits.
//!
//! Wire format from ApiVersionsResponse.json:
//!   v0-v2 (non-flexible): ErrorCode, ApiKeys[] (ApiKey+MinVer+MaxVer), [ThrottleTimeMs(v1+)]
//!   v3+ (flexible): same fields but with compact encoding + tagged_fields
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::errors::ProtocolError;
use crate::io::reader::Readable;
use crate::io::reader::Reader;
use crate::io::writer::{Writable, Writer};
use crate::messages::tagged_fields::TaggedFields;
use crate::MessageContext;
use getset::Getters;

/// ApiVersions request message.
///
/// This request has no body (just the header), but we provide a type
/// for consistency with the protocol API.
#[derive(Debug, Clone, Default)]
pub struct ApiVersionsRequest;

impl Readable for ApiVersionsRequest {
    fn read<R: Reader>(
        _r: &mut R,
        _ctx: &MessageContext,
    ) -> Result<Self, ProtocolError> {
        Ok(ApiVersionsRequest)
    }
}

// ── ApiVersion entry ───────────────────────────────────────────────────────

/// A single API version entry in the ApiVersions response.
#[derive(Debug, Clone, Getters)]
pub struct ApiVersionEntry {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    #[get]
    tagged_fields: TaggedFields,
}

impl ApiVersionEntry {
    pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
        ApiVersionEntry {
            api_key,
            min_version,
            max_version,
            tagged_fields: TaggedFields::empty(),
        }
    }

    pub fn api_key(&self) -> i16 { self.api_key }
    pub fn min_version(&self) -> i16 { self.min_version }
    pub fn max_version(&self) -> i16 { self.max_version }
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
            self.tagged_fields.write(w, ctx);
        }
    }
}

impl Readable for ApiVersionEntry {
    fn read<R: Reader>(r: &mut R, ctx: &MessageContext) -> Result<Self, ProtocolError> {
        let flexible = ctx.api_version() >= 3;
        let api_key = r.read_short()?;
        let min_version = r.read_short()?;
        let max_version = r.read_short()?;
        let tagged_fields = if flexible {
            TaggedFields::read(r, ctx)?
        } else {
            TaggedFields::empty()
        };
        Ok(ApiVersionEntry {
            api_key,
            min_version,
            max_version,
            tagged_fields,
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
#[derive(Debug, Clone, Default, Getters)]
pub struct ApiVersionsResponse {
    error_code: i16,
    #[get = "pub"]
    api_keys: Vec<ApiVersionEntry>,
    throttle_time_ms: i32,
    #[get = "pub"]
    tagged_fields: TaggedFields,
}

impl ApiVersionsResponse {
    pub fn new(error_code: i16, api_keys: Vec<ApiVersionEntry>, throttle_time_ms: i32) -> Self {
        ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms,
            tagged_fields: TaggedFields::empty(),
        }
    }

    pub fn error_code(&self) -> i16 { self.error_code }
    pub fn throttle_time_ms(&self) -> i32 { self.throttle_time_ms }

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
        if flexible {
            self.tagged_fields.write(w, ctx);
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
        let tagged_fields = if flexible {
            TaggedFields::read(r, ctx)?
        } else {
            TaggedFields::empty()
        };
        Ok(ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms,
            tagged_fields,
        })
    }
}

