//! Message context: carries API version and header flexibility info.
//!
//! Passed to `Writable` and `Readable` implementations so each
//! message can make version- and flexibility-aware serialization decisions.
//!
//! "Flexible" refers to the request/response header format (v2+): compact strings,
//! varint array counts, and tagged fields. The body flexibility is determined by
//! the API version's `flexibleVersions` field in the JSON spec — not by the
//! header flexibility. Both are captured in `MessageContext`.

use getset::CopyGetters;

/// Context for message serialization/deserialization.
///
/// - `api_version`: the version negotiated between client and server.
/// - `is_flexible`: whether the request header used flexible encoding.
///   This determines the response header format.
///
/// Body flexibility is NOT directly stored here — each message implementation
/// decides body flexibility based on its own `flexibleVersions` from the JSON spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct MessageContext {
    #[get_copy = "pub"]
    api_version: i16,
    #[get_copy = "pub"]
    is_flexible: bool,
}

impl MessageContext {
    pub fn new(api_version: i16, is_flexible: bool) -> Self {
        MessageContext {
            api_version,
            is_flexible,
        }
    }
}

/// Indicates whether the header used flexible (v2+) encoding.
/// This determines whether the response header includes a trailing tagged_fields varint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct HeaderContext {
    #[get_copy = "pub"]
    api_version: i16,
    #[get_copy = "pub"]
    is_flexible_header: bool,
}

impl HeaderContext {
    pub fn new(api_version: i16, is_flexible_header: bool) -> Self {
        HeaderContext {
            api_version,
            is_flexible_header,
        }
    }
}

impl From<HeaderContext> for MessageContext {
    fn from(header: HeaderContext) -> Self {
        MessageContext::new(header.api_version(), header.is_flexible_header())
    }
}
