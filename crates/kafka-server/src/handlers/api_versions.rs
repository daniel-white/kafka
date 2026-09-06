//! ApiVersions request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::handlers::build_response_frame;
use kafka_protocol::byte_utils;

/// Handle an ApiVersions request: return supported API versions.
///
/// Field order from JSON spec (clients/.../ApiVersionsResponse.json):
///   ErrorCode (int16, 0+)
///   ApiKeys (COMPACT_ARRAY, 0+)
///   ThrottleTimeMs (int32, 1+, ignorable)
///   [Tagged fields for v3+: SupportedFeatures, FinalizedFeaturesEpoch, etc.]
///
/// v0-v2 (non-flexible): error_code + ARRAY(api_key,min,max) [+ throttle_time_ms for v1+]
/// v3+ (flexible body): error_code + COMPACT_ARRAY(api_key,min,max,tags) + throttle_time_ms + tagged_fields
///
/// Response header format is determined by `is_flexible` (whether the request used a flexible header).
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
pub fn handle_api_versions(correlation_id: i32, api_version: i16, is_flexible: bool) -> Vec<u8> {
    // (api_key, min_version, max_version) — only APIs the server actually handles
    let api_entries: &[(i16, i16, i16)] = &[
        (0, 0, 13),    // Produce
        (1, 0, 16),    // Fetch
        (2, 0, 5),     // ListOffsets
        (3, 0, 13),    // Metadata
        (18, 0, 5),    // ApiVersions
    ];

    let mut body = Vec::new();
    let body_is_flexible = api_version >= 3;

    // Field order per JSON spec: ErrorCode, ApiKeys, ThrottleTimeMs, tagged_fields

    // ErrorCode: INT16 = 0 (NO_ERROR)
    body.extend_from_slice(&0i16.to_be_bytes());

    // ApiKeys: ARRAY / COMPACT_ARRAY
    if body_is_flexible {
        byte_utils::write_unsigned_varint((api_entries.len() + 1) as u32, &mut body).unwrap();
        for (api_key, min_version, max_version) in api_entries {
            body.extend_from_slice(&api_key.to_be_bytes());
            body.extend_from_slice(&min_version.to_be_bytes());
            body.extend_from_slice(&max_version.to_be_bytes());
            byte_utils::write_unsigned_varint(0, &mut body).unwrap(); // tagged_fields per entry
        }
    } else {
        body.extend_from_slice(&(api_entries.len() as i32).to_be_bytes());
        for (api_key, min_version, max_version) in api_entries {
            body.extend_from_slice(&api_key.to_be_bytes());
            body.extend_from_slice(&min_version.to_be_bytes());
            body.extend_from_slice(&max_version.to_be_bytes());
        }
    }

    // ThrottleTimeMs: INT32 = 0 (v1+, ignorable)
    body.extend_from_slice(&0i32.to_be_bytes());

    // Top-level tagged_fields (v3+ only)
    if body_is_flexible {
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    }

    build_response_frame(correlation_id, is_flexible, body)
}
