//! ListOffsets request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java

use crate::handlers::{build_response_frame, ApiRequest};
use kafka_protocol::list_offsets::ListOffsetsRequest;

/// Handle a ListOffsets request: return minimal response with empty topic list.
///
/// v0 (non-flexible): responses ARRAY (count=0)
/// v5+ (flexible body): throttle_time_ms + responses COMPACT_ARRAY + tagged_fields
///
/// Response header format is determined by `is_flexible`.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java
pub fn handle_list_offsets(ctx: ApiRequest) -> Vec<u8> {
    // Read the request body (empty for this broker implementation)
    let _req_msg = ctx.read_msg::<ListOffsetsRequest>();
    let mut body = Vec::new();
    let body_is_flexible = ctx.api_version() >= 5;

    if body_is_flexible {
        body.extend_from_slice(&0i32.to_be_bytes()); // throttle_time_ms = 0
    }

    // responses ARRAY / COMPACT_ARRAY (empty)
    if body_is_flexible {
        use kafka_protocol::byte_utils;
        byte_utils::write_unsigned_varint(1, &mut body).unwrap(); // 0 entries + 1
        // tagged_fields
        byte_utils::write_unsigned_varint(0, &mut body).unwrap();
    } else {
        body.extend_from_slice(&0i32.to_be_bytes()); // count = 0
    }

    build_response_frame(ctx.correlation_id(), ctx.is_flexible(), body)
}