//! ListOffsets request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java

use crate::handlers::{ApiHandlerResult, ApiRequest};
use kafka_protocol::messages::list_offsets::{ListOffsetsRequest, ListOffsetsResponse};

/// Handle a ListOffsets request: return minimal response with empty topic list.
///
/// v0 (non-flexible): responses ARRAY (count=0)
/// v5+ (flexible body): throttle_time_ms + responses COMPACT_ARRAY + tagged_fields
///
/// Response header format is determined by `is_flexible`.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsResponse.java
pub fn handle_list_offsets(ctx: ApiRequest) -> ApiHandlerResult {
    // Read the request body (empty for this broker implementation)
    let _ = ctx.read_msg::<ListOffsetsRequest>()?;

    let res_msg = ListOffsetsResponse::new(0, vec![], 0);
    ctx.respond_with(res_msg)
}