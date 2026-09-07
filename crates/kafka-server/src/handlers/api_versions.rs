//! ApiVersions request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::handlers::{build_response_frame_with, RequestContext};
use kafka_protocol::api_versions_response::ApiVersionsResponse;

/// Handle an ApiVersions request: return supported API versions from the registry.
///
/// Per the Kafka spec (KIP-511): ApiVersionsResponse always uses a non-flexible
/// response header (header version 0), even when the request has a flexible header.
/// Tagged fields are supported in the body only (for v3+).
///
/// MIGRATION SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
pub fn handle_api_versions(ctx: RequestContext) -> Vec<u8> {
    // Build response from the API registry (lock-free read)
    let state = ctx.state.read_atomic();
    let entries = state.api_registry.entries();
    let response = ApiVersionsResponse::from_entries(&entries);

    // Per Kafka spec (KIP-511): ApiVersions response header is ALWAYS non-flexible,
    // even when the request has a flexible header. Tagged fields are supported in
    // the body only (for v3+).
    build_response_frame_with(ctx.correlation_id, false, ctx.api_version, response)
}
