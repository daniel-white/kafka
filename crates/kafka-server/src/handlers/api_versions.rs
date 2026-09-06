//! ApiVersions request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::handlers::build_response_frame;
use crate::server_state::ServerState;
use kafka_protocol::api_versions_response::ApiVersionsResponse;
use kafka_protocol::{MessageContext, Writable};

/// Handle an ApiVersions request: return supported API versions from the registry.
///
/// Per the Kafka spec (KIP-511): ApiVersionsResponse always uses a non-flexible
/// response header (header version 0), even when the request has a flexible header.
/// Tagged fields are supported in the body only (for v3+).
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
pub fn handle_api_versions(state: &ServerState, correlation_id: i32, api_version: i16) -> Vec<u8> {
    // Build response from the API registry
    let entries = state.api_registry.entries();
    let response = ApiVersionsResponse::from_entries(entries);

    // ApiVersions response header is ALWAYS non-flexible (per Kafka spec)
    let ctx = MessageContext::new(api_version, false);
    let mut body = Vec::new();
    response.write(&mut body, &ctx);

    // is_flexible=false: ApiVersions response header is always non-flexible
    build_response_frame(correlation_id, false, body)
}
