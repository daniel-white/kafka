//! ApiVersions request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java

use crate::handlers::{ApiHandlerResult, ApiRequest};
use kafka_protocol::messages::api_versions::{ApiVersionsRequest, ApiVersionsResponse};

/// Handle an ApiVersions request: return supported API versions from the registry.
///
/// Per the Kafka spec (KIP-511): ApiVersionsResponse always uses a non-flexible
/// response header (header version 0), even when the request has a flexible header.
/// Tagged fields are supported in the body only (for v3+).
///
/// MIGRATION SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsResponse.java
pub fn handle_api_versions(req: ApiRequest) -> ApiHandlerResult<ApiVersionsResponse> {
    // Read the request body (empty for ApiVersions, but follows the standard pattern)
    let _ = req.read_msg::<ApiVersionsRequest>()?;

    let state = req.state.read_atomic();
    let entries = state.api_registry.entries();

    let res_msg = ApiVersionsResponse::from_entries(&entries);
    let res = req.send_msg(res_msg);
    Ok(res)
}