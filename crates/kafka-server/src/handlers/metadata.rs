//! Metadata request handler: return broker and topic metadata.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java

use crate::handlers::{build_response_frame_with, RequestContext};
use crate::server_state::ServerState;
use kafka_protocol::metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic};

/// Handle a Metadata request: return brokers list and topic metadata.
///
/// Uses the trait-based `MetadataResponse` message for version-aware serialization.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java
pub fn handle_metadata(ctx: RequestContext, state: &ServerState) -> Vec<u8> {
    // Get the broker's advertised endpoint from server state
    let (broker_host, broker_port) = state.get_broker_endpoint();
    let brokers = vec![kafka_protocol::metadata_response::MetadataResponseBroker::new(
        0,
        broker_host,
        broker_port,
        None,
    )];

    let mut topics = Vec::new();
    for (name, metadata) in state.topics.iter() {
        let partitions: Vec<MetadataResponsePartition> = metadata
            .partitions
            .iter()
            .map(|p| MetadataResponsePartition::new(p.partition_index))
            .collect();

        topics.push(MetadataResponseTopic::new(
            name.clone(),
            0, // error_code = NO_ERROR
            metadata.is_internal,
            partitions,
        ));
    }

    let response = MetadataResponse::new(
        0,  // throttle_time_ms
        brokers,
        None, // cluster_id
        0,  // controller_id
        topics,
        0,  // cluster_authorized_operations
        0,  // error_code = NO_ERROR
    );

    build_response_frame_with(ctx, response, None)
}
