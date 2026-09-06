//! Metadata request handler: return broker and topic metadata.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java

use crate::handlers::build_response_frame;
use crate::server_state::ServerState;
use kafka_protocol::metadata_response::{MetadataResponse, MetadataResponsePartition, MetadataResponseTopic};
use kafka_protocol::{MessageContext, Writable};

/// Handle a Metadata request: return brokers list and topic metadata.
///
/// Uses the trait-based `MetadataResponse` message for version-aware serialization.
///
/// MIGRATION_SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java
pub fn handle_metadata(correlation_id: i32, state: &ServerState, api_version: i16, is_flexible: bool) -> Vec<u8> {
    // Get the broker's advertised endpoint from server state
    let (broker_host, broker_port) = state.get_broker_endpoint();
    let brokers = vec![kafka_protocol::metadata_response::MetadataResponseBroker::new(
        0,
        broker_host,
        broker_port,
        None,
    )];

    let mut topics = Vec::new();
    for topic in state.topics.values() {
        let partitions: Vec<MetadataResponsePartition> = topic
            .partitions
            .iter()
            .map(|p| MetadataResponsePartition::new(p.partition_index))
            .collect();

        topics.push(MetadataResponseTopic::new(
            topic.name.clone(),
            0, // error_code = NO_ERROR
            topic.is_internal,
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

    let ctx = MessageContext::new(api_version, is_flexible);
    let mut body = Vec::with_capacity(response.body_size(&ctx));
    response.write_body(&mut body, &ctx);

    build_response_frame(correlation_id, is_flexible, body)
}
