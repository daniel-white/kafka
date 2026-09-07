//! Metadata request handler: return broker and topic metadata.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java

use crate::handlers::{ApiRequest, ApiHandlerResult};
use kafka_protocol::messages::metadata::{MetadataRequest, MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};

/// Handle a Metadata request: return brokers list and topic metadata.
///
/// Uses the trait-based `MetadataResponse` message for version-aware serialization.
///
/// MIGRATION SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java
pub fn handle_metadata(ctx: ApiRequest) -> ApiHandlerResult{
    let _ = ctx.read_msg::<MetadataRequest>()?;
    
    let state = ctx.state().read_atomic();
    // Get the broker's advertised endpoint from server state
    let (broker_host, broker_port) = state.get_broker_endpoint();
    let brokers = vec![MetadataResponseBroker::new(
        0,
        broker_host,
        broker_port,
        None,
    )];

    let mut topics = Vec::with_capacity(state.topics.len());
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

    let res_msg = MetadataResponse::new(
        0,  // throttle_time_ms
        brokers,
        None, // cluster_id
        0,  // controller_id
        topics,
        0,  // cluster_authorized_operations
        0,  // error_code = NO_ERROR
    );
    
    ctx.respond_with(res_msg)
}