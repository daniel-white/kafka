//! DescribeTopics request handler.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java

use crate::handlers::{ApiRequest, ApiHandlerResult};
use kafka_protocol::messages::describe_topics::{
    DescribeTopicsRequest, DescribeTopicsResponse, DescribeTopicsTopic, DescribeTopicsPartition,
};

/// Handle a DescribeTopics/DescribeTopicPartitions request: return metadata for all known topics.
///
/// MIGRATION SOURCE:
///   clients/src/main/java/org/apache/kafka/common/requests/DescribeTopicsResponse.java
pub fn handle_describe_topics(ctx: ApiRequest) -> ApiHandlerResult {
    // Read the request body (empty for this broker implementation)
    let _req_msg = ctx.read_msg::<DescribeTopicsRequest>()?;

    let state = ctx.state.read_atomic();

    // Build response topics from broker state
    let topics: Vec<DescribeTopicsTopic> = state
        .topics
        .values()
        .map(|t| {
            let partitions: Vec<DescribeTopicsPartition> = t
                .partitions
                .iter()
                .map(|p| DescribeTopicsPartition::new(
                    0,                    // error_code
                    p.partition_index,    // partition_index
                    0,                    // leader_id
                    0,                    // leader_epoch
                    vec![0],              // replica_nodes
                    vec![0],              // isr_nodes
                    vec![],               // adding_replicas
                    vec![],               // removing_replicas
                ))
                .collect();
            DescribeTopicsTopic::new(t.name.clone(), 0, t.is_internal, partitions)
        })
        .collect();

    let res_msg = DescribeTopicsResponse::new(0, topics);
    ctx.respond_with(res_msg)
}
