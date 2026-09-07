//! Produce request handler: parse request, store records, build response.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java
//!   core/src/main/scala/kafka/server/KafkaApis.scala (handleProduce)

use crate::handlers::{ApiHandlerResult, ApiRequest};
use kafka_protocol::messages::produce::{
    ProduceRequest, PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
};

/// Handle a Produce request: extract topic-partition info, append records
/// to the log, and return a Produce response with the assigned base_offset.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala (handleProduce)
pub fn handle_produce(req: ApiRequest) -> ApiHandlerResult {
    // Read the request body using the standard pattern
    let req_msg = req.read_msg::<ProduceRequest>()?;

    let mut base_offsets = Vec::with_capacity(req_msg.topics.len());
    for topic in &req_msg.topics {
        for partition in &topic.partitions {
            // Read current state atomically, clone it, apply mutations, and write back
            let mut new_state = req.state.read_atomic();
            if !new_state.topics.contains_key(&topic.name) {
                new_state.register_topic(&topic.name, 1);
            }
            let base_offset = new_state.append_records(
                &topic.name,
                partition.index,
                partition.records.clone(),
            );
            req.state().write_atomic(new_state);
            base_offsets.push(base_offset);
        }
    }

    let mut topics = Vec::new();

    for topic in &req_msg.topics {
        let mut partitions = Vec::new();
        for (i, partition) in topic.partitions.iter().enumerate() {
            let base_offset = base_offsets.get(i).copied().unwrap_or(0);

            partitions.push(PartitionProduceResponse {
                index: partition.index,
                error_code: 0,
                base_offset,
                log_append_time_ms: 0,
                log_start_offset: 0,
                record_errors: Vec::new(),
                error_message: None,
                current_leader: None,
            });
        }

        topics.push(TopicProduceResponse::new(
            topic.name.clone(),
            partitions,
        ));
    }

    let res_msg = ProduceResponse::new(topics, 0, 0);
    req.respond_with(res_msg)
}