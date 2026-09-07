//! Fetch request handler: parse request, retrieve stored records, build response.
//!
//! MIGRATION_SOURCE:
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java
//!   clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java
//!   core/src/main/scala/kafka/server/KafkaApis.scala (handleFetch)

use crate::handlers::{ApiHandlerResult, ApiRequest};
use crate::server_state::ServerState;
use kafka_protocol::messages::fetch::{FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicResponse};

/// Handle a Fetch request: extract topic/partition, retrieve stored records,
/// and return a Fetch response.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaApis.scala (handleFetch)
pub fn handle_fetch(ctx: ApiRequest) -> ApiHandlerResult {
    // Read the request body using the standard pattern
    let req_msg = ctx.read_msg::<FetchRequest>()?;

    let state = ctx.state.read_atomic();

    let mut responses = Vec::new();
    for topic in &req_msg.topics {
        for partition in &topic.partitions {
            let batches = state.read_records(&topic.name, partition.partition, partition.fetch_offset);
        let total: usize = batches.iter().map(|b| b.len()).sum();
        let mut records = Vec::with_capacity(total);
        for b in batches {
            records.extend_from_slice(&b);
        }

        let partition = FetchPartitionResponse::new(
            partition.partition,
            0, // error_code
            0, // high_watermark
            0, // last_stable_offset
            0, // log_start_offset
            0, // log_end_offset
            records,
        );

            responses.push(FetchTopicResponse::new(
                topic.name.clone(),
                vec![partition],
            ));
        }
    }

    let res_msg = FetchResponse::new(0, responses, 0);
    ctx.respond_with(res_msg)
}