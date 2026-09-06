//! End-to-end produce + fetch round trip test using raw TCP.
//!
//! Constructs wire-format Produce and Fetch requests, sends them to the
//! broker, and verifies the response contains the produced record batch.

use kafka_server::{KafkaServer, SocketServer};
use kafka_server_common::ProcessStatus;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build a minimal Kafka record batch (v2 magic) with one empty record.
///
/// RecordBatch format:
///   base_offset: INT64
///   batch_length: INT32
///   magic: INT8 (=2)
///   crc: INT32
///   attributes: INT16
///   last_offset_delta: INT32
///   base_timestamp: INT64
///   max_timestamp: INT64
///   producer_id: INT64 (-1)
///   producer_epoch: INT16 (-1)
///   first_sequence: INT32 (-1)
///   record_count: INT32 (=1)
///   records: [
///     length: INT32
///     attributes: INT8 (=0)
///     timestamp_delta: VARINT (=0)
///     offset_delta: VARINT (=0)
///     key: NULLABLE_BYTES (=null, varint 0)
///     value: NULLABLE_BYTES (=null, varint 0)
///     headers_count: VARINT (=0)
///   ]
fn build_record_batch() -> Vec<u8> {
    // Record content: attributes + timestamp_delta + offset_delta + key(null) + value(null) + headers_count
    let record: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
    let record_len = record.len() as i32;
    let mut record_data: Vec<u8> = Vec::new();
    record_data.extend_from_slice(&record_len.to_be_bytes());
    record_data.extend_from_slice(&record);

    // RecordBatch fields (without base_offset and batch_length)
    let mut inner = Vec::new();
    inner.push(0x02); // magic = 2
    inner.extend_from_slice(&0u32.to_be_bytes()); // crc = 0 (placeholder)
    inner.extend_from_slice(&0u16.to_be_bytes()); // attributes = 0
    inner.extend_from_slice(&0u32.to_be_bytes()); // last_offset_delta = 0
    inner.extend_from_slice(&0i64.to_be_bytes()); // base_timestamp = 0
    inner.extend_from_slice(&0i64.to_be_bytes()); // max_timestamp = 0
    inner.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id = -1
    inner.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch = -1
    inner.extend_from_slice(&(-1i32).to_be_bytes()); // first_sequence = -1
    inner.extend_from_slice(&1i32.to_be_bytes()); // record_count = 1
    inner.extend_from_slice(&record_data);

    // Full batch: base_offset + batch_length + inner
    let mut result = Vec::new();
    result.extend_from_slice(&0i64.to_be_bytes()); // base_offset = 0
    let inner_len = inner.len() as i32;
    result.extend_from_slice(&inner_len.to_be_bytes()); // batch_length
    result.extend_from_slice(&inner);
    result
}

/// Build a Produce request v0 frame: [size][header][body]
///
/// Produce v0 body:
///   transactional_id: STRING (empty)
///   acks: INT16 (-1)
///   timeout_ms: INT32 (1000)
///   topics: ARRAY [
///     topic_name: STRING
///     partitions: ARRAY [
///       partition_index: INT32
///       record_set: RECORDS (INT32 length + batch bytes)
///     ]
///   ]
fn build_produce_request(api_key: i16, api_version: i16, correlation_id: i32, topic: &str, partition: i32, record_batch: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0i16.to_be_bytes()); // transactional_id: empty STRING (length=0)
    body.extend_from_slice(&(-1i16).to_be_bytes()); // acks = -1
    body.extend_from_slice(&1000i32.to_be_bytes()); // timeout_ms = 1000
    body.extend_from_slice(&1i32.to_be_bytes()); // topics ARRAY count = 1
    body.extend_from_slice(&(topic.len() as i16).to_be_bytes()); // topic_name length
    body.extend_from_slice(topic.as_bytes()); // topic_name
    body.extend_from_slice(&1i32.to_be_bytes()); // partitions ARRAY count = 1
    body.extend_from_slice(&partition.to_be_bytes()); // partition_index
    body.extend_from_slice(&(record_batch.len() as i32).to_be_bytes()); // record_set length
    body.extend_from_slice(record_batch); // record_set bytes

    build_request_frame(api_key, api_version, correlation_id, &body)
}

/// Build a Fetch request v0 frame.
///
/// Fetch v0 body:
///   replica_id: INT32 (-1)
///   max_wait_ms: INT32
///   min_bytes: INT32
///   topics: ARRAY [
///     topic_name: STRING
///     partitions: ARRAY [
///       partition_index: INT32
///       fetch_offset: INT64
///       max_bytes: INT32
///     ]
///   ]
fn build_fetch_request(api_key: i16, api_version: i16, correlation_id: i32, topic: &str, partition: i32, fetch_offset: i64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id = -1
    body.extend_from_slice(&500i32.to_be_bytes()); // max_wait_ms = 500
    body.extend_from_slice(&1i32.to_be_bytes()); // min_bytes = 1
    body.extend_from_slice(&1i32.to_be_bytes()); // topics ARRAY count = 1
    body.extend_from_slice(&(topic.len() as i16).to_be_bytes()); // topic_name length
    body.extend_from_slice(topic.as_bytes()); // topic_name
    body.extend_from_slice(&1i32.to_be_bytes()); // partitions ARRAY count = 1
    body.extend_from_slice(&partition.to_be_bytes()); // partition_index
    body.extend_from_slice(&fetch_offset.to_be_bytes()); // fetch_offset
    body.extend_from_slice(&1048576i32.to_be_bytes()); // max_bytes = 1MB

    build_request_frame(api_key, api_version, correlation_id, &body)
}

/// Build a generic Kafka request frame: [size][header][body]
fn build_request_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&api_key.to_be_bytes());
    payload.extend_from_slice(&api_version.to_be_bytes());
    payload.extend_from_slice(&correlation_id.to_be_bytes());
    payload.extend_from_slice(&0i16.to_be_bytes()); // client_id: empty STRING
    payload.extend_from_slice(body);

    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Parse response: [size:4][correlation_id:4][body...]
fn parse_response(data: &[u8]) -> (i32, &[u8]) {
    let _size = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let correlation_id = i32::from_be_bytes(data[4..8].try_into().unwrap());
    (correlation_id, &data[8..])
}

/// Parse Produce v0 response to find base_offset for a topic+partition.
/// Format: responses ARRAY [ topic_name STRING, partitions ARRAY [ partition_index INT32, error_code INT16, base_offset INT64 ] ]
fn parse_produce_response_body(body: &[u8]) -> (i16, i64) {
    let mut buf = body;
    let count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
    buf = &buf[4..];
    for _ in 0..count {
        let topic_len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
        buf = &buf[2..];
        let _topic = String::from_utf8_lossy(&buf[..topic_len]).to_string();
        buf = &buf[topic_len..];
        let part_count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            buf = &buf[4..];
            if part_count > 0 {
                let _partition_index = i32::from_be_bytes(buf[0..4].try_into().unwrap());
                buf = &buf[4..];
                let error_code = i16::from_be_bytes(buf[0..2].try_into().unwrap());
                buf = &buf[2..];
                let base_offset = i64::from_be_bytes(buf[0..8].try_into().unwrap());
                return (error_code, base_offset);
        }
    }
    (-1, 0) // fallback if nothing found
}

/// Parse Fetch v0 response to extract record bytes for a topic+partition.
/// Format: responses ARRAY [ topic_name STRING, partitions ARRAY [
///   partition_index INT32, error_code INT16, high_watermark INT64,
///   records: NULLABLE_BYTES (INT32 length + bytes)
/// ] ], error_code INT16
fn parse_fetch_response_body(body: &[u8]) -> Option<Vec<u8>> {
    let mut buf = body;
    let count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
    buf = &buf[4..];
    for _ in 0..count {
        let topic_len = i16::from_be_bytes(buf[0..2].try_into().unwrap()) as usize;
        buf = &buf[2..];
        buf = &buf[topic_len..];
        let part_count = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        buf = &buf[4..];
        if part_count > 0 {
            buf = &buf[4..]; // skip partition_index
            buf = &buf[2..]; // skip error_code
            buf = &buf[8..]; // skip high_watermark
            let records_len = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            buf = &buf[4..];
            if records_len < 0 {
                return None; // null
            }
            let records = buf[..records_len as usize].to_vec();
            return Some(records);
        }
    }
    None
}

/// Start a test broker on a random port.
async fn start_test_broker() -> (Arc<KafkaServer>, std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let server = Arc::new(KafkaServer::new());
    server.startup();
    assert_eq!(server.status(), ProcessStatus::Started);

    let socket_server = SocketServer::new(server.clone());
    let listener = socket_server.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = tokio::spawn(async move {
        socket_server.accept_loop(listener).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (server, addr, handle)
}

#[tokio::test]
async fn test_e2e_produce_and_fetch() {
    let (server, addr, _handle) = start_test_broker().await;

    let topic = "test-topic";
    let partition = 0i32;

    // --- Produce ---
    let record_batch = build_record_batch();
    let produce_req = build_produce_request(0, 0, 42, topic, partition, &record_batch);

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&produce_req).await.unwrap();

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await.unwrap();
    let (corr_id, body) = parse_response(&buf[..n]);
    assert_eq!(corr_id, 42, "Produce response correlation_id mismatch");
    let (error_code, base_offset) = parse_produce_response_body(body);
    assert_eq!(error_code, 0, "Produce should succeed");
    assert_eq!(base_offset, 0, "Base offset should be 0 for first produce");

    // --- Fetch ---
    let fetch_req = build_fetch_request(1, 0, 43, topic, partition, 0);
    let mut stream2 = TcpStream::connect(addr).await.unwrap();
    stream2.write_all(&fetch_req).await.unwrap();

    let n = stream2.read(&mut buf).await.unwrap();
    let (corr_id, body) = parse_response(&buf[..n]);
    assert_eq!(corr_id, 43, "Fetch response correlation_id mismatch");
    let records = parse_fetch_response_body(body).expect("Should have records");
    assert_eq!(records.len(), record_batch.len(), "Record batch should match");
    assert_eq!(&records[..], &record_batch[..], "Records should match produced batch");

    server.shutdown();
}
