//! End-to-end tests: start the broker, connect a raw Kafka client,
//! send requests, and verify responses.

use kafka_net::request_header::RequestHeader;
use kafka_server::{KafkaServer, SocketServer};
use kafka_server_common::ProcessStatus;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build a wire-format request frame for the given api_key.
///
/// Wire format: [4-byte size][api_key int16][api_version int16]
///   [correlation_id int32][client_id string (2-byte len prefix)]
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
fn build_request(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
    let header = RequestHeader::new(api_key, api_version, correlation_id, "");
    let mut payload = Vec::with_capacity(header.size());
    header.write(&mut payload);
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Parse the response correlation_id from [4-byte size][correlation_id].
fn parse_correlation_id(data: &[u8]) -> Option<i32> {
    if data.len() < 8 {
        return None;
    }
    let _size = i32::from_be_bytes(data[0..4].try_into().unwrap());
    i32::from_be_bytes(data[4..8].try_into().unwrap()).into()
}

/// Start a test broker, returning (KafkaServer, addr, join handle).
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
async fn test_e2e_api_versions_request() {
    let (server, addr, _handle) = start_test_broker().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // ApiVersions: api_key=15, version=3, correlation_id=42
    let request = build_request(15, 3, 42);
    stream.write_all(&request).await.unwrap();

    // Read the response
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = &buf[..n];

    // Verify correlation_id matches
    let cid = parse_correlation_id(response).expect("Failed to parse response");
    assert_eq!(cid, 42, "Response correlation_id should match request");

    // Verify response contains error_code = 0 (no error)
    // Response layout: [size:4][correlation_id:4][throttle_time:4][error_code:2][...]
    assert!(response.len() >= 14, "Response too short: {} bytes", response.len());
    let error_code = i16::from_be_bytes(response[12..14].try_into().unwrap());
    assert_eq!(error_code, 0, "Expected NO_ERROR (0), got {}", error_code);

    server.shutdown();
}

#[tokio::test]
async fn test_e2e_multiple_requests() {
    let (server, addr, _handle) = start_test_broker().await;

    for corr_id in 1..=3 {
        // Mix of api keys: ApiVersions (15) and Produce (0)
        let api_key = if corr_id % 2 == 1 { 15 } else { 0 };
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = build_request(api_key, 3, corr_id);
        stream.write_all(&request).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let cid = parse_correlation_id(&buf[..n]).unwrap();
        assert_eq!(
            cid, corr_id,
            "Correlation ID mismatch for request {}",
            corr_id
        );
    }

    server.shutdown();
}

#[tokio::test]
async fn test_e2e_metadata_request() {
    let (server, addr, _handle) = start_test_broker().await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Metadata: api_key=1, correlation_id=99
    let request = build_request(1, 0, 99);
    stream.write_all(&request).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let cid = parse_correlation_id(&buf[..n]).unwrap();
    assert_eq!(cid, 99, "Response should echo correlation_id");

    server.shutdown();
}

#[tokio::test]
async fn test_e2e_server_lifecycle() {
    let server = Arc::new(KafkaServer::new());
    assert_eq!(server.status(), ProcessStatus::Shutdown);
    assert!(!server.is_running());

    server.startup();
    assert_eq!(server.status(), ProcessStatus::Started);
    assert!(server.is_running());

    server.shutdown();
    assert_eq!(server.status(), ProcessStatus::Shutdown);
    assert!(!server.is_running());
}
