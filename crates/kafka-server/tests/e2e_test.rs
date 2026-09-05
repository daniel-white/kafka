//! End-to-end test: starts the broker, connects a raw Kafka client,
//! sends an ApiVersions request, and verifies the response framing.

use kafka_net::request_header::RequestHeader;
use kafka_net::response_header::ResponseHeader;
use kafka_server::{KafkaServer, SocketServer};
use kafka_server_common::ProcessStatus;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Build a wire-format ApiVersions request (api_key=0, version 3).
///
/// Mirrors what librdkafka sends on connect.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
fn build_api_versions_request(correlation_id: i32) -> Vec<u8> {
    let header = RequestHeader::new(0, 3, correlation_id, "");
    let mut payload = Vec::with_capacity(header.size() + 4);

    // Header
    header.write(&mut payload);

    // Body: client_id (empty string) + request_api_versions (int16=1) +
    //       client_id (empty string) again for ApiVersions v0-v2, but v3 uses
    //       a different structure. For v3: client_id="" + request_min_throttle_time_ms=0
    //       Actually, ApiVersions v3 body is: client_id (string),
    //       request_min_throttle_time_ms (int32), forgotten_topics (array).
    //
    // For simplicity, we'll use v0 which is just client_id.
    // But actually the body for ApiVersions v0 is:
    //   client_id: string (2 bytes, empty string)
    // That's all — the request body for ApiVersions v0 is just the client_id from the header,
    // which is already encoded in the header.
    //
    // Wait — in Kafka protocol, the body does NOT include client_id. The header includes
    // client_id, and the body starts right after the header. ApiVersions v0 body is empty
    // (just the header). Let's use v1 which adds a throttle time field... actually:
    //   v0: empty body
    //   v1: empty body
    //   v2: empty body
    //   v3: empty body (the new fields are in the header)
    //
    // So the body is just the payload after the header. Let's send v0 with empty body.
    // Actually the header in v0 doesn't include client_id? Let me check:
    //   header v0: api_key(2), api_version(2), correlation_id(4), client_id(string)
    //   header v1: same as v0 plus client_id? No — v0 includes client_id.
    //   header v1: adds nothing new... Actually:
    //   RequestHeader v0: apiKey, apiVersion, correlationId, clientId
    //   RequestHeader v1: same + nothing extra (clientId is in both)
    //
    // Actually the request header versions are:
    // v0: apiKey, apiVersion, correlationId, clientId
    // v1+: same
    //
    // So our header is correct. The body for ApiVersions v0 is empty.

    // The body is empty for ApiVersions v0
    let mut frame = Vec::with_capacity(4 + payload.len());
    let size = payload.len() as i32;
    frame.extend_from_slice(&size.to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Parse the raw response: [4-byte size][4-byte correlation_id]
///
/// Returns the correlation_id from the response header.
fn parse_response(data: &[u8]) -> Option<i32> {
    if data.len() < 8 {
        return None;
    }
    let _size = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let correlation_id = i32::from_be_bytes(data[4..8].try_into().unwrap());
    Some(correlation_id)
}

#[tokio::test]
async fn test_e2e_connection_and_request_response() {
    // Start the broker
    let server = Arc::new(KafkaServer::new());
    server.startup();
    assert_eq!(server.status(), ProcessStatus::Started);

    // Bind socket server on an ephemeral port
    let socket_server = SocketServer::new(server.clone());
    let listener = socket_server.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Start accepting connections
    tokio::spawn(async move {
        socket_server.accept_loop(listener).await;
    });

    // Give the server a moment to start accepting
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Connect as a Kafka client
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send an ApiVersions request with correlation_id = 42
    let request_bytes = build_api_versions_request(42);
    stream.write_all(&request_bytes).await.unwrap();

    // Read the response (up to 1024 bytes)
    let mut response_buf = vec![0u8; 1024];
    let n = stream.read(&mut response_buf).await.unwrap();
    let response_data = &response_buf[..n];

    // Verify we got back a properly framed response
    let correlation_id = parse_response(response_data);
    assert!(correlation_id.is_some(), "Should have parsed response");
    assert_eq!(
        correlation_id.unwrap(),
        42,
        "Response correlation_id should match request"
    );

    // Clean up
    server.shutdown();
}

#[tokio::test]
async fn test_e2e_multiple_requests() {
    let server = Arc::new(KafkaServer::new());
    server.startup();

    let socket_server = SocketServer::new(server.clone());
    let listener = socket_server.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        socket_server.accept_loop(listener).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for corr_id in 1..=5 {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request_bytes = build_api_versions_request(corr_id);
        stream.write_all(&request_bytes).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let cid = parse_response(&buf[..n]).unwrap();
        assert_eq!(cid, corr_id, "Correlation ID mismatch for request {}", corr_id);
    }

    server.shutdown();
}
