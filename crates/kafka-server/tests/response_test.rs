use kafka_net::kafka_request::KafkaRequest;
use kafka_net::request_header::RequestHeader;
use kafka_server::{dispatch_request, ServerState};

fn make_request(api_key: i16, api_version: i16, correlation_id: i32) -> KafkaRequest {
    let header = RequestHeader::new(api_key, api_version, correlation_id, "");
    KafkaRequest::from_vec(header, Vec::new())
}

fn extract_correlation_id(response: &[u8]) -> i32 {
    i32::from_be_bytes(response[4..8].try_into().unwrap())
}

fn extract_body_size(response: &[u8]) -> i32 {
    i32::from_be_bytes(response[0..4].try_into().unwrap())
}

#[test]
fn test_produce_v0_response() {
    let req = make_request(0, 0, 42); // Produce v0
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // Frame: [size:4=8][correlation_id:4][topics_count:4=0]
    assert_eq!(resp.len(), 4 + 4 + 4);
    assert_eq!(extract_body_size(&resp), 8); // 4 (header) + 4 (topics_count)
    assert_eq!(extract_correlation_id(&resp), 42);
    // topics count = 0
    let topics_count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(topics_count, 0);
}

#[test]
fn test_produce_v3_with_throttle_and_tags() {
    let req = make_request(0, 3, 99); // Produce v3
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // v3+: [correlation_id][topics_count=0][tagged_fields=0(varint)]
    assert!(resp.len() >= 4 + 4 + 4 + 1);
    assert_eq!(extract_correlation_id(&resp), 99);
    // topics count = 0
    let topics_count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(topics_count, 0);
}

#[test]
fn test_fetch_response() {
    let req = make_request(1, 0, 100); // Fetch v0
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // [size][correlation_id][throttle_ms][topics_count=0][error_code]
    assert!(resp.len() >= 4 + 4 + 4 + 4 + 2);
    assert_eq!(extract_correlation_id(&resp), 100);
    // throttle_time_ms = 0
    let throttle = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(throttle, 0);
    // topics count = 0
    let count = i32::from_be_bytes(resp[12..16].try_into().unwrap());
    assert_eq!(count, 0);
    // error_code = 0
    let err = i16::from_be_bytes(resp[16..18].try_into().unwrap());
    assert_eq!(err, 0);
}

#[test]
fn test_list_offsets_response() {
    let req = make_request(2, 0, 200); // ListOffsets v0
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // [size][correlation_id][topics_count=0]
    assert_eq!(resp.len(), 4 + 4 + 4);
    assert_eq!(extract_correlation_id(&resp), 200);
    let count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(count, 0);
}

#[test]
fn test_unknown_api_response() {
    let req = make_request(999, 0, 300); // Unknown API
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // Should return empty response: [size=4][correlation_id=300]
    assert_eq!(resp.len(), 4 + 4);
    assert_eq!(extract_body_size(&resp), 4); // just the header (correlation_id only)
    assert_eq!(extract_correlation_id(&resp), 300);
}

#[test]
fn test_dispatched_produce_v6_has_throttle() {
    let req = make_request(0, 6, 42); // Produce v6
    let state = ServerState::default();
    let resp = dispatch_request(&req, &state);

    // v6+: has throttle_time_ms after topics array
    assert!(resp.len() >= 4 + 4 + 4 + 4); // header + topics_count + throttle
    assert_eq!(extract_correlation_id(&resp), 42);
}
