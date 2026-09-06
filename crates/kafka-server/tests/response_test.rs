use kafka_net::kafka_request::KafkaRequest;
use kafka_net::request_header::RequestHeader;
use kafka_server::handlers::dispatch;
use kafka_server::server_state::ServerState;
use std::sync::RwLock;

fn make_request(api_key: i16, api_version: i16, correlation_id: i32) -> KafkaRequest {
    let header = RequestHeader::new(api_key, api_version, correlation_id, "");
    KafkaRequest::from_vec(header, Vec::new())
}

fn make_state() -> RwLock<ServerState> {
    RwLock::new(ServerState::default())
}

fn extract_correlation_id(response: &[u8]) -> i32 {
    i32::from_be_bytes(response[4..8].try_into().unwrap())
}

fn extract_body_size(response: &[u8]) -> i32 {
    i32::from_be_bytes(response[0..4].try_into().unwrap())
}

#[test]
fn test_produce_v0_response() {
    let req = make_request(0, 0, 42); // Produce v0, non-flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    // Frame: [size][correlation_id][topics_count=0]
    assert_eq!(resp.len(), 4 + 4 + 4);
    assert_eq!(extract_body_size(&resp), 8); // 4 (header) + 4 (topics_count)
    assert_eq!(extract_correlation_id(&resp), 42);
    let topics_count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(topics_count, 0);
}

#[test]
fn test_produce_v3_with_throttle_and_tags() {
    let req = make_request(0, 3, 99); // Produce v3, flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    assert!(resp.len() > 4 + 4); // header + array_count + tagged_fields
    assert_eq!(extract_correlation_id(&resp), 99);
}

#[test]
fn test_fetch_response() {
    let req = make_request(1, 0, 100); // Fetch v0, non-flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    // v0: [size][correlation_id][topics_count=0][error_code=0]
    assert!(resp.len() >= 4 + 4 + 4 + 2);
    assert_eq!(extract_correlation_id(&resp), 100);
    let count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(count, 0);
    let err = i16::from_be_bytes(resp[12..14].try_into().unwrap());
    assert_eq!(err, 0);
}

#[test]
fn test_list_offsets_response() {
    let req = make_request(2, 0, 200); // ListOffsets v0, non-flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    assert_eq!(resp.len(), 4 + 4 + 4);
    assert_eq!(extract_correlation_id(&resp), 200);
    let count = i32::from_be_bytes(resp[8..12].try_into().unwrap());
    assert_eq!(count, 0);
}

#[test]
fn test_unknown_api_response() {
    let req = make_request(999, 0, 300); // Unknown API, non-flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    assert_eq!(resp.len(), 4 + 4);
    assert_eq!(extract_body_size(&resp), 4);
    assert_eq!(extract_correlation_id(&resp), 300);
}

#[test]
fn test_dispatched_produce_v6_has_throttle() {
    let req = make_request(0, 6, 42); // Produce v6, flexible
    let state = make_state();
    let resp = dispatch(&req, &state);

    // v6: [size][correlation_id][tagged_fields][topics_count][throttle][tagged_fields]
    assert!(resp.len() >= 4 + 4 + 1 + 1 + 4 + 1);
    assert_eq!(extract_correlation_id(&resp), 42);
}
