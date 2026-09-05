use kafka_net::connection::{ClientRequest, InFlightRequest, InFlightRequests};
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::kafka_response::KafkaResponse;
use kafka_net::request_header::RequestHeader;
use kafka_net::response_header::ResponseHeader;
use std::time::SystemTime;

fn ts_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn make_client_request(correlation_id: i32, body: Vec<u8>) -> ClientRequest {
    let header = RequestHeader::new(1, 0, correlation_id, "test-client");
    ClientRequest::new(
        "broker-1",
        correlation_id,
        header,
        body,
        ts_ms(),
        true,
        30000,
    )
}

fn make_in_flight_request(correlation_id: i32, body: Vec<u8>) -> InFlightRequest {
    let client_req = make_client_request(correlation_id, body.clone());
    let kafka_req = KafkaRequest::from_vec(
        RequestHeader::new(1, 0, correlation_id, "test-client"),
        body,
    );
    InFlightRequest::new(client_req, kafka_req, ts_ms())
}

fn make_in_flight_request_with_timeout(
    destination: &str,
    correlation_id: i32,
    send_time_ms: i64,
    request_timeout_ms: i32,
) -> InFlightRequest {
    let header = RequestHeader::new(1, 0, correlation_id, "test-client");
    let kafka_req = KafkaRequest::from_vec(header.clone(), vec![]);
    let client_req = ClientRequest::new(
        destination,
        correlation_id,
        header,
        vec![],
        send_time_ms,
        true,
        request_timeout_ms,
    );
    InFlightRequest::new(client_req, kafka_req, send_time_ms)
}

// --- Ported from InFlightRequestsTest ---

// Ported from testCompleteLastSent:
// add req1 then req2, completeLastSent returns req2 (most recent first due to addFirst)
#[test]
fn test_complete_last_sent() {
    let mut requests = InFlightRequests::new(12);

    let req1 = make_in_flight_request(1, vec![]);
    let req2 = make_in_flight_request(2, vec![]);
    requests.add(req1);
    requests.add(req2);
    assert_eq!(requests.total_count(), 2);

    // completeLastSent returns the most recently added (peekFirst / remove(0))
    let completed = requests.complete_last_sent("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 2);
    assert_eq!(requests.total_count(), 1);

    let completed = requests.complete_last_sent("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 1);
    assert_eq!(requests.total_count(), 0);
}

// Ported from testClearAll
#[test]
fn test_clear_all() {
    let mut requests = InFlightRequests::new(12);

    let req1 = make_in_flight_request(1, vec![]);
    let req2 = make_in_flight_request(2, vec![]);
    requests.add(req1);
    requests.add(req2);
    assert_eq!(requests.total_count(), 2);

    let cleared = requests.clear_all("broker-1");
    assert_eq!(requests.total_count(), 0);
    assert_eq!(cleared.len(), 2);
    // addFirst puts req2 at front, so descending iterator gives [req2, req1]
    assert_eq!(cleared[0].header.correlation_id, 2);
    assert_eq!(cleared[1].header.correlation_id, 1);
}

// Ported from testTimedOutNodes
#[test]
fn test_timed_out_nodes() {
    let mut requests = InFlightRequests::new(12);

    // Request A: send at 0, timeout 50
    let req_a = make_in_flight_request_with_timeout("A", 1, 0, 50);
    requests.add(req_a);
    // Request B1: send at 0, timeout 200
    let req_b1 = make_in_flight_request_with_timeout("B", 2, 0, 200);
    requests.add(req_b1);
    // Request B2: send at 0, timeout 100
    let req_b2 = make_in_flight_request_with_timeout("B", 3, 0, 100);
    requests.add(req_b2);

    // At t=50: A's elapsed = 50, not > 50. No timeouts.
    assert!(requests.nodes_with_timed_out_requests(50).is_empty());

    // At t=75: A's elapsed = 75 > 50. A timed out.
    let timed_out = requests.nodes_with_timed_out_requests(75);
    assert!(timed_out.contains(&"A".to_string()));
    assert!(!timed_out.contains(&"B".to_string()));

    // At t=125: A (125 > 50) and B2 (125 > 100) timed out, B1 (125 not > 200) not.
    let timed_out = requests.nodes_with_timed_out_requests(125);
    assert!(timed_out.contains(&"A".to_string()));
    assert!(timed_out.contains(&"B".to_string()));
}

// Ported from testCompleteNext: completes in FIFO order (oldest first)
#[test]
fn test_complete_next() {
    let mut requests = InFlightRequests::new(12);

    let req1 = make_in_flight_request(1, vec![]);
    let req2 = make_in_flight_request(2, vec![]);
    requests.add(req1);
    requests.add(req2);
    assert_eq!(requests.total_count(), 2);

    // completeNext returns the oldest (pollLast = pop from vec)
    let completed = requests.complete_next("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 1);
    assert_eq!(requests.total_count(), 1);

    let completed = requests.complete_next("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 2);
    assert_eq!(requests.total_count(), 0);
}

// Ported from testCompleteNextThrowsIfNoInFlights
#[test]
fn test_complete_next_no_in_flight() {
    let mut requests = InFlightRequests::new(12);
    assert!(requests.complete_next("broker-1").is_none());
}

// Ported from testCompleteLastSentThrowsIfNoInFlights
#[test]
fn test_complete_last_sent_no_in_flight() {
    let mut requests = InFlightRequests::new(12);
    assert!(requests.complete_last_sent("broker-1").is_none());
}

// --- Existing tests ---

#[test]
fn test_in_flight_requests_add_and_complete_next() {
    let mut requests = InFlightRequests::new(5);

    let req1 = make_in_flight_request(1, vec![0x01]);
    let req2 = make_in_flight_request(2, vec![0x02]);

    requests.add(req1);
    assert_eq!(requests.total_count(), 1);
    assert_eq!(requests.count("broker-1"), 1);
    assert!(!requests.is_empty("broker-1"));
    assert!(!requests.is_globally_empty());

    requests.add(req2);
    assert_eq!(requests.count("broker-1"), 2);

    // addFirst means req2 is at index 0, req1 at index 1
    // completeNext polls last (req1)
    let completed = requests.complete_next("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 1);
    assert_eq!(requests.total_count(), 1);

    let completed = requests.complete_next("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 2);
    assert!(requests.is_empty("broker-1"));
    assert!(requests.is_globally_empty());
}

#[test]
fn test_in_flight_requests_complete_last_sent() {
    let mut requests = InFlightRequests::new(5);

    let req1 = make_in_flight_request(1, vec![]);
    let req2 = make_in_flight_request(2, vec![]);

    requests.add(req1);
    requests.add(req2);

    // lastSent returns first in deque (req2, since addFirst)
    let last = requests.last_sent("broker-1").unwrap();
    assert_eq!(last.header.correlation_id, 2);

    // completeLastSent removes first (req2)
    let completed = requests.complete_last_sent("broker-1").unwrap();
    assert_eq!(completed.header.correlation_id, 2);
    assert_eq!(requests.total_count(), 1);
}

#[test]
fn test_in_flight_requests_can_send_more() {
    let mut requests = InFlightRequests::new(2);

    // Empty queue → can send
    assert!(requests.can_send_more("broker-1"));

    let mut req = make_in_flight_request(1, vec![]);
    req.send_completed = true; // send is complete
    requests.add(req);

    // Queue has 1 entry, max is 2, send completed → can send
    assert!(requests.can_send_more("broker-1"));

    let mut req2 = make_in_flight_request(2, vec![]);
    req2.send_completed = true;
    requests.add(req2);

    // Queue has 2 entries, max is 2 → cannot send more
    assert!(!requests.can_send_more("broker-1"));

    // Complete one → can send
    requests.complete_next("broker-1");
    assert!(requests.can_send_more("broker-1"));
}

#[test]
fn test_in_flight_requests_can_send_more_send_not_complete() {
    let mut requests = InFlightRequests::new(5);

    let mut req = make_in_flight_request(1, vec![]);
    req.send_completed = false; // send still in progress
    requests.add(req);

    // Send not complete → cannot send more
    assert!(!requests.can_send_more("broker-1"));
}

#[test]
fn test_in_flight_requests_nodes_with_timed_out() {
    let now = ts_ms();
    let mut requests = InFlightRequests::new(5);

    // Request with short timeout that will be timed out
    let client_req1 = ClientRequest::new(
        "broker-1",
        1,
        RequestHeader::new(1, 0, 1, "test-client"),
        vec![],
        now - 200,
        true,
        100, // 100ms timeout
    );
    let kafka_req1 = KafkaRequest::from_vec(
        RequestHeader::new(1, 0, 1, "test-client"),
        vec![],
    );
    let in_flight = InFlightRequest::new(client_req1, kafka_req1, now - 200);
    requests.add(in_flight);

    // Request that is not timed out
    let client_req2 = make_client_request(2, vec![]);
    let kafka_req2 = KafkaRequest::from_vec(
        RequestHeader::new(1, 0, 2, "test-client"),
        vec![],
    );
    let in_flight2 = InFlightRequest::new(client_req2, kafka_req2, now);
    requests.add(in_flight2);

    let timed_out_nodes = requests.nodes_with_timed_out_requests(now);
    assert!(timed_out_nodes.contains(&"broker-1".to_string()));
}

#[test]
fn test_in_flight_requests_clear_all() {
    let mut requests = InFlightRequests::new(5);

    let req1 = make_in_flight_request(1, vec![]);
    let req2 = make_in_flight_request(2, vec![]);
    requests.add(req1);
    requests.add(req2);
    assert_eq!(requests.total_count(), 2);

    let cleared = requests.clear_all("broker-1");
    assert_eq!(cleared.len(), 2);
    assert!(requests.is_empty("broker-1"));
    assert_eq!(requests.total_count(), 0);

    // Non-existent node returns empty
    let cleared = requests.clear_all("nonexistent");
    assert!(cleared.is_empty());
}

#[test]
fn test_in_flight_requests_per_node_isolation() {
    let mut requests = InFlightRequests::new(5);

    let req1 = make_in_flight_request(1, vec![]);
    let req1_clone = {
        let header = RequestHeader::new(1, 0, 1, "test-client");
        let kafka_req = KafkaRequest::from_vec(header, vec![]);
        let client_req = ClientRequest::new("broker-2", 1, RequestHeader::new(1, 0, 1, "test-client"), vec![], ts_ms(), true, 30000);
        InFlightRequest::new(client_req, kafka_req, ts_ms())
    };

    requests.add(req1);
    requests.add(req1_clone);

    // Should have 2 in-flight to different nodes
    assert_eq!(requests.total_count(), 2);
    assert_eq!(requests.count("broker-1"), 1);
    assert_eq!(requests.count("broker-2"), 1);
}

#[test]
fn test_in_flight_request_completion_response() {
    let now = ts_ms();
    let req = make_in_flight_request(42, vec![]);
    let response = KafkaResponse {
        header: ResponseHeader::new(42),
        body: vec![0xAB, 0xCD].into_boxed_slice(),
    };

    let client_response = req.completed(response, now);
    assert!(client_response.completed());
    assert!(!client_response.disconnected);
    assert!(client_response.body.is_some());
    assert_eq!(client_response.header.correlation_id, 42);
}

#[test]
fn test_in_flight_request_timed_out() {
    let now = ts_ms();
    let req = make_in_flight_request(1, vec![]);

    let response = req.timed_out(now);
    assert!(response.timed_out);
    assert!(!response.completed());
}

#[test]
fn test_in_flight_request_disconnected() {
    let now = ts_ms();
    let req = make_in_flight_request(1, vec![]);

    let response = req.disconnected(now);
    assert!(response.disconnected);
    assert!(!response.completed());
}

#[test]
fn test_client_request_elapsed_time() {
    let now = ts_ms();
    let req = ClientRequest::new("broker-1", 1, RequestHeader::default(), vec![], now, true, 30000);
    assert_eq!(req.time_elapsed_since_creation_ms(now), 0);
    assert_eq!(req.time_elapsed_since_creation_ms(now + 5000), 5000);
}

#[test]
fn test_client_request_to_kafka_request() {
    let header = RequestHeader::new(1, 0, 1, "test-client");
    let body = vec![0x01, 0x02, 0x03];
    let req = ClientRequest::new("broker-1", 1, header, body, ts_ms(), true, 30000);

    let kafka_req = req.to_kafka_request();
    assert_eq!(kafka_req.header.api_key, 1);
    assert_eq!(kafka_req.body, vec![0x01, 0x02, 0x03].into_boxed_slice());
}
