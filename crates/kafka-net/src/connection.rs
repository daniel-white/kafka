//! Connection-layer types: ClientRequest, ClientResponse, InFlightRequest, InFlightRequests.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`:
//!   - `InFlightRequests` (inner class collection)
//!   - `InFlightRequest` (inner class)
//!   - `ClientRequest`
//!   - `ClientResponse`
//!   - `RequestCompletionHandler`

use crate::kafka_request::KafkaRequest;
use crate::kafka_response::KafkaResponse;
use crate::request_header::RequestHeader;
use crate::response_header::ResponseHeader;
use std::collections::HashMap;

/// Callback invoked when a request completes (response received or disconnected).
///
/// Mirrors `org.apache.kafka.clients.RequestCompletionHandler`.
pub trait RequestCompletionHandler: Send {
    fn on_complete(&mut self, response: ClientResponse);
}

/// A client request with metadata for routing and timeouts.
///
/// Mirrors `org.apache.kafka.clients.ClientRequest`.
pub struct ClientRequest {
    pub destination: String,
    pub correlation_id: i32,
    pub header: RequestHeader,
    pub body: Vec<u8>,
    pub created_time_ms: i64,
    pub expect_response: bool,
    pub request_timeout_ms: i32,
    pub is_internal: bool,
    pub callback: Option<Box<dyn RequestCompletionHandler>>,
}

impl ClientRequest {
    pub fn new(
        destination: impl Into<String>,
        correlation_id: i32,
        header: RequestHeader,
        body: Vec<u8>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
    ) -> Self {
        ClientRequest {
            destination: destination.into(),
            correlation_id,
            header,
            body,
            created_time_ms,
            expect_response,
            request_timeout_ms,
            is_internal: false,
            callback: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_callback(
        destination: impl Into<String>,
        correlation_id: i32,
        header: RequestHeader,
        body: Vec<u8>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Box<dyn RequestCompletionHandler>,
    ) -> Self {
        ClientRequest {
            destination: destination.into(),
            correlation_id,
            header,
            body,
            created_time_ms,
            expect_response,
            request_timeout_ms,
            is_internal: false,
            callback: Some(callback),
        }
    }

    pub fn to_kafka_request(&self) -> KafkaRequest {
        KafkaRequest::new(self.header.clone(), self.body.clone())
    }

    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    pub fn expect_response(&self) -> bool {
        self.expect_response
    }

    pub fn time_elapsed_since_creation_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.created_time_ms).max(0)
    }
}

/// A response to a client request.
///
/// Mirrors `org.apache.kafka.clients.ClientResponse`.
#[derive(Debug)]
pub struct ClientResponse {
    pub header: ResponseHeader,
    pub destination: String,
    pub received_time_ms: i64,
    pub latency_ms: i64,
    pub disconnected: bool,
    pub timed_out: bool,
    pub version_mismatch: Option<String>,
    pub body: Option<KafkaResponse>,
}

impl ClientResponse {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        header: ResponseHeader,
        destination: impl Into<String>,
        created_time_ms: i64,
        received_time_ms: i64,
        disconnected: bool,
        timed_out: bool,
        version_mismatch: Option<String>,
        body: Option<KafkaResponse>,
    ) -> Self {
        ClientResponse {
            header,
            destination: destination.into(),
            received_time_ms,
            latency_ms: (received_time_ms - created_time_ms).max(0),
            disconnected,
            timed_out,
            version_mismatch,
            body,
        }
    }

    pub fn completed(&self) -> bool {
        !self.disconnected && !self.timed_out && self.version_mismatch.is_none()
    }
}

/// An in-flight request awaiting response.
///
/// Mirrors `NetworkClient.InFlightRequest`.
pub struct InFlightRequest {
    pub header: RequestHeader,
    pub destination: String,
    pub request: ClientRequest,
    pub send: KafkaRequest,
    pub send_completed: bool,
    pub send_time_ms: i64,
    pub created_time_ms: i64,
    pub request_timeout_ms: i32,
    pub throttle_time_ms: i64,
    pub expect_response: bool,
    pub is_internal: bool,
}

impl InFlightRequest {
    pub fn new(client_request: ClientRequest, send: KafkaRequest, send_time_ms: i64) -> Self {
        InFlightRequest {
            header: client_request.header.clone(),
            destination: client_request.destination.clone(),
            request_timeout_ms: client_request.request_timeout_ms,
            created_time_ms: client_request.created_time_ms,
            expect_response: client_request.expect_response,
            is_internal: client_request.is_internal,
            send,
            send_time_ms,
            send_completed: false,
            throttle_time_ms: 0,
            request: client_request,
        }
    }

    pub fn time_elapsed_since_send_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.send_time_ms).max(0)
    }

    pub fn time_elapsed_since_create_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.created_time_ms).max(0)
    }

    pub fn maybe_timed_out(&self, current_time_ms: i64) -> bool {
        // Total elapsed time (including throttle) exceeds the request timeout
        let elapsed = self.time_elapsed_since_send_ms(current_time_ms) - self.throttle_time_ms;
        elapsed > self.request_timeout_ms as i64
    }

    pub fn increment_throttle_time(&mut self, throttle_time_ms: i64) {
        self.throttle_time_ms += throttle_time_ms;
    }

    pub fn completed(&self, response: KafkaResponse, received_time_ms: i64) -> ClientResponse {
        ClientResponse::new(
            ResponseHeader::new(response.header.correlation_id),
            &self.destination,
            self.created_time_ms,
            received_time_ms,
            false,
            false,
            None,
            Some(response),
        )
    }

    pub fn timed_out(&self, time_ms: i64) -> ClientResponse {
        ClientResponse::new(
            ResponseHeader::new(self.header.correlation_id),
            &self.destination,
            self.created_time_ms,
            time_ms,
            false,
            true,
            None,
            None,
        )
    }

    pub fn disconnected(&self, time_ms: i64) -> ClientResponse {
        ClientResponse::new(
            ResponseHeader::new(self.header.correlation_id),
            &self.destination,
            self.created_time_ms,
            time_ms,
            true,
            false,
            None,
            None,
        )
    }
}

/// The set of requests that have been sent but not yet received responses.
///
/// Mirrors `NetworkClient.InFlightRequests`.
pub struct InFlightRequests {
    max_in_flight_per_connection: usize,
    requests: HashMap<String, Vec<InFlightRequest>>,
    in_flight_count: usize,
}

impl InFlightRequests {
    pub fn new(max_in_flight_per_connection: usize) -> Self {
        InFlightRequests {
            max_in_flight_per_connection,
            requests: HashMap::new(),
            in_flight_count: 0,
        }
    }

    pub fn add(&mut self, request: InFlightRequest) {
        let destination = request.destination.clone();
        let queue = self.requests.entry(destination).or_default();
        queue.insert(0, request); // addFirst
        self.in_flight_count += 1;
    }

    fn request_queue(&self, node: &str) -> Option<&Vec<InFlightRequest>> {
        self.requests.get(node)
    }

    fn request_queue_mut(&mut self, node: &str) -> Option<&mut Vec<InFlightRequest>> {
        self.requests.get_mut(node)
    }

    /// Complete the oldest request (last in the deque, since we addFirst).
    pub fn complete_next(&mut self, node: &str) -> Option<InFlightRequest> {
        let queue = self.request_queue_mut(node)?;
        if queue.is_empty() {
            return None;
        }
        let req = queue.pop()?;
        self.in_flight_count -= 1;
        Some(req)
    }

    /// Get the last request sent to the given node (but don't remove it).
    pub fn last_sent(&self, node: &str) -> Option<&InFlightRequest> {
        self.request_queue(node)?.first()
    }

    /// Complete the last request that was sent to a particular node.
    pub fn complete_last_sent(&mut self, node: &str) -> Option<InFlightRequest> {
        let queue = self.request_queue_mut(node)?;
        if queue.is_empty() {
            return None;
        }
        let req = queue.remove(0);
        self.in_flight_count -= 1;
        Some(req)
    }

    /// Can we send more requests to this node?
    pub fn can_send_more(&self, node: &str) -> bool {
        match self.request_queue(node) {
            None => true,
            Some(queue) => {
                if queue.is_empty() {
                    true
                } else {
                    let last = &queue[0];
                    last.send_completed && queue.len() < self.max_in_flight_per_connection
                }
            }
        }
    }

    pub fn count(&self, node: &str) -> usize {
        self.request_queue(node).map_or(0, |q| q.len())
    }

    pub fn is_empty(&self, node: &str) -> bool {
        match self.request_queue(node) {
            None => true,
            Some(queue) => queue.is_empty(),
        }
    }

    pub fn total_count(&self) -> usize {
        self.in_flight_count
    }

    pub fn is_globally_empty(&self) -> bool {
        self.requests.values().all(|q| q.is_empty())
    }

    /// Clear all requests for a node, returning them.
    pub fn clear_all(&mut self, node: &str) -> Vec<InFlightRequest> {
        match self.requests.remove(node) {
            Some(reqs) => {
                self.in_flight_count = self.in_flight_count.saturating_sub(reqs.len());
                reqs
            }
            None => Vec::new(),
        }
    }

    /// Find nodes with timed-out requests.
    pub fn nodes_with_timed_out_requests(&self, now: i64) -> Vec<String> {
        let mut timed_out = Vec::new();
        for (node, queue) in &self.requests {
            for req in queue {
                if req.maybe_timed_out(now) {
                    timed_out.push(node.clone());
                    break;
                }
            }
        }
        timed_out
    }

    pub fn increment_throttle_time(&mut self, node_id: &str, throttle_time_ms: i64) {
        if let Some(queue) = self.request_queue_mut(node_id) {
            for req in queue {
                req.increment_throttle_time(throttle_time_ms);
            }
        }
    }
}
