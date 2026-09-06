//! Socket server: accepts connections and parses Kafka request framing.
//!
//! Mirrors `org.apache.kafka.common.network.SocketServer` / `SocketServer`.
//!
//! MIGRATION_SOURCE:
//! - core/src/main/scala/kafka/server/SocketServer.scala
//! - clients/src/main/java/org/apache/kafka/common/network/SocketServer.java

use crate::KafkaServer;
use crate::handlers::dispatch;
use crate::server_state::ServerState;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::network_receive::NetworkReceive;
use kafka_net::request_header::RequestHeader;
use kafka_server_common::ProcessStatus;
use std::sync::{Arc, RwLock};

/// A single accepted connection.
///
/// Holds the receive buffer state for incremental byte processing.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/network/Selector.java
pub struct KafkaConnection {
    receive: NetworkReceive,
}

impl KafkaConnection {
    pub fn new() -> Self {
        KafkaConnection {
            receive: NetworkReceive::new(),
        }
    }

    /// Feed raw bytes from the network into the receive buffer.
    ///
    /// MIGRATION_SOURCE: clients/.../NetworkReceive.java
    pub fn feed(&mut self, data: &[u8]) -> Result<usize, kafka_net::errors::NetworkError> {
        self.receive.feed(data)
    }

    /// Attempt to parse a complete request header from the received data.
    ///
    /// MIGRATION_SOURCE: clients/.../RequestHeader.java
    pub fn try_parse_request(&self) -> Option<Result<RequestHeader, kafka_net::errors::NetworkError>> {
        if !self.receive.complete() {
            return None;
        }
        let buf = self.receive.payload()?;
        Some(RequestHeader::read(buf).map(|(header, _)| header))
    }

    /// Parse the received data into a full KafkaRequest (header + body).
    ///
    /// MIGRATION_SOURCE: clients/.../NetworkClient.java
    pub fn parse_request(&self) -> Option<KafkaRequest> {
        if !self.receive.complete() {
            return None;
        }
        let buf = self.receive.payload()?;
        let (header, body) = RequestHeader::read(buf).ok()?;
        let body = body.to_vec().into_boxed_slice();
        Some(KafkaRequest::new(header, body))
    }

    pub fn is_complete(&self) -> bool {
        self.receive.complete()
    }

    pub fn payload(&self) -> Option<&[u8]> {
        self.receive.payload()
    }
}

impl Default for KafkaConnection {
    fn default() -> Self {
        Self::new()
    }
}

/// Socket server that accepts connections and processes them.
///
/// Wraps a TCP listener and spawns tasks per connection.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
pub struct SocketServer {
    server: Arc<KafkaServer>,
}

impl SocketServer {
    pub fn new(server: Arc<KafkaServer>) -> Self {
        SocketServer { server }
    }

    /// Bind a listener on the given address.
    ///
    /// MIGRATION_SOURCE: clients/.../SocketServer.java
    pub async fn bind(&self, addr: &str) -> std::io::Result<tokio::net::TcpListener> {
        assert!(self.server.status() == ProcessStatus::Started);
        tokio::net::TcpListener::bind(addr).await
    }

    /// Accept incoming connections in a loop, spawning a handler task per connection.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
    pub async fn accept_loop(&self, listener: tokio::net::TcpListener) {
        let state = self.server.state().clone();
        loop {
            match listener.accept().await {
                Ok((mut socket, peer_addr)) => {
                    println!("Accepted connection from {}", peer_addr);
                    let state = state.clone();
                    tokio::spawn(async move {
                        handle_connection(&mut socket, peer_addr, &state).await;
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }
}

/// Handle a single TCP connection: read bytes, parse Kafka request framing,
/// and send a response back.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
pub async fn handle_connection(
    socket: &mut tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    state: &Arc<RwLock<ServerState>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut conn = KafkaConnection::new();
    let mut buf = vec![0u8; 65536];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) => {
                println!("Client {} disconnected", peer_addr);
                break;
            }
            Ok(n) => {
                eprintln!("DEBUG: read {} bytes", n);
                let mut offset = 0;
                loop {
                    let result = conn.feed(&buf[offset..n]);
                    match &result {
                        Ok(consumed) => {
                            eprintln!("DEBUG: consumed {} bytes, complete={}", consumed, conn.is_complete());
                            offset += consumed;
                        }
                        Err(e) => {
                            eprintln!("DEBUG: feed error: {:?}", e);
                            break;
                        }
                    }

                    if !conn.is_complete() {
                        break;
                    }

                    if let Some(request) = conn.parse_request() {
                        eprintln!(
                            "Received request: api_key={}, api_version={}, correlation_id={}",
                            request.header.api_key,
                            request.header.api_version,
                            request.header.correlation_id
                        );
                        eprintln!("DEBUG: body {} bytes: {:02x?}", request.body.len(), &request.body[0..request.body.len().min(40)]);
                        let response = dispatch(&request, state);
                        eprintln!("DEBUG: response {} bytes", response.len());
                        let _ = socket.write_all(&response).await;
                    } else {
                        // Log the raw payload for debugging
                        if let Some(payload) = conn.payload() {
                            eprintln!("DEBUG: parse_request returned None, payload {} bytes: {:02x?}", payload.len(), &payload[..payload.len().min(40)]);
                        }
                    }

                    conn = KafkaConnection::new();
                    if offset >= n {
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Read error: {}", e);
                break;
            }
        }
    }
}
