//! Socket server: accepts connections and parses Kafka request framing.
//!
//! Mirrors `org.apache.kafka.common.network.SocketServer` / `SocketServer`.
//!
//! MIGRATION_SOURCE:
//! - core/src/main/scala/kafka/server/SocketServer.scala
//! - clients/src/main/java/org/apache/kafka/common/network/SocketServer.java

use crate::KafkaServer;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::network_receive::NetworkReceive;
use kafka_net::request_header::RequestHeader;
use kafka_server_common::ProcessStatus;
use std::sync::Arc;

/// A single accepted connection.
///
/// Holds the receive buffer state for incremental byte processing.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/network/Selector.java
pub struct KafkaConnection {
    receive: NetworkReceive,
}

impl KafkaConnection {
    /// Create a new connection with unlimited receive size.
    ///
    /// MIGRATION_SOURCE: clients/.../NetworkReceive.java
    pub fn new() -> Self {
        KafkaConnection {
            receive: NetworkReceive::new(),
        }
    }

    /// Feed raw bytes from the network into the receive buffer.
    ///
    /// Returns the number of bytes consumed.
    ///
    /// MIGRATION_SOURCE: clients/.../NetworkReceive.java
    pub fn feed(&mut self, data: &[u8]) -> Result<usize, kafka_net::errors::NetworkError> {
        self.receive.feed(data)
    }

    /// Attempt to parse a complete request header from the received data.
    ///
    /// Returns the parsed header if the receive is complete.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/requests/RequestHeader.java
    pub fn try_parse_request(&self) -> Option<Result<RequestHeader, kafka_net::errors::NetworkError>> {
        if !self.receive.complete() {
            return None;
        }

        let mut buf = self.receive.payload()?;
        Some(RequestHeader::read(&mut buf))
    }

    /// Parse the received data into a full KafkaRequest (header + body).
    ///
    /// Returns the parsed request if the receive is complete.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/clients/NetworkClient.java
    pub fn parse_request(&self) -> Option<KafkaRequest> {
        if !self.receive.complete() {
            return None;
        }

        let mut buf = self.receive.payload()?;
        let header = RequestHeader::read(&mut buf).ok()?;
        let body = buf.to_vec().into_boxed_slice();
        Some(KafkaRequest::new(header, body))
    }

    /// Check if the receive is complete (size header + full payload read).
    pub fn is_complete(&self) -> bool {
        self.receive.complete()
    }

    /// Get the raw payload bytes.
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
    /// Create a new socket server from a KafkaServer.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
    pub fn new(server: Arc<KafkaServer>) -> Self {
        SocketServer { server }
    }

    /// Bind a listener on the given address.
    ///
    /// Returns the bound tokio TcpListener.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/network/SocketServer.java
    pub async fn bind(&self, addr: &str) -> std::io::Result<tokio::net::TcpListener> {
        assert!(self.server.status() == ProcessStatus::Started);
        tokio::net::TcpListener::bind(addr).await
    }

    /// Accept incoming connections in a loop, spawning a handler task per connection.
    ///
    /// Mirrors Java's SocketServer accept loop.
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
/// Mirrors Java's SocketServer.connection handling.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
pub async fn handle_connection(
    socket: &mut tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    state: &crate::ServerState,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut conn = KafkaConnection::new();
    let mut buf = vec![0u8; 1024];

    loop {
        match socket.read(&mut buf).await {
            Ok(0) => {
                println!("Client {} disconnected", peer_addr);
                break;
            }
            Ok(n) => {
                _ = conn.feed(&buf[..n]);
                if conn.is_complete() {
                    if let Some(request) = conn.parse_request() {
                        println!(
                            "Received request: api_key={}, api_version={}, correlation_id={}",
                            request.header.api_key,
                            request.header.api_version,
                            request.header.correlation_id
                        );
                        let response = crate::dispatch_request(&request, state);
                        let _ = socket.write_all(&response).await;
                        // Reset connection for next request on this stream
                        conn = KafkaConnection::new();
                    } else if let Some(Err(e)) = conn.try_parse_request() {
                        eprintln!("Parse error: {}", e);
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
