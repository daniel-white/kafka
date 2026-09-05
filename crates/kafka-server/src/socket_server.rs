//! Socket server: accepts connections and parses Kafka request framing.
//!
//! Mirrors `org.apache.kafka.common.network.SocketServer` / `SocketServer`.
//!
//! MIGRATION_SOURCE:
//! - core/src/main/scala/kafka/server/SocketServer.scala
//! - clients/src/main/java/org/apache/kafka/common/network/SocketServer.java

use crate::{KafkaServer, ProcessStatus};
use kafka_net::network_receive::NetworkReceive;
use kafka_net::request_header::RequestHeader;
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
        loop {
            match listener.accept().await {
                Ok((mut socket, peer_addr)) => {
                    println!("Accepted connection from {}", peer_addr);
                    tokio::spawn(async move {
                        handle_connection(&mut socket, peer_addr).await;
                    });
                }
                Err(e) => eprintln!("Accept error: {}", e),
            }
        }
    }
}

/// Handle a single TCP connection: read bytes, parse Kafka request framing.
///
/// Mirrors Java's SocketServer.connection handling.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/SocketServer.scala
pub async fn handle_connection(
    socket: &mut tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
) {
    use tokio::io::AsyncReadExt;

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
                    if let Some(result) = conn.try_parse_request() {
                        match result {
                            Ok(header) => println!(
                                "Received request: api_key={}, api_version={}, correlation_id={}",
                                header.api_key, header.api_version, header.correlation_id
                            ),
                            Err(e) => eprintln!("Parse error: {}", e),
                        }
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
