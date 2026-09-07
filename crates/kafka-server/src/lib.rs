//! Kafka broker server entry point.
//!
//! Provides KafkaServer lifecycle management and request routing.
//!
//! MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala

use kafka_server_common::ProcessStatus;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use fast_stm::TVar;

pub mod handlers;
pub mod request_handler;
pub mod server_state;
pub mod socket_server;

pub use handlers::{build_empty_response, build_response_frame, dispatch};
pub use server_state::{ServerPartition, ServerState, TopicMetadata};
pub use socket_server::{KafkaConnection, SocketServer};

/// Server lifecycle state, managed atomically.
///
/// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
#[derive(Debug, Clone)]
pub struct KafkaServer {
    /// Process lifecycle status.
    status: Arc<AtomicU8>,
    /// Broker-level state (topics, partitions) — accessed via TVar for
    /// lock-free reads. Writers use write_atomic() for atomic replacement.
    state: TVar<ServerState>,
}

impl KafkaServer {
    /// Create a new KafkaServer that starts in SHUTDOWN state.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn new() -> Self {
        KafkaServer {
            status: Arc::new(AtomicU8::new(ProcessStatus::Shutdown as u8)),
            state: TVar::new(ServerState::new()),
        }
    }

    /// Transition to STARTING state.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn startup(&self) {
        self.status
            .store(ProcessStatus::Starting as u8, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        self.status
            .store(ProcessStatus::Started as u8, Ordering::SeqCst);
    }

    /// Transition to SHUTTING_DOWN then SHUTDOWN.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn shutdown(&self) {
        self.status
            .store(ProcessStatus::ShuttingDown as u8, Ordering::SeqCst);
        self.status
            .store(ProcessStatus::Shutdown as u8, Ordering::SeqCst);
    }

    /// Get the current process status.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn status(&self) -> ProcessStatus {
        match self.status.load(Ordering::SeqCst) {
            0 => ProcessStatus::Shutdown,
            1 => ProcessStatus::Starting,
            2 => ProcessStatus::Started,
            3 => ProcessStatus::ShuttingDown,
            _ => ProcessStatus::Unknown,
        }
    }

    /// Check if the server is running.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn is_running(&self) -> bool {
        self.status() == ProcessStatus::Started
    }

    /// Get a clone of the server's state handle for lock-free access.
    ///
    /// Readers use .read_atomic() to atomically read the current state.
    /// Writers use .write_atomic(new_state) to atomically replace state.
    ///
    /// MIGRATION_SOURCE: core/src/main/scala/kafka/server/KafkaBroker.scala
    pub fn state(&self) -> TVar<ServerState> {
        self.state.clone()
    }
}

impl Default for KafkaServer {
    fn default() -> Self {
        KafkaServer::new()
    }
}
