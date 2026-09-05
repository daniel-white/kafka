//! Walking skeleton: starts broker, binds listener, accepts connections,
//! parses request framing (size header + RequestHeader).

use kafka_server::{KafkaServer, SocketServer};
use kafka_server_common::ProcessStatus;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let server = Arc::new(KafkaServer::new());

    println!("Kafka broker starting...");
    server.startup();
    println!("Server status: {:?}", server.status());
    assert!(server.status() == ProcessStatus::Started);

    let socket_server = SocketServer::new(server.clone());
    let listener = match socket_server.bind("127.0.0.1:9092").await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind: {}", e);
            server.shutdown();
            std::process::exit(1);
        }
    };

    println!("Listening on 127.0.0.1:9092");
    println!("Kafka broker RUNNING");

    tokio::spawn(async move {
        socket_server.accept_loop(listener).await;
    });

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
