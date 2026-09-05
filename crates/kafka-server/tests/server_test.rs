use kafka_server::KafkaServer;
use kafka_server_common::ProcessStatus;

#[test]
fn test_server_lifecycle() {
    let server = KafkaServer::new();
    assert_eq!(server.status(), ProcessStatus::Shutdown);

    server.startup();
    assert_eq!(server.status(), ProcessStatus::Started);
    assert!(server.is_running());

    server.shutdown();
    assert_eq!(server.status(), ProcessStatus::Shutdown);
    assert!(!server.is_running());
}

#[test]
fn test_server_clone() {
    let server = KafkaServer::new();
    let cloned = server.clone();

    server.startup();
    // Cloned shares state via Arc<AtomicU8>
    assert_eq!(cloned.status(), ProcessStatus::Started);

    server.shutdown();
    assert_eq!(cloned.status(), ProcessStatus::Shutdown);
}
