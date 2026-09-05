use kafka_errors::{KafkaError, Retriable};

#[test]
fn test_error_display() {
    let err = KafkaError::CorruptRecord {
        message: "CRC mismatch".into(),
    };
    assert_eq!(err.to_string(), "Corrupt record: CRC mismatch");
}

#[test]
fn test_retriable_errors() {
    assert!(KafkaError::CorruptRecord { message: "test".into() }.retriable());
    assert!(KafkaError::Timeout { timeout_ms: Some(5000) }.retriable());
    assert!(KafkaError::ConnectionFailure { source: None }.retriable());
    assert!(KafkaError::BrokerNotAvailable { source: None }.retriable());
    assert!(KafkaError::OffsetOutOfRange { message: "test".into(), source: None }.retriable());
    assert!(KafkaError::UnknownServer { message: "test".into(), source: None }.retriable());
}

#[test]
fn test_non_retriable_errors() {
    assert!(!KafkaError::InvalidRecord { message: "test".into() }.retriable());
    assert!(!KafkaError::InvalidConfiguration { message: "test".into() }.retriable());
    assert!(!KafkaError::Serialization { message: "test".into(), source: None }.retriable());
    assert!(!KafkaError::InvalidRequest { message: "test".into() }.retriable());
    assert!(!KafkaError::NotLeaderForPartition { message: "test".into() }.retriable());
    assert!(!KafkaError::UnsupportedVersion { message: "test".into() }.retriable());
}

#[test]
fn test_from_io_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "oops");
    let kafka_err: KafkaError = io_err.into();
    assert!(kafka_err.retriable());
    assert!(matches!(kafka_err, KafkaError::Io(_)));
}
