use kafka_metadata::{ApiMessageAndVersion, MetadataRecordSerde, MetadataRecordType, RecordSerde};
use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::writable::Writable;

fn build_message(data: Vec<u8>, api_key: i16, version: i16) -> ApiMessageAndVersion {
    ApiMessageAndVersion::new(data, api_key, version)
}

// --- MetadataRecordType tests ---

#[test]
fn test_record_type_from_api_key() {
    assert_eq!(
        MetadataRecordType::from_api_key(0),
        Some(MetadataRecordType::RegisterBroker)
    );
    assert_eq!(
        MetadataRecordType::from_api_key(2),
        Some(MetadataRecordType::Topic)
    );
    assert_eq!(
        MetadataRecordType::from_api_key(3),
        Some(MetadataRecordType::Partition)
    );
    assert_eq!(
        MetadataRecordType::from_api_key(20),
        Some(MetadataRecordType::NoOp)
    );
    assert_eq!(MetadataRecordType::from_api_key(6), None);
    assert_eq!(MetadataRecordType::from_api_key(99), None);
    assert_eq!(MetadataRecordType::from_api_key(-1), None);
}

#[test]
fn test_record_type_api_key_round_trip() {
    for key in [0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 12, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29] {
        let rt = MetadataRecordType::from_api_key(key).expect("known key");
        assert_eq!(rt.api_key(), key, "round-trip for key {}", key);
    }
}

#[test]
fn test_record_type_default_is_register_broker() {
    assert_eq!(
        MetadataRecordType::default(),
        MetadataRecordType::RegisterBroker
    );
}

// --- ApiMessageAndVersion tests ---

#[test]
fn test_api_message_and_version_accessors() {
    let data = vec![0x01, 0x02, 0x03];
    let msg = build_message(data.clone(), 2, 0);
    assert_eq!(msg.api_key(), 2);
    assert_eq!(msg.version(), 0);
    assert_eq!(msg.data(), &data);
}

#[test]
fn test_api_message_and_version_record_type() {
    let msg = build_message(vec![], 2, 0);
    assert_eq!(
        msg.record_type().unwrap(),
        MetadataRecordType::Topic
    );

    let msg_unknown = build_message(vec![], 99, 0);
    assert!(msg_unknown.record_type().is_err());
}

#[test]
fn test_api_message_and_version_default() {
    let msg = ApiMessageAndVersion::default();
    assert_eq!(msg.api_key(), 0);
    assert_eq!(msg.version(), 0);
    assert!(msg.data().is_empty());
}

// --- MetadataRecordSerde round-trip tests ---

#[test]
fn test_serde_round_trip_with_body() {
    let body = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let msg = build_message(body.clone(), 2, 0);

    // Write
    let mut writable = Vec::new();
    MetadataRecordSerde.write(&msg, &mut writable);

    // Read back
    let mut readable = ByteBufferAccessor::from_bytes(writable.clone());
    let decoded = MetadataRecordSerde.read(&mut readable, writable.len())
        .expect("read succeeds");

    assert_eq!(decoded, msg);
}

#[test]
fn test_serde_round_trip_empty_body() {
    let msg = build_message(vec![], 20, 0);

    let mut writable = ByteBufferAccessor::with_capacity(32);
    MetadataRecordSerde.write(&msg, &mut writable);
    writable.flip();

    let buf = writable.buffer().to_vec();
    let mut readable = ByteBufferAccessor::from_bytes(buf.clone());
    let decoded = MetadataRecordSerde.read(&mut readable, buf.len())
        .expect("read succeeds");

    assert_eq!(decoded, msg);
}

#[test]
fn test_serde_record_size() {
    let body = vec![0x01, 0x02, 0x03, 0x04, 0x05];
    let msg = build_message(body, 3, 1);

    let mut writable = Vec::new();
    MetadataRecordSerde.write(&msg, &mut writable);

    assert_eq!(MetadataRecordSerde.record_size(&msg), writable.len());
}

#[test]
fn test_serde_invalid_frame_version() {
    // Build bytes with frame_version = 2 (invalid; only 1 is supported)
    let mut acc = kafka_protocol::byte_buffer_accessor::ByteBufferAccessor::with_capacity(64);
    acc.write_unsigned_varint(2); // invalid frame version
    acc.write_unsigned_varint(2); // api_key = Topic
    acc.write_unsigned_varint(0); // version = 0
    acc.write_byte_buffer(&[0xFF]);
    acc.flip();

    let buf = acc.buffer().to_vec();
    let mut readable = ByteBufferAccessor::from_bytes(buf.clone());
    let result = MetadataRecordSerde.read(&mut readable, buf.len());
    assert!(result.is_err());
}

#[test]
fn test_serde_unknown_api_key() {
    let msg = build_message(vec![0x00], 99, 0);

    let mut writable = Vec::new();
    MetadataRecordSerde.write(&msg, &mut writable);

    let mut readable = ByteBufferAccessor::from_bytes(writable.clone());
    let decoded = MetadataRecordSerde.read(&mut readable, writable.len()).expect("read still succeeds");

    assert_eq!(decoded.api_key(), 99);
    assert!(decoded.record_type().is_err());
}
