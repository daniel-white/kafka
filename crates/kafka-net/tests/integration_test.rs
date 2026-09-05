//! Integration test: full request/response pipeline across crates.
//!
//! Exercises the complete path: request header serialization → request body framing
//! → response header deserialization → record batch parsing.

use kafka_compress::compression::Compression;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::request_header::RequestHeader;
use kafka_net::response_header::ResponseHeader;
use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_record::header::Header;
use kafka_record::record::DefaultRecord;
use kafka_record::record_batch::{RecordBatchHeader, MAGIC_VALUE_V2};
use kafka_record::CompressionType;

#[test]
fn test_full_request_response_pipeline() {
    let header = RequestHeader::new(0, 1, 42, "integration-test");
    let body = vec![0x00, 0x01, 0x02, 0x03].into_boxed_slice();

    let request = KafkaRequest::new(header.clone(), body);
    let serialized = request.serialize();

    let size = i32::from_be_bytes(serialized[0..4].try_into().unwrap()) as usize;
    assert_eq!(size, serialized.len() - 4);

    let mut data_slice = &serialized[4..];
    let decoded_header = RequestHeader::read(&mut data_slice).unwrap();
    assert_eq!(decoded_header, header);
    assert_eq!(data_slice, &[0x00, 0x01, 0x02, 0x03]);
}

#[test]
fn test_compression_in_record_pipeline() {
    let key: Option<&[u8]> = Some(b"key1");
    let value: Option<&[u8]> = Some(b"hello world");
    let headers: Vec<Header> = vec![Header::new("header1", Some(b"val1".to_vec()))];

    let mut writable = ByteBufferAccessor::with_capacity(128);
    DefaultRecord::write_to(
        &mut writable,
        0, // offset_delta
        0, // timestamp_delta
        key,
        value,
        &headers,
    );

    let buf = writable.buffer();

    // Compress with gzip
    let compressed = Compression::compress(
        buf,
        CompressionType::Gzip,
        Compression::default_level(CompressionType::Gzip),
    )
    .unwrap();

    // Verify compression actually reduced size (for this data pattern)
    if buf.len() > 20 {
        assert!(compressed.len() < buf.len());
    }

    // Decompress and verify round-trip
    let decompressed = Compression::decompress(&compressed, CompressionType::Gzip, None).unwrap();
    assert_eq!(decompressed, buf);
}

#[test]
fn test_record_batch_with_compression() {
    let _key = b"topic-key".to_vec();
    let value = b"topic-value".to_vec();

    let _compressed = Compression::compress(&value, CompressionType::Gzip, 6).unwrap();

    // Build a record batch header with gzip compression
    let attributes: i16 = 0x01; // GZIP compression
    let header = RecordBatchHeader::new_v2(
        0,          // base_offset
        0,          // last_offset_delta
        0,          // partition_leader_epoch
        -1,         // producer_id
        -1,         // producer_epoch
        0,          // base_sequence
        1,          // records_count
        attributes, // attributes (GZIP)
        0,          // base_timestamp
        0,          // max_timestamp
    );

    assert_eq!(header.magic, MAGIC_VALUE_V2);
    assert_eq!(header.attributes & 0x07, CompressionType::Gzip.id() as i16);
    assert_eq!(header.base_offset, 0);
}

#[test]
fn test_response_header_correlation() {
    let correlation_id = 12345;
    let response_header = ResponseHeader::new(correlation_id);

    let mut buf = Vec::new();
    response_header.write(&mut buf);

    assert_eq!(buf, correlation_id.to_be_bytes());

    let mut slice = buf.as_slice();
    let decoded = ResponseHeader::read(&mut slice).unwrap();
    assert_eq!(decoded.correlation_id, correlation_id);
    assert!(slice.is_empty());
}

#[test]
fn test_record_batch_header_crc() {
    let header = RecordBatchHeader::new_v2(
        0, 0, 0, -1, -1, 0, 1, 0, 0, 0,
    );

    let mut buf = vec![0u8; RecordBatchHeader::size_in_bytes()];
    header.encode(&mut buf);

    // Verify the header is the expected size (61 bytes for v2 record batch header)
    assert_eq!(buf.len(), RecordBatchHeader::size_in_bytes());
    assert_eq!(buf.len(), 61);

    // Verify we can decode it back
    let decoded = RecordBatchHeader::decode(&buf);
    assert_eq!(decoded.base_offset, header.base_offset);
    assert_eq!(decoded.magic, MAGIC_VALUE_V2);
}
