//! Tests for ApiVersionsResponse serialization round-trip.

use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::io::reader::Readable;
use kafka_protocol::io::sizing::compute_size;
use kafka_protocol::io::writer::Writable;
use kafka_protocol::messages::api_versions::{ApiVersionEntry, ApiVersionsResponse};
use kafka_protocol::MessageContext;

fn write_and_read(response: &ApiVersionsResponse, api_version: i16, flexible: bool) -> ApiVersionsResponse {
    let ctx = MessageContext::new(api_version, flexible);

    let mut buf = Vec::new();
    Writable::write(response, &mut buf, &ctx);
    assert_eq!(buf.len(), compute_size(response, &ctx));

    let mut reader = ByteBufferAccessor::from_bytes(buf);
    Readable::read::<ByteBufferAccessor>(&mut reader, &ctx).unwrap()
}

#[test]
fn test_api_versions_response_v0_non_flexible() {
    let entries = vec![
        ApiVersionEntry::new(3, 0, 13),  // Metadata
        ApiVersionEntry::new(18, 0, 5),  // ApiVersions
    ];
    let response = ApiVersionsResponse::new(0, entries, 0);

    let decoded = write_and_read(&response, 0, false);

    assert_eq!(decoded.error_code(), 0);
    assert_eq!(decoded.api_keys().len(), 2);
    assert_eq!(decoded.api_keys()[0].api_key(), 3);
    assert_eq!(decoded.api_keys()[0].min_version(), 0);
    assert_eq!(decoded.api_keys()[0].max_version(), 13);
    assert_eq!(decoded.throttle_time_ms(), 0);
}

#[test]
fn test_api_versions_response_v1_with_throttle() {
    let entries = vec![ApiVersionEntry::new(18, 0, 5)];
    let response = ApiVersionsResponse::new(0, entries, 150);

    let decoded = write_and_read(&response, 1, false);
    assert_eq!(decoded.throttle_time_ms(), 150);
}

#[test]
fn test_api_versions_response_v3_flexible() {
    let entries = vec![
        ApiVersionEntry::new(0, 3, 13),
        ApiVersionEntry::new(1, 0, 16),
        ApiVersionEntry::new(3, 0, 13),
        ApiVersionEntry::new(18, 0, 5),
    ];
    let response = ApiVersionsResponse::new(0, entries, 0);

    let decoded = write_and_read(&response, 3, true);
    assert_eq!(decoded.api_keys().len(), 4);
    assert_eq!(decoded.api_keys()[0].api_key(), 0);
    assert_eq!(decoded.api_keys()[3].api_key(), 18);
    assert_eq!(decoded.throttle_time_ms(), 0);
}

#[test]
fn test_api_versions_response_empty() {
    let response = ApiVersionsResponse::new(0, vec![], 0);

    let decoded = write_and_read(&response, 0, false);
    assert_eq!(decoded.api_keys().len(), 0);
}

#[test]
fn test_api_versions_response_v5() {
    let entries = vec![ApiVersionEntry::new(18, 0, 5)];
    let response = ApiVersionsResponse::new(0, entries, 200);

    let decoded = write_and_read(&response, 5, true);
    assert_eq!(decoded.api_keys().len(), 1);
    assert_eq!(decoded.throttle_time_ms(), 200);
}

#[test]
fn test_api_version_entry_sizes() {
    let entry = ApiVersionEntry::new(3, 0, 13);

    let ctx_v0 = MessageContext::new(0, false);
    assert_eq!(compute_size(&entry, &ctx_v0), 6);

    let ctx_v3 = MessageContext::new(3, false);
    assert_eq!(compute_size(&entry, &ctx_v3), 7);
}

#[test]
fn test_api_versions_response_byte_layout_v0() {
    let entries = vec![ApiVersionEntry::new(18, 0, 5)];
    let response = ApiVersionsResponse::new(0, entries, 0);

    let ctx = MessageContext::new(0, false);
    let mut buf = Vec::new();
    Writable::write(&response, &mut buf, &ctx);

    // v0: error_code(2) + array_count(4) + api_key(2) + min(2) + max(2) = 12
    assert_eq!(buf.len(), 12);
    assert_eq!(i16::from_be_bytes([buf[0], buf[1]]), 0); // ErrorCode = 0
    assert_eq!(i32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]), 1); // Count = 1
    assert_eq!(i16::from_be_bytes([buf[6], buf[7]]), 18); // ApiKey
    assert_eq!(i16::from_be_bytes([buf[8], buf[9]]), 0); // MinVersion
    assert_eq!(i16::from_be_bytes([buf[10], buf[11]]), 5); // MaxVersion
}

#[test]
fn test_api_versions_response_byte_layout_v3() {
    let entries = vec![ApiVersionEntry::new(18, 0, 5)];
    let response = ApiVersionsResponse::new(0, entries, 0);

    let ctx = MessageContext::new(3, true);
    let mut buf = Vec::new();
    Writable::write(&response, &mut buf, &ctx);

    // v3 flexible: error_code(2) + compact_array_count(1 varint) + api_key(2) + min(2) + max(2)
    // + entry_tags(1) + throttle(4) + top_tags(1) = 15
    assert_eq!(buf.len(), 15);
}
