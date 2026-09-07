use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_record::compression_type::CompressionType;
use kafka_record::header::Header;
use kafka_record::memory_records::MemoryRecords;
use kafka_record::record::DefaultRecord;
use kafka_record::record_batch::{
    self, RecordBatchHeader, ATTRIBUTES_OFFSET, CRC_LENGTH, CRC_OFFSET, RECORD_BATCH_OVERHEAD,
};
use kafka_record::DefaultRecordBatch;

fn write_record(
    offset_delta: i32,
    timestamp_delta: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[Header],
) -> Vec<u8> {
    let mut acc = ByteBufferAccessor::with_capacity(512);
    let written = DefaultRecord::write_to(&mut acc, offset_delta, timestamp_delta, key, value, headers);
    acc.flip();
    let bytes = acc.buffer().to_vec();
    assert_eq!(bytes.len(), written as usize);
    bytes
}

#[test]
fn test_record_bytes_empty() {
    let bytes = write_record(0, 0, None, None, &[]);
    // body = 1(attr) + 1(varint 0) + 1(varlong 0) + 1(varint -1) + 1(varint -1) + 1(varint 0 hdrs) = 6
    // size prefix: varint(6) → zigzag(6)=12 → 0x0C
    // total = 1 + 6 = 7
    assert_eq!(bytes, vec![0x0C, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00]);
}

#[test]
fn test_record_bytes_with_key_value() {
    let bytes = write_record(0, 0, Some(b"hello"), Some(b"world"), &[]);
    // body = 1(attr) + 1(varint 0) + 1(varlong 0) + 2(varint(5)→zigzag=10) + 5(hello)
    //      + 2(varint(5)→10) + 5(world) + 1(varint 0 hdrs) = 16
    // size prefix: varint(16) → zigzag(16)=32 → 0x20
    assert_eq!(
        bytes,
        vec![
            0x20, // varint(16) → zigzag(16)=32
            0x00, // attributes
            0x00, // varlong(0) timestamp delta
            0x00, // varint(0) offset delta
            0x0A, // varint(5) → zigzag(5)=10
            b'h', b'e', b'l', b'l', b'o',
            0x0A, // varint(5) → zigzag(5)=10
            b'w', b'o', b'r', b'l', b'd',
            0x00, // headers count = 0
        ]
    );
}

#[test]
fn test_record_bytes_with_timestamp_offset_delta() {
    let bytes = write_record(7, 128, Some(b"k"), Some(b"v"), &[]);
    // body: 1(attr) + varint(7→14=0x0E) + varlong(128→zigzag=256=[0x80,0x02])
    //       + varint(1→2=0x02) + 1 byte + varint(1→2=0x02) + 1 byte + varint(0→0)
    //       = 1 + 1 + 2 + 2 + 2 + 1 = 9
    // size prefix: varint(9) → zigzag(9)=18 → 0x12
    assert_eq!(
        bytes,
        vec![
            0x12, // varint(9) → zigzag(9)=18
            0x00,                  // attributes
            0x80, 0x02,            // varlong(128): zigzag(128)=256
            0x0E,                  // varint(7): zigzag(7)=14
            0x02, b'k',            // key length 1 → zigzag(1)=2
            0x02, b'v',            // value length 1 → zigzag(1)=2
            0x00,                  // headers count = 0
        ]
    );
}

#[test]
fn test_record_round_trip_with_key_value_headers() {
    let key = Some(b"my-key".as_slice());
    let value = Some(b"my-value".as_slice());
    let headers = vec![
        Header::new("content-type", Some(b"application/json".to_vec())),
        Header::new("correlation-id", Some(b"abc-123".to_vec())),
    ];

    let offset_delta = 5i32;
    let timestamp_delta = 1000i64;
    let base_offset = 100i64;
    let base_timestamp = 1_700_000_000_000i64;
    let base_sequence = 0i32;

    // Compute size for assertion
    let expected_size = DefaultRecord::size_in_bytes_for(
        offset_delta,
        timestamp_delta,
        key,
        value,
        &headers,
    );

    // Write
    let mut accessor = ByteBufferAccessor::with_capacity(256);
    let written = DefaultRecord::write_to(
        &mut accessor,
        offset_delta,
        timestamp_delta,
        key,
        value,
        &headers,
    );
    assert_eq!(written, expected_size);

    accessor.flip();

    // Read back
    let record = DefaultRecord::read_from(
        &mut accessor,
        base_offset,
        base_timestamp,
        base_sequence,
        None,
    )
    .expect("record should parse");

    assert_eq!(record.offset(), base_offset + offset_delta as i64);
    assert_eq!(record.timestamp(), base_timestamp + timestamp_delta);
    assert_eq!(record.sequence(), base_sequence + offset_delta);
    assert_eq!(record.key(), key);
    assert_eq!(record.value(), value);
    assert_eq!(record.headers().len(), 2);
    assert_eq!(record.headers()[0].key(), "content-type");
    assert_eq!(
        record.headers()[0].value(),
        Some(b"application/json".as_slice())
    );
    assert_eq!(record.headers()[1].key(), "correlation-id");
    assert_eq!(
        record.headers()[1].value(),
        Some(b"abc-123".as_slice())
    );
    assert_eq!(record.size_in_bytes(), expected_size);
}

#[test]
fn test_record_null_key_and_value() {
    let key: Option<&[u8]> = None;
    let value: Option<&[u8]> = None;
    let headers: Vec<Header> = vec![];

    let mut accessor = ByteBufferAccessor::with_capacity(128);
    let written = DefaultRecord::write_to(&mut accessor, 0, 0, key, value, &headers);
    accessor.flip();

    let record = DefaultRecord::read_from(&mut accessor, 0, 0, 0, None).unwrap();
    assert_eq!(record.key_size(), -1);
    assert_eq!(record.value_size(), -1);
    assert_eq!(record.headers().len(), 0);
    assert_eq!(record.size_in_bytes(), written);
}

#[test]
fn test_record_empty_headers() {
    let key = Some(b"k".as_slice());
    let value = Some(b"v".as_slice());
    let headers: Vec<Header> = vec![];

    let mut accessor = ByteBufferAccessor::with_capacity(128);
    DefaultRecord::write_to(&mut accessor, 0, 0, key, value, &headers);
    accessor.flip();

    let record = DefaultRecord::read_from(&mut accessor, 0, 0, 0, None).unwrap();
    assert_eq!(record.key(), Some(b"k".as_ref()));
    assert_eq!(record.value(), Some(b"v".as_ref()));
    assert_eq!(record.headers().len(), 0);
}

#[test]
fn test_record_with_null_header_value() {
    let headers = vec![Header::new("h1", None)];
    let mut accessor = ByteBufferAccessor::with_capacity(128);
    DefaultRecord::write_to(&mut accessor, 0, 0, None, None, &headers);
    accessor.flip();

    let record = DefaultRecord::read_from(&mut accessor, 0, 0, 0, None).unwrap();
    assert_eq!(record.headers().len(), 1);
    assert_eq!(record.headers()[0].value(), None);
}

#[test]
fn test_record_size_computation() {
    let key = Some(b"key".as_slice());
    let value = Some(b"value".to_vec());
    let headers = vec![Header::new("h", Some(b"v".to_vec()))];

    let size = DefaultRecord::size_in_bytes_for(3, 50, key, value.as_deref(), &headers);

    let mut accessor = ByteBufferAccessor::with_capacity(256);
    let written = DefaultRecord::write_to(&mut accessor, 3, 50, key, value.as_deref(), &headers);
    assert_eq!(written, size);
}

#[test]
fn test_record_batch_header_encode_decode() {
    let header = RecordBatchHeader::new_v2(
        100,
        5,
        0,
        12345,
        7,
        0,
        3,
        0,
        1_700_000_000_000,
        1_700_000_000_000 + 50,
    );

    let mut buf = vec![0u8; RECORD_BATCH_OVERHEAD];
    header.encode(&mut buf);

    let decoded = RecordBatchHeader::decode(&buf);
    assert_eq!(decoded.base_offset, 100);
    assert_eq!(decoded.last_offset_delta, 5);
    assert_eq!(decoded.partition_leader_epoch, 0);
    assert_eq!(decoded.magic, record_batch::MAGIC_VALUE_V2);
    assert_eq!(decoded.producer_id, 12345);
    assert_eq!(decoded.producer_epoch, 7);
    assert_eq!(decoded.base_sequence, 0);
    assert_eq!(decoded.records_count, 3);
    assert_eq!(decoded.base_timestamp, 1_700_000_000_000);
    assert_eq!(decoded.max_timestamp, 1_700_000_000_000 + 50);
}

#[test]
fn test_increment_sequence() {
    assert_eq!(DefaultRecord::increment_sequence(0, 5), 5);
    assert_eq!(DefaultRecord::increment_sequence(10, 3), 13);
    assert_eq!(DefaultRecord::increment_sequence(-1, 0), -1);
}

#[test]
fn test_decrement_sequence() {
    assert_eq!(DefaultRecord::decrement_sequence(10, 3), 7);
    assert_eq!(DefaultRecord::decrement_sequence(0, 0), 0);
    assert_eq!(DefaultRecord::decrement_sequence(-1, 5), -1);
}

#[test]
fn test_record_has_magic() {
    let mut accessor = ByteBufferAccessor::with_capacity(64);
    let headers: Vec<Header> = vec![];
    DefaultRecord::write_to(&mut accessor, 0, 0, None, None, &headers);
    accessor.flip();
    let record = DefaultRecord::read_from(&mut accessor, 0, 0, 0, None).unwrap();
    assert!(record.has_magic(2));
    assert!(record.has_magic(3));
    assert!(!record.has_magic(1));
    assert!(!record.is_compressed());
}

#[test]
fn test_record_batch_crc_valid() {
    // Create a simple empty record batch (no records)
    let batch = DefaultRecordBatch::create(
        0,   // base_offset
        0,   // last_offset_delta
        0,   // partition_leader_epoch
        -1,  // producer_id
        -1,  // producer_epoch
        -1,  // base_sequence
        0,   // records_count
        0,   // attributes
        0,   // base_timestamp
        0,   // max_timestamp
        &[], // no records
    );

    assert!(batch.validate());
    assert_eq!(batch.size_in_bytes(), RECORD_BATCH_OVERHEAD);
}

#[test]
fn test_record_batch_crc_corrupted() {
    let batch = DefaultRecordBatch::create(
        0, 0, 0, -1, -1, -1, 0, 0, 1000, 1000, &[],
    );
    assert!(batch.validate());

    let mut corrupted = batch.bytes().to_vec();
    // Corrupt a byte in the attributes area (after CRC, within the CRC-covered region)
    corrupted[ATTRIBUTES_OFFSET] ^= 0xFF;
    let corrupted_batch = DefaultRecordBatch::from_bytes(corrupted);
    assert!(!corrupted_batch.validate());
}

#[test]
fn test_record_batch_with_records() {
    // Create a single record
    let key = Some(b"key".as_slice());
    let value = Some(b"value".as_slice());
    let headers: Vec<Header> = vec![];
    let mut acc = ByteBufferAccessor::with_capacity(256);
    let written = DefaultRecord::write_to(&mut acc, 0, 0, key, value, &headers);
    acc.flip();
    let record_bytes = acc.buffer().to_vec();
    assert_eq!(written as usize, record_bytes.len());

    let batch = DefaultRecordBatch::create(
        100,  // base_offset
        0,    // last_offset_delta (single record, delta 0)
        0,    // partition_leader_epoch
        1,    // producer_id
        0,    // producer_epoch
        0,    // base_sequence
        1,    // records_count
        0,    // attributes
        1_000, // base_timestamp
        1_000, // max_timestamp
        &record_bytes,
    );

    assert!(batch.validate(), "batch CRC should be valid");
    assert_eq!(batch.size_in_bytes(), RECORD_BATCH_OVERHEAD + record_bytes.len());

    // Decode and verify header fields
    let header = batch.header();
    assert_eq!(header.base_offset, 100);
    assert_eq!(header.magic, record_batch::MAGIC_VALUE_V2);
    assert_eq!(header.producer_id, 1);
    assert_eq!(header.records_count, 1);
    assert_eq!(header.base_timestamp, 1_000);
    assert_eq!(header.max_timestamp, 1_000);
}

#[test]
fn test_record_batch_length_field() {
    // The Length field stores sizeInBytes - LOG_OVERHEAD
    let batch = DefaultRecordBatch::create(
        0, 0, 0, -1, -1, -1, 0, 0, 0, 0,
        &[0x0C, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00], // one empty record (7 bytes)
    );
    assert!(batch.validate());

    let header = batch.header();
    // Length = total_size - LOG_OVERHEAD = (RECORD_BATCH_OVERHEAD + 7) - 12
    assert_eq!(header.length, (RECORD_BATCH_OVERHEAD + 7 - 12) as i32);
}

#[test]
fn test_memory_records_iterate_multiple_batches() {
    // Create two batches concatenated into one buffer
    let batch1 = DefaultRecordBatch::create(0, 0, 0, -1, -1, -1, 0, 0, 100, 100, &[]);
    let batch2 = DefaultRecordBatch::create(5, 0, 0, -1, -1, -1, 0, 0, 200, 200, &[]);

    let mut buffer = Vec::new();
    buffer.extend_from_slice(batch1.bytes());
    buffer.extend_from_slice(batch2.bytes());

    let records = MemoryRecords::from_bytes(buffer);
    let batches: Vec<_> = records.batches().collect();
    assert_eq!(batches.len(), 2);

    let b1 = batches[0].as_ref().unwrap();
    assert!(b1.validate());
    assert_eq!(b1.header().base_offset, 0);
    assert_eq!(b1.header().base_timestamp, 100);

    let b2 = batches[1].as_ref().unwrap();
    assert!(b2.validate());
    assert_eq!(b2.header().base_offset, 5);
    assert_eq!(b2.header().base_timestamp, 200);
}

#[test]
fn test_memory_records_single_batch_with_record() {
    let key = Some(b"k".as_slice());
    let value = Some(b"v".as_slice());
    let mut acc = ByteBufferAccessor::with_capacity(256);
    let _written = DefaultRecord::write_to(&mut acc, 0, 0, key, value, &[]);
    acc.flip();
    let record_bytes = acc.buffer().to_vec();

    let batch = DefaultRecordBatch::create(0, 0, 0, -1, -1, -1, 0, 0, 0, 0, &record_bytes);

    let records = MemoryRecords::from_bytes(batch.bytes().to_vec());
    let batches: Vec<_> = records.batches().collect();
    assert_eq!(batches.len(), 1);
    assert!(batches[0].as_ref().unwrap().validate());
}

#[test]
fn test_compression_type_for_id() {
    assert_eq!(CompressionType::for_id(0).unwrap(), CompressionType::None);
    assert_eq!(CompressionType::for_id(1).unwrap(), CompressionType::Gzip);
    assert_eq!(CompressionType::for_id(2).unwrap(), CompressionType::Snappy);
    assert_eq!(CompressionType::for_id(3).unwrap(), CompressionType::Lz4);
    assert_eq!(CompressionType::for_id(4).unwrap(), CompressionType::Zstd);
    assert!(CompressionType::for_id(99).is_err());
}

#[test]
fn test_compression_type_for_name() {
    assert_eq!(CompressionType::for_name("none").unwrap(), CompressionType::None);
    assert_eq!(CompressionType::for_name("gzip").unwrap(), CompressionType::Gzip);
    assert_eq!(CompressionType::for_name("zstd").unwrap(), CompressionType::Zstd);
    assert!(CompressionType::for_name("unknown").is_err());
}

#[test]
fn test_compression_type_attributes() {
    assert_eq!(CompressionType::None.id(), 0);
    assert_eq!(CompressionType::Gzip.id(), 1);
    assert_eq!(CompressionType::Zstd.name(), "zstd");
    assert!(CompressionType::None.is_none());
    assert!(!CompressionType::Gzip.is_none());
}

#[test]
fn test_compression_levels() {
    assert_eq!(CompressionType::Gzip.default_level(), -1);
    assert_eq!(CompressionType::Gzip.max_level(), 9);
    assert_eq!(CompressionType::Gzip.min_level(), 1);

    assert_eq!(CompressionType::Lz4.default_level(), 9);
    assert_eq!(CompressionType::Lz4.max_level(), 17);
    assert_eq!(CompressionType::Lz4.min_level(), 1);

    assert_eq!(CompressionType::Zstd.default_level(), 3);
    assert_eq!(CompressionType::Zstd.max_level(), 22);
    assert_eq!(CompressionType::Zstd.min_level(), -131072);
}

#[test]
fn test_memory_records_empty() {
    let records = MemoryRecords::from_bytes(vec![]);
    let batches: Vec<_> = records.batches().collect();
    assert_eq!(batches.len(), 0);
    assert!(records.is_empty());
}

#[test]
fn test_crc32c_standard_vector() {
    // CRC32C of "123456789" must be 0xE3069283 (Castagnoli polynomial, same as Java's Crc32C)
    assert_eq!(crc32c::crc32c(b"123456789"), 0xE3069283);
    // CRC32C of empty data is 0
    assert_eq!(crc32c::crc32c(b""), 0);
}

#[test]
fn test_record_batch_crc_matches_java_calculation() {
    // Verify CRC is computed over ATTRIBUTES_OFFSET..end, matching Java:
    // Crc32C.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET)
    let batch = DefaultRecordBatch::create(
        100,  // base_offset
        0,    // last_offset_delta
        3,    // partition_leader_epoch
        999,  // producer_id
        1,    // producer_epoch
        0,    // base_sequence
        1,    // records_count
        0,    // attributes
        1_234_567, // base_timestamp
        1_234_567, // max_timestamp
        &[0x0C, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00], // 7 bytes of record data
    );

    assert!(batch.validate(), "CRC should match for valid batch");

    // Manually verify: compute CRC over ATTRIBUTES_OFFSET..end
    let buf = batch.bytes();
    let stored_crc = u32::from_be_bytes(
        buf[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].try_into().unwrap(),
    );
    let crc_input = &buf[ATTRIBUTES_OFFSET..];
    let computed_crc = crc32c::crc32c(crc_input);
    assert_eq!(
        stored_crc, computed_crc,
        "CRC should match Java's computation over ATTRIBUTES_OFFSET..end"
    );
}

