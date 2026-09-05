use kafka_storage::index::{OffsetIndex, OffsetIndexEntry, TimeIndex, TimeIndexEntry, OFFSET_INDEX_ENTRY_SIZE, TIME_INDEX_ENTRY_SIZE};

#[test]
fn test_offset_index_entry_encode_decode() {
    let entry = OffsetIndexEntry::new(5, 1024);
    let mut buf = [0u8; OFFSET_INDEX_ENTRY_SIZE];
    entry.encode(&mut buf);

    assert_eq!(buf, [0, 0, 0, 5, 0, 0, 4, 0]); // big-endian i32s

    let decoded = OffsetIndexEntry::from_bytes(&buf);
    assert_eq!(decoded, entry);
    assert_eq!(decoded.index_key(), 5);
    assert_eq!(decoded.index_value(), 1024);
}

#[test]
fn test_time_index_entry_encode_decode() {
    let entry = TimeIndexEntry::new(7, 1_700_000_000_000);
    let mut buf = [0u8; TIME_INDEX_ENTRY_SIZE];
    entry.encode(&mut buf);

    // 4 bytes offset + 8 bytes timestamp, all big-endian
    let decoded = TimeIndexEntry::from_bytes(&buf);
    assert_eq!(decoded, entry);
    assert_eq!(decoded.timestamp_ms(), 1_700_000_000_000);
    assert_eq!(decoded.relative_offset(), 7);
}

#[test]
fn test_offset_index_append_and_lookup() {
    let mut idx = OffsetIndex::new(100);
    idx.append(OffsetIndexEntry::new(0, 0)).unwrap();      // offset 100 → pos 0
    idx.append(OffsetIndexEntry::new(5, 512)).unwrap();     // offset 105 → pos 512
    idx.append(OffsetIndexEntry::new(10, 1024)).unwrap();   // offset 110 → pos 1024

    assert_eq!(idx.entries(), 3);
    assert_eq!(idx.as_bytes().len(), 24); // 3 * 8 bytes

    // Lookup offset 105 (relative 5)
    assert_eq!(idx.lookup(5).unwrap(), Some(512));
    // Lookup offset 108 (relative 8) → largest <= 8 is 5
    assert_eq!(idx.lookup(8).unwrap(), Some(512));
    // Lookup offset 99 (relative -1, below base) → no entry
    assert_eq!(idx.lookup(-1).unwrap(), None);
}

#[test]
fn test_offset_index_from_bytes() {
    let mut idx = OffsetIndex::new(0);
    idx.append(OffsetIndexEntry::new(0, 0)).unwrap();
    idx.append(OffsetIndexEntry::new(3, 256)).unwrap();

    let bytes = idx.as_bytes().to_vec();
    let restored = OffsetIndex::from_bytes(0, &bytes).unwrap();
    assert_eq!(restored.entries(), 2);
    assert_eq!(restored.entry(0).unwrap(), OffsetIndexEntry::new(0, 0));
    assert_eq!(restored.entry(1).unwrap(), OffsetIndexEntry::new(3, 256));
}

#[test]
fn test_offset_index_empty_lookup() {
    let idx = OffsetIndex::new(100);
    assert!(idx.is_empty());
    assert_eq!(idx.lookup(0).unwrap(), None);
    assert_eq!(idx.last_offset(), 100);
}

#[test]
fn test_offset_index_corrupt_data() {
    // 7 bytes is not a multiple of 8
    let result = OffsetIndex::from_bytes(0, &[0u8; 7]);
    assert!(result.is_err());
}

#[test]
fn test_time_index_append_and_lookup() {
    let mut idx = TimeIndex::new(0);
    idx.append(TimeIndexEntry::new(0, 1000)).unwrap();    // ts=1000, rel_offset=0
    idx.append(TimeIndexEntry::new(5, 2000)).unwrap();    // ts=2000, rel_offset=5
    idx.append(TimeIndexEntry::new(10, 3000)).unwrap();   // ts=3000, rel_offset=10

    assert_eq!(idx.entries(), 3);
    assert_eq!(idx.as_bytes().len(), 36); // 3 * 12 bytes

    // Lookup timestamp 2000 → rel_offset 5
    assert_eq!(idx.lookup_by_timestamp(2000).unwrap(), Some(5));
    // Lookup timestamp 2500 → largest ts <= 2500 is 2000 → rel_offset 5
    assert_eq!(idx.lookup_by_timestamp(2500).unwrap(), Some(5));
    // Lookup timestamp 500 → no entry with ts <= 500
    assert_eq!(idx.lookup_by_timestamp(500).unwrap(), None);
}

#[test]
fn test_time_index_from_bytes() {
    let mut idx = TimeIndex::new(0);
    idx.append(TimeIndexEntry::new(0, 100)).unwrap();
    idx.append(TimeIndexEntry::new(1, 200)).unwrap();

    let bytes = idx.as_bytes().to_vec();
    let restored = TimeIndex::from_bytes(0, &bytes).unwrap();
    assert_eq!(restored.entries(), 2);
    assert_eq!(restored.entry(0).unwrap(), TimeIndexEntry::new(0, 100));
    assert_eq!(restored.entry(1).unwrap(), TimeIndexEntry::new(1, 200));
}

#[test]
fn test_time_index_corrupt_data() {
    // 13 bytes is not a multiple of 12
    let result = TimeIndex::from_bytes(0, &[0u8; 13]);
    assert!(result.is_err());
}

#[test]
fn test_offset_index_last_offset() {
    let mut idx = OffsetIndex::new(1000);
    assert_eq!(idx.last_offset(), 1000);

    idx.append(OffsetIndexEntry::new(0, 0)).unwrap();
    assert_eq!(idx.last_offset(), 1000);

    idx.append(OffsetIndexEntry::new(50, 512)).unwrap();
    assert_eq!(idx.last_offset(), 1050);
}

#[test]
fn test_offset_index_byte_format_compatibility() {
    // Verify the exact byte format matches Java's OffsetIndex:
    // 8 bytes per entry: 4-byte relative offset + 4-byte position, big-endian
    let entry = OffsetIndexEntry::new(42, 9999);
    let mut buf = [0u8; 8];
    entry.encode(&mut buf);
    assert_eq!(buf, [0, 0, 0, 42, 0, 0, 0x27, 0x0F]); // 42 and 9999 in big-endian
}

#[test]
fn test_time_index_byte_format_compatibility() {
    // Verify the exact byte format matches Java's TimeIndex:
    // 12 bytes per entry: 4-byte relative offset + 8-byte timestamp, big-endian
    let entry = TimeIndexEntry::new(1, 1000);
    let mut buf = [0u8; 12];
    entry.encode(&mut buf);
    assert_eq!(buf, [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 3, 0xE8]); // 1 and 1000 in big-endian
}
