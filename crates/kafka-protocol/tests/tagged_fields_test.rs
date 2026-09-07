use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::messages::tagged_fields::RawTaggedField;
use kafka_protocol::messages::tagged_fields::TaggedFields;

// Test basic creation and properties
#[test]
fn test_tagged_fields_empty() {
    let tf = TaggedFields::new();
    assert!(tf.is_empty());
    assert_eq!(tf.len(), 0);
    assert!(tf.all_fields().next().is_none());
}

// Test adding a single tagged field
#[test]
fn test_tagged_fields_add_single() {
    let mut tf = TaggedFields::new();
    tf.add(RawTaggedField::new(1, vec![0xAB, 0xCD]));

    assert_eq!(tf.len(), 1);
    assert!(!tf.is_empty());

    let fields = tf.get(1).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(&**fields[0].data(), &[0xAB, 0xCD]);
}

// Test adding multiple tagged fields (including same tag)
#[test]
fn test_tagged_fields_add_multiple() {
    let mut tf = TaggedFields::new();
    tf.add(RawTaggedField::new(1, vec![0x01]));
    tf.add(RawTaggedField::new(2, vec![0x02]));
    tf.add(RawTaggedField::new(1, vec![0x03])); // same tag, different data

    assert_eq!(tf.len(), 2); // 2 distinct tags

    let tag_1_fields = tf.get(1).unwrap();
    assert_eq!(tag_1_fields.len(), 2);

    let tag_2_fields = tf.get(2).unwrap();
    assert_eq!(tag_2_fields.len(), 1);
}

// Test round-trip: write → read
#[test]
fn test_tagged_fields_round_trip() {
    let mut tf = TaggedFields::new();
    tf.add(RawTaggedField::new(0, vec![0x01, 0x02]));
    tf.add(RawTaggedField::new(3, vec![0xFF]));
    tf.add(RawTaggedField::new(7, vec![0xA, 0xB, 0xC]));

    let mut writable = ByteBufferAccessor::with_capacity(128);
    tf.write_to(&mut writable);

    let data = writable.buffer().to_vec();
    let mut readable = ByteBufferAccessor::from_bytes(data);

    let decoded = TaggedFields::read_from(&mut readable).unwrap();
    assert_eq!(decoded, tf);
}

// Test size calculation matches written bytes
#[test]
fn test_tagged_fields_size_matches_write() {
    let mut tf = TaggedFields::new();
    tf.add(RawTaggedField::new(1, vec![0x01, 0x02, 0x03]));
    tf.add(RawTaggedField::new(5, vec![0xFF, 0xEE]));

    let mut writable = ByteBufferAccessor::with_capacity(128);
    tf.write_to(&mut writable);
    writable.flip();

    assert_eq!(writable.buffer().len(), tf.size());
}

// Test empty tagged fields size
#[test]
fn test_empty_tagged_fields_size() {
    let tf = TaggedFields::empty();
    // Just the count varint (1 byte for 0)
    assert_eq!(tf.size(), 1);
}

// Test writing empty tagged fields
#[test]
fn test_write_empty_tagged_fields() {
    let tf = TaggedFields::empty();
    let mut buf = Vec::new();
    tf.write_to(&mut buf);

    // Should write a single 0x00 byte (count = 0 as unsigned varint)
    assert_eq!(buf, &[0x00]);
}

// Test reading empty tagged fields
#[test]
fn test_read_empty_tagged_fields() {
    let data = [0x00]; // count = 0
    let mut readable = ByteBufferAccessor::from_bytes(data.to_vec());
    let tf = TaggedFields::read_from(&mut readable).unwrap();
    assert!(tf.is_empty());
    assert_eq!(tf.len(), 0);
}

// Test reading fields from a raw byte buffer (no ByteBufferAccessor)
#[test]
fn test_tagged_fields_write_read_raw() {
    let mut tf = TaggedFields::new();
    tf.add(RawTaggedField::new(0, vec![0x01]));
    tf.add(RawTaggedField::new(1, vec![0x02, 0x03]));

    let mut buf = Vec::new();
    tf.write_to(&mut buf);

    // Wire format:
    // count (varint) = 2 → 0x02
    // tag 0 (varint) = 0 → 0x00
    // size (varint) = 1 → 0x01
    // data = 0x01
    // tag 1 (varint) = 1 → 0x01
    // size (varint) = 2 → 0x02
    // data = 0x02 0x03
    assert_eq!(buf, vec![0x02, 0x00, 0x01, 0x01, 0x01, 0x02, 0x02, 0x03]);
}

// Test Default impl
#[test]
fn test_tagged_fields_default() {
    let tf = TaggedFields::default();
    assert!(tf.is_empty());
}

// Test get with non-existent tag
#[test]
fn test_tagged_fields_get_nonexistent() {
    let tf = TaggedFields::new();
    assert!(tf.get(99).is_none());
}
