use kafka_protocol::io::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::io::reader::Readable;
use kafka_protocol::io::writer::Writable;
use kafka_protocol::messages::tagged_fields::RawTaggedField;
use kafka_protocol::messages::tagged_fields::TaggedFields;
use kafka_protocol::MessageContext;

// Test basic creation and properties
#[test]
fn test_tagged_fields_empty() {
    let tf = TaggedFields::empty();
    assert!(tf.is_empty());
    assert_eq!(tf.len(), 0);
    assert!(tf.all_fields().next().is_none());
}

// Test adding a single tagged field
#[test]
fn test_tagged_fields_add_single() {
    let mut tf = TaggedFields::empty();
    tf.add(RawTaggedField::new(1, vec![0xAB, 0xCD]));

    assert_eq!(tf.len(), 1);
    assert!(!tf.is_empty());

    let fields = tf.get(1).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].data().as_ref(), &[0xAB, 0xCD]);
}

// Test adding multiple tagged fields (including same tag)
#[test]
fn test_tagged_fields_add_multiple() {
    let mut tf = TaggedFields::empty();
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
    let mut tf = TaggedFields::empty();
    tf.add(RawTaggedField::new(0, vec![0x01, 0x02]));
    tf.add(RawTaggedField::new(3, vec![0xFF]));
    tf.add(RawTaggedField::new(7, vec![0xA, 0xB, 0xC]));

    let ctx = MessageContext::new(3, true); // flexible context
    let mut writable = ByteBufferAccessor::with_capacity(128);
    Writable::write(&tf, &mut writable, &ctx);

    let data = writable.buffer().to_vec();
    let mut readable = ByteBufferAccessor::from_bytes(data);

    let decoded: TaggedFields = Readable::read::<ByteBufferAccessor>(&mut readable, &ctx).unwrap();
    assert_eq!(decoded.len(), 3);
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
    let tf = TaggedFields::empty();
    assert!(tf.get(99).is_none());
}
