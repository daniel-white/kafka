use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::types::Type;

fn make_accessor(capacity: usize) -> ByteBufferAccessor {
    ByteBufferAccessor::with_capacity(capacity)
}

#[test]
fn test_bool_round_trip() {
    let mut acc = make_accessor(16);
    Type::Boolean.write_bool(&mut acc, true);
    Type::Boolean.write_bool(&mut acc, false);
    Type::Boolean.write_bool(&mut acc, true);
    acc.flip();
    assert_eq!(Type::Boolean.read_bool(&mut acc).unwrap(), true);
    assert_eq!(Type::Boolean.read_bool(&mut acc).unwrap(), false);
    assert_eq!(Type::Boolean.read_bool(&mut acc).unwrap(), true);
    assert_eq!(Type::Boolean.size_of_bool(), 1);
}

#[test]
fn test_int_primitives() {
    let mut acc = make_accessor(32);
    Type::Int8.write_int8(&mut acc, -128);
    Type::Int16.write_int16(&mut acc, -32768);
    Type::UInt16.write_uint16(&mut acc, 65535);
    Type::Int32.write_int32(&mut acc, -1);
    Type::UnsignedInt32.write_uint32(&mut acc, 0xFFFFFFFF);
    Type::Int64.write_int64(&mut acc, -9_223_372_036_854_775_808);
    acc.flip();

    assert_eq!(Type::Int8.read_int8(&mut acc).unwrap(), -128);
    assert_eq!(Type::Int16.read_int16(&mut acc).unwrap(), -32768);
    assert_eq!(Type::UInt16.read_uint16(&mut acc).unwrap(), 65535);
    assert_eq!(Type::Int32.read_int32(&mut acc).unwrap(), -1);
    assert_eq!(Type::UnsignedInt32.read_uint32(&mut acc).unwrap(), 0xFFFFFFFF);
    assert_eq!(Type::Int64.read_int64(&mut acc).unwrap(), -9_223_372_036_854_775_808);
}

#[test]
fn test_uuid_round_trip() {
    use kafka_common::uuid::Uuid;
    let uuid = Uuid::new(0x123456789ABCDEF0, -1);
    let mut acc = make_accessor(32);
    Type::Uuid.write_uuid(&mut acc, &uuid);
    acc.flip();
    let read_back = Type::Uuid.read_uuid(&mut acc).unwrap();
    assert_eq!(uuid, read_back);
    assert_eq!(Type::Uuid.size_of_uuid(), 16);
}

#[test]
fn test_string_round_trip() {
    let mut acc = make_accessor(256);
    Type::String.write_string(&mut acc, "hello");
    Type::CompactString.write_compact_string(&mut acc, "world");
    Type::NullableString.write_nullable_string(&mut acc, None);
    Type::NullableString.write_nullable_string(&mut acc, Some("test"));
    Type::CompactNullableString.write_compact_nullable_string(&mut acc, None);
    Type::CompactNullableString.write_compact_nullable_string(&mut acc, Some("data"));
    acc.flip();

    assert_eq!(Type::String.read_string(&mut acc).unwrap(), "hello");
    assert_eq!(Type::CompactString.read_compact_string(&mut acc).unwrap(), "world");
    assert_eq!(Type::NullableString.read_nullable_string(&mut acc).unwrap(), None);
    assert_eq!(Type::NullableString.read_nullable_string(&mut acc).unwrap(), Some("test".to_string()));
    assert_eq!(Type::CompactNullableString.read_compact_nullable_string(&mut acc).unwrap(), None);
    assert_eq!(Type::CompactNullableString.read_compact_nullable_string(&mut acc).unwrap(), Some("data".to_string()));
}

#[test]
fn test_bytes_round_trip() {
    let mut acc = make_accessor(256);
    Type::Bytes.write_bytes(&mut acc, &[0x01, 0x02, 0x03]);
    Type::CompactBytes.write_compact_bytes(&mut acc, &[0x04, 0x05]);
    Type::NullableBytes.write_nullable_bytes(&mut acc, None);
    Type::NullableBytes.write_nullable_bytes(&mut acc, Some(&[0x06, 0x07]));
    Type::CompactNullableBytes.write_compact_nullable_bytes(&mut acc, None);
    Type::CompactNullableBytes.write_compact_nullable_bytes(&mut acc, Some(&[0x08]));
    acc.flip();

    assert_eq!(Type::Bytes.read_bytes(&mut acc).unwrap(), vec![0x01, 0x02, 0x03]);
    assert_eq!(Type::CompactBytes.read_compact_bytes(&mut acc).unwrap(), vec![0x04, 0x05]);
    assert_eq!(Type::NullableBytes.read_nullable_bytes(&mut acc).unwrap(), None);
    assert_eq!(Type::NullableBytes.read_nullable_bytes(&mut acc).unwrap(), Some(vec![0x06, 0x07]));
    assert_eq!(Type::CompactNullableBytes.read_compact_nullable_bytes(&mut acc).unwrap(), None);
    assert_eq!(Type::CompactNullableBytes.read_compact_nullable_bytes(&mut acc).unwrap(), Some(vec![0x08]));
}

#[test]
fn test_varint_varlong_round_trip() {
    let mut acc = make_accessor(64);
    Type::Varint.write_varint(&mut acc, -1);
    Type::Varint.write_varint(&mut acc, 0);
    Type::Varint.write_varint(&mut acc, 300);
    Type::Varlong.write_varlong(&mut acc, -1);
    Type::Varlong.write_varlong(&mut acc, 0);
    Type::Varlong.write_varlong(&mut acc, 1_000_000);
    acc.flip();

    assert_eq!(Type::Varint.read_varint(&mut acc).unwrap(), -1);
    assert_eq!(Type::Varint.read_varint(&mut acc).unwrap(), 0);
    assert_eq!(Type::Varint.read_varint(&mut acc).unwrap(), 300);
    assert_eq!(Type::Varlong.read_varlong(&mut acc).unwrap(), -1);
    assert_eq!(Type::Varlong.read_varlong(&mut acc).unwrap(), 0);
    assert_eq!(Type::Varlong.read_varlong(&mut acc).unwrap(), 1_000_000);
}

#[test]
fn test_float64_round_trip() {
    let mut acc = make_accessor(32);
    Type::Float64.write_float64(&mut acc, 3.14159);
    acc.flip();
    assert!((Type::Float64.read_float64(&mut acc).unwrap() - 3.14159).abs() < 1e-10);
}

#[test]
fn test_type_names() {
    assert_eq!(Type::Int8.type_name(), "INT8");
    assert_eq!(Type::Int16.type_name(), "INT16");
    assert_eq!(Type::Int32.type_name(), "INT32");
    assert_eq!(Type::Int64.type_name(), "INT64");
    assert_eq!(Type::String.type_name(), "STRING");
    assert_eq!(Type::Uuid.type_name(), "UUID");
    assert_eq!(Type::Varint.type_name(), "VARINT");
    assert_eq!(Type::Varlong.type_name(), "VARLONG");
    assert_eq!(Type::Bytes.type_name(), "BYTES");
}

#[test]
fn test_is_nullable() {
    assert!(!Type::String.is_nullable());
    assert!(Type::NullableString.is_nullable());
    assert!(Type::CompactNullableString.is_nullable());
    assert!(Type::NullableBytes.is_nullable());
    assert!(!Type::Int32.is_nullable());
}

#[test]
fn test_string_size() {
    assert_eq!(Type::String.size_of_string("hello"), 7); // 2 (length) + 5 (bytes)
    assert_eq!(Type::Int32.size_of_int32(), 4);
    assert_eq!(Type::Int64.size_of_int64(), 8);
    assert_eq!(Type::Uuid.size_of_uuid(), 16);
}

#[test]
fn test_compact_string_encoding() {
    let s = "AB";
    let compact_size = Type::CompactString.size_of_compact_string(s);
    let mut acc = make_accessor(64);
    Type::CompactString.write_compact_string(&mut acc, s);
    acc.flip();
    // Compact string: varint(len+1) + bytes
    // "AB" = 2 bytes, so varint(3) = 1 byte, total = 1 + 2 = 3
    let bytes = acc.buffer();
    assert_eq!(bytes.len(), compact_size);
    assert_eq!(bytes[0], 3); // varint(2+1)
    assert_eq!(&bytes[1..], b"AB");
}
