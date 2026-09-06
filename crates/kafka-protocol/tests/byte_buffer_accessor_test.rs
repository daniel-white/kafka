// Tests mirroring clients/.../protocol/ByteBufferAccessorTest.java
// MIGRATION_SOURCE: clients/src/test/java/org/apache/kafka/common/protocol/ByteBufferAccessorTest.java

use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::reader::Reader;
use kafka_protocol::writer::Writer;

#[test]
fn test_read_array() {
    let mut accessor = ByteBufferAccessor::with_capacity(1024);
    let test_array = [0x4b, 0x61, 0x46];
    accessor.write_byte_array(&test_array);
    accessor.write_int(12345);
    accessor.flip();

    let test_array2 = accessor.read_array(3).unwrap();
    assert_eq!(test_array.to_vec(), test_array2);
    assert_eq!(12345, accessor.read_int().unwrap());

    let err = accessor.read_array(3).unwrap_err();
    assert_eq!(
        "Error reading byte array of 3 byte(s): only 0 byte(s) available",
        err.to_string()
    );
}

#[test]
fn test_read_string() {
    let mut accessor = ByteBufferAccessor::with_capacity(1024);
    let test_string = "ABC";
    accessor.write_byte_array(test_string.as_bytes());
    accessor.flip();

    assert_eq!("ABC", accessor.read_string(3).unwrap());
    let err = accessor.read_string(2).unwrap_err();
    assert_eq!(
        "Error reading byte array of 2 byte(s): only 0 byte(s) available",
        err.to_string()
    );
}
