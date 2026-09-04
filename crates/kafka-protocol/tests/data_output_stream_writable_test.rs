// Tests mirroring clients/.../protocol/DataOutputStreamWritableTest.java
// MIGRATION_SOURCE: clients/src/test/java/org/apache/kafka/common/protocol/DataOutputStreamWritableTest.java

use kafka_protocol::byte_buffer_accessor::ByteBufferAccessor;
use kafka_protocol::writable::Writable;

#[test]
fn test_writing_sliced_byte_buffer() {
    let expected_array = [2, 3, 0, 0];
    let source_buffer = [0u8, 1, 2, 3];
    let mut result = ByteBufferAccessor::with_capacity(4);

    // Move position forward to ensure slice is not whole buffer
    let slice = &source_buffer[2..];
    result.write_byte_buffer(slice);

    assert_eq!(2, result.position(), "Writing to the buffer moves the position forward");
    assert_eq!(expected_array.to_vec(), result.buffer().to_vec(), "Result buffer should have expected elements");
}

#[test]
fn test_writing_sliced_byte_buffer_with_nonzero_position() {
    let expected_array = [3, 0, 0, 0];
    let source_buffer = [0u8, 1, 2, 3];
    let mut result = ByteBufferAccessor::with_capacity(4);

    // Move forward to ensure slice starts at offset 2
    let slice = &source_buffer[3..];
    result.write_byte_buffer(slice);

    assert_eq!(1, result.position(), "Writing to the buffer moves the position forward");
    assert_eq!(expected_array.to_vec(), result.buffer().to_vec(), "Result buffer should have expected elements");
}
