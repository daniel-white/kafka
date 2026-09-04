// Tests mirroring clients/.../utils/internals/ByteUtilsTest.java (selected)
// MIGRATION_SOURCE: clients/src/test/java/org/apache/kafka/common/utils/internals/ByteUtilsTest.java

use kafka_protocol::byte_utils;
use std::io::Cursor;

fn assert_unsigned_varint_serde(value: u32, expected: &[u8]) {
    let mut buf = Vec::new();
    byte_utils::write_unsigned_varint(value, &mut buf).unwrap();
    assert_eq!(expected, buf.as_slice());
    let mut cursor = Cursor::new(buf.clone());
    assert_eq!(value, byte_utils::read_unsigned_varint(&mut cursor).unwrap());
}

#[test]
fn test_unsigned_varint_serde() {
    assert_unsigned_varint_serde(0, &[0x00]);
    assert_unsigned_varint_serde(0xFFFFFFFF, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_unsigned_varint_serde(1, &[0x01]);
    assert_unsigned_varint_serde(63, &[0x3F]);
    assert_unsigned_varint_serde(0xFFFFFFC0, &[0xC0, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_unsigned_varint_serde(64, &[0x40]);
    assert_unsigned_varint_serde(8191, &[0xFF, 0x3F]);
    assert_unsigned_varint_serde(0xFFFFE000, &[0x80, 0xC0, 0xFF, 0xFF, 0x0F]);
    assert_unsigned_varint_serde(8192, &[0x80, 0x40]);
    assert_unsigned_varint_serde(0xFFFFDFFF, &[0xFF, 0xBF, 0xFF, 0xFF, 0x0F]);
    assert_unsigned_varint_serde(1048575, &[0xFF, 0xFF, 0x3F]);
    assert_unsigned_varint_serde(1048576, &[0x80, 0x80, 0x40]);
    assert_unsigned_varint_serde(0x7FFFFFFF, &[0xFF, 0xFF, 0xFF, 0xFF, 0x07]);
    assert_unsigned_varint_serde(0x80000000, &[0x80, 0x80, 0x80, 0x80, 0x08]);
}

fn assert_varint_serde(value: i32, expected: &[u8]) {
    let mut buf = Vec::new();
    byte_utils::write_varint(value, &mut buf).unwrap();
    assert_eq!(expected, buf.as_slice());
    let mut cursor = Cursor::new(buf.clone());
    assert_eq!(value, byte_utils::read_varint(&mut cursor).unwrap());
}

#[test]
fn test_varint_serde() {
    assert_varint_serde(0, &[0x00]);
    assert_varint_serde(-1, &[0x01]);
    assert_varint_serde(1, &[0x02]);
    assert_varint_serde(63, &[0x7E]);
    assert_varint_serde(-64, &[0x7F]);
    assert_varint_serde(64, &[0x80, 0x01]);
    assert_varint_serde(-65, &[0x81, 0x01]);
    assert_varint_serde(8191, &[0xFE, 0x7F]);
    assert_varint_serde(-8192, &[0xFF, 0x7F]);
    assert_varint_serde(8192, &[0x80, 0x80, 0x01]);
    assert_varint_serde(-8193, &[0x81, 0x80, 0x01]);
    assert_varint_serde(1048575, &[0xFE, 0xFF, 0x7F]);
    assert_varint_serde(-1048576, &[0xFF, 0xFF, 0x7F]);
    assert_varint_serde(1048576, &[0x80, 0x80, 0x80, 0x01]);
    assert_varint_serde(-1048577, &[0x81, 0x80, 0x80, 0x01]);
    assert_varint_serde(134217727, &[0xFE, 0xFF, 0xFF, 0x7F]);
    assert_varint_serde(-134217728, &[0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varint_serde(134217728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varint_serde(-134217729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
    assert_varint_serde(i32::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_varint_serde(i32::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
}

fn assert_varlong_serde(value: i64, expected: &[u8]) {
    let mut buf = Vec::new();
    byte_utils::write_varlong(value, &mut buf).unwrap();
    assert_eq!(expected, buf.as_slice());
    let mut cursor = Cursor::new(buf.clone());
    assert_eq!(value, byte_utils::read_varlong(&mut cursor).unwrap());
}

#[test]
fn test_varlong_serde() {
    assert_varlong_serde(0, &[0x00]);
    assert_varlong_serde(-1, &[0x01]);
    assert_varlong_serde(1, &[0x02]);
    assert_varlong_serde(63, &[0x7E]);
    assert_varlong_serde(-64, &[0x7F]);
    assert_varlong_serde(64, &[0x80, 0x01]);
    assert_varlong_serde(-65, &[0x81, 0x01]);
    assert_varlong_serde(8191, &[0xFE, 0x7F]);
    assert_varlong_serde(-8192, &[0xFF, 0x7F]);
    assert_varlong_serde(8192, &[0x80, 0x80, 0x01]);
    assert_varlong_serde(-8193, &[0x81, 0x80, 0x01]);
    assert_varlong_serde(1048575, &[0xFE, 0xFF, 0x7F]);
    assert_varlong_serde(-1048576, &[0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(1048576, &[0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-1048577, &[0x81, 0x80, 0x80, 0x01]);
    assert_varlong_serde(134217727, &[0xFE, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-134217728, &[0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(134217728, &[0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-134217729, &[0x81, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(i32::MAX as i64, &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_varlong_serde(i32::MIN as i64, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    assert_varlong_serde(17179869183, &[0xFE, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-17179869184, &[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(17179869184, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-17179869185, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(2199023255551, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-2199023255552, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(2199023255552, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-2199023255553, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(281474976710655, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-281474976710656, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(281474976710656, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-281474976710657, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(36028797018963967, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-36028797018963968, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(36028797018963968, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-36028797018963969, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(4611686018427387903, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(-4611686018427387904, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    assert_varlong_serde(4611686018427387904, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(-4611686018427387905, &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
    assert_varlong_serde(i64::MAX, &[0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
    assert_varlong_serde(i64::MIN, &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
}

#[test]
fn test_invalid_varint() {
    // varint encoding with too many continuation bytes
    let buf = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(byte_utils::read_unsigned_varint(&mut cursor).is_err());
}

#[test]
fn test_invalid_varlong() {
    let buf = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
    let mut cursor = Cursor::new(&buf[..]);
    assert!(byte_utils::read_unsigned_varlong(&mut cursor).is_err());
}

fn assert_double_serde(value: f64, expected_bytes: &[u8]) {
    let mut buf = Vec::new();
    byte_utils::write_double(value, &mut buf).unwrap();
    assert_eq!(expected_bytes, buf.as_slice());
    let mut cursor = Cursor::new(&buf);
    let read_val = byte_utils::read_double(&mut cursor).unwrap();
    if value.is_nan() {
        assert!(read_val.is_nan(), "expected NaN round-trip");
    } else {
        assert_eq!(value, read_val);
    }
}

#[test]
fn test_double() {
    assert_double_serde(0.0, &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(-0.0, &[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(1.0, &[0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(-1.0, &[0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(f64::from_bits(1), &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
    assert_double_serde(f64::MAX, &[0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    assert_double_serde(f64::NEG_INFINITY, &[0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(f64::INFINITY, &[0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    assert_double_serde(f64::NAN, &[0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
}

#[test]
fn test_size_of_unsigned_varint() {
    // Compare against a simple reference implementation
    let simple = |value: u32| -> usize {
        let mut bytes = 1;
        let mut v = value;
        while (v & 0xFFFFFF80) != 0 {
            bytes += 1;
            v >>= 7;
        }
        bytes
    };
    for i in (0..10_000_000u32).step_by(13) {
        assert_eq!(simple(i), byte_utils::size_of_unsigned_varint(i));
    }
}

#[test]
fn test_size_of_varlong() {
    let simple = |value: i64| -> usize {
        let mut v = ((value << 1) ^ (value >> 63)) as u64;
        let mut bytes = 1;
        while (v & 0xFFFFFFFFFFFFFF80) != 0 {
            bytes += 1;
            v >>= 7;
        }
        bytes
    };
    let mut val: i64 = 1;
    while val > 0 {
        assert_eq!(simple(val), byte_utils::size_of_varlong(val));
        val <<= 1;
    }
    assert_eq!(simple(0), byte_utils::size_of_varlong(0));
}
