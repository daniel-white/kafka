// Test for UUID base64 URL encoding round-trip.
// MIGRATION_SOURCE: clients/src/test/java/org/apache/kafka/common/UuidTest.java

use kafka_common::uuid::Uuid;

#[test]
fn test_uuid_base64_round_trip() {
    // ONE_UUID = new Uuid(0L, 1L) in Java
    let uuid = Uuid::ONE_UUID;
    let s = uuid.to_string();
    let recovered = Uuid::from_string(&s).unwrap();
    assert_eq!(uuid, recovered);
}

#[test]
fn test_zero_uuid() {
    let uuid = Uuid::ZERO_UUID;
    assert_eq!(0, uuid.get_most_significant_bits());
    assert_eq!(0, uuid.get_least_significant_bits());
    let s = uuid.to_string();
    let recovered = Uuid::from_string(&s).unwrap();
    assert_eq!(uuid, recovered);
}

#[test]
fn test_uuid_from_string() {
    let uuid = Uuid::from_string("AAAAAAAAAAAAAAAAAAAAAA").unwrap();
    assert_eq!(0, uuid.get_most_significant_bits());
    assert_eq!(0, uuid.get_least_significant_bits());
}

#[test]
fn test_uuid_new() {
    let uuid = Uuid::new(0x123456789ABCDEF0, 0xFEDCBA9876543210u64 as i64);
    assert_eq!(0x123456789ABCDEF0i64, uuid.get_most_significant_bits());
    assert_eq!(0xFEDCBA9876543210u64 as i64, uuid.get_least_significant_bits());
    let s = uuid.to_string();
    let recovered = Uuid::from_string(&s).unwrap();
    assert_eq!(uuid, recovered);
}

#[test]
fn test_uuid_comparison() {
    let a = Uuid::new(1, 2);
    let b = Uuid::new(1, 3);
    assert!(a < b);
    assert_eq!(a.cmp(&a), std::cmp::Ordering::Equal);
}
