use kafka_common::OffsetTruncationState;
use rstest::rstest;

#[test]
fn test_new() {
    let ts = OffsetTruncationState::new(100, false);
    assert_eq!(ts.offset(), 100);
    assert!(!ts.truncation_completed());
}

#[test]
fn test_new_completed() {
    let ts = OffsetTruncationState::new(100, true);
    assert_eq!(ts.offset(), 100);
    assert!(ts.truncation_completed());
}

#[test]
fn test_with_offset() {
    let ts = OffsetTruncationState::with_offset(500);
    assert_eq!(ts.offset(), 500);
    assert!(ts.truncation_completed());
}

#[test]
fn test_default() {
    let ts = OffsetTruncationState::default();
    assert_eq!(ts.offset(), 0);
    assert!(ts.truncation_completed());
}

#[rstest]
#[case(0, true, 0, true, true)]
#[case(0, true, 0, false, false)]
#[case(100, true, 100, true, true)]
#[case(100, true, 200, true, false)]
#[case(-1, false, -1, false, true)]
fn test_equality(
    #[case] off1: i64,
    #[case] comp1: bool,
    #[case] off2: i64,
    #[case] comp2: bool,
    #[case] expected: bool,
) {
    let a = OffsetTruncationState::new(off1, comp1);
    let b = OffsetTruncationState::new(off2, comp2);
    assert_eq!(a == b, expected);
}

#[test]
fn test_display() {
    let ts = OffsetTruncationState::new(42, false);
    let s = format!("{}", ts);
    assert!(s.contains("offset=42"));
    assert!(s.contains("completed=false"));
}

#[test]
fn test_display_completed() {
    let ts = OffsetTruncationState::new(100, true);
    let s = format!("{}", ts);
    assert!(s.contains("completed=true"));
}
