use kafka_server_common::PersisterStateBatch;
use rstest::rstest;
use std::cmp::Ordering;

#[test]
fn test_new() {
    let batch = PersisterStateBatch::new(10, 20, 1, 3);
    assert_eq!(batch.first_offset(), 10);
    assert_eq!(batch.last_offset(), 20);
    assert_eq!(batch.delivery_state(), 1);
    assert_eq!(batch.delivery_count(), 3);
}

#[test]
fn test_default() {
    let batch = PersisterStateBatch::default();
    assert_eq!(batch.first_offset(), 0);
    assert_eq!(batch.last_offset(), 0);
    assert_eq!(batch.delivery_state(), 0);
    assert_eq!(batch.delivery_count(), 0);
}

#[rstest]
#[case(10, 20, 3, 1, 10, 20, 3, 1, Ordering::Equal)]
#[case(10, 20, 3, 1, 20, 30, 3, 1, Ordering::Less)]
#[case(20, 30, 3, 1, 10, 20, 3, 1, Ordering::Greater)]
#[case(10, 20, 3, 1, 10, 20, 5, 1, Ordering::Less)]
#[case(10, 20, 5, 1, 10, 20, 3, 1, Ordering::Greater)]
#[case(10, 20, 3, 1, 10, 20, 3, 2, Ordering::Less)]
#[case(10, 20, 3, 2, 10, 20, 3, 1, Ordering::Greater)]
fn test_ordering(
    #[case] fo1: i64,
    #[case] lo1: i64,
    #[case] dc1: i16,
    #[case] ds1: i8,
    #[case] fo2: i64,
    #[case] lo2: i64,
    #[case] dc2: i16,
    #[case] ds2: i8,
    #[case] expected: Ordering,
) {
    let a = PersisterStateBatch::new(fo1, lo1, ds1, dc1);
    let b = PersisterStateBatch::new(fo2, lo2, ds2, dc2);
    assert_eq!(a.cmp(&b), expected);
}

#[test]
fn test_equality() {
    let a = PersisterStateBatch::new(1, 2, 3, 4);
    let b = PersisterStateBatch::new(1, 2, 3, 4);
    assert_eq!(a, b);

    let c = PersisterStateBatch::new(1, 2, 3, 5);
    assert_ne!(a, c);
}

#[test]
fn test_sorted_by_offset() {
    let mut batches = vec![
        PersisterStateBatch::new(100, 200, 1, 5),
        PersisterStateBatch::new(10, 20, 1, 3),
        PersisterStateBatch::new(50, 60, 0, 1),
    ];
    batches.sort();
    assert_eq!(batches[0].first_offset(), 10);
    assert_eq!(batches[1].first_offset(), 50);
    assert_eq!(batches[2].first_offset(), 100);
}
