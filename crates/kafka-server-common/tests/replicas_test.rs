use kafka_server_common::Replicas;
use rstest::rstest;

#[test]
fn test_none_constant() {
    assert!(Replicas::NONE_ARRAY.is_empty());
}

#[test]
fn test_to_list() {
    assert_eq!(Replicas::to_list(&[1, 2, 3]), vec![1, 2, 3]);
    assert!(Replicas::to_list(&[]).is_empty());
}

#[test]
fn test_to_array() {
    assert_eq!(Replicas::to_array(&[1, 2, 3]), vec![1, 2, 3]);
    assert!(Replicas::to_array(&[]).is_empty());
}

#[test]
fn test_clone() {
    let original = vec![1, 2, 3];
    let cloned = Replicas::clone(&original);
    assert_eq!(cloned, original);
}

#[rstest]
#[case(&[1, 2, 3], true)]
#[case(&[], true)]
#[case(&[-1, 2, 3], false)]
#[case(&[1, 1, 2], false)]
#[case(&[3, 2, 1], true)]
#[case(&[1, 2, -1], false)]
fn test_validate(#[case] replicas: &[i32], #[case] expected: bool) {
    assert_eq!(Replicas::validate(replicas), expected);
}

#[rstest]
#[case(&[1, 2, 3], &[1, 2], true)]
#[case(&[1, 2, 3], &[4], false)]
#[case(&[1, 2, 3], &[], true)]
#[case(&[], &[1], false)]
fn test_validate_isr(
    #[case] replicas: &[i32],
    #[case] isr: &[i32],
    #[case] expected: bool,
) {
    assert_eq!(Replicas::validate_isr(replicas, isr), expected);
}

#[test]
fn test_validate_isr_duplicates() {
    assert!(!Replicas::validate_isr(&[1, 2, 3], &[1, 1]));
}

#[test]
fn test_validate_isr_negative() {
    assert!(!Replicas::validate_isr(&[1, 2, 3], &[-1]));
}

#[rstest]
#[case(&[1, 2, 3], 2, true)]
#[case(&[1, 2, 3], 4, false)]
#[case(&[], 1, false)]
fn test_contains(#[case] replicas: &[i32], #[case] value: i32, #[case] expected: bool) {
    assert_eq!(Replicas::contains(replicas, value), expected);
}

#[test]
fn test_contains_slice() {
    assert!(Replicas::contains_slice(&[1, 2, 3, 4, 5], &[1, 3, 5]));
    assert!(!Replicas::contains_slice(&[1, 2, 3], &[4]));
}

#[rstest]
#[case(&[1, 2, 3], 2, &[1, 3])]
#[case(&[1, 2, 3], 1, &[2, 3])]
#[case(&[1, 2, 2, 3], 2, &[1, 3])]
#[case(&[], 1, &[])]
fn test_copy_without(
    #[case] replicas: &[i32],
    #[case] value: i32,
    #[case] expected: &[i32],
) {
    assert_eq!(Replicas::copy_without(replicas, value), expected);
}

#[test]
fn test_copy_without_slice() {
    assert_eq!(
        Replicas::copy_without_slice(&[1, 2, 3, 4, 5], &[2, 4]),
        vec![1, 3, 5]
    );
}

#[rstest]
#[case(&[1, 2, 3], 4, &[1, 2, 3, 4])]
#[case(&[], 1, &[1])]
#[case(&[5], 3, &[5, 3])]
fn test_copy_with(
    #[case] replicas: &[i32],
    #[case] value: i32,
    #[case] expected: &[i32],
) {
    assert_eq!(Replicas::copy_with(replicas, value), expected);
}

#[test]
fn test_to_set() {
    let set = Replicas::to_set(&[1, 2, 3, 2, 1]);
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1));
    assert!(set.contains(&2));
    assert!(set.contains(&3));
}
