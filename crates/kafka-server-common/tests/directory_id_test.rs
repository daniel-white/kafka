use kafka_common::uuid::Uuid;
use kafka_server_common::DirectoryId;
use rstest::rstest;

#[test]
fn test_reserved_constants() {
    assert_eq!(DirectoryId::MIGRATING, Uuid::new(0, 0));
    assert_eq!(DirectoryId::UNASSIGNED, Uuid::new(0, 1));
    assert_eq!(DirectoryId::LOST, Uuid::new(0, 2));
}

#[rstest]
#[case(0, 0, true)]
#[case(0, 1, true)]
#[case(0, 99, true)]
#[case(0, 100, false)]
#[case(0, -1, true)]
#[case(1, 0, false)]
#[case(-1, 0, false)]
#[case(123, 456, false)]
fn test_is_reserved(#[case] msb: i64, #[case] lsb: i64, #[case] expected: bool) {
    let uuid = Uuid::new(msb, lsb);
    assert_eq!(DirectoryId::is_reserved(&uuid), expected);
}

#[test]
fn test_random_not_reserved() {
    for _ in 0..100 {
        let uuid = DirectoryId::random();
        assert!(!DirectoryId::is_reserved(&uuid));
    }
}

#[test]
fn test_create_assignment_map_success() {
    let replicas = vec![1, 2, 3];
    let dirs = vec![
        DirectoryId::UNASSIGNED,
        DirectoryId::MIGRATING,
        DirectoryId::LOST,
    ];
    let map = DirectoryId::create_assignment_map(&replicas, &dirs).unwrap();
    assert_eq!(map.get(&1), Some(&DirectoryId::UNASSIGNED));
    assert_eq!(map.get(&2), Some(&DirectoryId::MIGRATING));
    assert_eq!(map.get(&3), Some(&DirectoryId::LOST));
}

#[test]
fn test_create_assignment_map_length_mismatch() {
    let result = DirectoryId::create_assignment_map(&[1, 2, 3], &[DirectoryId::UNASSIGNED]);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("do not match"));
}

#[test]
fn test_create_assignment_map_duplicate() {
    let result = DirectoryId::create_assignment_map(
        &[1, 1, 3],
        &[DirectoryId::UNASSIGNED, DirectoryId::MIGRATING, DirectoryId::LOST],
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Duplicate"));
}

#[test]
fn test_unassigned_array() {
    let arr = DirectoryId::unassigned_array(5);
    assert_eq!(arr.len(), 5);
    for item in &arr {
        assert_eq!(*item, DirectoryId::UNASSIGNED);
    }
}

#[test]
fn test_migrating_array() {
    let arr = DirectoryId::migrating_array(3);
    assert_eq!(arr.len(), 3);
    for item in &arr {
        assert_eq!(*item, DirectoryId::MIGRATING);
    }
}

#[rstest]
#[case(DirectoryId::UNASSIGNED, true, "UNASSIGNED always online")]
#[case(DirectoryId::MIGRATING, true, "MIGRATING always online")]
#[case(DirectoryId::LOST, false, "LOST always offline")]
fn test_is_online_reserved(#[case] dir: Uuid, #[case] expected: bool, #[case] _desc: &str) {
    let online_dirs: Vec<Uuid> = vec![];
    assert_eq!(DirectoryId::is_online(&dir, &online_dirs), expected);
}

#[test]
fn test_is_online_empty_dirs() {
    let dir = Uuid::new(1, 2);
    let online_dirs: Vec<Uuid> = vec![];
    assert!(DirectoryId::is_online(&dir, &online_dirs));
}

#[test]
fn test_is_online_found() {
    let dir = Uuid::new(1, 2);
    let online_dirs = vec![Uuid::new(0, 1), Uuid::new(1, 2), Uuid::new(3, 4)];
    assert!(DirectoryId::is_online(&dir, &online_dirs));
}

#[test]
fn test_is_online_not_found() {
    let dir = Uuid::new(99, 99);
    let online_dirs = vec![Uuid::new(0, 1), Uuid::new(1, 2), Uuid::new(3, 4)];
    assert!(!DirectoryId::is_online(&dir, &online_dirs));
}
