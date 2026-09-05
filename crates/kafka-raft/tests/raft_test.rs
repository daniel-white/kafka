use kafka_raft::{ControlRecordType, LeaderAndEpoch};
use rstest::rstest;

// --- LeaderAndEpoch tests ---

#[test]
fn test_leader_and_epoch_unknown() {
    assert_eq!(LeaderAndEpoch::UNKNOWN.leader_id(), None);
    assert_eq!(LeaderAndEpoch::UNKNOWN.epoch(), 0);
}

#[test]
fn test_leader_and_epoch_new() {
    let lae = LeaderAndEpoch::new(Some(5), 3);
    assert_eq!(lae.leader_id(), Some(5));
    assert_eq!(lae.epoch(), 3);
}

#[rstest]
#[case(Some(5), 3, 5, true)]
#[case(Some(5), 3, 6, false)]
#[case(Some(5), 3, -1, false)]
#[case(None, 3, 5, false)]
#[case(Some(-1), 0, -1, true)]
fn test_is_leader(
    #[case] leader_id: Option<i32>,
    #[case] epoch: i32,
    #[case] node_id: i32,
    #[case] expected: bool,
) {
    let lae = LeaderAndEpoch::new(leader_id, epoch);
    assert_eq!(lae.is_leader(node_id), expected);
}

#[test]
fn test_leader_and_epoch_equality() {
    assert_eq!(
        LeaderAndEpoch::new(Some(1), 2),
        LeaderAndEpoch::new(Some(1), 2)
    );
    assert_ne!(
        LeaderAndEpoch::new(Some(1), 2),
        LeaderAndEpoch::new(Some(1), 3)
    );
    assert_ne!(
        LeaderAndEpoch::new(Some(1), 2),
        LeaderAndEpoch::new(Some(2), 2)
    );
    assert_ne!(
        LeaderAndEpoch::new(Some(1), 2),
        LeaderAndEpoch::new(None, 2)
    );
}

#[test]
fn test_leader_and_epoch_default_is_unknown() {
    assert_eq!(LeaderAndEpoch::default(), LeaderAndEpoch::UNKNOWN);
}

// --- ControlRecordType tests ---

#[rstest]
#[case(ControlRecordType::Abort, 0)]
#[case(ControlRecordType::Commit, 1)]
#[case(ControlRecordType::LeaderChange, 2)]
#[case(ControlRecordType::SnapshotHeader, 3)]
#[case(ControlRecordType::SnapshotFooter, 4)]
#[case(ControlRecordType::KRaftVersion, 5)]
#[case(ControlRecordType::KRaftVoters, 6)]
#[case(ControlRecordType::Unknown, -1)]
fn test_control_record_type_key(#[case] ct: ControlRecordType, #[case] expected: i16) {
    assert_eq!(ct.key(), expected);
}

#[rstest]
#[case(0, ControlRecordType::Abort)]
#[case(1, ControlRecordType::Commit)]
#[case(2, ControlRecordType::LeaderChange)]
#[case(3, ControlRecordType::SnapshotHeader)]
#[case(4, ControlRecordType::SnapshotFooter)]
#[case(5, ControlRecordType::KRaftVersion)]
#[case(6, ControlRecordType::KRaftVoters)]
#[case(-1, ControlRecordType::Unknown)]
#[case(7, ControlRecordType::Unknown)]
#[case(100, ControlRecordType::Unknown)]
fn test_parse(#[case] key: i16, #[case] expected: ControlRecordType) {
    assert_eq!(ControlRecordType::parse(key), expected);
}

#[test]
fn test_parse_key_round_trip() {
    for ct in [
        ControlRecordType::Abort,
        ControlRecordType::Commit,
        ControlRecordType::LeaderChange,
        ControlRecordType::SnapshotHeader,
        ControlRecordType::SnapshotFooter,
        ControlRecordType::KRaftVersion,
        ControlRecordType::KRaftVoters,
    ] {
        assert_eq!(ControlRecordType::parse(ct.key()), ct);
    }
}

#[test]
fn test_parse_from_bytes_valid() {
    // version=1 (i16 BE = 0x00 0x01), key=3 (i16 BE = 0x00 0x03) → SnapshotHeader
    let buf: Box<[u8]> = vec![0x00, 0x01, 0x00, 0x03].into_boxed_slice();
    let ct = ControlRecordType::parse_from_bytes(&buf).unwrap();
    assert_eq!(ct, ControlRecordType::SnapshotHeader);
}

#[test]
fn test_parse_from_bytes_too_short() {
    let buf: Box<[u8]> = vec![0x00, 0x01].into_boxed_slice();
    let result = ControlRecordType::parse_from_bytes(&buf);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("at least 4 bytes"));
}

#[test]
fn test_control_record_type_default() {
    assert_eq!(ControlRecordType::default(), ControlRecordType::Unknown);
}
