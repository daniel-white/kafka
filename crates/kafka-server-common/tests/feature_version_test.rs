use kafka_server_common::{EligibleLeaderReplicasVersion, GroupVersion, KRaftVersion, ProducerIdsBlock, ShareVersion, StreamsVersion, TransactionVersion, TV_UNKNOWN, PRODUCER_ID_BLOCK_SIZE};
use rstest::rstest;
use std::sync::Arc;

// --- ProducerIdsBlock tests ---

#[test]
fn test_producer_ids_block_new() {
    let block = ProducerIdsBlock::new(1, 1000, 5);
    assert_eq!(block.assigned_broker_id(), 1);
    assert_eq!(block.first_producer_id(), 1000);
    assert_eq!(block.block_size(), 5);
    assert_eq!(block.last_producer_id(), 1004);
}

#[test]
fn test_producer_ids_block_empty() {
    let block = ProducerIdsBlock::EMPTY;
    assert_eq!(block.assigned_broker_id(), -1);
    assert_eq!(block.block_size(), 0);
}

#[test]
fn test_producer_ids_block_default_is_empty() {
    assert_eq!(ProducerIdsBlock::default(), ProducerIdsBlock::EMPTY);
}

#[test]
fn test_block_size_constant() {
    assert_eq!(PRODUCER_ID_BLOCK_SIZE, 1000);
}

#[test]
fn test_claim_next_id() {
    let block = ProducerIdsBlock::new(1, 1000, 5);
    assert_eq!(block.claim_next_id(), Some(1000));
    assert_eq!(block.claim_next_id(), Some(1001));
    assert_eq!(block.claim_next_id(), Some(1002));
    assert_eq!(block.claim_next_id(), Some(1003));
    assert_eq!(block.claim_next_id(), Some(1004));
    assert_eq!(block.claim_next_id(), None);
}

#[test]
fn test_claim_next_id_concurrent() {
    let block = Arc::new(ProducerIdsBlock::new(1, 0, 100));
    let mut handles = vec![];
    for _ in 0..10 {
        let block = block.clone();
        handles.push(std::thread::spawn(move || {
            let mut ids = vec![];
            for _ in 0..10 {
                if let Some(id) = block.claim_next_id() {
                    ids.push(id);
                }
            }
            ids
        }));
    }
    let mut all_ids = vec![];
    for h in handles {
        all_ids.extend(h.join().unwrap());
    }
    assert_eq!(all_ids.len(), 100);
    all_ids.sort();
    for (i, id) in all_ids.iter().enumerate() {
        assert_eq!(*id as usize, i);
    }
}

// --- KRaftVersion tests ---

#[rstest]
#[case(KRaftVersion::V0, 0)]
#[case(KRaftVersion::V1, 1)]
#[case(KRaftVersion::Unknown, -1)]
fn test_kraft_version_feature_level(#[case] kv: KRaftVersion, #[case] expected: i16) {
    assert_eq!(kv.feature_level(), expected);
}

#[test]
fn test_kraft_version_constants() {
    assert_eq!(KRaftVersion::FEATURE_NAME, "kraft.version");
    assert_eq!(KRaftVersion::LATEST_PRODUCTION, KRaftVersion::V1);
}

#[test]
fn test_kraft_version_from_feature_level() {
    assert_eq!(KRaftVersion::from_feature_level(0), KRaftVersion::V0);
    assert_eq!(KRaftVersion::from_feature_level(1), KRaftVersion::V1);
}

#[test]
fn test_kraft_version_default() {
    assert_eq!(KRaftVersion::default(), KRaftVersion::Unknown);
}

// --- StreamsVersion tests ---

#[rstest]
#[case(StreamsVersion::V0, 0)]
#[case(StreamsVersion::V1, 1)]
#[case(StreamsVersion::Unknown, -1)]
fn test_streams_version_feature_level(#[case] sv: StreamsVersion, #[case] expected: i16) {
    assert_eq!(sv.feature_level(), expected);
}

#[rstest]
#[case(StreamsVersion::V0, false)]
#[case(StreamsVersion::V1, true)]
fn test_streams_group_supported(#[case] sv: StreamsVersion, #[case] expected: bool) {
    assert_eq!(sv.streams_group_supported(), expected);
}

#[test]
fn test_streams_version_constants() {
    assert_eq!(StreamsVersion::FEATURE_NAME, "streams.version");
    assert_eq!(StreamsVersion::LATEST_PRODUCTION, StreamsVersion::V1);
}

#[test]
fn test_streams_version_from_feature_level() {
    assert_eq!(StreamsVersion::from_feature_level(0), StreamsVersion::V0);
    assert_eq!(StreamsVersion::from_feature_level(1), StreamsVersion::V1);
}

// --- EligibleLeaderReplicasVersion tests ---

#[rstest]
#[case(EligibleLeaderReplicasVersion::V0, 0)]
#[case(EligibleLeaderReplicasVersion::V1, 1)]
#[case(EligibleLeaderReplicasVersion::Unknown, -1)]
fn test_elrv_feature_level(#[case] v: EligibleLeaderReplicasVersion, #[case] expected: i16) {
    assert_eq!(v.feature_level(), expected);
}

#[rstest]
#[case(EligibleLeaderReplicasVersion::V0, false)]
#[case(EligibleLeaderReplicasVersion::V1, true)]
fn test_elr_enabled(#[case] v: EligibleLeaderReplicasVersion, #[case] expected: bool) {
    assert_eq!(v.is_elr_enabled(), expected);
}

#[test]
fn test_elrv_constants() {
    assert_eq!(
        EligibleLeaderReplicasVersion::FEATURE_NAME,
        "eligible.leader.replicas.version"
    );
    assert_eq!(
        EligibleLeaderReplicasVersion::LATEST_PRODUCTION,
        EligibleLeaderReplicasVersion::V1
    );
}

#[test]
fn test_elrv_from_feature_level() {
    assert_eq!(
        EligibleLeaderReplicasVersion::from_feature_level(0),
        EligibleLeaderReplicasVersion::V0
    );
    assert_eq!(
        EligibleLeaderReplicasVersion::from_feature_level(1),
        EligibleLeaderReplicasVersion::V1
    );
}


#[rstest]
#[case(ShareVersion::V0, 0)]
#[case(ShareVersion::V1, 1)]
#[case(ShareVersion::V2, 2)]
#[case(ShareVersion::Unknown, -1)]
fn test_share_version_feature_level(#[case] sv: ShareVersion, #[case] expected: i16) {
    assert_eq!(sv.feature_level(), expected);
}

#[rstest]
#[case(ShareVersion::V0, false)]
#[case(ShareVersion::V1, true)]
#[case(ShareVersion::V2, true)]
#[case(ShareVersion::Unknown, false)]
fn test_share_version_has_share_groups(#[case] sv: ShareVersion, #[case] expected: bool) {
    assert_eq!(sv.has_share_groups(), expected);
}

#[rstest]
#[case(ShareVersion::V0, false)]
#[case(ShareVersion::V1, false)]
#[case(ShareVersion::V2, true)]
fn test_share_version_has_dlq(#[case] sv: ShareVersion, #[case] expected: bool) {
    assert_eq!(sv.has_dlq(), expected);
}

#[test]
fn test_share_version_round_trip() {
    for level in [0, 1, 2, -1, 99] {
        let sv = ShareVersion::from_feature_level(level);
        match level {
            0 => assert_eq!(sv, ShareVersion::V0),
            1 => assert_eq!(sv, ShareVersion::V1),
            2 => assert_eq!(sv, ShareVersion::V2),
            _ => assert_eq!(sv, ShareVersion::Unknown),
        }
    }
}

#[test]
fn test_share_version_constants() {
    assert_eq!(ShareVersion::FEATURE_NAME, "share.version");
    assert_eq!(ShareVersion::LATEST_PRODUCTION, ShareVersion::V2);
}

// --- TransactionVersion tests ---

#[rstest]
#[case(TransactionVersion::V0, 0)]
#[case(TransactionVersion::V1, 1)]
#[case(TransactionVersion::V2, 2)]
#[case(TransactionVersion::Unknown, -1)]
fn test_transaction_version_feature_level(#[case] tv: TransactionVersion, #[case] expected: i16) {
    assert_eq!(tv.feature_level(), expected);
}

#[rstest]
#[case(TransactionVersion::V0, false)]
#[case(TransactionVersion::V1, true)]
#[case(TransactionVersion::V2, true)]
fn test_transaction_version_has_flexible(#[case] tv: TransactionVersion, #[case] expected: bool) {
    assert_eq!(tv.has_flexible_transactions(), expected);
}

#[test]
fn test_transaction_version_constants() {
    assert_eq!(TransactionVersion::FEATURE_NAME, "transaction.version");
    assert_eq!(TransactionVersion::LATEST_PRODUCTION, TransactionVersion::V2);
    assert_eq!(TV_UNKNOWN, -1);
}

#[test]
fn test_transaction_version_round_trip() {
    for level in [0, 1, 2, -1, 99] {
        let tv = TransactionVersion::from_feature_level(level);
        match level {
            0 => assert_eq!(tv, TransactionVersion::V0),
            1 => assert_eq!(tv, TransactionVersion::V1),
            2 => assert_eq!(tv, TransactionVersion::V2),
            _ => assert_eq!(tv, TransactionVersion::Unknown),
        }
    }
}

// --- GroupVersion tests ---

#[rstest]
#[case(GroupVersion::V0, 0)]
#[case(GroupVersion::V1, 1)]
#[case(GroupVersion::Unknown, -1)]
fn test_group_version_feature_level(#[case] gv: GroupVersion, #[case] expected: i16) {
    assert_eq!(gv.feature_level(), expected);
}

#[rstest]
#[case(GroupVersion::V0, false)]
#[case(GroupVersion::V1, true)]
fn test_group_version_has_rebalance_protocol(#[case] gv: GroupVersion, #[case] expected: bool) {
    assert_eq!(gv.has_rebalance_protocol(), expected);
}

#[test]
fn test_group_version_constants() {
    assert_eq!(GroupVersion::FEATURE_NAME, "group.version");
    assert_eq!(GroupVersion::LATEST_PRODUCTION, GroupVersion::V1);
}

#[test]
fn test_group_version_round_trip() {
    for level in [0, 1, -1, 99] {
        let gv = GroupVersion::from_feature_level(level);
        match level {
            0 => assert_eq!(gv, GroupVersion::V0),
            1 => assert_eq!(gv, GroupVersion::V1),
            _ => assert_eq!(gv, GroupVersion::Unknown),
        }
    }
}
