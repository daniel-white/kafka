// Tests mirroring clients/.../protocol/ErrorsTest.java
// MIGRATION_SOURCE: clients/src/test/java/org/apache/kafka/common/protocol/ErrorsTest.java

use kafka_protocol::errors::Errors;
use rstest::rstest;

fn all_errors() -> Vec<Errors> {
    vec![
        Errors::UnknownServerError,
        Errors::None,
        Errors::OffsetOutOfRange,
        Errors::CorruptMessage,
        Errors::UnknownTopicOrPartition,
        Errors::InvalidFetchSize,
        Errors::LeaderNotAvailable,
        Errors::NotLeaderOrFollower,
        Errors::RequestTimedOut,
        Errors::BrokerNotAvailable,
        Errors::ReplicaNotAvailable,
        Errors::MessageTooLarge,
        Errors::StaleControllerEpoch,
        Errors::OffsetMetadataTooLarge,
        Errors::NetworkException,
        Errors::CoordinatorLoadInProgress,
        Errors::CoordinatorNotAvailable,
        Errors::NotCoordinator,
        Errors::InvalidTopicException,
        Errors::RecordListTooLarge,
        Errors::NotEnoughReplicas,
        Errors::NotEnoughReplicasAfterAppend,
        Errors::InvalidRequiredAcks,
        Errors::IllegalGeneration,
        Errors::InconsistentGroupProtocol,
        Errors::InvalidGroupId,
        Errors::UnknownMemberId,
        Errors::InvalidSessionTimeout,
        Errors::RebalanceInProgress,
        Errors::InvalidCommitOffsetSize,
        Errors::TopicAuthorizationFailed,
        Errors::GroupAuthorizationFailed,
        Errors::ClusterAuthorizationFailed,
        Errors::InvalidTimestamp,
        Errors::UnsupportedSaslMechanism,
        Errors::IllegalSaslState,
        Errors::UnsupportedVersion,
        Errors::TopicAlreadyExists,
        Errors::InvalidPartitions,
        Errors::InvalidReplicationFactor,
        Errors::InvalidReplicaAssignment,
        Errors::InvalidConfig,
        Errors::NotController,
        Errors::InvalidRequest,
        Errors::UnsupportedForMessageFormat,
        Errors::PolicyViolation,
        Errors::OutOfOrderSequenceNumber,
        Errors::DuplicateSequenceNumber,
        Errors::InvalidProducerEpoch,
        Errors::InvalidTxnState,
        Errors::InvalidProducerIdMapping,
        Errors::InvalidTransactionTimeout,
        Errors::ConcurrentTransactions,
        Errors::TransactionCoordinatorFenced,
        Errors::TransactionalIdAuthorizationFailed,
        Errors::SecurityDisabled,
        Errors::OperationNotAttempted,
        Errors::KafkaStorageError,
        Errors::LogDirNotFound,
        Errors::SaslAuthenticationFailed,
        Errors::UnknownProducerId,
        Errors::ReassignmentInProgress,
        Errors::DelegationTokenAuthDisabled,
        Errors::DelegationTokenNotFound,
        Errors::DelegationTokenOwnerMismatch,
        Errors::DelegationTokenRequestNotAllowed,
        Errors::DelegationTokenAuthorizationFailed,
        Errors::DelegationTokenExpired,
        Errors::InvalidPrincipalType,
        Errors::NonEmptyGroup,
        Errors::GroupIdNotFound,
        Errors::FetchSessionIdNotFound,
        Errors::InvalidFetchSessionEpoch,
        Errors::ListenerNotFound,
        Errors::TopicDeletionDisabled,
        Errors::FencedLeaderEpoch,
        Errors::UnknownLeaderEpoch,
        Errors::UnsupportedCompressionType,
        Errors::StaleBrokerEpoch,
        Errors::OffsetNotAvailable,
        Errors::MemberIdRequired,
        Errors::PreferredLeaderNotAvailable,
        Errors::GroupMaxSizeReached,
        Errors::FencedInstanceId,
        Errors::EligibleLeadersNotAvailable,
        Errors::ElectionNotNeeded,
        Errors::NoReassignmentInProgress,
        Errors::GroupSubscribedToTopic,
        Errors::InvalidRecord,
        Errors::UnstableOffsetCommit,
        Errors::ThrottlingQuotaExceeded,
        Errors::ProducerFenced,
        Errors::ResourceNotFound,
        Errors::DuplicateResource,
        Errors::UnacceptableCredential,
        Errors::InconsistentVoterSet,
        Errors::InvalidUpdateVersion,
        Errors::FeatureUpdateFailed,
        Errors::PrincipalDeserializationFailure,
        Errors::SnapshotNotFound,
        Errors::PositionOutOfRange,
        Errors::UnknownTopicId,
        Errors::DuplicateBrokerRegistration,
        Errors::BrokerIdNotRegistered,
        Errors::InconsistentTopicId,
        Errors::InconsistentClusterId,
        Errors::TransactionalIdNotFound,
        Errors::FetchSessionTopicIdError,
        Errors::IneligibleReplica,
        Errors::NewLeaderElected,
        Errors::OffsetMovedToTieredStorage,
        Errors::FencedMemberEpoch,
        Errors::UnreleasedInstanceId,
        Errors::UnsupportedAssignor,
        Errors::StaleMemberEpoch,
        Errors::MismatchedEndpointType,
        Errors::UnsupportedEndpointType,
        Errors::UnknownControllerId,
        Errors::UnknownSubscriptionId,
        Errors::TelemetryTooLarge,
        Errors::InvalidRegistration,
        Errors::TransactionAbortable,
        Errors::InvalidRecordState,
        Errors::ShareSessionNotFound,
        Errors::InvalidShareSessionEpoch,
        Errors::FencedStateEpoch,
        Errors::InvalidVoterKey,
        Errors::DuplicateVoter,
        Errors::VoterNotFound,
        Errors::InvalidRegularExpression,
        Errors::RebootstrapRequired,
        Errors::StreamsInvalidTopology,
        Errors::StreamsInvalidTopologyEpoch,
        Errors::StreamsTopologyFenced,
        Errors::ShareSessionLimitReached,
        Errors::GroupDeletionFailed,
        Errors::StreamsTopologyDescriptionUpdateFailed,
        Errors::ControllerIdNotRegistered,
    ]
}

#[test]
fn test_unique_error_codes() {
    let mut seen = std::collections::HashSet::new();
    for error in all_errors() {
        assert!(seen.insert(error.code()), "Error codes must be unique");
    }
    assert_eq!(seen.len(), all_errors().len());
}

#[test]
fn test_none_exception() {
    assert!(Errors::None.exception_name().is_none());
}

#[rstest]
#[case(Errors::UnknownServerError, "org.apache.kafka.common.errors.UnknownServerException")]
#[case(Errors::InvalidTopicException, "org.apache.kafka.common.errors.InvalidTopicException")]
#[case(Errors::MessageTooLarge, "org.apache.kafka.common.errors.RecordTooLargeException")]
#[case(Errors::NetworkException, "org.apache.kafka.common.errors.NetworkException")]
fn test_exception_name(#[case] error: Errors, #[case] expected: &str) {
    assert_eq!(error.exception_name().unwrap(), expected);
}

#[rstest]
#[case(-1, Errors::UnknownServerError)]
#[case(0, Errors::None)]
#[case(1, Errors::OffsetOutOfRange)]
#[case(7, Errors::RequestTimedOut)]
#[case(100, Errors::UnknownTopicId)]
#[case(999, Errors::UnknownServerError)]
fn test_for_code(#[case] code: i16, #[case] expected: Errors) {
    assert_eq!(Errors::for_code(code), expected);
}

#[test]
fn test_maybe_throw_none() {
    assert!(Errors::None.maybe_throw().is_ok());
}

#[test]
fn test_maybe_throw_error() {
    assert_eq!(Errors::MessageTooLarge.maybe_throw(), Err(Errors::MessageTooLarge));
}
