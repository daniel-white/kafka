//! Wire-protocol error codes, mirroring Java's `org.apache.kafka.common.protocol.Errors`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java

use std::io;

/// Protocol-level error enum for IO and version operations.
///
/// MIGRATION_SOURCE: (new)
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Unsupported version")]
    UnsupportedVersion,
}

/// Wire-protocol error codes, mirroring Java's `org.apache.kafka.common.protocol.Errors`.
/// Each variant maps a numeric error code to a message and exception class.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
pub enum Errors {
    #[error("The server experienced an unexpected error when processing the request.")]
    UnknownServerError,
    #[error("")]
    None,
    #[error("The requested offset is not within the range of offsets maintained by the server.")]
    OffsetOutOfRange,
    #[error("This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.")]
    CorruptMessage,
    #[error("This server does not host this topic-partition.")]
    UnknownTopicOrPartition,
    #[error("The requested fetch size is invalid.")]
    InvalidFetchSize,
    #[error("There is no leader for this topic-partition as we are in the middle of a leadership election.")]
    LeaderNotAvailable,
    #[error("For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.")]
    NotLeaderOrFollower,
    #[error("The request timed out.")]
    RequestTimedOut,
    #[error("The broker is not available.")]
    BrokerNotAvailable,
    #[error("The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.")]
    ReplicaNotAvailable,
    #[error("The request included a message larger than the max message size the server will accept.")]
    MessageTooLarge,
    #[error("The controller moved to another broker.")]
    StaleControllerEpoch,
    #[error("The metadata field of the offset request was too large.")]
    OffsetMetadataTooLarge,
    #[error("The server disconnected before a response was received.")]
    NetworkException,
    #[error("The coordinator is loading and hence can't process requests.")]
    CoordinatorLoadInProgress,
    #[error("The coordinator is not available.")]
    CoordinatorNotAvailable,
    #[error("This is not the correct coordinator.")]
    NotCoordinator,
    #[error("The request attempted to perform an operation on an invalid topic.")]
    InvalidTopicException,
    #[error("The request included message batch larger than the configured segment size on the server.")]
    RecordListTooLarge,
    #[error("Messages are rejected since there are fewer in-sync replicas than required.")]
    NotEnoughReplicas,
    #[error("Messages are written to the log, but to fewer in-sync replicas than required.")]
    NotEnoughReplicasAfterAppend,
    #[error("Produce request specified an invalid value for required acks.")]
    InvalidRequiredAcks,
    #[error("Specified group generation id is not valid.")]
    IllegalGeneration,
    #[error("The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.")]
    InconsistentGroupProtocol,
    #[error("The group id is invalid.")]
    InvalidGroupId,
    #[error("The coordinator is not aware of this member.")]
    UnknownMemberId,
    #[error("The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).")]
    InvalidSessionTimeout,
    #[error("The group is rebalancing, so a rejoin is needed.")]
    RebalanceInProgress,
    #[error("The committing offset data size is not valid.")]
    InvalidCommitOffsetSize,
    #[error("Topic authorization failed.")]
    TopicAuthorizationFailed,
    #[error("Group authorization failed.")]
    GroupAuthorizationFailed,
    #[error("Cluster authorization failed.")]
    ClusterAuthorizationFailed,
    #[error("The timestamp of the message is out of acceptable range.")]
    InvalidTimestamp,
    #[error("The broker does not support the requested SASL mechanism.")]
    UnsupportedSaslMechanism,
    #[error("Request is not valid given the current SASL state.")]
    IllegalSaslState,
    #[error("The version of API is not supported.")]
    UnsupportedVersion,
    #[error("Topic with this name already exists.")]
    TopicAlreadyExists,
    #[error("Number of partitions is below 1.")]
    InvalidPartitions,
    #[error("Replication factor is below 1 or larger than the number of available brokers.")]
    InvalidReplicationFactor,
    #[error("Replica assignment is invalid.")]
    InvalidReplicaAssignment,
    #[error("Configuration is invalid.")]
    InvalidConfig,
    #[error("This is not the correct controller for this cluster.")]
    NotController,
    #[error("This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.")]
    InvalidRequest,
    #[error("The message format version on the broker does not support the request.")]
    UnsupportedForMessageFormat,
    #[error("Request parameters do not satisfy the configured policy.")]
    PolicyViolation,
    #[error("The broker received an out of order sequence number.")]
    OutOfOrderSequenceNumber,
    #[error("The broker received a duplicate sequence number.")]
    DuplicateSequenceNumber,
    #[error("Producer attempted to produce with an old epoch.")]
    InvalidProducerEpoch,
    #[error("The producer attempted a transactional operation in an invalid state.")]
    InvalidTxnState,
    #[error("The producer attempted to use a producer id which is not currently assigned to its transactional id.")]
    InvalidProducerIdMapping,
    #[error("The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).")]
    InvalidTransactionTimeout,
    #[error("The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.")]
    ConcurrentTransactions,
    #[error("Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.")]
    TransactionCoordinatorFenced,
    #[error("Transactional Id authorization failed.")]
    TransactionalIdAuthorizationFailed,
    #[error("Security features are disabled.")]
    SecurityDisabled,
    #[error("The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.")]
    OperationNotAttempted,
    #[error("Disk error when trying to access log file on the disk.")]
    KafkaStorageError,
    #[error("The user-specified log directory is not found in the broker config.")]
    LogDirNotFound,
    #[error("SASL Authentication failed.")]
    SaslAuthenticationFailed,
    #[error("This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question.")]
    UnknownProducerId,
    #[error("A partition reassignment is in progress.")]
    ReassignmentInProgress,
    #[error("Delegation Token feature is not enabled.")]
    DelegationTokenAuthDisabled,
    #[error("Delegation Token is not found on server.")]
    DelegationTokenNotFound,
    #[error("Specified Principal is not valid Owner/Renewer.")]
    DelegationTokenOwnerMismatch,
    #[error("Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.")]
    DelegationTokenRequestNotAllowed,
    #[error("Delegation Token authorization failed.")]
    DelegationTokenAuthorizationFailed,
    #[error("Delegation Token is expired.")]
    DelegationTokenExpired,
    #[error("Supplied principalType is not supported.")]
    InvalidPrincipalType,
    #[error("The group is not empty.")]
    NonEmptyGroup,
    #[error("The group id does not exist.")]
    GroupIdNotFound,
    #[error("The fetch session ID was not found.")]
    FetchSessionIdNotFound,
    #[error("The fetch session epoch is invalid.")]
    InvalidFetchSessionEpoch,
    #[error("There is no listener on the leader broker that matches the listener on which metadata request was processed.")]
    ListenerNotFound,
    #[error("Topic deletion is disabled.")]
    TopicDeletionDisabled,
    #[error("The leader epoch in the request is older than the epoch on the broker.")]
    FencedLeaderEpoch,
    #[error("The leader epoch in the request is newer than the epoch on the broker.")]
    UnknownLeaderEpoch,
    #[error("The requesting client does not support the compression type of given partition.")]
    UnsupportedCompressionType,
    #[error("Broker epoch has changed.")]
    StaleBrokerEpoch,
    #[error("The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.")]
    OffsetNotAvailable,
    #[error("The group member needs to have a valid member id before actually entering a consumer group.")]
    MemberIdRequired,
    #[error("The preferred leader was not available.")]
    PreferredLeaderNotAvailable,
    #[error("The group has reached its maximum size.")]
    GroupMaxSizeReached,
    #[error("The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.")]
    FencedInstanceId,
    #[error("Eligible topic partition leaders are not available.")]
    EligibleLeadersNotAvailable,
    #[error("Leader election not needed for topic partition.")]
    ElectionNotNeeded,
    #[error("No partition reassignment is in progress.")]
    NoReassignmentInProgress,
    #[error("Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.")]
    GroupSubscribedToTopic,
    #[error("This record has failed the validation on broker and hence will be rejected.")]
    InvalidRecord,
    #[error("There are unstable offsets that need to be cleared.")]
    UnstableOffsetCommit,
    #[error("The throttling quota has been exceeded.")]
    ThrottlingQuotaExceeded,
    #[error("There is a newer producer with the same transactionalId which fences the current one.")]
    ProducerFenced,
    #[error("A request illegally referred to a resource that does not exist.")]
    ResourceNotFound,
    #[error("A request illegally referred to the same resource twice.")]
    DuplicateResource,
    #[error("Requested credential would not meet criteria for acceptability.")]
    UnacceptableCredential,
    #[error("Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.")]
    InconsistentVoterSet,
    #[error("The given update version was invalid.")]
    InvalidUpdateVersion,
    #[error("Unable to update finalized features due to an unexpected server error.")]
    FeatureUpdateFailed,
    #[error("Request principal deserialization failed during forwarding.")]
    PrincipalDeserializationFailure,
    #[error("Requested snapshot was not found.")]
    SnapshotNotFound,
    #[error("Requested position is not greater than or equal to zero, and less than the size of the snapshot.")]
    PositionOutOfRange,
    #[error("This server does not host this topic ID.")]
    UnknownTopicId,
    #[error("This broker ID is already in use.")]
    DuplicateBrokerRegistration,
    #[error("The given broker ID was not registered.")]
    BrokerIdNotRegistered,
    #[error("The log's topic ID did not match the topic ID in the request.")]
    InconsistentTopicId,
    #[error("The clusterId in the request does not match that found on the server.")]
    InconsistentClusterId,
    #[error("The transactionalId could not be found.")]
    TransactionalIdNotFound,
    #[error("The fetch session encountered inconsistent topic ID usage.")]
    FetchSessionTopicIdError,
    #[error("The new ISR contains at least one ineligible replica.")]
    IneligibleReplica,
    #[error("The AlterPartition request successfully updated the partition state but the leader has changed.")]
    NewLeaderElected,
    #[error("The requested offset is moved to tiered storage.")]
    OffsetMovedToTieredStorage,
    #[error("The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.")]
    FencedMemberEpoch,
    #[error("The instance ID is still used by another member in the consumer group. That member must leave first.")]
    UnreleasedInstanceId,
    #[error("The assignor or its version range is not supported by the consumer group.")]
    UnsupportedAssignor,
    #[error("The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.")]
    StaleMemberEpoch,
    #[error("The request was sent to an endpoint of the wrong type.")]
    MismatchedEndpointType,
    #[error("This endpoint type is not supported yet.")]
    UnsupportedEndpointType,
    #[error("This controller ID is not known.")]
    UnknownControllerId,
    #[error("Client sent a push telemetry request with an invalid or outdated subscription ID.")]
    UnknownSubscriptionId,
    #[error("Client sent a push telemetry request larger than the maximum size the broker will accept.")]
    TelemetryTooLarge,
    #[error("The controller has considered the broker registration to be invalid.")]
    InvalidRegistration,
    #[error("The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.")]
    TransactionAbortable,
    #[error("The record state is invalid. The acknowledgement of delivery could not be completed.")]
    InvalidRecordState,
    #[error("The share session was not found.")]
    ShareSessionNotFound,
    #[error("The share session epoch is invalid.")]
    InvalidShareSessionEpoch,
    #[error("The coordinator rejected the request because the state epoch did not match.")]
    FencedStateEpoch,
    #[error("The voter key doesn't match the receiving replica's key.")]
    InvalidVoterKey,
    #[error("The voter is already part of the set of voters.")]
    DuplicateVoter,
    #[error("The voter is not part of the set of voters.")]
    VoterNotFound,
    #[error("The regular expression is not valid.")]
    InvalidRegularExpression,
    #[error("Client metadata is stale. The client should rebootstrap to obtain new metadata.")]
    RebootstrapRequired,
    #[error("The supplied topology is invalid.")]
    StreamsInvalidTopology,
    #[error("The supplied topology epoch is invalid.")]
    StreamsInvalidTopologyEpoch,
    #[error("The supplied topology epoch is outdated.")]
    StreamsTopologyFenced,
    #[error("The limit of share sessions has been reached.")]
    ShareSessionLimitReached,
    #[error("DeleteGroups could not complete; see the error message on the per-group result for details.")]
    GroupDeletionFailed,
    #[error("The broker could not process the topology description update; see the error message for details.")]
    StreamsTopologyDescriptionUpdateFailed,
    #[error("The given controller ID was not registered.")]
    ControllerIdNotRegistered,
}

impl Errors {
    /// The error code for this error.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub const fn code(&self) -> i16 {
        match self {
            Errors::UnknownServerError => -1,
            Errors::None => 0,
            Errors::OffsetOutOfRange => 1,
            Errors::CorruptMessage => 2,
            Errors::UnknownTopicOrPartition => 3,
            Errors::InvalidFetchSize => 4,
            Errors::LeaderNotAvailable => 5,
            Errors::NotLeaderOrFollower => 6,
            Errors::RequestTimedOut => 7,
            Errors::BrokerNotAvailable => 8,
            Errors::ReplicaNotAvailable => 9,
            Errors::MessageTooLarge => 10,
            Errors::StaleControllerEpoch => 11,
            Errors::OffsetMetadataTooLarge => 12,
            Errors::NetworkException => 13,
            Errors::CoordinatorLoadInProgress => 14,
            Errors::CoordinatorNotAvailable => 15,
            Errors::NotCoordinator => 16,
            Errors::InvalidTopicException => 17,
            Errors::RecordListTooLarge => 18,
            Errors::NotEnoughReplicas => 19,
            Errors::NotEnoughReplicasAfterAppend => 20,
            Errors::InvalidRequiredAcks => 21,
            Errors::IllegalGeneration => 22,
            Errors::InconsistentGroupProtocol => 23,
            Errors::InvalidGroupId => 24,
            Errors::UnknownMemberId => 25,
            Errors::InvalidSessionTimeout => 26,
            Errors::RebalanceInProgress => 27,
            Errors::InvalidCommitOffsetSize => 28,
            Errors::TopicAuthorizationFailed => 29,
            Errors::GroupAuthorizationFailed => 30,
            Errors::ClusterAuthorizationFailed => 31,
            Errors::InvalidTimestamp => 32,
            Errors::UnsupportedSaslMechanism => 33,
            Errors::IllegalSaslState => 34,
            Errors::UnsupportedVersion => 35,
            Errors::TopicAlreadyExists => 36,
            Errors::InvalidPartitions => 37,
            Errors::InvalidReplicationFactor => 38,
            Errors::InvalidReplicaAssignment => 39,
            Errors::InvalidConfig => 40,
            Errors::NotController => 41,
            Errors::InvalidRequest => 42,
            Errors::UnsupportedForMessageFormat => 43,
            Errors::PolicyViolation => 44,
            Errors::OutOfOrderSequenceNumber => 45,
            Errors::DuplicateSequenceNumber => 46,
            Errors::InvalidProducerEpoch => 47,
            Errors::InvalidTxnState => 48,
            Errors::InvalidProducerIdMapping => 49,
            Errors::InvalidTransactionTimeout => 50,
            Errors::ConcurrentTransactions => 51,
            Errors::TransactionCoordinatorFenced => 52,
            Errors::TransactionalIdAuthorizationFailed => 53,
            Errors::SecurityDisabled => 54,
            Errors::OperationNotAttempted => 55,
            Errors::KafkaStorageError => 56,
            Errors::LogDirNotFound => 57,
            Errors::SaslAuthenticationFailed => 58,
            Errors::UnknownProducerId => 59,
            Errors::ReassignmentInProgress => 60,
            Errors::DelegationTokenAuthDisabled => 61,
            Errors::DelegationTokenNotFound => 62,
            Errors::DelegationTokenOwnerMismatch => 63,
            Errors::DelegationTokenRequestNotAllowed => 64,
            Errors::DelegationTokenAuthorizationFailed => 65,
            Errors::DelegationTokenExpired => 66,
            Errors::InvalidPrincipalType => 67,
            Errors::NonEmptyGroup => 68,
            Errors::GroupIdNotFound => 69,
            Errors::FetchSessionIdNotFound => 70,
            Errors::InvalidFetchSessionEpoch => 71,
            Errors::ListenerNotFound => 72,
            Errors::TopicDeletionDisabled => 73,
            Errors::FencedLeaderEpoch => 74,
            Errors::UnknownLeaderEpoch => 75,
            Errors::UnsupportedCompressionType => 76,
            Errors::StaleBrokerEpoch => 77,
            Errors::OffsetNotAvailable => 78,
            Errors::MemberIdRequired => 79,
            Errors::PreferredLeaderNotAvailable => 80,
            Errors::GroupMaxSizeReached => 81,
            Errors::FencedInstanceId => 82,
            Errors::EligibleLeadersNotAvailable => 83,
            Errors::ElectionNotNeeded => 84,
            Errors::NoReassignmentInProgress => 85,
            Errors::GroupSubscribedToTopic => 86,
            Errors::InvalidRecord => 87,
            Errors::UnstableOffsetCommit => 88,
            Errors::ThrottlingQuotaExceeded => 89,
            Errors::ProducerFenced => 90,
            Errors::ResourceNotFound => 91,
            Errors::DuplicateResource => 92,
            Errors::UnacceptableCredential => 93,
            Errors::InconsistentVoterSet => 94,
            Errors::InvalidUpdateVersion => 95,
            Errors::FeatureUpdateFailed => 96,
            Errors::PrincipalDeserializationFailure => 97,
            Errors::SnapshotNotFound => 98,
            Errors::PositionOutOfRange => 99,
            Errors::UnknownTopicId => 100,
            Errors::DuplicateBrokerRegistration => 101,
            Errors::BrokerIdNotRegistered => 102,
            Errors::InconsistentTopicId => 103,
            Errors::InconsistentClusterId => 104,
            Errors::TransactionalIdNotFound => 105,
            Errors::FetchSessionTopicIdError => 106,
            Errors::IneligibleReplica => 107,
            Errors::NewLeaderElected => 108,
            Errors::OffsetMovedToTieredStorage => 109,
            Errors::FencedMemberEpoch => 110,
            Errors::UnreleasedInstanceId => 111,
            Errors::UnsupportedAssignor => 112,
            Errors::StaleMemberEpoch => 113,
            Errors::MismatchedEndpointType => 114,
            Errors::UnsupportedEndpointType => 115,
            Errors::UnknownControllerId => 116,
            Errors::UnknownSubscriptionId => 117,
            Errors::TelemetryTooLarge => 118,
            Errors::InvalidRegistration => 119,
            Errors::TransactionAbortable => 120,
            Errors::InvalidRecordState => 121,
            Errors::ShareSessionNotFound => 122,
            Errors::InvalidShareSessionEpoch => 123,
            Errors::FencedStateEpoch => 124,
            Errors::InvalidVoterKey => 125,
            Errors::DuplicateVoter => 126,
            Errors::VoterNotFound => 127,
            Errors::InvalidRegularExpression => 128,
            Errors::RebootstrapRequired => 129,
            Errors::StreamsInvalidTopology => 130,
            Errors::StreamsInvalidTopologyEpoch => 131,
            Errors::StreamsTopologyFenced => 132,
            Errors::ShareSessionLimitReached => 133,
            Errors::GroupDeletionFailed => 134,
            Errors::StreamsTopologyDescriptionUpdateFailed => 135,
            Errors::ControllerIdNotRegistered => 136,
            #[allow(unreachable_patterns)]
            _ => -1,
        }
    }

    /// The default description of this error.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub fn message(&self) -> &'static str {
        #[allow(unreachable_patterns)]
        match self {
            Errors::UnknownServerError => "The server experienced an unexpected error when processing the request.",
            Errors::None => "",
            Errors::OffsetOutOfRange => "The requested offset is not within the range of offsets maintained by the server.",
            Errors::CorruptMessage => "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
            Errors::UnknownTopicOrPartition => "This server does not host this topic-partition.",
            Errors::InvalidFetchSize => "The requested fetch size is invalid.",
            Errors::LeaderNotAvailable => "There is no leader for this topic-partition as we are in the middle of a leadership election.",
            Errors::NotLeaderOrFollower => "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
            Errors::RequestTimedOut => "The request timed out.",
            Errors::BrokerNotAvailable => "The broker is not available.",
            Errors::ReplicaNotAvailable => "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
            Errors::MessageTooLarge => "The request included a message larger than the max message size the server will accept.",
            Errors::StaleControllerEpoch => "The controller moved to another broker.",
            Errors::OffsetMetadataTooLarge => "The metadata field of the offset request was too large.",
            Errors::NetworkException => "The server disconnected before a response was received.",
            Errors::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests.",
            Errors::CoordinatorNotAvailable => "The coordinator is not available.",
            Errors::NotCoordinator => "This is not the correct coordinator.",
            Errors::InvalidTopicException => "The request attempted to perform an operation on an invalid topic.",
            Errors::RecordListTooLarge => "The request included message batch larger than the configured segment size on the server.",
            Errors::NotEnoughReplicas => "Messages are rejected since there are fewer in-sync replicas than required.",
            Errors::NotEnoughReplicasAfterAppend => "Messages are written to the log, but to fewer in-sync replicas than required.",
            Errors::InvalidRequiredAcks => "Produce request specified an invalid value for required acks.",
            Errors::IllegalGeneration => "Specified group generation id is not valid.",
            Errors::InconsistentGroupProtocol => "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
            Errors::InvalidGroupId => "The group id is invalid.",
            Errors::UnknownMemberId => "The coordinator is not aware of this member.",
            Errors::InvalidSessionTimeout => "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            Errors::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed.",
            Errors::InvalidCommitOffsetSize => "The committing offset data size is not valid.",
            Errors::TopicAuthorizationFailed => "Topic authorization failed.",
            Errors::GroupAuthorizationFailed => "Group authorization failed.",
            Errors::ClusterAuthorizationFailed => "Cluster authorization failed.",
            Errors::InvalidTimestamp => "The timestamp of the message is out of acceptable range.",
            Errors::UnsupportedSaslMechanism => "The broker does not support the requested SASL mechanism.",
            Errors::IllegalSaslState => "Request is not valid given the current SASL state.",
            Errors::UnsupportedVersion => "The version of API is not supported.",
            Errors::TopicAlreadyExists => "Topic with this name already exists.",
            Errors::InvalidPartitions => "Number of partitions is below 1.",
            Errors::InvalidReplicationFactor => "Replication factor is below 1 or larger than the number of available brokers.",
            Errors::InvalidReplicaAssignment => "Replica assignment is invalid.",
            Errors::InvalidConfig => "Configuration is invalid.",
            Errors::NotController => "This is not the correct controller for this cluster.",
            Errors::InvalidRequest => "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
            Errors::UnsupportedForMessageFormat => "The message format version on the broker does not support the request.",
            Errors::PolicyViolation => "Request parameters do not satisfy the configured policy.",
            Errors::OutOfOrderSequenceNumber => "The broker received an out of order sequence number.",
            Errors::DuplicateSequenceNumber => "The broker received a duplicate sequence number.",
            Errors::InvalidProducerEpoch => "Producer attempted to produce with an old epoch.",
            Errors::InvalidTxnState => "The producer attempted a transactional operation in an invalid state.",
            Errors::InvalidProducerIdMapping => "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
            Errors::InvalidTransactionTimeout => "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
            Errors::ConcurrentTransactions => "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
            Errors::TransactionCoordinatorFenced => "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
            Errors::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed.",
            Errors::SecurityDisabled => "Security features are disabled.",
            Errors::OperationNotAttempted => "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
            Errors::KafkaStorageError => "Disk error when trying to access log file on the disk.",
            Errors::LogDirNotFound => "The user-specified log directory is not found in the broker config.",
            Errors::SaslAuthenticationFailed => "SASL Authentication failed.",
            Errors::UnknownProducerId => "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
            Errors::ReassignmentInProgress => "A partition reassignment is in progress.",
            Errors::DelegationTokenAuthDisabled => "Delegation Token feature is not enabled.",
            Errors::DelegationTokenNotFound => "Delegation Token is not found on server.",
            Errors::DelegationTokenOwnerMismatch => "Specified Principal is not valid Owner/Renewer.",
            Errors::DelegationTokenRequestNotAllowed => "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
            Errors::DelegationTokenAuthorizationFailed => "Delegation Token authorization failed.",
            Errors::DelegationTokenExpired => "Delegation Token is expired.",
            Errors::InvalidPrincipalType => "Supplied principalType is not supported.",
            Errors::NonEmptyGroup => "The group is not empty.",
            Errors::GroupIdNotFound => "The group id does not exist.",
            Errors::FetchSessionIdNotFound => "The fetch session ID was not found.",
            Errors::InvalidFetchSessionEpoch => "The fetch session epoch is invalid.",
            Errors::ListenerNotFound => "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
            Errors::TopicDeletionDisabled => "Topic deletion is disabled.",
            Errors::FencedLeaderEpoch => "The leader epoch in the request is older than the epoch on the broker.",
            Errors::UnknownLeaderEpoch => "The leader epoch in the request is newer than the epoch on the broker.",
            Errors::UnsupportedCompressionType => "The requesting client does not support the compression type of given partition.",
            Errors::StaleBrokerEpoch => "Broker epoch has changed.",
            Errors::OffsetNotAvailable => "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
            Errors::MemberIdRequired => "The group member needs to have a valid member id before actually entering a consumer group.",
            Errors::PreferredLeaderNotAvailable => "The preferred leader was not available.",
            Errors::GroupMaxSizeReached => "The group has reached its maximum size.",
            Errors::FencedInstanceId => "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
            Errors::EligibleLeadersNotAvailable => "Eligible topic partition leaders are not available.",
            Errors::ElectionNotNeeded => "Leader election not needed for topic partition.",
            Errors::NoReassignmentInProgress => "No partition reassignment is in progress.",
            Errors::GroupSubscribedToTopic => "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
            Errors::InvalidRecord => "This record has failed the validation on broker and hence will be rejected.",
            Errors::UnstableOffsetCommit => "There are unstable offsets that need to be cleared.",
            Errors::ThrottlingQuotaExceeded => "The throttling quota has been exceeded.",
            Errors::ProducerFenced => "There is a newer producer with the same transactionalId which fences the current one.",
            Errors::ResourceNotFound => "A request illegally referred to a resource that does not exist.",
            Errors::DuplicateResource => "A request illegally referred to the same resource twice.",
            Errors::UnacceptableCredential => "Requested credential would not meet criteria for acceptability.",
            Errors::InconsistentVoterSet => "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.",
            Errors::InvalidUpdateVersion => "The given update version was invalid.",
            Errors::FeatureUpdateFailed => "Unable to update finalized features due to an unexpected server error.",
            Errors::PrincipalDeserializationFailure => "Request principal deserialization failed during forwarding.",
            Errors::SnapshotNotFound => "Requested snapshot was not found.",
            Errors::PositionOutOfRange => "Requested position is not greater than or equal to zero, and less than the size of the snapshot.",
            Errors::UnknownTopicId => "This server does not host this topic ID.",
            Errors::DuplicateBrokerRegistration => "This broker ID is already in use.",
            Errors::BrokerIdNotRegistered => "The given broker ID was not registered.",
            Errors::InconsistentTopicId => "The log's topic ID did not match the topic ID in the request.",
            Errors::InconsistentClusterId => "The clusterId in the request does not match that found on the server.",
            Errors::TransactionalIdNotFound => "The transactionalId could not be found.",
            Errors::FetchSessionTopicIdError => "The fetch session encountered inconsistent topic ID usage.",
            Errors::IneligibleReplica => "The new ISR contains at least one ineligible replica.",
            Errors::NewLeaderElected => "The AlterPartition request successfully updated the partition state but the leader has changed.",
            Errors::OffsetMovedToTieredStorage => "The requested offset is moved to tiered storage.",
            Errors::FencedMemberEpoch => "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.",
            Errors::UnreleasedInstanceId => "The instance ID is still used by another member in the consumer group. That member must leave first.",
            Errors::UnsupportedAssignor => "The assignor or its version range is not supported by the consumer group.",
            Errors::StaleMemberEpoch => "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.",
            Errors::MismatchedEndpointType => "The request was sent to an endpoint of the wrong type.",
            Errors::UnsupportedEndpointType => "This endpoint type is not supported yet.",
            Errors::UnknownControllerId => "This controller ID is not known.",
            Errors::UnknownSubscriptionId => "Client sent a push telemetry request with an invalid or outdated subscription ID.",
            Errors::TelemetryTooLarge => "Client sent a push telemetry request larger than the maximum size the broker will accept.",
            Errors::InvalidRegistration => "The controller has considered the broker registration to be invalid.",
            Errors::TransactionAbortable => "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.",
            Errors::InvalidRecordState => "The record state is invalid. The acknowledgement of delivery could not be completed.",
            Errors::ShareSessionNotFound => "The share session was not found.",
            Errors::InvalidShareSessionEpoch => "The share session epoch is invalid.",
            Errors::FencedStateEpoch => "The coordinator rejected the request because the state epoch did not match.",
            Errors::InvalidVoterKey => "The voter key doesn't match the receiving replica's key.",
            Errors::DuplicateVoter => "The voter is already part of the set of voters.",
            Errors::VoterNotFound => "The voter is not part of the set of voters.",
            Errors::InvalidRegularExpression => "The regular expression is not valid.",
            Errors::RebootstrapRequired => "Client metadata is stale. The client should rebootstrap to obtain new metadata.",
            Errors::StreamsInvalidTopology => "The supplied topology is invalid.",
            Errors::StreamsInvalidTopologyEpoch => "The supplied topology epoch is invalid.",
            Errors::StreamsTopologyFenced => "The supplied topology epoch is outdated.",
            Errors::ShareSessionLimitReached => "The limit of share sessions has been reached.",
            Errors::GroupDeletionFailed => "DeleteGroups could not complete; see the error message on the per-group result for details.",
            Errors::StreamsTopologyDescriptionUpdateFailed => "The broker could not process the topology description update; see the error message for details.",
            Errors::ControllerIdNotRegistered => "The given controller ID was not registered.",
            _ => "",
        }
    }

    /// Returns the exception class name or `None` for `Errors::None`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub const fn exception_name(&self) -> Option<&'static str> {
        match self {
            Errors::UnknownServerError => Some("org.apache.kafka.common.errors.UnknownServerException"),
            Errors::None => None,
            Errors::OffsetOutOfRange => Some("org.apache.kafka.common.errors.OffsetOutOfRangeException"),
            Errors::CorruptMessage => Some("org.apache.kafka.common.errors.CorruptRecordException"),
            Errors::UnknownTopicOrPartition => Some("org.apache.kafka.common.errors.UnknownTopicOrPartitionException"),
            Errors::InvalidFetchSize => Some("org.apache.kafka.common.errors.InvalidFetchSizeException"),
            Errors::LeaderNotAvailable => Some("org.apache.kafka.common.errors.LeaderNotAvailableException"),
            Errors::NotLeaderOrFollower => Some("org.apache.kafka.common.errors.NotLeaderOrFollowerException"),
            Errors::RequestTimedOut => Some("org.apache.kafka.common.errors.TimeoutException"),
            Errors::BrokerNotAvailable => Some("org.apache.kafka.common.errors.BrokerNotAvailableException"),
            Errors::ReplicaNotAvailable => Some("org.apache.kafka.common.errors.ReplicaNotAvailableException"),
            Errors::MessageTooLarge => Some("org.apache.kafka.common.errors.RecordTooLargeException"),
            Errors::StaleControllerEpoch => Some("org.apache.kafka.common.errors.ControllerMovedException"),
            Errors::OffsetMetadataTooLarge => Some("org.apache.kafka.common.errors.OffsetMetadataTooLarge"),
            Errors::NetworkException => Some("org.apache.kafka.common.errors.NetworkException"),
            Errors::CoordinatorLoadInProgress => Some("org.apache.kafka.common.errors.CoordinatorLoadInProgressException"),
            Errors::CoordinatorNotAvailable => Some("org.apache.kafka.common.errors.CoordinatorNotAvailableException"),
            Errors::NotCoordinator => Some("org.apache.kafka.common.errors.NotCoordinatorException"),
            Errors::InvalidTopicException => Some("org.apache.kafka.common.errors.InvalidTopicException"),
            Errors::RecordListTooLarge => Some("org.apache.kafka.common.errors.RecordBatchTooLargeException"),
            Errors::NotEnoughReplicas => Some("org.apache.kafka.common.errors.NotEnoughReplicasException"),
            Errors::NotEnoughReplicasAfterAppend => Some("org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException"),
            Errors::InvalidRequiredAcks => Some("org.apache.kafka.common.errors.InvalidRequiredAcksException"),
            Errors::IllegalGeneration => Some("org.apache.kafka.common.errors.IllegalGenerationException"),
            Errors::InconsistentGroupProtocol => Some("org.apache.kafka.common.errors.InconsistentGroupProtocolException"),
            Errors::InvalidGroupId => Some("org.apache.kafka.common.errors.InvalidGroupIdException"),
            Errors::UnknownMemberId => Some("org.apache.kafka.common.errors.UnknownMemberIdException"),
            Errors::InvalidSessionTimeout => Some("org.apache.kafka.common.errors.InvalidSessionTimeoutException"),
            Errors::RebalanceInProgress => Some("org.apache.kafka.common.errors.RebalanceInProgressException"),
            Errors::InvalidCommitOffsetSize => Some("org.apache.kafka.common.errors.InvalidCommitOffsetSizeException"),
            Errors::TopicAuthorizationFailed => Some("org.apache.kafka.common.errors.TopicAuthorizationException"),
            Errors::GroupAuthorizationFailed => Some("org.apache.kafka.common.errors.GroupAuthorizationException"),
            Errors::ClusterAuthorizationFailed => Some("org.apache.kafka.common.errors.ClusterAuthorizationException"),
            Errors::InvalidTimestamp => Some("org.apache.kafka.common.errors.InvalidTimestampException"),
            Errors::UnsupportedSaslMechanism => Some("org.apache.kafka.common.errors.UnsupportedSaslMechanismException"),
            Errors::IllegalSaslState => Some("org.apache.kafka.common.errors.IllegalSaslStateException"),
            Errors::UnsupportedVersion => Some("org.apache.kafka.common.errors.UnsupportedVersionException"),
            Errors::TopicAlreadyExists => Some("org.apache.kafka.common.errors.TopicExistsException"),
            Errors::InvalidPartitions => Some("org.apache.kafka.common.errors.InvalidPartitionsException"),
            Errors::InvalidReplicationFactor => Some("org.apache.kafka.common.errors.InvalidReplicationFactorException"),
            Errors::InvalidReplicaAssignment => Some("org.apache.kafka.common.errors.InvalidReplicaAssignmentException"),
            Errors::InvalidConfig => Some("org.apache.kafka.common.errors.InvalidConfigurationException"),
            Errors::NotController => Some("org.apache.kafka.common.errors.NotControllerException"),
            Errors::InvalidRequest => Some("org.apache.kafka.common.errors.InvalidRequestException"),
            Errors::UnsupportedForMessageFormat => Some("org.apache.kafka.common.errors.UnsupportedForMessageFormatException"),
            Errors::PolicyViolation => Some("org.apache.kafka.common.errors.PolicyViolationException"),
            Errors::OutOfOrderSequenceNumber => Some("org.apache.kafka.common.errors.OutOfOrderSequenceException"),
            Errors::DuplicateSequenceNumber => Some("org.apache.kafka.common.errors.DuplicateSequenceException"),
            Errors::InvalidProducerEpoch => Some("org.apache.kafka.common.errors.InvalidProducerEpochException"),
            Errors::InvalidTxnState => Some("org.apache.kafka.common.errors.InvalidTxnStateException"),
            Errors::InvalidProducerIdMapping => Some("org.apache.kafka.common.errors.InvalidPidMappingException"),
            Errors::InvalidTransactionTimeout => Some("org.apache.kafka.common.errors.InvalidTxnTimeoutException"),
            Errors::ConcurrentTransactions => Some("org.apache.kafka.common.errors.ConcurrentTransactionsException"),
            Errors::TransactionCoordinatorFenced => Some("org.apache.kafka.common.errors.TransactionCoordinatorFencedException"),
            Errors::TransactionalIdAuthorizationFailed => Some("org.apache.kafka.common.errors.TransactionalIdAuthorizationException"),
            Errors::SecurityDisabled => Some("org.apache.kafka.common.errors.SecurityDisabledException"),
            Errors::OperationNotAttempted => Some("org.apache.kafka.common.errors.OperationNotAttemptedException"),
            Errors::KafkaStorageError => Some("org.apache.kafka.common.errors.KafkaStorageException"),
            Errors::LogDirNotFound => Some("org.apache.kafka.common.errors.LogDirNotFoundException"),
            Errors::SaslAuthenticationFailed => Some("org.apache.kafka.common.errors.SaslAuthenticationException"),
            Errors::UnknownProducerId => Some("org.apache.kafka.common.errors.UnknownProducerIdException"),
            Errors::ReassignmentInProgress => Some("org.apache.kafka.common.errors.ReassignmentInProgressException"),
            Errors::DelegationTokenAuthDisabled => Some("org.apache.kafka.common.errors.DelegationTokenDisabledException"),
            Errors::DelegationTokenNotFound => Some("org.apache.kafka.common.errors.DelegationTokenNotFoundException"),
            Errors::DelegationTokenOwnerMismatch => Some("org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException"),
            Errors::DelegationTokenRequestNotAllowed => Some("org.apache.kafka.common.errors.UnsupportedByAuthenticationException"),
            Errors::DelegationTokenAuthorizationFailed => Some("org.apache.kafka.common.errors.DelegationTokenAuthorizationException"),
            Errors::DelegationTokenExpired => Some("org.apache.kafka.common.errors.DelegationTokenExpiredException"),
            Errors::InvalidPrincipalType => Some("org.apache.kafka.common.errors.InvalidPrincipalTypeException"),
            Errors::NonEmptyGroup => Some("org.apache.kafka.common.errors.GroupNotEmptyException"),
            Errors::GroupIdNotFound => Some("org.apache.kafka.common.errors.GroupIdNotFoundException"),
            Errors::FetchSessionIdNotFound => Some("org.apache.kafka.common.errors.FetchSessionIdNotFoundException"),
            Errors::InvalidFetchSessionEpoch => Some("org.apache.kafka.common.errors.InvalidFetchSessionEpochException"),
            Errors::ListenerNotFound => Some("org.apache.kafka.common.errors.ListenerNotFoundException"),
            Errors::TopicDeletionDisabled => Some("org.apache.kafka.common.errors.TopicDeletionDisabledException"),
            Errors::FencedLeaderEpoch => Some("org.apache.kafka.common.errors.FencedLeaderEpochException"),
            Errors::UnknownLeaderEpoch => Some("org.apache.kafka.common.errors.UnknownLeaderEpochException"),
            Errors::UnsupportedCompressionType => Some("org.apache.kafka.common.errors.UnsupportedCompressionTypeException"),
            Errors::StaleBrokerEpoch => Some("org.apache.kafka.common.errors.StaleBrokerEpochException"),
            Errors::OffsetNotAvailable => Some("org.apache.kafka.common.errors.OffsetNotAvailableException"),
            Errors::MemberIdRequired => Some("org.apache.kafka.common.errors.MemberIdRequiredException"),
            Errors::PreferredLeaderNotAvailable => Some("org.apache.kafka.common.errors.PreferredLeaderNotAvailableException"),
            Errors::GroupMaxSizeReached => Some("org.apache.kafka.common.errors.GroupMaxSizeReachedException"),
            Errors::FencedInstanceId => Some("org.apache.kafka.common.errors.FencedInstanceIdException"),
            Errors::EligibleLeadersNotAvailable => Some("org.apache.kafka.common.errors.EligibleLeadersNotAvailableException"),
            Errors::ElectionNotNeeded => Some("org.apache.kafka.common.errors.ElectionNotNeededException"),
            Errors::NoReassignmentInProgress => Some("org.apache.kafka.common.errors.NoReassignmentInProgressException"),
            Errors::GroupSubscribedToTopic => Some("org.apache.kafka.common.errors.GroupSubscribedToTopicException"),
            Errors::InvalidRecord => Some("org.apache.kafka.common.InvalidRecordException"),
            Errors::UnstableOffsetCommit => Some("org.apache.kafka.common.errors.UnstableOffsetCommitException"),
            Errors::ThrottlingQuotaExceeded => Some("org.apache.kafka.common.errors.ThrottlingQuotaExceededException"),
            Errors::ProducerFenced => Some("org.apache.kafka.common.errors.ProducerFencedException"),
            Errors::ResourceNotFound => Some("org.apache.kafka.common.errors.ResourceNotFoundException"),
            Errors::DuplicateResource => Some("org.apache.kafka.common.errors.DuplicateResourceException"),
            Errors::UnacceptableCredential => Some("org.apache.kafka.common.errors.UnacceptableCredentialException"),
            Errors::InconsistentVoterSet => Some("org.apache.kafka.common.errors.InconsistentVoterSetException"),
            Errors::InvalidUpdateVersion => Some("org.apache.kafka.common.errors.InvalidUpdateVersionException"),
            Errors::FeatureUpdateFailed => Some("org.apache.kafka.common.errors.FeatureUpdateFailedException"),
            Errors::PrincipalDeserializationFailure => Some("org.apache.kafka.common.errors.PrincipalDeserializationException"),
            Errors::SnapshotNotFound => Some("org.apache.kafka.common.errors.SnapshotNotFoundException"),
            Errors::PositionOutOfRange => Some("org.apache.kafka.common.errors.PositionOutOfRangeException"),
            Errors::UnknownTopicId => Some("org.apache.kafka.common.errors.UnknownTopicIdException"),
            Errors::DuplicateBrokerRegistration => Some("org.apache.kafka.common.errors.DuplicateBrokerRegistrationException"),
            Errors::BrokerIdNotRegistered => Some("org.apache.kafka.common.errors.BrokerIdNotRegisteredException"),
            Errors::InconsistentTopicId => Some("org.apache.kafka.common.errors.InconsistentTopicIdException"),
            Errors::InconsistentClusterId => Some("org.apache.kafka.common.errors.InconsistentClusterIdException"),
            Errors::TransactionalIdNotFound => Some("org.apache.kafka.common.errors.TransactionalIdNotFoundException"),
            Errors::FetchSessionTopicIdError => Some("org.apache.kafka.common.errors.FetchSessionTopicIdException"),
            Errors::IneligibleReplica => Some("org.apache.kafka.common.errors.IneligibleReplicaException"),
            Errors::NewLeaderElected => Some("org.apache.kafka.common.errors.NewLeaderElectedException"),
            Errors::OffsetMovedToTieredStorage => Some("org.apache.kafka.common.errors.OffsetMovedToTieredStorageException"),
            Errors::FencedMemberEpoch => Some("org.apache.kafka.common.errors.FencedMemberEpochException"),
            Errors::UnreleasedInstanceId => Some("org.apache.kafka.common.errors.UnreleasedInstanceIdException"),
            Errors::UnsupportedAssignor => Some("org.apache.kafka.common.errors.UnsupportedAssignorException"),
            Errors::StaleMemberEpoch => Some("org.apache.kafka.common.errors.StaleMemberEpochException"),
            Errors::MismatchedEndpointType => Some("org.apache.kafka.common.errors.MismatchedEndpointTypeException"),
            Errors::UnsupportedEndpointType => Some("org.apache.kafka.common.errors.UnsupportedEndpointTypeException"),
            Errors::UnknownControllerId => Some("org.apache.kafka.common.errors.UnknownControllerIdException"),
            Errors::UnknownSubscriptionId => Some("org.apache.kafka.common.errors.UnknownSubscriptionIdException"),
            Errors::TelemetryTooLarge => Some("org.apache.kafka.common.errors.TelemetryTooLargeException"),
            Errors::InvalidRegistration => Some("org.apache.kafka.common.errors.InvalidRegistrationException"),
            Errors::TransactionAbortable => Some("org.apache.kafka.common.errors.TransactionAbortableException"),
            Errors::InvalidRecordState => Some("org.apache.kafka.common.errors.InvalidRecordStateException"),
            Errors::ShareSessionNotFound => Some("org.apache.kafka.common.errors.ShareSessionNotFoundException"),
            Errors::InvalidShareSessionEpoch => Some("org.apache.kafka.common.errors.InvalidShareSessionEpochException"),
            Errors::FencedStateEpoch => Some("org.apache.kafka.common.errors.FencedStateEpochException"),
            Errors::InvalidVoterKey => Some("org.apache.kafka.common.errors.InvalidVoterKeyException"),
            Errors::DuplicateVoter => Some("org.apache.kafka.common.errors.DuplicateVoterException"),
            Errors::VoterNotFound => Some("org.apache.kafka.common.errors.VoterNotFoundException"),
            Errors::InvalidRegularExpression => Some("org.apache.kafka.common.errors.InvalidRegularExpression"),
            Errors::RebootstrapRequired => Some("org.apache.kafka.common.errors.RebootstrapRequiredException"),
            Errors::StreamsInvalidTopology => Some("org.apache.kafka.common.errors.StreamsInvalidTopologyException"),
            Errors::StreamsInvalidTopologyEpoch => Some("org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException"),
            Errors::StreamsTopologyFenced => Some("org.apache.kafka.common.errors.StreamsTopologyFencedException"),
            Errors::ShareSessionLimitReached => Some("org.apache.kafka.common.errors.ShareSessionLimitReachedException"),
            Errors::GroupDeletionFailed => Some("org.apache.kafka.common.errors.GroupDeletionFailedException"),
            Errors::StreamsTopologyDescriptionUpdateFailed => Some("org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException"),
            Errors::ControllerIdNotRegistered => Some("org.apache.kafka.common.errors.ControllerIdNotRegisteredException"),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Look up an `Errors` variant by error code.
    ///
    /// Unknown codes return `UnknownServerError`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub const fn for_code(code: i16) -> Self {
        match code {
            -1 => Errors::UnknownServerError,
            0 => Errors::None,
            1 => Errors::OffsetOutOfRange,
            2 => Errors::CorruptMessage,
            3 => Errors::UnknownTopicOrPartition,
            4 => Errors::InvalidFetchSize,
            5 => Errors::LeaderNotAvailable,
            6 => Errors::NotLeaderOrFollower,
            7 => Errors::RequestTimedOut,
            8 => Errors::BrokerNotAvailable,
            9 => Errors::ReplicaNotAvailable,
            10 => Errors::MessageTooLarge,
            11 => Errors::StaleControllerEpoch,
            12 => Errors::OffsetMetadataTooLarge,
            13 => Errors::NetworkException,
            14 => Errors::CoordinatorLoadInProgress,
            15 => Errors::CoordinatorNotAvailable,
            16 => Errors::NotCoordinator,
            17 => Errors::InvalidTopicException,
            18 => Errors::RecordListTooLarge,
            19 => Errors::NotEnoughReplicas,
            20 => Errors::NotEnoughReplicasAfterAppend,
            21 => Errors::InvalidRequiredAcks,
            22 => Errors::IllegalGeneration,
            23 => Errors::InconsistentGroupProtocol,
            24 => Errors::InvalidGroupId,
            25 => Errors::UnknownMemberId,
            26 => Errors::InvalidSessionTimeout,
            27 => Errors::RebalanceInProgress,
            28 => Errors::InvalidCommitOffsetSize,
            29 => Errors::TopicAuthorizationFailed,
            30 => Errors::GroupAuthorizationFailed,
            31 => Errors::ClusterAuthorizationFailed,
            32 => Errors::InvalidTimestamp,
            33 => Errors::UnsupportedSaslMechanism,
            34 => Errors::IllegalSaslState,
            35 => Errors::UnsupportedVersion,
            36 => Errors::TopicAlreadyExists,
            37 => Errors::InvalidPartitions,
            38 => Errors::InvalidReplicationFactor,
            39 => Errors::InvalidReplicaAssignment,
            40 => Errors::InvalidConfig,
            41 => Errors::NotController,
            42 => Errors::InvalidRequest,
            43 => Errors::UnsupportedForMessageFormat,
            44 => Errors::PolicyViolation,
            45 => Errors::OutOfOrderSequenceNumber,
            46 => Errors::DuplicateSequenceNumber,
            47 => Errors::InvalidProducerEpoch,
            48 => Errors::InvalidTxnState,
            49 => Errors::InvalidProducerIdMapping,
            50 => Errors::InvalidTransactionTimeout,
            51 => Errors::ConcurrentTransactions,
            52 => Errors::TransactionCoordinatorFenced,
            53 => Errors::TransactionalIdAuthorizationFailed,
            54 => Errors::SecurityDisabled,
            55 => Errors::OperationNotAttempted,
            56 => Errors::KafkaStorageError,
            57 => Errors::LogDirNotFound,
            58 => Errors::SaslAuthenticationFailed,
            59 => Errors::UnknownProducerId,
            60 => Errors::ReassignmentInProgress,
            61 => Errors::DelegationTokenAuthDisabled,
            62 => Errors::DelegationTokenNotFound,
            63 => Errors::DelegationTokenOwnerMismatch,
            64 => Errors::DelegationTokenRequestNotAllowed,
            65 => Errors::DelegationTokenAuthorizationFailed,
            66 => Errors::DelegationTokenExpired,
            67 => Errors::InvalidPrincipalType,
            68 => Errors::NonEmptyGroup,
            69 => Errors::GroupIdNotFound,
            70 => Errors::FetchSessionIdNotFound,
            71 => Errors::InvalidFetchSessionEpoch,
            72 => Errors::ListenerNotFound,
            73 => Errors::TopicDeletionDisabled,
            74 => Errors::FencedLeaderEpoch,
            75 => Errors::UnknownLeaderEpoch,
            76 => Errors::UnsupportedCompressionType,
            77 => Errors::StaleBrokerEpoch,
            78 => Errors::OffsetNotAvailable,
            79 => Errors::MemberIdRequired,
            80 => Errors::PreferredLeaderNotAvailable,
            81 => Errors::GroupMaxSizeReached,
            82 => Errors::FencedInstanceId,
            83 => Errors::EligibleLeadersNotAvailable,
            84 => Errors::ElectionNotNeeded,
            85 => Errors::NoReassignmentInProgress,
            86 => Errors::GroupSubscribedToTopic,
            87 => Errors::InvalidRecord,
            88 => Errors::UnstableOffsetCommit,
            89 => Errors::ThrottlingQuotaExceeded,
            90 => Errors::ProducerFenced,
            91 => Errors::ResourceNotFound,
            92 => Errors::DuplicateResource,
            93 => Errors::UnacceptableCredential,
            94 => Errors::InconsistentVoterSet,
            95 => Errors::InvalidUpdateVersion,
            96 => Errors::FeatureUpdateFailed,
            97 => Errors::PrincipalDeserializationFailure,
            98 => Errors::SnapshotNotFound,
            99 => Errors::PositionOutOfRange,
            100 => Errors::UnknownTopicId,
            101 => Errors::DuplicateBrokerRegistration,
            102 => Errors::BrokerIdNotRegistered,
            103 => Errors::InconsistentTopicId,
            104 => Errors::InconsistentClusterId,
            105 => Errors::TransactionalIdNotFound,
            106 => Errors::FetchSessionTopicIdError,
            107 => Errors::IneligibleReplica,
            108 => Errors::NewLeaderElected,
            109 => Errors::OffsetMovedToTieredStorage,
            110 => Errors::FencedMemberEpoch,
            111 => Errors::UnreleasedInstanceId,
            112 => Errors::UnsupportedAssignor,
            113 => Errors::StaleMemberEpoch,
            114 => Errors::MismatchedEndpointType,
            115 => Errors::UnsupportedEndpointType,
            116 => Errors::UnknownControllerId,
            117 => Errors::UnknownSubscriptionId,
            118 => Errors::TelemetryTooLarge,
            119 => Errors::InvalidRegistration,
            120 => Errors::TransactionAbortable,
            121 => Errors::InvalidRecordState,
            122 => Errors::ShareSessionNotFound,
            123 => Errors::InvalidShareSessionEpoch,
            124 => Errors::FencedStateEpoch,
            125 => Errors::InvalidVoterKey,
            126 => Errors::DuplicateVoter,
            127 => Errors::VoterNotFound,
            128 => Errors::InvalidRegularExpression,
            129 => Errors::RebootstrapRequired,
            130 => Errors::StreamsInvalidTopology,
            131 => Errors::StreamsInvalidTopologyEpoch,
            132 => Errors::StreamsTopologyFenced,
            133 => Errors::ShareSessionLimitReached,
            134 => Errors::GroupDeletionFailed,
            135 => Errors::StreamsTopologyDescriptionUpdateFailed,
            136 => Errors::ControllerIdNotRegistered,
            #[allow(unreachable_patterns)]
            _ => Errors::UnknownServerError,
        }
    }

    /// Look up an `Errors` variant by Java exception class name.
    ///
    /// Unknown class names return `UnknownServerError`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub fn for_exception_name(name: &str) -> Self {
        match name {
            "org.apache.kafka.common.errors.UnknownServerException" => Errors::UnknownServerError,
            "org.apache.kafka.common.errors.OffsetOutOfRangeException" => Errors::OffsetOutOfRange,
            "org.apache.kafka.common.errors.CorruptRecordException" => Errors::CorruptMessage,
            "org.apache.kafka.common.errors.UnknownTopicOrPartitionException" => Errors::UnknownTopicOrPartition,
            "org.apache.kafka.common.errors.InvalidFetchSizeException" => Errors::InvalidFetchSize,
            "org.apache.kafka.common.errors.LeaderNotAvailableException" => Errors::LeaderNotAvailable,
            "org.apache.kafka.common.errors.NotLeaderOrFollowerException" => Errors::NotLeaderOrFollower,
            "org.apache.kafka.common.errors.TimeoutException" => Errors::RequestTimedOut,
            "org.apache.kafka.common.errors.BrokerNotAvailableException" => Errors::BrokerNotAvailable,
            "org.apache.kafka.common.errors.ReplicaNotAvailableException" => Errors::ReplicaNotAvailable,
            "org.apache.kafka.common.errors.RecordTooLargeException" => Errors::MessageTooLarge,
            "org.apache.kafka.common.errors.ControllerMovedException" => Errors::StaleControllerEpoch,
            "org.apache.kafka.common.errors.OffsetMetadataTooLarge" => Errors::OffsetMetadataTooLarge,
            "org.apache.kafka.common.errors.NetworkException" => Errors::NetworkException,
            "org.apache.kafka.common.errors.CoordinatorLoadInProgressException" => Errors::CoordinatorLoadInProgress,
            "org.apache.kafka.common.errors.CoordinatorNotAvailableException" => Errors::CoordinatorNotAvailable,
            "org.apache.kafka.common.errors.NotCoordinatorException" => Errors::NotCoordinator,
            "org.apache.kafka.common.errors.InvalidTopicException" => Errors::InvalidTopicException,
            "org.apache.kafka.common.errors.RecordBatchTooLargeException" => Errors::RecordListTooLarge,
            "org.apache.kafka.common.errors.NotEnoughReplicasException" => Errors::NotEnoughReplicas,
            "org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException" => Errors::NotEnoughReplicasAfterAppend,
            "org.apache.kafka.common.errors.InvalidRequiredAcksException" => Errors::InvalidRequiredAcks,
            "org.apache.kafka.common.errors.IllegalGenerationException" => Errors::IllegalGeneration,
            "org.apache.kafka.common.errors.InconsistentGroupProtocolException" => Errors::InconsistentGroupProtocol,
            "org.apache.kafka.common.errors.InvalidGroupIdException" => Errors::InvalidGroupId,
            "org.apache.kafka.common.errors.UnknownMemberIdException" => Errors::UnknownMemberId,
            "org.apache.kafka.common.errors.InvalidSessionTimeoutException" => Errors::InvalidSessionTimeout,
            "org.apache.kafka.common.errors.RebalanceInProgressException" => Errors::RebalanceInProgress,
            "org.apache.kafka.common.errors.InvalidCommitOffsetSizeException" => Errors::InvalidCommitOffsetSize,
            "org.apache.kafka.common.errors.TopicAuthorizationException" => Errors::TopicAuthorizationFailed,
            "org.apache.kafka.common.errors.GroupAuthorizationException" => Errors::GroupAuthorizationFailed,
            "org.apache.kafka.common.errors.ClusterAuthorizationException" => Errors::ClusterAuthorizationFailed,
            "org.apache.kafka.common.errors.InvalidTimestampException" => Errors::InvalidTimestamp,
            "org.apache.kafka.common.errors.UnsupportedSaslMechanismException" => Errors::UnsupportedSaslMechanism,
            "org.apache.kafka.common.errors.IllegalSaslStateException" => Errors::IllegalSaslState,
            "org.apache.kafka.common.errors.UnsupportedVersionException" => Errors::UnsupportedVersion,
            "org.apache.kafka.common.errors.TopicExistsException" => Errors::TopicAlreadyExists,
            "org.apache.kafka.common.errors.InvalidPartitionsException" => Errors::InvalidPartitions,
            "org.apache.kafka.common.errors.InvalidReplicationFactorException" => Errors::InvalidReplicationFactor,
            "org.apache.kafka.common.errors.InvalidReplicaAssignmentException" => Errors::InvalidReplicaAssignment,
            "org.apache.kafka.common.errors.InvalidConfigurationException" => Errors::InvalidConfig,
            "org.apache.kafka.common.errors.NotControllerException" => Errors::NotController,
            "org.apache.kafka.common.errors.InvalidRequestException" => Errors::InvalidRequest,
            "org.apache.kafka.common.errors.UnsupportedForMessageFormatException" => Errors::UnsupportedForMessageFormat,
            "org.apache.kafka.common.errors.PolicyViolationException" => Errors::PolicyViolation,
            "org.apache.kafka.common.errors.OutOfOrderSequenceException" => Errors::OutOfOrderSequenceNumber,
            "org.apache.kafka.common.errors.DuplicateSequenceException" => Errors::DuplicateSequenceNumber,
            "org.apache.kafka.common.errors.InvalidProducerEpochException" => Errors::InvalidProducerEpoch,
            "org.apache.kafka.common.errors.InvalidTxnStateException" => Errors::InvalidTxnState,
            "org.apache.kafka.common.errors.InvalidPidMappingException" => Errors::InvalidProducerIdMapping,
            "org.apache.kafka.common.errors.InvalidTxnTimeoutException" => Errors::InvalidTransactionTimeout,
            "org.apache.kafka.common.errors.ConcurrentTransactionsException" => Errors::ConcurrentTransactions,
            "org.apache.kafka.common.errors.TransactionCoordinatorFencedException" => Errors::TransactionCoordinatorFenced,
            "org.apache.kafka.common.errors.TransactionalIdAuthorizationException" => Errors::TransactionalIdAuthorizationFailed,
            "org.apache.kafka.common.errors.SecurityDisabledException" => Errors::SecurityDisabled,
            "org.apache.kafka.common.errors.OperationNotAttemptedException" => Errors::OperationNotAttempted,
            "org.apache.kafka.common.errors.KafkaStorageException" => Errors::KafkaStorageError,
            "org.apache.kafka.common.errors.LogDirNotFoundException" => Errors::LogDirNotFound,
            "org.apache.kafka.common.errors.SaslAuthenticationException" => Errors::SaslAuthenticationFailed,
            "org.apache.kafka.common.errors.UnknownProducerIdException" => Errors::UnknownProducerId,
            "org.apache.kafka.common.errors.ReassignmentInProgressException" => Errors::ReassignmentInProgress,
            "org.apache.kafka.common.errors.DelegationTokenDisabledException" => Errors::DelegationTokenAuthDisabled,
            "org.apache.kafka.common.errors.DelegationTokenNotFoundException" => Errors::DelegationTokenNotFound,
            "org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException" => Errors::DelegationTokenOwnerMismatch,
            "org.apache.kafka.common.errors.UnsupportedByAuthenticationException" => Errors::DelegationTokenRequestNotAllowed,
            "org.apache.kafka.common.errors.DelegationTokenAuthorizationException" => Errors::DelegationTokenAuthorizationFailed,
            "org.apache.kafka.common.errors.DelegationTokenExpiredException" => Errors::DelegationTokenExpired,
            "org.apache.kafka.common.errors.InvalidPrincipalTypeException" => Errors::InvalidPrincipalType,
            "org.apache.kafka.common.errors.GroupNotEmptyException" => Errors::NonEmptyGroup,
            "org.apache.kafka.common.errors.GroupIdNotFoundException" => Errors::GroupIdNotFound,
            "org.apache.kafka.common.errors.FetchSessionIdNotFoundException" => Errors::FetchSessionIdNotFound,
            "org.apache.kafka.common.errors.InvalidFetchSessionEpochException" => Errors::InvalidFetchSessionEpoch,
            "org.apache.kafka.common.errors.ListenerNotFoundException" => Errors::ListenerNotFound,
            "org.apache.kafka.common.errors.TopicDeletionDisabledException" => Errors::TopicDeletionDisabled,
            "org.apache.kafka.common.errors.FencedLeaderEpochException" => Errors::FencedLeaderEpoch,
            "org.apache.kafka.common.errors.UnknownLeaderEpochException" => Errors::UnknownLeaderEpoch,
            "org.apache.kafka.common.errors.UnsupportedCompressionTypeException" => Errors::UnsupportedCompressionType,
            "org.apache.kafka.common.errors.StaleBrokerEpochException" => Errors::StaleBrokerEpoch,
            "org.apache.kafka.common.errors.OffsetNotAvailableException" => Errors::OffsetNotAvailable,
            "org.apache.kafka.common.errors.MemberIdRequiredException" => Errors::MemberIdRequired,
            "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException" => Errors::PreferredLeaderNotAvailable,
            "org.apache.kafka.common.errors.GroupMaxSizeReachedException" => Errors::GroupMaxSizeReached,
            "org.apache.kafka.common.errors.FencedInstanceIdException" => Errors::FencedInstanceId,
            "org.apache.kafka.common.errors.EligibleLeadersNotAvailableException" => Errors::EligibleLeadersNotAvailable,
            "org.apache.kafka.common.errors.ElectionNotNeededException" => Errors::ElectionNotNeeded,
            "org.apache.kafka.common.errors.NoReassignmentInProgressException" => Errors::NoReassignmentInProgress,
            "org.apache.kafka.common.errors.GroupSubscribedToTopicException" => Errors::GroupSubscribedToTopic,
            "org.apache.kafka.common.InvalidRecordException" => Errors::InvalidRecord,
            "org.apache.kafka.common.errors.UnstableOffsetCommitException" => Errors::UnstableOffsetCommit,
            "org.apache.kafka.common.errors.ThrottlingQuotaExceededException" => Errors::ThrottlingQuotaExceeded,
            "org.apache.kafka.common.errors.ProducerFencedException" => Errors::ProducerFenced,
            "org.apache.kafka.common.errors.ResourceNotFoundException" => Errors::ResourceNotFound,
            "org.apache.kafka.common.errors.DuplicateResourceException" => Errors::DuplicateResource,
            "org.apache.kafka.common.errors.UnacceptableCredentialException" => Errors::UnacceptableCredential,
            "org.apache.kafka.common.errors.InconsistentVoterSetException" => Errors::InconsistentVoterSet,
            "org.apache.kafka.common.errors.InvalidUpdateVersionException" => Errors::InvalidUpdateVersion,
            "org.apache.kafka.common.errors.FeatureUpdateFailedException" => Errors::FeatureUpdateFailed,
            "org.apache.kafka.common.errors.PrincipalDeserializationException" => Errors::PrincipalDeserializationFailure,
            "org.apache.kafka.common.errors.SnapshotNotFoundException" => Errors::SnapshotNotFound,
            "org.apache.kafka.common.errors.PositionOutOfRangeException" => Errors::PositionOutOfRange,
            "org.apache.kafka.common.errors.UnknownTopicIdException" => Errors::UnknownTopicId,
            "org.apache.kafka.common.errors.DuplicateBrokerRegistrationException" => Errors::DuplicateBrokerRegistration,
            "org.apache.kafka.common.errors.BrokerIdNotRegisteredException" => Errors::BrokerIdNotRegistered,
            "org.apache.kafka.common.errors.InconsistentTopicIdException" => Errors::InconsistentTopicId,
            "org.apache.kafka.common.errors.InconsistentClusterIdException" => Errors::InconsistentClusterId,
            "org.apache.kafka.common.errors.TransactionalIdNotFoundException" => Errors::TransactionalIdNotFound,
            "org.apache.kafka.common.errors.FetchSessionTopicIdException" => Errors::FetchSessionTopicIdError,
            "org.apache.kafka.common.errors.IneligibleReplicaException" => Errors::IneligibleReplica,
            "org.apache.kafka.common.errors.NewLeaderElectedException" => Errors::NewLeaderElected,
            "org.apache.kafka.common.errors.OffsetMovedToTieredStorageException" => Errors::OffsetMovedToTieredStorage,
            "org.apache.kafka.common.errors.FencedMemberEpochException" => Errors::FencedMemberEpoch,
            "org.apache.kafka.common.errors.UnreleasedInstanceIdException" => Errors::UnreleasedInstanceId,
            "org.apache.kafka.common.errors.UnsupportedAssignorException" => Errors::UnsupportedAssignor,
            "org.apache.kafka.common.errors.StaleMemberEpochException" => Errors::StaleMemberEpoch,
            "org.apache.kafka.common.errors.MismatchedEndpointTypeException" => Errors::MismatchedEndpointType,
            "org.apache.kafka.common.errors.UnsupportedEndpointTypeException" => Errors::UnsupportedEndpointType,
            "org.apache.kafka.common.errors.UnknownControllerIdException" => Errors::UnknownControllerId,
            "org.apache.kafka.common.errors.UnknownSubscriptionIdException" => Errors::UnknownSubscriptionId,
            "org.apache.kafka.common.errors.TelemetryTooLargeException" => Errors::TelemetryTooLarge,
            "org.apache.kafka.common.errors.InvalidRegistrationException" => Errors::InvalidRegistration,
            "org.apache.kafka.common.errors.TransactionAbortableException" => Errors::TransactionAbortable,
            "org.apache.kafka.common.errors.InvalidRecordStateException" => Errors::InvalidRecordState,
            "org.apache.kafka.common.errors.ShareSessionNotFoundException" => Errors::ShareSessionNotFound,
            "org.apache.kafka.common.errors.InvalidShareSessionEpochException" => Errors::InvalidShareSessionEpoch,
            "org.apache.kafka.common.errors.FencedStateEpochException" => Errors::FencedStateEpoch,
            "org.apache.kafka.common.errors.InvalidVoterKeyException" => Errors::InvalidVoterKey,
            "org.apache.kafka.common.errors.DuplicateVoterException" => Errors::DuplicateVoter,
            "org.apache.kafka.common.errors.VoterNotFoundException" => Errors::VoterNotFound,
            "org.apache.kafka.common.errors.InvalidRegularExpression" => Errors::InvalidRegularExpression,
            "org.apache.kafka.common.errors.RebootstrapRequiredException" => Errors::RebootstrapRequired,
            "org.apache.kafka.common.errors.StreamsInvalidTopologyException" => Errors::StreamsInvalidTopology,
            "org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException" => Errors::StreamsInvalidTopologyEpoch,
            "org.apache.kafka.common.errors.StreamsTopologyFencedException" => Errors::StreamsTopologyFenced,
            "org.apache.kafka.common.errors.ShareSessionLimitReachedException" => Errors::ShareSessionLimitReached,
            "org.apache.kafka.common.errors.GroupDeletionFailedException" => Errors::GroupDeletionFailed,
            "org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException" => Errors::StreamsTopologyDescriptionUpdateFailed,
            "org.apache.kafka.common.errors.ControllerIdNotRegisteredException" => Errors::ControllerIdNotRegistered,
            #[allow(unreachable_patterns)]
            _ => Errors::UnknownServerError,
        }
    }

    /// Throw the exception corresponding to this error if there is one.
    ///
    /// In Rust this converts to a `Result` — `Ok(())` for `None`, `Err` otherwise.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
    pub fn maybe_throw(&self) -> Result<(), Errors> {
        match self {
            Errors::None => Ok(()),
            other => Err(*other),
        }
    }
}
