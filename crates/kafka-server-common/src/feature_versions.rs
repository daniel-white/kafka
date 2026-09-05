//! Feature version enums for Kafka's version-gated features.
//!
//! Simplified Rust ports of Java's `ShareVersion`, `TransactionVersion`,
//! and `GroupVersion` enums. These enums track feature levels for
//! share groups, transactions, and consumer groups respectively.
//!
//! The Java originals depend on `MetadataVersion` for bootstrap checks.
//! The Rust port uses `i16` feature levels directly — `MetadataVersion`
//! integration is deferred.
//!
//! MIGRATION_SOURCE:
//! - server-common/src/main/java/org/apache/kafka/server/common/ShareVersion.java
//! - server-common/src/main/java/org/apache/kafka/server/common/TransactionVersion.java
//! - server-common/src/main/java/org/apache/kafka/server/common/GroupVersion.java

/// Share group feature versions.
///
/// Mirrors Java's `ShareVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/ShareVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ShareVersion {
    /// Version 0: share groups not enabled.
    V0,
    /// Version 1: share groups enabled (KIP-932).
    V1,
    /// Version 2: adds DLQ support (KIP-1191).
    V2,
    /// Unknown share version.
    #[default]
    Unknown,
}

impl ShareVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "share.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: ShareVersion = ShareVersion::V2;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            ShareVersion::V0 => 0,
            ShareVersion::V1 => 1,
            ShareVersion::V2 => 2,
            ShareVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    pub fn from_feature_level(level: i16) -> ShareVersion {
        match level {
            0 => ShareVersion::V0,
            1 => ShareVersion::V1,
            2 => ShareVersion::V2,
            _ => ShareVersion::Unknown,
        }
    }

    /// Check if this version supports share groups.
    pub fn has_share_groups(&self) -> bool {
        self.feature_level() >= ShareVersion::V1.feature_level()
    }

    /// Check if this version supports DLQ.
    pub fn has_dlq(&self) -> bool {
        self.feature_level() >= ShareVersion::V2.feature_level()
    }
}

/// Transaction feature versions.
///
/// Mirrors Java's `TransactionVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/TransactionVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionVersion {
    /// Version 0: original transaction coordinator.
    V0,
    /// Version 1: flexible transactional state records (KIP-890).
    V1,
    /// Version 2: epoch bump per transaction, optimizations (KIP-890).
    V2,
    /// Unknown transaction version.
    #[default]
    Unknown,
}

/// Unknown transaction version constant.
///
/// Mirrors Java's `TransactionVersion.TV_UNKNOWN`.
pub const TV_UNKNOWN: i16 = -1;

impl TransactionVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "transaction.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: TransactionVersion = TransactionVersion::V2;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            TransactionVersion::V0 => 0,
            TransactionVersion::V1 => 1,
            TransactionVersion::V2 => 2,
            TransactionVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    pub fn from_feature_level(level: i16) -> TransactionVersion {
        match level {
            0 => TransactionVersion::V0,
            1 => TransactionVersion::V1,
            2 => TransactionVersion::V2,
            _ => TransactionVersion::Unknown,
        }
    }

    /// Check if this version supports flexible transactional state records.
    pub fn has_flexible_transactions(&self) -> bool {
        self.feature_level() >= TransactionVersion::V1.feature_level()
    }
}

/// KRaft metadata protocol versions.
///
/// Mirrors Java's `KRaftVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/KRaftVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum KRaftVersion {
    /// Version 0: initial KRaft version.
    V0,
    /// Version 1: KIP-853 enabled.
    V1,
    /// Unknown KRaft version.
    #[default]
    Unknown,
}

impl KRaftVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "kraft.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: KRaftVersion = KRaftVersion::V1;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            KRaftVersion::V0 => 0,
            KRaftVersion::V1 => 1,
            KRaftVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    ///
    /// Mirrors Java's `fromFeatureLevel(short)`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/KRaftVersion.java
    pub fn from_feature_level(level: i16) -> KRaftVersion {
        match level {
            0 => KRaftVersion::V0,
            1 => KRaftVersion::V1,
            _ => panic!("Unknown KRaft feature level: {}", level),
        }
    }
}

/// Consumer group feature versions.
///
/// Mirrors Java's `GroupVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/GroupVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum GroupVersion {
    /// Version 0: original group coordinator.
    V0,
    /// Version 1: consumer rebalance protocol (KIP-848).
    V1,
    /// Unknown group version.
    #[default]
    Unknown,
}

impl GroupVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "group.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: GroupVersion = GroupVersion::V1;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            GroupVersion::V0 => 0,
            GroupVersion::V1 => 1,
            GroupVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    pub fn from_feature_level(level: i16) -> GroupVersion {
        match level {
            0 => GroupVersion::V0,
            1 => GroupVersion::V1,
            _ => GroupVersion::Unknown,
        }
    }

    /// Check if this version supports the consumer rebalance protocol.
    pub fn has_rebalance_protocol(&self) -> bool {
        self.feature_level() >= GroupVersion::V1.feature_level()
    }
}

/// Streams group feature versions.
///
/// Mirrors Java's `StreamsVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/StreamsVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StreamsVersion {
    /// Version 0: streams groups disabled.
    V0,
    /// Version 1: streams groups enabled (KIP-1071).
    V1,
    /// Unknown streams version.
    #[default]
    Unknown,
}

impl StreamsVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "streams.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: StreamsVersion = StreamsVersion::V1;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            StreamsVersion::V0 => 0,
            StreamsVersion::V1 => 1,
            StreamsVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    pub fn from_feature_level(level: i16) -> StreamsVersion {
        match level {
            0 => StreamsVersion::V0,
            1 => StreamsVersion::V1,
            _ => panic!("Unknown streams feature level: {}", level),
        }
    }

    /// Check if streams groups are supported.
    pub fn streams_group_supported(&self) -> bool {
        self.feature_level() >= StreamsVersion::V1.feature_level()
    }
}

/// Eligible Leader Replicas (ELR) feature versions.
///
/// Mirrors Java's `EligibleLeaderReplicasVersion` enum.
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/server/common/EligibleLeaderReplicasVersion.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EligibleLeaderReplicasVersion {
    /// Version 0: ELR disabled.
    V0,
    /// Version 1: ELR enabled (KIP-966).
    V1,
    /// Unknown ELR version.
    #[default]
    Unknown,
}

impl EligibleLeaderReplicasVersion {
    /// Feature name used in metadata.
    pub const FEATURE_NAME: &'static str = "eligible.leader.replicas.version";

    /// Latest production version.
    pub const LATEST_PRODUCTION: EligibleLeaderReplicasVersion = EligibleLeaderReplicasVersion::V1;

    /// Feature level as a short.
    pub fn feature_level(&self) -> i16 {
        match self {
            EligibleLeaderReplicasVersion::V0 => 0,
            EligibleLeaderReplicasVersion::V1 => 1,
            EligibleLeaderReplicasVersion::Unknown => -1,
        }
    }

    /// Parse from a feature level.
    pub fn from_feature_level(level: i16) -> EligibleLeaderReplicasVersion {
        match level {
            0 => EligibleLeaderReplicasVersion::V0,
            1 => EligibleLeaderReplicasVersion::V1,
            _ => panic!(
                "Unknown eligible leader replicas feature level: {}",
                level
            ),
        }
    }

    /// Check if ELR is enabled.
    pub fn is_elr_enabled(&self) -> bool {
        self.feature_level() >= EligibleLeaderReplicasVersion::V1.feature_level()
    }
}

