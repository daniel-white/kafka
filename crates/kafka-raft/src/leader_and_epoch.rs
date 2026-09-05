//! Leader and epoch pair for KRaft quorum state.
//!
//! A Java `record<OptionalInt, int>` with `UNKNOWN` sentinel.
//! Mirrors Java's `LeaderAndEpoch`.
//!
//! MIGRATION_SOURCE: raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java

use getset::CopyGetters;

/// Immutable pair of leader ID and epoch.
///
/// Mirrors `LeaderAndEpoch` Java record.
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, CopyGetters)]
pub struct LeaderAndEpoch {
    #[get_copy = "pub"]
    leader_id: Option<i32>,
    #[get_copy = "pub"]
    epoch: i32,
}

impl LeaderAndEpoch {
    /// Sentinel value representing an unknown leader.
    ///
    /// Mirrors `LeaderAndEpoch.UNKNOWN`.
    pub const UNKNOWN: LeaderAndEpoch = LeaderAndEpoch {
        leader_id: None,
        epoch: 0,
    };

    /// Create a new LeaderAndEpoch.
    ///
    /// MIGRATION_SOURCE: raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java
    pub const fn new(leader_id: Option<i32>, epoch: i32) -> Self {
        LeaderAndEpoch { leader_id, epoch }
    }

    /// Check if this leader matches the given node ID.
    ///
    /// Mirrors `isLeader(int nodeId)`.
    ///
    /// MIGRATION_SOURCE: raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java
    pub fn is_leader(&self, node_id: i32) -> bool {
        self.leader_id.is_some_and(|id| id == node_id)
    }
}

impl Default for LeaderAndEpoch {
    fn default() -> Self {
        LeaderAndEpoch::UNKNOWN
    }
}
