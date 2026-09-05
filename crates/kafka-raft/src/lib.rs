//! Raft consensus layer types.
//!
//! Migrates:
//! - `LeaderAndEpoch` from raft/src/main/java/org/apache/kafka/raft/
//! - `ControlRecordType` from clients/src/main/java/org/apache/kafka/common/record/internal/
//!
//! MIGRATION_SOURCE: raft/src/main/java/org/apache/kafka/raft/LeaderAndEpoch.java
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java

pub mod control_record_type;
pub mod leader_and_epoch;

pub use control_record_type::ControlRecordType;
pub use leader_and_epoch::LeaderAndEpoch;
