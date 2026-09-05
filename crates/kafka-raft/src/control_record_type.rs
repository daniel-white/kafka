//! Control record types used in the KRaft metadata log.
//!
//! These are special record keys that mark control records (not data records).
//! Each control record type has a short ID used as the record key.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java

use std::io;

/// Control record type for records in the KRaft metadata log and Kafka transaction log.
///
/// Each variant maps to a short ID on the wire. Used as the key of a control record.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ControlRecordType {
    /// Transaction abort marker (key = 0).
    Abort,
    /// Transaction commit marker (key = 1).
    Commit,
    /// Leader change control record (key = 2).
    LeaderChange,
    /// Snapshot header control record (key = 3).
    SnapshotHeader,
    /// Snapshot footer control record (key = 4).
    SnapshotFooter,
    /// KRaft version change control record (key = 5).
    KRaftVersion,
    /// KRaft voters change control record (key = 6).
    KRaftVoters,
    /// Unknown control record type — used to indicate a type not recognized by this version.
    ///
    /// Mirrors Java's `UNKNOWN`.
    #[default]
    Unknown,
}

impl ControlRecordType {
    /// Wire key (short) for this control record type.
    ///
    /// Returns -1 for `Unknown`.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java
    pub fn key(&self) -> i16 {
        match self {
            ControlRecordType::Abort => 0,
            ControlRecordType::Commit => 1,
            ControlRecordType::LeaderChange => 2,
            ControlRecordType::SnapshotHeader => 3,
            ControlRecordType::SnapshotFooter => 4,
            ControlRecordType::KRaftVersion => 5,
            ControlRecordType::KRaftVoters => 6,
            ControlRecordType::Unknown => -1,
        }
    }

    /// Parse a control record type from a short key.
    ///
    /// Returns `ControlRecordType::Unknown` for unrecognized keys.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java
    pub fn parse(key: i16) -> ControlRecordType {
        match key {
            0 => ControlRecordType::Abort,
            1 => ControlRecordType::Commit,
            2 => ControlRecordType::LeaderChange,
            3 => ControlRecordType::SnapshotHeader,
            4 => ControlRecordType::SnapshotFooter,
            5 => ControlRecordType::KRaftVersion,
            6 => ControlRecordType::KRaftVoters,
            _ => ControlRecordType::Unknown,
        }
    }

    /// Parse from a byte buffer at the given position.
    ///
    /// The buffer must contain at least 4 bytes: a short (version) followed by a short (type).
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/record/internal/ControlRecordType.java
    pub fn parse_from_bytes(buf: &[u8]) -> Result<ControlRecordType, io::Error> {
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid value size found for control record key. Must have at least 4 bytes, but found only {}",
                    buf.len()
                ),
            ));
        }
        let _version = i16::from_be_bytes([buf[0], buf[1]]);
        let key = i16::from_be_bytes([buf[2], buf[3]]);
        Ok(ControlRecordType::parse(key))
    }
}
