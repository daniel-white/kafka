//! Directory ID constants and utilities for Kafka replica placement.
//!
//! `DirectoryId` is a utility class (all static methods) that manages
//! reserved UUIDs for special log directory states.
//!
//! MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java

use kafka_common::uuid::Uuid;

/// Reserved directory IDs and utility methods.
///
/// All methods are static — mirrors Java's `DirectoryId` utility class
/// (no instances needed).
///
/// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
pub struct DirectoryId;

impl DirectoryId {
    /// Represents an unspecified log directory that was previously selected.
    ///
    /// Mirrors Java's `DirectoryId.MIGRATING`.
    pub const MIGRATING: Uuid = Uuid::new(0, 0);

    /// Represents directories pending assignment.
    ///
    /// Mirrors Java's `DirectoryId.UNASSIGNED`.
    pub const UNASSIGNED: Uuid = Uuid::new(0, 1);

    /// Represents unspecified offline directories.
    ///
    /// Mirrors Java's `DirectoryId.LOST`.
    pub const LOST: Uuid = Uuid::new(0, 2);

    /// Check if a directory ID is part of the first 100 reserved IDs.
    ///
    /// A UUID is reserved if its most significant bits are 0 and its
    /// least significant bits are < 100.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn is_reserved(uuid: &Uuid) -> bool {
        uuid.get_most_significant_bits() == 0 && uuid.get_least_significant_bits() < 100
    }

    /// Generate a random directory ID, avoiding reserved IDs.
    ///
    /// Mirrors Java's `DirectoryId.random()`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn random() -> Uuid {
        loop {
            let uuid = Uuid::random_uuid();
            if !Self::is_reserved(&uuid) {
                return uuid;
            }
        }
    }

    /// Build a mapping from broker ID to directory UUID.
    ///
    /// Mirrors Java's `DirectoryId.createAssignmentMap()`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn create_assignment_map(
        replicas: &[i32],
        directories: &[Uuid],
    ) -> Result<std::collections::HashMap<i32, Uuid>, String> {
        if replicas.len() != directories.len() {
            return Err(
                "The lengths for replicas and directories do not match.".to_string(),
            );
        }
        let mut assignments = std::collections::HashMap::new();
        for (i, &broker_id) in replicas.iter().enumerate() {
            if assignments.insert(broker_id, directories[i]).is_some() {
                return Err("Duplicate broker ID in assignment".to_string());
            }
        }
        Ok(assignments)
    }

    /// Create a slice with `length` entries set to `UNASSIGNED`.
    ///
    /// Mirrors Java's `DirectoryId.unassignedArray()`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn unassigned_array(length: usize) -> Vec<Uuid> {
        vec![Self::UNASSIGNED; length]
    }

    /// Create a slice with `length` entries set to `MIGRATING`.
    ///
    /// Mirrors Java's `DirectoryId.migratingArray()`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn migrating_array(length: usize) -> Vec<Uuid> {
        vec![Self::MIGRATING; length]
    }

    /// Check if a directory is online, given a sorted list of online directories.
    ///
    /// Mirrors Java's `DirectoryId.isOnline()`.
    ///
    /// MIGRATION_SOURCE: server-common/src/main/java/org/apache/kafka/common/DirectoryId.java
    pub fn is_online(dir: &Uuid, sorted_online_dirs: &[Uuid]) -> bool {
        if dir == &Self::UNASSIGNED || dir == &Self::MIGRATING {
            return true;
        }
        if dir == &Self::LOST {
            return false;
        }
        if sorted_online_dirs.is_empty() {
            return true;
        }
        sorted_online_dirs.binary_search(dir).is_ok()
    }
}
