//! Share group persister data types: PartitionData and TopicData.
//!
//! Mirrors types from `server-common/src/main/java/org/apache/kafka/server/share/persister/`.
//!
//! MIGRATION_SOURCE:
//! - server-common/.../share/persister/PartitionData.java
//! - server-common/.../share/persister/TopicData.java

use crate::PersisterStateBatch;
use getset::{CopyGetters, Getters};

/// Data for a single partition in a share group persistence operation.
///
/// Fields are private; access via `getset` generated accessors.
/// Builder API mirrors Java's `PartitionData.Builder`.
///
/// MIGRATION_SOURCE: server-common/.../share/persister/PartitionData.java
#[derive(Debug, Clone, PartialEq, CopyGetters, Getters)]
pub struct PartitionData {
    #[get_copy = "pub"]
    partition: i32,
    #[get_copy = "pub"]
    state_epoch: i32,
    #[get_copy = "pub"]
    start_offset: i64,
    #[get_copy = "pub"]
    delivery_complete_count: i32,
    #[get_copy = "pub"]
    error_code: i16,
    #[get = "pub"]
    error_message: String,
    #[get_copy = "pub"]
    leader_epoch: i32,
    #[get = "pub"]
    state_batches: Vec<PersisterStateBatch>,
}

/// Builder for `PartitionData` mirroring Java's `PartitionData.Builder`.
///
/// MIGRATION_SOURCE: server-common/.../share/persister/PartitionData.java
#[derive(Debug, Clone, Default)]
pub struct PartitionDataBuilder {
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    error_code: i16,
    error_message: String,
    leader_epoch: i32,
    state_batches: Vec<PersisterStateBatch>,
}

impl PartitionData {
    /// Create a new PartitionData.
    ///
    /// Note: field order follows Java's constructor parameter order.
    ///
    /// MIGRATION_SOURCE: server-common/.../share/persister/PartitionData.java
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition: i32,
        state_epoch: i32,
        start_offset: i64,
        delivery_complete_count: i32,
        error_code: i16,
        error_message: String,
        leader_epoch: i32,
        state_batches: Vec<PersisterStateBatch>,
    ) -> Self {
        PartitionData {
            partition,
            state_epoch,
            start_offset,
            delivery_complete_count,
            error_code,
            error_message,
            leader_epoch,
            state_batches,
        }
    }

    /// Create a builder for fine-grained construction.
    ///
    /// MIGRATION_SOURCE: server-common/.../share/persister/PartitionData.java
    pub fn builder() -> PartitionDataBuilder {
        PartitionDataBuilder::default()
    }
}

impl PartitionDataBuilder {
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }

    pub fn state_epoch(mut self, state_epoch: i32) -> Self {
        self.state_epoch = state_epoch;
        self
    }

    pub fn start_offset(mut self, start_offset: i64) -> Self {
        self.start_offset = start_offset;
        self
    }

    pub fn delivery_complete_count(mut self, delivery_complete_count: i32) -> Self {
        self.delivery_complete_count = delivery_complete_count;
        self
    }

    pub fn error_code(mut self, error_code: i16) -> Self {
        self.error_code = error_code;
        self
    }

    pub fn error_message(mut self, error_message: String) -> Self {
        self.error_message = error_message;
        self
    }

    pub fn leader_epoch(mut self, leader_epoch: i32) -> Self {
        self.leader_epoch = leader_epoch;
        self
    }

    pub fn state_batches(mut self, state_batches: Vec<PersisterStateBatch>) -> Self {
        self.state_batches = state_batches;
        self
    }

    pub fn build(self) -> PartitionData {
        PartitionData::new(
            self.partition,
            self.state_epoch,
            self.start_offset,
            self.delivery_complete_count,
            self.error_code,
            self.error_message,
            self.leader_epoch,
            self.state_batches,
        )
    }
}

/// Data for a topic with its partitions in a share group persistence operation.
///
/// Generic over `P` (the partition info type), mirroring Java's `TopicData<P>`.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/.../share/persister/TopicData.java
#[derive(Debug, Clone, PartialEq, Eq, CopyGetters, Getters)]
pub struct TopicData<P> {
    #[get_copy = "pub"]
    topic_id: kafka_common::Uuid,
    #[get = "pub"]
    partitions: Vec<P>,
}

/// Group topic partition data: a group ID and its topic partition data.
///
/// Generic over `P` (the partition info type), mirroring Java's
/// `GroupTopicPartitionData<P>`.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: server-common/.../share/persister/GroupTopicPartitionData.java
#[derive(Debug, Clone, PartialEq, Eq, Getters)]
pub struct GroupTopicPartitionData<P> {
    #[get = "pub"]
    group_id: String,
    #[get = "pub"]
    topics_data: Vec<TopicData<P>>,
}

impl<P> GroupTopicPartitionData<P> {
    /// Create a new GroupTopicPartitionData.
    ///
    /// MIGRATION_SOURCE: server-common/.../share/persister/GroupTopicPartitionData.java
    pub fn new(group_id: String, topics_data: Vec<TopicData<P>>) -> Self {
        GroupTopicPartitionData {
            group_id,
            topics_data,
        }
    }
}

impl<P> Default for GroupTopicPartitionData<P> {
    fn default() -> Self {
        GroupTopicPartitionData {
            group_id: String::new(),
            topics_data: Vec::new(),
        }
    }
}

impl<P> TopicData<P> {
    /// Create a new TopicData.
    ///
    /// MIGRATION_SOURCE: server-common/.../share/persister/TopicData.java
    pub fn new(topic_id: kafka_common::Uuid, partitions: Vec<P>) -> Self {
        TopicData {
            topic_id,
            partitions,
        }
    }
}

impl<P> Default for TopicData<P> {
    fn default() -> Self {
        TopicData {
            topic_id: kafka_common::Uuid::ZERO_UUID,
            partitions: Vec::new(),
        }
    }
}
