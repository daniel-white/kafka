//! PartitionInfo: metadata for a topic partition.
//!
//! Mirrors Java's `PartitionInfo` class.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/PartitionInfo.java

use crate::Node;
use getset::{CopyGetters, Getters};
use std::fmt;

/// Information about a topic partition: leader, replicas, ISR, and offline replicas.
///
/// Fields are private; access via `getset` generated accessors.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/PartitionInfo.java
#[derive(Debug, Clone, CopyGetters, Getters)]
pub struct PartitionInfo {
    #[get = "pub"]
    topic: String,
    #[get_copy = "pub"]
    partition: i32,
    #[get = "pub"]
    leader: Option<Node>,
    #[get = "pub"]
    replicas: Vec<Node>,
    #[get = "pub"]
    in_sync_replicas: Vec<Node>,
    #[get = "pub"]
    offline_replicas: Vec<Node>,
}

impl PartitionInfo {
    /// Create a new PartitionInfo with no offline replicas.
    ///
    /// Mirrors Java's 5-arg constructor.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/PartitionInfo.java
    pub fn new(
        topic: String,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
    ) -> Self {
        PartitionInfo::with_offline(topic, partition, leader, replicas, in_sync_replicas, vec![])
    }

    /// Create a new PartitionInfo with all replica lists specified.
    ///
    /// Mirrors Java's 6-arg constructor.
    ///
    /// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/PartitionInfo.java
    pub fn with_offline(
        topic: String,
        partition: i32,
        leader: Option<Node>,
        replicas: Vec<Node>,
        in_sync_replicas: Vec<Node>,
        offline_replicas: Vec<Node>,
    ) -> Self {
        PartitionInfo {
            topic,
            partition,
            leader,
            replicas,
            in_sync_replicas,
            offline_replicas,
        }
    }
}

impl PartialEq for PartitionInfo {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic
            && self.partition == other.partition
            && self.leader == other.leader
            && self.replicas == other.replicas
            && self.in_sync_replicas == other.in_sync_replicas
            && self.offline_replicas == other.offline_replicas
    }
}

impl Eq for PartitionInfo {}

impl fmt::Display for PartitionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leader_str = match &self.leader {
            Some(n) => n.id_string().to_string(),
            None => "none".to_string(),
        };
        let replicas_str = format_node_ids(&self.replicas);
        let isr_str = format_node_ids(&self.in_sync_replicas);
        let offline_str = format_node_ids(&self.offline_replicas);
        write!(
            f,
            "Partition(topic = {}, partition = {}, leader = {}, replicas = {}, isr = {}, offlineReplicas = {})",
            self.topic, self.partition, leader_str, replicas_str, isr_str, offline_str
        )
    }
}

fn format_node_ids(nodes: &[Node]) -> String {
    let ids: Vec<String> = nodes.iter().map(|n| n.id_string().to_string()).collect();
    format!("[{}]", ids.join(","))
}

impl Default for PartitionInfo {
    fn default() -> Self {
        PartitionInfo::new(String::new(), -1, None, vec![], vec![])
    }
}
