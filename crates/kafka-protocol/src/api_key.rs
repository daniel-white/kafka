//! Kafka API keys — maps integer api_key values to their enum variants.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java

use std::fmt;

/// API key for Kafka protocol requests.
///
/// Each variant corresponds to a request/response type in the Kafka protocol.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApiKey {
    Produce,
    Fetch,
    ListOffsets,
    Heartbeat,
    Leave,
    JoinGroup,
    ElectLeaders,
    AlterConfigs,
    DescribeConfigs,
    UpdateFeatures,
    UnavailableApiKeys,
    ListGroups,
    DeleteGroups,
    AddPartitionsToTxn,
    RemoveFromTxn,
    ApiVersions,
    CreateTopics,
    DeleteTopics,
    DescribeTopics,
    AlterReplicaLogDirs,
    DescribeAcls,
    DescribeUsers,
    AlterUserScramCredentials,
    DeleteRecords,
    AddQuotas,
    RemoveQuotas,
    AlterPartition,
    UnassignReplica,
    ModifyAcl,
    DescribeQuotas,
    /// Unknown API key (for unrecognized values).
    Unknown(i16),
}

impl ApiKey {
    /// Convert an integer API key to the corresponding variant.
    ///
    /// Returns `Unknown(key)` for unrecognized values.
    ///
    /// MIGRATION_SOURCE: clients/.../ApiKeys.java (fromId / forId)
    pub fn from_id(id: i16) -> Self {
        match id {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Heartbeat,
            4 => ApiKey::Leave,
            5 => ApiKey::JoinGroup,
            6 => ApiKey::ElectLeaders,
            7 => ApiKey::AlterConfigs,
            8 => ApiKey::DescribeConfigs,
            9 => ApiKey::UpdateFeatures,
            10 => ApiKey::UnavailableApiKeys,
            11 => ApiKey::ListGroups,
            12 => ApiKey::DeleteGroups,
            13 => ApiKey::AddPartitionsToTxn,
            14 => ApiKey::RemoveFromTxn,
            15 => ApiKey::ApiVersions,
            16 => ApiKey::CreateTopics,
            17 => ApiKey::DeleteTopics,
            18 => ApiKey::DescribeTopics,
            19 => ApiKey::AlterReplicaLogDirs,
            20 => ApiKey::DescribeAcls,
            21 => ApiKey::DescribeUsers,
            22 => ApiKey::AlterUserScramCredentials,
            23 => ApiKey::DeleteRecords,
            24 => ApiKey::AddQuotas,
            25 => ApiKey::RemoveQuotas,
            26 => ApiKey::AlterPartition,
            27 => ApiKey::UnassignReplica,
            28 => ApiKey::ModifyAcl,
            29 => ApiKey::DescribeQuotas,
            other => ApiKey::Unknown(other),
        }
    }

    /// Get the raw integer ID of this API key.
    ///
    /// MIGRATION_SOURCE: clients/.../ApiKeys.java (id())
    pub fn id(&self) -> i16 {
        match self {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Heartbeat => 3,
            ApiKey::Leave => 4,
            ApiKey::JoinGroup => 5,
            ApiKey::ElectLeaders => 6,
            ApiKey::AlterConfigs => 7,
            ApiKey::DescribeConfigs => 8,
            ApiKey::UpdateFeatures => 9,
            ApiKey::UnavailableApiKeys => 10,
            ApiKey::ListGroups => 11,
            ApiKey::DeleteGroups => 12,
            ApiKey::AddPartitionsToTxn => 13,
            ApiKey::RemoveFromTxn => 14,
            ApiKey::ApiVersions => 15,
            ApiKey::CreateTopics => 16,
            ApiKey::DeleteTopics => 17,
            ApiKey::DescribeTopics => 18,
            ApiKey::AlterReplicaLogDirs => 19,
            ApiKey::DescribeAcls => 20,
            ApiKey::DescribeUsers => 21,
            ApiKey::AlterUserScramCredentials => 22,
            ApiKey::DeleteRecords => 23,
            ApiKey::AddQuotas => 24,
            ApiKey::RemoveQuotas => 25,
            ApiKey::AlterPartition => 26,
            ApiKey::UnassignReplica => 27,
            ApiKey::ModifyAcl => 28,
            ApiKey::DescribeQuotas => 29,
            ApiKey::Unknown(id) => *id,
        }
    }

    /// Human-readable name used in the wire protocol.
    ///
    /// MIGRATION_SOURCE: clients/.../ApiKeys.java (name())
    pub fn name(&self) -> &'static str {
        match self {
            ApiKey::Produce => "Produce",
            ApiKey::Fetch => "Fetch",
            ApiKey::ListOffsets => "ListOffsets",
            ApiKey::Heartbeat => "Heartbeat",
            ApiKey::Leave => "Leave",
            ApiKey::JoinGroup => "JoinGroup",
            ApiKey::ElectLeaders => "ElectLeaders",
            ApiKey::AlterConfigs => "AlterConfigs",
            ApiKey::DescribeConfigs => "DescribeConfigs",
            ApiKey::UpdateFeatures => "UpdateFeatures",
            ApiKey::UnavailableApiKeys => "UnavailableApiKeys",
            ApiKey::ListGroups => "ListGroups",
            ApiKey::DeleteGroups => "DeleteGroups",
            ApiKey::AddPartitionsToTxn => "AddPartitionsToTxn",
            ApiKey::RemoveFromTxn => "RemoveFromTxn",
            ApiKey::ApiVersions => "ApiVersions",
            ApiKey::CreateTopics => "CreateTopics",
            ApiKey::DeleteTopics => "DeleteTopics",
            ApiKey::DescribeTopics => "DescribeTopics",
            ApiKey::AlterReplicaLogDirs => "AlterReplicaLogDirs",
            ApiKey::DescribeAcls => "DescribeAcls",
            ApiKey::DescribeUsers => "DescribeUsers",
            ApiKey::AlterUserScramCredentials => "AlterUserScramCredentials",
            ApiKey::DeleteRecords => "DeleteRecords",
            ApiKey::AddQuotas => "AddQuotas",
            ApiKey::RemoveQuotas => "RemoveQuotas",
            ApiKey::AlterPartition => "AlterPartition",
            ApiKey::UnassignReplica => "UnassignReplica",
            ApiKey::ModifyAcl => "ModifyAcl",
            ApiKey::DescribeQuotas => "DescribeQuotas",
            ApiKey::Unknown(_) => "Unknown",
        }
    }

    /// Highest supported version for this API key.
    ///
    /// These values track the latest wire protocol version implemented.
    ///
    /// MIGRATION_SOURCE: clients/.../ApiKeys.java
    pub fn max_version(&self) -> i16 {
        match self {
            ApiKey::Produce => 13,
            ApiKey::Fetch => 16,
            ApiKey::ListOffsets => 5,
            ApiKey::ApiVersions => 5,
            _ => 0,
        }
    }

    /// Lowest supported version for this API key.
    ///
    /// MIGRATION_SOURCE: clients/.../ApiKeys.java
    pub fn min_version(&self) -> i16 {
        0
    }
}

impl fmt::Display for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
