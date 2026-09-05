//! Metadata record type enumeration.
//!
//! Maps API key → record type for KRaft metadata records.
//! Generated from `metadata/src/main/resources/common/metadata/*.json` specs.
//!
//! MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/message/MetadataRecordTypeGenerator.java
//!
//! API keys for metadata records are defined in:
//! /Users/daniel/Projects/kafka/metadata/src/main/resources/common/metadata/*.json

/// Metadata record types identified by their API key on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MetadataRecordType {
    #[default]
    RegisterBroker,
    UnregisterBroker,
    Topic,
    Partition,
    Config,
    PartitionChange,
    FenceBroker,
    UnfenceBroker,
    RemoveTopic,
    DelegationToken,
    UserScramCredential,
    FeatureLevel,
    ClientQuota,
    ProducerIds,
    BrokerRegistrationChange,
    AccessControlEntry,
    RemoveAccessControlEntry,
    NoOp,
    ZkMigration,
    RemoveUserScramCredential,
    BeginTransaction,
    EndTransaction,
    AbortTransaction,
    RemoveDelegationToken,
    RegisterController,
    ClearElr,
    UnregisterController,
}

impl MetadataRecordType {
    /// Convert an API key (short) to a MetadataRecordType.
    ///
    /// Returns `None` for unknown keys.
    pub fn from_api_key(api_key: i16) -> Option<Self> {
        match api_key {
            0 => Some(MetadataRecordType::RegisterBroker),
            1 => Some(MetadataRecordType::UnregisterBroker),
            2 => Some(MetadataRecordType::Topic),
            3 => Some(MetadataRecordType::Partition),
            4 => Some(MetadataRecordType::Config),
            5 => Some(MetadataRecordType::PartitionChange),
            7 => Some(MetadataRecordType::FenceBroker),
            8 => Some(MetadataRecordType::UnfenceBroker),
            9 => Some(MetadataRecordType::RemoveTopic),
            10 => Some(MetadataRecordType::DelegationToken),
            11 => Some(MetadataRecordType::UserScramCredential),
            12 => Some(MetadataRecordType::FeatureLevel),
            14 => Some(MetadataRecordType::ClientQuota),
            15 => Some(MetadataRecordType::ProducerIds),
            17 => Some(MetadataRecordType::BrokerRegistrationChange),
            18 => Some(MetadataRecordType::AccessControlEntry),
            19 => Some(MetadataRecordType::RemoveAccessControlEntry),
            20 => Some(MetadataRecordType::NoOp),
            21 => Some(MetadataRecordType::ZkMigration),
            22 => Some(MetadataRecordType::RemoveUserScramCredential),
            23 => Some(MetadataRecordType::BeginTransaction),
            24 => Some(MetadataRecordType::EndTransaction),
            25 => Some(MetadataRecordType::AbortTransaction),
            26 => Some(MetadataRecordType::RemoveDelegationToken),
            27 => Some(MetadataRecordType::RegisterController),
            28 => Some(MetadataRecordType::ClearElr),
            29 => Some(MetadataRecordType::UnregisterController),
            _ => None,
        }
    }

    /// Return the API key for this record type.
    pub fn api_key(&self) -> i16 {
        match self {
            MetadataRecordType::RegisterBroker => 0,
            MetadataRecordType::UnregisterBroker => 1,
            MetadataRecordType::Topic => 2,
            MetadataRecordType::Partition => 3,
            MetadataRecordType::Config => 4,
            MetadataRecordType::PartitionChange => 5,
            MetadataRecordType::FenceBroker => 7,
            MetadataRecordType::UnfenceBroker => 8,
            MetadataRecordType::RemoveTopic => 9,
            MetadataRecordType::DelegationToken => 10,
            MetadataRecordType::UserScramCredential => 11,
            MetadataRecordType::FeatureLevel => 12,
            MetadataRecordType::ClientQuota => 14,
            MetadataRecordType::ProducerIds => 15,
            MetadataRecordType::BrokerRegistrationChange => 17,
            MetadataRecordType::AccessControlEntry => 18,
            MetadataRecordType::RemoveAccessControlEntry => 19,
            MetadataRecordType::NoOp => 20,
            MetadataRecordType::ZkMigration => 21,
            MetadataRecordType::RemoveUserScramCredential => 22,
            MetadataRecordType::BeginTransaction => 23,
            MetadataRecordType::EndTransaction => 24,
            MetadataRecordType::AbortTransaction => 25,
            MetadataRecordType::RemoveDelegationToken => 26,
            MetadataRecordType::RegisterController => 27,
            MetadataRecordType::ClearElr => 28,
            MetadataRecordType::UnregisterController => 29,
        }
    }
}
