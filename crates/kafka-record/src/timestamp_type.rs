//! Timestamp type enum.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/TimestampType.java`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampType {
    NoTimestampType = -1,
    #[default]
    CreateTime = 0,
    LogAppendTime = 1,
}

impl TimestampType {
    pub fn id(&self) -> i32 {
        match self {
            TimestampType::NoTimestampType => -1,
            TimestampType::CreateTime => 0,
            TimestampType::LogAppendTime => 1,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            TimestampType::NoTimestampType => "NoTimestampType",
            TimestampType::CreateTime => "CreateTime",
            TimestampType::LogAppendTime => "LogAppendTime",
        }
    }

    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            -1 => Some(TimestampType::NoTimestampType),
            0 => Some(TimestampType::CreateTime),
            1 => Some(TimestampType::LogAppendTime),
            _ => None,
        }
    }
}
