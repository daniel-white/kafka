//! Index entries and index containers for log segments.
//!
//! Mirrors `storage/src/main/java/org/apache/kafka/storage/internals/log/`.
//!
//! **OffsetIndex entry format (8 bytes):**
//!   - relative_offset  (i32, 4 bytes, big-endian)
//!   - physical_position (i32, 4 bytes, big-endian)
//!
//! **TimeIndex entry format (12 bytes):**
//!   - relative_offset  (i32, 4 bytes, big-endian)
//!   - timestamp_ms     (i64, 8 bytes, big-endian)

use crate::errors::StorageError;

pub const OFFSET_INDEX_ENTRY_SIZE: usize = 8;
pub const TIME_INDEX_ENTRY_SIZE: usize = 12;

/// An entry in the offset-to-position index.
///
/// Each entry is 8 bytes: 4-byte relative offset + 4-byte physical position.
/// Fields are private; access via getters (mirroring Java's private-field pattern).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetIndexEntry {
    relative_offset: i32,
    physical_position: i32,
}

impl OffsetIndexEntry {
    pub fn new(relative_offset: i32, physical_position: i32) -> Self {
        OffsetIndexEntry {
            relative_offset,
            physical_position,
        }
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        assert!(buf.len() >= OFFSET_INDEX_ENTRY_SIZE, "need {} bytes, got {}", OFFSET_INDEX_ENTRY_SIZE, buf.len());
        OffsetIndexEntry {
            relative_offset: i32::from_be_bytes(buf[0..4].try_into().unwrap()),
            physical_position: i32::from_be_bytes(buf[4..8].try_into().unwrap()),
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        assert!(buf.len() >= OFFSET_INDEX_ENTRY_SIZE);
        buf[0..4].copy_from_slice(&self.relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&self.physical_position.to_be_bytes());
    }

    pub fn relative_offset(&self) -> i32 {
        self.relative_offset
    }

    pub fn physical_position(&self) -> i32 {
        self.physical_position
    }

    pub fn index_key(&self) -> i64 {
        self.relative_offset as i64
    }

    pub fn index_value(&self) -> i64 {
        self.physical_position as i64
    }
}

/// An entry in the timestamp-to-offset index.
///
/// Each entry is 12 bytes: 4-byte relative offset + 8-byte timestamp (ms).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeIndexEntry {
    relative_offset: i32,
    timestamp_ms: i64,
}

impl TimeIndexEntry {
    pub fn new(relative_offset: i32, timestamp_ms: i64) -> Self {
        TimeIndexEntry {
            relative_offset,
            timestamp_ms,
        }
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        assert!(buf.len() >= TIME_INDEX_ENTRY_SIZE);
        TimeIndexEntry {
            relative_offset: i32::from_be_bytes(buf[0..4].try_into().unwrap()),
            timestamp_ms: i64::from_be_bytes(buf[4..12].try_into().unwrap()),
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        assert!(buf.len() >= TIME_INDEX_ENTRY_SIZE);
        buf[0..4].copy_from_slice(&self.relative_offset.to_be_bytes());
        buf[4..12].copy_from_slice(&self.timestamp_ms.to_be_bytes());
    }

    pub fn relative_offset(&self) -> i32 {
        self.relative_offset
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn index_key(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn index_value(&self) -> i64 {
        self.relative_offset as i64
    }
}

/// Trait for index entries that can be binary-searched.
pub trait IndexEntryTrait {
    fn index_key(&self) -> i64;
    fn index_value(&self) -> i64;
}

impl IndexEntryTrait for OffsetIndexEntry {
    fn index_key(&self) -> i64 {
        OffsetIndexEntry::index_key(self)
    }
    fn index_value(&self) -> i64 {
        OffsetIndexEntry::index_value(self)
    }
}

impl IndexEntryTrait for TimeIndexEntry {
    fn index_key(&self) -> i64 {
        TimeIndexEntry::index_key(self)
    }
    fn index_value(&self) -> i64 {
        TimeIndexEntry::index_value(self)
    }
}

/// Base offset for the index (all relative offsets are relative to this).
#[derive(Debug, Clone, Copy)]
pub struct BaseOffset {
    offset: i64,
}

impl BaseOffset {
    pub fn new(offset: i64) -> Self {
        BaseOffset { offset }
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
    pub fn relative_offset(&self, absolute: i64) -> i32 {
        (absolute - self.offset) as i32
    }
    pub fn absolute_offset(&self, relative: i32) -> i64 {
        self.offset + relative as i64
    }
}

/// Offset-to-position index, backed by a growable byte buffer.
///
/// Mirrors `OffsetIndex` which wraps a `MappedByteBuffer` of 8-byte entries.
/// This in-memory version stores entries as a `Vec<u8>` for byte-for-byte
/// format compatibility.
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/storage/internals/log/OffsetIndex.java
pub struct OffsetIndex {
    base_offset: BaseOffset,
    buffer: Vec<u8>,
    entry_count: i32,
}

impl OffsetIndex {
    pub fn new(base_offset: i64) -> Self {
        OffsetIndex {
            base_offset: BaseOffset::new(base_offset),
            buffer: Vec::new(),
            entry_count: 0,
        }
    }

    pub fn from_bytes(base_offset: i64, data: &[u8]) -> Result<Self, StorageError> {
        if !data.len().is_multiple_of(OFFSET_INDEX_ENTRY_SIZE) {
            return Err(StorageError::CorruptIndex {
                message: format!(
                    "Index file length {} is not a multiple of entry size {}",
                    data.len(),
                    OFFSET_INDEX_ENTRY_SIZE
                ),
            });
        }
        let entries = (data.len() / OFFSET_INDEX_ENTRY_SIZE) as i32;
        Ok(OffsetIndex {
            base_offset: BaseOffset::new(base_offset),
            buffer: data.to_vec(),
            entry_count: entries,
        })
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset.offset()
    }

    pub fn entries(&self) -> i32 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn last_offset(&self) -> i64 {
        if self.entry_count == 0 {
            self.base_offset.offset()
        } else {
            let last = self.entries_iter().last().unwrap();
            self.base_offset.absolute_offset(last.relative_offset())
        }
    }

    pub fn entries_iter(&self) -> impl Iterator<Item = OffsetIndexEntry> + '_ {
        (0..self.entry_count as usize).map(move |i| {
            let start = i * OFFSET_INDEX_ENTRY_SIZE;
            OffsetIndexEntry::from_bytes(&self.buffer[start..start + OFFSET_INDEX_ENTRY_SIZE])
        })
    }

    pub fn entry(&self, n: usize) -> Option<OffsetIndexEntry> {
        let start = n * OFFSET_INDEX_ENTRY_SIZE;
        if start + OFFSET_INDEX_ENTRY_SIZE > self.buffer.len() {
            return None;
        }
        Some(OffsetIndexEntry::from_bytes(
            &self.buffer[start..start + OFFSET_INDEX_ENTRY_SIZE],
        ))
    }

    pub fn append(&mut self, entry: OffsetIndexEntry) -> Result<(), StorageError> {
        let mut buf = [0u8; OFFSET_INDEX_ENTRY_SIZE];
        entry.encode(&mut buf);
        self.buffer.extend_from_slice(&buf);
        self.entry_count += 1;
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Find the largest entry whose key (relative_offset) is <= the given relative offset.
    /// Returns the physical position.
    pub fn lookup(&self, relative_offset: i64) -> Result<Option<i32>, StorageError> {
        if self.entry_count == 0 {
            return Ok(None);
        }
        // Binary search for the largest entry whose relative_offset <= target
        let mut low = 0i64;
        let mut high = self.entry_count as i64 - 1;
        while low < high {
            let mid = (low + high + 1) / 2;
            let entry = self.entry(mid as usize).unwrap();
            if entry.relative_offset() as i64 <= relative_offset {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let entry = self.entry(low as usize).unwrap();
        if entry.relative_offset() as i64 > relative_offset {
            Ok(None)
        } else {
            Ok(Some(entry.physical_position()))
        }
    }
}

/// Timestamp-to-offset index, backed by a growable byte buffer.
///
/// Each entry is 12 bytes: 4-byte relative offset + 8-byte timestamp (ms).
///
/// MIGRATION_SOURCE: clients/src/main/java/org/apache/kafka/storage/internals/log/TimeIndex.java
pub struct TimeIndex {
    base_offset: BaseOffset,
    buffer: Vec<u8>,
    entry_count: i32,
}

impl TimeIndex {
    pub fn new(base_offset: i64) -> Self {
        TimeIndex {
            base_offset: BaseOffset::new(base_offset),
            buffer: Vec::new(),
            entry_count: 0,
        }
    }

    pub fn from_bytes(base_offset: i64, data: &[u8]) -> Result<Self, StorageError> {
        if !data.len().is_multiple_of(TIME_INDEX_ENTRY_SIZE) {
            return Err(StorageError::CorruptIndex {
                message: format!(
                    "Time index file length {} is not a multiple of entry size {}",
                    data.len(),
                    TIME_INDEX_ENTRY_SIZE
                ),
            });
        }
        let entries = (data.len() / TIME_INDEX_ENTRY_SIZE) as i32;
        Ok(TimeIndex {
            base_offset: BaseOffset::new(base_offset),
            buffer: data.to_vec(),
            entry_count: entries,
        })
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset.offset()
    }

    pub fn entries(&self) -> i32 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn entries_iter(&self) -> impl Iterator<Item = TimeIndexEntry> + '_ {
        (0..self.entry_count as usize).map(move |i| {
            let start = i * TIME_INDEX_ENTRY_SIZE;
            TimeIndexEntry::from_bytes(&self.buffer[start..start + TIME_INDEX_ENTRY_SIZE])
        })
    }

    pub fn entry(&self, n: usize) -> Option<TimeIndexEntry> {
        let start = n * TIME_INDEX_ENTRY_SIZE;
        if start + TIME_INDEX_ENTRY_SIZE > self.buffer.len() {
            return None;
        }
        Some(TimeIndexEntry::from_bytes(
            &self.buffer[start..start + TIME_INDEX_ENTRY_SIZE],
        ))
    }

    pub fn append(&mut self, entry: TimeIndexEntry) -> Result<(), StorageError> {
        let mut buf = [0u8; TIME_INDEX_ENTRY_SIZE];
        entry.encode(&mut buf);
        self.buffer.extend_from_slice(&buf);
        self.entry_count += 1;
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Find the largest entry whose timestamp is <= the given timestamp.
    /// Returns the relative offset.
    pub fn lookup_by_timestamp(&self, timestamp_ms: i64) -> Result<Option<i32>, StorageError> {
        if self.entry_count == 0 {
            return Ok(None);
        }
        let mut low = 0i64;
        let mut high = self.entry_count as i64 - 1;
        while low < high {
            let mid = (low + high + 1) / 2;
            let entry = self.entry(mid as usize).unwrap();
            if entry.timestamp_ms() <= timestamp_ms {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let entry = self.entry(low as usize).unwrap();
        if entry.timestamp_ms() > timestamp_ms {
            Ok(None)
        } else {
            Ok(Some(entry.relative_offset()))
        }
    }
}
