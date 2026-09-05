//! MemoryRecords: in-memory container for multiple record batches.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/MemoryRecords.java`.

use crate::errors::RecordError;
use crate::record_batch::{DefaultRecordBatch, RECORD_BATCH_OVERHEAD};

pub struct MemoryRecords {
    buffer: Vec<u8>,
}

impl MemoryRecords {
    pub fn from_bytes(buffer: Vec<u8>) -> Self {
        MemoryRecords { buffer }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Iterate over `DefaultRecordBatch` slices in order.
    ///
    /// Each batch starts at the current offset and has length determined by
    /// the `Length` field (`sizeInBytes - LOG_OVERHEAD + LOG_OVERHEAD = sizeInBytes`).
    pub fn batches(&self) -> impl Iterator<Item = Result<DefaultRecordBatch, RecordError>> + '_ {
        MemoryRecordsIter {
            buffer: &self.buffer,
            pos: 0,
        }
    }
}

struct MemoryRecordsIter<'a> {
    buffer: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for MemoryRecordsIter<'a> {
    type Item = Result<DefaultRecordBatch, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buffer.len() {
            return None;
        }

        if self.buffer.len() - self.pos < RECORD_BATCH_OVERHEAD {
            return Some(Err(RecordError::BatchTooSmall {
                size: (self.buffer.len() - self.pos) as i32,
                overhead: RECORD_BATCH_OVERHEAD as i32,
            }));
        }

        // Read the Length field to get the total batch size.
        // Length = sizeInBytes - LOG_OVERHEAD, so sizeInBytes = Length + LOG_OVERHEAD.
        let length = i32::from_be_bytes(
            self.buffer[self.pos + 8..self.pos + 12]
                .try_into()
                .unwrap(),
        );
        let batch_size = length as usize + crate::record_batch::LOG_OVERHEAD;

        if self.pos + batch_size > self.buffer.len() {
            return Some(Err(RecordError::InvalidRecordSize {
                expected: batch_size,
                actual: self.buffer.len() - self.pos,
            }));
        }

        let batch_bytes = self.buffer[self.pos..self.pos + batch_size].to_vec();
        self.pos += batch_size;

        Some(Ok(DefaultRecordBatch::from_bytes(batch_bytes)))
    }
}
