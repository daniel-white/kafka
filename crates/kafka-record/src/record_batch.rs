//! Record batch constants and header.
//!
//! Mirrors `clients/src/main/java/org/apache/kafka/common/record/internal/RecordBatch.java`
//! and `clients/src/main/java/org/apache/kafka/common/record/internal/DefaultRecordBatch.java`.

pub const MAGIC_VALUE_V0: i8 = 0;
pub const MAGIC_VALUE_V1: i8 = 1;
pub const MAGIC_VALUE_V2: i8 = 2;

pub const CURRENT_MAGIC_VALUE: i8 = MAGIC_VALUE_V2;

pub const NO_TIMESTAMP: i64 = -1;
pub const NO_PRODUCER_ID: i64 = -1;
pub const NO_PRODUCER_EPOCH: i16 = -1;
pub const NO_SEQUENCE: i32 = -1;
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;

pub const TIMESTAMP_TYPE_MASK: i16 = 0x08;
pub const COMPRESSION_CODEC_MASK: i16 = 0x07;
pub const TRANSACTIONAL_FLAG_MASK: i16 = 0x10;
pub const CONTROL_FLAG_MASK: i16 = 0x20;
pub const DELETE_HORIZON_FLAG_MASK: i16 = 0x40;

pub const NO_TIMESTAMP_TYPE: i32 = -1;
pub const CREATE_TIME: i32 = 0;
pub const LOG_APPEND_TIME: i32 = 1;

pub const NULL_VARINT_SIZE_BYTES: usize = 1;
pub const MAX_RECORD_OVERHEAD: i32 = 21;
pub const RECORD_SIZE_UPPER_BOUND_FIXED_OVERHEAD: usize = 21;

// Records.java offsets
pub const OFFSET_OFFSET: usize = 0;
pub const OFFSET_LENGTH: usize = 8;
pub const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
pub const SIZE_LENGTH: usize = 4;
pub const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;

// DefaultRecordBatch.java offsets
pub const PARTITION_LEADER_EPOCH_OFFSET: usize = LOG_OVERHEAD;
pub const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
pub const MAGIC_OFFSET: usize = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
pub const MAGIC_LENGTH: usize = 1;
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_LENGTH: usize = 4;
pub const ATTRIBUTES_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTES_LENGTH: usize = 2;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const BASE_TIMESTAMP_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BASE_TIMESTAMP_LENGTH: usize = 8;
pub const MAX_TIMESTAMP_OFFSET: usize = BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH;
pub const MAX_TIMESTAMP_LENGTH: usize = 8;
pub const PRODUCER_ID_OFFSET: usize = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
pub const PRODUCER_ID_LENGTH: usize = 8;
pub const PRODUCER_EPOCH_OFFSET: usize = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
pub const PRODUCER_EPOCH_LENGTH: usize = 2;
pub const BASE_SEQUENCE_OFFSET: usize = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
pub const BASE_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_OFFSET: usize = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
pub const RECORDS_COUNT_LENGTH: usize = 4;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const RECORD_BATCH_OVERHEAD: usize = RECORDS_OFFSET;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatchHeader {
    pub base_offset: i64,
    pub length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records_count: i32,
}

impl RecordBatchHeader {
    #[allow(clippy::too_many_arguments)]
    pub fn new_v2(
        base_offset: i64,
        last_offset_delta: i32,
        partition_leader_epoch: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records_count: i32,
        attributes: i16,
        base_timestamp: i64,
        max_timestamp: i64,
    ) -> Self {
        RecordBatchHeader {
            base_offset,
            length: 0,
            partition_leader_epoch,
            magic: MAGIC_VALUE_V2,
            crc: 0,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        buf[OFFSET_OFFSET..OFFSET_OFFSET + OFFSET_LENGTH]
            .copy_from_slice(&self.base_offset.to_be_bytes());
        buf[SIZE_OFFSET..SIZE_OFFSET + SIZE_LENGTH]
            .copy_from_slice(&self.length.to_be_bytes());
        buf[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH]
            .copy_from_slice(&self.partition_leader_epoch.to_be_bytes());
        buf[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LENGTH]
            .copy_from_slice(&[self.magic as u8]);
        buf[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH]
            .copy_from_slice(&self.crc.to_be_bytes());
        buf[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH]
            .copy_from_slice(&self.attributes.to_be_bytes());
        buf[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH]
            .copy_from_slice(&self.last_offset_delta.to_be_bytes());
        buf[BASE_TIMESTAMP_OFFSET..BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH]
            .copy_from_slice(&self.base_timestamp.to_be_bytes());
        buf[MAX_TIMESTAMP_OFFSET..MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH]
            .copy_from_slice(&self.max_timestamp.to_be_bytes());
        buf[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH]
            .copy_from_slice(&self.producer_id.to_be_bytes());
        buf[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH]
            .copy_from_slice(&self.producer_epoch.to_be_bytes());
        buf[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH]
            .copy_from_slice(&self.base_sequence.to_be_bytes());
        buf[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH]
            .copy_from_slice(&self.records_count.to_be_bytes());
    }

    pub fn decode(buf: &[u8]) -> Self {
        RecordBatchHeader {
            base_offset: i64::from_be_bytes(
                buf[OFFSET_OFFSET..OFFSET_OFFSET + OFFSET_LENGTH].try_into().unwrap(),
            ),
            length: i32::from_be_bytes(
                buf[SIZE_OFFSET..SIZE_OFFSET + SIZE_LENGTH].try_into().unwrap(),
            ),
            partition_leader_epoch: i32::from_be_bytes(
                buf[PARTITION_LEADER_EPOCH_OFFSET..PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            magic: buf[MAGIC_OFFSET] as i8,
            crc: u32::from_be_bytes(
                buf[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].try_into().unwrap(),
            ),
            attributes: i16::from_be_bytes(
                buf[ATTRIBUTES_OFFSET..ATTRIBUTES_OFFSET + ATTRIBUTES_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            last_offset_delta: i32::from_be_bytes(
                buf[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            base_timestamp: i64::from_be_bytes(
                buf[BASE_TIMESTAMP_OFFSET..BASE_TIMESTAMP_OFFSET + BASE_TIMESTAMP_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            max_timestamp: i64::from_be_bytes(
                buf[MAX_TIMESTAMP_OFFSET..MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            producer_id: i64::from_be_bytes(
                buf[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            producer_epoch: i16::from_be_bytes(
                buf[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            base_sequence: i32::from_be_bytes(
                buf[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
            records_count: i32::from_be_bytes(
                buf[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH]
                    .try_into()
                    .unwrap(),
            ),
        }
    }

    pub fn compute_crc(&mut self, records_bytes: &[u8]) {
        let mut crc_data = Vec::new();
        crc_data.extend_from_slice(&self.attributes.to_be_bytes());
        crc_data.extend_from_slice(&self.last_offset_delta.to_be_bytes());
        crc_data.extend_from_slice(&self.base_timestamp.to_be_bytes());
        crc_data.extend_from_slice(&self.max_timestamp.to_be_bytes());
        crc_data.extend_from_slice(&self.producer_id.to_be_bytes());
        crc_data.extend_from_slice(&self.producer_epoch.to_be_bytes());
        crc_data.extend_from_slice(&self.base_sequence.to_be_bytes());
        crc_data.extend_from_slice(&self.records_count.to_be_bytes());
        crc_data.extend_from_slice(records_bytes);
        self.crc = crc32c::crc32c(&crc_data);
    }

    pub fn size_in_bytes() -> usize {
        RECORD_BATCH_OVERHEAD
    }

    pub fn validate(&self) -> bool {
        self.magic >= MAGIC_VALUE_V2
    }
}

/// A decoded record batch that owns its full byte representation (header + records).
///
/// Mirrors `DefaultRecordBatch` which wraps a `ByteBuffer`.
pub struct DefaultRecordBatch {
    buffer: Vec<u8>,
}

impl DefaultRecordBatch {
    /// Create a batch from a header and serialized record bytes.
    ///
    /// Computes the CRC over the attributes-through-end range, matching
    /// `DefaultRecordBatch.writeHeader(...)` + `Crc32C.compute(...)`.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        base_offset: i64,
        last_offset_delta: i32,
        partition_leader_epoch: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        records_count: i32,
        attributes: i16,
        base_timestamp: i64,
        max_timestamp: i64,
        records: &[u8],
    ) -> Self {
        let records_size = records.len();
        let size_in_bytes = RECORD_BATCH_OVERHEAD + records_size;

        let mut header = RecordBatchHeader::new_v2(
            base_offset,
            last_offset_delta,
            partition_leader_epoch,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            attributes,
            base_timestamp,
            max_timestamp,
        );
        header.length = (size_in_bytes - LOG_OVERHEAD) as i32;
        header.compute_crc(records);

        let mut buffer = vec![0u8; size_in_bytes];
        header.encode(&mut buffer);
        buffer[RECORDS_OFFSET..RECORDS_OFFSET + records_size].copy_from_slice(records);

        DefaultRecordBatch { buffer }
    }

    /// Wrap an existing buffer as a decoded batch (no validation).
    pub fn from_bytes(buffer: Vec<u8>) -> Self {
        DefaultRecordBatch { buffer }
    }

    /// Returns the full byte representation of the batch.
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Total size including header and records.
    pub fn size_in_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Decode the header fields from the buffer.
    pub fn header(&self) -> RecordBatchHeader {
        RecordBatchHeader::decode(&self.buffer)
    }

    /// Validate the CRC: compute CRC over ATTRIBUTES_OFFSET..len and compare.
    pub fn validate(&self) -> bool {
        if self.buffer.len() < RECORD_BATCH_OVERHEAD {
            return false;
        }
        let stored_crc = u32::from_be_bytes(
            self.buffer[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH]
                .try_into()
                .unwrap(),
        );

        let crc_input = &self.buffer[ATTRIBUTES_OFFSET..];
        let computed_crc = crc32c::crc32c(crc_input);

        stored_crc == computed_crc && RecordBatchHeader::decode(&self.buffer).validate()
    }
}

