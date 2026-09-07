//! Protocol type definitions, mirroring `clients/src/main/java/org/apache/kafka/common/protocol/types/Type.java`.
//!
//! Each variant knows how to read/write/compute-size on any `Reader`/`Writer`
//! via generic parameter dispatch (no `dyn Trait`), per the no-virtual-dispatch rule.

use crate::io::reader::Reader;
use crate::io::writer::Writer;
use kafka_common::uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Boolean,
    Int8,
    Int16,
    UInt16,
    Int32,
    UnsignedInt32,
    Int64,
    Float64,
    Uuid,
    String,
    CompactString,
    NullableString,
    CompactNullableString,
    Bytes,
    CompactBytes,
    NullableBytes,
    CompactNullableBytes,
    Varint,
    Varlong,
    ArrayOf(Box<Type>),
    CompactArrayOf(Box<Type>),
    NullableArrayOf(Box<Type>),
    CompactNullableArrayOf(Box<Type>),
}

impl Type {
    pub fn type_name(&self) -> &'static str {
        match self {
            Type::Boolean => "BOOLEAN",
            Type::Int8 => "INT8",
            Type::Int16 => "INT16",
            Type::UInt16 => "UINT16",
            Type::Int32 => "INT32",
            Type::UnsignedInt32 => "UINT32",
            Type::Int64 => "INT64",
            Type::Float64 => "FLOAT64",
            Type::Uuid => "UUID",
            Type::String => "STRING",
            Type::CompactString => "COMPACT_STRING",
            Type::NullableString => "NULLABLE_STRING",
            Type::CompactNullableString => "COMPACT_NULLABLE_STRING",
            Type::Bytes => "BYTES",
            Type::CompactBytes => "COMPACT_BYTES",
            Type::NullableBytes => "NULLABLE_BYTES",
            Type::CompactNullableBytes => "COMPACT_NULLABLE_BYTES",
            Type::Varint => "VARINT",
            Type::Varlong => "VARLONG",
            Type::ArrayOf(t) => t.type_name(),
            Type::CompactArrayOf(t) => t.type_name(),
            Type::NullableArrayOf(t) => t.type_name(),
            Type::CompactNullableArrayOf(t) => t.type_name(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        matches!(
            self,
            Type::NullableString
                | Type::CompactNullableString
                | Type::NullableBytes
                | Type::CompactNullableBytes
                | Type::NullableArrayOf(_)
                | Type::CompactNullableArrayOf(_)
        )
    }

    pub fn array_element_type(&self) -> Option<&Type> {
        match self {
            Type::ArrayOf(t) | Type::CompactArrayOf(t) | Type::NullableArrayOf(t) | Type::CompactNullableArrayOf(t) => Some(t),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        self.array_element_type().is_some()
    }

    pub fn read_bool<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<bool> {
        Ok(r.read_byte()? != 0)
    }

    pub fn read_int8<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i8> {
        Ok(r.read_byte()? as i8)
    }

    pub fn read_int16<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i16> {
        Ok(r.read_short()?)
    }

    pub fn read_uint16<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<u16> {
        let v = r.read_short()?;
        Ok(v as u16)
    }

    pub fn read_int32<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i32> {
        Ok(r.read_int()?)
    }

    pub fn read_uint32<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<u32> {
        let v = r.read_int()?;
        Ok(v as u32)
    }

    pub fn read_int64<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i64> {
        Ok(r.read_long()?)
    }

    pub fn read_float64<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<f64> {
        Ok(r.read_double()?)
    }

    pub fn read_uuid<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Uuid> {
        let most = r.read_long()?;
        let least = r.read_long()?;
        Ok(Uuid::new(most, least))
    }

    pub fn read_string<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<String> {
        let len = r.read_short()?;
        if len < 0 {
            return Err(kafka_errors::KafkaError::InvalidRecord {
                message: "String length cannot be negative".into(),
            });
        }
        Ok(r.read_string(len as usize)?)
    }

    pub fn read_compact_string<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<String> {
        let len = r.read_unsigned_varint()?;
        if len == 0 {
            return Err(kafka_errors::KafkaError::InvalidRecord {
                message: "Compact string length 0 is not valid".into(),
            });
        }
        let actual_len = (len - 1) as usize;
        Ok(r.read_string(actual_len)?)
    }

    pub fn read_nullable_string<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Option<String>> {
        let len = r.read_short()?;
        if len < 0 {
            return Ok(None);
        }
        Ok(Some(r.read_string(len as usize)?))
    }

    pub fn read_compact_nullable_string<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Option<String>> {
        let len = r.read_unsigned_varint()?;
        if len == 0 {
            return Ok(None);
        }
        let actual_len = (len - 1) as usize;
        Ok(Some(r.read_string(actual_len)?))
    }

    pub fn read_bytes<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Vec<u8>> {
        let size = r.read_int()?;
        if size < 0 {
            return Err(kafka_errors::KafkaError::InvalidRecord {
                message: format!("Bytes size {} cannot be negative", size),
            });
        }
        Ok(r.read_bytes_array(size as usize)?)
    }

    pub fn read_compact_bytes<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Vec<u8>> {
        let size = r.read_unsigned_varint()?;
        if size == 0 {
            return Err(kafka_errors::KafkaError::InvalidRecord {
                message: "Compact bytes length 0 is not valid".into(),
            });
        }
        let actual_size = (size - 1) as usize;
        Ok(r.read_bytes_array(actual_size)?)
    }

    pub fn read_nullable_bytes<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Option<Vec<u8>>> {
        let size = r.read_int()?;
        if size < 0 {
            return Ok(None);
        }
        Ok(Some(r.read_bytes_array(size as usize)?))
    }

    pub fn read_compact_nullable_bytes<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<Option<Vec<u8>>> {
        let size = r.read_unsigned_varint()?;
        if size == 0 {
            return Ok(None);
        }
        let actual_size = (size - 1) as usize;
        Ok(Some(r.read_bytes_array(actual_size)?))
    }

    pub fn read_varint<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i32> {
        Ok(r.read_varint()?)
    }

    pub fn read_varlong<R: Reader>(&self, r: &mut R) -> kafka_errors::Result<i64> {
        Ok(r.read_varlong()?)
    }

    pub fn write_bool<W: Writer>(&self, w: &mut W, val: bool) {
        w.write_byte(if val { 1 } else { 0 });
    }

    pub fn write_int8<W: Writer>(&self, w: &mut W, val: i8) {
        w.write_byte(val as u8);
    }

    pub fn write_int16<W: Writer>(&self, w: &mut W, val: i16) {
        w.write_short(val);
    }

    pub fn write_uint16<W: Writer>(&self, w: &mut W, val: u16) {
        w.write_unsigned_short(val);
    }

    pub fn write_int32<W: Writer>(&self, w: &mut W, val: i32) {
        w.write_int(val);
    }

    pub fn write_uint32<W: Writer>(&self, w: &mut W, val: u32) {
        w.write_unsigned_int(val);
    }

    pub fn write_int64<W: Writer>(&self, w: &mut W, val: i64) {
        w.write_long(val);
    }

    pub fn write_float64<W: Writer>(&self, w: &mut W, val: f64) {
        w.write_double(val);
    }

    pub fn write_uuid<W: Writer>(&self, w: &mut W, val: &Uuid) {
        w.write_long(val.get_most_significant_bits());
        w.write_long(val.get_least_significant_bits());
    }

    pub fn write_string<W: Writer>(&self, w: &mut W, val: &str) {
        let bytes = val.as_bytes();
        if bytes.len() > i16::MAX as usize {
            panic!("String length {} is larger than the maximum string length", bytes.len());
        }
        w.write_short(bytes.len() as i16);
        w.write_bytes(bytes);
    }

    pub fn write_compact_string<W: Writer>(&self, w: &mut W, val: &str) {
        let bytes = val.as_bytes();
        w.write_unsigned_varint(bytes.len() as u32 + 1);
        w.write_bytes(bytes);
    }

    pub fn write_nullable_string<W: Writer>(&self, w: &mut W, val: Option<&str>) {
        match val {
            None => w.write_short(-1),
            Some(s) => self.write_string(w, s),
        }
    }

    pub fn write_compact_nullable_string<W: Writer>(&self, w: &mut W, val: Option<&str>) {
        match val {
            None => w.write_unsigned_varint(0),
            Some(s) => self.write_compact_string(w, s),
        }
    }

    pub fn write_bytes<W: Writer>(&self, w: &mut W, val: &[u8]) {
        w.write_int(val.len() as i32);
        w.write_bytes(val);
    }

    pub fn write_compact_bytes<W: Writer>(&self, w: &mut W, val: &[u8]) {
        w.write_unsigned_varint(val.len() as u32 + 1);
        w.write_bytes(val);
    }

    pub fn write_nullable_bytes<W: Writer>(&self, w: &mut W, val: Option<&[u8]>) {
        match val {
            None => w.write_int(-1),
            Some(b) => self.write_bytes(w, b),
        }
    }

    pub fn write_compact_nullable_bytes<W: Writer>(&self, w: &mut W, val: Option<&[u8]>) {
        match val {
            None => w.write_unsigned_varint(0),
            Some(b) => self.write_compact_bytes(w, b),
        }
    }

    pub fn write_varint<W: Writer>(&self, w: &mut W, val: i32) {
        w.write_varint(val);
    }

    pub fn write_varlong<W: Writer>(&self, w: &mut W, val: i64) {
        w.write_varlong(val);
    }

    pub fn size_of_bool(&self) -> usize {
        1
    }

    pub fn size_of_int8(&self) -> usize {
        1
    }

    pub fn size_of_int16(&self) -> usize {
        2
    }

    pub fn size_of_uint16(&self) -> usize {
        2
    }

    pub fn size_of_int32(&self) -> usize {
        4
    }

    pub fn size_of_uint32(&self) -> usize {
        4
    }

    pub fn size_of_int64(&self) -> usize {
        8
    }

    pub fn size_of_float64(&self) -> usize {
        8
    }

    pub fn size_of_uuid(&self) -> usize {
        16
    }

    pub fn size_of_string(&self, val: &str) -> usize {
        2 + val.len()
    }

    pub fn size_of_compact_string(&self, val: &str) -> usize {
        Writer::size_of_compact_string(val)
    }

    pub fn size_of_nullable_string(&self, val: Option<&str>) -> usize {
        match val {
            None => 2,
            Some(s) => self.size_of_string(s),
        }
    }

    pub fn size_of_compact_nullable_string(&self, val: Option<&str>) -> usize {
        match val {
            None => 1,
            Some(s) => self.size_of_compact_string(s),
        }
    }

    pub fn size_of_bytes(&self, val: &[u8]) -> usize {
        4 + val.len()
    }

    pub fn size_of_compact_bytes(&self, val: &[u8]) -> usize {
        Writer::size_of_unsigned_varint(val.len() as u32 + 1) + val.len()
    }

    pub fn size_of_nullable_bytes(&self, val: Option<&[u8]>) -> usize {
        match val {
            None => 4,
            Some(b) => self.size_of_bytes(b),
        }
    }

    pub fn size_of_compact_nullable_bytes(&self, val: Option<&[u8]>) -> usize {
        match val {
            None => 1,
            Some(b) => self.size_of_compact_bytes(b),
        }
    }

    pub fn size_of_varint(&self, val: i32) -> usize {
        Writer::size_of_varint(val)
    }

    pub fn size_of_varlong(&self, val: i64) -> usize {
        Writer::size_of_varlong(val)
    }
}
