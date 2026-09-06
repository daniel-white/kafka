//! Tagged fields section for flexible protocol messages (v2+).
//!
//! Mirrors `org.apache.kafka.common.protocol.types.TaggedFields`.
//!
//! Wire format (for flexible versions):
//!   count   unsigned varint — number of tagged fields
//!   for each field:
//!     tag   unsigned varint — tag number (field index)
//!     size  unsigned varint — size of the field payload
//!     data  <size> bytes    — raw field payload
//!
//! In the generated `MessageData` code, known tagged fields are deserialized
//! as typed values; unknown ones become `RawTaggedField` entries.

use crate::byte_utils;
use crate::raw_tagged_field::RawTaggedField;
use crate::reader::Reader;
use crate::writer::Writer;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedFields {
    fields: BTreeMap<i32, Vec<RawTaggedField>>,
}

impl TaggedFields {
    pub fn new() -> Self {
        TaggedFields {
            fields: BTreeMap::new(),
        }
    }

    pub fn empty() -> Self {
        TaggedFields::new()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn add(&mut self, field: RawTaggedField) {
        let tag = field.tag();
        self.fields.entry(*tag).or_default().push(field);
    }

    pub fn get(&self, tag: i32) -> Option<&[RawTaggedField]> {
        self.fields.get(&tag).map(|v| v.as_slice())
    }

    pub fn all_fields(&self) -> impl Iterator<Item = &RawTaggedField> {
        self.fields.values().flatten()
    }

    /// Read tagged fields from `readable`.
    /// Returns the number of bytes read.
    pub fn read_from<R: Reader>(readable: &mut R) -> Result<Self, crate::errors::ProtocolError> {
        let count = readable.read_unsigned_varint()?;
        let mut fields: BTreeMap<i32, Vec<RawTaggedField>> = BTreeMap::new();
        for _ in 0..count {
            let tag = readable.read_unsigned_varint()?;
            let size = readable.read_unsigned_varint()?;
            let data = readable.read_array(size as usize)?;
            let field = RawTaggedField::new(tag as i32, data);
            fields.entry(*field.tag()).or_default().push(field);
        }
        Ok(TaggedFields { fields })
    }

    /// Write tagged fields to `writable`.
    pub fn write_to<W: Writer>(&self, writable: &mut W) {
        let total: usize = self.fields.values().map(|v| v.len()).sum();
        writable.write_unsigned_varint(total as u32);
        for (tag, list) in self.fields.iter() {
            for field in list {
                writable.write_unsigned_varint(*tag as u32);
                writable.write_unsigned_varint(field.data().len() as u32);
                writable.write_byte_array(field.data());
            }
        }
    }

    pub fn size(&self) -> usize {
        let total: usize = self.fields.values().map(|v| v.len()).sum();
        let mut size = byte_utils::size_of_unsigned_varint(total as u32);
        for (tag, list) in self.fields.iter() {
            for field in list {
                size += byte_utils::size_of_unsigned_varint(*tag as u32);
                size += byte_utils::size_of_unsigned_varint(field.data().len() as u32);
                size += field.data().len();
            }
        }
        size
    }
}

impl Default for TaggedFields {
    fn default() -> Self {
        TaggedFields::new()
    }
}
