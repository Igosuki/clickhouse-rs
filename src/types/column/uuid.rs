use std::cmp;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::column_data::BoxColumnData, from_sql::*, Column, SqlType, Value,
        ValueRef, ColumnType
    },
};

use super::column_data::ColumnData;

pub const UUID_LEN : usize = 16;

pub(crate) struct UuidColumnData {
    buffer: Vec<u8>,
}

pub(crate) struct UuidAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
}

pub(crate) struct NullableUuidAdapter<K: ColumnType> {
    pub(crate) column: Column<K>,
}

impl UuidColumnData {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity * UUID_LEN),
        }
    }

    pub(crate) fn load<T: ReadEx>(reader: &mut T, size: usize) -> Result<Self> {
        let mut instance = Self::with_capacity(size);

        for _ in 0..size {
            let old_len = instance.buffer.len();
            instance.buffer.resize(old_len + UUID_LEN, 0_u8);
            reader.read_bytes(&mut instance.buffer[old_len..old_len + UUID_LEN])?;
        }

        Ok(instance)
    }
}

impl ColumnData for UuidColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Uuid
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let start_index = start * UUID_LEN;
        let end_index = end * UUID_LEN;
        encoder.write_bytes(&self.buffer[start_index..end_index]);
    }

    fn len(&self) -> usize {
        self.buffer.len() / UUID_LEN
    }

    fn push(&mut self, value: Value) {
        let bs: String = String::from(value);
        let l = cmp::min(bs.len(), UUID_LEN);
        let old_len = self.buffer.len();
        self.buffer.extend_from_slice(&bs.as_bytes()[0..l]);
        self.buffer.resize(old_len + (UUID_LEN - l), 0_u8);
    }

    fn at(&self, index: usize) -> ValueRef {
        let shift = index * UUID_LEN;
        let str_ref = &self.buffer[shift..shift + UUID_LEN];
        ValueRef::String(str_ref)
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            buffer: self.buffer.clone(),
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        assert_eq!(level, 0);
        *pointers[0] = self.buffer.as_ptr() as *const u8;
        *(pointers[1] as *mut usize) = self.len();
        Ok(())
    }
}

impl<K: ColumnType> ColumnData for UuidAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Uuid
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let mut buffer = Vec::with_capacity(UUID_LEN);
        for index in start..end {
            buffer.resize(0, 0);
            match self.column.at(index) {
                ValueRef::String(_) => {
                    let string_ref = self.column.at(index).as_bytes().unwrap();
                    buffer.extend(string_ref.as_ref());
                }
                ValueRef::Array(SqlType::UInt8, vs) => {
                    let mut string_val: Vec<u8> = Vec::with_capacity(vs.len());
                    for v in vs.iter() {
                        let byte: u8 = v.clone().into();
                        string_val.push(byte);
                    }
                    let string_ref: &[u8] = string_val.as_ref();
                    buffer.extend(string_ref);
                }
                _ => unimplemented!(),
            }
            buffer.resize(UUID_LEN, 0);
            encoder.write_bytes(&buffer[..]);
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        self.column.at(index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl<K: ColumnType> ColumnData for NullableUuidAdapter<K> {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::Uuid.into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<&[u8]>> = vec![None; size];

        for (i, index) in (start..end).enumerate() {
            values[i] = Option::from_sql(self.at(index)).unwrap();
            if values[i].is_none() {
                nulls[i] = 1;
            }
        }

        encoder.write_bytes(nulls.as_ref());

        let mut buffer = Vec::with_capacity(UUID_LEN);
        for value in values {
            buffer.resize(0, 0);
            if let Some(string_ref) = value {
                buffer.extend(string_ref);
            }
            buffer.resize(UUID_LEN, 0);
            encoder.write_bytes(buffer.as_ref());
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        self.column.at(index)
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}
