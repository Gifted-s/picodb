use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::str_encoder_decoder::StrEncoderDecoder;
use crate::encodex::U8EncoderDecoder;
use crate::encodex::{EncoderDecoder, EndOffset};
use crate::encodex::{U16EncoderDecoder, U32EncoderDecoder};

const RESERVED_SIZE_FOR_TYPE: usize = size_of::<u8>();

pub(crate) struct Fields {
    types: Vec<FieldType>,
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum FieldType {
    TypeU8,
    TypeU16,
    TypeU32,
    TypeBytes,
    TypeString,
}

impl From<u8> for FieldType {
    fn from(value: u8) -> Self {
        match value {
            0 => FieldType::TypeU8,
            1 => FieldType::TypeU16,
            2 => FieldType::TypeU32,
            3 => FieldType::TypeBytes,
            4 => FieldType::TypeString,
            _ => unreachable!(),
        }
    }
}

impl From<FieldType> for u8 {
    fn from(val: FieldType) -> Self {
        match val {
            FieldType::TypeU8 => 0,
            FieldType::TypeU16 => 1,
            FieldType::TypeU32 => 2,
            FieldType::TypeBytes => 3,
            FieldType::TypeString => 4,
        }
    }
}

impl FieldType {
    pub(crate) fn end_offset_post_decode(&self, buffer: &[u8], from_offset: usize) -> EndOffset {
        match self {
            FieldType::TypeU8 => U8EncoderDecoder.decode(buffer, from_offset).1,
            FieldType::TypeU16 => U16EncoderDecoder.decode(buffer, from_offset).1,
            FieldType::TypeU32 => U32EncoderDecoder.decode(buffer, from_offset).1,
            FieldType::TypeBytes => BytesEncoderDecoder.decode(buffer, from_offset).1,
            FieldType::TypeString => StrEncoderDecoder.decode(buffer, from_offset).1,
        }
    }
}

impl Fields {
    pub(crate) fn new() -> Fields {
        Fields { types: vec![] }
    }

    pub(crate) fn decode_from(bytes: &[u8]) -> Fields {
        let mut types = Fields::new();
        for description in bytes {
            types.add(FieldType::from(*description));
        }
        types
    }

    pub(crate) fn add(&mut self, field_type: FieldType) {
        self.types.push(field_type);
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.types.len() * RESERVED_SIZE_FOR_TYPE];
        for (offset_index, &field_type) in self.types.iter().enumerate() {
            buffer[offset_index * RESERVED_SIZE_FOR_TYPE] = field_type.into();
        }
        buffer
    }

    pub(crate) fn type_at(&self, index: usize) -> Option<&FieldType> {
        self.types.get(index)
    }

    pub(crate) fn last(&self) -> Option<&FieldType> {
        self.types.last()
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.length() * RESERVED_SIZE_FOR_TYPE
    }

    pub(crate) fn size_in_bytes_for(number_of_types: usize) -> usize {
        RESERVED_SIZE_FOR_TYPE * number_of_types
    }

    fn length(&self) -> usize {
        self.types.len()
    }
}

#[cfg(test)]
mod fields_tests {
    use crate::buffer::field_types::{FieldType, Fields};

    #[test]
    fn encode_and_decode_types_with_a_single_field() {
        let mut types = Fields::new();
        types.add(FieldType::TypeU8);

        let encoded = types.encode();
        let decoded = Fields::decode_from(&encoded);

        assert_eq!(&FieldType::TypeU8, decoded.type_at(0).unwrap());
    }

    #[test]
    fn encode_and_decode_types_with_a_couple_of_fields() {
        let mut types = Fields::new();
        types.add(FieldType::TypeU8);
        types.add(FieldType::TypeBytes);

        let encoded = types.encode();
        let decoded = Fields::decode_from(&encoded);

        assert_eq!(&FieldType::TypeU8, decoded.type_at(0).unwrap());
        assert_eq!(&FieldType::TypeBytes, decoded.type_at(1).unwrap());
    }

    #[test]
    fn encode_and_decode_types_with_a_few_fields() {
        let mut types = Fields::new();
        types.add(FieldType::TypeU8);
        types.add(FieldType::TypeBytes);
        types.add(FieldType::TypeString);
        types.add(FieldType::TypeU16);
        types.add(FieldType::TypeU32);

        let encoded = types.encode();
        let decoded = Fields::decode_from(&encoded);

        assert_eq!(&FieldType::TypeU8, decoded.type_at(0).unwrap());
        assert_eq!(&FieldType::TypeBytes, decoded.type_at(1).unwrap());
        assert_eq!(&FieldType::TypeString, decoded.type_at(2).unwrap());
        assert_eq!(&FieldType::TypeU16, decoded.type_at(3).unwrap());
        assert_eq!(&FieldType::TypeU32, decoded.type_at(4).unwrap());
    }

    #[test]
    fn get_type_at_an_index() {
        let mut types = Fields::new();
        types.add(FieldType::TypeU8);

        assert_eq!(&FieldType::TypeU8, types.type_at(0).unwrap());
    }

    #[test]
    fn length_of_types() {
        let mut types = Fields::new();
        types.add(FieldType::TypeU8);

        assert_eq!(1, types.length());
    }
}

#[cfg(test)]
mod field_type_tests {
    use crate::buffer::field_types::FieldType;
    use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
    use crate::encodex::str_encoder_decoder::StrEncoderDecoder;
    use crate::encodex::EncoderDecoder;
    use byteorder::ByteOrder;

    #[test]
    fn end_offset_post_decode_for_u8() {
        let mut buffer = vec![0; 100];
        buffer[0] = 250;

        assert_eq!(11, FieldType::TypeU8.end_offset_post_decode(&buffer, 10));
    }

    #[test]
    fn end_offset_post_decode_for_u16() {
        let mut buffer = vec![0; 100];
        byteorder::LittleEndian::write_u16(&mut buffer[0..2], 250);

        assert_eq!(12, FieldType::TypeU16.end_offset_post_decode(&buffer, 10));
    }

    #[test]
    fn end_offset_post_decode_for_u32() {
        let mut buffer = vec![0; 100];
        byteorder::LittleEndian::write_u32(&mut buffer[0..4], 250);

        assert_eq!(14, FieldType::TypeU32.end_offset_post_decode(&buffer, 10));
    }

    #[test]
    fn end_offset_post_decode_for_bytes() {
        let mut buffer = vec![0; 100];
        let _ = BytesEncoderDecoder.encode(b"Rocksdb", &mut buffer, 10);

        assert!(FieldType::TypeBytes.end_offset_post_decode(&buffer, 10) > 16);
    }

    #[test]
    fn end_offset_post_decode_for_string() {
        let mut buffer = vec![0; 100];
        let _ = StrEncoderDecoder.encode(&String::from("Rocksdb"), &mut buffer, 10);

        assert!(FieldType::TypeString.end_offset_post_decode(&buffer, 10) > 16);
    }
}
