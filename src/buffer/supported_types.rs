use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
use crate::encodex::{EncoderDecoder, EndOffset};

const RESERVED_SIZE_FOR_TYPE: usize = size_of::<u8>();

pub(crate) struct Types {
    supported_types: Vec<SupportedType>,
}

#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum SupportedType {
    TypeU8,
    TypeU16,
    TypeBytes,
    TypeString,
}

impl From<u8> for SupportedType {
    fn from(value: u8) -> Self {
        match value {
            0 => SupportedType::TypeU8,
            1 => SupportedType::TypeU16,
            2 => SupportedType::TypeBytes,
            3 => SupportedType::TypeString,
            _ => unreachable!(),
        }
    }
}

impl From<SupportedType> for u8 {
    fn from(val: SupportedType) -> Self {
        match val {
            SupportedType::TypeU8 => 0,
            SupportedType::TypeU16 => 1,
            SupportedType::TypeBytes => 2,
            SupportedType::TypeString => 3,
        }
    }
}

impl SupportedType {
    pub(crate) fn end_offset_post_decode(&self, buffer: &[u8], from_offset: usize) -> EndOffset {
        match self {
            SupportedType::TypeU8 => U8EncoderDecoder.decode(buffer, from_offset).1,
            SupportedType::TypeU16 => U16EncoderDecoder.decode(buffer, from_offset).1,
            SupportedType::TypeBytes => BytesEncoderDecoder.decode(buffer, from_offset).1,
            SupportedType::TypeString => StringEncoderDecoder.decode(buffer, from_offset).1,
        }
    }
}

impl Types {
    pub(crate) fn new() -> Types {
        Types {
            supported_types: vec![],
        }
    }

    pub(crate) fn decode_from(bytes: &[u8]) -> Types {
        let mut types = Types::new();
        for description in bytes {
            types.add(SupportedType::from(*description));
        }
        types
    }

    pub(crate) fn add(&mut self, supported_type: SupportedType) {
        self.supported_types.push(supported_type);
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; self.supported_types.len() * RESERVED_SIZE_FOR_TYPE];
        for (offset_index, &supported_type) in self.supported_types.iter().enumerate() {
            buffer[offset_index * RESERVED_SIZE_FOR_TYPE] = supported_type.into();
        }
        buffer
    }

    pub(crate) fn type_at(&self, index: usize) -> Option<&SupportedType> {
        self.supported_types.get(index)
    }

    pub(crate) fn last(&self) -> Option<&SupportedType> {
        self.supported_types.last()
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.length() * RESERVED_SIZE_FOR_TYPE
    }

    pub(crate) fn size_in_bytes_for(number_of_types: usize) -> usize {
        RESERVED_SIZE_FOR_TYPE * number_of_types
    }

    fn length(&self) -> usize {
        self.supported_types.len()
    }
}

#[cfg(test)]
mod types_tests {
    use crate::buffer::supported_types::{SupportedType, Types};

    #[test]
    fn encode_and_decode_types_with_a_single_supported_type() {
        let mut types = Types::new();
        types.add(SupportedType::TypeU8);

        let encoded = types.encode();
        let decoded = Types::decode_from(&encoded);

        assert_eq!(&SupportedType::TypeU8, decoded.type_at(0).unwrap());
    }

    #[test]
    fn encode_and_decode_types_with_a_couple_of_supported_types() {
        let mut types = Types::new();
        types.add(SupportedType::TypeU8);
        types.add(SupportedType::TypeBytes);

        let encoded = types.encode();
        let decoded = Types::decode_from(&encoded);

        assert_eq!(&SupportedType::TypeU8, decoded.type_at(0).unwrap());
        assert_eq!(&SupportedType::TypeBytes, decoded.type_at(1).unwrap());
    }

    #[test]
    fn encode_and_decode_types_with_a_few_supported_types() {
        let mut types = Types::new();
        types.add(SupportedType::TypeU8);
        types.add(SupportedType::TypeBytes);
        types.add(SupportedType::TypeString);
        types.add(SupportedType::TypeU16);

        let encoded = types.encode();
        let decoded = Types::decode_from(&encoded);

        assert_eq!(&SupportedType::TypeU8, decoded.type_at(0).unwrap());
        assert_eq!(&SupportedType::TypeBytes, decoded.type_at(1).unwrap());
        assert_eq!(&SupportedType::TypeString, decoded.type_at(2).unwrap());
        assert_eq!(&SupportedType::TypeU16, decoded.type_at(3).unwrap());
    }

    #[test]
    fn get_type_at_an_index() {
        let mut types = Types::new();
        types.add(SupportedType::TypeU8);

        assert_eq!(&SupportedType::TypeU8, types.type_at(0).unwrap());
    }

    #[test]
    fn length_of_types() {
        let mut types = Types::new();
        types.add(SupportedType::TypeU8);

        assert_eq!(1, types.length());
    }
}

#[cfg(test)]
mod support_type_tests {
    use crate::buffer::supported_types::SupportedType;
    use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
    use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
    use crate::encodex::EncoderDecoder;
    use byteorder::ByteOrder;

    #[test]
    fn end_offset_post_decode_for_u8() {
        let mut buffer = vec![0; 100];
        buffer[0] = 250;

        assert_eq!(
            11,
            SupportedType::TypeU8.end_offset_post_decode(&buffer, 10)
        );
    }

    #[test]
    fn end_offset_post_decode_for_u16() {
        let mut buffer = vec![0; 100];
        byteorder::LittleEndian::write_u16(&mut buffer[0..2], 250);

        assert_eq!(
            12,
            SupportedType::TypeU16.end_offset_post_decode(&buffer, 10)
        );
    }

    #[test]
    fn end_offset_post_decode_for_bytes() {
        let mut buffer = vec![0; 100];
        let _ = BytesEncoderDecoder.encode(b"Rocksdb", &mut buffer, 10);

        assert!(SupportedType::TypeBytes.end_offset_post_decode(&buffer, 10) > 16);
    }

    #[test]
    fn end_offset_post_decode_for_string() {
        let mut buffer = vec![0; 100];
        let _ = StringEncoderDecoder.encode(&String::from("Rocksdb"), &mut buffer, 10);

        assert!(SupportedType::TypeString.end_offset_post_decode(&buffer, 10) > 16);
    }
}
