use crate::buffer::supported_types::{SupportedType, Types};
use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
use crate::encodex::{BytesNeededForEncoding, EncoderDecoder};
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;
use std::borrow::Cow;

const RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS: usize = size_of::<u16>();

struct Page {
    buffer: Vec<u8>,
    starting_offsets: StartingOffsets,
    types: Types,
    current_write_offset: usize,
}

impl Page {
    fn new(block_size: usize) -> Self {
        Page {
            buffer: vec![0; block_size],
            starting_offsets: StartingOffsets::new(),
            types: Types::new(),
            current_write_offset: 0,
        }
    }

    fn decode_from(buffer: Vec<u8>) -> Self {
        if buffer.is_empty() {
            panic!("buffer cannot be empty while decoding the page");
        }

        let offset_containing_number_of_offsets =
            buffer.len() - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS;
        let number_of_offsets =
            byteorder::LittleEndian::read_u16(&buffer[offset_containing_number_of_offsets..])
                as usize;

        match number_of_offsets {
            0 => Page {
                buffer,
                starting_offsets: StartingOffsets::new(),
                types: Types::new(),
                current_write_offset: 0,
            },
            _ => {
                let starting_offsets = Self::decode_starting_offsets(&buffer, number_of_offsets);
                let types = Self::decode_types(&buffer, number_of_offsets);
                let end_offset = types.last().unwrap().end_offset_post_decode(
                    &buffer,
                    *(starting_offsets.last_offset().unwrap()) as usize,
                );
                Page {
                    buffer,
                    starting_offsets,
                    types,
                    current_write_offset: end_offset,
                }
            }
        }
    }

    fn add_u8(&mut self, value: u8) {
        self.add_field(
            |destination, current_write_offset| {
                U8EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            SupportedType::TypeU8,
        )
    }

    fn add_u16(&mut self, value: u16) {
        self.add_field(
            |destination, current_write_offset| {
                U16EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            SupportedType::TypeU16,
        )
    }

    fn add_bytes(&mut self, value: Vec<u8>) {
        self.add_field(
            |destination, current_write_offset| {
                BytesEncoderDecoder.encode(&value, destination, current_write_offset)
            },
            SupportedType::TypeBytes,
        )
    }

    fn add_string(&mut self, value: String) {
        self.add_field(
            |destination, current_write_offset| {
                StringEncoderDecoder.encode(&value, destination, current_write_offset)
            },
            SupportedType::TypeString,
        )
    }

    fn get_u8(&self, index: usize) -> Option<u8> {
        self.assert_field_type(index, SupportedType::TypeU8);
        self.get(
            |starting_offset| {
                U8EncoderDecoder
                    .decode(&self.buffer, starting_offset)
                    .0
                    .into_owned()
            },
            index,
        )
    }

    fn get_u16(&self, index: usize) -> Option<u16> {
        self.assert_field_type(index, SupportedType::TypeU16);
        self.get(
            |starting_offset| {
                U16EncoderDecoder
                    .decode(&self.buffer, starting_offset)
                    .0
                    .into_owned()
            },
            index,
        )
    }

    fn get_bytes(&self, index: usize) -> Option<Cow<[u8]>> {
        self.assert_field_type(index, SupportedType::TypeBytes);
        self.get(
            |starting_offset| BytesEncoderDecoder.decode(&self.buffer, starting_offset).0,
            index,
        )
    }

    fn get_string(&self, index: usize) -> Option<Cow<String>> {
        self.assert_field_type(index, SupportedType::TypeString);
        self.get(
            |starting_offset| StringEncoderDecoder.decode(&self.buffer, starting_offset).0,
            index,
        )
    }

    fn finish(&mut self) -> &[u8] {
        if self.starting_offsets.length() == 0 {
            panic!("empty page")
        }
        self.write_encoded_starting_offsets(&self.starting_offsets.encode());
        self.write_types(&self.types.encode());
        self.write_number_of_starting_offsets();
        &self.buffer
    }

    fn write_encoded_starting_offsets(&mut self, encoded_starting_offsets: &[u8]) {
        let encoded_page = &mut self.buffer;
        let offset_to_write_encoded_starting_offsets = encoded_page.len()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS
            - self.starting_offsets.size_in_bytes();

        encoded_page[offset_to_write_encoded_starting_offsets
            ..offset_to_write_encoded_starting_offsets + encoded_starting_offsets.len()]
            .copy_from_slice(encoded_starting_offsets);
    }

    fn write_types(&mut self, encoded_types: &[u8]) {
        let encoded_page = &mut self.buffer;
        let offset_to_write_types = encoded_page.len()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS
            - self.starting_offsets.size_in_bytes()
            - self.types.size_in_bytes();

        encoded_page[offset_to_write_types..offset_to_write_types + encoded_types.len()]
            .copy_from_slice(encoded_types);
    }

    fn write_number_of_starting_offsets(&mut self) {
        let encoded_page = &mut self.buffer;
        let encoded_page_length = encoded_page.len();

        byteorder::LittleEndian::write_u16(
            &mut encoded_page[encoded_page_length - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS..],
            self.starting_offsets.length() as u16,
        );
    }

    fn decode_starting_offsets(buffer: &[u8], number_of_offsets: usize) -> StartingOffsets {
        let offset_containing_encoded_starting_offsets = buffer.len()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS
            - StartingOffsets::size_in_bytes_for(number_of_offsets);

        StartingOffsets::decode_from(
            &buffer[offset_containing_encoded_starting_offsets
                ..offset_containing_encoded_starting_offsets
                    + StartingOffsets::size_in_bytes_for(number_of_offsets)],
        )
    }

    fn decode_types(buffer: &[u8], number_of_offsets: usize) -> Types {
        let number_of_types = number_of_offsets;
        let offset_containing_types = buffer.len()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS
            - StartingOffsets::size_in_bytes_for(number_of_offsets)
            - Types::size_in_bytes_for(number_of_types);

        Types::decode_from(
            &buffer[offset_containing_types
                ..offset_containing_types + Types::size_in_bytes_for(number_of_types)],
        )
    }

    fn assert_field_type(&self, index: usize, expected: SupportedType) {
        assert_eq!(Some(&expected), self.types.type_at(index))
    }

    fn add_field<F: Fn(&mut [u8], usize) -> BytesNeededForEncoding>(
        &mut self,
        encode_fn: F,
        field_type: SupportedType,
    ) {
        let bytes_needed_for_encoding = encode_fn(&mut self.buffer, self.current_write_offset);
        self.starting_offsets
            .add_offset(self.current_write_offset as u32);
        self.types.add(field_type);
        self.current_write_offset += bytes_needed_for_encoding as usize;
    }

    fn get<T, F: Fn(usize) -> T>(&self, decode_fn: F, index: usize) -> Option<T> {
        self.starting_offsets
            .offset_at(index)
            .map(|starting_offset| decode_fn(*starting_offset as usize))
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::page::Page;
    use std::borrow::Cow;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn add_a_single_field_and_get_the_value() {
        let mut page = Page::new(BLOCK_SIZE);
        page.add_u8(250);

        assert_eq!(Some(250), page.get_u8(0));
    }

    #[test]
    fn add_a_couple_of_fields_and_get_the_values() {
        let mut page = Page::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_u16(500);

        assert_eq!(Some(250), page.get_u8(0));
        assert_eq!(Some(500), page.get_u16(1));
    }

    #[test]
    fn add_a_few_fields_and_get_the_values() {
        let mut page = Page::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_string(String::from("PebbleDB is an LSM-based storage engine"));
        page.add_bytes(b"RocksDB is an LSM-based storage engine".to_vec());

        assert_eq!(Some(250), page.get_u8(0));
        assert_eq!(
            Some(Cow::Owned(String::from(
                "PebbleDB is an LSM-based storage engine"
            ))),
            page.get_string(1)
        );
        assert_eq!(
            Some(Cow::Owned(
                b"RocksDB is an LSM-based storage engine".to_vec()
            )),
            page.get_bytes(2)
        );
    }

    #[test]
    fn decode_a_page_with_single_field() {
        let mut page = Page::new(BLOCK_SIZE);
        page.add_u8(250);

        let encoded = page.finish();
        let decoded = Page::decode_from(encoded.to_vec());

        assert_eq!(Some(250), decoded.get_u8(0));
    }

    #[test]
    fn decode_a_page_with_few_fields() {
        let mut page = Page::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_string(String::from("PebbleDB is an LSM-based storage engine"));
        page.add_bytes(b"RocksDB is an LSM-based storage engine".to_vec());
        page.add_u16(500);

        let encoded = page.finish();
        let decoded = Page::decode_from(encoded.to_vec());

        assert_eq!(Some(250), decoded.get_u8(0));
        assert_eq!(
            Some(Cow::Owned(String::from(
                "PebbleDB is an LSM-based storage engine"
            ))),
            decoded.get_string(1)
        );
        assert_eq!(
            Some(Cow::Owned(
                b"RocksDB is an LSM-based storage engine".to_vec()
            )),
            decoded.get_bytes(2)
        );
        assert_eq!(Some(500), decoded.get_u16(3));
    }
}
