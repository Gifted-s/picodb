use crate::buffer::supported_types::{SupportedType, Types};
use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
use crate::encodex::{BytesNeededForEncoding, EncoderDecoder};
use crate::file::starting_offsets::StartingOffsets;
use std::borrow::Cow;

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
            |buffer, starting_offset| {
                U8EncoderDecoder
                    .decode(&self.buffer, starting_offset as usize)
                    .0
                    .into_owned()
            },
            index,
        )
    }

    fn get_u16(&self, index: usize) -> Option<u16> {
        self.assert_field_type(index, SupportedType::TypeU16);
        self.get(
            |buffer, starting_offset| {
                U16EncoderDecoder
                    .decode(&self.buffer, starting_offset as usize)
                    .0
                    .into_owned()
            },
            index,
        )
    }

    fn get_bytes(&self, index: usize) -> Option<Cow<[u8]>> {
        self.assert_field_type(index, SupportedType::TypeBytes);
        self.get(
            |buffer, starting_offset| {
                BytesEncoderDecoder
                    .decode(&self.buffer, starting_offset as usize)
                    .0
            },
            index,
        )
    }

    fn get_string(&self, index: usize) -> Option<Cow<String>> {
        self.assert_field_type(index, SupportedType::TypeString);
        self.get(
            |buffer, starting_offset| {
                StringEncoderDecoder
                    .decode(&self.buffer, starting_offset as usize)
                    .0
            },
            index,
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

    fn get<T, F: Fn(&[u8], usize) -> T>(&self, decode_fn: F, index: usize) -> Option<T> {
        self.starting_offsets
            .offset_at(index)
            .map(|starting_offset| decode_fn(&self.buffer, *starting_offset as usize))
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

        assert_eq!(Some(250), page.get_u8(0));
        assert_eq!(
            Some(Cow::Owned(String::from(
                "PebbleDB is an LSM-based storage engine"
            ))),
            page.get_string(1)
        );
    }
}
