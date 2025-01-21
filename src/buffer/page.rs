use crate::buffer::page_encoder_decoder::{PageDecoder, PageEncoder};
use crate::buffer::supported_types::{SupportedType, Types};
use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
use crate::encodex::{BytesNeededForEncoding, EncoderDecoder};
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;
use std::borrow::Cow;

pub(crate) struct Page {
    pub(crate) buffer: Vec<u8>,
    pub(crate) starting_offsets: StartingOffsets,
    pub(crate) types: Types,
    pub(crate) current_write_offset: usize,
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
        PageDecoder::decode_page(buffer)
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

        let mut encoder = PageEncoder {
            buffer: &mut self.buffer,
            starting_offsets: &self.starting_offsets,
            types: &self.types,
        };
        encoder.encode();
        &self.buffer
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
