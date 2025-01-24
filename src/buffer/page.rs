use crate::assert_borrowed_type;
use crate::buffer::field_types::{FieldType, Fields};
use crate::buffer::page_encoder_decoder::{PageDecoder, PageEncoder};
use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::str_encoder_decoder::StrEncoderDecoder;
use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
use crate::encodex::{BytesNeededForEncoding, EncoderDecoder};
use crate::file::starting_offsets::StartingOffsets;

pub(crate) struct BufferPage {
    pub(crate) buffer: Vec<u8>,
    pub(crate) starting_offsets: StartingOffsets,
    pub(crate) types: Fields,
    pub(crate) current_write_offset: usize,
}

impl crate::page::Page for BufferPage {
    fn decode_from(buffer: Vec<u8>) -> Self {
        if buffer.is_empty() {
            panic!("buffer cannot be empty while decoding the page");
        }
        PageDecoder::decode_page(buffer)
    }
}

impl BufferPage {
    pub(crate) fn new(block_size: usize) -> Self {
        BufferPage {
            buffer: vec![0; block_size],
            starting_offsets: StartingOffsets::new(),
            types: Fields::new(),
            current_write_offset: 0,
        }
    }

    pub(crate) fn add_u8(&mut self, value: u8) {
        self.add_field(
            |destination, current_write_offset| {
                U8EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            FieldType::TypeU8,
        )
    }

    pub(crate) fn mutate_u8(&mut self, value: u8, index: usize) {
        self.assert_field_type(index, FieldType::TypeU8);
        self.mutate_field(
            |destination, current_write_offset| {
                U8EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            index,
        );
    }

    pub(crate) fn add_u16(&mut self, value: u16) {
        self.add_field(
            |destination, current_write_offset| {
                U16EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            FieldType::TypeU16,
        )
    }

    pub(crate) fn mutate_u16(&mut self, value: u16, index: usize) {
        self.assert_field_type(index, FieldType::TypeU16);
        self.mutate_field(
            |destination, current_write_offset| {
                U16EncoderDecoder.encode(&value, destination, current_write_offset)
            },
            index,
        );
    }

    pub(crate) fn add_bytes(&mut self, value: Vec<u8>) {
        self.add_field(
            |destination, current_write_offset| {
                BytesEncoderDecoder.encode(&value, destination, current_write_offset)
            },
            FieldType::TypeBytes,
        )
    }

    //TODO: What if the new value does not match the old size
    pub(crate) fn mutate_bytes(&mut self, value: Vec<u8>, index: usize) {
        self.assert_field_type(index, FieldType::TypeBytes);
        self.mutate_field(
            |destination, current_write_offset| {
                BytesEncoderDecoder.encode(&value, destination, current_write_offset)
            },
            index,
        );
    }

    pub(crate) fn add_string(&mut self, value: &str) {
        self.add_field(
            |destination, current_write_offset| {
                StrEncoderDecoder.encode(value, destination, current_write_offset)
            },
            FieldType::TypeString,
        )
    }

    //TODO: What if the new value does not match the old size
    pub(crate) fn mutate_string(&mut self, value: &str, index: usize) {
        self.assert_field_type(index, FieldType::TypeString);
        self.mutate_field(
            |destination, current_write_offset| {
                StrEncoderDecoder.encode(value, destination, current_write_offset)
            },
            index,
        );
    }

    pub(crate) fn get_u8(&self, index: usize) -> Option<u8> {
        self.assert_field_type(index, FieldType::TypeU8);
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

    pub(crate) fn get_u16(&self, index: usize) -> Option<u16> {
        self.assert_field_type(index, FieldType::TypeU16);
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

    pub(crate) fn get_bytes(&self, index: usize) -> Option<&[u8]> {
        self.assert_field_type(index, FieldType::TypeBytes);
        let buffer = self.get(
            |starting_offset| BytesEncoderDecoder.decode(&self.buffer, starting_offset).0,
            index,
        )?;
        Some(assert_borrowed_type(buffer))
    }

    pub(crate) fn get_string(&self, index: usize) -> Option<&str> {
        self.assert_field_type(index, FieldType::TypeString);
        let str = self.get(
            |starting_offset| StrEncoderDecoder.decode(&self.buffer, starting_offset).0,
            index,
        )?;
        Some(assert_borrowed_type(str))
    }

    pub(crate) fn encode(&mut self) -> &[u8] {
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

    fn assert_field_type(&self, index: usize, expected: FieldType) {
        assert_eq!(Some(&expected), self.types.type_at(index))
    }

    fn add_field<F: Fn(&mut [u8], usize) -> BytesNeededForEncoding>(
        &mut self,
        encode_fn: F,
        field_type: FieldType,
    ) {
        let bytes_needed_for_encoding = encode_fn(&mut self.buffer, self.current_write_offset);
        self.starting_offsets
            .add_offset(self.current_write_offset as u32);
        self.types.add(field_type);
        self.current_write_offset += bytes_needed_for_encoding;
    }

    fn mutate_field<F: Fn(&mut [u8], usize) -> BytesNeededForEncoding>(
        &mut self,
        encode_fn: F,
        index: usize,
    ) {
        encode_fn(
            &mut self.buffer,
            *(self.starting_offsets.offset_at(index).unwrap()) as usize,
        );
    }

    fn get<T, F: Fn(usize) -> T>(&self, decode_fn: F, index: usize) -> Option<T> {
        self.starting_offsets
            .offset_at(index)
            .map(|starting_offset| decode_fn(*starting_offset as usize))
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::page::BufferPage;
    use crate::page::Page;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    #[should_panic]
    fn attempt_to_decode_with_an_empty_buffer() {
        BufferPage::decode_from(vec![]);
    }

    #[test]
    fn add_a_single_field_and_get_the_value() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(250);

        assert_eq!(Some(250), page.get_u8(0));
    }

    #[test]
    fn add_a_couple_of_fields_and_get_the_values() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_u16(500);

        assert_eq!(Some(250), page.get_u8(0));
        assert_eq!(Some(500), page.get_u16(1));
    }

    #[test]
    fn add_a_few_fields_and_get_the_values() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_string("PebbleDB is an LSM-based storage engine");
        page.add_bytes(b"RocksDB is an LSM-based storage engine".to_vec());

        assert_eq!(Some(250), page.get_u8(0));
        assert_eq!(
            Some("PebbleDB is an LSM-based storage engine"),
            page.get_string(1)
        );
        assert_eq!(
            Some("RocksDB is an LSM-based storage engine".as_bytes()),
            page.get_bytes(2)
        );
    }

    #[test]
    #[should_panic]
    fn attempt_to_decode_an_empty_page() {
        let mut page = BufferPage::new(BLOCK_SIZE);

        page.encode();
    }

    #[test]
    fn decode_a_page_with_single_field() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(250);

        let encoded = page.encode();
        let decoded = BufferPage::decode_from(encoded.to_vec());

        assert_eq!(Some(250), decoded.get_u8(0));
    }

    #[test]
    fn decode_a_page_with_few_fields() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(250);
        page.add_string("PebbleDB is an LSM-based storage engine");
        page.add_bytes(b"RocksDB is an LSM-based storage engine".to_vec());
        page.add_u16(500);

        let encoded = page.encode();
        let decoded = BufferPage::decode_from(encoded.to_vec());

        assert_eq!(Some(250), decoded.get_u8(0));
        assert_eq!(
            Some("PebbleDB is an LSM-based storage engine"),
            decoded.get_string(1)
        );
        assert_eq!(
            Some("RocksDB is an LSM-based storage engine".as_bytes()),
            decoded.get_bytes(2)
        );
        assert_eq!(Some(500), decoded.get_u16(3));
    }

    #[test]
    fn mutate_an_u8() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u8(50);
        page.mutate_u8(252, 0);

        assert_eq!(Some(252), page.get_u8(0));
    }

    #[test]
    fn mutate_an_u16() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u16(50);
        page.mutate_u16(252, 0);

        assert_eq!(Some(252), page.get_u16(0));
    }

    #[test]
    fn mutate_bytes() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_bytes(b"Bolt-DB".to_vec());
        page.mutate_bytes(b"RocksDB".to_vec(), 0);

        assert_eq!(Some("RocksDB".as_bytes()), page.get_bytes(0));
    }

    #[test]
    fn mutate_string() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_string("Bolt-DB");
        page.mutate_string("RocksDB", 0);

        assert_eq!(Some("RocksDB"), page.get_string(0));
    }

    #[test]
    fn add_fields_and_then_mutate_those_fields_in_the_decoded_page() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_string("PebbleDB is an LSM-based key/value storage engine");
        page.add_u8(80);
        page.add_u16(160);

        let encoded = page.encode();
        let mut decoded = BufferPage::decode_from(encoded.to_vec());

        decoded.mutate_string("Rocks-DB is an LSM-based key/value storage engine", 0);
        decoded.mutate_u8(160, 1);
        decoded.mutate_u16(320, 2);

        assert_eq!(
            Some("Rocks-DB is an LSM-based key/value storage engine"),
            decoded.get_string(0)
        );
        assert_eq!(Some(160), decoded.get_u8(1));
        assert_eq!(Some(320), decoded.get_u16(2));
    }

    #[test]
    fn add_fields_in_the_decoded_page() {
        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_string("PebbleDB is an LSM-based key/value storage engine");
        page.add_u8(80);
        page.add_u16(160);

        let encoded = page.encode();
        let mut decoded = BufferPage::decode_from(encoded.to_vec());

        decoded.add_string("BoltDB");

        assert_eq!(
            Some("PebbleDB is an LSM-based key/value storage engine"),
            decoded.get_string(0)
        );
        assert_eq!(Some(80), decoded.get_u8(1));
        assert_eq!(Some(160), decoded.get_u16(2));
        assert_eq!(Some("BoltDB"), decoded.get_string(3));
    }
}
