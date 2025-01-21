use crate::buffer::supported_types::Types;
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;

const RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS: usize = size_of::<u16>();

pub(crate) struct PageEncoder<'a> {
    pub(crate) buffer: &'a mut [u8],
    pub(crate) starting_offsets: &'a StartingOffsets,
    pub(crate) types: &'a Types,
}

impl<'a> PageEncoder<'a> {

    pub(crate) fn encode(&mut self) {
        self.write_encoded_starting_offsets(&self.starting_offsets.encode());
        self.write_types(&self.types.encode());
        self.write_number_of_starting_offsets();
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
}
