use crate::buffer::page::BufferPage;
use crate::buffer::supported_types::Types;
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;

const RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS: usize = size_of::<u16>();

pub(crate) struct PageEncoder<'a> {
    pub(crate) buffer: &'a mut [u8],
    pub(crate) starting_offsets: &'a StartingOffsets,
    pub(crate) types: &'a Types,
}

pub(crate) struct PageDecoder;

impl PageEncoder<'_> {
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

impl PageDecoder {
    pub(crate) fn decode_page(buffer: Vec<u8>) -> BufferPage {
        let offset_containing_number_of_offsets =
            buffer.len() - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS;
        let number_of_offsets =
            byteorder::LittleEndian::read_u16(&buffer[offset_containing_number_of_offsets..])
                as usize;
        if number_of_offsets == 0 {
            return BufferPage {
                buffer,
                starting_offsets: StartingOffsets::new(),
                types: Types::new(),
                current_write_offset: 0,
            };
        }

        let starting_offsets = Self::decode_starting_offsets(&buffer, number_of_offsets);
        let types = Self::decode_types(&buffer, number_of_offsets);
        let end_offset = types
            .last()
            .unwrap()
            .end_offset_post_decode(&buffer, *(starting_offsets.last_offset().unwrap()) as usize);

        BufferPage {
            buffer,
            starting_offsets,
            types,
            current_write_offset: end_offset,
        }
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
}

#[cfg(test)]
mod tests {
    use crate::buffer::page_encoder_decoder::{PageDecoder, PageEncoder};
    use crate::buffer::supported_types::{SupportedType, Types};
    use crate::file::starting_offsets::StartingOffsets;
    use byteorder::ByteOrder;

    #[test]
    fn encode_and_decode_a_page() {
        let mut starting_offsets = StartingOffsets::new();
        starting_offsets.add_offset(0);
        starting_offsets.add_offset(2);

        let mut types = Types::new();
        types.add(SupportedType::TypeU16);
        types.add(SupportedType::TypeU16);

        let mut buffer = vec![0; 512];
        byteorder::LittleEndian::write_u16(&mut buffer[0..2], 200);
        byteorder::LittleEndian::write_u16(&mut buffer[2..4], 400);

        let mut encoder = PageEncoder {
            buffer: &mut buffer,
            starting_offsets: &starting_offsets,
            types: &types,
        };
        encoder.encode();

        let decoded = PageDecoder::decode_page(encoder.buffer.to_vec());
        assert_eq!(2, decoded.starting_offsets.length());
        assert_eq!(&SupportedType::TypeU16, decoded.types.type_at(0).unwrap());
        assert_eq!(&SupportedType::TypeU16, decoded.types.type_at(1).unwrap());
    }
}
