use crate::encodex::{BytesNeededForEncoding, EncoderDecoder, EndOffset};
use byteorder::ByteOrder;
use std::borrow::Cow;

pub(crate) struct BytesEncoderDecoder;

impl BytesEncoderDecoder {
    pub(crate) const RESERVED_SIZE_FOR_BYTE_SLICE: usize = size_of::<u16>();
}

impl EncoderDecoder<[u8]> for BytesEncoderDecoder {
    fn bytes_needed_for_encoding(&self, source: &[u8]) -> BytesNeededForEncoding {
        Self::RESERVED_SIZE_FOR_BYTE_SLICE + source.len()
    }

    fn encode(
        &self,
        source: &[u8],
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding {
        let required_size = Self::RESERVED_SIZE_FOR_BYTE_SLICE + source.len();
        if destination_starting_offset + required_size > destination.len() {
            panic!(
                "Destination slice is too small: required size {}, available size {}",
                required_size,
                destination.len() - destination_starting_offset
            );
        }

        byteorder::LittleEndian::write_u16(
            &mut destination[destination_starting_offset..],
            source.len() as u16,
        );
        let start_index = destination_starting_offset + Self::RESERVED_SIZE_FOR_BYTE_SLICE;
        let end_index = start_index + source.len();

        destination[start_index..end_index].copy_from_slice(source);
        required_size
    }

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (Cow<'a, [u8]>, EndOffset) {
        let source_length = byteorder::LittleEndian::read_u16(&encoded[from_offset..]);
        let end_offset = from_offset + Self::RESERVED_SIZE_FOR_BYTE_SLICE + source_length as usize;
        (
            Cow::Borrowed(&encoded[from_offset + Self::RESERVED_SIZE_FOR_BYTE_SLICE..end_offset]),
            end_offset,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
    use crate::encodex::EncoderDecoder;

    #[test]
    fn numer_of_bytes_needed_for_encoding_bytes() {
        let source = b"raft";
        let source_length = source.len();

        assert_eq!(
            source_length + BytesEncoderDecoder::RESERVED_SIZE_FOR_BYTE_SLICE,
            BytesEncoderDecoder.bytes_needed_for_encoding(&source[..])
        );
    }

    #[test]
    fn encode_decode_bytes() {
        let source = b"Rocks is LSM-based";
        let mut destination = vec![0; 100];

        let number_of_bytes_for_encoding =
            BytesEncoderDecoder.encode(&source[..], &mut destination, 0);

        let (decoded, _) =
            BytesEncoderDecoder.decode(&destination[..number_of_bytes_for_encoding], 0);

        assert_eq!(&decoded[..], &source[..]);
    }

    #[test]
    fn encode_decode_bytes_at_a_different_offset() {
        let source = b"Rocks is LSM-based";
        let mut destination = vec![0; 100];
        let _ = BytesEncoderDecoder.encode(&source[..], &mut destination, 10);

        let (decoded, _) = BytesEncoderDecoder.decode(&destination[..], 10);

        assert_eq!(&decoded[..], &source[..]);
    }
}
