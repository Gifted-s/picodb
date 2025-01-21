use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::{BytesNeededForEncoding, EncoderDecoder, EndOffset};
use std::borrow::Cow;

pub struct StringEncoderDecoder;

impl EncoderDecoder<String> for StringEncoderDecoder {
    fn bytes_needed_for_encoding(&self, source: &String) -> BytesNeededForEncoding {
        BytesEncoderDecoder.bytes_needed_for_encoding(source.as_bytes())
    }

    fn encode(
        &self,
        source: &String,
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding {
        BytesEncoderDecoder.encode(source.as_bytes(), destination, destination_starting_offset)
    }

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (Cow<'a, String>, EndOffset) {
        let (decoded_slice, end_offset) = BytesEncoderDecoder.decode(encoded, from_offset);
        match decoded_slice {
            Cow::Borrowed(bytes) => (
                Cow::Owned(String::from_utf8_lossy(bytes).into_owned()),
                end_offset,
            ),
            Cow::Owned(bytes) => (
                Cow::Owned(String::from_utf8_lossy(&bytes).into_owned()),
                end_offset,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
    use crate::encodex::string_encoder_decoder::StringEncoderDecoder;
    use crate::encodex::EncoderDecoder;

    #[test]
    fn numer_of_bytes_needed_for_encoding_string() {
        let source = String::from("raft");
        let source_length = source.len();

        assert_eq!(
            source_length + BytesEncoderDecoder::RESERVED_SIZE_FOR_BYTE_SLICE,
            StringEncoderDecoder.bytes_needed_for_encoding(&source)
        );
    }

    #[test]
    fn encode_decode_string() {
        let source = String::from("Rocks is LSM-based");
        let mut destination = vec![0; 100];

        let number_of_bytes_for_encoding =
            StringEncoderDecoder.encode(&source, &mut destination, 0);

        let (decoded, _) =
            StringEncoderDecoder.decode(&destination[..number_of_bytes_for_encoding], 0);

        assert_eq!(decoded.as_bytes(), source.as_bytes());
    }

    #[test]
    fn encode_decode_string_at_a_different_offset() {
        let source = String::from("Rocks is LSM-based");
        let mut destination = vec![0; 100];
        let _ = StringEncoderDecoder.encode(&source, &mut destination, 10);

        let (decoded, _) = StringEncoderDecoder.decode(&destination[..], 10);

        assert_eq!(decoded.as_bytes(), source.as_bytes());
    }
}
