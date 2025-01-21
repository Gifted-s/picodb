use crate::encodex::{BytesNeededForEncoding, EncoderDecoder, EndOffset};

pub struct U8EncoderDecoder;

impl U8EncoderDecoder {
    const U8_SIZE: usize = size_of::<u8>();
}

impl EncoderDecoder<u8> for U8EncoderDecoder {
    fn bytes_needed_for_encoding(&self, _source: &u8) -> BytesNeededForEncoding {
        Self::U8_SIZE
    }

    fn encode(
        &self,
        source: &u8,
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding {
        destination[destination_starting_offset] = *source;
        Self::U8_SIZE
    }

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (&'a u8, EndOffset) {
        (&encoded[from_offset], from_offset + Self::U8_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use crate::encodex::u8_encoder_decoder::U8EncoderDecoder;
    use crate::encodex::EncoderDecoder;

    #[test]
    fn numer_of_bytes_needed_for_encoding_u8() {
        let source: u8 = 10;

        assert_eq!(
            U8EncoderDecoder::U8_SIZE,
            U8EncoderDecoder.bytes_needed_for_encoding(&source)
        );
    }

    #[test]
    fn encode_decode_u8() {
        let source: u8 = 10;
        let mut destination = vec![0; 100];

        let number_of_bytes_for_encoding = U8EncoderDecoder.encode(&source, &mut destination, 0);

        let (decoded, _) = U8EncoderDecoder.decode(&destination[..number_of_bytes_for_encoding], 0);
        assert_eq!(decoded, &source);
    }

    #[test]
    fn encode_decode_u8_at_a_different_offset() {
        let source: u8 = 129;
        let mut destination = vec![0; 100];

        let _ = U8EncoderDecoder.encode(&source, &mut destination, 10);

        let (decoded, _) = U8EncoderDecoder.decode(&destination[..], 10);
        assert_eq!(decoded, &source);
    }
}
