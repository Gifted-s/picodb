use crate::encodex::{BytesNeededForEncoding, EncoderDecoder, EndOffset};
use byteorder::ByteOrder;
use std::borrow::Cow;

pub(crate) struct U16EncoderDecoder;

impl U16EncoderDecoder {
    const U16_SIZE: usize = size_of::<u16>();
}

impl EncoderDecoder<u16> for U16EncoderDecoder {
    fn bytes_needed_for_encoding(&self, _source: &u16) -> BytesNeededForEncoding {
        Self::U16_SIZE
    }

    fn encode(
        &self,
        source: &u16,
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding {
        byteorder::LittleEndian::write_u16(
            &mut destination[destination_starting_offset..],
            *source,
        );
        Self::U16_SIZE
    }

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (Cow<'a, u16>, EndOffset) {
        (
            Cow::Owned(byteorder::LittleEndian::read_u16(&encoded[from_offset..])),
            from_offset + Self::U16_SIZE,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::encodex::u16_encoder_decoder::U16EncoderDecoder;
    use crate::encodex::EncoderDecoder;

    #[test]
    fn numer_of_bytes_needed_for_encoding_u16() {
        let source: u16 = 10;

        assert_eq!(
            U16EncoderDecoder::U16_SIZE,
            U16EncoderDecoder.bytes_needed_for_encoding(&source)
        );
    }

    #[test]
    fn encode_decode_u16() {
        let source: u16 = 10;
        let mut destination = vec![0; 100];

        let number_of_bytes_for_encoding = U16EncoderDecoder.encode(&source, &mut destination, 0);

        let (decoded, _) =
            U16EncoderDecoder.decode(&destination[..number_of_bytes_for_encoding], 0);
        assert_eq!(decoded.as_ref(), &source);
    }

    #[test]
    fn encode_decode_u16_at_a_different_offset() {
        let source: u16 = 129;
        let mut destination = vec![0; 100];

        let _ = U16EncoderDecoder.encode(&source, &mut destination, 10);

        let (decoded, _) = U16EncoderDecoder.decode(&destination[..], 10);
        assert_eq!(decoded.as_ref(), &source);
    }
}
