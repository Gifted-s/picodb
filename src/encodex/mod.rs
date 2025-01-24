use byteorder::ByteOrder;
use std::borrow::Cow;

pub(crate) mod bytes_encoder_decoder;
pub(crate) mod str_encoder_decoder;
pub(crate) mod u8_encoder_decoder;

pub(crate) type BytesNeededForEncoding = usize;
pub(crate) type EndOffset = usize;

pub(crate) trait EncoderDecoder<T: ?Sized + ToOwned> {
    fn bytes_needed_for_encoding(&self, source: &T) -> BytesNeededForEncoding;

    fn encode(
        &self,
        source: &T,
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding;

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (Cow<'a, T>, EndOffset);
}

macro_rules! generate_fixed_size_numeric_encoder_decoder {
    ($type:ty, $name:ident, $encode_fn:path, $decode_fn:path) => {
        pub(crate) struct $name;

        impl $name {
            const SIZE: usize = std::mem::size_of::<$type>();
        }

        impl EncoderDecoder<$type> for $name {
            fn bytes_needed_for_encoding(&self, _source: &$type) -> BytesNeededForEncoding {
                Self::SIZE
            }

            fn encode(
                &self,
                source: &$type,
                destination: &mut [u8],
                destination_starting_offset: usize,
            ) -> BytesNeededForEncoding {
                $encode_fn(&mut destination[destination_starting_offset..], *source);
                Self::SIZE
            }

            fn decode<'a>(
                &self,
                encoded: &'a [u8],
                from_offset: usize,
            ) -> (Cow<'a, $type>, EndOffset) {
                (
                    Cow::Owned($decode_fn(&encoded[from_offset..])),
                    from_offset + Self::SIZE,
                )
            }
        }
    };
}

macro_rules! generate_fixed_size_numeric_encoder_decoder_tests {
    ($type:ty, $test_module_name:ident, $encoder_name:ident) => {
        #[cfg(test)]
        mod $test_module_name {
            use super::*;

            #[test]
            fn encode_decode() {
                let encoder = $encoder_name;

                let value: $type = match std::mem::size_of::<$type>() {
                    1 => 250,
                    _ => 2500,
                };

                let mut buffer = vec![0u8; std::mem::size_of::<$type>()];
                encoder.encode(&value, &mut buffer, 0);

                let (decoded, _) = encoder.decode(&buffer, 0);
                assert_eq!(value, *decoded);
            }

            #[test]
            fn encode_decode_at_a_diffent_offset() {
                let encoder = $encoder_name;

                let value: $type = match std::mem::size_of::<$type>() {
                    1 => 250,
                    _ => 2500,
                };

                let mut buffer = vec![0u8; 100];
                encoder.encode(&value, &mut buffer, 10);

                let (decoded, _) = encoder.decode(&buffer, 10);
                assert_eq!(value, *decoded);
            }
        }
    };
}

fn encode_u16(buffer: &mut [u8], value: u16) {
    byteorder::LittleEndian::write_u16(buffer, value);
}
fn decode_u16(buffer: &[u8]) -> u16 {
    byteorder::LittleEndian::read_u16(buffer)
}
fn encode_u32(buffer: &mut [u8], value: u32) {
    byteorder::LittleEndian::write_u32(buffer, value);
}
fn decode_u32(buffer: &[u8]) -> u32 {
    byteorder::LittleEndian::read_u32(buffer)
}

generate_fixed_size_numeric_encoder_decoder!(u16, U16EncoderDecoder, encode_u16, decode_u16);
generate_fixed_size_numeric_encoder_decoder!(u32, U32EncoderDecoder, encode_u32, decode_u32);

generate_fixed_size_numeric_encoder_decoder_tests!(u16, u16_encoder_decoder_tests, U16EncoderDecoder);
generate_fixed_size_numeric_encoder_decoder_tests!(u32, u32_encoder_decoder_tests, U32EncoderDecoder);
