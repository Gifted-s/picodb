use std::borrow::Cow;

pub(crate) mod bytes_encoder_decoder;
mod u16_encoder_decoder;
mod u8_encoder_decoder;
mod string_encoder_decoder;

type BytesNeededForEncoding = usize;
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
