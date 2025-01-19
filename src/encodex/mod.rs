pub(crate) mod bytes_encoder_decoder;

type BytesNeededForEncoding = usize;
type EndOffset = usize;

pub(crate) trait EncoderDecoder<T: ?Sized> {
    fn bytes_needed_for_encoding(&self, source: &T) -> BytesNeededForEncoding;

    fn encode(
        &self,
        source: &T,
        destination: &mut [u8],
        destination_starting_offset: usize,
    ) -> BytesNeededForEncoding;

    fn decode<'a>(&self, encoded: &'a [u8], from_offset: usize) -> (&'a T, EndOffset);
}
