pub(crate) trait Page {
    fn decode_from(buffer: Vec<u8>) -> Self;
    fn buffer(&self) -> &[u8];
}
