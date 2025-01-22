use byteorder::ByteOrder;

const SIZE_OF_OFFSET: usize = size_of::<u32>();

pub(crate) struct StartingOffsets {
    offsets: Vec<u32>,
}

impl StartingOffsets {
    pub(crate) fn new() -> Self {
        Self { offsets: vec![] }
    }

    pub(crate) fn decode_from(buffer: &[u8]) -> Self {
        let mut starting_offsets = Self::new();
        buffer
            .chunks_exact(SIZE_OF_OFFSET)
            .map(byteorder::LittleEndian::read_u32)
            .for_each(|offset| starting_offsets.offsets.push(offset));

        starting_offsets
    }

    pub(crate) fn add_offset(&mut self, offset: u32) {
        self.offsets.push(offset);
    }

    pub(crate) fn offset_at(&self, index: usize) -> Option<&u32> {
        self.offsets.get(index)
    }

    pub(crate) fn last_offset(&self) -> Option<&u32> {
        self.offsets.last()
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded_offsets = Vec::with_capacity(self.offsets.len() * SIZE_OF_OFFSET);
        for &offset in &self.offsets {
            encoded_offsets.extend(&offset.to_le_bytes());
        }
        encoded_offsets
    }

    pub(crate) fn size_in_bytes(&self) -> usize {
        self.offsets.len() * SIZE_OF_OFFSET
    }

    pub(crate) fn size_in_bytes_for_an_offset() -> usize {
        SIZE_OF_OFFSET
    }

    pub(crate) fn size_in_bytes_for(number_of_offsets: usize) -> usize {
        SIZE_OF_OFFSET * number_of_offsets
    }

    pub(crate) fn length(&self) -> usize {
        self.offsets.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::file::starting_offsets::StartingOffsets;

    #[test]
    fn encode_decode_starting_a_single_starting_offset() {
        let mut starting_offsets = StartingOffsets::new();
        starting_offsets.add_offset(20);

        let encoded = starting_offsets.encode();
        let decoded = StartingOffsets::decode_from(&encoded);

        assert_eq!(Some(&20), decoded.offset_at(0));
    }

    #[test]
    fn encode_decode_starting_starting_offsets_with_a_few_offsets() {
        let mut starting_offsets = StartingOffsets::new();
        starting_offsets.add_offset(20);
        starting_offsets.add_offset(400);
        starting_offsets.add_offset(520);

        let encoded = starting_offsets.encode();
        let decoded = StartingOffsets::decode_from(&encoded);

        assert_eq!(Some(&20), decoded.offset_at(0));
        assert_eq!(Some(&400), decoded.offset_at(1));
        assert_eq!(Some(&520), decoded.offset_at(2));
    }
}
