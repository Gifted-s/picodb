use crate::encoder_decoder::EncoderDecoder;
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;

const RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS: usize = size_of::<u16>();

struct Page {
    buffer: Vec<u8>,
    starting_offsets: StartingOffsets,
    current_write_offset: usize,
}

struct BackwardRecordIterator<'p> {
    page: &'p Page,
    current_offset_index: Option<usize>,
}

impl<'p> BackwardRecordIterator<'p> {
    fn record(&mut self) -> Option<&'p [u8]> {
        self.current_offset_index.and_then(|offset_index| {
            self.page
                .starting_offsets
                .offset_at(offset_index)
                .map(|record_starting_offset| {
                    let record = self.page.bytes_at(*record_starting_offset as usize);
                    self.current_offset_index = offset_index.checked_sub(1);
                    record
                })
        })
    }
}

impl Page {
    fn new(block_size: usize) -> Self {
        Page {
            buffer: vec![0; block_size],
            starting_offsets: StartingOffsets::new(),
            current_write_offset: 0,
        }
    }

    fn add(&mut self, data: &[u8]) -> bool {
        if !self.has_capacity_for(data) {
            return false;
        }
        self.starting_offsets
            .add_offset(self.current_write_offset as u32);

        let bytes_needed_for_encoding =
            EncoderDecoder::encode_bytes(data, &mut self.buffer, self.current_write_offset);

        self.current_write_offset += bytes_needed_for_encoding;
        true
    }

    fn bytes_at(&self, offset: usize) -> &[u8] {
        let (decoded, _) = EncoderDecoder::decode_bytes(&self.buffer, offset);
        decoded
    }

    fn finish(&mut self) -> &[u8] {
        if self.starting_offsets.length() == 0 {
            panic!("empty log page")
        }
        self.write_encoded_starting_offsets(&self.encode_starting_offsets());
        self.write_number_of_starting_offsets();
        &self.buffer
    }

    fn backward_iterator(&self) -> BackwardRecordIterator {
        if self.starting_offsets.length() == 0 {
            panic!("empty log page")
        }
        BackwardRecordIterator {
            page: &self,
            current_offset_index: Some(self.starting_offsets.length() - 1),
        }
    }

    fn has_capacity_for(&self, buffer: &[u8]) -> bool {
        let bytes_available = self.buffer.len()
            - self.current_write_offset
            - self.starting_offsets.size_in_bytes()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS;

        let bytes_needed = EncoderDecoder::bytes_needed_for(buffer)
            + StartingOffsets::size_in_bytes_for_an_offset();

        bytes_available >= bytes_needed
    }

    fn encode_starting_offsets(&self) -> Vec<u8> {
        self.starting_offsets.encode()
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

    fn write_number_of_starting_offsets(&mut self) {
        let encoded_page = &mut self.buffer;
        let encoded_page_length = encoded_page.len();

        byteorder::LittleEndian::write_u16(
            &mut encoded_page[encoded_page_length - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS..],
            self.starting_offsets.length() as u16,
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::log::page::Page;

    #[test]
    fn attempt_to_add_a_record_to_a_page_with_insufficient_size() {
        let mut page = Page::new(30);
        assert_eq!(
            false,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
    }

    #[test]
    fn attempt_to_add_a_couple_of_records_in_a_page_with_size_sufficient_for_only_one_record() {
        let mut page = Page::new(60);
        assert_eq!(
            true,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
        assert_eq!(
            false,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
    }

    #[test]
    fn attempt_to_add_a_couple_of_records_successfully_in_a_page_with_just_enough_size() {
        let mut page = Page::new(110);
        assert_eq!(
            true,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
        assert_eq!(
            true,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
    }

    #[test]
    #[should_panic]
    fn attempt_to_create_a_log_with_no_records() {
        let mut page = Page::new(110);
        let _ = page.finish();
    }

    #[test]
    fn create_a_log_with_a_single_record() {
        let mut page = Page::new(4096);
        page.add(b"RocksDB is an LSM-based key/value storage engine");

        let _ = page.finish();
        let mut iterator = page.backward_iterator();
        assert_eq!(
            b"RocksDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
    }

    #[test]
    fn create_a_log_with_a_couple_of_records() {
        let mut page = Page::new(4096);
        page.add(b"RocksDB is an LSM-based key/value storage engine");
        page.add(b"PebbleDB is an LSM-based key/value storage engine");

        let _ = page.finish();
        let mut iterator = page.backward_iterator();

        assert_eq!(
            b"PebbleDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
        assert_eq!(
            b"RocksDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn create_a_log_with_a_few_records() {
        let mut page = Page::new(4096);
        (1..=100)
            .map(|record_id| format!("Record {}", record_id))
            .for_each(|record| {
                page.add(record.as_bytes());
            });

        let _ = page.finish();
        let mut iterator = page.backward_iterator();

        (1..=100).rev().for_each(|record_id| {
            let record = format!("Record {}", record_id);
            assert_eq!(record.as_bytes(), iterator.record().unwrap());
        });
        assert_eq!(None, iterator.record());
    }
}
