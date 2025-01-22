use crate::encodex::bytes_encoder_decoder::BytesEncoderDecoder;
use crate::encodex::{EncoderDecoder, EndOffset};
use crate::file::starting_offsets::StartingOffsets;
use byteorder::ByteOrder;
use std::borrow::Cow;
use std::rc::Rc;

const RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS: usize = size_of::<u16>();

pub(crate) struct LogPage {
    buffer: Vec<u8>,
    starting_offsets: StartingOffsets,
    current_write_offset: usize,
}

pub(crate) struct BackwardRecordIterator {
    //TODO: revisit, maybe a reference, or an Arc
    page: Rc<LogPage>,
    current_offset_index: Option<usize>,
}

impl BackwardRecordIterator {
    pub(crate) fn new(page: Rc<LogPage>) -> Self {
        let current_offset_index = page.starting_offsets.length() - 1;
        Self {
            page,
            current_offset_index: Some(current_offset_index),
        }
    }

    pub(crate) fn record(&mut self) -> Option<&[u8]> {
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

impl crate::page::Page for LogPage {
    fn decode_from(buffer: Vec<u8>) -> Self {
        if buffer.is_empty() {
            panic!("buffer cannot be empty while decoding the log page");
        }
        PageDecoder::decode_page(buffer)
    }
}

impl LogPage {
    pub(crate) fn new(block_size: usize) -> Self {
        LogPage {
            buffer: vec![0; block_size],
            starting_offsets: StartingOffsets::new(),
            current_write_offset: 0,
        }
    }

    pub(crate) fn add(&mut self, data: &[u8]) -> bool {
        if !self.has_capacity_for(data) {
            return false;
        }
        self.starting_offsets
            .add_offset(self.current_write_offset as u32);

        let bytes_needed_for_encoding =
            BytesEncoderDecoder.encode(data, &mut self.buffer, self.current_write_offset);

        self.current_write_offset += bytes_needed_for_encoding;
        true
    }

    pub(crate) fn finish(&mut self) -> &[u8] {
        if self.starting_offsets.length() == 0 {
            panic!("empty log page")
        }
        let mut page_encoder = PageEncoder {
            buffer: &mut self.buffer,
            starting_offsets: &self.starting_offsets,
        };
        page_encoder.encode();
        &self.buffer
    }

    fn backward_iterator(self: Rc<LogPage>) -> BackwardRecordIterator {
        if self.starting_offsets.length() == 0 {
            panic!("empty log page")
        }
        BackwardRecordIterator::new(self.clone())
    }

    fn bytes_at(&self, offset: usize) -> &[u8] {
        let (decoded, _) = BytesEncoderDecoder.decode(&self.buffer, offset);
        match decoded {
            Cow::Borrowed(slice) => slice,
            _ => unreachable!(),
        }
    }

    fn has_capacity_for(&self, buffer: &[u8]) -> bool {
        let bytes_available = self.buffer.len()
            - self.current_write_offset
            - self.starting_offsets.size_in_bytes()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS;

        let bytes_needed = BytesEncoderDecoder.bytes_needed_for_encoding(buffer)
            + StartingOffsets::size_in_bytes_for_an_offset();

        bytes_available >= bytes_needed
    }
}

struct PageEncoder<'a> {
    buffer: &'a mut [u8],
    starting_offsets: &'a StartingOffsets,
}

struct PageDecoder;

impl<'a> PageEncoder<'a> {
    fn encode(&mut self) {
        self.write_encoded_starting_offsets(&self.starting_offsets.encode());
        self.write_number_of_starting_offsets();
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

impl PageDecoder {
    pub(crate) fn decode_page(buffer: Vec<u8>) -> LogPage {
        let offset_containing_number_of_offsets =
            buffer.len() - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS;

        let number_of_offsets =
            byteorder::LittleEndian::read_u16(&buffer[offset_containing_number_of_offsets..])
                as usize;

        let starting_offsets = Self::decode_starting_offsets(&buffer, &number_of_offsets);
        let end_offset = Self::current_write_offset(&buffer, &starting_offsets);

        LogPage {
            buffer,
            starting_offsets,
            current_write_offset: end_offset,
        }
    }

    fn decode_starting_offsets(buffer: &[u8], number_of_offsets: &usize) -> StartingOffsets {
        let offset_containing_encoded_starting_offsets = buffer.len()
            - RESERVED_SIZE_FOR_NUMBER_OF_OFFSETS
            - StartingOffsets::size_in_bytes_for(*number_of_offsets);

        StartingOffsets::decode_from(
            &buffer[offset_containing_encoded_starting_offsets
                ..offset_containing_encoded_starting_offsets
                    + StartingOffsets::size_in_bytes_for(*number_of_offsets)],
        )
    }

    fn current_write_offset(buffer: &[u8], starting_offsets: &StartingOffsets) -> EndOffset {
        let last_starting_offset = starting_offsets.last_offset().unwrap();
        let (_, end_offset) = BytesEncoderDecoder.decode(&buffer, *last_starting_offset as usize);
        end_offset
    }
}

#[cfg(test)]
mod tests {
    use crate::log::page::LogPage;
    use crate::page::Page;
    use std::rc::Rc;

    #[test]
    fn attempt_to_add_a_record_to_a_page_with_insufficient_size() {
        let mut page = LogPage::new(30);
        assert_eq!(
            false,
            page.add(b"RocksDB is an LSM-based key/value storage engine")
        );
    }

    #[test]
    fn attempt_to_add_a_couple_of_records_in_a_page_with_size_sufficient_for_only_one_record() {
        let mut page = LogPage::new(60);
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
        let mut page = LogPage::new(110);
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
        let mut page = LogPage::new(110);
        let _ = page.finish();
    }

    #[test]
    fn create_a_log_with_a_single_record() {
        let mut page = LogPage::new(4096);
        page.add(b"RocksDB is an LSM-based key/value storage engine");

        let _ = page.finish();
        let mut iterator = Rc::new(page).backward_iterator();
        assert_eq!(
            b"RocksDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
    }

    #[test]
    fn create_a_log_with_a_couple_of_records() {
        let mut page = LogPage::new(4096);
        page.add(b"RocksDB is an LSM-based key/value storage engine");
        page.add(b"PebbleDB is an LSM-based key/value storage engine");

        let _ = page.finish();
        let mut iterator = Rc::new(page).backward_iterator();

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
        let mut page = LogPage::new(4096);
        (1..=100)
            .map(|record_id| format!("Record {}", record_id))
            .for_each(|record| {
                page.add(record.as_bytes());
            });

        let _ = page.finish();
        let mut iterator = Rc::new(page).backward_iterator();

        (1..=100).rev().for_each(|record_id| {
            let record = format!("Record {}", record_id);
            assert_eq!(record.as_bytes(), iterator.record().unwrap());
        });
        assert_eq!(None, iterator.record());
    }

    #[test]
    #[should_panic]
    fn attempt_to_decode_page_with_zero_records() {
        LogPage::decode_from(vec![]);
    }

    #[test]
    fn decode_page_with_a_single_record() {
        let mut page = LogPage::new(4096);
        page.add(b"PebbleDB is an LSM-based key/value storage engine");

        let buffer = page.finish();
        let decoded_page = LogPage::decode_from(buffer.to_vec());

        let _ = page.finish();
        let mut iterator = Rc::new(decoded_page).backward_iterator();

        assert_eq!(
            b"PebbleDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn decode_page_with_a_couple_of_records() {
        let mut page = LogPage::new(4096);
        page.add(b"PebbleDB is an LSM-based key/value storage engine");
        page.add(b"RocksDB is an LSM-based key/value storage engine");

        let buffer = page.finish();
        let decoded_page = LogPage::decode_from(buffer.to_vec());

        let _ = page.finish();
        let mut iterator = Rc::new(decoded_page).backward_iterator();

        assert_eq!(
            b"RocksDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
        assert_eq!(
            b"PebbleDB is an LSM-based key/value storage engine",
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn decode_page_with_a_few_records() {
        let mut page = LogPage::new(4096);
        (1..=50)
            .map(|record_id| format!("Record {}", record_id))
            .for_each(|record| {
                page.add(record.as_bytes());
            });

        let buffer = page.finish();
        let decoded_page = LogPage::decode_from(buffer.to_vec());
        let mut iterator = Rc::new(decoded_page).backward_iterator();

        (1..=50).rev().for_each(|record_id| {
            let record = format!("Record {}", record_id);
            assert_eq!(record.as_bytes(), iterator.record().unwrap());
        });
        assert_eq!(None, iterator.record());
    }
}
