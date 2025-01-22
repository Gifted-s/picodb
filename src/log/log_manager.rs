use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::iterator::BackwardLogIterator;
use crate::log::page::LogPage;
use std::io;
use std::path::Path;

pub(crate) struct LogManager<'a, PathType: AsRef<Path>> {
    file_manager: &'a FileManager<PathType>,
    log_file_name: String,
    log_page: LogPage,
    current_block_id: BlockId,
    latest_log_sequence_number: usize,
    last_saved_log_sequence_number: usize,
}

impl<'a, PathType: AsRef<Path>> LogManager<'a, PathType> {
    pub(crate) fn new(
        file_manager: &'a FileManager<PathType>,
        log_file_name: String,
    ) -> Result<LogManager<'a, PathType>, io::Error> {
        let number_of_blocks = file_manager.number_of_blocks(&log_file_name)?;
        let (block_id, log_page) = match number_of_blocks {
            0 => (
                file_manager.append_empty_block(&log_file_name)?,
                LogPage::new(file_manager.block_size),
            ),
            _ => {
                let block_id = BlockId::new(&log_file_name, number_of_blocks - 1);
                let page = file_manager.read::<LogPage>(&block_id)?;
                (block_id, page)
            }
        };
        Ok(LogManager {
            file_manager,
            log_file_name,
            log_page,
            current_block_id: block_id,
            latest_log_sequence_number: 0,     //TODO: revisit
            last_saved_log_sequence_number: 0, //TODO: revisit
        })
    }

    fn append(&mut self, buffer: &[u8]) -> Result<(), io::Error> {
        if !self.log_page.add(buffer) {
            self.force_flush()?;
            self.current_block_id = self
                .file_manager
                .append_empty_block(self.log_file_name.as_ref())?;
            self.log_page = LogPage::new(self.file_manager.block_size);
            assert!(self.log_page.add(buffer));
        }
        self.latest_log_sequence_number = self.latest_log_sequence_number + 1;
        Ok(())
    }

    fn backward_iterator(&mut self) -> Result<BackwardLogIterator<PathType>, io::Error> {
        self.force_flush()?;
        BackwardLogIterator::new(self.file_manager, self.current_block_id.clone())
    }

    pub(crate) fn flush(&mut self, log_sequence_number: usize) -> Result<(), io::Error> {
        if log_sequence_number >= self.last_saved_log_sequence_number {
            self.force_flush()?
        }
        Ok(())
    }

    pub(crate) fn file_manager(&self) -> &FileManager<PathType> {
        &self.file_manager
    }

    fn force_flush(&mut self) -> Result<(), io::Error> {
        self.file_manager
            .write(&self.current_block_id, &self.log_page.finish())?;
        self.last_saved_log_sequence_number = self.latest_log_sequence_number;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use tempfile::NamedTempFile;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn append_a_record_in_log() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let log_file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        assert!(log_manager
            .append(b"RocksDB is an LSM-based storage engine")
            .is_ok());
    }

    #[test]
    fn append_a_record_in_log_and_iterate_over_it() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let log_file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        assert!(log_manager
            .append(b"RocksDB is an LSM-based storage engine")
            .is_ok());

        let mut iterator = log_manager.backward_iterator().unwrap();
        assert_eq!(
            b"RocksDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn append_a_few_records_in_log_and_iterate_over_it() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let log_file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        assert!(log_manager
            .append(b"RocksDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager
            .append(b"PebbleDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager
            .append(b"BoltDB is a B+Tree storage engine")
            .is_ok());

        let mut iterator = log_manager.backward_iterator().unwrap();
        assert_eq!(
            b"BoltDB is a B+Tree storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"PebbleDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"RocksDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn append_a_few_records_in_log_with_smaller_block_size_and_iterate_over_it() {
        const BLOCK_SIZE_IN_BYTES: usize = 200;
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let log_file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE_IN_BYTES).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        assert!(log_manager
            .append(b"RocksDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager
            .append(b"PebbleDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager
            .append(b"BoltDB is a B+Tree storage engine")
            .is_ok());

        let mut iterator = log_manager.backward_iterator().unwrap();
        assert_eq!(
            b"BoltDB is a B+Tree storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"PebbleDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"RocksDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }

    #[test]
    fn append_a_few_records_in_log_and_recreate_log_manager_to_simulate_restart() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let log_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        assert!(log_manager
            .append(b"RocksDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager
            .append(b"PebbleDB is an LSM-based storage engine")
            .is_ok());
        assert!(log_manager.force_flush().is_ok());

        drop(log_manager);

        let mut reloaded_log_manager =
            LogManager::new(&file_manager, log_file_name.to_string()).unwrap();
        assert!(reloaded_log_manager
            .append(b"BoltDB is a B+Tree storage engine")
            .is_ok());

        let mut iterator = reloaded_log_manager.backward_iterator().unwrap();
        assert_eq!(
            b"BoltDB is a B+Tree storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"PebbleDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(
            b"RocksDB is an LSM-based storage engine".to_vec(),
            iterator.record().unwrap()
        );
        assert_eq!(None, iterator.record());
    }
}
