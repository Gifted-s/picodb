use crate::file::block_id::BlockId;
use crate::page::Page;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::{fs, io};

pub(crate) struct FileManager<PathType: AsRef<Path>> {
    directory: PathType,
    pub(crate) block_size: usize,
    open_files: RefCell<HashMap<String, File>>,
}

impl<PathType: AsRef<Path>> FileManager<PathType> {
    pub(crate) fn new(directory: PathType, block_size: usize) -> Result<Self, io::Error> {
        let exists = fs::metadata(directory.as_ref()).is_ok();
        if !exists {
            fs::create_dir(directory.as_ref())?
        }
        Ok(FileManager {
            directory,
            block_size,
            open_files: RefCell::new(HashMap::new()),
        })
    }

    pub(crate) fn read<T: Page>(&self, block_id: &BlockId) -> Result<T, io::Error> {
        let mut read_buffer = vec![0; self.block_size];
        self.seek_and_run(block_id, |file| {
            file.read(&mut read_buffer).map(|_number_of_bytes_read| ())
        })?;
        Ok(T::decode_from(read_buffer))
    }

    pub(crate) fn write(&self, block_id: &BlockId, data: &[u8]) -> Result<(), io::Error> {
        self.seek_and_run(block_id, |file| {
            file.write_all(data)?;
            file.sync_data()
        })
    }

    pub(crate) fn append_empty_block(&self, file_name: &str) -> Result<BlockId, io::Error> {
        let block_id = BlockId::new(file_name, self.number_of_blocks(file_name)?);
        let block_size = self.block_size;

        self.seek_and_run(&block_id, |file| {
            file.write_all(&vec![0; block_size])?;
            file.sync_data()
        })?;

        Ok(block_id)
    }

    pub(crate) fn number_of_blocks(&self, file_name: &str) -> Result<usize, io::Error> {
        let file = self.get_or_create(file_name)?;
        let metadata = file.metadata()?;
        Ok(metadata.len() as usize / self.block_size) //TODO: validate
    }

    fn seek_and_run<Block: FnMut(&mut File) -> Result<(), io::Error>>(
        &self,
        block_id: &BlockId,
        mut block: Block,
    ) -> Result<(), io::Error> {
        let block_size = self.block_size;
        let mut file = self.get_or_create(block_id.file_name())?;
        file.seek(SeekFrom::Start(block_id.starting_offset(block_size) as u64))?;
        block(&mut file)
    }

    fn get_or_create(&self, file_name: &str) -> Result<RefMut<'_, File>, io::Error> {
        let path = self.directory.as_ref().join(Path::new(&file_name));
        let path = path.to_str().unwrap();

        let mut open_files = self.open_files.borrow_mut();
        if !open_files.contains_key(path) {
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;

            open_files.insert(path.to_string(), file);
        }
        Ok(RefMut::map(open_files, |files| {
            files.get_mut(path).unwrap()
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::file::block_id::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::page::Page;
    use tempfile::NamedTempFile;

    const BLOCK_SIZE: usize = 4096;

    struct TestPage {
        buffer: Vec<u8>,
    }

    impl Page for TestPage {
        fn decode_from(buffer: Vec<u8>) -> Self {
            TestPage { buffer }
        }
    }

    #[test]
    fn write_content_at_block_zero() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(&block_id, b"RocksDB is an LSM-based storage engine");
        assert!(result.is_ok());
    }

    #[test]
    fn read_content_at_block_zero_and_read_the_same() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let write_buffer = b"RocksDB is an LSM-based storage engine";
        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(&block_id, write_buffer);
        assert!(result.is_ok());

        let page = file_manager.read::<TestPage>(&block_id).unwrap();
        assert_eq!(&page.buffer[..write_buffer.len()], write_buffer);
    }

    #[test]
    fn read_content_at_block_higher_than_zero_and_read_the_same() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let write_buffer = b"PebbleDB is an LSM-based storage engine";
        let block_id = BlockId::new(file_name, 5);
        let result = file_manager.write(&block_id, write_buffer);
        assert!(result.is_ok());

        let page = file_manager.read::<TestPage>(&block_id).unwrap();
        assert_eq!(&page.buffer[..write_buffer.len()], write_buffer);
    }

    #[test]
    fn number_of_blocks_zero() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let number_of_blocks = file_manager.number_of_blocks(file_name).unwrap();

        assert_eq!(0, number_of_blocks);
    }

    #[test]
    fn number_of_blocks_with_content_in_a_file() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, 40).unwrap();
        let write_buffer = b"PebbleDB is an LSM-based storage engine.";
        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(&block_id, write_buffer);
        assert!(result.is_ok());

        let number_of_blocks = file_manager.number_of_blocks(file_name).unwrap();
        assert_eq!(1, number_of_blocks);
    }

    #[test]
    fn append_empty_block() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        file_manager.append_empty_block(&file_name).unwrap();

        let block_id = BlockId::new(file_name, 0);

        let page = file_manager.read::<TestPage>(&block_id).unwrap();
        assert_eq!(vec![0; BLOCK_SIZE], page.buffer);
    }

    #[test]
    fn append_a_couple_of_empty_blocks() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        file_manager.append_empty_block(&file_name).unwrap();

        let mut buffer = vec![0; BLOCK_SIZE];
        let content = b"PebbleDB is an LSM-based storage engine.";
        let content_length = content.len();
        buffer[..content_length].copy_from_slice(&content[..content_length]);

        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(&block_id, &buffer);
        assert!(result.is_ok());

        let new_block_id = file_manager.append_empty_block(&file_name).unwrap();
        assert_eq!(1, new_block_id.block_number);
    }
}
