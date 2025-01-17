use crate::file::block_id::BlockId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::{fs, io};

pub(crate) struct FileManager<PathType: AsRef<Path>> {
    directory: PathType,
    block_size: usize,
    open_files: HashMap<String, File>,
}

impl<PathType: AsRef<Path>> FileManager<PathType> {
    fn new(directory: PathType, block_size: usize) -> Result<Self, io::Error> {
        let exists = fs::metadata(directory.as_ref()).is_ok();
        if !exists {
            fs::create_dir(directory.as_ref())?
        }
        Ok(FileManager {
            directory,
            block_size,
            open_files: HashMap::new(),
        })
    }

    fn read_into(&mut self, block_id: &BlockId, buffer: &mut [u8]) -> Result<(), io::Error> {
        self.seek_and_run(block_id, |file| {
            file.read_exact(buffer).map(|_number_of_bytes_read| ())
        })
    }

    fn write(&mut self, block_id: &BlockId, data: &[u8]) -> Result<(), io::Error> {
        self.seek_and_run(block_id, |file| {
            file.write_all(data)?;
            file.sync_data()
        })
    }

    fn seek_and_run<Block: FnMut(&mut File) -> Result<(), io::Error>>(
        &mut self,
        block_id: &BlockId,
        mut block: Block,
    ) -> Result<(), io::Error> {
        let block_size = self.block_size;
        let mut file = self.get_or_create(block_id.file_name())?;
        file.seek(SeekFrom::Start(block_id.starting_offset(block_size) as u64))?;
        block(&mut file)
    }

    fn get_or_create(&mut self, file_name: &str) -> Result<&mut File, io::Error> {
        let path = self.directory.as_ref().join(Path::new(&file_name));
        let path = path.to_str().unwrap();
        if !self.open_files.contains_key(path) {
            let file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            self.open_files.insert(path.to_string(), file);
        }
        Ok(self.open_files.get_mut(path).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use crate::file::block_id::BlockId;
    use crate::file::file_manager::FileManager;
    use tempfile::NamedTempFile;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn write_content_at_block_zero() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let mut file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(
            &block_id,
            b"RocksDB is an LSM-based storage engine",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn read_content_at_block_zero_and_read_the_same() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let mut file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let write_buffer = b"RocksDB is an LSM-based storage engine";
        let block_id = BlockId::new(file_name, 0);
        let result = file_manager.write(&block_id, write_buffer);
        assert!(result.is_ok());

        let mut read_buffer = vec![0; write_buffer.len()];
        file_manager
            .read_into(&block_id, &mut read_buffer)
            .unwrap();

        assert_eq!(read_buffer, write_buffer);
    }

    #[test]
    fn read_content_at_block_higher_than_zero_and_read_the_same() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let file_name = file.path().file_name().unwrap().to_str().unwrap();

        let mut file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let write_buffer = b"PebbleDB is an LSM-based storage engine";
        let block_id = BlockId::new(file_name, 5);
        let result = file_manager.write(&block_id, write_buffer);
        assert!(result.is_ok());

        let mut read_buffer = vec![0; write_buffer.len()];
        file_manager
            .read_into(&block_id, &mut read_buffer)
            .unwrap();

        assert_eq!(read_buffer, write_buffer);
    }
}
