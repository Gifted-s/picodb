use crate::buffer::buffer::Buffer;
use crate::file::block_id::BlockId;
use crate::log::log_manager::LogManager;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::path::Path;

#[derive(Debug)]
enum BufferPinError {
    IO(io::Error),
    UNAVAILABLE,
}

impl BufferPinError {
    fn is_unavailable_error(&self) -> bool {
        if let BufferPinError::UNAVAILABLE = self {
            return true;
        }
        false
    }
}

impl From<io::Error> for BufferPinError {
    fn from(error: io::Error) -> Self {
        BufferPinError::IO(error)
    }
}

impl Display for BufferPinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BufferPinError::IO(err) => write!(f, "Buffer I/O error: {}", err),
            BufferPinError::UNAVAILABLE => write!(f, "Buffer is unavailable"),
        }
    }
}

impl Error for BufferPinError {}

struct BufferManager<'a, PathType: AsRef<Path>> {
    buffer_pool: Vec<Buffer>,
    log_manager: &'a mut LogManager<'a, PathType>,
    available_buffers: usize,
}

impl<'a, PathType: AsRef<Path>> BufferManager<'a, PathType> {
    fn new(
        capacity: usize,
        log_manager: &'a mut LogManager<'a, PathType>,
    ) -> BufferManager<'a, PathType> {
        BufferManager {
            buffer_pool: vec![Buffer::new()],
            log_manager,
            available_buffers: capacity,
        }
    }

    fn pin(&mut self, block_id: BlockId) -> Result<&mut Buffer, BufferPinError> {
        self.try_pin(block_id)
    }

    fn unpin(&mut self, block_id: &BlockId) {
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.has_block_id(&block_id) {
                buffer.unpin();
                if !buffer.is_pinned() {
                    self.available_buffers += 1;
                }
                return;
            }
        }
    }

    fn try_pin(&mut self, block_id: BlockId) -> Result<&mut Buffer, BufferPinError> {
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.has_block_id(&block_id) {
                if !buffer.is_pinned() {
                    self.available_buffers -= 1;
                }
                buffer.pin();
                return Ok(buffer);
            }
            if !buffer.is_pinned() {
                buffer.assign_to_block(block_id, &mut self.log_manager)?;
                self.available_buffers -= 1;
                buffer.pin();
                return Ok(buffer);
            }
        }
        Err(BufferPinError::UNAVAILABLE)
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_manager::BufferManager;
    use crate::file::block_id::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use std::borrow::Cow;
    use tempfile::NamedTempFile;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn fail_to_pin_a_buffer() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let mut buffer_manager = BufferManager::new(1, &mut log_manager);
        buffer_manager.buffer_pool[0].pin();

        assert!(buffer_manager
            .pin(BlockId::new(buffer_file_name, 0))
            .err()
            .unwrap()
            .is_unavailable_error());
    }

    #[test]
    fn available_buffers() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let buffer_manager = BufferManager::new(1, &mut log_manager);
        assert_eq!(1, buffer_manager.available_buffers);
    }

    #[test]
    fn pin_a_buffer() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let mut buffer_manager = BufferManager::new(1, &mut log_manager);
        let buffer = buffer_manager
            .pin(BlockId::new(buffer_file_name, 0))
            .unwrap();

        let page = buffer.page().unwrap();
        page.add_string(String::from("RocksDB is an LSM based storage engine"));
        page.add_u16(250);

        buffer.set_modified(10, 100);
        {
            //simulate flush
            buffer_manager.unpin(&BlockId::new(buffer_file_name, 0));
            let _ = buffer_manager
                .pin(BlockId::new(buffer_file_name, 1))
                .unwrap();

            buffer_manager.unpin(&BlockId::new(buffer_file_name, 1));
        }

        let pinned = buffer_manager
            .pin(BlockId::new(buffer_file_name, 0))
            .unwrap();

        let pinned_page = pinned.page();
        let reassigned_buffer_page = pinned_page.as_ref().unwrap();
        assert_eq!(
            Some(Cow::Owned(String::from(
                "RocksDB is an LSM based storage engine"
            ))),
            reassigned_buffer_page.get_string(0)
        );
        assert_eq!(250, reassigned_buffer_page.get_u16(1).unwrap());
    }
}
