use crate::buffer::page::BufferPage;
use crate::file::block_id::BlockId;
use crate::log::log_manager::LogManager;
use std::io;
use std::path::Path;

struct Buffer {
    page: Option<BufferPage>,
    block_id: Option<BlockId>,
    pins: isize,
    transaction_number: isize,
    log_sequence_number: usize,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            page: None,
            block_id: None,
            pins: 0,
            transaction_number: -1,
            log_sequence_number: 0,
        }
    }

    fn assign_to_block<'a, PathType: AsRef<Path>>(
        &mut self,
        block_id: BlockId,
        log_manager: &'a mut LogManager<'a, PathType>,
    ) -> Result<(), io::Error> {
        self.flush(log_manager)?;
        self.page = Some(log_manager.file_manager().read::<BufferPage>(&block_id)?);
        self.block_id = Some(block_id);
        self.pins = 0;
        Ok(())
    }

    fn pin(&mut self) {
        self.pins += 1;
    }

    fn unpin(&mut self) {
        self.pins -= 1;
    }

    fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    fn flush<'a, PathType: AsRef<Path>>(
        &mut self,
        log_manager: &mut LogManager<PathType>,
    ) -> Result<(), io::Error> {
        if self.transaction_number >= 0 && self.page.is_some() {
            let _ = &mut log_manager.flush(self.log_sequence_number)?;
            log_manager.file_manager().write(
                &self.block_id.as_ref().unwrap(),
                self.page.as_mut().unwrap().finish(),
            )?;
            self.transaction_number = -1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer::Buffer;
    use crate::buffer::page::BufferPage;
    use crate::file::block_id::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use std::borrow::Cow;
    use tempfile::NamedTempFile;

    const BLOCK_SIZE: usize = 4096;

    #[test]
    fn buffer_is_not_pinned() {
        let buffer = Buffer::new();
        assert_eq!(false, buffer.is_pinned());
    }

    #[test]
    fn assign_block_to_buffer() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u16(250);
        page.add_string(String::from("BoltDB is a B+Tree based storage engine"));

        assert!(log_manager
            .file_manager()
            .write(&BlockId::new(buffer_file_name, 0), page.finish())
            .is_ok());

        let mut buffer = Buffer::new();
        buffer
            .assign_to_block(BlockId::new(buffer_file_name, 0), &mut log_manager)
            .unwrap();

        let buffer_page = buffer.page.unwrap();
        assert_eq!(250, buffer_page.get_u16(0).unwrap());
        assert_eq!(
            Some(Cow::Owned(String::from(
                "BoltDB is a B+Tree based storage engine"
            ))),
            buffer_page.get_string(1)
        );
    }

    #[test]
    fn pin_a_buffer() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u16(250);
        page.add_string(String::from("BoltDB is a B+Tree based storage engine"));

        assert!(log_manager
            .file_manager()
            .write(&BlockId::new(buffer_file_name, 0), page.finish())
            .is_ok());

        let mut buffer = Buffer::new();
        buffer
            .assign_to_block(BlockId::new(buffer_file_name, 0), &mut log_manager)
            .unwrap();

        buffer.pin();

        assert!(buffer.is_pinned());
        assert_eq!(1, buffer.pins);
    }

    #[test]
    fn unpin_a_buffer() {
        let file = NamedTempFile::new().expect("Failed to create temp file");
        let directory_path = file.path().parent().unwrap();
        let buffer_file_name = file.path().file_name().unwrap().to_str().unwrap();
        let log_file_name = format!("{}.log", buffer_file_name);

        let file_manager = FileManager::new(directory_path, BLOCK_SIZE).unwrap();
        let mut log_manager = LogManager::new(&file_manager, log_file_name.to_string()).unwrap();

        let mut page = BufferPage::new(BLOCK_SIZE);
        page.add_u16(250);
        page.add_string(String::from("BoltDB is a B+Tree based storage engine"));

        assert!(log_manager
            .file_manager()
            .write(&BlockId::new(buffer_file_name, 0), page.finish())
            .is_ok());

        let mut buffer = Buffer::new();
        buffer
            .assign_to_block(BlockId::new(buffer_file_name, 0), &mut log_manager)
            .unwrap();

        buffer.pin();
        buffer.unpin();

        assert_eq!(false, buffer.is_pinned());
        assert_eq!(0, buffer.pins);
    }

    //TODO: add a test for flushing a page
}
