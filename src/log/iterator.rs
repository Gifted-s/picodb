use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::page::{BackwardRecordIterator, LogPage};
use crate::page::Page;
use std::io;
use std::path::Path;
use std::rc::Rc;

pub(crate) struct BackwardLogIterator<'a, PathType: AsRef<Path>> {
    file_manager: &'a mut FileManager<PathType>,
    current_block_id: BlockId,
    record_iterator: BackwardRecordIterator,
}

impl<'a, PathType: AsRef<Path>> BackwardLogIterator<'a, PathType> {
    pub(crate) fn new(
        file_manager: &'a mut FileManager<PathType>,
        current_block_id: BlockId,
    ) -> Result<BackwardLogIterator<'a, PathType>, io::Error> {
        let mut buffer = vec![0; file_manager.block_size];
        file_manager.read_into(&mut buffer, &current_block_id)?;

        Ok(BackwardLogIterator {
            file_manager,
            current_block_id,
            record_iterator: BackwardRecordIterator::new(Rc::new(LogPage::decode_from(buffer))),
        })
    }

    //TODO: avoid copy in the return type
    pub(crate) fn record(&mut self) -> Option<Vec<u8>> {
        if let Some(record) = self.record_iterator.record() {
            return Some(record.to_vec());
        }
        if self.current_block_id.block_number > 0 {
            self.current_block_id = self.current_block_id.previous().unwrap();
            let mut buffer = vec![0; self.file_manager.block_size];
            self.file_manager
                .read_into(&mut buffer, &self.current_block_id)
                .unwrap();

            self.record_iterator =
                BackwardRecordIterator::new(Rc::new(LogPage::decode_from(buffer)));
            return self.record_iterator.record().map(Vec::from);
        }
        None
    }
}
