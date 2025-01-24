use crate::file::block_id::BlockId;
use crate::file::file_manager::FileManager;
use crate::log::page::{BackwardRecordIterator, LogPage};
use std::io;
use std::path::Path;
use std::rc::Rc;

pub(crate) struct BackwardLogIterator<'a, PathType: AsRef<Path>> {
    file_manager: &'a FileManager<PathType>,
    current_block_id: BlockId,
    record_iterator: BackwardRecordIterator,
}

impl<'a, PathType: AsRef<Path>> Iterator for BackwardLogIterator<'a, PathType> {
    type Item = Vec<u8>;

    //TODO: avoid copy in the return type
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(record) = self.record_iterator.record() {
            return Some(record.to_vec());
        }
        if self.current_block_id.block_number > 0 {
            self.current_block_id = self.current_block_id.previous().unwrap();
            let page = self
                .file_manager
                .read::<LogPage>(&self.current_block_id)
                .unwrap();

            self.record_iterator = BackwardRecordIterator::new(Rc::new(page));
            return self.record_iterator.record().map(Vec::from);
        }
        None
    }
}

impl<'a, PathType: AsRef<Path>> BackwardLogIterator<'a, PathType> {
    pub(crate) fn new(
        file_manager: &'a FileManager<PathType>,
        current_block_id: BlockId,
    ) -> Result<BackwardLogIterator<'a, PathType>, io::Error> {
        let page = file_manager.read::<LogPage>(&current_block_id)?;

        Ok(BackwardLogIterator {
            file_manager,
            current_block_id,
            record_iterator: BackwardRecordIterator::new(Rc::new(page)),
        })
    }
}
