use std::path::{Path, PathBuf};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
};

pub trait Transaction {
    fn commit() -> Result<(), crate::error::Error>;

    fn rollback() -> Result<(), crate::error::Error>;

    fn recover() -> Result<(), crate::error::Error>;

    fn pin(block_id: &BlockId);

    fn unpin(block_id: &BlockId);

    fn get_int(block_id: &BlockId, offset: usize);

    fn get_str(block_id: &BlockId, offset: usize);

    fn set_int(block_id: &BlockId, offset: usize, val: i32, should_log: bool);

    fn set_str(block_id: &BlockId, offset: usize, val: i32, should_log: bool);

    fn available_bufs() -> usize;

    fn size(filename: impl AsRef<Path>) -> usize;

    fn append(filename: impl AsRef<Path>) -> BlockId;

    fn block_size() -> usize;
}

struct PicoDBTransaction<'a, PathType: AsRef<Path>> {
    file_manager: &'a mut FileManager<PathType>,
    log_manager: &'a mut LogManager<'a, PathType>,
    buffer_manager: &'a mut BufferManager<'a, PathType>,
}

impl<'a, P: AsRef<Path>> Transaction for PicoDBTransaction<'a, P> {
    fn commit() -> Result<(), crate::error::Error> {
        todo!()
    }

    fn rollback() -> Result<(), crate::error::Error> {
        todo!()
    }

    fn recover() -> Result<(), crate::error::Error> {
        todo!()
    }

    fn pin(block_id: &BlockId) {
        todo!()
    }

    fn unpin(block_id: &BlockId) {
        todo!()
    }

    fn get_int(block_id: &BlockId, offset: usize) {
        todo!()
    }

    fn get_str(block_id: &BlockId, offset: usize) {
        todo!()
    }

    fn set_int(block_id: &BlockId, offset: usize, val: i32, should_log: bool) {
        todo!()
    }

    fn set_str(block_id: &BlockId, offset: usize, val: i32, should_log: bool) {
        todo!()
    }

    fn available_bufs() -> usize {
        todo!()
    }

    fn size(filename: impl AsRef<Path>) -> usize {
        todo!()
    }

    fn append(filename: impl AsRef<Path>) -> BlockId {
        todo!()
    }

    fn block_size() -> usize {
        todo!()
    }
}
