#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct BlockId {
    file_name: String,
    pub(crate) block_number: usize,
}

impl BlockId {
    pub(crate) fn new(file_name: &str, block_number: usize) -> Self {
        BlockId {
            file_name: file_name.to_owned(),
            block_number,
        }
    }

    pub(crate) fn starting_offset(&self, block_size: usize) -> i64 {
        block_size as i64 * self.block_number as i64
    }

    pub(crate) fn file_name(&self) -> &str {
        &self.file_name
    }

    pub(crate) fn previous(&self) -> Option<Self> {
        if self.block_number == 0 {
            None
        } else {
            Some(Self::new(&self.file_name, self.block_number - 1))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::file::block_id::BlockId;

    #[test]
    fn starting_offset_with_block_zero() {
        let block_id = BlockId::new("lsm.log", 0);
        let block_size = 400;

        let offset = block_id.starting_offset(block_size);
        assert_eq!(0, offset);
    }

    #[test]
    fn starting_offset_with_non_zero_block() {
        let block_id = BlockId::new("lsm.log", 3);
        let block_size = 400;

        let offset = block_id.starting_offset(block_size);
        assert_eq!(1200, offset);
    }

    #[test]
    fn previous_block_id() {
        let block_id = BlockId::new("lsm.log", 1);

        let previous_block_id = block_id.previous();
        assert_eq!(BlockId::new("lsm.log", 0), previous_block_id.unwrap());
    }

    #[test]
    fn previous_block_id_of_block_zero() {
        let block_id = BlockId::new("lsm.log", 0);

        let previous_block_id = block_id.previous();
        assert_eq!(None, previous_block_id);
    }
}
