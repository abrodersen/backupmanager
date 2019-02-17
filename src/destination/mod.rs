pub(crate) mod aws;
pub(crate) mod fd;
pub(crate) mod null;

use super::io::Chunk;

use std::io;

use failure::Error;

use futures::stream;

pub trait Destination {
    fn allocate(&self, name: &str) -> Result<Box<Target>, Error>;
}

pub trait Target: Sync {
    fn block_size(&self) -> usize;
    fn upload(&self, idx: u64, chunk: Chunk) -> Result<(), Error>;
    fn finalize(self: Box<Self>) -> Result<(), Error>;
}




