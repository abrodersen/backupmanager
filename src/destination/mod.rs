pub(crate) mod aws;

use std::io;

use failure::Error;

pub trait Destination {
    fn allocate(&self, name: &str) -> Result<Box<Target>, Error>;
}

pub trait Target {
    fn block_size(&self) -> usize;
    fn upload(&self, idx: u64, data: Vec<u8>) -> Result<(), Error>;
    fn finalize(self) -> Result<(), Error>;
}




