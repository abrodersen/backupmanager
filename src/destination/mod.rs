
use std::io;

use failure::Error;

pub trait Destination {
    fn allocate(&self, name: &str) -> Result<Box<Target>, Error>;
}

pub trait Target {
    fn block_size() -> usize;
    fn upload(idx: u64, data: &[u8]) -> Result<(), Error>;
    fn delete(&self) -> Result<(), Error>;
}




