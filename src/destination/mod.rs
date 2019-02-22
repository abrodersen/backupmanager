pub(crate) mod aws;
pub(crate) mod fd;
pub(crate) mod null;

use std::io;

use failure::Error;

pub trait Destination {
    fn allocate(&self, name: &str, size_hint: u64) -> Result<Box<Target>, Error>;
}

pub trait Target: io::Write + Sync {
    fn finalize(self: Box<Self>) -> Result<(), Error>;
}




