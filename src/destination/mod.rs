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

pub trait Target: io::Write + Sync {
    fn finalize(self: Box<Self>) -> Result<(), Error>;
}




