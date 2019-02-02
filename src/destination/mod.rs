pub(crate) mod aws;

use std::io;

use failure::Error;

use futures::stream;

pub trait Destination {
    type Alloc: Target;

    fn allocate(&self, name: &str) -> Result<Self::Alloc, Error>;
}

pub trait Target {
    fn block_size(&self) -> usize;
    fn upload<S>(&self, idx: u64, s: S) -> Result<(), Error>
        where S: stream::Stream<Item=Vec<u8>, Error=io::Error> + Send + 'static;
    fn finalize(self) -> Result<(), Error>;
}




