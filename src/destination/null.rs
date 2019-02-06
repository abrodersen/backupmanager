
use std::fs;
use std::io::{self, Write};

use failure::Error;

use futures::stream;

pub struct NullDestination;

impl super::Destination for NullDestination {
    type Alloc = NullTarget;

    fn allocate(&self, name: &str) -> Result<Self::Alloc, Error> {
        Ok(NullTarget)
    }

}

pub struct NullTarget;

impl super::Target for NullTarget {
    fn block_size(&self) -> usize {
        1 << 10
    }

    fn upload<S>(&self, idx: u64, s: S) -> Result<(), Error>
        where S: stream::Stream<Item=Vec<u8>, Error=io::Error> + Send + 'static
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")?;

        for chunk in s.wait() {
            file.write(&(chunk?))?;
        }

        Ok(())
    }

    fn finalize(self) -> Result<(), Error> {
        Ok(())
    }
}