
use std::fs;
use std::io::{self, Write};
use std::sync::atomic;

use failure::Error;

use futures::stream;

pub struct NullDestination;

impl super::Destination for NullDestination {
    type Alloc = NullTarget;

    fn allocate(&self, name: &str) -> Result<Self::Alloc, Error> {
        Ok(NullTarget { written: atomic::AtomicUsize::new(0) })
    }

}

pub struct NullTarget {
    written: atomic::AtomicUsize,
}

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

        for result in s.wait() {
            let batch = result?;
            file.write_all(&batch)?;
            self.written.fetch_add(batch.len(), atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    fn finalize(self) -> Result<(), Error> {
        trace!("wrote {} bytes", self.written.load(atomic::Ordering::Relaxed));
        Ok(())
    }
}