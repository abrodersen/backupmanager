
use std::fs;
use std::io::{self, Write};
use std::sync::atomic;

use failure::Error;

use futures::{stream, Stream};

pub struct NullDestination;

impl super::Destination for NullDestination {

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        Ok(Box::new(NullTarget { written: atomic::AtomicUsize::new(0) }))
    }

}

pub struct NullTarget {
    written: atomic::AtomicUsize,
}

impl super::Target for NullTarget {
    fn block_size(&self) -> usize {
        1 << 10
    }

    fn upload(&self, idx: u64, chunk: crate::io::Chunk) -> Result<(), Error> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")?;

        for result in chunk.wait() {
            let batch = result?;
            file.write_all(&batch)?;
            self.written.fetch_add(batch.len(), atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<(), Error> {
        trace!("wrote {} bytes", self.written.load(atomic::Ordering::Relaxed));
        Ok(())
    }
}