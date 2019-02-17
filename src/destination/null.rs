
use std::fs;
use std::io::{self, Write};
use std::sync::atomic;

use failure::Error;

use futures::{stream, Stream};

pub struct NullDestination;

impl super::Destination for NullDestination {

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        let file = fs::OpenOptions::new()
            .write(true)
            .open("/dev/null")?;

        Ok(Box::new(NullTarget {
            file: file,
            written: atomic::AtomicUsize::new(0)
        }))
    }

}

pub struct NullTarget {
    file: fs::File,
    written: atomic::AtomicUsize,
}

impl io::Write for NullTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.file.write(buf) {
            Ok(w) => {
                self.written.fetch_add(w, atomic::Ordering::Relaxed);
                Ok(w)
            },
            e => e,
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl super::Target for NullTarget {

    fn finalize(self: Box<Self>) -> Result<(), Error> {
        trace!("wrote {} bytes", self.written.load(atomic::Ordering::Relaxed));
        Ok(())
    }
}