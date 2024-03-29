
use std::fs;
use std::io;
use std::sync::atomic;

use anyhow::Error;

pub struct NullDestination;

impl super::Destination for NullDestination {
    fn list_backups(&self, _: &super::BackupSearchRequest) -> Result<Vec<super::TargetDescriptor>, Error> {
        unimplemented!();
    }

    fn fetch_manifest(&self, _: &super::TargetDescriptor) -> Result<Vec<u8>, Error> {
        unimplemented!();
    }

    fn upload_manifest(&self, _: &super::TargetDescriptor, _: &[u8]) -> Result<(), Error> {
        unimplemented!();
    }

    fn allocate(&self, _: &super::TargetDescriptor, _: u64) -> Result<Box<super::Target>, Error> {
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