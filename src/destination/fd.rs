
use std::fs;
use std::io;

use failure::Error;

pub struct FileDescriptorDestination {
    file: fs::File
}

impl FileDescriptorDestination {
    pub fn new(f: fs::File) -> FileDescriptorDestination {
        FileDescriptorDestination {
            file: f
        }
    }
}

impl super::Destination for FileDescriptorDestination {

    fn list_backups(&self, request: &super::BackupSearchRequest) -> Result<Vec<super::TargetDescriptor>, Error> {
        unimplemented!();
    }

    fn fetch_manifest(&self, desc: &super::TargetDescriptor) -> Result<Vec<u8>, Error> {
        unimplemented!();
    }

    fn upload_manifest(&self, desc: &super::TargetDescriptor, data: &[u8]) -> Result<(), Error> {
        unimplemented!();
    }

    fn allocate(&self, _: &super::TargetDescriptor, _: u64) -> Result<Box<super::Target>, Error> {
        let fd = self.file.try_clone()?;
        Ok(Box::new(FileDescriptorTarget { file: fd }))
    }
}

pub struct FileDescriptorTarget {
    file: fs::File,
}

impl io::Write for FileDescriptorTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl super::Target for FileDescriptorTarget {
    fn finalize(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }
}