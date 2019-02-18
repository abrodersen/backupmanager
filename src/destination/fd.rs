
use std::collections;
use std::fs;
use std::io::{self, Write};
use std::sync;

use failure::Error;

use futures::{stream, Stream};

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

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        let fd = self.file.try_clone()?;
        Ok(Box::new(FileDescriptorTarget { file: fd }))
    }
}

#[derive(Debug)]
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