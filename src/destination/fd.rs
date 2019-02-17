
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
        let state = WriteState::new(fd);
        Ok(Box::new(FileDescriptorTarget { state: sync::Mutex::new(state) }))
    }

}

struct WriteState {
    file: fs::File,
    pending_chunks: collections::BTreeMap<u64, crate::io::Chunk>,
    current_chunk: u64,
}

impl WriteState {
    fn new(file: fs::File) -> WriteState {
        WriteState {
            file: file,
            pending_chunks: collections::BTreeMap::new(),
            current_chunk: 0,
        }
    }

    fn remove_current(&mut self) -> Option<crate::io::Chunk> {
        let current = self.current_chunk;
        self.pending_chunks.remove(&current)
    }
}

pub struct FileDescriptorTarget {
    state: sync::Mutex<WriteState>,
}

impl super::Target for FileDescriptorTarget {
    fn block_size(&self) -> usize {
        1 << 10
    }

    fn upload(&self, idx: u64, chunk: crate::io::Chunk) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap();
        state.pending_chunks.insert(idx, chunk);

        if idx == state.current_chunk {
            while let Some(next_chunk) = state.remove_current() {
                for result in next_chunk.wait() {
                    let batch = result?;
                    state.file.write_all(&batch)?;
                }
                state.current_chunk += 1;
            }
        }

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<(), Error> {
        Ok(())
    }
}