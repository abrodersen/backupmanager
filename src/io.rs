
use std::io;
use std::mem;
use std::cmp;

use failure::Error;

use crossbeam::channel;

use futures::stream;


pub struct WriteChunker {
    limit: usize,
    wrote: usize,
    buffer: Vec<u8>,
    sender: channel::Sender<Chunk>,
    idx: u64, 
}

impl WriteChunker {
    pub fn new(limit: usize, sender: channel::Sender<Chunk>) -> WriteChunker {
        WriteChunker {
            limit: limit,
            wrote: 0,
            buffer: Vec::with_capacity(limit),
            sender: sender,
            idx: 0,
        }
    }

    fn send_chunk(&mut self) -> Result<(), Error> {
        let chunk = Chunk::new(self.idx, mem::replace(&mut self.buffer, Vec::with_capacity(self.limit)));
        self.idx += 1;
        debug!("flushing chunk {} to upload queue", chunk.idx);
        self.sender.send(chunk).unwrap(); //TODO: propagate this error
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), Error> {
        trace!("flushing last chunk");
        self.send_chunk()?;
        trace!("closing channel");
        drop(self.sender);
        trace!("wrote {} bytes", self.wrote);
        Ok(())
    }
}

impl io::Write for WriteChunker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        trace!("write called");

        if self.buffer.len() == self.limit {
            self.send_chunk().unwrap();
        }

        let to_write = cmp::min(buf.len(), self.limit - self.buffer.len());
        trace!("writing {} bytes to current chunk", to_write);
        let written = self.buffer.write(&buf[..to_write])?;
        self.wrote += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct Chunk {
    idx: u64,
    read: bool,
    buffer: Vec<u8>,
}

impl Chunk {
    fn new(index: u64, data: Vec<u8>) -> Chunk {
        Chunk {
            idx: index,
            read: false,
            buffer: data,
        }
    }

    pub fn index(&self) -> u64 {
        self.idx
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl stream::Stream for Chunk {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Option<Self::Item>>, Self::Error> {
        if !self.read {
            let data = mem::replace(&mut self.buffer, Vec::with_capacity(0));
            self.read = true;
            Ok(futures::Async::Ready(Some(data)))
        } else {
            Ok(futures::Async::Ready(None))
        }
    }
}