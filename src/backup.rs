
use super::config;
use super::source::{self, Source, Snapshot, lvm};
use super::destination::{self, Destination, Target, aws};

use std::fs;
use std::io;
use std::cmp;
use std::mem;
use std::sync::mpsc::{sync_channel, SyncSender, Receiver};
use std::thread;

use tar;

use failure::Error;

use futures;
use futures::stream;

pub struct Job {
   pub name: String,
   pub source: config::Source,
   pub destination: config::Destination,
   pub compression: Option<config::Compression>,
   pub encryption: Option<config::Encryption>,
}

pub fn full_backup(job: &Job) -> Result<(), Error> {
    let source = match &job.source.typ {
        config::SourceType::LVM { volume_group, logical_volume } => {
            lvm::LogicalVolume::new(volume_group.as_ref(), logical_volume.as_ref())
        }
    };

    let destination = match &job.destination.typ {
        config::DestinationType::S3 { region, bucket } => {
            aws::AwsBucket::new(region.as_ref(), bucket.as_ref())
        }
    };

    let snapshot = source.snapshot()?;
    let files = snapshot.files()?;
    let base_path = files.base_path();

    let allocation = destination.allocate(job.name.as_ref())?;
    let block_size = allocation.block_size();
    let (tx, rx) = sync_channel(0);
    let writer = WriteChunker::new(block_size, tx);

    thread::spawn(move || {
        let chunk = rx.recv().unwrap();
        allocation.upload(chunk.idx, chunk).unwrap();
    });

    let mut builder = tar::Builder::new(writer);
    builder.follow_symlinks(false);

    for entry in files {
        let (rel_path, _) = entry?;
        let full_path = base_path.join(&rel_path);
        let mut file = fs::File::open(full_path)?;
        builder.append_file(rel_path, &mut file)?;
    }

    Ok(())
}

struct WriteChunker {
    limit: usize,
    buffer: Vec<u8>,
    sender: SyncSender<Chunk>,
    idx: u64, 
}

impl WriteChunker {
    fn new(limit: usize, sender: SyncSender<Chunk>) -> WriteChunker {
        WriteChunker {
            limit: limit,
            buffer: Vec::with_capacity(limit),
            sender: sender,
            idx: 0,
        }
    }
}

impl io::Write for WriteChunker {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let to_write = cmp::min(buf.len(), self.limit - self.buffer.len());
        if to_write <= 0 {
            self.flush()?;
        }
        self.buffer.write(&buf[..to_write])
    }

    fn flush(&mut self) -> io::Result<()> {
        let chunk = Chunk::new(self.idx, mem::replace(&mut self.buffer, Vec::with_capacity(self.limit)));
        self.idx += 1;
        self.sender.send(chunk).unwrap(); //TODO: propagate this error
        Ok(())
    }
}

struct Chunk {
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
