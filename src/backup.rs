
use super::config;
use super::source::{self, Source, Snapshot, lvm};
use super::destination::{self, Destination, Target, aws, null};

use std::any;
use std::fs;
use std::io;
use std::cmp;
use std::mem;
use std::sync;

use tar;

use failure::Error;

use futures;
use futures::stream;

use crossbeam::channel;
use crossbeam::thread;

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
        // config::DestinationType::S3 { region, bucket } => {
        //     Box::new(aws::AwsBucket::new(region.as_ref(), bucket.as_ref()))
        // },
        config::DestinationType::Null => null::NullDestination,
        _ => panic!("destination not implemented"),
    };

    let snapshot = source.snapshot()?;
    let target = destination.allocate(job.name.as_ref())?;
    
    let result = upload_archive(snapshot, &target);

    Ok(())
}

fn upload_archive<S, T>(snapshot: S, target: &T) -> Result<(), Error> 
    where S: Snapshot, T: Target + Sync + ?Sized
{
    let block_size = target.block_size();
    let (tx, rx) = channel::bounded(0);
    let writer = WriteChunker::new(block_size, tx);

    thread::scope(|s| {
        let worker_thread = s.spawn(|_| {
            trace!("new thread spawned");
            loop {
                trace!("listening for message");
                match rx.recv() {
                    Err(_) => {
                        debug!("channel closed, exiting thread...");
                        return Result::Ok::<(), Error>(());
                    },
                    Ok(chunk) => {
                        let index = chunk.idx;
                        info!("received chunk '{}'", index);
                        target.upload(index, chunk)?;
                    }
                }
            }
        });

        let mut builder = tar::Builder::new(writer);
        builder.follow_symlinks(false);

        let files = snapshot.files()?;
        let base_path = files.base_path();

        for entry in files {
            let (rel_path, _) = entry?;
            let full_path = base_path.join(&rel_path);
            let mut file = fs::File::open(full_path)?;
            debug!("appending file '{}' to archive stream", rel_path.display());
            builder.append_file(rel_path, &mut file)?;
        }

        worker_thread.join()
            .map_err(|inner| {
                let r: &Error = inner.downcast_ref().expect("invalid error returned from thread");
                format_err!("error returned from worker thread: {}", r)
            })
            .and_then(|x| x)?;

        Result::Ok::<(), Error>(())
    })
    .map_err(|inner| {
        let r: &Error = inner.downcast_ref().expect("invalid error returned from thread");
        format_err!("error returned from thread scope: {}", r)
    })
    .and_then(|x| x)?;

    Ok(())
}

struct WriteChunker {
    limit: usize,
    buffer: Vec<u8>,
    sender: channel::Sender<Chunk>,
    idx: u64, 
}

impl WriteChunker {
    fn new(limit: usize, sender: channel::Sender<Chunk>) -> WriteChunker {
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
