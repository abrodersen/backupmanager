
use std::cmp;
use std::sync;
use std::str::FromStr;
use std::mem;
use std::io;
use std::thread;

use rusoto_core as aws;
use rusoto_s3 as s3;
use rusoto_s3::S3;

use crossbeam::channel;

use failure::{self, Error};

use futures::{stream, Stream};

pub struct AwsBucket {
    region: String,
    bucket: String,
    prefix: String,
}

impl AwsBucket {
    pub fn new(region: &str, bucket: &str, prefix: &str) -> AwsBucket {
        AwsBucket { 
            region: region.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }
}

pub struct AwsUpload {
    bucket: String,
    key: String,
    id: String,
    client: s3::S3Client,
    chunker: WriteChunker,
    threads: Vec<thread::JoinHandle<Result<(), Error>>>,
    state: sync::Arc<sync::Mutex<s3::CompletedMultipartUpload>>,
}

const BLOCK_SIZE: usize = 1 << 26;
const NUM_THREADS: u8 = 1;

impl super::Destination for AwsBucket {

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        let region = aws::Region::from_str(name)?;
        let client = s3::S3Client::new(region.clone());
        let name = format!("{}{}", self.prefix, name);

        let mut upload_req = s3::CreateMultipartUploadRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name.clone();

        let response = client.create_multipart_upload(upload_req).sync()?;
        let id = response.upload_id.ok_or(failure::err_msg("no upload id returned"))?;

        let (tx, rx) = channel::bounded(0);
        let writer = WriteChunker::new(BLOCK_SIZE, tx);
        let state = sync::Arc::new(sync::Mutex::new(s3::CompletedMultipartUpload::default()));

        let threads = (0..NUM_THREADS).map(|idx| {

            let bucket = self.bucket.clone();
            let key = name.to_string();
            let id = id.clone();
            let client = s3::S3Client::new(region.clone());
            let state = state.clone();
            let rx = rx.clone();

            thread::spawn(move || {
                trace!("new thread spawned");
                loop {
                    trace!("listening for message");
                    match rx.recv() {
                        Err(_) => {
                            debug!("channel closed, exiting thread...");
                            return Result::Ok::<(), Error>(());
                        },
                        Ok(chunk) => {
                            let index = chunk.index();
                            trace!("received chunk '{}' with {} bytes", index, chunk.len());
                            let mut upload_req = s3::UploadPartRequest::default();
                            upload_req.bucket = bucket.clone();
                            upload_req.key = key.clone();
                            upload_req.upload_id = id.clone();

                            upload_req.body = Some(s3::StreamingBody::new(chunk));
                            upload_req.part_number = idx as i64;

                            let result = client.upload_part(upload_req).sync()?;
                            
                            {
                                let mut state = state.lock().unwrap();
                                let mut parts = mem::replace(&mut state.parts, None).unwrap_or_else(|| Vec::new());
                                parts.push(s3::CompletedPart {
                                    part_number: Some(idx as i64),
                                    e_tag: result.e_tag,
                                });
                                state.parts = Some(parts);
                            }

                            trace!("chunk '{}' uploaded successfully", index);
                        }
                    }
                }
                
                Ok(())
            })
        }).collect();

        Ok(Box::new(AwsUpload { 
            bucket: self.bucket.clone(),
            key: name.into(), 
            id: id.into(), 
            client: client,
            chunker: writer,
            threads: threads,
            state: state,
        }))
    }
}

impl io::Write for AwsUpload {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.chunker.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl super::Target for AwsUpload {

    fn finalize(self: Box<Self>) -> Result<(), Error> {

        let AwsUpload { chunker, threads, bucket, key, id, client, state } = { *self };

        chunker.finish()?;

        for thread in threads {
            thread.join()
                .map_err(|_| format_err!("thread failed"))??;
        }

        let mut complete_req = s3::CompleteMultipartUploadRequest::default();
        complete_req.bucket = bucket.clone();
        complete_req.key = key.clone();
        complete_req.upload_id = id.clone();
        {
            let mut state = state.lock().unwrap();
            complete_req.multipart_upload = Some(state.clone());
        }

        client.complete_multipart_upload(complete_req).sync()?;

        Ok(())
    }
}

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

