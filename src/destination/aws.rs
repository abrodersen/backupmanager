
use std::cmp;
use std::sync;
use std::str::FromStr;
use std::mem;
use std::io;
use std::thread;

use rusoto_core as aws;
use rusoto_credential as auth;
use rusoto_s3 as s3;
use rusoto_s3::S3;

use crossbeam::channel;

use failure::{self, Error};

use futures::{Async, Future, Poll, stream, Stream};

pub struct AwsBucket {
    region: String,
    bucket: String,
    prefix: String,
    access_key_id: String,
    secret_access_key: String,
}

impl AwsBucket {
    pub fn new(region: &str, bucket: &str, prefix: &str, key_id: &str, secret: &str) -> AwsBucket {
        AwsBucket { 
            region: region.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
            access_key_id: key_id.into(),
            secret_access_key: secret.into(),
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

struct CredentialWrapper {
    id: String,
    secret: String,
}

struct CredentialFuture {
    id: String,
    secret: String,
}

impl aws::ProvideAwsCredentials for CredentialWrapper {
    type Future = CredentialFuture;

    fn credentials(&self) -> Self::Future {
        CredentialFuture { id: self.id.clone(), secret: self.secret.clone() }
    }
}

impl Future for CredentialFuture {
    type Item = auth::AwsCredentials;
    type Error = auth::CredentialsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let creds = auth::AwsCredentials::new(self.id.as_ref(), self.secret.as_ref(), None, None);
        Ok(Async::Ready(creds))
    }
}

const BLOCK_SIZE: usize = 1 << 26;
const NUM_THREADS: u8 = 1;

impl AwsBucket {
    fn get_client(&self) -> Result<s3::S3Client, Error> {
        let client = aws::request::HttpClient::new()?;
        let creds = CredentialWrapper { id: self.access_key_id.clone(), secret: self.secret_access_key.clone() };
        let region = aws::Region::from_str(&self.region)?;
        Ok(s3::S3Client::new_with(client, creds, region.clone()))
    }
}

impl super::Destination for AwsBucket {

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        let client = self.get_client()?;
        let name = format!("{}{}", self.prefix, name);

        let mut upload_req = s3::CreateMultipartUploadRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name.clone();

        let response = client.create_multipart_upload(upload_req).sync()?;
        let id = response.upload_id.ok_or(failure::err_msg("no upload id returned"))?;

        let (tx, rx) = channel::bounded(0);
        let writer = WriteChunker::new(BLOCK_SIZE, tx);
        let state = sync::Arc::new(sync::Mutex::new(s3::CompletedMultipartUpload::default()));

        let threads = (0..NUM_THREADS).map(|_| {

            let bucket = self.bucket.clone();
            let key = name.to_string();
            let id = id.clone();
            let client = self.get_client()?;
            let state = state.clone();
            let rx = rx.clone();

            Ok(thread::spawn(move || {
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

                            upload_req.part_number = index as i64;
                            upload_req.content_length = Some(chunk.len() as i64);
                            upload_req.body = Some(s3::StreamingBody::new(chunk));
                            trace!("upload request: {:?}", upload_req);

                            let result = client.upload_part(upload_req).sync()?;
                            
                            {
                                let mut state = state.lock().unwrap();
                                let mut parts = mem::replace(&mut state.parts, None).unwrap_or_else(|| Vec::new());
                                parts.push(s3::CompletedPart {
                                    part_number: Some(index as i64),
                                    e_tag: result.e_tag,
                                });
                                state.parts = Some(parts);
                            }

                            trace!("chunk '{}' uploaded successfully", index);
                        }
                    }
                }
                
                Ok(())
            }))
        }).collect::<Result<Vec<_>, Error>>()?;

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

        trace!("finalizing chunker");
        chunker.finish()?;

        trace!("waiting for threads to finish");
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
            trace!("finalizing s3 object with state: {:?}", state);
            complete_req.multipart_upload = Some(state.clone());
        }

        info!("finalzing s3 object {}", key);
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
            idx: 1,
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

