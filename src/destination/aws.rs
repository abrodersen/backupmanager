
use std::cmp;
use std::sync;
use std::str::FromStr;
use std::mem;
use std::io::{self, Read};
use std::thread;
use std::time;

use super::*;

use rusoto_core as aws;
use rusoto_credential as auth;
use rusoto_s3 as s3;
use rusoto_s3::S3;

use crossbeam::channel;

use failure::{self, Error, ResultExt};

use futures::{Async, Future, Poll, stream};

use exponential_backoff::Backoff;

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
    errors: channel::Receiver<Error>,
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

const NUM_THREADS: u8 = 4;
const MAX_RETRIES: u32 = 32;



impl AwsBucket {
    fn get_client(&self) -> Result<s3::S3Client, Error> {
        let client = aws::request::HttpClient::new()?;
        let creds = CredentialWrapper { id: self.access_key_id.clone(), secret: self.secret_access_key.clone() };
        let region = aws::Region::from_str(&self.region)?;
        Ok(s3::S3Client::new_with(client, creds, region.clone()))
    }
    
    fn get_object_dir(&self, host: &str, job: &str) -> String {
        format!("{}{}/{}/", self.prefix, host, job)
    }

    fn get_object_name(&self, desc: &TargetDescriptor) -> String {
        let prefix = self.get_object_dir(&desc.host, &desc.job);
        let ext = match desc.typ {
            TargetType::Full => "full",
            TargetType::Differential => "diff",
        };
        let time = desc.timestamp.to_rfc3339_opts( SecondsFormat::Secs, true);
        format!("{}{}.{}", prefix, time, ext)
    }

    fn parse_object(&self, host: &str, job: &str, obj: &s3::Object) -> Option<TargetDescriptor> {
        let key = match obj.key {
            Some(ref x) => x.to_string(),
            _ => return None,
        };

        trace!("evaluating object '{}'", key);

        let prefix = self.get_object_dir(host, job);
        let name = key.trim_start_matches(&prefix);
        let parts: Vec<&str> = name.splitn(2, ".").collect();

        if parts.len() < 2 {
            trace!("expected 2 parts, found {}", parts.len());
            return None;
        }

        let timestamp = match DateTime::parse_from_rfc3339(parts[0]) {
            Ok(t) => DateTime::from_utc(t.naive_utc(), Utc),
            Err(_) => {
                trace!("datetime '{}' could not be parsed", parts[0]);
                return None;
            },
        };

        let typ = match parts[1] {
            "full.manifest" => TargetType::Full,
            "diff.manifest" => TargetType::Differential,
            part => {
                trace!("backup type '{}' could not be parsed", part);
                return None;
            },
        };

        Some(TargetDescriptor {
            host: host.into(),
            job: job.into(),
            timestamp: timestamp.into(),
            typ: typ,
        })
    }
}

fn get_next_pow2(s: u64) -> u64 {
    let log = (s as f64).log2().ceil() as u64;
    1 << log
}

enum ObjectType {
    Manifest,
    Data,
}

fn get_object_tags(desc: &TargetDescriptor, kind: ObjectType) -> String {
    let backup_type = match desc.kind() {
        TargetType::Full => "full",
        TargetType::Differential => "diff",
    };

    let object_type = match kind {
        ObjectType::Manifest => "manifest",
        ObjectType::Data => "data",
    };

    format!("backup={}&type={}", backup_type, object_type)
}

impl Destination for AwsBucket {

    fn list_backups(&self, search: &BackupSearchRequest) -> Result<Vec<TargetDescriptor>, Error> {
        let client = self.get_client()?;

        let mut backups = Vec::new();
        let mut token = None;
        let dir = self.get_object_dir(&search.host, &search.job);

        debug!("enumerating aws objects in '{}'", &dir);
        loop {

            let mut request = s3::ListObjectsV2Request::default();
            request.bucket = self.bucket.clone();
            request.continuation_token = token.clone();
            request.prefix = Some(dir.clone());

            trace!("aws list objects v2 request: {:?}", request);

            let result = client.list_objects_v2(request).sync()?;

            trace!("aws list objects v2 response: {:?}", result);

            if let Some(objs) = result.contents {
                trace!("found {} objects", objs.len());
                backups.extend(objs.iter().filter_map(|obj| self.parse_object(&search.host, &search.job, obj)));
            } else {
                trace!("response contained no objects");
            }

            if !result.is_truncated.unwrap_or(false) {
                break;
            }

            token = result.next_continuation_token;
        }

        debug!("found {} valid backup manifests", backups.len());

        Ok(backups)
    }

    fn fetch_manifest(&self, desc: &TargetDescriptor) -> Result<Vec<u8>, Error> {
        let client = self.get_client()?;
        let name = format!("{}.manifest", self.get_object_name(&desc));

        let mut get_req = s3::GetObjectRequest::default();
        get_req.bucket = self.bucket.clone();
        get_req.key = name;

        let resp = client.get_object(get_req).sync()?;
        let body = resp.body.ok_or_else(|| format_err!("no body on response"))?;
        let mut buffer = Vec::new();
        body.into_blocking_read().read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    fn upload_manifest(&self, desc: &TargetDescriptor, data: &[u8]) -> Result<(), Error> {
        let client = self.get_client()?;
        let name = format!("{}.manifest", self.get_object_name(&desc));
        let body: Vec<_> = data.iter().collect();

        let mut upload_req = s3::PutObjectRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name;
        upload_req.storage_class = Some("STANDARD".into());
        upload_req.tagging = Some(get_object_tags(desc, ObjectType::Manifest));
        upload_req.content_length = Some(body.len() as i64);
        upload_req.body = Some(s3::StreamingBody::new(stream::once(Ok(data.to_vec()))));

        let _ = client.put_object(upload_req).sync()?;

        Ok(())
    }

    fn allocate(&self, desc: &TargetDescriptor, size_hint: u64) -> Result<Box<super::Target>, Error> {
        let client = self.get_client()?;
        let name = self.get_object_name(desc);

        let mut upload_req = s3::CreateMultipartUploadRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name.clone();
        upload_req.tagging = Some(get_object_tags(desc, ObjectType::Data));
        upload_req.storage_class = Some("DEEP_ARCHIVE".into());

        let response = client.create_multipart_upload(upload_req).sync()?;
        let id = response.upload_id.ok_or(failure::err_msg("no upload id returned"))?;

        let hint_size = get_next_pow2(f64::ceil((size_hint as f64) / 10000.0) as u64);
        let block_size = cmp::max(1 << 26, hint_size as usize);
        info!("using parts of size {}", block_size);

        let (tx, rx) = channel::bounded(0);
        let (tx_err, rx_err) = channel::bounded(0);
        let writer = WriteChunker::new(block_size, tx);
        let state = sync::Arc::new(sync::Mutex::new(s3::CompletedMultipartUpload::default()));

        info!("allocating {} upload threads", NUM_THREADS);

        let threads = (0..NUM_THREADS).map(|_| {

            let bucket = self.bucket.clone();
            let key = name.to_string();
            let id = id.clone();
            let client = self.get_client()?;
            let state = state.clone();
            let rx = rx.clone();
            let tx_err = tx_err.clone();

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
                            let size = chunk.len();
                            trace!("received chunk {} with {} bytes", index, size);

                            let backoff = Backoff::new(MAX_RETRIES)
                                .timeout_range(time::Duration::from_millis(100), time::Duration::from_secs(10))
                                .jitter(0.3)
                                .factor(2);

                            let mut backoff_iter = backoff.iter();

                            let result = loop {
                                let mut upload_req = s3::UploadPartRequest::default();
                                upload_req.bucket = bucket.clone();
                                upload_req.key = key.clone();
                                upload_req.upload_id = id.clone();

                                upload_req.part_number = index as i64;
                                upload_req.content_length = Some(size as i64);
                                upload_req.body = Some(s3::StreamingBody::new(chunk.clone()));
                                trace!("upload request: {:?}", upload_req);

                                match client.upload_part(upload_req).sync() {
                                    Ok(r) => break r,
                                    Err(e) => {
                                        error!("upload request failed: {}", e);
                                        match backoff_iter.next().and_then(|x| x) {
                                            Some(wait) => thread::sleep(wait),
                                            None => {
                                                tx_err.send(e.into()).expect("failed to send thread error");
                                                return Err(format_err!("upload retry limit exceeded")); 
                                            }
                                        }
                                    }
                                }
                            };
                            
                            {
                                let mut state = state.lock().expect("mutex has been poisoned");
                                let mut parts = mem::replace(&mut state.parts, None).unwrap_or_else(|| Vec::new());
                                parts.push(s3::CompletedPart {
                                    part_number: Some(index as i64),
                                    e_tag: result.e_tag,
                                });
                                parts.sort_unstable_by(|x, y| x.part_number.unwrap().cmp(&y.part_number.unwrap()));
                                state.parts = Some(parts);
                            }

                            debug!("chunk {} uploaded successfully", index);
                        }
                    }
                }
            }))
        }).collect::<Result<Vec<_>, Error>>()?;

        Ok(Box::new(AwsUpload { 
            bucket: self.bucket.clone(),
            key: name.into(), 
            id: id.into(), 
            client: client,
            chunker: writer,
            threads: threads,
            errors: rx_err,
            state: state,
        }))
    }
}

impl io::Write for AwsUpload {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.errors.try_recv() {
            Ok(e) => {
                error!("received error message from upload thread: {}", e);
                return Err(io::Error::new(io::ErrorKind::Other, e));
            },
            _ => (),
        };

        self.chunker.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Target for AwsUpload {

    fn finalize(self: Box<Self>) -> Result<(), Error> {

        let AwsUpload { chunker, threads, bucket, key, id, client, state, .. } = { *self };

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
            let state = state.lock().expect("mutex has been poisoned");
            let state = state.clone();
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
        self.sender.send(chunk)
            .context("faild to send chunk")?;
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
            self.send_chunk()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
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

#[derive(Clone)]
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

