
use std::sync;
use std::str::FromStr;
use std::mem;
use std::io;

use rusoto_core as aws;
use rusoto_s3 as s3;
use rusoto_s3::S3;

use failure::{self, Error};

use futures::{stream, Stream};

pub struct AwsBucket {
    region: String,
    bucket: String,
}

impl AwsBucket {
    pub fn new(region: &str, bucket: &str) -> AwsBucket {
        AwsBucket { 
            region: region.into(),
            bucket: bucket.into(),
        }
    }
}

pub struct AwsUpload {
    bucket: String,
    key: String,
    id: String,
    client: s3::S3Client,
    state: sync::Mutex<s3::CompletedMultipartUpload>,
}

const BLOCK_SIZE: usize = 1 << 26;

impl super::Destination for AwsBucket {

    fn allocate(&self, name: &str) -> Result<Box<super::Target>, Error> {
        let region = aws::Region::from_str(name)?;
        let client = s3::S3Client::new(region);

        let mut upload_req = s3::CreateMultipartUploadRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name.into();

        let response = client.create_multipart_upload(upload_req).sync()?;
        let id = response.upload_id.ok_or(failure::err_msg("no upload id returned"))?;

        Ok(Box::new(AwsUpload { 
            bucket: self.bucket.clone(),
            key: name.into(), 
            id: id.into(), 
            client: client,
            state: sync::Mutex::new(s3::CompletedMultipartUpload::default()),
        }))
    }
}

impl super::Target for AwsUpload {
    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn upload(&self, idx: u64, chunk: crate::io::Chunk) -> Result<(), Error> {
        let mut upload_req = s3::UploadPartRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = self.key.clone();
        upload_req.upload_id = self.id.clone();

        upload_req.body = Some(s3::StreamingBody::new(chunk));
        upload_req.part_number = idx as i64;

        let result = self.client.upload_part(upload_req).sync()?;
        
        {
            let mut state = self.state.lock().unwrap();
            let mut parts = mem::replace(&mut state.parts, None).unwrap_or_else(|| Vec::new());
            parts.push(s3::CompletedPart {
                part_number: Some(idx as i64),
                e_tag: result.e_tag,
            });
            state.parts = Some(parts);
        }

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<(), Error> {
        let mut complete_req = s3::CompleteMultipartUploadRequest::default();
        complete_req.bucket = self.bucket.clone();
        complete_req.key = self.key.clone();
        complete_req.upload_id = self.id.clone();
        {
            let mut state = self.state.lock().unwrap();
            complete_req.multipart_upload = Some(state.clone());
        }

        self.client.complete_multipart_upload(complete_req).sync()?;

        Ok(())
    }
}

