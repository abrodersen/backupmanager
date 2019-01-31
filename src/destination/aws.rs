
use failure::Error;

pub struct AwsBucket {
    region: String,
    bucket: String,
    profile: String
}

pub struct AwsUpload {
    bucket: String,
    key: String,
    id: String,
    client: S3Client,
    state: Mutex<CompletedMultipartUpload>,
}

const BLOCK_SIZE: usize = 1 << 26;

impl Destination for AwsBucket {
    fn allocate(&self, name: &str) -> Result<Box<Target>, Error> {
        let region = Region::from_str(name)?;
        let client = S3Client::new(region);

        let upload_req = CreateMultipartUploadRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = name.into();

        let response = client.create_multipart_upload(upload_req).sync()?;
        AwsUpload { 
            bucket: self.bucket.clone(),
            key: name.into(), 
            id: response.id, 
            client: client,
            state: Mutex::new(CompletedMultipartUpload::default()),
        }
    }
}

impl Target for AwsObject {
    fn block_size(&self) -> usize {
        BLOCK_SIZE
    }

    fn upload(&self, idx: u64, data: &[u8]) -> Result<(), Error> {
        let upload_req = UploadPartRequest::default();
        upload_req.bucket = self.bucket.clone();
        upload_req.key = self.key.clone();
        upload_req.id = self.id.clone();

        let stream = stream::iter_ok(data).chunks(4096);
        upload_req.body = Some(StreamingBody::new(stream));
        upload_req.content_length = Some(data.len() as i64);
        upload_req.part_number = idx as i64;

        let result = self.client.upload_part(upload_req).sync()?;
        
        {
            let mut state = self.state.lock().unwrap();
            let mut parts = state.parts.unwrap_or_else(|| Vec::new());
            parts.push(CompletedPart {
                part_number: Some(idx as i64),
                e_tag: result.e_tag,
            });
            state.parts = parts;
        }

        Ok(())
    }

    fn finalize(self) -> Result<(), (Self, Error)> {
        let complete_req = CompleteMultipartUploadRequest::default();
        complete_req.bucket = self.bucket.clone();
        complete_req.key = self.key.clone();
        complete_req.id = self.id.clone();
        {
            let mut state = self.state.lock.unwrap();
            complete_req.multipart_upload = Some(state);
        }

        self.client.complete_multipart_upload(complete_req)
            .sync()
            .map_err(|e| (self, e));

        Ok(())
    }
}

