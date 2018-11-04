
use failure::Error;

pub struct AwsBucket {
    region: String,
    bucket: String,
    access_key: String
}

pub struct AwsObject {

}

const BLOCK_SIZE: usize = 1 << 20;

impl Target for AwsObject {
    fn block_size() -> usize {
        BLOCK_SIZE
    }

    fn upload(idx: u64, data: &[u8]) -> Result<(), Error> {
        Ok(())
    }
}

impl Target for 
