pub(crate) mod aws;
pub(crate) mod fd;
pub(crate) mod null;

use std::io;

use failure::Error;

use chrono::prelude::*;

pub enum TargetType {
    Full,
    Differential,
}

pub struct TargetDescriptor {
    host: String,
    job: String,
    timestamp: DateTime<Utc>,
    typ: TargetType,
}

impl TargetDescriptor {
    pub fn new<S, T, U>(host: S, job: T, timestamp: U, typ: TargetType) -> TargetDescriptor
        where S: Into<String>, T: Into<String>, U: Into<DateTime<Utc>>
    {
        TargetDescriptor {
            host: host.into(),
            job: job.into(),
            timestamp: timestamp.into(),
            typ: typ,
        }
    }
}

pub struct BackupSearchRequest {
    host: String,
    job: String,
}

pub trait Destination {
    fn list_backups(&self, request: &BackupSearchRequest) -> Result<Vec<TargetDescriptor>, Error>;
    fn fetch_manifest(&self, desc: &TargetDescriptor) -> Result<Vec<u8>, Error>;
    fn upload_manifest(&self, desc: &TargetDescriptor, data: &[u8]) -> Result<(), Error>;
    fn allocate(&self, desc: &TargetDescriptor, size_hint: u64) -> Result<Box<Target>, Error>;
}

pub trait Target: io::Write + Sync {
    fn finalize(self: Box<Self>) -> Result<(), Error>;
}




