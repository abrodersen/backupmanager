pub(crate) mod identity;
pub(crate) mod gzip;

use std::io;

use anyhow::Error;

use crate::encryption::Cryptor;

pub trait Compressor: io::Write {
    fn finalize(self: Box<Self>) -> Result<Box<dyn Cryptor>, Error>;
}
