pub(crate) mod identity;
pub(crate) mod gzip;

use std::io;

use failure::Error;

use encryption::Cryptor;

pub trait Compressor: io::Write {
    fn finalize(self: Box<Self>) -> Result<Box<Cryptor>, Error>;
}
