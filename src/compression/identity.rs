
use std::io;

use encryption::Cryptor;

use failure::Error;

pub struct IdentityCompressor {
    inner: Box<Cryptor>,
}

impl IdentityCompressor {
    pub fn new(w: Box<Cryptor>) -> Self {
        IdentityCompressor { inner: w }
    }
}

impl io::Write for IdentityCompressor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl super::Compressor for IdentityCompressor {
    fn finalize(self: Box<Self>) -> Result<Box<Cryptor>, Error> {
        Ok(self.inner)
    }
}

