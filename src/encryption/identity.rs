
use std::io;

use destination::Target;

pub struct IdentityCryptor {
    inner: Box<Target>,
}

impl IdentityCryptor {
    pub fn new(w: Box<Target>) -> IdentityCryptor {
        IdentityCryptor { inner: w }
    }
}

impl io::Write for IdentityCryptor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl super::Cryptor for IdentityCryptor {
    fn finalize(self: Box<Self>) -> Box<Target> {
        self.inner
    }
}

