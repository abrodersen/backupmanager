
use std::io;

use encryption::Cryptor;

use failure::Error;

use flate2::Compression;
use flate2::write::GzEncoder;

pub struct GzipCompressor {
    encoder: GzEncoder<Box<Cryptor>>,
}

impl GzipCompressor {
    pub fn new(w: Box<Cryptor>) -> Self {
        GzipCompressor { 
            encoder: GzEncoder::new(w, Compression::best())
        }
    }
}

impl io::Write for GzipCompressor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.encoder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl super::Compressor for GzipCompressor {
    fn finalize(self: Box<Self>) -> Result<Box<Cryptor>, Error> {
        let inner = self.encoder.finish()?;
        Ok(inner)
    }
}