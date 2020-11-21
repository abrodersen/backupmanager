
use std::io;
use std::sync;

use openpgp::parse::Parse;
use openpgp::constants;
use openpgp::serialize::stream;
use openpgp::serialize::writer;

use crate::destination::Target;

use failure::{Error, ResultExt};

pub struct PgpCryptor {
    target: sync::Arc<sync::Mutex<Box<Target>>>,
    writer: writer::Stack<'static, stream::Cookie>,
}

impl PgpCryptor {
    pub fn new(w: Box<Target>, key_file: &str) -> Result<PgpCryptor, Error> {
        let target = sync::Arc::new(sync::Mutex::new(w));

        let tpk = openpgp::TPK::from_file(key_file)
            .context("failed to read backup encryption key")?;

        let message = stream::Message::new(Wrapper(target.clone()));
        let encryptor = stream::Encryptor::new(message,
            &[],
            &[&tpk],
            stream::EncryptionMode::AtRest,
            None)
            .context("failed to initialize cryptor")?;
        let writer = stream::LiteralWriter::new(encryptor, constants::DataFormat::Binary, None, None)
            .context("failed to initialize writer")?;

        Ok(PgpCryptor {
            target: target,
            writer: writer,
        })
    }
}

struct Wrapper(sync::Arc<sync::Mutex<Box<Target>>>);

impl io::Write for Wrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut locked = self.0.lock().expect("mutex lock poisoned");
        locked.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut locked = self.0.lock().expect("mutex lock poisoned");
        locked.flush()
    }
}

impl io::Write for PgpCryptor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl super::Cryptor for PgpCryptor {
    fn finalize(self: Box<Self>) -> Result<Box<Target>, Error> {
        let PgpCryptor { writer, target } = { *self };
        writer.finalize()?;
        let mutex = match sync::Arc::try_unwrap(target) {
            Ok(m) => m,
            Err(_) => panic!("failed to unwrap arc"),
        };
        let target = mutex.into_inner().expect("mutex lock poisoned");
        
        Ok(target)
    }
}
