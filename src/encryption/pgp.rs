
use std::io;
use std::sync;
use std::cell;

use openpgp::packet::{Key, key::KeyParts, key::KeyRole};
use openpgp::parse::Parse;
use openpgp::serialize::stream::{Message, Encryptor, LiteralWriter, Recipient};
use openpgp::parse::PacketParser;
use openpgp::cert::{Cert, CertParser};
use openpgp::policy::{Policy, StandardPolicy};

use crate::destination::Target;

use anyhow::{Error, Context};

use owning_ref::BoxRef;

pub struct PgpCryptor<'a> {
    target: sync::Arc<sync::Mutex<Box<dyn Target>>>,
    //context: std::vec::Vec<openpgp::cert::prelude::ValidKeyAmalgamation<openpgp::packet::key::PublicParts, openpgp::packet::key::UnspecifiedRole, bool>>,
    writer: Message<'a>,
}

pub struct PgpContext {
    cert: Cert,
    policy: Box<dyn Policy>,
}

impl PgpContext {
    pub fn new(key_file: &str) -> Result<PgpContext, Error> {
        let policy = StandardPolicy::new();
        let cert = CertParser::from_file(key_file)
            .context("failed to initialize certificate parser file")?
            .next()
            .ok_or_else(|| Error::msg("no public keys found in file"))
            .and_then(|x| x)?;

        Ok(PgpContext {
            cert: cert,
            policy: Box::new(policy),
        })
    }
}

// struct KeyWrapper<P: KeyParts, R: KeyRole>(Box<Key<P, R>>);

// impl<P, R> From<KeyWrapper<P, R>> for Recipient<'static>
//     where P: KeyParts, R: KeyRole
// {
//     fn from(key: KeyWrapper<P, R>) -> Self {
//         Self::new(key.0.keyid(), key.0)
//     }
// }

impl<'a> PgpCryptor<'a> {
    pub fn new<'b: 'a>(w: Box<dyn Target>, ctx: &'b PgpContext) -> Result<PgpCryptor<'a>, Error> {
        let target = sync::Arc::new(sync::Mutex::new(w));

        

        //let mut bundle = Vec::new();

        let recipients = ctx.cert.keys()
            .with_policy(&*ctx.policy, None)
            .alive()
            .revoked(false)
            .for_storage_encryption()
            // .map(|alg| KeyWrapper(Box::new(alg.key().clone())))
            .collect::<Vec<_>>();

        // for key_amalg in recipients {
        //     let key = key_amalg.key().clone();
        //     bundle.push(Recipient::new(key.keyid(), &key));
        // }

        // let tpk = openpgp::TPK::from_file(key_file)
        //     .context("failed to read backup encryption key")?;

        let message = Message::new(Wrapper(target.clone()));
        let encryptor = Encryptor::for_recipients(message, recipients).build()
            .context("failed to initialize encryptor")?;
        let writer = LiteralWriter::new(encryptor).build()
            .context("failed to initialize writer")?;

        Ok(PgpCryptor {
            target: target,
            //context: recipients,
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

impl<'a> io::Write for PgpCryptor<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<'a> super::Cryptor for PgpCryptor<'a> {
    fn finalize(self: Box<Self>) -> Result<Box<dyn Target>, Error> {
        let PgpCryptor { writer, target, .. } = { *self };
        writer.finalize()?;
        let mutex = match sync::Arc::try_unwrap(target) {
            Ok(m) => m,
            Err(_) => panic!("failed to unwrap arc"),
        };
        let target = mutex.into_inner().expect("mutex lock poisoned");
        
        Ok(target)
    }
}
