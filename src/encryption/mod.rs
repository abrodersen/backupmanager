pub(crate) mod identity;
pub(crate) mod pgp;

use std::io;

use crate::destination::Target;

use anyhow::Error;

pub trait Cryptor: io::Write {
    fn finalize(self: Box<Self>) -> Result<Box<dyn Target>, Error>;
}
