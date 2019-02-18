pub(crate) mod identity;
pub(crate) mod pgp;

use std::io;

use destination::Target;

use failure::Error;

pub trait Cryptor: io::Write {
    fn finalize(self: Box<Self>) -> Result<Box<Target>, Error>;
}
