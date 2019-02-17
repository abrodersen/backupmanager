pub(crate) mod identity;

use std::io;

use destination::Target;

pub trait Cryptor: io::Write {
    fn finalize(self: Box<Self>) -> Box<Target>;
}
