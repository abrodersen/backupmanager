pub(crate) mod identity;

use std::io;

use encryption::Cryptor;

pub trait Compressor: io::Write {
    fn finalize(self: Box<Self>) -> Box<Cryptor>;
}