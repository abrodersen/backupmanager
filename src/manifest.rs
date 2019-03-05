
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read};

use failure::Error;

use rand::rngs::OsRng;
use rand::Rng;

pub struct Manifest {
    salt: Key,
    keys: HashSet<Key>,
}

const KEY_LENGTH: usize = 16;

impl Manifest {
    pub fn new() -> Result<Manifest, Error> {
        let mut rng = OsRng::new()?;
        let salt_data: [u8; KEY_LENGTH] = rng.gen();

        Ok(Manifest {
            salt: Key { data: salt_data },
            keys: HashSet::new(),
        })
    }

    pub fn parse<R>(r: R) -> Result<Manifest, Error> 
        where R: Read
    {
        let mut reader = BufReader::new(r);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let salt = hex::decode(&line)?;

        if salt.len() != KEY_LENGTH {
            bail!("salt must be {} bytes", KEY_LENGTH)
        }

        let mut salt_data = [0; KEY_LENGTH];
        salt_data.copy_from_slice(&salt[..]);

        let mut manifest = Manifest {
            salt: Key { data: salt_data },
            keys: HashSet::new(),
        };

        loop {
            line.clear();
            if reader.read_line(&mut line)? > 0 {
                break;
            }

            let hash_data = hex::decode(&line)?;

            if hash_data.len() != KEY_LENGTH {
                bail!("hash must be {} bytes", KEY_LENGTH);
            }

            let mut key_data = [0; KEY_LENGTH];
            key_data.copy_from_slice(&hash_data[..]);

            let mut key = Key { data: key_data };
            manifest.keys.insert(key);
        }

        Ok(manifest)
    }

    pub fn insert(&mut self, desc: &Descriptor) {

    }

    pub fn contains(&self, desc: &Descriptor) -> bool {
        false
    }
}

pub struct Descriptor {
    path: String,
}

#[derive(Hash, Eq, PartialEq)]
struct Key {
    data: [u8; KEY_LENGTH]
}

