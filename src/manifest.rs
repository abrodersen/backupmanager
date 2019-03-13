
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::convert::From;
use std::path::{Path, PathBuf};

use failure::Error;

use rand::rngs::OsRng;
use rand::Rng;

use sha2::Digest;

use chrono::prelude::*;

#[derive(Eq, PartialEq, Debug)]
pub struct Manifest {
    salt: Key,
    keys: BTreeSet<Key>,
}

const KEY_LENGTH: usize = 32;

impl Manifest {
    pub fn new() -> Result<Manifest, Error> {
        let mut rng = OsRng::new()?;
        let salt_data: [u8; KEY_LENGTH] = rng.gen();

        Ok(Manifest {
            salt: Key { data: salt_data },
            keys: BTreeSet::new(),
        })
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn deserialize<R>(r: R) -> Result<Manifest, Error> 
        where R: Read
    {
        let mut reader = BufReader::new(r);
        let mut line = String::new();
        reader.read_line(&mut line)?;

        let mut manifest = {
            let mut parts: Vec<_> = line.split(' ')
                .map(|s| s.trim())
                .collect();

            parts.reverse();

            let _ = Algorithm::from_u32(pop(&mut parts)?.parse()?)?;

            let salt = hex::decode(pop(&mut parts)?)?;

            if salt.len() != KEY_LENGTH {
                bail!("salt must be {} bytes", KEY_LENGTH)
            }

            let mut salt_data = [0; KEY_LENGTH];
            salt_data.copy_from_slice(&salt[..]);

            Manifest {
                salt: Key { data: salt_data },
                keys: BTreeSet::new(),
            }
        };

        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }

            let hash_data = hex::decode(line.trim())?;

            if hash_data.len() != KEY_LENGTH {
                bail!("hash must be {} bytes", KEY_LENGTH);
            }

            let mut key_data = [0; KEY_LENGTH];
            key_data.copy_from_slice(&hash_data[..]);

            let key = Key { data: key_data };
            manifest.keys.insert(key);
        }

        Ok(manifest)
    }

    pub fn serialize<W>(&self, mut w: W) -> Result<(), Error> 
        where W: Write
    {
        let algo = Algorithm::Sha256.as_u32();
        let salt = hex::encode(self.salt.data);
        write!(w, "{} {}\n", algo, salt)?;
        for key in &self.keys {
            let encoded = hex::encode(key.data);
            write!(w, "{}\n", encoded)?;
        }

        Ok(())
    }

    pub fn insert(&mut self, e: &Entry) {
        let key = self.gen_key(&e);
        self.keys.insert(key);
    }

    pub fn contains(&self, e: &Entry) -> bool {
        let key = self.gen_key(&e);
        self.keys.contains(&key)
    }

    fn gen_key(&self, e: &Entry) -> Key {
        let mut hasher = sha2::Sha256::new();
        let mut buffer = Vec::new();
        e.serialize(&mut buffer).expect("failed to write buffer data");
        hasher.input(&buffer);
        hasher.input(&self.salt.data);
        let hash = hasher.result();
        let mut key_data = [0; KEY_LENGTH];
        key_data.copy_from_slice(&hash[..]);

        Key { data: key_data }
    }
}

fn pop<T>(vec: &mut Vec<T>) -> Result<T, Error> 
    where T: std::fmt::Display
{
    vec.pop().ok_or_else(|| format_err!("not enough values"))
}

pub struct Entry {
    path: PathBuf,
    modified: DateTime<Utc>,
    uid: u32,
    gid: u32,
    mode: u32,
}

impl Entry {
    pub fn new<P, T>(path: P, modified: T, uid: u32, gid: u32, mode: u32) -> Entry 
        where P: AsRef<Path>, T: Into<DateTime<Utc>>
    {
        Entry {
            path: path.as_ref().into(),
            modified: modified.into(),
            uid: uid,
            gid: gid,
            mode: mode,
        }
    }

    fn serialize<W>(&self, mut w: W) -> Result<(), Error>
        where W: Write
    {
        let path = self.path.to_str()
            .ok_or_else(|| format_err!("path is not valid utf-8"))?;
        bincode::serialize_into(&mut w, path.as_bytes())?;
        bincode::serialize_into(&mut w, &self.modified.timestamp())?;
        bincode::serialize_into(&mut w, &self.uid)?;
        bincode::serialize_into(&mut w, &self.gid)?;
        bincode::serialize_into(&mut w, &self.mode)?;
        Ok(())
    }
}

#[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Debug)]
struct Key {
    data: [u8; KEY_LENGTH]
}

impl From<[u8; KEY_LENGTH]> for Key {
    fn from(d: [u8; KEY_LENGTH]) -> Self {
        Key { data: d }
    }
}

enum Algorithm {
    Sha256
}

impl Algorithm {
    fn as_u32(&self) -> u32 {
        match self {
            Algorithm::Sha256 => 0x1,
        }
    }

    fn from_u32(val: u32) -> Result<Algorithm, Error> {
        match val {
            0x1 => Ok(Algorithm::Sha256),
            _ => Err(format_err!("invalid algorithm"))
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use std::collections::BTreeSet;


    #[test]
    fn round_trip() {
        let key = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31];

        let mut keys = BTreeSet::new();
        for i in 0..31 {
            keys.insert({ let mut arr = key.clone(); arr.rotate_left(i); arr.into() });

        }

        let manifest = Manifest {
            salt: key.into(),
            keys: keys,
        };

        let mut buffer = Vec::new();
        manifest.serialize(&mut buffer).unwrap();

        let text = std::str::from_utf8(&buffer).unwrap();
        println!("{}", text);


        let result = Manifest::deserialize(&buffer[..]).unwrap();

        assert_eq!(manifest, result);
    }

    #[test]
    fn test_contains() {
        let salt = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31];

        let mut manifest = Manifest {
            salt: salt.into(),
            keys: BTreeSet::new(),
        };

        use chrono::TimeZone;
        let dt = Utc.timestamp(0, 0);
        let entry = Entry::new("foo/bar", dt, 0, 0, 0);
        manifest.insert(&entry);

        assert_eq!(true, manifest.contains(&entry))
    }
}

