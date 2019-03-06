
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read, Write};

use failure::Error;

use rand::rngs::OsRng;
use rand::Rng;

use argon2::{self, Config};

pub struct Manifest {
    salt: Key,
    keys: HashSet<Key>,
    lanes: u32,
    mem_cost: u32,
    time_cost: u32,
    variant: argon2::Variant,
    version: argon2::Version,
}

const KEY_LENGTH: usize = 32;
const DEFAULT_PARALLELISM: u32 = 8;
const DEFAULT_MEM_COST: u32 = 1 << 18;
const DEFAULT_TIME_COST: u32 = 1 << 4;

impl Manifest {
    pub fn new() -> Result<Manifest, Error> {
        let mut rng = OsRng::new()?;
        let salt_data: [u8; KEY_LENGTH] = rng.gen();

        Ok(Manifest {
            salt: Key { data: salt_data },
            keys: HashSet::new(),
            variant: argon2::Variant::Argon2i,
            version: argon2::Version::Version13,
            time_cost: DEFAULT_TIME_COST,
            mem_cost: DEFAULT_MEM_COST,
            lanes: DEFAULT_PARALLELISM,
        })
    }

    pub fn parse<R>(r: R) -> Result<Manifest, Error> 
        where R: Read
    {
        let mut reader = BufReader::new(r);
        let mut line = String::new();
        reader.read_line(&mut line)?;

        let mut manifest = {
            let mut parts: Vec<_> = line.split(' ')
                .collect();

            parts.reverse();

            let algo = Algorithm::from_u32(pop(&mut parts)?.parse()?)?;
            let variant = argon2::Variant::from_u32(pop(&mut parts)?.parse()?)?;
            let version = argon2::Version::from_u32(pop(&mut parts)?.parse()?)?;
            let time_cost = pop(&mut parts)?.parse()?;
            let mem_cost = pop(&mut parts)?.parse()?;
            let lanes = pop(&mut parts)?.parse()?;

            let salt = hex::decode(pop(&mut parts)?)?;

            if salt.len() != KEY_LENGTH {
                bail!("salt must be {} bytes", KEY_LENGTH)
            }

            let mut salt_data = [0; KEY_LENGTH];
            salt_data.copy_from_slice(&salt[..]);

            Manifest {
                salt: Key { data: salt_data },
                keys: HashSet::new(),
                variant: variant,
                version: version,
                time_cost: time_cost,
                mem_cost: mem_cost,
                lanes: lanes,
            }
        };

        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }

            let hash_data = hex::decode(&line)?;

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

    pub fn insert(&mut self, e: &Entry) {
        let key = self.gen_key(&e);
        self.keys.insert(key);
    }

    pub fn contains(&self, e: &Entry) -> bool {
        let key = self.gen_key(&e);
        self.keys.contains(&key)
    }

    fn gen_key(&self, e: &Entry) -> Key {
        let config = Config {
            secret: &[],
            ad: &[],
            thread_mode: argon2::ThreadMode::Parallel,
            hash_length: KEY_LENGTH as u32,
            lanes: self.lanes,
            mem_cost: self.mem_cost,
            time_cost: self.time_cost,
            variant: self.variant,
            version: self.version,
        };

        let mut buffer = Vec::new();
        e.serialize(&mut buffer).expect("failed to write buffer data");
        let hash = argon2::hash_raw(&buffer, &self.salt.data, &config).expect("failed to hash data");
        let mut key_data = [0; KEY_LENGTH];
        key_data.copy_from_slice(&hash[..]);

        Key { data: key_data }
    }
}

fn pop<T>(vec: &mut Vec<T>) -> Result<T, Error> {
    vec.pop().ok_or_else(|| format_err!("not enough values"))
}

pub struct Entry {
    path: String,
}

impl Entry {
    pub fn new<S>(path: S) -> Entry 
        where S: Into<String> 
    {
        Entry {
            path: path.into(),
        }
    }

    fn serialize<W>(&self, w: W) -> Result<(), Error>
        where W: Write
    {
        bincode::serialize_into(w, &self.path)?;
        Ok(())
    }
}

#[derive(Hash, Eq, PartialEq)]
struct Key {
    data: [u8; KEY_LENGTH]
}

enum Algorithm {
    Argon2
}

impl Algorithm {
    fn as_u32(&self) -> u32 {
        match self {
            Algorithm::Argon2 => 0x1,
        }
    }

    fn from_u32(val: u32) -> Result<Algorithm, Error> {
        match val {
            0x1 => Ok(Algorithm::Argon2),
            _ => Err(format_err!("invalid algorithm"))
        }
    }
}

