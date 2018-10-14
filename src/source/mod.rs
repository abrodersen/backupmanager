
use std::fs::{Metadata, ReadDir};
use std::path::PathBuf;

pub trait Source {
    type S: Snapshot;

    fn snapshot() -> Self::S;
}

pub trait Snapshot: Drop {
    fn files<'a>(&'a self) -> Files<'a>;
}

pub struct Files<'a> {
    base: &'a Path,
    current: ReadDir,
    stack: Vec<ReadDir>,
}

impl<'a> Files<'a> {
    fn new(base: &'a Path) -> Result<Files<'a>, Error> {
        let start = fs::read_dir(base)?;
        Ok(Files {
            base: base,
            current: start,
            stack: Vec::new(),
        })
    }
}

impl<'a> Iterator for Files<'a> {
    type Item = Result<(PathBuf, Metadata), Error>;

    fn next() -> Option<Self::Item> {
        fs::
    }
}
