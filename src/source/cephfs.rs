
use std::fs;
use std::path::PathBuf;
use std::cmp;

use super::{Source, Snapshot, Files};
use mount;

use failure::{Error};

use uuid::Uuid;

use tempfile::{self, TempDir};

pub struct CephFileSystem {
    mon: String,
    path: String,
    name: String,
    secret: String,
}

impl CephFileSystem {
    pub fn new(mon: String, path: String, name: String, secret: String) -> CephFileSystem {
        CephFileSystem {
            mon: mon,
            path: path,
            name: name,
            secret: secret,
        }
    }
}

impl Source for CephFileSystem {
    fn snapshot(&self) -> Result<Box<Snapshot>, Error> {
        trace!("snapshot of cephfs '{}:{}' started", self.mon, self.path);

        let dir = tempfile::tempdir()?;
        debug!("created tempdir '{}'", dir.path().display());

        let dest = dir.path().to_path_buf();

        trace!("mounting cephfs to tempdir '{}'", dest.display());
        mount::mount_ceph(&self.mon, &self.path, &self.name, &self.secret, &dest)?;

        let id = Uuid::new_v4();
        let mut snap = dest;
        snap.push(".ceph");
        snap.push(format!("{}", id));
        trace!("creating snapshot at '{}'", snap.display());
        fs::create_dir_all(&snap)?;
        trace!("snapshot created");

        Ok(Box::new(CephFileSystemSnapshot {
            dir: dir,
            snap: snap,
        }))
    }
}

pub struct CephFileSystemSnapshot {
    dir: TempDir,
    snap: PathBuf,
}

impl Snapshot for CephFileSystemSnapshot {
    fn size_hint(&self) -> Result<u64, Error> {
        crate::stat::get_fs_size(&self.snap)
    }

    fn files<'a>(&'a self) -> Result<Files<'a>, Error> {
        Files::new(&self.snap)
    }

    fn destroy(self: Box<Self>) -> Result<(), Error> {
        fs::remove_dir_all(&self.snap)?;
        mount::unmount(self.dir.path())?;

        Ok(())
    }
}
