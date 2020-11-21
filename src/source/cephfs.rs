
use std::fs;
use std::path::PathBuf;

use super::{Source, Snapshot, Files};

use failure::{Error};

use uuid::Uuid;

use tempfile::{self, TempDir};

pub struct CephFileSystem {
    path: String,
}

impl CephFileSystem {
    pub fn new<S: Into<String>>(path: S,) -> CephFileSystem {
        CephFileSystem {
            path: path.into(),
        }
    }
}

impl Source for CephFileSystem {
    fn snapshot(&self) -> Result<Box<Snapshot>, Error> {
        trace!("snapshot of cephfs '{}' started", self.path);

        // let dir = tempfile::tempdir()?;
        // debug!("created tempdir '{}'", dir.path().display());

        let dest: PathBuf = self.path.clone().into();

        // trace!("mounting cephfs to tempdir '{}'", dest.display());
        // mount::mount_ceph(&self.mon, &self.path, &self.name, &self.secret, &dest)?;

        let id = Uuid::new_v4();
        let mut snap = dest;
        snap.push(".snap");
        snap.push(format!("{}", id));
        trace!("creating snapshot at '{}'", snap.display());
        fs::create_dir_all(&snap)?;
        trace!("snapshot created");

        Ok(Box::new(CephFileSystemSnapshot {
            snap: snap,
        }))
    }
}

pub struct CephFileSystemSnapshot {
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
        debug!("unlinking snapshot {}", self.snap.display());
        fs::remove_dir(&self.snap)?;

        // debug!("unmounting temp dir {}", self.dir.path().display());
        // mount::unmount(self.dir.path())?;

        Ok(())
    }
}
