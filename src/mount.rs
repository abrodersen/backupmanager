
use std::path::Path;
use std::io;

use failure::Error;

use sys_mount::{self, Mount, MountFlags, SupportedFilesystems, UnmountFlags};

pub fn mount<P: AsRef<Path> >(src: P, dst: P) -> Result<(), Error> {
    debug!("mounting '{}' to '{}'", src.as_ref().display(), dst.as_ref().display());
    let supported = SupportedFilesystems::new()?;
    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::NOEXEC);
    flags.insert(MountFlags::NOSUID);
    flags.insert(MountFlags::RDONLY);
    trace!("creating new mount, filesystems: {:?} flags: {:?}", supported, flags);

    loop {
        trace!("start loop iteration");
        match Mount::new(src.as_ref(), dst.as_ref(), &supported, flags, None) {
            Ok(_) => {
                trace!("mount successful");
                return Ok(())
            },
            Err(e) => {
                error!("mount failed: {}", e);
                if e.kind() != io::ErrorKind::NotFound {
                    bail!(e);
                }
            },
        }
    }
}

pub fn unmount<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    sys_mount::unmount(path, UnmountFlags::FORCE)
        .map_err(Error::from)
}