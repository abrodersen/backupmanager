
use std::path::Path;

use failure::Error;

use sys_mount::{self, Mount, MountFlags, SupportedFilesystems, UnmountFlags};

pub fn mount<P: AsRef<Path>>(src: P, dst: P) -> Result<(), Error> {
    let supported = SupportedFilesystems::new()?;
    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::NOEXEC);
    flags.insert(MountFlags::NOSUID);
    flags.insert(MountFlags::RDONLY);

    let _ = Mount::new(src, dst, &supported, flags, None)?;
    Ok(())
}

pub fn unmount<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    sys_mount::unmount(path, UnmountFlags::FORCE)
        .map_err(Error::from)
}