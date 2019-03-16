
use std::path::Path;
use std::io;
use std::thread;
use std::time;

use failure::Error;

use sys_mount::{self, Mount, MountFlags, SupportedFilesystems, FilesystemType, UnmountFlags};

pub fn mount<P: AsRef<Path> >(src: P, dst: P) -> Result<(), Error> {
    debug!("mounting '{}' to '{}'", src.as_ref().display(), dst.as_ref().display());
    let supported = SupportedFilesystems::new()?;
    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::NOEXEC);
    flags.insert(MountFlags::NOSUID);
    flags.insert(MountFlags::RDONLY);
    trace!("creating new mount, filesystems: {:?} flags: {:?}", supported, flags);

    trace!("begin mount loop");
    loop {
        match Mount::new(src.as_ref(), dst.as_ref(), &supported, flags, None) {
            Ok(_) => {
                trace!("mount successful");
                return Ok(())
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::NotFound => {
                        thread::sleep(time::Duration::from_millis(10));
                    },
                    _ => {
                        error!("mount failed: {}", e);
                        bail!(e);
                    }
                }
            },
        }
    }
}

pub fn mount_ceph<P: AsRef<Path>>(mon: &str, path: &str, name: &str, secret: &str, dst: P) -> Result<(), Error> {
    let source = format!("{}:{}", mon, path);
    debug!("mounting '{}' to '{}'", source, dst.as_ref().display());

    let mut flags = MountFlags::empty();
    flags.insert(MountFlags::NOEXEC);
    flags.insert(MountFlags::NOSUID);
    //flags.insert(MountFlags::RDONLY);
    let data = format!("name={},secret={}", name, secret);
    trace!("creating new ceph mount, flags: {:?}", flags);

    trace!("begin mount loop");
    loop {
        let fs = FilesystemType::Manual("ceph");
        match Mount::new(&source, dst.as_ref(), fs, flags, Some(&data)) {
            Ok(_) => {
                trace!("mount successful");
                return Ok(())
            },
            Err(e) => {
                match e.kind() {
                    io::ErrorKind::NotFound => {
                        thread::sleep(time::Duration::from_millis(10));
                    },
                    _ => {
                        error!("mount failed: {}", e);
                        bail!(e);
                    }
                }
            },
        }
    }
}

pub fn unmount<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    sys_mount::unmount(path, UnmountFlags::FORCE)
        .map_err(Error::from)
}