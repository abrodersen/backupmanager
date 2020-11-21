
use std::path::PathBuf;
use std::cmp;

use super::{Source, Snapshot, Files};
use crate::mount;

use lvm2::{Context, Mode};

use failure::{Error};

use uuid::Uuid;

use tempfile::{self, TempDir};

pub struct LogicalVolume {
    vg: String,
    lv: String,
}

impl LogicalVolume {
    pub fn new(vg: &str, lv: &str) -> LogicalVolume {
        LogicalVolume { vg: vg.into(), lv: lv.into() }
    }
}

impl Source for LogicalVolume {

    

    fn snapshot(&self) -> Result<Box<Snapshot>, Error> {
        trace!("snapshot of lv '{}/{}' started", self.vg, self.lv);

        let id = Uuid::new_v4();
        let ctx = Context::new();
        trace!("scanning LVM volumes");
        ctx.scan();

        trace!("opening vg '{}'", self.vg);
        let vg = ctx.open_volume_group(&self.vg, &Mode::ReadWrite);

        trace!("listing logical volumes");
        let volume = vg.list_logical_volumes()
            .into_iter()
            .find(|lv| lv.name() == self.lv)
            .ok_or_else(|| format_err!("snapshot not found"))?;

        let size = volume.size();
        debug!("lvm volume is {} bytes", size);
        let snapshot_size = cmp::max(size / 64, 1 << 26);
        debug!("using snapshot size of {} bytes", snapshot_size);

        let name = snapshot_name(&self.lv, id);
        debug!("creating snapshot with name '{}'", &name);
        let snapshot = volume.snapshot(&name, snapshot_size);

        debug!("created snapshot '{}'", snapshot.name());

        let dir = tempfile::tempdir()?;
        debug!("created tempdir '{}'", dir.path().display());

        let mut buf = PathBuf::from("/dev");
        buf.push(&self.vg);
        buf.push(snapshot.name());

        let src = buf;
        let dest = dir.path().to_path_buf();


        trace!("mounting block device '{}' to tempdir '{}'", src.display(), dest.display());
        if let Err(e) = mount::mount(src, dest) {
            error!("failed to mount snapshot: {}", e);
            let name = snapshot.name().to_string();
            trace!("removing snapshot '{}'", name);
            snapshot.remove();
            trace!("snapshot '{}' removed", name);
            return Err(e);
        }

        Ok(Box::new(LogicalVolumeSnaphsot {
            vg: self.vg.clone(),
            lv: self.lv.clone(),
            id: id,
            dir: dir,
        }))
    }
}

fn snapshot_name(lv: &str, id: Uuid) -> String {
    format!("{}_snapshot_{}", lv, id)
}

pub struct LogicalVolumeSnaphsot {
    vg: String,
    lv: String,
    id: Uuid,
    dir: TempDir,
}

impl Snapshot for LogicalVolumeSnaphsot {
    fn size_hint(&self) -> Result<u64, Error> {
        crate::stat::get_fs_size(self.dir.path())
    }

    fn files<'a>(&'a self) -> Result<Files<'a>, Error> {
        Files::new(&self.dir.path())
    }

    fn destroy(self: Box<Self>) -> Result<(), Error> {
        mount::unmount(self.dir.path())?;

        let snapshot = snapshot_name(&self.lv, self.id);
        let ctx = Context::new();
        ctx.scan();
        let group = ctx.open_volume_group(&self.vg, &Mode::ReadWrite);
        let volume = group.list_logical_volumes()
            .into_iter()
            .find(|v| v.name() == snapshot)
            .ok_or_else(|| format_err!("snapshot not found"))?;

        volume.remove();

        Ok(())
    }
}
