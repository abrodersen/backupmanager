
use std::path::PathBuf;

use super::{Source, Snapshot, Files};
use mount;

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
    type S = LogicalVolumeSnaphsot;

    fn snapshot(&self) -> Result<LogicalVolumeSnaphsot, Error> {
        let id = Uuid::new_v4();
        let ctx = Context::new();
        ctx.scan();
        let vg = ctx.open_volume_group(&self.vg, &Mode::ReadWrite);
        let volume = vg.list_logical_volumes()
            .into_iter()
            .find(|lv| lv.name() == self.lv);

        let name = snapshot_name(&self.lv, id);
        let _ = volume
            .ok_or_else(|| format_err!("snapshot not found"))
            .map(|v| v.snapshot(&name, 0))?;

        let mut buf = PathBuf::from("/dev");
        buf.push(&self.vg);
        buf.push(&self.lv);

        let dir = tempfile::tempdir()?;

        mount::mount(buf, dir.path().to_path_buf())?;

        Ok(LogicalVolumeSnaphsot {
            vg: self.vg.clone(),
            lv: self.lv.clone(),
            id: id,
            dir: dir,
        })
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
    fn files<'a>(&'a self) -> Result<Files<'a>, Error> {
        Files::new(&self.dir.path())
    }

    fn destroy(self) -> Result<(), Error> {
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
