
use std::path::Path;

use nix::sys::statvfs;

use anyhow::Error;

pub fn get_fs_size<P: AsRef<Path>>(path: P) -> Result<u64, Error> {
    let stat = statvfs::statvfs(path.as_ref())?;
    let used_blocks = stat.blocks() - stat.blocks_free();
    Ok(used_blocks * stat.block_size())
}