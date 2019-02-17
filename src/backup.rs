
use super::config;
use super::source::{self, Source, Snapshot, lvm};
use super::destination::{self, Destination, Target, aws, fd, null};
use super::io::{WriteChunker, Chunk};

use std::any;
use std::fs;
use std::io::{self, Write};
use std::cmp;
use std::mem;
use std::sync;

use tar;

use failure::Error;

use futures;
use futures::stream;

use crossbeam::channel;
use crossbeam::thread;

pub struct Job {
   pub name: String,
   pub source: config::Source,
   pub destination: config::Destination,
   pub compression: Option<config::Compression>,
   pub encryption: Option<config::Encryption>,
}

pub fn full_backup(job: &Job) -> Result<(), Error> {
    let source = match &job.source.typ {
        config::SourceType::LVM { volume_group, logical_volume } => {
            Box::new(lvm::LogicalVolume::new(volume_group.as_ref(), logical_volume.as_ref())) as Box<Source>
        }
    };

    let destination = match &job.destination.typ {
        config::DestinationType::S3 { region, bucket } => {
            Box::new(aws::AwsBucket::new(region.as_ref(), bucket.as_ref())) as Box<Destination>
        },
        config::DestinationType::File { path } => {
            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(path)?;
            Box::new(fd::FileDescriptorDestination::new(file)) as Box<Destination>
        }
        config::DestinationType::Null => Box::new(null::NullDestination) as Box<Destination>,
        _ => panic!("destination not implemented"),
    };

    info!("creating snapshot of source disk");
    let snapshot = source.snapshot()?;
    info!("allocating a target for backup data");
    let target = destination.allocate(job.name.as_ref())?;
    
    info!("copying data from snapshot to target");
    let result = upload_archive(snapshot.as_ref(), target);

    debug!("tearing down snapshot");
    if let Err(e) = snapshot.destroy() {
        error!("failed to tear down snaphsot: {}", e);
    }

    result.and_then(|target| {
        info!("upload succeeded, finalizing target");
        target.finalize()
    })
}

fn upload_archive(snapshot: &Snapshot, target: Box<Target>) -> Result<Box<Target>, Error> {
    let mut builder = tar::Builder::new(target);
    builder.follow_symlinks(false);

    let files = snapshot.files()?;
    let base_path = files.base_path();

    debug!("enumerating snapshot files");
    for entry in files {
        let (rel_path, metadata) = entry?;
        let full_path = base_path.join(&rel_path);
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            trace!("appending dir '{}' to archive", rel_path.display());
            builder.append_dir(&rel_path, &full_path)?;
        }

        if file_type.is_file() {
            trace!("appending file '{}' to archive", rel_path.display());
            let mut file = fs::File::open(&full_path)?;
            builder.append_file(&rel_path, &mut file)?;
        }

        if file_type.is_symlink() {
            trace!("appending symlink '{}' to archive", rel_path.display());
            let mut header = tar::Header::new_gnu();
            header.set_path(rel_path)?;
            header.set_metadata(&metadata);
            let link = fs::read_link(&full_path)?;
            header.set_link_name(link)?;
        }
    }

    let target = builder.into_inner()?;

    Ok(target)
}


