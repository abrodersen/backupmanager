
use super::config;
use super::source::{Source, Snapshot, lvm};
use super::destination::{Destination, aws, fd, null};
use super::encryption::{self, Cryptor};
use super::compression::{self, Compressor};

use std::fs;

use tar;

use failure::{Error};

use chrono::prelude::*;

use gethostname::gethostname;

pub struct Job {
   pub name: String,
   pub source: config::Source,
   pub destination: config::Destination,
   pub compression: Option<config::Compression>,
   pub encryption: Option<config::Encryption>,
}

pub fn full_backup(job: &Job) -> Result<(), Error> {
    info!("using source '{}'", &job.source.name);
    let source = match &job.source.typ {
        config::SourceType::LVM { volume_group, logical_volume } => {
            Box::new(lvm::LogicalVolume::new(volume_group.as_ref(), logical_volume.as_ref())) as Box<Source>
        }
    };

    info!("creating snapshot of source disk");
    let snapshot = source.snapshot()?;

    let result = snapshot.size_hint()
        .and_then(|hint| {
            info!("creating write pipeline");
            create_pipeline(job, hint)
        })
        .and_then(|compressor| {
            info!("copying data from snapshot to target");
            upload_archive(snapshot.as_ref(), compressor)
        });

    debug!("tearing down snapshot");
    if let Err(e) = snapshot.destroy() {
        error!("failed to tear down snaphsot: {}", e);
    }

    result.and_then(|target| {
        let target = target.finalize()?;
        let target = target.finalize()?;
        info!("upload succeeded, finalizing target");
        target.finalize()
    })
}

fn create_pipeline(job: &Job, size_hint: u64) -> Result<Box<Compressor>, Error> {
    info!("using destination '{}'", &job.source.name);
    let destination = match &job.destination.typ {
        config::DestinationType::S3 { region, bucket, prefix, access_key_id, secret_access_key } => {
            Box::new(aws::AwsBucket::new(
                region.as_ref(), 
                bucket.as_ref(), 
                prefix.as_ref(),
                access_key_id.as_ref(),
                secret_access_key.as_ref())) as Box<Destination>
        },
        config::DestinationType::File { path } => {
            let file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(path)?;
            Box::new(fd::FileDescriptorDestination::new(file)) as Box<Destination>
        }
        config::DestinationType::Null => Box::new(null::NullDestination) as Box<Destination>,
    };

    let timestamp = Utc::now();
    let hostname = gethostname().into_string()
        .map_err(|_| format_err!("failed to convert hostname to string"))?;
    let name = format!("{}/{}/{}.full", hostname, job.name, timestamp.to_rfc3339());

    info!("allocating a target with size hint {} for backup data", size_hint);
    let target = destination.allocate(&name, size_hint)?;

    let cryptor = match &job.encryption {
        None => Box::new(encryption::identity::IdentityCryptor::new(target)) as Box<Cryptor>,
        Some(cfg) => match cfg.typ {
            config::EncryptionType::Pgp { ref pubkey_file } => {
                let pgp = encryption::pgp::PgpCryptor::new(target, pubkey_file)?;
                Box::new(pgp) as Box<Cryptor>
            }
        }
    };

    let compressor = match &job.compression {
        None => Box::new(compression::identity::IdentityCompressor::new(cryptor)) as Box<Compressor>,
        Some(cfg) => match cfg.typ {
            config::CompressionType::Gzip => Box::new(compression::gzip::GzipCompressor::new(cryptor)) as Box<Compressor>,
        }
    };

    Ok(compressor)
}

fn upload_archive(snapshot: &Snapshot, target: Box<Compressor>) -> Result<Box<Compressor>, Error> {
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
            header.set_metadata(&metadata);
            let link = fs::read_link(&full_path)?;
            builder.append_link(&mut header, rel_path, link)?;
        }
    }

    let target = builder.into_inner()?;

    Ok(target)
}


