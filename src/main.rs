
extern crate lvm2;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate toml;
#[macro_use]
extern crate failure;
extern crate sys_mount;
extern crate uuid;
extern crate tempfile;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate futures;
extern crate tar;
extern crate crossbeam;
extern crate flate2;
extern crate sequoia_openpgp as openpgp;
extern crate chrono;
extern crate gethostname;
#[macro_use]
extern crate structopt;

mod mount;
mod config;
mod source;
mod compression;
mod encryption;
mod destination;
mod backup;

use std::path;

use structopt::StructOpt;

use failure::Error;

#[derive(Debug, StructOpt)]
#[structopt(name = "backupmanager", about = "a file backup program")]
struct Opt {
    #[structopt(short = "j", long = "job")]
    job: String,
    #[structopt(short = "c", long = "config", default_value = "/etc/backupmanager/config.toml")]
    config: path::PathBuf,
}

fn main() -> Result<(), Error> {
    env_logger::init();

    let opt = Opt::from_args();

    let config = config::load_config(&opt.config)?;

    let jobs = config.jobs
        .ok_or_else(|| format_err!("no job configs found"))?;

    let job = jobs.into_iter()
        .find(|j| j.name == opt.job)
        .ok_or_else(|| format_err!("backup job {} not found", opt.job))?;

    let sources = config.sources
        .ok_or_else(|| format_err!("no source configs found"))?;
    let src = sources.into_iter()
        .find(|s| s.name == job.source)
        .ok_or_else(|| format_err!("source {} not found", job.source))?;

    let destinations = config.destinations
        .ok_or_else(|| format_err!("no destination configs found"))?;
    let dest = destinations.into_iter()
        .find(|d| d.name == job.destination)
        .ok_or_else(|| format_err!("destination {} not found", job.destination))?;

    let comp = match job.compression {
        None => None,
        Some(comp) => {
            let compressions = config.compression
                .ok_or_else(|| format_err!("no compression configs found"))?;
            let compression = compressions.into_iter()
                .find(|c| c.name == comp)
                .ok_or_else(|| format_err!("compression {} not found", comp))?;
            Some(compression)
        }
    };

    let encr = match job.encryption {
        None => None,
        Some(enc) => {
            let encryptions = config.encryption
                .ok_or_else(|| format_err!("no encryption configs found"))?;
            let encryption = encryptions.into_iter()
                .find(|e| e.name == enc)
                .ok_or_else(|| format_err!("encryption {} not found", enc))?;
            Some(encryption)
        }
    };

    let job = backup::Job {
        name: job.name,
        source: src,
        destination: dest,
        encryption: encr,
        compression: comp,
    };

    backup::full_backup(&job)?;

    Ok(())
}
