
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

mod mount;
mod config;
mod source;
mod compression;
mod encryption;
mod destination;
mod backup;

fn main() {
    env_logger::init();

    let source = config::Source {
        name: "lvm".into(),
        typ: config::SourceType::LVM { 
            volume_group: "vg-test".into(), 
            logical_volume: "lv-test".into(),
        }
    };

    let dest = config::Destination {
        name: "null".into(),
        typ: config::DestinationType::File { path: "/dev/stdout".into() },
    };

    let encr = config::Encryption {
        name: "pgp".into(),
        typ: config::EncryptionType::Pgp { pubkey_file: "/dev/stdin".into() }
    };

    let comp = config::Compression {
        name: "gzip".into(),
        typ: config::CompressionType::Gzip,
    };

    let job = backup::Job {
        name: "test-backup".into(),
        source: source,
        destination: dest,
        encryption: Some(encr),
        compression: Some(comp),
    };

    backup::full_backup(&job).unwrap();
}
