
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

mod mount;
mod config;
mod source;
mod destination;
mod backup;

fn main() {
    env_logger::init();

    let context = lvm2::Context::new();
    context.scan();

    let vg_names = context.list_volume_group_names();

    for vg_name in vg_names {
        println!("group: {}", vg_name);
        let vg = context.open_volume_group(&vg_name, &lvm2::Mode::ReadWrite);

        for lv in vg.list_logical_volumes() {
            if lv.name() == "home" {
                let size = (1 << 30) * 4;
                let snapshot = lv.snapshot("home_snap", size);
                println!("snapshot: {}", snapshot.name());
                println!("origin: {}", snapshot.origin().unwrap());
                snapshot.remove();
            } else {
                println!("volume: {}", lv.name());

            }
        }
    }

    
}
