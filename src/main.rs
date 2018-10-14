
extern crate lvm2;
extern crate env_logger;

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
