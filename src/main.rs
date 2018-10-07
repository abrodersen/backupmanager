
extern crate lvm2;
extern crate env_logger;

fn main() {
    env_logger::init();

    let volume_groups = lvm2::list_volume_groups();
    for group in volume_groups {
        println!("group: {:?}", group);

        let volumes = lvm2::list_logical_volumes(&group);
        for volume in volumes {
            println!("volume: {:?}", volume);
        }
    }
}
