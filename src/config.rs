
use failure::Error;

pub fn load_config(path: &Path) -> Result<Config, Error> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)?;
    

}

pub struct Config {
    targets: Vec<Target>
}

pub struct Target {
    name: String,
    typ: TargetType,
}

pub enum TargetType {
    S3(S3Config)
}

pub struct S3Config {
    region: String,
    bucket: String,
    key_prefix: Option<String>,
}

pub struct Source {
    name: String,
    target: String,
}

pub enum SourceType {
    LVM(LVMConfig)
}

pub struct LVMConfig {
    vg: String,
    lv: String,
}