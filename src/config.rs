
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

use failure::Error;

use toml;

pub fn load_config(path: &Path) -> Result<Config, Error> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)?;

    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let config = toml::de::from_slice(&data)?;

    Ok(config)
}

#[derive(Deserialize)]
pub struct Config {
    targets: Vec<Target>
}

#[derive(Deserialize)]
pub struct Target {
    name: String,
    #[serde(flatten)]
    typ: TargetType,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum TargetType {
    #[serde(rename = "s3")]
    S3 { region: String, bucket: String }
}

#[derive(Deserialize)]
pub struct Source {
    name: String,
    target: String,
    #[serde(flatten)]
    typ: SourceType,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum SourceType {
    #[serde(rename = "lvm")]
    LVM { volume_group: String, logical_volume: String }
}