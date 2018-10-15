
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
    destinations: Option<Vec<Destination>>,
    sources: Option<Vec<Source>>,
}

#[derive(Deserialize)]
pub struct Destination {
    name: String,
    #[serde(flatten)]
    typ: DestinationType,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum DestinationType {
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

mod test {
    use super::*;
    use std::env;
    use std::path::PathBuf;

    fn config_path(name: &str) -> PathBuf {
        let mut buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        buf.push("resources");
        buf.push("test");
        buf.push(name);
        buf
    }

    #[test]
    fn read_simple_source_config() {
        let config = load_config(&config_path("source.toml")).unwrap();
        let source = &config.sources.unwrap()[0];
        assert_eq!(source.name, "foo");
    }

    #[test]
    fn read_simple_destination_config() {
        let config = load_config(&config_path("destination.toml")).unwrap();
        let destinations = &config.destinations.unwrap()[0];
        assert_eq!(destinations.name, "foo");
    }
}