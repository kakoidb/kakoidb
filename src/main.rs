extern crate config;
extern crate simplelog;
#[macro_use]
extern crate log;
#[macro_use]
extern crate juniper;
extern crate juniper_warp;
extern crate rocksdb;
extern crate warp;
#[macro_use]
extern crate serde_derive;
extern crate atty;
extern crate bincode;
extern crate chrono;
extern crate tokio;
extern crate tokio_timer;

mod api;
mod database;
mod entities;
mod janitor;

use api::{start_api, ApiConfig};
use atty::Stream;
use database::Database;
use janitor::start_janitor;
use janitor::JanitorConfig;
use simplelog::{SimpleLogger, TermLogger};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct StorageConfig {
  path: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Config {
  server: Option<ApiConfig>,
  storage: StorageConfig,
  janitor: Option<JanitorConfig>,
  log_level: Option<String>,
}

fn main() {
  let mut settings: config::Config = config::Config::default();
  settings
    // Add in `./Settings.toml`
    .merge(config::File::with_name("Settings"))
    .unwrap()
    // Add in settings from the environment (with a prefix of KAKOI)
    // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
    .merge(
      config::Environment::with_prefix("KAKOI")
        .separator("_")
        .ignore_empty(true),
    )
    .unwrap();

  let config = settings.try_into::<Config>().unwrap_or_else(|err| {
    eprintln!("Invalid config: {}", err);
    ::std::process::exit(1);
  });

  let log_level = log::Level::from_str(&config.log_level.as_ref().unwrap_or(&"info".to_string()))
    .unwrap()
    .to_level_filter();
  let mut log_config = simplelog::Config::default();
  log_config.time_format = Some("%+");
  if atty::is(Stream::Stdout) {
    TermLogger::init(log_level, log_config).unwrap();
  } else {
    SimpleLogger::init(log_level, log_config).unwrap();
  }

  // Print out our settings
  debug!("Config: {:?}", &config);

  let db_dir = Path::new(&config.storage.path).join("test.db");
  let db = Arc::new(RwLock::new(Database::open(db_dir)));

  start_janitor(&config.janitor, db.clone()).unwrap_or_else(|err| {
    eprintln!("Invalid config [janitor]: {}", err);
    ::std::process::exit(1);
  });
  start_api(&config.server, db.clone());
}
