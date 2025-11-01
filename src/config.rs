use serde::Deserialize;
use std::io::ErrorKind;
use std::net::SocketAddrV4;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    tcp_socket: SocketAddrV4,
    storage_directory: PathBuf,
    /// Logging level.
    /// ### Allowed values:
    /// - 1 => Info
    /// - 2 => Warn
    /// - 3 => Error
    log_level: usize,
    /// Max concurrent connections.
    max_connections: usize,
}

impl Config {
    /// Get TCP socket address from configuration
    pub const fn get_tcp_socket_addr(&self) -> SocketAddrV4 {
        self.tcp_socket
    }

    /// Get database directory from configuration
    pub const fn get_db_dir(&self) -> &PathBuf {
        &self.storage_directory
    }

    /// Get logging level from configuration
    pub const fn get_log_level(&self) -> log::LevelFilter {
        match &self.log_level {
            2 => log::LevelFilter::Warn,
            3 => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        }
    }

    /// Get max connections from configuration
    pub const fn get_max_connections(&self) -> usize {
        self.max_connections
    }

    /// Ensures that directory exists and is indeed directory. Creates one, if not exists
    ///
    /// # Panics:
    ///
    /// 1. When Permission denied to create a directory
    /// 2. When supplied invalid name
    /// 3. Any `std::fs::create_dir_all()` error
    /// 4. When path already exists, but is not a directory
    fn ensure_directory_exists(dir: &PathBuf) {
        std::fs::create_dir_all(dir).unwrap_or_else(|e| match e.kind() {
            ErrorKind::PermissionDenied => {
                panic!("Permission denied to create database")
            }
            ErrorKind::InvalidInput => panic!("Invalid database name"),
            e => panic!("Invalid directory: {e:?}"),
        });

        std::fs::exists(dir)
            .unwrap_or_else(|_| panic!("Can't check existence of database directory"));

        assert!(
            dir.is_dir(),
            "Database path {} exists but is not a directory.",
            dir.display()
        );
    }

    /// Builds a configuration from environment variables.
    ///
    /// # Panics:
    ///
    /// 1. When `CONFIG_PATH` env var is not set
    /// 2. When `CONFIG_PATH` env var is invalid UTF-8
    /// 2. When config file does not exist
    /// 2. When config file is invalid toml
    pub fn build() -> Self {
        let config_path =
            std::env::var("CONFIG_PATH").unwrap_or_else(|_| "touch_config.toml".to_string());

        let config_file = std::fs::read_to_string(config_path).expect("Couldn't read config file");
        let raw_config: Self = toml::from_str(config_file.as_str()).expect("Invalid config file");

        Self::ensure_directory_exists(&raw_config.storage_directory);

        raw_config
    }
}

pub static CONFIG: std::sync::LazyLock<Config> = std::sync::LazyLock::new(Config::build);
