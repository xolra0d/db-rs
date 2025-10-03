use log::{self, error};
use std::fmt::Display;
use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::str::FromStr;

/// Represents current database configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Socket address for server to run. Set by `HOST` and `PORT` environment variables. Default `HOST` value: 127.0.0.1. Default `PORT` value: 7070.
    socket_addr: SocketAddrV4,
    /// Directory for storing databases. Set by `DB_DIR` environment variables. Default `DB_DIR` value: `db_files`.
    db_dir: PathBuf,
    /// Logging level. Set by `LOG_LEVEL` environment variable.
    /// ### Allowed values:
    /// - 1 => Info
    /// - 2 => Warn
    /// - 3 => Error
    log_level: log::Level,
}

/// Writes `msg` error and exits
pub fn write_error_and_exit(msg: impl Display) -> ! {
    error!("Fatal error: {msg}");
    std::process::exit(1);
}

impl Config {
    /// Get socket address from configuration
    pub const fn get_socket_addr(&self) -> SocketAddrV4 {
        self.socket_addr
    }

    /// Get database directory from configuration
    pub const fn get_db_dir(&self) -> &PathBuf {
        &self.db_dir
    }

    /// Get logging level from configuration
    pub const fn get_log_level(&self) -> log::Level {
        self.log_level
    }

    /// Retrieves environment variable `key`. If not defined returns `default`
    fn get_env_or_default(key: &str, default: &str) -> String {
        std::env::var(key).unwrap_or_else(|_| default.to_string())
    }

    /// Ensures that directory exists and is indeed directory. Creates one, if not exists
    fn ensure_directory_exists(dir: &PathBuf) {
        std::fs::create_dir_all(dir).unwrap_or_else(|e| match e.kind() {
            ErrorKind::PermissionDenied => {
                write_error_and_exit("Permission denied to create database")
            }
            ErrorKind::InvalidInput => write_error_and_exit("Invalid database name"),
            _ => (),
        });

        std::fs::exists(dir).unwrap_or_else(|_| {
            write_error_and_exit("Can't check existence of database directory")
        });

        if !dir.is_dir() {
            write_error_and_exit(format!(
                "Database path {} exists but is not a directory.",
                dir.display()
            ));
        }
    }

    /// Parses log level from string variable
    fn parse_log_level(level_str: &str) -> log::Level {
        match level_str {
            "1" => log::Level::Info,
            "2" => log::Level::Warn,
            "3" => log::Level::Error,
            _ => write_error_and_exit(format!(
                "Invalid LOG_LEVEL '{level_str}'. Valid values: 1 (INFO), 2 (WARN), 3 (ERROR)",
            )),
        }
    }

    /// Builds a configuration from environment variables.
    pub fn build() -> Self {
        let host = Self::get_env_or_default("HOST", "127.0.0.1");
        let port = Self::get_env_or_default("PORT", "7070");

        let ip_addr = SocketAddrV4::new(
            Ipv4Addr::from_str(&host)
                .unwrap_or_else(|e| write_error_and_exit(format!("Invalid HOST '{host}': {e}."))),
            u16::from_str(&port)
                .unwrap_or_else(|e| write_error_and_exit(format!("Invalid PORT '{port}': {e}."))),
        );

        let db_dir = PathBuf::from(Self::get_env_or_default("DB_DIR", "db_files"));
        Self::ensure_directory_exists(&db_dir);

        let log_level_str = Self::get_env_or_default("LOG_LEVEL", "1");
        let log_level = Self::parse_log_level(&log_level_str);

        Self {
            socket_addr: ip_addr,
            db_dir,
            log_level,
        }
    }
}
