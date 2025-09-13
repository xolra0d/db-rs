use log::{self, error};

pub struct Config {
    pub _log_level: log::Level,
    pub addr: String,
}

fn get_env_or_exit(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|e| {
        error!("env error: {}", e);
        std::process::exit(1);
    })
}

impl Config {
    pub fn build() -> Self {
        env_logger::init();

        let host = get_env_or_exit("HOST");
        let port = get_env_or_exit("PORT");

        Self {
            _log_level: log::Level::Info,
            addr: format!("{}:{}", host, port),
        }
    }
}
