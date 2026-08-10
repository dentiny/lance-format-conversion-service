use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, ValueEnum};
use tracing::Level;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen_address: SocketAddr,

    #[arg(long, default_value = "./data/service.db")]
    pub database_path: PathBuf,

    #[arg(long, value_enum, default_value = "info")]
    pub log_level: LogLevel,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Error => Self::ERROR,
            LogLevel::Warn => Self::WARN,
            LogLevel::Info => Self::INFO,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Trace => Self::TRACE,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn defaults_match_web_contract() {
        let config = Config::parse_from(["lance-web"]);
        assert_eq!(config.listen_address.to_string(), "127.0.0.1:8080");
        assert_eq!(
            config.database_path,
            std::path::Path::new("./data/service.db")
        );
    }
}
