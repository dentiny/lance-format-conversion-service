use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use lance_conversion_core::env::{self, EnvError};

const LISTEN_ADDRESS_ENV: &str = "LISTEN_ADDRESS";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const DEFAULT_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
const DEFAULT_DATABASE_URL: &str = "sqlite://./data/service.db";

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub database_url: String,
}

impl Config {
    /// Loads web settings from environment variables, using defaults for values
    /// that are not set.
    ///
    /// # Errors
    ///
    /// Returns an error when an environment value is not valid for its setting.
    pub fn from_env() -> Result<Self, EnvError> {
        Ok(Self {
            listen_address: env::parse_or(LISTEN_ADDRESS_ENV, DEFAULT_LISTEN_ADDRESS)?,
            database_url: env::string_or(DATABASE_URL_ENV, DEFAULT_DATABASE_URL)?,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_address: DEFAULT_LISTEN_ADDRESS,
            database_url: DEFAULT_DATABASE_URL.to_owned(),
        }
    }
}
