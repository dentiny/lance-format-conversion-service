use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;

use lance_conversion_core::env::{self, EnvError};

const LISTEN_ADDRESS_ENV: &str = "LISTEN_ADDRESS";
const DATABASE_URL_ENV: &str = "DATABASE_URL";
const DATABASE_MAX_CONNECTIONS_ENV: &str = "DATABASE_MAX_CONNECTIONS";
const DEFAULT_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
const DEFAULT_DATABASE_URL: &str = "postgres://127.0.0.1:5432/lance_jobs";
const DEFAULT_DATABASE_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(8).unwrap();

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub database_url: String,
    pub database_max_connections: NonZeroU32,
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
            database_max_connections: env::parse_or(
                DATABASE_MAX_CONNECTIONS_ENV,
                DEFAULT_DATABASE_MAX_CONNECTIONS,
            )?,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_address: DEFAULT_LISTEN_ADDRESS,
            database_url: DEFAULT_DATABASE_URL.to_owned(),
            database_max_connections: DEFAULT_DATABASE_MAX_CONNECTIONS,
        }
    }
}
