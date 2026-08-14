use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;

use clap::Parser;

const DEFAULT_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
const DEFAULT_DATABASE_URL: &str = "postgres://127.0.0.1:5432/lance_jobs";
const DEFAULT_DATABASE_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(8).unwrap();

/// HTTP control plane for Lance format conversion.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(name = "lance-web", version = env!("CARGO_PKG_VERSION"), about)]
pub struct Config {
    /// Address the HTTP server binds to.
    #[arg(long, default_value_t = DEFAULT_LISTEN_ADDRESS)]
    pub listen_address: SocketAddr,
    /// Job-store database URL.
    #[arg(long, default_value = DEFAULT_DATABASE_URL)]
    pub database_url: String,
    /// `PostgreSQL` connection pool size. Ignored for `SQLite`.
    #[arg(
        long,
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        help = "PostgreSQL connection pool size. Ignored for SQLite."
    )]
    pub database_max_connections: NonZeroU32,
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

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn parses_cli_defaults() {
        let config = Config::try_parse_from(["lance-web"]).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn parses_cli_overrides() {
        let config = Config::try_parse_from([
            "lance-web",
            "--listen-address",
            "0.0.0.0:9090",
            "--database-url",
            "sqlite://./data/service.db",
            "--database-max-connections",
            "4",
        ])
        .unwrap();
        assert_eq!(
            config.listen_address,
            "0.0.0.0:9090".parse::<std::net::SocketAddr>().unwrap()
        );
        assert_eq!(config.database_url, "sqlite://./data/service.db");
        assert_eq!(config.database_max_connections.get(), 4);
    }
}
