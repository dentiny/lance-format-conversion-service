use std::net::SocketAddr;

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen_address: SocketAddr,

    #[arg(long, default_value = "sqlite://./data/service.db")]
    pub database_url: String,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn defaults_match_web_contract() {
        let config = Config::parse_from(["lance-web"]);
        assert_eq!(config.listen_address.to_string(), "127.0.0.1:8080");
        assert_eq!(config.database_url, "sqlite://./data/service.db");
    }
}
