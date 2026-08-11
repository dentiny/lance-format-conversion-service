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
