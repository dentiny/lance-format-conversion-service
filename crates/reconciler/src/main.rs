use std::error::Error;

use clap::Parser;
use config::Config;
use lance_job_store_factory::connect;

mod config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    config.validate()?;

    let _store = connect(&config.database_url).await?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
