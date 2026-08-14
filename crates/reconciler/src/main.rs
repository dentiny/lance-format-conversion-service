use std::{error::Error, sync::Arc};

use clap::Parser;
use config::Config;
use lance_converter::Converter;
use lance_job_store_factory::connect;
use reconciler::run;

mod config;
mod reconciler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    config.validate()?;

    let store = connect(&config.database_url, config.database_max_connections.get()).await?;
    let converter = Arc::new(Converter::new(config.converter_config())?);
    run(store, converter, config).await?;
    Ok(())
}
