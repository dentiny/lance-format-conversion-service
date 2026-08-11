use std::{error::Error, sync::Arc};

use clap::Parser;
use config::Config;
use lance_converter::{Converter, ConverterConfig};
use lance_job_store_factory::connect;
use reconciler::run;

mod config;
mod reconciler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    config.validate()?;

    let store = connect(&config.database_url).await?;
    let converter = Arc::new(Converter::new(ConverterConfig {
        target_lance_file_size_mib: config.target_lance_file_size_mib.get(),
        blob_inline_threshold_mib: config.blob_inline_threshold_mib.get(),
        blob_dedicated_threshold_mib: config.blob_dedicated_threshold_mib.get(),
    }));
    run(store, converter, config).await?;
    Ok(())
}
