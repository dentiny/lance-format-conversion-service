use std::{error::Error, sync::Arc};

use clap::Parser;
use config::Config;
use lance_converter::Converter;
use lance_job_store_factory::connect;
use reconciler::run;

mod config;
#[cfg(unix)]
mod pprof;
mod reconciler;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    config.validate()?;

    #[cfg(unix)]
    if let Some(addr) = config.pprof_listen_address {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        eprintln!("pprof listening on {addr}");
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, pprof::routes()).await {
                eprintln!("pprof server failed: {error}");
            }
        });
    }

    let store = connect(&config.database_url, config.database_max_connections.get()).await?;
    let converter = Arc::new(Converter::new(config.converter_config())?);
    run(store, converter, config).await?;
    Ok(())
}
