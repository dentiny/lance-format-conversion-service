use std::{error::Error, sync::Arc};

use clap::Parser;
use lance_job_store_sqlite::SqliteJobStore;
use lance_web::{config::Config, router};
use tracing::info;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::from(config.log_level))
        .with_target(false)
        .compact()
        .init();

    if let Some(parent) = config.database_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    let store = Arc::new(SqliteJobStore::open(&config.database_path)?);
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;
    info!(
        address = %config.listen_address,
        database = %config.database_path.display(),
        "web service started"
    );

    axum::serve(listener, router(store))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal handler");
    }
}
