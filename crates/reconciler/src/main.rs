use std::error::Error;

use clap::Parser;
use config::Config;
use lance_job_store_sqlite::SqliteJobStore;
use tracing::info;

mod config;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();
    config.validate()?;
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

    let _store = SqliteJobStore::open(&config.database_path)?;
    info!(
        database = %config.database_path.display(),
        worker_count = config.worker_count.get(),
        poll_interval_ms = config.poll_interval_ms.get(),
        lease_duration_secs = config.lease_duration_secs.get(),
        lease_renew_interval_secs = config.lease_renew_interval_secs.get(),
        progress_interval_secs = config.progress_interval_secs.get(),
        target_lance_file_size_mib = config.target_lance_file_size_mib.get(),
        blob_inline_threshold_mib = config.blob_inline_threshold_mib.get(),
        "reconciler initialized; conversion execution starts in Milestone 1; no jobs will be claimed"
    );

    tokio::signal::ctrl_c().await?;
    info!("reconciler shutting down");
    Ok(())
}
