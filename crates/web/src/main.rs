use std::error::Error;

use clap::Parser;
use lance_job_store_factory::connect;
use lance_web::{config::Config, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse();

    let store = connect(&config.database_url).await?;
    let listener = tokio::net::TcpListener::bind(config.listen_address).await?;

    axum::serve(listener, router(store))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
