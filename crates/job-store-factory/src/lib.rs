use std::sync::Arc;

use thiserror::Error;

use lance_job_store::{JobStore, StoreError};
use lance_job_store_sqlite::SqliteJobStore;

/// Opens the job-store backend selected by `database_url`.
///
/// `SQLite` URLs use `sqlite://<path>`. Additional backend schemes can be added
/// here without coupling deployable binaries to concrete store implementations.
///
/// # Errors
///
/// Returns an error when the URL is malformed, the backend is unsupported, or
/// the selected store cannot be opened.
pub async fn connect(database_url: &str) -> Result<Arc<dyn JobStore>, StoreFactoryError> {
    if let Some(path) = database_url.strip_prefix("sqlite://") {
        if path.is_empty() {
            return Err(StoreFactoryError::InvalidDatabaseUrl);
        }
        return Ok(Arc::new(SqliteJobStore::open(path).await?));
    }

    let scheme = database_url
        .split_once("://")
        .map_or(database_url, |(scheme, _)| scheme);
    Err(StoreFactoryError::UnsupportedBackend(scheme.to_owned()))
}

#[derive(Debug, Error)]
pub enum StoreFactoryError {
    #[error("database URL must include a backend scheme and location")]
    InvalidDatabaseUrl,
    #[error("unsupported database backend: {0}")]
    UnsupportedBackend(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}
