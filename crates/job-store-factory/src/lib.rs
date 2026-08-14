use std::sync::Arc;

use thiserror::Error;

use lance_job_store::{JobStore, StoreError};
use lance_job_store_sqlite::SqliteJobStore;

/// Opens the job-store backend selected by `database_url`.
///
/// # Errors
///
/// Returns an error when the URL is malformed, the backend is unsupported, or
/// the selected store cannot be opened.
pub async fn connect(database_url: &str) -> Result<Arc<dyn JobStore>, StoreFactoryError> {
    let (backend, location) = database_url
        .split_once("://")
        .ok_or(StoreFactoryError::InvalidDatabaseUrl)?;
    if location.is_empty() {
        return Err(StoreFactoryError::InvalidDatabaseUrl);
    }

    match backend {
        "sqlite" => Ok(Arc::new(SqliteJobStore::open(location).await?)),
        _ => Err(StoreFactoryError::UnsupportedBackend(backend.to_owned())),
    }
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
