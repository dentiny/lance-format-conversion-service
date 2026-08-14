use std::sync::Arc;

use thiserror::Error;

use lance_job_store::{JobStore, StoreError};
#[cfg(feature = "postgres")]
use lance_job_store_postgres::PostgresJobStore;
#[cfg(feature = "sqlite")]
use lance_job_store_sqlite::SqliteJobStore;

#[cfg(not(any(feature = "postgres", feature = "sqlite")))]
compile_error!("enable at least one of the `postgres` or `sqlite` features");

/// Opens the job-store backend selected by `database_url`.
///
/// `max_connections` sizes the `PostgreSQL` pool. `SQLite` always uses one
/// connection.
///
/// # Errors
///
/// Returns an error when the URL is malformed, `max_connections` is 0, the
/// backend is unsupported, or the selected store cannot be opened.
pub async fn connect(
    database_url: &str,
    max_connections: u32,
) -> Result<Arc<dyn JobStore>, StoreFactoryError> {
    let (backend, location) = database_url
        .split_once("://")
        .ok_or(StoreFactoryError::InvalidDatabaseUrl)?;
    if location.is_empty() {
        return Err(StoreFactoryError::InvalidDatabaseUrl);
    }
    if max_connections == 0 {
        return Err(StoreFactoryError::InvalidMaxConnections);
    }

    match backend {
        #[cfg(feature = "sqlite")]
        "sqlite" => Ok(Arc::new(SqliteJobStore::open(location).await?)),
        #[cfg(feature = "postgres")]
        "postgres" => Ok(Arc::new(
            PostgresJobStore::open(database_url, max_connections).await?,
        )),
        _ => Err(StoreFactoryError::UnsupportedBackend(backend.to_owned())),
    }
}

#[derive(Debug, Error)]
pub enum StoreFactoryError {
    #[error("database URL must include a backend scheme and location")]
    InvalidDatabaseUrl,
    #[error("database max connections must be at least 1")]
    InvalidMaxConnections,
    #[error("unsupported database backend: {0}")]
    UnsupportedBackend(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[cfg(test)]
mod tests;
