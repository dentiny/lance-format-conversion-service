use std::sync::{Arc, OnceLock};

use pglite_oxide::PgliteServer;

use lance_job_store::{Clock, SystemClock};

use super::store::PostgresJobStore;

/// Opens an isolated PGlite-backed job store for tests.
///
/// # Panics
///
/// Panics if `PGlite` cannot start or the store cannot be opened.
pub async fn open_isolated() -> PostgresJobStore {
    open_isolated_with_clock(Arc::new(SystemClock)).await
}

/// Opens an isolated PGlite-backed job store with a caller-supplied clock.
///
/// # Panics
///
/// Panics if `PGlite` cannot start or the store cannot be opened.
pub async fn open_isolated_with_clock(clock: Arc<dyn Clock>) -> PostgresJobStore {
    PostgresJobStore::open_with_clock(&pglite_url(), clock)
        .await
        .expect("failed to open isolated postgres job store")
}

fn pglite_url() -> String {
    static SERVER: OnceLock<PgliteServer> = OnceLock::new();
    SERVER
        .get_or_init(|| PgliteServer::temporary_tcp().expect("failed to start pglite-oxide"))
        .database_url()
}
