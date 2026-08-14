use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};

use pglite_oxide::PgliteServer;

use super::store::{Clock, PostgresJobStore, SystemClock};

/// Opens an isolated PGlite-backed job store for tests.
pub async fn open_isolated() -> PostgresJobStore {
    open_isolated_with_clock(Arc::new(SystemClock)).await
}

/// Opens an isolated PGlite-backed job store with a caller-supplied clock.
pub(crate) async fn open_isolated_with_clock(clock: Arc<dyn Clock>) -> PostgresJobStore {
    static SCHEMA: AtomicU64 = AtomicU64::new(1);
    let schema = format!("job_store_{}", SCHEMA.fetch_add(1, Ordering::Relaxed));
    PostgresJobStore::open_with_clock(&pglite_url(), clock, &schema)
        .await
        .expect("failed to open isolated postgres job store")
}

fn pglite_url() -> String {
    static SERVER: OnceLock<PgliteServer> = OnceLock::new();
    SERVER
        .get_or_init(|| PgliteServer::temporary_tcp().expect("failed to start pglite-oxide"))
        .database_url()
}
