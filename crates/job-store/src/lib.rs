use std::{future::Future, pin::Pin};

use thiserror::Error;
use uuid::Uuid;

use lance_conversion_core::job::{ClaimedJob, Job, LeaseUpdate, NewJob, ProgressUpdate};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

/// Persists jobs and coordinates lease-based conversion execution.
pub trait JobStore: Send + Sync {
    /// Creates and returns a queued job.
    fn create_job(&self, job: NewJob) -> StoreFuture<'_, Job>;

    /// Returns a job by its unique identifier.
    fn get_job(&self, id: Uuid) -> StoreFuture<'_, Job>;

    /// Returns up to `limit` jobs, ordered from newest to oldest.
    fn list_jobs(&self, limit: usize) -> StoreFuture<'_, Vec<Job>>;

    /// Atomically claims queued or expired jobs and assigns each a new attempt and lease.
    fn claim_jobs(&self, limit: usize, lease_duration_ms: i64) -> StoreFuture<'_, Vec<ClaimedJob>>;

    /// Extends a current lease and persists monotonic progress for its attempt.
    fn renew_lease(&self, update: LeaseUpdate) -> StoreFuture<'_, Job>;

    /// Persists monotonic progress without extending the current lease.
    fn checkpoint_progress(&self, update: ProgressUpdate) -> StoreFuture<'_, Job>;
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("record not found")]
    NotFound,
    #[error("move jobs are supported only for NFS and S3 sources")]
    UnsupportedMoveSource,
    #[error("job lease has expired or belongs to another worker")]
    LeaseLost,
    #[error("invalid store input: {0}")]
    InvalidInput(String),
    #[error("job conflicts with existing state: {0}")]
    Conflict(String),
    #[error("database operation failed: {0}")]
    Database(String),
    #[error("database worker failed: {0}")]
    Worker(String),
}
