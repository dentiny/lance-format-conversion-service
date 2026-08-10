use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use lance_conversion_core::job::{ClaimedJob, Job, LeaseUpdate, NewJob, ProgressUpdate};

/// Persists jobs and coordinates lease-based conversion execution.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Persists and returns a new job in the queuing state with attempt zero,
    /// no lease, empty error history, and zero progress.
    async fn create_job(&self, job: NewJob) -> Result<Job, StoreError>;

    /// Returns a job by its unique identifier, or [`StoreError::NotFound`] when
    /// no matching row exists.
    async fn get_job(&self, id: Uuid) -> Result<Job, StoreError>;

    /// Returns at most `limit` jobs ordered by creation time from newest to
    /// oldest, with job ID descending as the deterministic tie-breaker.
    ///
    /// A zero limit returns an empty list. This method always starts from the
    /// newest job and does not provide pagination.
    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>, StoreError>;

    /// Atomically claims at most `limit` queuing or lease-expired running jobs.
    ///
    /// Jobs are claimed from oldest to newest, with job ID as the deterministic
    /// tie-breaker. Claiming sets the status to running, increments the attempt,
    /// and sets the lease expiration relative to the store's current time.
    /// A zero limit or non-positive lease duration returns an empty list.
    async fn claim_jobs(
        &self,
        limit: usize,
        lease_duration_ms: i64,
    ) -> Result<Vec<ClaimedJob>, StoreError>;

    /// Extends an unexpired running job's lease and updates its progress
    /// snapshot.
    ///
    /// The update succeeds only when its attempt matches the current attempt.
    /// A stale attempt or expired lease returns [`StoreError::LeaseLost`].
    async fn renew_lease(&self, update: LeaseUpdate) -> Result<Job, StoreError>;

    /// Updates an unexpired running job's progress without extending its lease.
    ///
    /// The update succeeds only when its attempt matches the current attempt.
    /// A stale attempt or expired lease returns [`StoreError::LeaseLost`].
    async fn checkpoint_progress(&self, update: ProgressUpdate) -> Result<Job, StoreError>;
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("record not found")]
    NotFound,
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
