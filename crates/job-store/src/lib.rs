use async_trait::async_trait;
use thiserror::Error;
use uuid::Uuid;

use lance_conversion_core::job::{ClaimedJob, Job, LeaseUpdate, NewJob, ProgressUpdate};

/// Persists jobs and coordinates lease-based conversion execution.
#[async_trait]
pub trait JobStore: Send + Sync {
    /// Creates and returns a job in the queuing state.
    async fn create_job(&self, job: NewJob) -> Result<Job, StoreError>;

    /// Returns a job by its unique identifier.
    async fn get_job(&self, id: Uuid) -> Result<Job, StoreError>;

    /// Returns at most `limit` jobs ordered by creation time from newest to
    /// oldest, with job ID descending as the deterministic tie-breaker.
    ///
    /// A zero limit returns an empty list. This method always starts from the
    /// newest job and does not provide pagination.
    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>, StoreError>;

    /// Atomically claims queuing or expired jobs and assigns each a new attempt and lease.
    async fn claim_jobs(
        &self,
        limit: usize,
        lease_duration_ms: i64,
    ) -> Result<Vec<ClaimedJob>, StoreError>;

    /// Extends a current lease and updates its latest progress snapshot.
    async fn renew_lease(&self, update: LeaseUpdate) -> Result<Job, StoreError>;

    /// Updates the latest progress snapshot without extending the current lease.
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
