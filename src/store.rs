use std::{future::Future, pin::Pin};

use thiserror::Error;
use uuid::Uuid;

use crate::domain::{
    ClaimedJob, CompletedInspection, Inspection, Job, LeaseUpdate, NewInspection, NewJob,
    ProgressUpdate,
};

pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

pub trait JobStore: Send + Sync {
    fn create_inspection(&self, inspection: NewInspection) -> StoreFuture<'_, Inspection>;

    fn complete_inspection(&self, inspection: CompletedInspection) -> StoreFuture<'_, Inspection>;

    fn get_inspection(&self, id: Uuid) -> StoreFuture<'_, Inspection>;

    fn create_job(&self, job: NewJob) -> StoreFuture<'_, Job>;

    fn get_job(&self, id: Uuid) -> StoreFuture<'_, Job>;

    fn list_jobs(&self, limit: usize) -> StoreFuture<'_, Vec<Job>>;

    fn claim_jobs(
        &self,
        owner: String,
        limit: usize,
        lease_duration_ms: i64,
    ) -> StoreFuture<'_, Vec<ClaimedJob>>;

    fn renew_lease(&self, update: LeaseUpdate) -> StoreFuture<'_, Job>;

    fn checkpoint_progress(&self, update: ProgressUpdate) -> StoreFuture<'_, Job>;
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("record not found")]
    NotFound,
    #[error("inspection has not completed successfully")]
    InspectionNotReady,
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
