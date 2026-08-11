use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use lance_conversion_core::{
    job::{
        BlobColumnSpec, CompletionUpdate, FailureUpdate, IndexSpec, IndexType, JobKind,
        JobProgress, JobStatus, LeaseUpdate, MAX_JOB_ATTEMPTS, NewJob, ProgressUpdate,
    },
    location::DatasetLocation,
};
use lance_job_store::{JobStore, StoreError};

use super::store::{Clock, SqliteJobStore};

struct TestClock(AtomicI64);

impl TestClock {
    fn new(now_ms: i64) -> Self {
        Self(AtomicI64::new(now_ms))
    }

    fn set(&self, now_ms: i64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> Result<i64, StoreError> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

fn source(source_uri: &str) -> DatasetLocation {
    DatasetLocation::parse_location(source_uri).unwrap()
}

#[tokio::test]
async fn created_job_can_be_listed() {
    let store = SqliteJobStore::open(":memory:").await.unwrap();
    let destination_uri = "s3://destination-bucket/data.lance";
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location(destination_uri).unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();

    let jobs = store.list_jobs(10).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].destination_uri, destination_uri);
    assert_eq!(jobs[0].status, JobStatus::Queuing);
    assert_eq!(
        store
            .get_job(destination_uri)
            .await
            .unwrap()
            .destination_uri,
        destination_uri
    );
}

#[tokio::test]
async fn blob_and_index_specs_round_trip_through_store() {
    let store = SqliteJobStore::open(":memory:").await.unwrap();
    let destination_uri = "s3://destination-bucket/specs.lance";
    let blob_columns = vec![BlobColumnSpec {
        column: "image".to_owned(),
    }];
    let indices = vec![
        IndexSpec {
            columns: vec!["category".to_owned()],
            index_type: IndexType::Bitmap,
        },
        IndexSpec {
            columns: vec!["location".to_owned()],
            index_type: IndexType::RTree,
        },
    ];

    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location(destination_uri).unwrap(),
            blob_columns: blob_columns.clone(),
            indices: indices.clone(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();

    let job = store.get_job(destination_uri).await.unwrap();
    assert_eq!(job.blob_columns, blob_columns);
    assert_eq!(job.indices, indices);
}

#[tokio::test]
async fn claiming_a_job_sets_its_status_to_running() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock.clone())
        .await
        .unwrap();
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("s3://source-bucket/data"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location("s3://destination-bucket/data.lance")
                .unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();

    let first_claim = store.claim_jobs(1, 100).await.unwrap();
    assert_eq!(first_claim.len(), 1);
    let destination_uri = first_claim[0].job.destination_uri.clone();
    assert_eq!(first_claim[0].job.attempt, 1);
    assert_eq!(
        store.get_job(&destination_uri).await.unwrap().status,
        JobStatus::Running
    );
}

#[tokio::test]
async fn updating_progress_keeps_job_running() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock.clone())
        .await
        .unwrap();
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Move,
            destination: DatasetLocation::parse_location("s3://destination-bucket/data.lance")
                .unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();
    let claim = store.claim_jobs(1, 1_000).await.unwrap().remove(0);
    let destination_uri = claim.job.destination_uri.clone();

    let progress = JobProgress {
        rows_read: 10,
        rows_written: 10,
        rows_total: 20,
    };
    clock.set(20);
    store
        .checkpoint_progress(ProgressUpdate {
            destination_uri: destination_uri.clone(),
            attempt: claim.job.attempt,
            progress,
        })
        .await
        .unwrap();

    let job = store.get_job(&destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.progress, progress);
}

#[tokio::test]
async fn updating_lease_keeps_job_running() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock.clone())
        .await
        .unwrap();
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location("s3://destination-bucket/data.lance")
                .unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();
    let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
    let destination_uri = claim.job.destination_uri.clone();

    clock.set(20);
    store
        .renew_lease(LeaseUpdate {
            destination_uri: destination_uri.clone(),
            attempt: claim.job.attempt,
            convert_lease_duration_ms: 200,
            progress: JobProgress::default(),
        })
        .await
        .unwrap();

    let job = store.get_job(&destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.lease_expiration_timestamp_ms, Some(220));
}

#[tokio::test]
async fn completing_a_job_clears_its_lease() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock)
        .await
        .unwrap();
    let destination_uri = "s3://destination-bucket/completed.lance";
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location(destination_uri).unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();
    let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
    let progress = JobProgress {
        rows_read: 3,
        rows_written: 3,
        rows_total: 3,
    };

    store
        .complete_job(CompletionUpdate {
            destination_uri: destination_uri.to_owned(),
            attempt: claim.job.attempt,
            progress,
        })
        .await
        .unwrap();

    let job = store.get_job(destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Succeeded);
    assert_eq!(job.lease_expiration_timestamp_ms, None);
    assert_eq!(job.progress, progress);
}

#[tokio::test]
async fn failures_retry_until_attempt_cap() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock)
        .await
        .unwrap();
    let destination_uri = "s3://destination-bucket/failed.lance";
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location(destination_uri).unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();

    for attempt in 1..=MAX_JOB_ATTEMPTS {
        let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
        assert_eq!(claim.job.attempt, attempt);
        store
            .fail_job(FailureUpdate {
                destination_uri: destination_uri.to_owned(),
                attempt,
                progress: JobProgress::default(),
                reason: format!("failure {attempt}"),
            })
            .await
            .unwrap();
    }

    let job = store.get_job(destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(job.error_reasons.len(), MAX_JOB_ATTEMPTS as usize);
    assert!(store.claim_jobs(1, 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn final_expired_attempt_becomes_failed() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock.clone())
        .await
        .unwrap();
    let destination_uri = "s3://destination-bucket/expired.lance";
    store
        .create_job(NewJob {
            creator: "test-user".to_owned(),
            source: source("/datasets/source"),
            kind: JobKind::Copy,
            destination: DatasetLocation::parse_location(destination_uri).unwrap(),
            blob_columns: Vec::new(),
            indices: Vec::new(),
            creation_timestamp_ms: 3,
        })
        .await
        .unwrap();

    for attempt in 1..MAX_JOB_ATTEMPTS {
        let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
        store
            .fail_job(FailureUpdate {
                destination_uri: destination_uri.to_owned(),
                attempt: claim.job.attempt,
                progress: JobProgress::default(),
                reason: format!("failure {attempt}"),
            })
            .await
            .unwrap();
    }
    let final_claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
    assert_eq!(final_claim.job.attempt, MAX_JOB_ATTEMPTS);
    clock.set(110);

    assert!(store.claim_jobs(1, 100).await.unwrap().is_empty());
    let job = store.get_job(destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(job.error_reasons.len(), MAX_JOB_ATTEMPTS as usize);
    assert_eq!(
        job.error_reasons.last().unwrap().reason,
        "lease expired on final attempt"
    );
}
