use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use lance_conversion_core::job::{
    BlobColumnSpec, CompletionUpdate, FailureUpdate, IndexSpec, IndexType, JobKind, JobProgress,
    JobStatus, LeaseUpdate, MAX_JOB_ATTEMPTS, ProgressUpdate,
};
use lance_job_store::{JobOrderField, JobQuery, JobStore, StoreError};
use lance_test_support::new_job;

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

#[tokio::test]
async fn created_job_can_be_listed() {
    let store = SqliteJobStore::open(":memory:").await.unwrap();
    let destination_uri = "s3://destination-bucket/data.lance";
    store
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
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
async fn jobs_can_be_filtered_by_creator_and_creation_timestamp() {
    const QUERY_LIMIT: usize = 10;
    const LOWER_BOUND_MS: i64 = 15;
    const UPPER_BOUND_MS: i64 = 35;

    let store = SqliteJobStore::open_with_clock(":memory:", Arc::new(TestClock::new(100)))
        .await
        .unwrap();
    for (creator, timestamp, destination) in [
        ("alice", 10, "/destinations/alice-old.lance"),
        ("bob", 20, "/destinations/bob.lance"),
        ("alice", 30, "/destinations/alice-new.lance"),
    ] {
        store
            .create_job(new_job(creator, "/datasets/source", destination, timestamp).unwrap())
            .await
            .unwrap();
    }

    let jobs = store
        .query_jobs(JobQuery {
            creator: Some("alice".to_owned()),
            failed_only: false,
            ongoing_only: false,
            creation_timestamp_ms_from: Some(LOWER_BOUND_MS),
            creation_timestamp_ms_to: Some(UPPER_BOUND_MS),
            order_by: JobOrderField::CreationTimestamp,
            descending: true,
            limit: QUERY_LIMIT,
        })
        .await
        .unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].creator, "alice");
    assert_eq!(jobs[0].creation_timestamp_ms, 30);

    let oldest_first = store
        .query_jobs(JobQuery {
            creator: None,
            failed_only: false,
            ongoing_only: false,
            creation_timestamp_ms_from: None,
            creation_timestamp_ms_to: None,
            order_by: JobOrderField::CreationTimestamp,
            descending: false,
            limit: QUERY_LIMIT,
        })
        .await
        .unwrap();
    assert_eq!(
        oldest_first
            .iter()
            .map(|job| job.creation_timestamp_ms)
            .collect::<Vec<_>>(),
        [10, 20, 30]
    );

    store.claim_jobs(1, 100).await.unwrap();
    let recently_updated = store
        .query_jobs(JobQuery {
            creator: None,
            failed_only: false,
            ongoing_only: false,
            creation_timestamp_ms_from: None,
            creation_timestamp_ms_to: None,
            order_by: JobOrderField::UpdateTimestamp,
            descending: true,
            limit: QUERY_LIMIT,
        })
        .await
        .unwrap();
    assert_eq!(
        recently_updated[0].destination_uri,
        "/destinations/alice-old.lance"
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
    let mut job = new_job("test-user", "/datasets/source", destination_uri, 3).unwrap();
    job.blob_columns.clone_from(&blob_columns);
    job.indices.clone_from(&indices);

    store.create_job(job).await.unwrap();

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
        .create_job(
            new_job(
                "test-user",
                "s3://source-bucket/data",
                "s3://destination-bucket/data.lance",
                3,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let first_claim = store.claim_jobs(1, 100).await.unwrap();
    assert_eq!(first_claim.len(), 1);
    let destination_uri = first_claim[0].destination_uri.clone();
    assert_eq!(first_claim[0].attempt, 1);
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
    let mut job = new_job(
        "test-user",
        "/datasets/source",
        "s3://destination-bucket/data.lance",
        3,
    )
    .unwrap();
    job.kind = JobKind::Move;
    store.create_job(job).await.unwrap();
    let claim = store.claim_jobs(1, 1_000).await.unwrap().remove(0);
    let destination_uri = claim.destination_uri.clone();

    let progress = JobProgress {
        rows_read: 10,
        rows_written: 10,
        rows_total: 20,
    };
    clock.set(20);
    store
        .checkpoint_progress(ProgressUpdate {
            destination_uri: destination_uri.clone(),
            attempt: claim.attempt,
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
        .create_job(
            new_job(
                "test-user",
                "/datasets/source",
                "s3://destination-bucket/data.lance",
                3,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
    let destination_uri = claim.destination_uri.clone();

    clock.set(20);
    store
        .renew_lease(LeaseUpdate {
            destination_uri: destination_uri.clone(),
            attempt: claim.attempt,
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
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
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
            attempt: claim.attempt,
            progress,
        })
        .await
        .unwrap();

    let job = store.get_job(destination_uri).await.unwrap();
    assert_eq!(job.status, JobStatus::Succeeded);
    assert_eq!(job.lease_expiration_timestamp_ms, None);
    assert_eq!(job.progress, progress);
    let ongoing_jobs = store
        .query_jobs(JobQuery {
            creator: None,
            failed_only: false,
            ongoing_only: true,
            creation_timestamp_ms_from: None,
            creation_timestamp_ms_to: None,
            order_by: JobOrderField::CreationTimestamp,
            descending: true,
            limit: 1,
        })
        .await
        .unwrap();
    assert!(ongoing_jobs.is_empty());
}

#[tokio::test]
async fn failures_retry_until_attempt_cap() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock)
        .await
        .unwrap();
    let destination_uri = "s3://destination-bucket/failed.lance";
    store
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
        .await
        .unwrap();

    for attempt in 1..=MAX_JOB_ATTEMPTS {
        let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
        assert_eq!(claim.attempt, attempt);
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
    let failed_jobs = store
        .query_jobs(JobQuery {
            creator: Some("test-user".to_owned()),
            failed_only: true,
            ongoing_only: false,
            creation_timestamp_ms_from: None,
            creation_timestamp_ms_to: None,
            order_by: JobOrderField::CreationTimestamp,
            descending: true,
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(failed_jobs.len(), 1);
    assert_eq!(failed_jobs[0].destination_uri, destination_uri);
}

#[tokio::test]
async fn final_expired_attempt_becomes_failed() {
    let clock = Arc::new(TestClock::new(10));
    let store = SqliteJobStore::open_with_clock(":memory:", clock.clone())
        .await
        .unwrap();
    let destination_uri = "s3://destination-bucket/expired.lance";
    store
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
        .await
        .unwrap();

    for attempt in 1..MAX_JOB_ATTEMPTS {
        let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
        store
            .fail_job(FailureUpdate {
                destination_uri: destination_uri.to_owned(),
                attempt: claim.attempt,
                progress: JobProgress::default(),
                reason: format!("failure {attempt}"),
            })
            .await
            .unwrap();
    }
    let final_claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
    assert_eq!(final_claim.attempt, MAX_JOB_ATTEMPTS);
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
