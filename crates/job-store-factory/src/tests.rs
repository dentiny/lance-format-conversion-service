use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use lance_conversion_core::job::{
    BlobColumnSpec, CompletionUpdate, FailureUpdate, IndexSpec, IndexType, Job, JobProgress,
    JobStatus, LeaseUpdate, MAX_JOB_ATTEMPTS, ProgressUpdate,
};
use lance_job_store::{Clock, JobOrderField, JobQuery, JobStore, StoreError};
use lance_job_store_postgres::test_utils::{open_isolated, open_isolated_with_clock};
use lance_job_store_sqlite::SqliteJobStore;
use lance_test_support::new_job;
use serial_test::serial;

const BACKENDS: [&str; 2] = ["sqlite", "postgres"];

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

async fn test_stores() -> [(&'static str, Arc<dyn JobStore>); 2] {
    [
        (
            "sqlite",
            Arc::new(SqliteJobStore::open(":memory:").await.unwrap()),
        ),
        ("postgres", Arc::new(open_isolated().await)),
    ]
}

async fn store_with_clock(backend: &str, clock: Arc<dyn Clock>) -> Arc<dyn JobStore> {
    match backend {
        "sqlite" => Arc::new(
            SqliteJobStore::open_with_clock(":memory:", clock)
                .await
                .unwrap(),
        ),
        "postgres" => Arc::new(open_isolated_with_clock(clock).await),
        _ => unreachable!("unknown job-store backend {backend}"),
    }
}

async fn job(store: &dyn JobStore, destination_uri: &str) -> Job {
    store
        .get_job(destination_uri)
        .await
        .unwrap_or_else(|error| panic!("missing job {destination_uri}: {error}"))
}

#[tokio::test]
#[serial]
async fn created_job_can_be_listed() {
    for (backend, store) in test_stores().await {
        created_job_can_be_listed_impl(backend, store).await;
    }
}

async fn created_job_can_be_listed_impl(backend: &str, store: Arc<dyn JobStore>) {
    let destination_uri = "s3://destination-bucket/data.lance";
    store
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
        .await
        .unwrap();

    let jobs = store.list_jobs(10).await.unwrap();
    assert_eq!(jobs.len(), 1, "{backend}");
    assert_eq!(jobs[0].destination_uri, destination_uri, "{backend}");
    assert_eq!(jobs[0].status, JobStatus::Queuing, "{backend}");
    assert_eq!(
        job(store.as_ref(), destination_uri).await.destination_uri,
        destination_uri,
        "{backend}"
    );
}

#[tokio::test]
#[serial]
async fn missing_job_returns_not_found() {
    for (backend, store) in test_stores().await {
        missing_job_returns_not_found_impl(backend, store).await;
    }
}

async fn missing_job_returns_not_found_impl(backend: &str, store: Arc<dyn JobStore>) {
    let error = store
        .get_job("s3://destination-bucket/missing.lance")
        .await
        .expect_err("missing job should not be found");
    assert!(
        matches!(error, StoreError::NotFound),
        "{backend}: {error:?}"
    );
}

#[tokio::test]
#[serial]
async fn jobs_can_be_filtered_by_creator_and_creation_timestamp() {
    for backend in BACKENDS {
        let store = store_with_clock(backend, Arc::new(TestClock::new(100))).await;
        jobs_can_be_filtered_by_creator_and_creation_timestamp_impl(backend, store).await;
    }
}

async fn jobs_can_be_filtered_by_creator_and_creation_timestamp_impl(
    backend: &str,
    store: Arc<dyn JobStore>,
) {
    const QUERY_LIMIT: usize = 10;
    const LOWER_BOUND_MS: i64 = 15;
    const UPPER_BOUND_MS: i64 = 35;

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

    assert_eq!(jobs.len(), 1, "{backend}");
    assert_eq!(jobs[0].creator, "alice", "{backend}");
    assert_eq!(jobs[0].creation_timestamp_ms, 30, "{backend}");

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
        [10, 20, 30],
        "{backend}"
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
        recently_updated[0].destination_uri, "/destinations/alice-old.lance",
        "{backend}"
    );
}

#[tokio::test]
#[serial]
async fn blob_and_index_specs_round_trip_through_store() {
    for (backend, store) in test_stores().await {
        blob_and_index_specs_round_trip_through_store_impl(backend, store).await;
    }
}

async fn blob_and_index_specs_round_trip_through_store_impl(
    backend: &str,
    store: Arc<dyn JobStore>,
) {
    let destination_uri = "s3://destination-bucket/specs.lance";
    let blob_columns = vec![BlobColumnSpec {
        column: "image".to_owned(),
    }];
    let indices = vec![
        IndexSpec {
            column: "category".to_owned(),
            index_type: IndexType::Scalar,
        },
        IndexSpec {
            column: "description".to_owned(),
            index_type: IndexType::Text,
        },
    ];
    let mut created = new_job("test-user", "/datasets/source", destination_uri, 3).unwrap();
    created.blob_columns.clone_from(&blob_columns);
    created.indices.clone_from(&indices);

    store.create_job(created).await.unwrap();

    let stored = job(store.as_ref(), destination_uri).await;
    assert_eq!(stored.blob_columns, blob_columns, "{backend}");
    assert_eq!(stored.indices, indices, "{backend}");
}

#[tokio::test]
#[serial]
async fn claiming_a_job_sets_its_status_to_running() {
    for backend in BACKENDS {
        let store = store_with_clock(backend, Arc::new(TestClock::new(10))).await;
        claiming_a_job_sets_its_status_to_running_impl(backend, store).await;
    }
}

async fn claiming_a_job_sets_its_status_to_running_impl(backend: &str, store: Arc<dyn JobStore>) {
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
    assert_eq!(first_claim.len(), 1, "{backend}");
    let destination_uri = first_claim[0].destination_uri.clone();
    assert_eq!(first_claim[0].attempt, 1, "{backend}");
    assert_eq!(
        job(store.as_ref(), &destination_uri).await.status,
        JobStatus::Running,
        "{backend}"
    );
}

#[tokio::test]
#[serial]
async fn updating_progress_keeps_job_running() {
    for backend in BACKENDS {
        let clock = Arc::new(TestClock::new(10));
        let store = store_with_clock(backend, Arc::clone(&clock) as Arc<dyn Clock>).await;
        updating_progress_keeps_job_running_impl(backend, store, clock).await;
    }
}

async fn updating_progress_keeps_job_running_impl(
    backend: &str,
    store: Arc<dyn JobStore>,
    clock: Arc<TestClock>,
) {
    let created = new_job(
        "test-user",
        "/datasets/source",
        "s3://destination-bucket/data.lance",
        3,
    )
    .unwrap();
    store.create_job(created).await.unwrap();
    let claim = store.claim_jobs(1, 1_000).await.unwrap().remove(0);
    let destination_uri = claim.destination_uri.clone();

    let progress = JobProgress {
        rows_read: 10,
        rows_written: 10,
        rows_total: 20,
        rows_missing_blobs: 2,
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

    let stored = job(store.as_ref(), &destination_uri).await;
    assert_eq!(stored.status, JobStatus::Running, "{backend}");
    assert_eq!(stored.progress, progress, "{backend}");
}

#[tokio::test]
#[serial]
async fn updating_lease_keeps_job_running() {
    for backend in BACKENDS {
        let clock = Arc::new(TestClock::new(10));
        let store = store_with_clock(backend, Arc::clone(&clock) as Arc<dyn Clock>).await;
        updating_lease_keeps_job_running_impl(backend, store, clock).await;
    }
}

async fn updating_lease_keeps_job_running_impl(
    backend: &str,
    store: Arc<dyn JobStore>,
    clock: Arc<TestClock>,
) {
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

    let stored = job(store.as_ref(), &destination_uri).await;
    assert_eq!(stored.status, JobStatus::Running, "{backend}");
    assert_eq!(stored.lease_expiration_timestamp_ms, Some(220), "{backend}");
}

#[tokio::test]
#[serial]
async fn completing_a_job_clears_its_lease() {
    for backend in BACKENDS {
        let store = store_with_clock(backend, Arc::new(TestClock::new(10))).await;
        completing_a_job_clears_its_lease_impl(backend, store).await;
    }
}

async fn completing_a_job_clears_its_lease_impl(backend: &str, store: Arc<dyn JobStore>) {
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
        rows_missing_blobs: 0,
    };

    store
        .complete_job(CompletionUpdate {
            destination_uri: destination_uri.to_owned(),
            attempt: claim.attempt,
            progress,
        })
        .await
        .unwrap();

    let stored = job(store.as_ref(), destination_uri).await;
    assert_eq!(stored.status, JobStatus::Succeeded, "{backend}");
    assert_eq!(stored.lease_expiration_timestamp_ms, None, "{backend}");
    assert_eq!(stored.progress, progress, "{backend}");
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
    assert!(ongoing_jobs.is_empty(), "{backend}");
}

#[tokio::test]
#[serial]
async fn failures_retry_until_attempt_cap() {
    for backend in BACKENDS {
        let store = store_with_clock(backend, Arc::new(TestClock::new(10))).await;
        failures_retry_until_attempt_cap_impl(backend, store).await;
    }
}

async fn failures_retry_until_attempt_cap_impl(backend: &str, store: Arc<dyn JobStore>) {
    let destination_uri = "s3://destination-bucket/failed.lance";
    store
        .create_job(new_job("test-user", "/datasets/source", destination_uri, 3).unwrap())
        .await
        .unwrap();

    for attempt in 1..=MAX_JOB_ATTEMPTS {
        let claim = store.claim_jobs(1, 100).await.unwrap().remove(0);
        assert_eq!(claim.attempt, attempt, "{backend}");
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

    let stored = job(store.as_ref(), destination_uri).await;
    assert_eq!(stored.status, JobStatus::Failed, "{backend}");
    assert_eq!(
        stored.error_reasons.len(),
        MAX_JOB_ATTEMPTS as usize,
        "{backend}"
    );
    assert!(
        store.claim_jobs(1, 100).await.unwrap().is_empty(),
        "{backend}"
    );
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
    assert_eq!(failed_jobs.len(), 1, "{backend}");
    assert_eq!(failed_jobs[0].destination_uri, destination_uri, "{backend}");
}

#[tokio::test]
#[serial]
async fn final_expired_attempt_becomes_failed() {
    for backend in BACKENDS {
        let clock = Arc::new(TestClock::new(10));
        let store = store_with_clock(backend, Arc::clone(&clock) as Arc<dyn Clock>).await;
        final_expired_attempt_becomes_failed_impl(backend, store, clock).await;
    }
}

async fn final_expired_attempt_becomes_failed_impl(
    backend: &str,
    store: Arc<dyn JobStore>,
    clock: Arc<TestClock>,
) {
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
    assert_eq!(final_claim.attempt, MAX_JOB_ATTEMPTS, "{backend}");
    clock.set(110);

    assert!(
        store.claim_jobs(1, 100).await.unwrap().is_empty(),
        "{backend}"
    );
    let stored = job(store.as_ref(), destination_uri).await;
    assert_eq!(stored.status, JobStatus::Failed, "{backend}");
    assert_eq!(
        stored.error_reasons.len(),
        MAX_JOB_ATTEMPTS as usize,
        "{backend}"
    );
    assert_eq!(
        stored.error_reasons.last().unwrap().reason,
        "lease expired on final attempt",
        "{backend}"
    );
}
