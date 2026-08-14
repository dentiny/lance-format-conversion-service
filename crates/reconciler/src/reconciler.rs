use std::{sync::Arc, time::Duration};

use lance_conversion_core::job::{
    CompletionUpdate, FailureUpdate, Job, LeaseUpdate, ProgressUpdate,
};
use lance_converter::{ConversionProgress, Converter};
use lance_job_store::{JobStore, StoreError};
use thiserror::Error;
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, interval, interval_at},
};

use crate::config::Config;

pub async fn run(
    store: Arc<dyn JobStore>,
    converter: Arc<Converter>,
    config: Config,
) -> Result<(), ReconcilerError> {
    let convert_lease_duration_ms = config
        .convert_lease_duration_ms()
        .map_err(|error| ReconcilerError::Configuration(error.to_string()))?;
    let config = Arc::new(config);
    let mut poll = interval(Duration::from_millis(config.poll_interval_ms.get()));
    let mut workers = JoinSet::new();

    loop {
        poll.tick().await;
        while let Some(result) = workers.try_join_next() {
            result??;
        }

        let available = config.worker_count.get().saturating_sub(workers.len());
        if available == 0 {
            continue;
        }
        let jobs = store
            .claim_jobs(available, convert_lease_duration_ms)
            .await?;
        for job in jobs {
            workers.spawn(run_job(
                job,
                Arc::clone(&store),
                Arc::clone(&converter),
                Arc::clone(&config),
                convert_lease_duration_ms,
            ));
        }
    }
}

async fn run_job(
    job: Job,
    store: Arc<dyn JobStore>,
    converter: Arc<Converter>,
    config: Arc<Config>,
    convert_lease_duration_ms: i64,
) -> Result<(), ReconcilerError> {
    let destination_uri = job.destination_uri.clone();
    let attempt = job.attempt;
    let progress = Arc::new(ConversionProgress::default());
    let conversion_progress = Arc::clone(&progress);
    let conversion = converter.convert(&job, conversion_progress);
    tokio::pin!(conversion);
    let now = Instant::now();
    let renew_every = Duration::from_secs(config.lease_renew_interval_secs.get());
    let progress_every = Duration::from_secs(config.progress_interval_secs.get());
    let mut lease_renewal = interval_at(now + renew_every, renew_every);
    let mut progress_checkpoint = interval_at(now + progress_every, progress_every);

    loop {
        enum Event<T> {
            ConversionFinished(T),
            RenewLease,
            CheckpointProgress,
        }

        // `conversion` is pinned and retained across iterations, and
        // `Interval::tick` is cancellation-safe. Store writes happen after the
        // selection so another ready arm cannot cancel them midway.
        let event = tokio::select! {
            result = &mut conversion => Event::ConversionFinished(result),
            _ = lease_renewal.tick() => Event::RenewLease,
            _ = progress_checkpoint.tick() => Event::CheckpointProgress,
        };

        match event {
            Event::ConversionFinished(result) => {
                return match result {
                    Ok(final_progress) => {
                        store
                            .complete_job(CompletionUpdate {
                                destination_uri,
                                attempt,
                                progress: final_progress,
                            })
                            .await?;
                        Ok(())
                    }
                    Err(error) => {
                        store
                            .fail_job(FailureUpdate {
                                destination_uri,
                                attempt,
                                progress: progress.snapshot(),
                                reason: error.to_string(),
                            })
                            .await?;
                        Ok(())
                    }
                };
            }
            Event::RenewLease => {
                if let Err(error) = store
                    .renew_lease(LeaseUpdate {
                        destination_uri: destination_uri.clone(),
                        attempt,
                        convert_lease_duration_ms,
                        progress: progress.snapshot(),
                    })
                    .await
                {
                    return Err(error.into());
                }
            }
            Event::CheckpointProgress => {
                if let Err(error) = store
                    .checkpoint_progress(ProgressUpdate {
                        destination_uri: destination_uri.clone(),
                        attempt,
                        progress: progress.snapshot(),
                    })
                    .await
                {
                    return Err(error.into());
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ReconcilerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("conversion worker task failed: {0}")]
    Worker(#[from] JoinError),
    #[error("invalid reconciler configuration: {0}")]
    Configuration(String),
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use futures::TryStreamExt;
    use lance::{Dataset, index::DatasetIndexExt};
    use lance_conversion_core::job::{IndexSpec, IndexType, Job, JobStatus, MAX_JOB_ATTEMPTS};
    use lance_converter::Converter;
    use lance_job_store::JobStore;
    use lance_job_store_sqlite::SqliteJobStore;
    use lance_test_support::{new_job, write_parquet as write_parquet_file};
    use tempfile::TempDir;

    use super::run_job;
    use crate::config::Config;

    const TEST_JOB_LIMIT: usize = 1;
    const TEST_CONVERT_LEASE_DURATION_MS: i64 = 60_000;
    const EXPIRED_LEASE_DURATION_MS: i64 = 1;
    const LEASE_EXPIRATION_WAIT: Duration = Duration::from_millis(20);
    const TEST_CREATION_TIMESTAMP_MS: i64 = 1;
    const FIRST_ATTEMPT: u32 = 1;
    const SECOND_ATTEMPT: u32 = 2;
    const EXPECTED_ERROR_COUNT: usize = 1;
    const TEST_VALUES: [i64; 3] = [1, 2, 3];
    const EXPECTED_ROW_COUNT: u64 = TEST_VALUES.len() as u64;
    const TEST_INDEX_NAME: &str = "conversion_0_scalar_idx";

    #[tokio::test]
    async fn conversion_success_marks_job_succeeded() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("source");
        tokio::fs::create_dir(&source).await.unwrap();
        write_test_source(&source).await;
        let destination = temp_dir.path().join("destination.lance");
        create_job(
            &store,
            &source,
            &destination,
            vec![IndexSpec {
                columns: vec!["value".to_owned()],
                index_type: IndexType::Scalar,
            }],
        )
        .await;
        let claimed = store
            .claim_jobs(TEST_JOB_LIMIT, TEST_CONVERT_LEASE_DURATION_MS)
            .await
            .unwrap()
            .remove(0);

        run_claimed_job(&store, claimed).await;

        let job = store.list_jobs(TEST_JOB_LIMIT).await.unwrap().remove(0);
        assert_eq!(job.status, JobStatus::Succeeded);
        assert_eq!(job.progress.rows_written, EXPECTED_ROW_COUNT);
        assert_eq!(job.progress.rows_total, EXPECTED_ROW_COUNT);
        assert!(job.error_reasons.is_empty());

        let dataset = Dataset::open(destination.to_string_lossy().as_ref())
            .await
            .unwrap();
        let output = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let values = output
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, TEST_VALUES);
        assert!(
            dataset
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|index| index.name == TEST_INDEX_NAME)
        );
    }

    #[tokio::test]
    async fn conversion_failure_returns_job_to_queue() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("empty-source");
        tokio::fs::create_dir(&source).await.unwrap();
        let destination = temp_dir.path().join("destination.lance");
        create_job(&store, &source, &destination, Vec::new()).await;
        let claimed = store
            .claim_jobs(TEST_JOB_LIMIT, TEST_CONVERT_LEASE_DURATION_MS)
            .await
            .unwrap()
            .remove(0);

        run_claimed_job(&store, claimed).await;

        let job = store.list_jobs(TEST_JOB_LIMIT).await.unwrap().remove(0);
        assert_eq!(job.status, JobStatus::Queuing);
        assert_eq!(job.error_reasons.len(), EXPECTED_ERROR_COUNT);
    }

    #[tokio::test]
    async fn conversion_failure_marks_job_failed_after_all_attempts() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("empty-source");
        tokio::fs::create_dir(&source).await.unwrap();
        let destination = temp_dir.path().join("destination.lance");
        create_job(&store, &source, &destination, Vec::new()).await;

        for expected_attempt in 1..=MAX_JOB_ATTEMPTS {
            let claimed = store
                .claim_jobs(TEST_JOB_LIMIT, TEST_CONVERT_LEASE_DURATION_MS)
                .await
                .unwrap()
                .remove(0);
            assert_eq!(claimed.attempt, expected_attempt);

            run_claimed_job(&store, claimed).await;

            let job = store.list_jobs(TEST_JOB_LIMIT).await.unwrap().remove(0);
            let expected_status = if expected_attempt == MAX_JOB_ATTEMPTS {
                JobStatus::Failed
            } else {
                JobStatus::Queuing
            };
            assert_eq!(job.status, expected_status);
            assert_eq!(job.error_reasons.len(), expected_attempt as usize);
            let latest_error = job.error_reasons.last().unwrap();
            assert_eq!(latest_error.attempt, expected_attempt);
            assert!(latest_error.error_timestamp_ms > 0);
        }

        assert!(
            store
                .claim_jobs(TEST_JOB_LIMIT, TEST_CONVERT_LEASE_DURATION_MS)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn expired_job_is_reclaimed_after_previous_worker_dies() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("source");
        tokio::fs::create_dir(&source).await.unwrap();
        write_test_source(&source).await;
        let destination = temp_dir.path().join("destination.lance");
        create_job(&store, &source, &destination, Vec::new()).await;

        let abandoned = store
            .claim_jobs(TEST_JOB_LIMIT, EXPIRED_LEASE_DURATION_MS)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(abandoned.attempt, FIRST_ATTEMPT);
        tokio::time::sleep(LEASE_EXPIRATION_WAIT).await;
        let reclaimed = store
            .claim_jobs(TEST_JOB_LIMIT, TEST_CONVERT_LEASE_DURATION_MS)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(reclaimed.attempt, SECOND_ATTEMPT);
        assert_eq!(reclaimed.error_reasons.len(), EXPECTED_ERROR_COUNT);
        assert_eq!(
            reclaimed.error_reasons[0].reason,
            "lease expired before completion"
        );

        run_claimed_job(&store, reclaimed).await;

        let job = store.list_jobs(TEST_JOB_LIMIT).await.unwrap().remove(0);
        assert_eq!(job.status, JobStatus::Succeeded);
        assert_eq!(job.attempt, SECOND_ATTEMPT);
        assert_eq!(job.progress.rows_written, EXPECTED_ROW_COUNT);
    }

    async fn create_job(
        store: &SqliteJobStore,
        source: &std::path::Path,
        destination: &std::path::Path,
        indices: Vec<IndexSpec>,
    ) {
        let mut job = new_job(
            "test-user",
            source.to_string_lossy(),
            destination.to_string_lossy(),
            TEST_CREATION_TIMESTAMP_MS,
        )
        .unwrap();
        job.indices = indices;
        store.create_job(job).await.unwrap();
    }

    async fn run_claimed_job(store: &Arc<SqliteJobStore>, job: Job) {
        let config = Arc::new(Config::default());
        let converter = Arc::new(Converter::new(config.converter_config()).unwrap());
        let trait_store: Arc<dyn JobStore> = store.clone();
        run_job(
            job,
            trait_store,
            converter,
            config,
            TEST_CONVERT_LEASE_DURATION_MS,
        )
        .await
        .unwrap();
    }

    async fn write_test_source(directory: &std::path::Path) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(TEST_VALUES.to_vec()))],
        )
        .unwrap();
        write_parquet_file(directory.join("part.parquet"), &batch)
            .await
            .unwrap();
    }
}
