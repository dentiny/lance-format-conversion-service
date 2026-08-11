use std::{sync::Arc, time::Duration};

use lance_conversion_core::job::{
    ClaimedJob, CompletionUpdate, FailureUpdate, LeaseUpdate, ProgressUpdate,
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
    claimed: ClaimedJob,
    store: Arc<dyn JobStore>,
    converter: Arc<Converter>,
    config: Arc<Config>,
    convert_lease_duration_ms: i64,
) -> Result<(), ReconcilerError> {
    let job = claimed.job;
    let destination_uri = job.destination_uri.clone();
    let attempt = job.attempt;
    let progress = Arc::new(ConversionProgress::default());
    let conversion_progress = Arc::clone(&progress);
    let mut conversion =
        tokio::spawn(async move { converter.convert(&job, conversion_progress).await });
    let now = Instant::now();
    let renew_every = Duration::from_secs(config.lease_renew_interval_secs.get());
    let progress_every = Duration::from_secs(config.progress_interval_secs.get());
    let mut lease_renewal = interval_at(now + renew_every, renew_every);
    let mut progress_checkpoint = interval_at(now + progress_every, progress_every);

    loop {
        tokio::select! {
            result = &mut conversion => {
                return match result? {
                    Ok(final_progress) => {
                        store.complete_job(CompletionUpdate {
                            destination_uri,
                            attempt,
                            progress: final_progress,
                        }).await?;
                        Ok(())
                    }
                    Err(error) => {
                        store.fail_job(FailureUpdate {
                            destination_uri,
                            attempt,
                            progress: progress.snapshot(),
                            reason: error.to_string(),
                        }).await?;
                        Ok(())
                    }
                };
            }
            _ = lease_renewal.tick() => {
                if let Err(error) = store.renew_lease(LeaseUpdate {
                    destination_uri: destination_uri.clone(),
                    attempt,
                    convert_lease_duration_ms,
                    progress: progress.snapshot(),
                }).await {
                    conversion.abort();
                    return Err(error.into());
                }
            }
            _ = progress_checkpoint.tick() => {
                if let Err(error) = store.checkpoint_progress(ProgressUpdate {
                    destination_uri: destination_uri.clone(),
                    attempt,
                    progress: progress.snapshot(),
                }).await {
                    conversion.abort();
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
    use std::sync::Arc;

    use clap::Parser;
    use lance_conversion_core::{
        job::{JobKind, JobStatus, NewJob},
        location::DatasetLocation,
    };
    use lance_converter::{Converter, ConverterConfig};
    use lance_job_store::JobStore;
    use lance_job_store_sqlite::SqliteJobStore;
    use tempfile::TempDir;

    use super::run_job;
    use crate::config::Config;

    #[tokio::test]
    async fn conversion_failure_returns_job_to_queue() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let temp_dir = TempDir::new().unwrap();
        let source = temp_dir.path().join("empty-source");
        tokio::fs::create_dir(&source).await.unwrap();
        let destination = temp_dir.path().join("destination.lance");
        store
            .create_job(NewJob {
                creator: "test-user".to_owned(),
                source: DatasetLocation::parse_location(source.to_string_lossy()).unwrap(),
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_location(destination.to_string_lossy())
                    .unwrap(),
                creation_timestamp_ms: 1,
            })
            .await
            .unwrap();
        let claimed = store.claim_jobs(1, 60_000).await.unwrap().remove(0);
        let config = Arc::new(Config::parse_from(["lance-reconciler"]));
        let converter = Arc::new(Converter::new(ConverterConfig {
            target_lance_file_size_mib: config.target_lance_file_size_mib.get(),
            blob_inline_threshold_mib: config.blob_inline_threshold_mib.get(),
        }));
        let trait_store: Arc<dyn JobStore> = store.clone();

        run_job(claimed, trait_store, converter, config, 60_000)
            .await
            .unwrap();

        let job = store.list_jobs(1).await.unwrap().remove(0);
        assert_eq!(job.status, JobStatus::Queuing);
        assert_eq!(job.error_reasons.len(), 1);
    }
}
