use std::{
    io,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Row, TransactionBehavior, params, types::Type,
};

use lance_conversion_core::job::{
    ClaimedJob, Job, JobError, JobProgress, JobStatus, LeaseUpdate, NewJob, ProgressUpdate,
};
use lance_job_store::{JobStore, StoreError};

const INITIAL_MIGRATION: &str = include_str!("../migrations/0001_initial.sql");
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const JOB_COLUMNS: &str = "creator, kind, source_uri, destination_uri, \
    status, creation_timestamp_ms, update_timestamp_ms, attempt, error_reasons_json, \
    lease_expiration_timestamp_ms, source_bytes_read, lance_bytes_written, rows_read, \
    rows_written, rows_total, work_units_completed, work_units_total";

struct SqlProgress {
    source_bytes_read: i64,
    lance_bytes_written: i64,
    rows_read: i64,
    rows_written: i64,
    rows_total: i64,
    work_units_completed: i64,
    work_units_total: i64,
}

#[derive(Clone)]
pub struct SqliteJobStore {
    connection: Arc<Mutex<Connection>>,
    clock: Arc<dyn Clock>,
}

impl SqliteJobStore {
    /// Opens a `SQLite` job store and applies embedded migrations.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot be opened, configured, or migrated.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_clock(path, Arc::new(SystemClock)).await
    }

    async fn open_with_clock(
        path: impl AsRef<Path>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        tokio::task::spawn_blocking(move || Self::open_blocking(&path, clock))
            .await
            .map_err(|error| StoreError::Worker(error.to_string()))?
    }

    fn open_blocking(path: &Path, clock: Arc<dyn Clock>) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(database_error)?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(database_error)?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(database_error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(database_error)?;
        connection
            .execute_batch(INITIAL_MIGRATION)
            .map_err(database_error)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            clock,
        })
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|error| StoreError::Worker(error.to_string()))?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| StoreError::Worker(error.to_string()))?
    }
}

trait Clock: Send + Sync {
    fn now_ms(&self) -> Result<i64, StoreError>;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Result<i64, StoreError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| StoreError::Worker(error.to_string()))?
            .as_millis();
        i64::try_from(millis).map_err(|error| StoreError::Worker(error.to_string()))
    }
}

#[async_trait]
impl JobStore for SqliteJobStore {
    async fn create_job(&self, job: NewJob) -> Result<(), StoreError> {
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            transaction
                .execute(
                    "INSERT INTO jobs(
                        creator, kind, source_uri, destination_uri, status,
                        creation_timestamp_ms, update_timestamp_ms
                    ) VALUES (?1, ?2, ?3, ?4, 'queuing', ?5, ?5)",
                    params![
                        job.creator,
                        job.kind.to_string(),
                        job.source.uri(),
                        job.destination.uri(),
                        job.creation_timestamp_ms,
                    ],
                )
                .map_err(job_insert_error)?;
            transaction.commit().map_err(database_error)?;
            Ok(())
        })
        .await
    }

    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>, StoreError> {
        self.run(move |connection| {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM jobs
                 ORDER BY creation_timestamp_ms DESC, destination_uri DESC
                 LIMIT ?1"
            );
            let mut statement = connection.prepare(&sql).map_err(database_error)?;
            let rows = statement
                .query_map([usize_to_i64(limit)?], row_to_job)
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
        .await
    }

    async fn claim_jobs(
        &self,
        limit: usize,
        lease_duration_ms: i64,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            if limit == 0 || lease_duration_ms <= 0 {
                return Ok(Vec::new());
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            let lease_expiration_timestamp_ms = now_ms
                .checked_add(lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            let destinations = {
                let mut statement = transaction
                    .prepare(
                        "SELECT destination_uri FROM jobs
                         WHERE status = 'queuing'
                            OR (status = 'running' AND lease_expiration_timestamp_ms <= ?1)
                         ORDER BY creation_timestamp_ms, destination_uri
                         LIMIT ?2",
                    )
                    .map_err(database_error)?;
                let rows = statement
                    .query_map(params![now_ms, usize_to_i64(limit)?], |row| {
                        row.get::<_, String>(0)
                    })
                    .map_err(database_error)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(database_error)?
            };

            let mut claimed = Vec::with_capacity(destinations.len());
            for destination_uri in destinations {
                transaction
                    .execute(
                        "UPDATE jobs
                         SET status = 'running',
                             lease_expiration_timestamp_ms = ?2,
                             attempt = attempt + 1,
                             update_timestamp_ms = ?3
                         WHERE destination_uri = ?1",
                        params![destination_uri, lease_expiration_timestamp_ms, now_ms],
                    )
                    .map_err(database_error)?;
                let job = load_job(&transaction, &destination_uri)?;
                claimed.push(ClaimedJob { job });
            }
            transaction.commit().map_err(database_error)?;
            Ok(claimed)
        })
        .await
    }

    async fn renew_lease(&self, update: LeaseUpdate) -> Result<Job, StoreError> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            if update.lease_duration_ms <= 0 {
                return Err(StoreError::InvalidInput(
                    "lease duration must be positive".to_owned(),
                ));
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            validate_progress_update(
                &transaction,
                &update.destination_uri,
                update.attempt,
                now_ms,
                update.progress,
            )?;
            let lease_expiration_timestamp_ms = now_ms
                .checked_add(update.lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET lease_expiration_timestamp_ms = ?3,
                         update_timestamp_ms = ?4,
                         source_bytes_read = ?5,
                         lance_bytes_written = ?6,
                         rows_read = ?7,
                         rows_written = ?8,
                         rows_total = ?9,
                         work_units_completed = ?10,
                         work_units_total = ?11
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?4",
                    params![
                        &update.destination_uri,
                        i64::from(update.attempt),
                        lease_expiration_timestamp_ms,
                        now_ms,
                        progress.source_bytes_read,
                        progress.lance_bytes_written,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
                        progress.work_units_completed,
                        progress.work_units_total,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            let job = load_job(&transaction, &update.destination_uri)?;
            transaction.commit().map_err(database_error)?;
            Ok(job)
        })
        .await
    }

    async fn checkpoint_progress(&self, update: ProgressUpdate) -> Result<Job, StoreError> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            validate_progress_update(
                &transaction,
                &update.destination_uri,
                update.attempt,
                now_ms,
                update.progress,
            )?;
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET update_timestamp_ms = ?3,
                         source_bytes_read = ?4,
                         lance_bytes_written = ?5,
                         rows_read = ?6,
                         rows_written = ?7,
                         rows_total = ?8,
                         work_units_completed = ?9,
                         work_units_total = ?10
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?3",
                    params![
                        &update.destination_uri,
                        i64::from(update.attempt),
                        now_ms,
                        progress.source_bytes_read,
                        progress.lance_bytes_written,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
                        progress.work_units_completed,
                        progress.work_units_total,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            let job = load_job(&transaction, &update.destination_uri)?;
            transaction.commit().map_err(database_error)?;
            Ok(job)
        })
        .await
    }
}

fn validate_progress_update(
    connection: &Connection,
    destination_uri: &str,
    attempt: u32,
    now_ms: i64,
    incoming: JobProgress,
) -> Result<(), StoreError> {
    let job = load_job(connection, destination_uri)?;
    if job.status != JobStatus::Running
        || job.attempt != attempt
        || job
            .lease_expiration_timestamp_ms
            .is_none_or(|expiry| expiry <= now_ms)
    {
        return Err(StoreError::LeaseLost);
    }
    if incoming.work_units_total > 0 && incoming.work_units_completed > incoming.work_units_total {
        return Err(StoreError::InvalidInput(
            "completed work units exceed total work units".to_owned(),
        ));
    }
    if incoming.rows_total > 0
        && (incoming.rows_read > incoming.rows_total || incoming.rows_written > incoming.rows_total)
    {
        return Err(StoreError::InvalidInput(
            "read or written rows exceed total rows".to_owned(),
        ));
    }
    Ok(())
}

fn load_job(connection: &Connection, destination_uri: &str) -> Result<Job, StoreError> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE destination_uri = ?1");
    connection
        .query_row(&sql, [destination_uri], row_to_job)
        .optional()
        .map_err(database_error)?
        .ok_or(StoreError::NotFound)
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<Job> {
    let error_reasons = serde_json::from_str::<Vec<JobError>>(&row.get::<_, String>(8)?)
        .map_err(|error| conversion_error(8, Type::Text, error))?;
    Ok(Job {
        creator: row.get(0)?,
        kind: parse_value(&row.get::<_, String>(1)?, 1)?,
        source_uri: row.get(2)?,
        destination_uri: row.get(3)?,
        status: parse_value(&row.get::<_, String>(4)?, 4)?,
        creation_timestamp_ms: row.get(5)?,
        update_timestamp_ms: row.get(6)?,
        attempt: i64_to_u32(row.get(7)?, 7)?,
        error_reasons,
        lease_expiration_timestamp_ms: row.get(9)?,
        progress: JobProgress {
            source_bytes_read: i64_to_u64(row.get(10)?, 10)?,
            lance_bytes_written: i64_to_u64(row.get(11)?, 11)?,
            rows_read: i64_to_u64(row.get(12)?, 12)?,
            rows_written: i64_to_u64(row.get(13)?, 13)?,
            rows_total: i64_to_u64(row.get(14)?, 14)?,
            work_units_completed: i64_to_u64(row.get(15)?, 15)?,
            work_units_total: i64_to_u64(row.get(16)?, 16)?,
        },
    })
}

fn parse_value<T>(value: &str, index: usize) -> rusqlite::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    T::from_str(value).map_err(|error| {
        conversion_error(
            index,
            Type::Text,
            io::Error::new(io::ErrorKind::InvalidData, error.to_string()),
        )
    })
}

fn i64_to_u64(value: i64, index: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| conversion_error(index, Type::Integer, error))
}

fn i64_to_u32(value: i64, index: usize) -> rusqlite::Result<u32> {
    u32::try_from(value).map_err(|error| conversion_error(index, Type::Integer, error))
}

fn usize_to_i64(value: usize) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|error| StoreError::InvalidInput(error.to_string()))
}

fn progress_as_i64(progress: JobProgress) -> Result<SqlProgress, StoreError> {
    Ok(SqlProgress {
        source_bytes_read: u64_as_i64(progress.source_bytes_read)?,
        lance_bytes_written: u64_as_i64(progress.lance_bytes_written)?,
        rows_read: u64_as_i64(progress.rows_read)?,
        rows_written: u64_as_i64(progress.rows_written)?,
        rows_total: u64_as_i64(progress.rows_total)?,
        work_units_completed: u64_as_i64(progress.work_units_completed)?,
        work_units_total: u64_as_i64(progress.work_units_total)?,
    })
}

fn u64_as_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|error| StoreError::InvalidInput(error.to_string()))
}

fn conversion_error(
    index: usize,
    value_type: Type,
    error: impl std::error::Error + Send + Sync + 'static,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, value_type, Box::new(error))
}

// This signature intentionally matches `Result::map_err`, which transfers ownership.
#[allow(clippy::needless_pass_by_value)]
fn database_error(error: rusqlite::Error) -> StoreError {
    StoreError::Database(error.to_string())
}

fn job_insert_error(error: rusqlite::Error) -> StoreError {
    if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) {
        StoreError::Conflict("destination already has a job".to_owned())
    } else {
        database_error(error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };

    use lance_conversion_core::{
        job::{JobKind, JobProgress, LeaseUpdate, NewJob, ProgressUpdate},
        location::DatasetLocation,
    };
    use lance_job_store::{JobStore, StoreError};

    use super::{Clock, SqliteJobStore};

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
    async fn claims_and_reclaims_expired_jobs_with_attempt_fencing() {
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
                creation_timestamp_ms: 3,
            })
            .await
            .unwrap();

        let first_claim = store.claim_jobs(1, 100).await.unwrap();
        assert_eq!(first_claim.len(), 1);
        let destination_uri = first_claim[0].job.destination_uri.clone();
        assert_eq!(first_claim[0].job.attempt, 1);

        clock.set(111);
        let expired_error = store
            .renew_lease(LeaseUpdate {
                destination_uri: destination_uri.clone(),
                attempt: first_claim[0].job.attempt,
                lease_duration_ms: 100,
                progress: JobProgress::default(),
            })
            .await
            .unwrap_err();
        assert!(matches!(expired_error, StoreError::LeaseLost));

        let reclaimed = store.claim_jobs(1, 100).await.unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].job.attempt, 2);

        clock.set(112);
        let error = store
            .renew_lease(LeaseUpdate {
                destination_uri,
                attempt: first_claim[0].job.attempt,
                lease_duration_ms: 188,
                progress: JobProgress::default(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::LeaseLost));
    }

    #[tokio::test]
    async fn heartbeat_updates_progress_snapshot() {
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
                creation_timestamp_ms: 3,
            })
            .await
            .unwrap();
        let claim = store.claim_jobs(1, 1_000).await.unwrap().remove(0);
        let destination_uri = claim.job.destination_uri.clone();

        let progress = JobProgress {
            source_bytes_read: 100,
            lance_bytes_written: 80,
            rows_read: 10,
            rows_written: 10,
            rows_total: 20,
            work_units_completed: 1,
            work_units_total: 2,
        };
        clock.set(20);
        let checkpointed = store
            .checkpoint_progress(ProgressUpdate {
                destination_uri: destination_uri.clone(),
                attempt: claim.job.attempt,
                progress,
            })
            .await
            .unwrap();
        assert_eq!(checkpointed.lease_expiration_timestamp_ms, Some(1_010));

        clock.set(21);
        let updated = store
            .renew_lease(LeaseUpdate {
                destination_uri: destination_uri.clone(),
                attempt: claim.job.attempt,
                lease_duration_ms: 1_979,
                progress: JobProgress::default(),
            })
            .await
            .unwrap();
        assert_eq!(updated.progress, JobProgress::default());
        assert_eq!(updated.lease_expiration_timestamp_ms, Some(2_000));

        clock.set(22);
        let error = store
            .checkpoint_progress(ProgressUpdate {
                destination_uri,
                attempt: claim.job.attempt,
                progress: JobProgress {
                    work_units_completed: 3,
                    work_units_total: 2,
                    ..JobProgress::default()
                },
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidInput(_)));
        assert_eq!(
            store
                .list_jobs(1)
                .await
                .unwrap()
                .remove(0)
                .progress
                .work_units_completed,
            0
        );
    }

    #[tokio::test]
    async fn concurrent_progress_snapshots_remain_valid() {
        let path = std::env::temp_dir().join(format!("lance-service-{}.db", uuid::Uuid::new_v4()));
        let clock = Arc::new(TestClock::new(10));
        let first = SqliteJobStore::open_with_clock(&path, clock.clone())
            .await
            .unwrap();
        let second = SqliteJobStore::open_with_clock(&path, clock.clone())
            .await
            .unwrap();
        first
            .create_job(NewJob {
                creator: "test-user".to_owned(),
                source: source("/datasets/source"),
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_location(
                    "s3://destination-bucket/atomic.lance",
                )
                .unwrap(),
                creation_timestamp_ms: 3,
            })
            .await
            .unwrap();
        let claim = first.claim_jobs(1, 1_000).await.unwrap().remove(0);
        let destination_uri = claim.job.destination_uri.clone();

        clock.set(20);
        let completed_update = first.checkpoint_progress(ProgressUpdate {
            destination_uri: destination_uri.clone(),
            attempt: claim.job.attempt,
            progress: JobProgress {
                work_units_completed: 10,
                work_units_total: 0,
                ..JobProgress::default()
            },
        });
        let total_update = second.checkpoint_progress(ProgressUpdate {
            destination_uri,
            attempt: claim.job.attempt,
            progress: JobProgress {
                work_units_completed: 0,
                work_units_total: 5,
                ..JobProgress::default()
            },
        });
        let (completed_result, total_result) = tokio::join!(completed_update, total_update);
        completed_result.unwrap();
        total_result.unwrap();

        let progress = first.list_jobs(1).await.unwrap().remove(0).progress;
        assert!(
            progress.work_units_total == 0
                || progress.work_units_completed <= progress.work_units_total
        );

        drop(first);
        drop(second);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn migrations_are_idempotent_across_reopen() {
        let path = std::env::temp_dir().join(format!("lance-service-{}.db", uuid::Uuid::new_v4()));
        SqliteJobStore::open(&path).await.unwrap();
        SqliteJobStore::open(&path).await.unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
