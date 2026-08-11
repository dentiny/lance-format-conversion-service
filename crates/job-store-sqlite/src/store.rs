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
    ClaimedJob, CompletionUpdate, FailureUpdate, Job, JobError, JobProgress, JobStatus,
    LeaseUpdate, MAX_JOB_ATTEMPTS, NewJob, ProgressUpdate,
};
use lance_job_store::{JobStore, StoreError};

const INITIAL_MIGRATION: &str = include_str!("../migrations/0001_initial.sql");
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const JOB_COLUMNS: &str = "creator, kind, source_uri, destination_uri, \
    status, creation_timestamp_ms, update_timestamp_ms, attempt, error_reasons_json, \
    lease_expiration_timestamp_ms, rows_read, rows_written, rows_total";

#[allow(clippy::struct_field_names)]
struct SqlProgress {
    rows_read: i64,
    rows_written: i64,
    rows_total: i64,
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

    pub(super) async fn open_with_clock(
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

    #[cfg(test)]
    pub(super) async fn get_job(&self, destination_uri: &str) -> Result<Job, StoreError> {
        let destination_uri = destination_uri.to_owned();
        self.run(move |connection| load_job(connection, &destination_uri))
            .await
    }
}

pub(super) trait Clock: Send + Sync {
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
        convert_lease_duration_ms: i64,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            if limit == 0 || convert_lease_duration_ms <= 0 {
                return Ok(Vec::new());
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            let lease_expiration_timestamp_ms = now_ms
                .checked_add(convert_lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            transaction
                .execute(
                    "UPDATE jobs
                     SET status = 'failed',
                         update_timestamp_ms = ?1,
                         lease_expiration_timestamp_ms = NULL,
                         error_reasons_json = json_insert(
                             error_reasons_json,
                             '$[#]',
                             json_object(
                                 'attempt', attempt,
                                 'error_timestamp_ms', ?1,
                                 'reason', 'lease expired on final attempt'
                             )
                         )
                     WHERE status = 'running'
                       AND lease_expiration_timestamp_ms <= ?1
                       AND attempt >= ?2",
                    params![now_ms, i64::from(MAX_JOB_ATTEMPTS)],
                )
                .map_err(database_error)?;
            let destinations = {
                let mut statement = transaction
                    .prepare(
                        "SELECT destination_uri FROM jobs
                         WHERE attempt < ?3
                           AND (
                               status = 'queuing'
                               OR (status = 'running' AND lease_expiration_timestamp_ms <= ?1)
                           )
                         ORDER BY creation_timestamp_ms, destination_uri
                         LIMIT ?2",
                    )
                    .map_err(database_error)?;
                let rows = statement
                    .query_map(
                        params![now_ms, usize_to_i64(limit)?, i64::from(MAX_JOB_ATTEMPTS)],
                        |row| row.get::<_, String>(0),
                    )
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
                             update_timestamp_ms = ?3,
                             error_reasons_json = CASE
                                 WHEN status = 'running' THEN json_insert(
                                     error_reasons_json,
                                     '$[#]',
                                     json_object(
                                         'attempt', attempt,
                                         'error_timestamp_ms', ?3,
                                         'reason', 'lease expired before completion'
                                     )
                                 )
                                 ELSE error_reasons_json
                             END
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
            if update.convert_lease_duration_ms <= 0 {
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
                .checked_add(update.convert_lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET lease_expiration_timestamp_ms = ?3,
                         update_timestamp_ms = ?4,
                         rows_read = ?5,
                         rows_written = ?6,
                         rows_total = ?7
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?4",
                    params![
                        &update.destination_uri,
                        i64::from(update.attempt),
                        lease_expiration_timestamp_ms,
                        now_ms,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
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
                         rows_read = ?4,
                         rows_written = ?5,
                         rows_total = ?6
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?3",
                    params![
                        &update.destination_uri,
                        i64::from(update.attempt),
                        now_ms,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
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

    async fn complete_job(&self, update: CompletionUpdate) -> Result<(), StoreError> {
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
                     SET status = 'succeeded',
                         update_timestamp_ms = ?3,
                         lease_expiration_timestamp_ms = NULL,
                         rows_read = ?4,
                         rows_written = ?5,
                         rows_total = ?6
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?3",
                    params![
                        update.destination_uri,
                        i64::from(update.attempt),
                        now_ms,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            transaction.commit().map_err(database_error)
        })
        .await
    }

    async fn fail_job(&self, update: FailureUpdate) -> Result<(), StoreError> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            let mut job = validate_progress_update(
                &transaction,
                &update.destination_uri,
                update.attempt,
                now_ms,
                update.progress,
            )?;
            job.error_reasons.push(JobError {
                attempt: update.attempt,
                error_timestamp_ms: now_ms,
                reason: update.reason,
            });
            let error_reasons_json = serde_json::to_string(&job.error_reasons)
                .map_err(|error| StoreError::Worker(error.to_string()))?;
            let status = if update.attempt >= MAX_JOB_ATTEMPTS {
                JobStatus::Failed
            } else {
                JobStatus::Queuing
            };
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET status = ?3,
                         update_timestamp_ms = ?4,
                         error_reasons_json = ?5,
                         lease_expiration_timestamp_ms = NULL,
                         rows_read = ?6,
                         rows_written = ?7,
                         rows_total = ?8
                     WHERE destination_uri = ?1
                       AND status = 'running'
                       AND attempt = ?2
                       AND lease_expiration_timestamp_ms > ?4",
                    params![
                        update.destination_uri,
                        i64::from(update.attempt),
                        status.to_string(),
                        now_ms,
                        error_reasons_json,
                        progress.rows_read,
                        progress.rows_written,
                        progress.rows_total,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            transaction.commit().map_err(database_error)
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
) -> Result<Job, StoreError> {
    let job = load_job(connection, destination_uri)?;
    if job.status != JobStatus::Running
        || job.attempt != attempt
        || job
            .lease_expiration_timestamp_ms
            .is_none_or(|expiry| expiry <= now_ms)
    {
        return Err(StoreError::LeaseLost);
    }
    if incoming.rows_total > 0
        && (incoming.rows_read > incoming.rows_total || incoming.rows_written > incoming.rows_total)
    {
        return Err(StoreError::InvalidInput(
            "read or written rows exceed total rows".to_owned(),
        ));
    }
    Ok(job)
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
            rows_read: i64_to_u64(row.get(10)?, 10)?,
            rows_written: i64_to_u64(row.get(11)?, 11)?,
            rows_total: i64_to_u64(row.get(12)?, 12)?,
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
        rows_read: u64_as_i64(progress.rows_read)?,
        rows_written: u64_as_i64(progress.rows_written)?,
        rows_total: u64_as_i64(progress.rows_total)?,
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
