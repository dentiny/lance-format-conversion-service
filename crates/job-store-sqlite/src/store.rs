use std::{
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use sqlx::{
    Row, Sqlite, SqliteConnection, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow},
};

use lance_conversion_core::job::{
    BlobColumnSpec, ClaimedJob, CompletionUpdate, FailureUpdate, IndexSpec, Job, JobError,
    JobProgress, JobStatus, LeaseUpdate, MAX_JOB_ATTEMPTS, NewJob, ProgressUpdate,
};
use lance_job_store::{JobStore, StoreError};

const INITIAL_MIGRATION: &str = include_str!("../migrations/0001_initial.sql");
const BLOB_COLUMNS_JSON_COLUMN: &str = "blob_columns_json";
const INDICES_JSON_COLUMN: &str = "indices_json";
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const LIST_JOBS_SQL: &str = "SELECT creator, kind, source_uri, destination_uri,
    status, creation_timestamp_ms, update_timestamp_ms, attempt, error_reasons_json,
    lease_expiration_timestamp_ms, rows_read, rows_written, rows_total,
    blob_columns_json, indices_json
    FROM jobs
    ORDER BY creation_timestamp_ms DESC, destination_uri DESC
    LIMIT ?1";
const LOAD_JOB_SQL: &str = "SELECT creator, kind, source_uri, destination_uri,
    status, creation_timestamp_ms, update_timestamp_ms, attempt, error_reasons_json,
    lease_expiration_timestamp_ms, rows_read, rows_written, rows_total,
    blob_columns_json, indices_json
    FROM jobs
    WHERE destination_uri = ?1";

#[allow(clippy::struct_field_names)]
struct SqlProgress {
    rows_read: i64,
    rows_written: i64,
    rows_total: i64,
}

#[derive(Clone)]
pub struct SqliteJobStore {
    pool: SqlitePool,
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
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .in_memory(path == Path::new(":memory:"))
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(SQLITE_BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(database_error)?;
        sqlx::raw_sql(INITIAL_MIGRATION)
            .execute(&pool)
            .await
            .map_err(database_error)?;

        Ok(Self { pool, clock })
    }

    async fn begin_immediate(&self) -> Result<Transaction<'static, Sqlite>, StoreError> {
        self.pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(database_error)
    }

    #[cfg(test)]
    pub(super) async fn get_job(&self, destination_uri: &str) -> Result<Job, StoreError> {
        let mut connection = self.pool.acquire().await.map_err(database_error)?;
        load_job(&mut connection, destination_uri).await
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
        let blob_columns_json = serialize_json(&job.blob_columns)?;
        let indices_json = serialize_json(&job.indices)?;
        let mut transaction = self.begin_immediate().await?;
        sqlx::query(
            "INSERT INTO jobs(
                creator, kind, source_uri, destination_uri, status,
                creation_timestamp_ms, update_timestamp_ms, blob_columns_json, indices_json
             ) VALUES (?1, ?2, ?3, ?4, 'queuing', ?5, ?5, ?6, ?7)",
        )
        .bind(job.creator)
        .bind(job.kind.to_string())
        .bind(job.source.uri())
        .bind(job.destination.uri())
        .bind(job.creation_timestamp_ms)
        .bind(blob_columns_json)
        .bind(indices_json)
        .execute(&mut *transaction)
        .await
        .map_err(job_insert_error)?;
        transaction.commit().await.map_err(database_error)
    }

    async fn list_jobs(&self, limit: usize) -> Result<Vec<Job>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(LIST_JOBS_SQL)
            .bind(usize_to_i64(limit)?)
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn claim_jobs(
        &self,
        limit: usize,
        convert_lease_duration_ms: i64,
    ) -> Result<Vec<ClaimedJob>, StoreError> {
        if limit == 0 || convert_lease_duration_ms <= 0 {
            return Ok(Vec::new());
        }

        let mut transaction = self.begin_immediate().await?;
        let now_ms = self.clock.now_ms()?;
        let lease_expiration_timestamp_ms = now_ms
            .checked_add(convert_lease_duration_ms)
            .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;

        sqlx::query(
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
        )
        .bind(now_ms)
        .bind(i64::from(MAX_JOB_ATTEMPTS))
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;

        let destination_rows = sqlx::query(
            "SELECT destination_uri FROM jobs
             WHERE attempt < ?3
               AND (
                   status = 'queuing'
                   OR (status = 'running' AND lease_expiration_timestamp_ms <= ?1)
               )
             ORDER BY creation_timestamp_ms, destination_uri
             LIMIT ?2",
        )
        .bind(now_ms)
        .bind(usize_to_i64(limit)?)
        .bind(i64::from(MAX_JOB_ATTEMPTS))
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;
        let destinations = destination_rows
            .iter()
            .map(|row| row.try_get::<String, _>(0).map_err(database_error))
            .collect::<Result<Vec<_>, _>>()?;

        let mut claimed = Vec::with_capacity(destinations.len());
        for destination_uri in destinations {
            sqlx::query(
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
            )
            .bind(&destination_uri)
            .bind(lease_expiration_timestamp_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            let job = load_job(&mut transaction, &destination_uri).await?;
            claimed.push(ClaimedJob { job });
        }

        transaction.commit().await.map_err(database_error)?;
        Ok(claimed)
    }

    async fn renew_lease(&self, update: LeaseUpdate) -> Result<Job, StoreError> {
        if update.convert_lease_duration_ms <= 0 {
            return Err(StoreError::InvalidInput(
                "lease duration must be positive".to_owned(),
            ));
        }

        let mut transaction = self.begin_immediate().await?;
        let now_ms = self.clock.now_ms()?;
        validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
        let lease_expiration_timestamp_ms = now_ms
            .checked_add(update.convert_lease_duration_ms)
            .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
        let progress = progress_as_i64(update.progress)?;
        let changed = sqlx::query(
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
        )
        .bind(&update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(lease_expiration_timestamp_ms)
        .bind(now_ms)
        .bind(progress.rows_read)
        .bind(progress.rows_written)
        .bind(progress.rows_total)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed == 0 {
            return Err(StoreError::LeaseLost);
        }
        let job = load_job(&mut transaction, &update.destination_uri).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(job)
    }

    async fn checkpoint_progress(&self, update: ProgressUpdate) -> Result<Job, StoreError> {
        let mut transaction = self.begin_immediate().await?;
        let now_ms = self.clock.now_ms()?;
        validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
        let progress = progress_as_i64(update.progress)?;
        let changed = sqlx::query(
            "UPDATE jobs
             SET update_timestamp_ms = ?3,
                 rows_read = ?4,
                 rows_written = ?5,
                 rows_total = ?6
             WHERE destination_uri = ?1
               AND status = 'running'
               AND attempt = ?2
               AND lease_expiration_timestamp_ms > ?3",
        )
        .bind(&update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(now_ms)
        .bind(progress.rows_read)
        .bind(progress.rows_written)
        .bind(progress.rows_total)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed == 0 {
            return Err(StoreError::LeaseLost);
        }
        let job = load_job(&mut transaction, &update.destination_uri).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(job)
    }

    async fn complete_job(&self, update: CompletionUpdate) -> Result<(), StoreError> {
        let mut transaction = self.begin_immediate().await?;
        let now_ms = self.clock.now_ms()?;
        validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
        let progress = progress_as_i64(update.progress)?;
        let changed = sqlx::query(
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
        )
        .bind(update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(now_ms)
        .bind(progress.rows_read)
        .bind(progress.rows_written)
        .bind(progress.rows_total)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed == 0 {
            return Err(StoreError::LeaseLost);
        }
        transaction.commit().await.map_err(database_error)
    }

    async fn fail_job(&self, update: FailureUpdate) -> Result<(), StoreError> {
        let mut transaction = self.begin_immediate().await?;
        let now_ms = self.clock.now_ms()?;
        let mut job = validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
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
        let changed = sqlx::query(
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
        )
        .bind(update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(status.to_string())
        .bind(now_ms)
        .bind(error_reasons_json)
        .bind(progress.rows_read)
        .bind(progress.rows_written)
        .bind(progress.rows_total)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?
        .rows_affected();
        if changed == 0 {
            return Err(StoreError::LeaseLost);
        }
        transaction.commit().await.map_err(database_error)
    }
}

async fn validate_progress_update(
    transaction: &mut Transaction<'_, Sqlite>,
    destination_uri: &str,
    attempt: u32,
    now_ms: i64,
    incoming: JobProgress,
) -> Result<Job, StoreError> {
    let job = load_job(transaction, destination_uri).await?;
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

async fn load_job(
    connection: &mut SqliteConnection,
    destination_uri: &str,
) -> Result<Job, StoreError> {
    let row = sqlx::query(LOAD_JOB_SQL)
        .bind(destination_uri)
        .fetch_optional(connection)
        .await
        .map_err(database_error)?
        .ok_or(StoreError::NotFound)?;
    row_to_job(&row)
}

fn row_to_job(row: &SqliteRow) -> Result<Job, StoreError> {
    let error_reasons_json = row
        .try_get::<String, _>("error_reasons_json")
        .map_err(database_error)?;
    let error_reasons = deserialize_json::<Vec<JobError>>(&error_reasons_json)?;
    let blob_columns_json = row
        .try_get::<String, _>(BLOB_COLUMNS_JSON_COLUMN)
        .map_err(database_error)?;
    let indices_json = row
        .try_get::<String, _>(INDICES_JSON_COLUMN)
        .map_err(database_error)?;
    Ok(Job {
        creator: row.try_get("creator").map_err(database_error)?,
        kind: parse_value(&row.try_get::<String, _>("kind").map_err(database_error)?)?,
        source_uri: row.try_get("source_uri").map_err(database_error)?,
        destination_uri: row.try_get("destination_uri").map_err(database_error)?,
        blob_columns: deserialize_json::<Vec<BlobColumnSpec>>(&blob_columns_json)?,
        indices: deserialize_json::<Vec<IndexSpec>>(&indices_json)?,
        status: parse_value(&row.try_get::<String, _>("status").map_err(database_error)?)?,
        creation_timestamp_ms: row
            .try_get("creation_timestamp_ms")
            .map_err(database_error)?,
        update_timestamp_ms: row.try_get("update_timestamp_ms").map_err(database_error)?,
        attempt: i64_to_u32(row.try_get("attempt").map_err(database_error)?)?,
        error_reasons,
        lease_expiration_timestamp_ms: row
            .try_get("lease_expiration_timestamp_ms")
            .map_err(database_error)?,
        progress: JobProgress {
            rows_read: i64_to_u64(row.try_get("rows_read").map_err(database_error)?)?,
            rows_written: i64_to_u64(row.try_get("rows_written").map_err(database_error)?)?,
            rows_total: i64_to_u64(row.try_get("rows_total").map_err(database_error)?)?,
        },
    })
}

fn parse_value<T>(value: &str) -> Result<T, StoreError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    T::from_str(value).map_err(|error| StoreError::Database(error.to_string()))
}

fn serialize_json<T: serde::Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(|error| StoreError::Worker(error.to_string()))
}

fn deserialize_json<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    serde_json::from_str(value).map_err(|error| StoreError::Database(error.to_string()))
}

fn i64_to_u64(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|error| StoreError::Database(error.to_string()))
}

fn i64_to_u32(value: i64) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(|error| StoreError::Database(error.to_string()))
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

// This signature intentionally matches `Result::map_err`, which transfers ownership.
#[allow(clippy::needless_pass_by_value)]
fn database_error(error: sqlx::Error) -> StoreError {
    StoreError::Database(error.to_string())
}

fn job_insert_error(error: sqlx::Error) -> StoreError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        StoreError::Conflict("destination already has a job".to_owned())
    } else {
        database_error(error)
    }
}
