use std::{
    io,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Row, TransactionBehavior, params, types::Type,
};
use uuid::Uuid;

use crate::{
    domain::{
        ClaimedJob, CompletedInspection, Inspection, InspectionStatus, Job, JobKind, JobProgress,
        JobStatus, LeaseUpdate, NewInspection, NewJob, ProgressUpdate,
    },
    location::{DatasetLocation, LocationKind},
    store::{JobStore, StoreError, StoreFuture},
};

const INITIAL_MIGRATION: &str = include_str!("../migrations/0001_initial.sql");
const INSPECTION_COLUMNS: &str =
    "id, source_uri, source_kind, status, schema_fingerprint, error, created_at_ms, updated_at_ms";
const JOB_COLUMNS: &str = "id, inspection_id, kind, source_uri, destination_uri, \
    schema_fingerprint, status, submitted_at_ms, updated_at_ms, attempt, lease_owner, \
    lease_token, lease_expires_at_ms, source_bytes_read, lance_bytes_written, rows_read, \
    rows_written, work_units_completed, work_units_total";

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
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_clock(path, Arc::new(SystemClock))
    }

    fn open_with_clock(path: impl AsRef<Path>, clock: Arc<dyn Clock>) -> Result<Self, StoreError> {
        let connection = Connection::open(path).map_err(database_error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
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
        connection
            .execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at_ms) VALUES (1, 0)",
                [],
            )
            .map_err(database_error)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            clock,
        })
    }

    fn run<T, F>(&self, operation: F) -> StoreFuture<'_, T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|error| StoreError::Worker(error.to_string()))?;
                operation(&mut connection)
            })
            .await
            .map_err(|error| StoreError::Worker(error.to_string()))?
        })
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

impl JobStore for SqliteJobStore {
    fn create_inspection(&self, inspection: NewInspection) -> StoreFuture<'_, Inspection> {
        self.run(move |connection| {
            let id = Uuid::new_v4();
            connection
                .execute(
                    "INSERT INTO inspections(
                        id, source_uri, source_kind, status, created_at_ms, updated_at_ms
                    ) VALUES (?1, ?2, ?3, 'pending', ?4, ?4)",
                    params![
                        id.to_string(),
                        inspection.source.uri(),
                        inspection.source.kind().to_string(),
                        inspection.created_at_ms,
                    ],
                )
                .map_err(database_error)?;
            get_inspection(connection, id)
        })
    }

    fn complete_inspection(&self, inspection: CompletedInspection) -> StoreFuture<'_, Inspection> {
        self.run(move |connection| {
            if inspection.schema_fingerprint.trim().is_empty() {
                return Err(StoreError::InvalidInput(
                    "schema fingerprint cannot be empty".to_owned(),
                ));
            }
            let changed = connection
                .execute(
                    "UPDATE inspections
                     SET status = 'ready', schema_fingerprint = ?2, error = NULL, updated_at_ms = ?3
                     WHERE id = ?1 AND status = 'pending'",
                    params![
                        inspection.id.to_string(),
                        inspection.schema_fingerprint,
                        inspection.completed_at_ms,
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::NotFound);
            }
            get_inspection(connection, inspection.id)
        })
    }

    fn get_inspection(&self, id: Uuid) -> StoreFuture<'_, Inspection> {
        self.run(move |connection| get_inspection(connection, id))
    }

    fn create_job(&self, job: NewJob) -> StoreFuture<'_, Job> {
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let inspection = get_inspection(&transaction, job.inspection_id)?;
            if inspection.status != InspectionStatus::Ready {
                return Err(StoreError::InspectionNotReady);
            }
            if job.kind == JobKind::Move && inspection.source_kind == LocationKind::HuggingFace {
                return Err(StoreError::UnsupportedMoveSource);
            }
            let source = DatasetLocation::parse_source(inspection.source_uri.clone())
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
            if source.overlaps(&job.destination) {
                return Err(StoreError::InvalidInput(
                    "source and destination prefixes must not overlap".to_owned(),
                ));
            }
            let active_jobs = {
                let mut statement = transaction
                    .prepare(
                        "SELECT source_uri, destination_uri, kind FROM jobs
                         WHERE status IN (
                            'queued', 'running', 'validating', 'publishing', 'deleting_source'
                         )",
                    )
                    .map_err(database_error)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(database_error)?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(database_error)?
            };
            for (active_source_uri, active_destination_uri, active_kind) in active_jobs {
                let active_source = DatasetLocation::parse_source(active_source_uri)
                    .map_err(|error| StoreError::Database(error.to_string()))?;
                let active_destination = DatasetLocation::parse_destination(active_destination_uri)
                    .map_err(|error| StoreError::Database(error.to_string()))?;
                let active_kind = JobKind::from_str(&active_kind).map_err(StoreError::Database)?;
                let paths_conflict = job.destination.overlaps(&active_destination)
                    || job.destination.overlaps(&active_source)
                    || source.overlaps(&active_destination)
                    || ((job.kind == JobKind::Move || active_kind == JobKind::Move)
                        && source.overlaps(&active_source));
                if paths_conflict {
                    return Err(StoreError::Conflict(
                        "source or destination overlaps an active job".to_owned(),
                    ));
                }
            }

            let id = Uuid::new_v4();
            transaction
                .execute(
                    "INSERT INTO jobs(
                        id, inspection_id, kind, source_uri, destination_uri,
                        schema_fingerprint, status, submitted_at_ms, updated_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'queued', ?7, ?7)",
                    params![
                        id.to_string(),
                        job.inspection_id.to_string(),
                        job.kind.to_string(),
                        inspection.source_uri,
                        job.destination.uri(),
                        inspection.schema_fingerprint,
                        job.submitted_at_ms,
                    ],
                )
                .map_err(job_insert_error)?;
            transaction.commit().map_err(database_error)?;
            get_job(connection, id)
        })
    }

    fn get_job(&self, id: Uuid) -> StoreFuture<'_, Job> {
        self.run(move |connection| get_job(connection, id))
    }

    fn list_jobs(&self, limit: usize) -> StoreFuture<'_, Vec<Job>> {
        self.run(move |connection| {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let sql = format!(
                "SELECT {JOB_COLUMNS} FROM jobs ORDER BY submitted_at_ms DESC, id DESC LIMIT ?1"
            );
            let mut statement = connection.prepare(&sql).map_err(database_error)?;
            let rows = statement
                .query_map([usize_to_i64(limit)?], row_to_job)
                .map_err(database_error)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(database_error)
        })
    }

    fn claim_jobs(
        &self,
        owner: String,
        limit: usize,
        lease_duration_ms: i64,
    ) -> StoreFuture<'_, Vec<ClaimedJob>> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            if owner.trim().is_empty() {
                return Err(StoreError::InvalidInput(
                    "lease owner cannot be empty".to_owned(),
                ));
            }
            if limit == 0 || lease_duration_ms <= 0 {
                return Ok(Vec::new());
            }
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            let lease_expires_at_ms = now_ms
                .checked_add(lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            let ids = {
                let mut statement = transaction
                    .prepare(
                        "SELECT id FROM jobs
                         WHERE status = 'queued'
                            OR (status = 'running' AND lease_expires_at_ms <= ?1)
                         ORDER BY submitted_at_ms, id
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

            let mut claimed = Vec::with_capacity(ids.len());
            for id in ids {
                let lease_token = Uuid::new_v4();
                transaction
                    .execute(
                        "UPDATE jobs
                         SET status = 'running',
                             lease_owner = ?2,
                             lease_token = ?3,
                             lease_expires_at_ms = ?4,
                             attempt = attempt + 1,
                             updated_at_ms = ?5
                         WHERE id = ?1",
                        params![
                            id,
                            owner,
                            lease_token.to_string(),
                            lease_expires_at_ms,
                            now_ms,
                        ],
                    )
                    .map_err(database_error)?;
                let job_id = parse_uuid(&id, 0).map_err(database_error)?;
                let job = get_job(&transaction, job_id)?;
                claimed.push(ClaimedJob { job, lease_token });
            }
            transaction.commit().map_err(database_error)?;
            Ok(claimed)
        })
    }

    fn renew_lease(&self, update: LeaseUpdate) -> StoreFuture<'_, Job> {
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
                update.job_id,
                update.lease_token,
                now_ms,
                update.progress,
            )?;
            let lease_expires_at_ms = now_ms
                .checked_add(update.lease_duration_ms)
                .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET lease_expires_at_ms = ?3,
                         updated_at_ms = ?4,
                         source_bytes_read = MAX(source_bytes_read, ?5),
                         lance_bytes_written = MAX(lance_bytes_written, ?6),
                         rows_read = MAX(rows_read, ?7),
                         rows_written = MAX(rows_written, ?8),
                         work_units_completed = MAX(work_units_completed, ?9),
                         work_units_total = MAX(work_units_total, ?10)
                     WHERE id = ?1
                       AND status = 'running'
                       AND lease_token = ?2
                       AND lease_expires_at_ms > ?4",
                    params![
                        update.job_id.to_string(),
                        update.lease_token.to_string(),
                        lease_expires_at_ms,
                        now_ms,
                        progress[0],
                        progress[1],
                        progress[2],
                        progress[3],
                        progress[4],
                        progress[5],
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            let job = get_job(&transaction, update.job_id)?;
            transaction.commit().map_err(database_error)?;
            Ok(job)
        })
    }

    fn checkpoint_progress(&self, update: ProgressUpdate) -> StoreFuture<'_, Job> {
        let clock = Arc::clone(&self.clock);
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(database_error)?;
            let now_ms = clock.now_ms()?;
            validate_progress_update(
                &transaction,
                update.job_id,
                update.lease_token,
                now_ms,
                update.progress,
            )?;
            let progress = progress_as_i64(update.progress)?;
            let changed = transaction
                .execute(
                    "UPDATE jobs
                     SET updated_at_ms = ?3,
                         source_bytes_read = MAX(source_bytes_read, ?4),
                         lance_bytes_written = MAX(lance_bytes_written, ?5),
                         rows_read = MAX(rows_read, ?6),
                         rows_written = MAX(rows_written, ?7),
                         work_units_completed = MAX(work_units_completed, ?8),
                         work_units_total = MAX(work_units_total, ?9)
                     WHERE id = ?1
                       AND status = 'running'
                       AND lease_token = ?2
                       AND lease_expires_at_ms > ?3",
                    params![
                        update.job_id.to_string(),
                        update.lease_token.to_string(),
                        now_ms,
                        progress[0],
                        progress[1],
                        progress[2],
                        progress[3],
                        progress[4],
                        progress[5],
                    ],
                )
                .map_err(database_error)?;
            if changed == 0 {
                return Err(StoreError::LeaseLost);
            }
            let job = get_job(&transaction, update.job_id)?;
            transaction.commit().map_err(database_error)?;
            Ok(job)
        })
    }
}

fn validate_progress_update(
    connection: &Connection,
    job_id: Uuid,
    lease_token: Uuid,
    now_ms: i64,
    incoming: JobProgress,
) -> Result<(), StoreError> {
    let job = get_job(connection, job_id)?;
    if job.status != JobStatus::Running
        || job.lease_token != Some(lease_token)
        || job
            .lease_expires_at_ms
            .is_none_or(|expiry| expiry <= now_ms)
    {
        return Err(StoreError::LeaseLost);
    }
    let effective_completed = job
        .progress
        .work_units_completed
        .max(incoming.work_units_completed);
    let effective_total = job.progress.work_units_total.max(incoming.work_units_total);
    if effective_total > 0 && effective_completed > effective_total {
        return Err(StoreError::InvalidInput(
            "completed work units exceed total work units".to_owned(),
        ));
    }
    Ok(())
}

fn get_inspection(connection: &Connection, id: Uuid) -> Result<Inspection, StoreError> {
    let sql = format!("SELECT {INSPECTION_COLUMNS} FROM inspections WHERE id = ?1");
    connection
        .query_row(&sql, [id.to_string()], row_to_inspection)
        .optional()
        .map_err(database_error)?
        .ok_or(StoreError::NotFound)
}

fn get_job(connection: &Connection, id: Uuid) -> Result<Job, StoreError> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1");
    connection
        .query_row(&sql, [id.to_string()], row_to_job)
        .optional()
        .map_err(database_error)?
        .ok_or(StoreError::NotFound)
}

fn row_to_inspection(row: &Row<'_>) -> rusqlite::Result<Inspection> {
    Ok(Inspection {
        id: parse_uuid(&row.get::<_, String>(0)?, 0)?,
        source_uri: row.get(1)?,
        source_kind: parse_value(&row.get::<_, String>(2)?, 2)?,
        status: parse_value(&row.get::<_, String>(3)?, 3)?,
        schema_fingerprint: row.get(4)?,
        error: row.get(5)?,
        created_at_ms: row.get(6)?,
        updated_at_ms: row.get(7)?,
    })
}

fn row_to_job(row: &Row<'_>) -> rusqlite::Result<Job> {
    let lease_token = row
        .get::<_, Option<String>>(11)?
        .map(|value| parse_uuid(&value, 11))
        .transpose()?;
    Ok(Job {
        id: parse_uuid(&row.get::<_, String>(0)?, 0)?,
        inspection_id: parse_uuid(&row.get::<_, String>(1)?, 1)?,
        kind: parse_value(&row.get::<_, String>(2)?, 2)?,
        source_uri: row.get(3)?,
        destination_uri: row.get(4)?,
        schema_fingerprint: row.get(5)?,
        status: parse_value(&row.get::<_, String>(6)?, 6)?,
        submitted_at_ms: row.get(7)?,
        updated_at_ms: row.get(8)?,
        attempt: i64_to_u32(row.get(9)?, 9)?,
        lease_owner: row.get(10)?,
        lease_token,
        lease_expires_at_ms: row.get(12)?,
        progress: JobProgress {
            source_bytes_read: i64_to_u64(row.get(13)?, 13)?,
            lance_bytes_written: i64_to_u64(row.get(14)?, 14)?,
            rows_read: i64_to_u64(row.get(15)?, 15)?,
            rows_written: i64_to_u64(row.get(16)?, 16)?,
            work_units_completed: i64_to_u64(row.get(17)?, 17)?,
            work_units_total: i64_to_u64(row.get(18)?, 18)?,
        },
    })
}

fn parse_uuid(value: &str, index: usize) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(value).map_err(|error| conversion_error(index, Type::Text, error))
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

fn progress_as_i64(progress: JobProgress) -> Result<[i64; 6], StoreError> {
    [
        progress.source_bytes_read,
        progress.lance_bytes_written,
        progress.rows_read,
        progress.rows_written,
        progress.work_units_completed,
        progress.work_units_total,
    ]
    .map(|value| i64::try_from(value).map_err(|error| StoreError::InvalidInput(error.to_string())))
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?
    .try_into()
    .map_err(|_| StoreError::Worker("invalid progress field count".to_owned()))
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
        StoreError::Conflict("destination is already reserved by an active job".to_owned())
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

    use crate::{
        domain::{
            CompletedInspection, JobKind, JobProgress, LeaseUpdate, NewInspection, NewJob,
            ProgressUpdate,
        },
        location::DatasetLocation,
        store::{JobStore, StoreError},
    };

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

    async fn ready_inspection(
        store: &SqliteJobStore,
        source_uri: &str,
        timestamp: i64,
    ) -> crate::domain::Inspection {
        let inspection = store
            .create_inspection(NewInspection {
                source: DatasetLocation::parse_source(source_uri).unwrap(),
                created_at_ms: timestamp,
            })
            .await
            .unwrap();
        store
            .complete_inspection(CompletedInspection {
                id: inspection.id,
                schema_fingerprint: format!("schema-{timestamp}"),
                completed_at_ms: timestamp + 1,
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn claims_and_reclaims_expired_jobs_with_fencing_tokens() {
        let clock = Arc::new(TestClock::new(10));
        let store = SqliteJobStore::open_with_clock(":memory:", clock.clone()).unwrap();
        let inspection = store
            .create_inspection(NewInspection {
                source: DatasetLocation::parse_source("s3://source-bucket/data").unwrap(),
                created_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .complete_inspection(CompletedInspection {
                id: inspection.id,
                schema_fingerprint: "schema-v1".to_owned(),
                completed_at_ms: 2,
            })
            .await
            .unwrap();
        let job = store
            .create_job(NewJob {
                inspection_id: inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/data.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap();

        let first_claim = store
            .claim_jobs("worker-a".to_owned(), 1, 100)
            .await
            .unwrap();
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].job.id, job.id);
        assert_eq!(first_claim[0].job.attempt, 1);

        clock.set(111);
        let expired_error = store
            .renew_lease(LeaseUpdate {
                job_id: job.id,
                lease_token: first_claim[0].lease_token,
                lease_duration_ms: 100,
                progress: JobProgress::default(),
            })
            .await
            .unwrap_err();
        assert!(matches!(expired_error, StoreError::LeaseLost));

        let reclaimed = store
            .claim_jobs("worker-b".to_owned(), 1, 100)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].job.attempt, 2);
        assert_ne!(reclaimed[0].lease_token, first_claim[0].lease_token);

        clock.set(112);
        let error = store
            .renew_lease(LeaseUpdate {
                job_id: job.id,
                lease_token: first_claim[0].lease_token,
                lease_duration_ms: 188,
                progress: JobProgress::default(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::LeaseLost));
    }

    #[tokio::test]
    async fn heartbeat_updates_monotonic_progress() {
        let clock = Arc::new(TestClock::new(10));
        let store = SqliteJobStore::open_with_clock(":memory:", clock.clone()).unwrap();
        let inspection = store
            .create_inspection(NewInspection {
                source: DatasetLocation::parse_source("nfs:///datasets/source").unwrap(),
                created_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .complete_inspection(CompletedInspection {
                id: inspection.id,
                schema_fingerprint: "schema-v1".to_owned(),
                completed_at_ms: 2,
            })
            .await
            .unwrap();
        let job = store
            .create_job(NewJob {
                inspection_id: inspection.id,
                kind: JobKind::Move,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/data.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap();
        let claim = store
            .claim_jobs("worker-a".to_owned(), 1, 1_000)
            .await
            .unwrap()
            .remove(0);

        let progress = JobProgress {
            source_bytes_read: 100,
            lance_bytes_written: 80,
            rows_read: 10,
            rows_written: 10,
            work_units_completed: 1,
            work_units_total: 2,
        };
        clock.set(20);
        let checkpointed = store
            .checkpoint_progress(ProgressUpdate {
                job_id: job.id,
                lease_token: claim.lease_token,
                progress,
            })
            .await
            .unwrap();
        assert_eq!(checkpointed.lease_expires_at_ms, Some(1_010));

        clock.set(21);
        let updated = store
            .renew_lease(LeaseUpdate {
                job_id: job.id,
                lease_token: claim.lease_token,
                lease_duration_ms: 1_979,
                progress: JobProgress::default(),
            })
            .await
            .unwrap();
        assert_eq!(updated.progress.rows_written, 10);
        assert_eq!(updated.lease_expires_at_ms, Some(2_000));

        clock.set(22);
        let error = store
            .checkpoint_progress(ProgressUpdate {
                job_id: job.id,
                lease_token: claim.lease_token,
                progress: JobProgress {
                    work_units_completed: 3,
                    work_units_total: 0,
                    ..JobProgress::default()
                },
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidInput(_)));
        assert_eq!(
            store
                .get_job(job.id)
                .await
                .unwrap()
                .progress
                .work_units_completed,
            1
        );
    }

    #[tokio::test]
    async fn rejects_overlapping_move_paths() {
        let store = SqliteJobStore::open(":memory:").unwrap();
        let inspection = ready_inspection(&store, "s3://source-bucket/data", 1).await;
        let error = store
            .create_job(NewJob {
                inspection_id: inspection.id,
                kind: JobKind::Move,
                destination: DatasetLocation::parse_destination(
                    "s3://source-bucket/data/output.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn active_move_reserves_its_source() {
        let store = SqliteJobStore::open(":memory:").unwrap();
        let move_inspection = ready_inspection(&store, "nfs:///datasets/source", 1).await;
        store
            .create_job(NewJob {
                inspection_id: move_inspection.id,
                kind: JobKind::Move,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/first.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap();

        let copy_inspection = ready_inspection(&store, "nfs:///datasets/source/child", 4).await;
        let error = store
            .create_job(NewJob {
                inspection_id: copy_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/second.lance",
                )
                .unwrap(),
                submitted_at_ms: 6,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        let store = SqliteJobStore::open(":memory:").unwrap();
        let copy_inspection = ready_inspection(&store, "nfs:///datasets/source/child", 10).await;
        store
            .create_job(NewJob {
                inspection_id: copy_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/third.lance",
                )
                .unwrap(),
                submitted_at_ms: 12,
            })
            .await
            .unwrap();
        let move_inspection = ready_inspection(&store, "nfs:///datasets/source", 13).await;
        let error = store
            .create_job(NewJob {
                inspection_id: move_inspection.id,
                kind: JobKind::Move,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/fourth.lance",
                )
                .unwrap(),
                submitted_at_ms: 15,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));
    }

    #[tokio::test]
    async fn reserves_overlapping_active_sources_and_destinations() {
        let store = SqliteJobStore::open(":memory:").unwrap();
        let first_inspection = ready_inspection(&store, "s3://source-bucket/data", 1).await;
        store
            .create_job(NewJob {
                inspection_id: first_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/data.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap();

        let nested_destination_inspection =
            ready_inspection(&store, "nfs:///datasets/other", 4).await;
        let error = store
            .create_job(NewJob {
                inspection_id: nested_destination_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/data.lance/nested",
                )
                .unwrap(),
                submitted_at_ms: 6,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        let destination_over_source_inspection =
            ready_inspection(&store, "nfs:///datasets/third", 7).await;
        let error = store
            .create_job(NewJob {
                inspection_id: destination_over_source_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://source-bucket/data/output.lance",
                )
                .unwrap(),
                submitted_at_ms: 9,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));

        let source_over_destination_inspection =
            ready_inspection(&store, "s3://destination-bucket/data.lance/source", 10).await;
        let error = store
            .create_job(NewJob {
                inspection_id: source_over_destination_inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://another-destination/output.lance",
                )
                .unwrap(),
                submitted_at_ms: 12,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::Conflict(_)));
    }

    #[tokio::test]
    async fn progress_invariant_is_atomic_across_connections() {
        let path = std::env::temp_dir().join(format!("lance-service-{}.db", uuid::Uuid::new_v4()));
        let clock = Arc::new(TestClock::new(10));
        let first = SqliteJobStore::open_with_clock(&path, clock.clone()).unwrap();
        let second = SqliteJobStore::open_with_clock(&path, clock.clone()).unwrap();
        let inspection = ready_inspection(&first, "nfs:///datasets/source", 1).await;
        let job = first
            .create_job(NewJob {
                inspection_id: inspection.id,
                kind: JobKind::Copy,
                destination: DatasetLocation::parse_destination(
                    "s3://destination-bucket/atomic.lance",
                )
                .unwrap(),
                submitted_at_ms: 3,
            })
            .await
            .unwrap();
        let claim = first
            .claim_jobs("worker-a".to_owned(), 1, 1_000)
            .await
            .unwrap()
            .remove(0);

        clock.set(20);
        let completed_update = first.checkpoint_progress(ProgressUpdate {
            job_id: job.id,
            lease_token: claim.lease_token,
            progress: JobProgress {
                work_units_completed: 10,
                work_units_total: 0,
                ..JobProgress::default()
            },
        });
        let total_update = second.checkpoint_progress(ProgressUpdate {
            job_id: job.id,
            lease_token: claim.lease_token,
            progress: JobProgress {
                work_units_completed: 0,
                work_units_total: 5,
                ..JobProgress::default()
            },
        });
        let (completed_result, total_result) = tokio::join!(completed_update, total_update);
        assert_ne!(completed_result.is_ok(), total_result.is_ok());
        let error = completed_result
            .err()
            .or_else(|| total_result.err())
            .unwrap();
        assert!(matches!(error, StoreError::InvalidInput(_)));

        let progress = first.get_job(job.id).await.unwrap().progress;
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

    #[test]
    fn migrations_are_idempotent_across_reopen() {
        let path = std::env::temp_dir().join(format!("lance-service-{}.db", uuid::Uuid::new_v4()));
        SqliteJobStore::open(&path).unwrap();
        SqliteJobStore::open(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
