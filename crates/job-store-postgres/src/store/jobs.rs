use async_trait::async_trait;
use sqlx::{Postgres, QueryBuilder, Row, Transaction};

use lance_conversion_core::job::{
    CompletionUpdate, FailureUpdate, Job, JobError, JobProgress, JobStatus, LeaseUpdate,
    MAX_JOB_ATTEMPTS, NewJob, ProgressUpdate,
};
use lance_job_store::{JobOrderField, JobQuery, JobStore, StoreError};

use super::{
    PostgresJobStore, database_error, job_insert_error,
    row::{SELECT_JOBS_SQL, load_job, row_to_job},
    types::{PgBlobColumnSpec, PgIndexSpec, PgJobError, PgJobProgress, PgJobStatus},
    usize_to_i64,
};

#[async_trait]
impl JobStore for PostgresJobStore {
    async fn create_job(&self, job: NewJob) -> Result<(), StoreError> {
        let blob_columns = job
            .blob_columns
            .into_iter()
            .map(PgBlobColumnSpec::from)
            .collect::<Vec<_>>();
        let indices = job
            .indices
            .into_iter()
            .map(PgIndexSpec::from)
            .collect::<Vec<_>>();
        let mut transaction = self.begin().await?;
        sqlx::query(
            "INSERT INTO jobs(
                creator, source_uri, destination_uri, status,
                creation_timestamp_ms, update_timestamp_ms, blob_columns, indices
             ) VALUES ($1, $2, $3, 'queuing', $4, $4, $5, $6)",
        )
        .bind(job.creator)
        .bind(job.source.uri())
        .bind(job.destination.uri())
        .bind(job.creation_timestamp_ms)
        .bind(blob_columns)
        .bind(indices)
        .execute(&mut *transaction)
        .await
        .map_err(job_insert_error)?;
        transaction.commit().await.map_err(database_error)
    }

    async fn get_job(&self, destination_uri: &str) -> Result<Job, StoreError> {
        let mut connection = self.pool.acquire().await.map_err(database_error)?;
        load_job(&mut connection, destination_uri).await
    }

    async fn query_jobs(&self, query: JobQuery) -> Result<Vec<Job>, StoreError> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        if query.failed_only && query.ongoing_only {
            return Err(StoreError::InvalidInput(
                "failed-only and ongoing-only filters are mutually exclusive".to_owned(),
            ));
        }
        if let (Some(from), Some(to)) = (
            query.creation_timestamp_ms_from,
            query.creation_timestamp_ms_to,
        ) && from > to
        {
            return Err(StoreError::InvalidInput(
                "creation timestamp lower bound exceeds upper bound".to_owned(),
            ));
        }

        let mut sql = QueryBuilder::<Postgres>::new(SELECT_JOBS_SQL);
        sql.push(" WHERE 1 = 1");
        if let Some(creator) = query.creator {
            sql.push(" AND creator = ").push_bind(creator);
        }
        if query.failed_only {
            sql.push(" AND status = 'failed'");
        } else if query.ongoing_only {
            sql.push(" AND status IN ('queuing', 'running')");
        }
        if let Some(timestamp) = query.creation_timestamp_ms_from {
            sql.push(" AND creation_timestamp_ms >= ")
                .push_bind(timestamp);
        }
        if let Some(timestamp) = query.creation_timestamp_ms_to {
            sql.push(" AND creation_timestamp_ms <= ")
                .push_bind(timestamp);
        }
        sql.push(" ORDER BY ");
        match query.order_by {
            JobOrderField::CreationTimestamp => {
                sql.push("creation_timestamp_ms");
            }
            JobOrderField::UpdateTimestamp => {
                sql.push("update_timestamp_ms");
            }
        }
        if query.descending {
            sql.push(" DESC, destination_uri DESC");
        } else {
            sql.push(" ASC, destination_uri ASC");
        }
        sql.push(" LIMIT ").push_bind(usize_to_i64(query.limit)?);
        let rows = sql
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(database_error)?;
        rows.iter().map(row_to_job).collect()
    }

    async fn claim_jobs(
        &self,
        limit: usize,
        convert_lease_duration_ms: i64,
    ) -> Result<Vec<Job>, StoreError> {
        if limit == 0 || convert_lease_duration_ms <= 0 {
            return Ok(Vec::new());
        }

        let mut transaction = self.begin().await?;
        let now_ms = self.clock.now_ms()?;
        let lease_expiration_timestamp_ms = now_ms
            .checked_add(convert_lease_duration_ms)
            .ok_or_else(|| StoreError::InvalidInput("lease timestamp overflow".to_owned()))?;

        let destination_rows = sqlx::query(
            "SELECT destination_uri FROM jobs
             WHERE (status = 'queuing' AND attempt < $3)
                OR (status = 'running' AND lease_expiration_timestamp_ms <= $1)
             ORDER BY creation_timestamp_ms, destination_uri
             LIMIT $2
             FOR UPDATE SKIP LOCKED",
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
            let job = load_job(&mut transaction, &destination_uri).await?;
            // The last worker died or missed lease renewal on the final attempt.
            // There is no retry left, so mark this job failed instead of reclaiming it.
            if job.status == JobStatus::Running && job.attempt >= MAX_JOB_ATTEMPTS {
                sqlx::query(
                    "UPDATE jobs
                     SET status = 'failed',
                         update_timestamp_ms = $2,
                         lease_expiration_timestamp_ms = NULL,
                         error_reasons = error_reasons || ARRAY[(
                             attempt,
                             $2,
                             'lease expired on final attempt'
                         )::job_error]
                     WHERE destination_uri = $1",
                )
                .bind(&destination_uri)
                .bind(now_ms)
                .execute(&mut *transaction)
                .await
                .map_err(database_error)?;
                continue;
            }
            sqlx::query(
                "UPDATE jobs
                 SET status = 'running',
                     lease_expiration_timestamp_ms = $2,
                     attempt = attempt + 1,
                     update_timestamp_ms = $3,
                     error_reasons = CASE
                         WHEN status = 'running' THEN error_reasons || ARRAY[(
                             attempt,
                             $3,
                             'lease expired before completion'
                         )::job_error]
                         ELSE error_reasons
                     END
                 WHERE destination_uri = $1",
            )
            .bind(&destination_uri)
            .bind(lease_expiration_timestamp_ms)
            .bind(now_ms)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            claimed.push(load_job(&mut transaction, &destination_uri).await?);
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

        let mut transaction = self.begin().await?;
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
        let progress = PgJobProgress::try_from(update.progress)?;
        let changed = sqlx::query(
            "UPDATE jobs
             SET lease_expiration_timestamp_ms = $3,
                 update_timestamp_ms = $4,
                 progress = $5
             WHERE destination_uri = $1
               AND status = 'running'
               AND attempt = $2
               AND lease_expiration_timestamp_ms > $4",
        )
        .bind(&update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(lease_expiration_timestamp_ms)
        .bind(now_ms)
        .bind(progress)
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
        let mut transaction = self.begin().await?;
        let now_ms = self.clock.now_ms()?;
        validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
        let progress = PgJobProgress::try_from(update.progress)?;
        let changed = sqlx::query(
            "UPDATE jobs
             SET update_timestamp_ms = $3,
                 progress = $4
             WHERE destination_uri = $1
               AND status = 'running'
               AND attempt = $2
               AND lease_expiration_timestamp_ms > $3",
        )
        .bind(&update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(now_ms)
        .bind(progress)
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
        let mut transaction = self.begin().await?;
        let now_ms = self.clock.now_ms()?;
        validate_progress_update(
            &mut transaction,
            &update.destination_uri,
            update.attempt,
            now_ms,
            update.progress,
        )
        .await?;
        let progress = PgJobProgress::try_from(update.progress)?;
        let changed = sqlx::query(
            "UPDATE jobs
             SET status = 'succeeded',
                 update_timestamp_ms = $3,
                 lease_expiration_timestamp_ms = NULL,
                 progress = $4
             WHERE destination_uri = $1
               AND status = 'running'
               AND attempt = $2
               AND lease_expiration_timestamp_ms > $3",
        )
        .bind(update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(now_ms)
        .bind(progress)
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
        let mut transaction = self.begin().await?;
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
        let error_reasons = job
            .error_reasons
            .into_iter()
            .map(PgJobError::from)
            .collect::<Vec<_>>();
        let status = if update.attempt >= MAX_JOB_ATTEMPTS {
            JobStatus::Failed
        } else {
            JobStatus::Queuing
        };
        let progress = PgJobProgress::try_from(update.progress)?;
        let changed = sqlx::query(
            "UPDATE jobs
             SET status = $3,
                 update_timestamp_ms = $4,
                 error_reasons = $5,
                 lease_expiration_timestamp_ms = NULL,
                 progress = $6
             WHERE destination_uri = $1
               AND status = 'running'
               AND attempt = $2
               AND lease_expiration_timestamp_ms > $4",
        )
        .bind(update.destination_uri)
        .bind(i64::from(update.attempt))
        .bind(PgJobStatus::from(status))
        .bind(now_ms)
        .bind(error_reasons)
        .bind(progress)
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
    transaction: &mut Transaction<'_, Postgres>,
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
        && (incoming.rows_read > incoming.rows_total
            || incoming.rows_written > incoming.rows_total
            || incoming.rows_missing_blobs > incoming.rows_total)
    {
        return Err(StoreError::InvalidInput(
            "read, written, or missing-blob rows exceed total rows".to_owned(),
        ));
    }
    Ok(job)
}
