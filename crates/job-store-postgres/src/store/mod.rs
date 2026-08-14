mod jobs;
mod row;
mod types;

use std::{str::FromStr, sync::Arc};

use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};

use lance_job_store::{Clock, StoreError, SystemClock};

#[derive(Clone)]
pub struct PostgresJobStore {
    pool: PgPool,
    clock: Arc<dyn Clock>,
}

impl PostgresJobStore {
    /// Opens a `PostgreSQL` job store with a bounded connection pool.
    ///
    /// The schema is applied out of band (Terraform). This only connects.
    ///
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` cannot be reached or configured.
    pub async fn open(database_url: &str, max_connections: u32) -> Result<Self, StoreError> {
        Self::connect(database_url, Arc::new(SystemClock), max_connections).await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn open_with_clock(
        database_url: &str,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let store = Self::connect(database_url, clock, 1).await?;
        apply_test_schema(&store.pool).await?;
        Ok(store)
    }

    async fn connect(
        database_url: &str,
        clock: Arc<dyn Clock>,
        max_connections: u32,
    ) -> Result<Self, StoreError> {
        if max_connections == 0 {
            return Err(StoreError::InvalidInput(
                "max connections must be at least 1".to_owned(),
            ));
        }
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .map_err(database_error)?;

        Ok(Self { pool, clock })
    }

    async fn begin(&self) -> Result<Transaction<'static, Postgres>, StoreError> {
        self.pool.begin().await.map_err(database_error)
    }
}

#[cfg(any(test, feature = "test-utils"))]
async fn apply_test_schema(pool: &PgPool) -> Result<(), StoreError> {
    let jobs_exist = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_tables
            WHERE schemaname = 'public' AND tablename = 'jobs'
        )",
    )
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if !jobs_exist {
        sqlx::raw_sql(include_str!("../../migrations/0001_initial.sql"))
            .execute(pool)
            .await
            .map_err(database_error)?;
    }
    sqlx::query("TRUNCATE TABLE jobs")
        .execute(pool)
        .await
        .map_err(database_error)?;
    Ok(())
}

fn parse_value<T>(value: &str) -> Result<T, StoreError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    T::from_str(value).map_err(|error| StoreError::Database(error.to_string()))
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
