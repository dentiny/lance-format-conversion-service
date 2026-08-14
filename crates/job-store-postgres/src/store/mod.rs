mod jobs;
mod row;
mod types;

use std::{
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};

use lance_job_store::StoreError;

const DEFAULT_MAX_CONNECTIONS: u32 = 8;

#[derive(Clone)]
pub struct PostgresJobStore {
    pool: PgPool,
    clock: Arc<dyn Clock>,
}

impl PostgresJobStore {
    /// Opens a `PostgreSQL` job store.
    ///
    /// The schema is applied out of band (Terraform). This only connects.
    ///
    /// # Errors
    ///
    /// Returns an error when `PostgreSQL` cannot be reached or configured.
    pub async fn open(database_url: &str) -> Result<Self, StoreError> {
        Self::connect(
            database_url,
            Arc::new(SystemClock),
            DEFAULT_MAX_CONNECTIONS,
            None,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn open_with_clock(
        database_url: &str,
        clock: Arc<dyn Clock>,
        schema: &str,
    ) -> Result<Self, StoreError> {
        let store = Self::connect(database_url, clock, 1, Some(schema)).await?;
        sqlx::raw_sql(include_str!("../../migrations/0001_initial.sql"))
            .execute(&store.pool)
            .await
            .map_err(database_error)?;
        Ok(store)
    }

    async fn connect(
        database_url: &str,
        clock: Arc<dyn Clock>,
        max_connections: u32,
        schema: Option<&str>,
    ) -> Result<Self, StoreError> {
        let search_path = match schema {
            Some(schema) => {
                create_schema(database_url, schema).await?;
                Some(format!("{}, public", quote_ident(schema)?))
            }
            None => None,
        };
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .after_connect(move |connection, _metadata| {
                let search_path = search_path.clone();
                Box::pin(async move {
                    if let Some(search_path) = search_path.as_deref() {
                        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                            "SET search_path TO {search_path}"
                        )))
                        .execute(&mut *connection)
                        .await?;
                    }
                    Ok(())
                })
            })
            .connect(database_url)
            .await
            .map_err(database_error)?;

        Ok(Self { pool, clock })
    }

    async fn begin(&self) -> Result<Transaction<'static, Postgres>, StoreError> {
        self.pool.begin().await.map_err(database_error)
    }

    #[cfg(test)]
    pub(super) async fn get_job(
        &self,
        destination_uri: &str,
    ) -> Result<lance_conversion_core::job::Job, StoreError> {
        let mut connection = self.pool.acquire().await.map_err(database_error)?;
        row::load_job(&mut connection, destination_uri).await
    }
}

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> Result<i64, StoreError>;
}

pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Result<i64, StoreError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| StoreError::Worker(error.to_string()))?
            .as_millis();
        i64::try_from(millis).map_err(|error| StoreError::Worker(error.to_string()))
    }
}

async fn create_schema(database_url: &str, schema: &str) -> Result<(), StoreError> {
    let ident = quote_ident(schema)?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .map_err(database_error)?;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {ident}")))
        .execute(&pool)
        .await
        .map_err(database_error)?;
    pool.close().await;
    Ok(())
}

fn quote_ident(name: &str) -> Result<String, StoreError> {
    let valid = !name.is_empty()
        && !name.starts_with(|character: char| character.is_ascii_digit())
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        });
    if valid {
        Ok(format!("\"{name}\""))
    } else {
        Err(StoreError::InvalidInput(
            "schema name must be a lowercase SQL identifier".to_owned(),
        ))
    }
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
