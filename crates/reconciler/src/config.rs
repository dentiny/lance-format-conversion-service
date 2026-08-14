use std::num::{NonZeroU64, NonZeroUsize};

use lance_conversion_core::env::{self, EnvError};
use lance_converter::ConverterConfig;
use thiserror::Error;

const DATABASE_URL_ENV: &str = "DATABASE_URL";
const WORKER_COUNT_ENV: &str = "WORKER_COUNT";
const POLL_INTERVAL_MS_ENV: &str = "POLL_INTERVAL_MS";
const LEASE_DURATION_SECS_ENV: &str = "LEASE_DURATION_SECS";
const LEASE_RENEW_INTERVAL_SECS_ENV: &str = "LEASE_RENEW_INTERVAL_SECS";
const PROGRESS_INTERVAL_SECS_ENV: &str = "PROGRESS_INTERVAL_SECS";
const TARGET_LANCE_FILE_SIZE_MIB_ENV: &str = "TARGET_LANCE_FILE_SIZE_MIB";
const BLOB_INLINE_THRESHOLD_MIB_ENV: &str = "BLOB_INLINE_THRESHOLD_MIB";
const BLOB_DEDICATED_THRESHOLD_MIB_ENV: &str = "BLOB_DEDICATED_THRESHOLD_MIB";

/// Default job-store database URL.
pub const DEFAULT_DATABASE_URL: &str = "postgres://127.0.0.1:5432/lance_jobs";
/// Default number of conversion workers.
pub const DEFAULT_WORKER_COUNT: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// Default queue polling interval in milliseconds.
pub const DEFAULT_POLL_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(1_000).unwrap();
/// Default lease duration in seconds.
pub const DEFAULT_LEASE_DURATION_SECS: NonZeroU64 = NonZeroU64::new(900).unwrap();
/// Default lease renewal interval in seconds.
pub const DEFAULT_LEASE_RENEW_INTERVAL_SECS: NonZeroU64 = NonZeroU64::new(180).unwrap();
/// Default durable progress checkpoint interval in seconds.
pub const DEFAULT_PROGRESS_INTERVAL_SECS: NonZeroU64 = NonZeroU64::new(30).unwrap();
/// Default soft target size for generated Lance data files in MiB.
pub const DEFAULT_TARGET_LANCE_FILE_SIZE_MIB: NonZeroU64 = NonZeroU64::new(512).unwrap();
/// Default Blob V2 inline payload threshold in MiB.
pub const DEFAULT_BLOB_INLINE_THRESHOLD_MIB: NonZeroU64 = NonZeroU64::new(2).unwrap();
/// Default Blob V2 dedicated-file payload threshold in MiB.
pub const DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB: NonZeroU64 = NonZeroU64::new(32).unwrap();

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub worker_count: NonZeroUsize,
    pub poll_interval_ms: NonZeroU64,
    pub lease_duration_secs: NonZeroU64,
    pub lease_renew_interval_secs: NonZeroU64,
    pub progress_interval_secs: NonZeroU64,
    pub target_lance_file_size_mib: NonZeroU64,
    pub blob_inline_threshold_mib: NonZeroU64,
    pub blob_dedicated_threshold_mib: NonZeroU64,
}

impl Config {
    /// Loads reconciler settings from environment variables, using defaults
    /// for values that are not set.
    ///
    /// # Errors
    ///
    /// Returns an error when an environment value is not valid for its setting.
    pub fn from_env() -> Result<Self, EnvError> {
        Ok(Self {
            database_url: env::string_or(DATABASE_URL_ENV, DEFAULT_DATABASE_URL)?,
            worker_count: env::parse_or(WORKER_COUNT_ENV, DEFAULT_WORKER_COUNT)?,
            poll_interval_ms: env::parse_or(POLL_INTERVAL_MS_ENV, DEFAULT_POLL_INTERVAL_MS)?,
            lease_duration_secs: env::parse_or(
                LEASE_DURATION_SECS_ENV,
                DEFAULT_LEASE_DURATION_SECS,
            )?,
            lease_renew_interval_secs: env::parse_or(
                LEASE_RENEW_INTERVAL_SECS_ENV,
                DEFAULT_LEASE_RENEW_INTERVAL_SECS,
            )?,
            progress_interval_secs: env::parse_or(
                PROGRESS_INTERVAL_SECS_ENV,
                DEFAULT_PROGRESS_INTERVAL_SECS,
            )?,
            target_lance_file_size_mib: env::parse_or(
                TARGET_LANCE_FILE_SIZE_MIB_ENV,
                DEFAULT_TARGET_LANCE_FILE_SIZE_MIB,
            )?,
            blob_inline_threshold_mib: env::parse_or(
                BLOB_INLINE_THRESHOLD_MIB_ENV,
                DEFAULT_BLOB_INLINE_THRESHOLD_MIB,
            )?,
            blob_dedicated_threshold_mib: env::parse_or(
                BLOB_DEDICATED_THRESHOLD_MIB_ENV,
                DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB,
            )?,
        })
    }

    /// Validates relationships between runtime intervals.
    ///
    /// # Errors
    ///
    /// Returns an error when the lease is less than five renewal intervals,
    /// progress updates are less frequent than lease renewal, or the lease
    /// duration cannot be represented in milliseconds.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.lease_duration_secs.get() / self.lease_renew_interval_secs.get() < 5 {
            return Err(ConfigError::InvalidInput(
                "lease duration must be at least five times the lease renewal interval",
            ));
        }
        if self.progress_interval_secs > self.lease_renew_interval_secs {
            return Err(ConfigError::InvalidInput(
                "progress interval must not be longer than lease renewal interval",
            ));
        }
        self.convert_lease_duration_ms()?;
        Ok(())
    }

    pub fn converter_config(&self) -> ConverterConfig {
        ConverterConfig {
            target_lance_file_size_mib: self.target_lance_file_size_mib.get(),
            blob_inline_threshold_mib: self.blob_inline_threshold_mib.get(),
            blob_dedicated_threshold_mib: self.blob_dedicated_threshold_mib.get(),
        }
    }

    pub fn convert_lease_duration_ms(&self) -> Result<i64, ConfigError> {
        i64::try_from(self.lease_duration_secs.get())
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000))
            .ok_or(ConfigError::InvalidInput(
                "lease duration does not fit milliseconds",
            ))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: DEFAULT_DATABASE_URL.to_owned(),
            worker_count: DEFAULT_WORKER_COUNT,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            lease_renew_interval_secs: DEFAULT_LEASE_RENEW_INTERVAL_SECS,
            progress_interval_secs: DEFAULT_PROGRESS_INTERVAL_SECS,
            target_lance_file_size_mib: DEFAULT_TARGET_LANCE_FILE_SIZE_MIB,
            blob_inline_threshold_mib: DEFAULT_BLOB_INLINE_THRESHOLD_MIB,
            blob_dedicated_threshold_mib: DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("invalid configuration: {0}")]
    InvalidInput(&'static str),
}
