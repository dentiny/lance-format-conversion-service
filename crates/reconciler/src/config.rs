use std::num::{NonZeroU64, NonZeroUsize};

use clap::Parser;
use lance_converter::ConverterConfig;
use thiserror::Error;

/// Default job-store database URL.
pub const DEFAULT_DATABASE_URL: &str = "sqlite://./data/service.db";
/// Default number of conversion workers.
pub const DEFAULT_WORKER_COUNT: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// Default queue polling interval in milliseconds.
pub const DEFAULT_POLL_INTERVAL_MS: NonZeroU64 = NonZeroU64::new(1_000).unwrap();
/// Default lease duration in seconds.
pub const DEFAULT_LEASE_DURATION_SECS: NonZeroU64 = NonZeroU64::new(900).unwrap();
/// Default lease renewal interval in seconds.
pub const DEFAULT_LEASE_RENEW_INTERVAL_SECS: NonZeroU64 = NonZeroU64::new(300).unwrap();
/// Default durable progress checkpoint interval in seconds.
pub const DEFAULT_PROGRESS_INTERVAL_SECS: NonZeroU64 = NonZeroU64::new(30).unwrap();
/// Default soft target size for generated Lance data files in MiB.
pub const DEFAULT_TARGET_LANCE_FILE_SIZE_MIB: NonZeroU64 = NonZeroU64::new(512).unwrap();
/// Default Blob V2 inline payload threshold in MiB.
pub const DEFAULT_BLOB_INLINE_THRESHOLD_MIB: NonZeroU64 = NonZeroU64::new(2).unwrap();
/// Default Blob V2 dedicated-file payload threshold in MiB.
pub const DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB: NonZeroU64 = NonZeroU64::new(32).unwrap();

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = DEFAULT_DATABASE_URL)]
    pub database_url: String,

    #[arg(long, default_value_t = DEFAULT_WORKER_COUNT)]
    pub worker_count: NonZeroUsize,

    #[arg(long, default_value_t = DEFAULT_POLL_INTERVAL_MS)]
    pub poll_interval_ms: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_LEASE_DURATION_SECS)]
    pub lease_duration_secs: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_LEASE_RENEW_INTERVAL_SECS)]
    pub lease_renew_interval_secs: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_PROGRESS_INTERVAL_SECS)]
    pub progress_interval_secs: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_TARGET_LANCE_FILE_SIZE_MIB)]
    pub target_lance_file_size_mib: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_BLOB_INLINE_THRESHOLD_MIB)]
    pub blob_inline_threshold_mib: NonZeroU64,

    #[arg(long, default_value_t = DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB)]
    pub blob_dedicated_threshold_mib: NonZeroU64,
}

impl Config {
    /// Validates relationships between runtime intervals.
    ///
    /// # Errors
    ///
    /// Returns an error when lease renewal is not shorter than the lease,
    /// progress updates are less frequent than lease renewal, or the lease
    /// duration cannot be represented in milliseconds.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.lease_renew_interval_secs >= self.lease_duration_secs {
            return Err(ConfigError::InvalidInput(
                "lease renewal interval must be shorter than lease duration",
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

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("invalid configuration: {0}")]
    InvalidInput(&'static str),
}
