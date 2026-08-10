use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use thiserror::Error;
use tracing::Level;

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

#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen_address: SocketAddr,

    #[arg(long, default_value = "./data/service.db")]
    pub database_path: PathBuf,

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

    #[arg(long, value_enum, default_value = "info")]
    pub log_level: LogLevel,
}

impl Config {
    /// Validates relationships between runtime intervals.
    ///
    /// # Errors
    ///
    /// Returns an error when lease renewal is not shorter than the lease,
    /// progress updates are less frequent than lease renewal, or the blob
    /// inline threshold exceeds the target Lance file size.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.lease_renew_interval_secs >= self.lease_duration_secs {
            return Err(ConfigError::LeaseRenewalNotShorter);
        }
        if self.progress_interval_secs > self.lease_renew_interval_secs {
            return Err(ConfigError::ProgressSlowerThanLeaseRenewal);
        }
        if self.blob_inline_threshold_mib > self.target_lance_file_size_mib {
            return Err(ConfigError::BlobThresholdExceedsFileSize);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<LogLevel> for Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Error => Self::ERROR,
            LogLevel::Warn => Self::WARN,
            LogLevel::Info => Self::INFO,
            LogLevel::Debug => Self::DEBUG,
            LogLevel::Trace => Self::TRACE,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("lease renewal interval must be shorter than lease duration")]
    LeaseRenewalNotShorter,
    #[error("progress interval must not be longer than lease renewal interval")]
    ProgressSlowerThanLeaseRenewal,
    #[error("blob inline threshold must not exceed the target Lance file size")]
    BlobThresholdExceedsFileSize,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use clap::Parser;

    use super::{Config, ConfigError};

    #[test]
    fn defaults_match_the_service_contract() {
        let config = Config::parse_from(["service"]);
        assert_eq!(config.listen_address.to_string(), "127.0.0.1:8080");
        assert_eq!(config.worker_count.get(), 256);
        assert_eq!(config.lease_duration_secs.get(), 900);
        assert_eq!(config.lease_renew_interval_secs.get(), 300);
        assert_eq!(config.progress_interval_secs.get(), 30);
        assert_eq!(config.target_lance_file_size_mib.get(), 512);
        assert_eq!(config.blob_inline_threshold_mib.get(), 2);
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_lease_intervals() {
        let mut config = Config::parse_from(["service"]);
        config.lease_renew_interval_secs = NonZeroU64::new(900).unwrap();
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::LeaseRenewalNotShorter
        );
    }

    #[test]
    fn rejects_blob_threshold_larger_than_output_file() {
        let mut config = Config::parse_from(["service"]);
        config.blob_inline_threshold_mib = NonZeroU64::new(513).unwrap();
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::BlobThresholdExceedsFileSize
        );
    }
}
