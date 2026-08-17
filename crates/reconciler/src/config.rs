use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use clap::Parser;
use lance_converter::ConverterConfig;
use thiserror::Error;

/// Default job-store database URL.
pub const DEFAULT_DATABASE_URL: &str = "postgres://127.0.0.1:5432/lance_jobs";
/// Default `PostgreSQL` connection pool size.
pub const DEFAULT_DATABASE_MAX_CONNECTIONS: NonZeroU32 = NonZeroU32::new(8).unwrap();
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

/// Conversion reconciler for Lance format conversion.
#[derive(Debug, Clone, PartialEq, Eq, Parser)]
#[command(name = "lance-reconciler", version = env!("CARGO_PKG_VERSION"), about)]
pub struct Config {
    /// Job-store database URL.
    #[arg(long, default_value = DEFAULT_DATABASE_URL)]
    pub database_url: String,
    /// `PostgreSQL` connection pool size. Ignored for `SQLite`.
    #[arg(
        long,
        default_value_t = DEFAULT_DATABASE_MAX_CONNECTIONS,
        help = "PostgreSQL connection pool size. Ignored for SQLite."
    )]
    pub database_max_connections: NonZeroU32,
    /// Maximum number of conversion workers.
    #[arg(long, default_value_t = DEFAULT_WORKER_COUNT)]
    pub worker_count: NonZeroUsize,
    /// Queue polling interval in milliseconds.
    #[arg(long, default_value_t = DEFAULT_POLL_INTERVAL_MS)]
    pub poll_interval_ms: NonZeroU64,
    /// Running-job lease duration in seconds.
    #[arg(long, default_value_t = DEFAULT_LEASE_DURATION_SECS)]
    pub lease_duration_secs: NonZeroU64,
    /// Lease renewal interval in seconds.
    #[arg(long, default_value_t = DEFAULT_LEASE_RENEW_INTERVAL_SECS)]
    pub lease_renew_interval_secs: NonZeroU64,
    /// Durable progress checkpoint interval in seconds.
    #[arg(long, default_value_t = DEFAULT_PROGRESS_INTERVAL_SECS)]
    pub progress_interval_secs: NonZeroU64,
    /// Soft target size for generated Lance data files in MiB.
    #[arg(long, default_value_t = DEFAULT_TARGET_LANCE_FILE_SIZE_MIB)]
    pub target_lance_file_size_mib: NonZeroU64,
    /// Blob V2 inline payload threshold in MiB.
    #[arg(long, default_value_t = DEFAULT_BLOB_INLINE_THRESHOLD_MIB)]
    pub blob_inline_threshold_mib: NonZeroU64,
    /// Blob V2 dedicated-file payload threshold in MiB.
    #[arg(long, default_value_t = DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB)]
    pub blob_dedicated_threshold_mib: NonZeroU64,
    /// If set, serve request-triggered CPU pprof. Sampling starts only when a
    /// `/debug/pprof/cpu/flamegraph` request arrives.
    #[arg(long, env = "PPROF_LISTEN_ADDRESS")]
    pub pprof_listen_address: Option<SocketAddr>,
}

impl Config {
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
            database_max_connections: DEFAULT_DATABASE_MAX_CONNECTIONS,
            worker_count: DEFAULT_WORKER_COUNT,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
            lease_renew_interval_secs: DEFAULT_LEASE_RENEW_INTERVAL_SECS,
            progress_interval_secs: DEFAULT_PROGRESS_INTERVAL_SECS,
            target_lance_file_size_mib: DEFAULT_TARGET_LANCE_FILE_SIZE_MIB,
            blob_inline_threshold_mib: DEFAULT_BLOB_INLINE_THRESHOLD_MIB,
            blob_dedicated_threshold_mib: DEFAULT_BLOB_DEDICATED_THRESHOLD_MIB,
            pprof_listen_address: None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("invalid configuration: {0}")]
    InvalidInput(&'static str),
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn parses_cli_defaults() {
        let config = Config::try_parse_from(["lance-reconciler"]).unwrap();
        assert_eq!(config, Config::default());
        config.validate().unwrap();
    }

    #[test]
    fn parses_cli_overrides() {
        let config = Config::try_parse_from([
            "lance-reconciler",
            "--database-url",
            "sqlite://./data/service.db",
            "--worker-count",
            "4",
            "--poll-interval-ms",
            "500",
        ])
        .unwrap();
        assert_eq!(config.database_url, "sqlite://./data/service.db");
        assert_eq!(config.worker_count.get(), 4);
        assert_eq!(config.poll_interval_ms.get(), 500);
        assert_eq!(config.pprof_listen_address, None);
    }
}
