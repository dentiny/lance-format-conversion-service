use std::time::{SystemTime, UNIX_EPOCH};

use crate::StoreError;

/// Source of millisecond unix timestamps used by job-store backends.
pub trait Clock: Send + Sync {
    /// Returns the current unix time in milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error when the system clock is before the unix epoch or the
    /// timestamp does not fit in `i64`.
    fn now_ms(&self) -> Result<i64, StoreError>;
}

/// Operating-system wall clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> Result<i64, StoreError> {
        now_ms()
    }
}

/// Returns the current unix time in milliseconds.
///
/// # Errors
///
/// Returns an error when the system clock is before the unix epoch or the
/// timestamp does not fit in `i64`.
pub fn now_ms() -> Result<i64, StoreError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| StoreError::Worker(error.to_string()))?
        .as_millis();
    i64::try_from(millis).map_err(|error| StoreError::Worker(error.to_string()))
}
