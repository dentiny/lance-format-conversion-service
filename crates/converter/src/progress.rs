use std::sync::atomic::{AtomicU64, Ordering};

use lance_conversion_core::job::JobProgress;

#[derive(Default)]
pub struct ConversionProgress {
    rows_read: AtomicU64,
    rows_written: AtomicU64,
    rows_total: AtomicU64,
}

impl ConversionProgress {
    #[must_use]
    pub fn snapshot(&self) -> JobProgress {
        JobProgress {
            rows_read: self.rows_read.load(Ordering::Relaxed),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            rows_total: self.rows_total.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_write(&self, rows_written: u64) {
        self.rows_read.store(rows_written, Ordering::Relaxed);
        self.rows_written.store(rows_written, Ordering::Relaxed);
    }

    pub(crate) fn finish(&self) {
        let rows = self.rows_written.load(Ordering::Relaxed);
        self.rows_total.store(rows, Ordering::Relaxed);
    }
}
