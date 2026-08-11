use std::sync::atomic::{AtomicU64, Ordering};

use lance_conversion_core::job::JobProgress;

#[derive(Default)]
pub struct ConversionProgress {
    source_bytes_read: AtomicU64,
    lance_bytes_written: AtomicU64,
    rows_read: AtomicU64,
    rows_written: AtomicU64,
    rows_total: AtomicU64,
}

impl ConversionProgress {
    #[must_use]
    pub fn snapshot(&self) -> JobProgress {
        JobProgress {
            source_bytes_read: self.source_bytes_read.load(Ordering::Relaxed),
            lance_bytes_written: self.lance_bytes_written.load(Ordering::Relaxed),
            rows_read: self.rows_read.load(Ordering::Relaxed),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            rows_total: self.rows_total.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_write(&self, bytes_written: u64, rows_written: u64) {
        self.lance_bytes_written
            .store(bytes_written, Ordering::Relaxed);
        self.rows_read.store(rows_written, Ordering::Relaxed);
        self.rows_written.store(rows_written, Ordering::Relaxed);
    }

    pub(crate) fn finish(&self, source_bytes: u64) {
        let rows = self.rows_written.load(Ordering::Relaxed);
        self.rows_total.store(rows, Ordering::Relaxed);
        self.source_bytes_read
            .store(source_bytes, Ordering::Relaxed);
    }
}
