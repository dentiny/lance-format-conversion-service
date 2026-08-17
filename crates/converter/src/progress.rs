use std::sync::atomic::{AtomicU64, Ordering};

use lance_conversion_core::job::JobProgress;

#[derive(Default)]
#[allow(clippy::struct_field_names)]
pub struct ConversionProgress {
    rows_read: AtomicU64,
    rows_written: AtomicU64,
    rows_total: AtomicU64,
    rows_missing_blobs: AtomicU64,
}

impl ConversionProgress {
    #[must_use]
    pub fn snapshot(&self) -> JobProgress {
        JobProgress {
            rows_read: self.rows_read.load(Ordering::SeqCst),
            rows_written: self.rows_written.load(Ordering::SeqCst),
            rows_total: self.rows_total.load(Ordering::SeqCst),
            rows_missing_blobs: self.rows_missing_blobs.load(Ordering::SeqCst),
        }
    }

    pub(crate) fn record_read(&self, rows_read: usize) {
        let rows_read = u64::try_from(rows_read).unwrap_or(u64::MAX);
        let _ = self
            .rows_read
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_add(rows_read))
            });
    }

    pub(crate) fn record_rows_written_total(&self, rows_written: u64) {
        self.rows_written.store(rows_written, Ordering::SeqCst);
    }

    pub(crate) fn record_missing_blobs(&self, rows: usize) {
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        let _ =
            self.rows_missing_blobs
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    Some(current.saturating_add(rows))
                });
    }

    pub(crate) fn finish(&self) {
        let rows = self.rows_read.load(Ordering::SeqCst);
        self.rows_total.store(rows, Ordering::SeqCst);
    }
}
