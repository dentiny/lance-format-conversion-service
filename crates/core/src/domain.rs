use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::location::DatasetLocation;

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(match self {
                    $(Self::$variant => $value),+
                })
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(format!("invalid {} value: {value}", stringify!($name))),
                }
            }
        }
    };
}

string_enum!(JobKind {
    Copy => "copy",
    Move => "move",
});

string_enum!(JobStatus {
    Queued => "queued",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub kind: JobKind,
    pub source_uri: String,
    pub destination_uri: String,
    pub status: JobStatus,
    pub submitted_at_ms: i64,
    pub update_timestamp: i64,
    pub attempt: u32,
    pub lease_owner: Option<String>,
    pub lease_expiration_timestamp: Option<i64>,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub source: DatasetLocation,
    pub kind: JobKind,
    pub destination: DatasetLocation,
    pub submitted_at_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub source_bytes_read: u64,
    pub lance_bytes_written: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub work_units_completed: u64,
    pub work_units_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob {
    pub job: Job,
}

#[derive(Debug, Clone)]
pub struct LeaseUpdate {
    pub job_id: Uuid,
    pub attempt: u32,
    pub lease_duration_ms: i64,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    pub job_id: Uuid,
    pub attempt: u32,
    pub progress: JobProgress,
}
