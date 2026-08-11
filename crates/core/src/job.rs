use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::location::DatasetLocation;

pub const MAX_JOB_ATTEMPTS: u32 = 16;

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
    Queuing => "queuing",
    Running => "running",
    Succeeded => "succeeded",
    Failed => "failed",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub creator: String,
    pub kind: JobKind,
    pub source_uri: String,
    pub destination_uri: String,
    pub status: JobStatus,
    pub creation_timestamp_ms: i64,
    pub update_timestamp_ms: i64,
    pub attempt: u32,
    pub error_reasons: Vec<JobError>,
    pub lease_expiration_timestamp_ms: Option<i64>,
    pub progress: JobProgress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobError {
    pub attempt: u32,
    pub error_timestamp_ms: i64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub creator: String,
    pub source: DatasetLocation,
    pub kind: JobKind,
    pub destination: DatasetLocation,
    pub creation_timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub rows_read: u64,
    pub rows_written: u64,
    pub rows_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedJob {
    pub job: Job,
}

#[derive(Debug, Clone)]
pub struct LeaseUpdate {
    pub destination_uri: String,
    pub attempt: u32,
    pub convert_lease_duration_ms: i64,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    pub destination_uri: String,
    pub attempt: u32,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct CompletionUpdate {
    pub destination_uri: String,
    pub attempt: u32,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct FailureUpdate {
    pub destination_uri: String,
    pub attempt: u32,
    pub progress: JobProgress,
    pub reason: String,
}
