use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::location::{DatasetLocation, LocationKind};

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

string_enum!(InspectionStatus {
    Pending => "pending",
    Ready => "ready",
    Failed => "failed",
});

string_enum!(JobKind {
    Copy => "copy",
    Move => "move",
});

string_enum!(JobStatus {
    Queued => "queued",
    Running => "running",
    Validating => "validating",
    Publishing => "publishing",
    DeletingSource => "deleting_source",
    Succeeded => "succeeded",
    Failed => "failed",
    Cancelled => "cancelled",
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Inspection {
    pub id: Uuid,
    pub source_uri: String,
    pub source_kind: LocationKind,
    pub status: InspectionStatus,
    pub schema_fingerprint: Option<String>,
    pub error: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct NewInspection {
    pub source: DatasetLocation,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct CompletedInspection {
    pub id: Uuid,
    pub schema_fingerprint: String,
    pub completed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub inspection_id: Uuid,
    pub kind: JobKind,
    pub source_uri: String,
    pub destination_uri: String,
    pub schema_fingerprint: String,
    pub status: JobStatus,
    pub submitted_at_ms: i64,
    pub updated_at_ms: i64,
    pub attempt: u32,
    pub lease_owner: Option<String>,
    pub lease_token: Option<Uuid>,
    pub lease_expires_at_ms: Option<i64>,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub inspection_id: Uuid,
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
    pub lease_token: Uuid,
}

#[derive(Debug, Clone)]
pub struct LeaseUpdate {
    pub job_id: Uuid,
    pub lease_token: Uuid,
    pub lease_duration_ms: i64,
    pub progress: JobProgress,
}

#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    pub job_id: Uuid,
    pub lease_token: Uuid,
    pub progress: JobProgress,
}
