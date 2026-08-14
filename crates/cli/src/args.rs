use std::str::FromStr;

use clap::{Parser, Subcommand};

use lance_conversion_core::job::{IndexSpec, IndexType};

const DEFAULT_API_URL: &str = "http://127.0.0.1:8080";

const ROOT_EXAMPLES: &str = "\
Examples:
  lance-convert submit --help
  lance-convert list --help
  lance-convert status --help
  lance-convert --url http://10.0.0.12:8080 list --ongoing
";

const SUBMIT_EXAMPLES: &str = "\
Examples:
  lance-convert submit --creator test-user --source testdata/sample.parquet --destination /tmp/sample.lance
  lance-convert submit --creator test-user --source testdata/sample.parquet --destination /tmp/sample.lance --blob-column asset_url --index label:scalar
  lance-convert submit --creator test-user --source s3://src/data --destination s3://dst/data.lance --blob-column image_url --index label:scalar --index embedding:vector
  lance-convert --url http://10.0.0.12:8080 submit --creator test-user --source /data/in --destination /data/out.lance
";

const LIST_EXAMPLES: &str = "\
Examples:
  lance-convert list
  lance-convert list --creator test-user --ongoing
  lance-convert list --failed --limit 20
  lance-convert --url http://10.0.0.12:8080 list --creator test-user
";

const STATUS_EXAMPLES: &str = "\
Examples:
  lance-convert status --destination /tmp/sample.lance
  lance-convert status --destination s3://dst/data.lance
  lance-convert --url http://10.0.0.12:8080 status --destination /tmp/sample.lance
";

/// Submit and query Lance format conversion jobs.
#[derive(Debug, Parser)]
#[command(
    name = "lance-convert",
    version = env!("CARGO_PKG_VERSION"),
    about,
    after_help = ROOT_EXAMPLES
)]
pub struct Cli {
    /// Base URL of the conversion control plane.
    #[arg(long, env = "LANCE_API_URL", default_value = DEFAULT_API_URL)]
    pub url: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Submit a format conversion job.
    ///
    /// Creates a queued conversion. The reconciler writes the Lance dataset.
    /// Repeat `--blob-column` and `--index` for multiple columns.
    #[command(after_help = SUBMIT_EXAMPLES)]
    Submit {
        /// Job creator identity recorded with the job.
        #[arg(long)]
        creator: String,
        /// Source dataset URI or filesystem path.
        #[arg(long)]
        source: String,
        /// Destination Lance dataset URI or filesystem path.
        #[arg(long)]
        destination: String,
        /// Source column ingested as a Lance blob. Repeatable.
        #[arg(long = "blob-column")]
        blob_columns: Vec<String>,
        /// Index to build after conversion, as `column:scalar|text|vector`. Repeatable.
        #[arg(long = "index", value_parser = parse_index_spec)]
        indices: Vec<IndexSpec>,
    },
    /// List conversion jobs.
    ///
    /// Returns the newest matching jobs as JSON. `--failed` and `--ongoing`
    /// cannot be combined.
    #[command(after_help = LIST_EXAMPLES)]
    List {
        /// Restrict results to this creator.
        #[arg(long)]
        creator: Option<String>,
        /// Return only failed jobs.
        #[arg(long)]
        failed: bool,
        /// Return only queuing and running jobs.
        #[arg(long)]
        ongoing: bool,
        /// Maximum number of jobs to return.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show one job by destination URI.
    ///
    /// The destination URI is the job key. Prints the full job record,
    /// including status, progress, attempts, and errors.
    #[command(after_help = STATUS_EXAMPLES)]
    Status {
        /// Destination Lance dataset URI or filesystem path.
        #[arg(long)]
        destination: String,
    },
}

/// Parses `column:index_type` CLI index specifications.
///
/// # Errors
///
/// Returns an error when the value is missing a colon, the column is empty, or
/// the index type is not `scalar`, `text`, or `vector`.
pub fn parse_index_spec(value: &str) -> Result<IndexSpec, String> {
    let (column, index_type) = value.split_once(':').ok_or_else(|| {
        format!("expected COLUMN:TYPE, got '{value}'; type is scalar, text, or vector")
    })?;
    if column.is_empty() {
        return Err("index column must not be empty".to_owned());
    }
    Ok(IndexSpec {
        column: column.to_owned(),
        index_type: IndexType::from_str(index_type)?,
    })
}
