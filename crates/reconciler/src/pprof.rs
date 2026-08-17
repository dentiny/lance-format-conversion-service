use std::time::Duration;

use axum::{
    Router,
    body::Body,
    extract::Query,
    http::{
        HeaderValue, StatusCode,
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    response::Response,
    routing::get,
};
use serde::Deserialize;
use tokio::sync::Mutex;

static CPU_PROFILER: Mutex<()> = Mutex::const_new(());

const DEFAULT_PROFILE_SECONDS: u64 = 30;
const MAX_PROFILE_SECONDS: u64 = 120;
const DEFAULT_SAMPLE_FREQUENCY: i32 = 99;
const MAX_SAMPLE_FREQUENCY: i32 = 1_000;

#[derive(Debug, Deserialize)]
struct CpuProfileQuery {
    seconds: Option<u64>,
    frequency: Option<i32>,
}

pub(crate) fn routes() -> Router {
    Router::new().route("/debug/pprof/cpu/flamegraph", get(cpu_flamegraph))
}

async fn cpu_flamegraph(Query(query): Query<CpuProfileQuery>) -> Response {
    let Ok(_permit) = CPU_PROFILER.try_lock() else {
        return error_response(
            StatusCode::CONFLICT,
            "another CPU profile is already running",
        );
    };
    let seconds = clamp_seconds(query.seconds);
    let frequency = clamp_frequency(query.frequency);

    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(guard) => guard,
        Err(error) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    };

    // Sleep on the runtime so conversion workers keep running in the sample window.
    tokio::time::sleep(Duration::from_secs(seconds)).await;

    match tokio::task::spawn_blocking(move || {
        let report = guard.report().build().map_err(|error| error.to_string())?;
        let mut svg = Vec::new();
        report
            .flamegraph(&mut svg)
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(svg)
    })
    .await
    {
        Ok(Ok(svg)) => svg_response(svg),
        Ok(Err(error)) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

fn clamp_seconds(seconds: Option<u64>) -> u64 {
    seconds
        .unwrap_or(DEFAULT_PROFILE_SECONDS)
        .clamp(1, MAX_PROFILE_SECONDS)
}

fn clamp_frequency(frequency: Option<i32>) -> i32 {
    frequency
        .unwrap_or(DEFAULT_SAMPLE_FREQUENCY)
        .clamp(1, MAX_SAMPLE_FREQUENCY)
}

fn svg_response(svg: Vec<u8>) -> Response {
    let mut response = Response::new(Body::from(svg));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));
    response.headers_mut().insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("inline; filename=\"cpu-flamegraph.svg\""),
    );
    response
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(message.into()))
        .expect("static profiling error response is valid")
}
