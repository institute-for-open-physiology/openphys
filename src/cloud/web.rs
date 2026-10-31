use super::auth::{
    AuthService, EnrollmentExchangeRequest, INVITATION_LIFETIME_MS, MacEnrollmentExchangeRequest,
    PasswordLoginError, PasswordLoginRequest, PasswordRegisterRequest, PasswordResetFinishRequest,
    PasswordResetRequest, PasswordResetRequestError, PublicAuthError, RefreshRequest,
    RefreshServiceError, TokenResponse, random_token,
};
use super::db::{
    CURRENT_SCHEMA_VERSION, CloudDb, DevicePrincipal, PassiveCheckpointResult, SessionUser, User,
    hash_secret, normalize_email,
};
use super::ingest::{
    CheckpointActivity, HttpPhaseTiming, IngestBatchRequest, IngestStore, NORMAL_INGEST_LOG_SAMPLE,
    SLOW_INGEST_US,
};
use crate::live_server::{
    DASHBOARD_HTML, LIVE_WORKER_JS, LiveDeltaBatch, LiveEcg, LiveRuntime, LiveServerConfig,
    LiveStatus, MarkerPostRequest, start_runtime_tasks,
};
use anyhow::{Context, Result, anyhow};
use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, response};
use chrono::Utc;
use futures::stream;
use qrcode::{QrCode, render::svg};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Digest;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs::OpenOptions;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path as StdPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use uuid::Uuid;

const AUTH_BODY_LIMIT_BYTES: usize = 8 * 1024;
const PASSWORD_WORK_CONCURRENCY: usize = 2;
const SHARED_AUTH_CLIENT_KEY: &str = "shared-client";
const INGEST_EXTRACTOR_BODY_LIMIT_BYTES: usize = super::ingest::MAX_INGEST_JSON_BYTES + 1;
const ADMIN_OVERVIEW_USER_BATCH_LIMIT: usize = 512;
const ADMIN_OVERVIEW_HEARTBEAT_TICKS: u64 = 10;
const DB_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);
const DB_CHECKPOINT_STUCK_MS: i64 = 30_000;
const DB_CHECKPOINT_STALE_MS: i64 = 60_000;
const DB_CHECKPOINT_MAX_OUTSTANDING_FRAMES: u64 = 1_000;
// The approved v4 private-beta APK is about 8.3 MiB. A deliberately conservative
// bound keeps startup's immutable in-memory pin finite while leaving room for
// normal beta growth.
const MAX_ANDROID_APK_BYTES: u64 = 32 * 1024 * 1024;

/// Axum owns an SSE response body after the handler returns, so connection
/// accounting must follow the body's lifetime rather than the handler future.
/// The final Arc is dropped on client disconnect, authorization expiry, or
/// response cancellation and decrements every tenant runtime it tracked.
#[derive(Default)]
struct CloudSseClients {
    runtimes: Mutex<HashMap<Uuid, Arc<Mutex<LiveRuntime>>>>,
}

impl CloudSseClients {
    fn track(&self, user_id: Uuid, runtime: Arc<Mutex<LiveRuntime>>) -> Result<()> {
        let mut runtimes = self
            .runtimes
            .lock()
            .map_err(|_| anyhow!("cloud SSE client registry lock poisoned"))?;
        if runtimes.contains_key(&user_id) {
            return Ok(());
        }
        runtime
            .lock()
            .map_err(|_| anyhow!("dashboard lock poisoned"))?
            .sse_client_connected();
        runtimes.insert(user_id, runtime);
        Ok(())
    }

    fn note_write_error(&self, error: String) {
        let Ok(runtimes) = self.runtimes.lock() else {
            return;
        };
        for runtime in runtimes.values() {
            if let Ok(mut runtime) = runtime.lock() {
                runtime.note_sse_write_error(error.clone());
            }
        }
    }
}

impl Drop for CloudSseClients {
    fn drop(&mut self) {
        let Ok(runtimes) = self.runtimes.get_mut() else {
            return;
        };
        for runtime in runtimes.values() {
            if let Ok(mut runtime) = runtime.lock() {
                runtime.sse_client_disconnected();
            }
        }
    }
}

#[derive(Clone)]
pub struct CloudConfig {
    pub data_dir: PathBuf,
    pub db_path: PathBuf,
    pub port: u16,
    pub public_origin: String,
    pub admin_email: String,
    pub android_package: String,
    pub android_cert_sha256: String,
    pub android_apk_path: PathBuf,
    pub android_apk_sha256: String,
    pub android_apk_version_name: String,
    pub android_apk_version_code: u64,
    pub resend_api_key: String,
    pub resend_from: String,
    pub resend_reply_to: String,
    pub token_derivation_secret: String,
}

impl std::fmt::Debug for CloudConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CloudConfig")
            .field("data_dir", &self.data_dir)
            .field("db_path", &self.db_path)
            .field("port", &self.port)
            .field("public_origin", &self.public_origin)
            .field("admin_email", &"[REDACTED]")
            .field("android_package", &self.android_package)
            .field("android_cert_sha256", &"[REDACTED]")
            .field("android_apk_path", &self.android_apk_path)
            .field("android_apk_sha256", &self.android_apk_sha256)
            .field("android_apk_version_name", &self.android_apk_version_name)
            .field("android_apk_version_code", &self.android_apk_version_code)
            .field("resend_api_key", &"[REDACTED]")
            .field("resend_from", &"[REDACTED]")
            .field("resend_reply_to", &"[REDACTED]")
            .field("token_derivation_secret", &"[REDACTED]")
            .finish()
    }
}

impl CloudConfig {
    pub fn from_env(data_dir: PathBuf, db_path: PathBuf, port: u16) -> Result<Self> {
        Ok(Self {
            data_dir,
            db_path,
            port,
            public_origin: required_env("SENSORS_PUBLIC_ORIGIN")?,
            admin_email: required_env("SENSORS_ADMIN_EMAIL")?,
            android_package: std::env::var("SENSORS_ANDROID_PACKAGE")
                .unwrap_or_else(|_| "com.vineyardfinance.sensors".to_string()),
            android_cert_sha256: required_env("SENSORS_ANDROID_CERT_SHA256")?,
            android_apk_path: PathBuf::from(required_env("SENSORS_ANDROID_APK_PATH")?),
            android_apk_sha256: required_env("SENSORS_ANDROID_APK_SHA256")?,
            android_apk_version_name: required_env("SENSORS_ANDROID_APK_VERSION_NAME")?,
            android_apk_version_code: required_env("SENSORS_ANDROID_APK_VERSION_CODE")?
                .parse()
                .context("SENSORS_ANDROID_APK_VERSION_CODE must be a positive integer")?,
            resend_api_key: required_env("RESEND_API_KEY")?,
            resend_from: required_env("SENSORS_RESEND_FROM")?,
            resend_reply_to: required_env("SENSORS_RESEND_REPLY_TO")?,
            token_derivation_secret: required_env("SENSORS_TOKEN_DERIVATION_SECRET")?,
        })
    }
}

// Bounded lookback for cloud dashboard runtimes; see
// docs/cloud_memory_bounds_20260809.md.
const CLOUD_HISTORY_WINDOW: Duration = Duration::from_secs(2 * 60 * 60);
// Bounded ingest admission: beyond this many concurrently admitted batch
// requests the server answers 429 and the phones' durable queues absorb the
// difference, instead of server RAM.
const MAX_ADMITTED_INGEST: usize = 16;
// How long a database integrity verdict may be served from cache.
const DB_CHECK_CACHE_MS: u64 = 60_000;

struct CloudState {
    data_dir: PathBuf,
    db: Arc<CloudDb>,
    auth: AuthService,
    ingest: IngestStore,
    seca: super::seca::Store,
    dashboards: Mutex<HashMap<Uuid, Arc<Mutex<LiveRuntime>>>>,
    assetlinks: Value,
    android_apk: AndroidApk,
    public_origin: String,
    resend: ResendConfig,
    email_delivery_limit: Arc<tokio::sync::Semaphore>,
    password_work_limit: Arc<tokio::sync::Semaphore>,
    archive_health: Arc<Mutex<ArchiveHealth>>,
    checkpoint_health: Arc<Mutex<CheckpointHealth>>,
    // Per-enrollment APK download tickets for holders of a valid, unconsumed
    // enrollment link (phone-browser bootstrap): enrollment-token hash ->
    // (ticket hash, expiry). Keying by enrollment means repeated previews of
    // one link overwrite a single slot instead of filling the bounded map.
    // In-memory only: a restart just requires one more QR scan.
    apk_tickets: Mutex<HashMap<String, (String, i64)>>,
    ingest_admission: Arc<tokio::sync::Semaphore>,
    ingest_rejections: std::sync::atomic::AtomicU64,
    // Cached result of the expensive SQLite quick_check + schema read.
    // Recomputed at most once per DB_CHECK_CACHE_MS in the background of a
    // health request: a cold-page-cache quick_check on the grown database
    // takes seconds, and running it inline made /healthz time out during
    // deployments while the system was perfectly healthy.
    db_health_cache: Mutex<DbHealthCache>,
}

#[derive(Clone, Copy)]
struct DbHealthCache {
    checked_at: Option<Instant>,
    db_ok: bool,
    schema_version: i64,
    refreshing: bool,
}

#[derive(Clone)]
struct AndroidApk {
    bytes: Bytes,
    sha256: String,
    version_name: String,
    version_code: u64,
    download_filename: String,
}

impl std::fmt::Debug for AndroidApk {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AndroidApk")
            .field("size_bytes", &self.bytes.len())
            .field("sha256", &self.sha256)
            .field("version_name", &self.version_name)
            .field("version_code", &self.version_code)
            .field("download_filename", &self.download_filename)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
struct ArchiveHealth {
    state: &'static str,
    pending: bool,
    last_success_unix_ms: Option<i64>,
    last_failure_unix_ms: Option<i64>,
    consecutive_failures: u64,
    last_archived_segments: usize,
    last_output_files: usize,
    error: Option<&'static str>,
}

impl Default for ArchiveHealth {
    fn default() -> Self {
        Self {
            state: "ok",
            pending: true,
            last_success_unix_ms: None,
            last_failure_unix_ms: None,
            consecutive_failures: 0,
            last_archived_segments: 0,
            last_output_files: 0,
            error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct CheckpointHealth {
    state: &'static str,
    pending: bool,
    in_flight_started_unix_ms: Option<i64>,
    last_attempt_unix_ms: Option<i64>,
    last_completed_attempt_unix_ms: Option<i64>,
    last_success_unix_ms: Option<i64>,
    last_full_completion_unix_ms: Option<i64>,
    last_failure_unix_ms: Option<i64>,
    outstanding_since_unix_ms: Option<i64>,
    consecutive_failures: u64,
    consecutive_partials: u64,
    busy: bool,
    log_frames: i64,
    checkpointed_frames: i64,
    outstanding_frames: u64,
    wal_bytes: u64,
    last_blocking_queue_us: Option<u64>,
    max_blocking_queue_us: u64,
    last_checkpoint_operation_us: Option<u64>,
    max_checkpoint_operation_us: u64,
    last_wal_stat_us: Option<u64>,
    max_wal_stat_us: u64,
    last_attempt_total_us: Option<u64>,
    max_attempt_total_us: u64,
    error: Option<&'static str>,
}

impl Default for CheckpointHealth {
    fn default() -> Self {
        Self {
            state: "ok",
            pending: true,
            in_flight_started_unix_ms: None,
            last_attempt_unix_ms: None,
            last_completed_attempt_unix_ms: None,
            last_success_unix_ms: None,
            last_full_completion_unix_ms: None,
            last_failure_unix_ms: None,
            outstanding_since_unix_ms: None,
            consecutive_failures: 0,
            consecutive_partials: 0,
            busy: false,
            log_frames: 0,
            checkpointed_frames: 0,
            outstanding_frames: 0,
            wal_bytes: 0,
            last_blocking_queue_us: None,
            max_blocking_queue_us: 0,
            last_checkpoint_operation_us: None,
            max_checkpoint_operation_us: 0,
            last_wal_stat_us: None,
            max_wal_stat_us: 0,
            last_attempt_total_us: None,
            max_attempt_total_us: 0,
            error: None,
        }
    }
}

pub async fn run(config: CloudConfig) -> Result<()> {
    let bind = format!("127.0.0.1:{}", config.port);
    let archive_health = Arc::new(Mutex::new(ArchiveHealth::default()));
    let checkpoint_health = Arc::new(Mutex::new(CheckpointHealth::default()));
    let state = build_cloud_state(&config, archive_health.clone(), checkpoint_health.clone())?;
    let checkpoint_db = state.db.clone();
    let checkpoint_activity = state.ingest.checkpoint_activity();
    let app = router_from_state(state);
    let archive_task = start_archive_worker(config.data_dir.clone(), archive_health);
    let checkpoint_task =
        start_checkpoint_worker(checkpoint_db, checkpoint_health, checkpoint_activity);
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind cloud service on {bind}"))?;
    println!(
        "cloud service listening on {} for {}",
        listener.local_addr()?,
        config.public_origin
    );
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("cloud shutdown signal failed: {error}");
            }
        })
        .await
        .context("serve cloud HTTP");
    archive_task.abort();
    checkpoint_task.abort();
    result
}

fn start_checkpoint_worker(
    db: Arc<CloudDb>,
    health: Arc<Mutex<CheckpointHealth>>,
    activity: Arc<CheckpointActivity>,
) -> tokio::task::JoinHandle<()> {
    start_checkpoint_worker_with_interval(db, health, activity, DB_CHECKPOINT_INTERVAL)
}

fn start_checkpoint_worker_with_interval(
    db: Arc<CloudDb>,
    health: Arc<Mutex<CheckpointHealth>>,
    activity: Arc<CheckpointActivity>,
    cadence: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let start = tokio::time::Instant::now() + cadence;
        let mut interval = tokio::time::interval_at(start, cadence);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let started_ms = unix_ms();
            match begin_checkpoint_attempt(&health, started_ms) {
                Ok(false) => continue,
                Ok(true) => {}
                Err(error) => {
                    eprintln!("cloud checkpoint health start failed: {error:#}");
                    continue;
                }
            }
            let checkpoint_started = Instant::now();
            let checkpoint_db = db.clone();
            let checkpoint_activity = activity.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                let blocking_queue_us =
                    u64::try_from(checkpoint_started.elapsed().as_micros()).unwrap_or(u64::MAX);
                let _activity = checkpoint_activity.guard();
                (blocking_queue_us, checkpoint_db.passive_checkpoint())
            })
            .await;
            let attempt_total_us =
                u64::try_from(checkpoint_started.elapsed().as_micros()).unwrap_or(u64::MAX);
            match outcome {
                Ok((blocking_queue_us, Ok(result))) => note_checkpoint_result(
                    &health,
                    unix_ms(),
                    blocking_queue_us,
                    attempt_total_us,
                    result,
                ),
                Ok((blocking_queue_us, Err(error))) => {
                    eprintln!("cloud checkpoint worker failed: {error:#}");
                    note_checkpoint_failure(
                        &health,
                        unix_ms(),
                        Some(blocking_queue_us),
                        attempt_total_us,
                    );
                }
                Err(error) => {
                    eprintln!("cloud checkpoint worker join failed: {error}");
                    note_checkpoint_failure(&health, unix_ms(), None, attempt_total_us);
                }
            }
        }
    })
}

fn begin_checkpoint_attempt(health: &Arc<Mutex<CheckpointHealth>>, now_ms: i64) -> Result<bool> {
    let mut health = health
        .lock()
        .map_err(|_| anyhow!("checkpoint health mutex was poisoned"))?;
    if health.in_flight_started_unix_ms.is_some() {
        return Ok(false);
    }
    health.in_flight_started_unix_ms = Some(now_ms);
    health.last_attempt_unix_ms = Some(now_ms);
    health.pending = true;
    Ok(true)
}

fn note_checkpoint_result(
    health: &Arc<Mutex<CheckpointHealth>>,
    now_ms: i64,
    blocking_queue_us: u64,
    attempt_total_us: u64,
    result: PassiveCheckpointResult,
) {
    let Ok(mut health) = health.lock() else {
        return;
    };
    let outstanding = result
        .log_frames
        .max(0)
        .saturating_sub(result.checkpointed_frames.max(0)) as u64;
    let full = !result.busy && outstanding == 0;
    health.in_flight_started_unix_ms = None;
    health.last_completed_attempt_unix_ms = Some(now_ms);
    health.last_success_unix_ms = Some(now_ms);
    health.consecutive_failures = 0;
    health.busy = result.busy;
    health.log_frames = result.log_frames;
    health.checkpointed_frames = result.checkpointed_frames;
    health.outstanding_frames = outstanding;
    health.wal_bytes = result.wal_bytes;
    let checkpoint_operation_us =
        u64::try_from(result.checkpoint_operation_ns / 1_000).unwrap_or(u64::MAX);
    let wal_stat_us = u64::try_from(result.wal_stat_ns / 1_000).unwrap_or(u64::MAX);
    health.last_blocking_queue_us = Some(blocking_queue_us);
    health.max_blocking_queue_us = health.max_blocking_queue_us.max(blocking_queue_us);
    health.last_checkpoint_operation_us = Some(checkpoint_operation_us);
    health.max_checkpoint_operation_us = health
        .max_checkpoint_operation_us
        .max(checkpoint_operation_us);
    health.last_wal_stat_us = Some(wal_stat_us);
    health.max_wal_stat_us = health.max_wal_stat_us.max(wal_stat_us);
    health.last_attempt_total_us = Some(attempt_total_us);
    health.max_attempt_total_us = health.max_attempt_total_us.max(attempt_total_us);
    health.error = None;
    if full {
        health.pending = false;
        health.last_full_completion_unix_ms = Some(now_ms);
        health.outstanding_since_unix_ms = None;
        health.consecutive_partials = 0;
    } else {
        health.pending = true;
        health.outstanding_since_unix_ms.get_or_insert(now_ms);
        health.consecutive_partials = health.consecutive_partials.saturating_add(1);
    }
    evaluate_checkpoint_health(&mut health, now_ms);
}

fn note_checkpoint_failure(
    health: &Arc<Mutex<CheckpointHealth>>,
    now_ms: i64,
    blocking_queue_us: Option<u64>,
    attempt_total_us: u64,
) {
    let Ok(mut health) = health.lock() else {
        return;
    };
    health.in_flight_started_unix_ms = None;
    health.last_completed_attempt_unix_ms = Some(now_ms);
    health.last_failure_unix_ms = Some(now_ms);
    health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    health.pending = true;
    if let Some(blocking_queue_us) = blocking_queue_us {
        health.last_blocking_queue_us = Some(blocking_queue_us);
        health.max_blocking_queue_us = health.max_blocking_queue_us.max(blocking_queue_us);
    }
    health.last_attempt_total_us = Some(attempt_total_us);
    health.max_attempt_total_us = health.max_attempt_total_us.max(attempt_total_us);
    health.state = "error";
    health.error = Some("checkpoint failure; inspect service logs");
}

fn evaluate_checkpoint_health(health: &mut CheckpointHealth, now_ms: i64) {
    let stuck = health
        .in_flight_started_unix_ms
        .is_some_and(|started| now_ms.saturating_sub(started) > DB_CHECKPOINT_STUCK_MS);
    let stale = health.outstanding_frames > 0
        && health
            .outstanding_since_unix_ms
            .is_some_and(|started| now_ms.saturating_sub(started) > DB_CHECKPOINT_STALE_MS);
    let excessive = health.outstanding_frames > DB_CHECKPOINT_MAX_OUTSTANDING_FRAMES;
    if stuck {
        health.state = "error";
        health.error = Some("checkpoint attempt stuck; inspect service logs");
    } else if excessive {
        health.state = "error";
        health.error = Some("checkpoint backlog exceeded limit; inspect service logs");
    } else if stale {
        health.state = "error";
        health.error = Some("checkpoint backlog stale; inspect service logs");
    } else if health.consecutive_failures == 0 {
        health.state = "ok";
        health.error = None;
    }
}

fn checkpoint_health_snapshot(state: &CloudState) -> CheckpointHealth {
    state
        .checkpoint_health
        .lock()
        .map(|health| {
            let mut snapshot = health.clone();
            evaluate_checkpoint_health(&mut snapshot, unix_ms());
            snapshot
        })
        .unwrap_or_else(|_| CheckpointHealth {
            state: "error",
            pending: true,
            error: Some("checkpoint health lock failure"),
            ..CheckpointHealth::default()
        })
}

fn start_archive_worker(
    data_dir: PathBuf,
    health: Arc<Mutex<ArchiveHealth>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let archive_dir = data_dir.clone();
            match tokio::task::spawn_blocking(move || {
                crate::archive::run_wal_archiver(&archive_dir)
            })
            .await
            {
                Ok(Ok(summary)) => {
                    if summary.archived_segments > 0 {
                        eprintln!(
                            "cloud archive worker: archived={} outputs={} skipped={}",
                            summary.archived_segments,
                            summary.output_files,
                            summary.skipped_segments
                        );
                    }
                    if let Ok(mut state) = health.lock() {
                        state.state = "ok";
                        state.pending = false;
                        state.last_success_unix_ms = Some(unix_ms());
                        state.consecutive_failures = 0;
                        state.last_archived_segments = summary.archived_segments;
                        state.last_output_files = summary.output_files;
                        state.error = None;
                    }
                }
                Ok(Err(error)) => {
                    eprintln!("cloud archive worker failed: {error:#}");
                    note_archive_failure(&health);
                }
                Err(error) => {
                    eprintln!("cloud archive worker join failed: {error}");
                    note_archive_failure(&health);
                }
            }
        }
    })
}

fn note_archive_failure(health: &Arc<Mutex<ArchiveHealth>>) {
    if let Ok(mut state) = health.lock() {
        state.state = "error";
        state.pending = false;
        state.last_failure_unix_ms = Some(unix_ms());
        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        state.error = Some("archive failure; inspect service logs");
    }
}

fn archive_health_snapshot(state: &CloudState) -> ArchiveHealth {
    state
        .archive_health
        .lock()
        .map(|health| health.clone())
        .unwrap_or_else(|_| ArchiveHealth {
            state: "error",
            pending: false,
            error: Some("archive health lock failure"),
            ..ArchiveHealth::default()
        })
}

fn dashboard_payload_with_archive<T: Serialize>(
    payload: T,
    archive: ArchiveHealth,
    attach_to_nested_status: bool,
) -> Result<Value> {
    let mut value = serde_json::to_value(payload).context("serialize dashboard payload")?;
    let archive_value = serde_json::to_value(archive).context("serialize archive health")?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow!("dashboard payload is not a JSON object"))?;
    object.insert("archive".to_string(), archive_value.clone());
    if attach_to_nested_status {
        let status = object
            .get_mut("status")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("dashboard snapshot status is not a JSON object"))?;
        status.insert("archive".to_string(), archive_value);
    }
    Ok(value)
}

#[cfg(test)]
fn build_router(config: &CloudConfig) -> Result<Router> {
    build_router_with_archive_health(config, Arc::new(Mutex::new(ArchiveHealth::default())))
}

#[cfg(test)]
fn build_router_with_archive_health(
    config: &CloudConfig,
    archive_health: Arc<Mutex<ArchiveHealth>>,
) -> Result<Router> {
    Ok(router_from_state(build_cloud_state(
        config,
        archive_health,
        Arc::new(Mutex::new(CheckpointHealth::default())),
    )?))
}

#[cfg(test)]
fn build_router_and_state(config: &CloudConfig) -> Result<(Router, Arc<CloudState>)> {
    let state = build_cloud_state(
        config,
        Arc::new(Mutex::new(ArchiveHealth::default())),
        Arc::new(Mutex::new(CheckpointHealth::default())),
    )?;
    Ok((router_from_state(state.clone()), state))
}

fn build_cloud_state(
    config: &CloudConfig,
    archive_health: Arc<Mutex<ArchiveHealth>>,
    checkpoint_health: Arc<Mutex<CheckpointHealth>>,
) -> Result<Arc<CloudState>> {
    let android_apk = load_android_apk(config)?;
    let db = Arc::new(CloudDb::open(&config.db_path)?);
    db.ensure_admin(&config.admin_email, unix_ms())?;
    let auth = AuthService::new(db.clone(), &config.token_derivation_secret)?;
    let ingest = IngestStore::new(config.data_dir.clone(), db.clone())?;
    let state = Arc::new(CloudState {
        data_dir: config.data_dir.clone(),
        db,
        auth,
        ingest,
        seca: super::seca::Store::new(&config.data_dir)?,
        dashboards: Mutex::new(HashMap::new()),
        assetlinks: json!([{
            "relation": ["delegate_permission/common.handle_all_urls"],
            "target": {
                "namespace": "android_app",
                "package_name": config.android_package,
                "sha256_cert_fingerprints": [config.android_cert_sha256]
            }
        }]),
        android_apk,
        public_origin: config.public_origin.trim_end_matches('/').to_string(),
        resend: ResendConfig {
            api_key: config.resend_api_key.clone(),
            from: config.resend_from.clone(),
            reply_to: config.resend_reply_to.clone(),
        },
        email_delivery_limit: Arc::new(tokio::sync::Semaphore::new(4)),
        password_work_limit: Arc::new(tokio::sync::Semaphore::new(PASSWORD_WORK_CONCURRENCY)),
        archive_health,
        checkpoint_health,
        apk_tickets: Mutex::new(HashMap::new()),
        ingest_admission: Arc::new(tokio::sync::Semaphore::new(MAX_ADMITTED_INGEST)),
        ingest_rejections: std::sync::atomic::AtomicU64::new(0),
        db_health_cache: Mutex::new(DbHealthCache {
            checked_at: None,
            db_ok: false,
            schema_version: 0,
            refreshing: false,
        }),
    });
    // Deliberately NO eager per-user runtime warm-up here: loading every
    // tenant's WAL history at startup needed ~3.7 GiB during the 9 Aug 2026
    // OOM crashloop (docs/cloud_oom_incident_20260809.md) and fired before
    // the service could serve. Runtimes build lazily on first dashboard or
    // admin access, so startup memory is flat regardless of user count or
    // backlog size (docs/cloud_memory_bounds_20260809.md).
    Ok(state)
}

fn load_android_apk(config: &CloudConfig) -> Result<AndroidApk> {
    validate_apk_metadata(
        &config.android_apk_path,
        &config.android_apk_sha256,
        &config.android_apk_version_name,
        config.android_apk_version_code,
    )?;
    let visible = std::fs::symlink_metadata(&config.android_apk_path).with_context(|| {
        format!(
            "inspect configured Android APK {}",
            config.android_apk_path.display()
        )
    })?;
    if visible.file_type().is_symlink() || !visible.is_file() {
        anyhow::bail!("configured Android APK must be a non-symlink regular file");
    }
    if visible.len() == 0 || visible.len() > MAX_ANDROID_APK_BYTES {
        anyhow::bail!(
            "configured Android APK size must be between 1 byte and {MAX_ANDROID_APK_BYTES} bytes"
        );
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let mut file = options.open(&config.android_apk_path).with_context(|| {
        format!(
            "open configured Android APK {} without following the final path component",
            config.android_apk_path.display()
        )
    })?;
    let opened = file.metadata().context("inspect opened Android APK")?;
    if !opened.is_file() || opened.len() != visible.len() {
        anyhow::bail!("configured Android APK changed while it was opened");
    }
    #[cfg(unix)]
    if opened.dev() != visible.dev() || opened.ino() != visible.ino() {
        anyhow::bail!("configured Android APK path identity changed while it was opened");
    }

    let capacity =
        usize::try_from(opened.len()).context("Android APK is too large for this host")?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)
        .context("read configured Android APK")?;
    if bytes.len() != capacity {
        anyhow::bail!("configured Android APK length changed while it was read");
    }
    let actual_sha256 = hex::encode(sha2::Sha256::digest(&bytes));
    if actual_sha256 != config.android_apk_sha256 {
        anyhow::bail!("configured Android APK SHA-256 does not match the approved artifact");
    }
    let after = file.metadata().context("reinspect opened Android APK")?;
    if after.len() != opened.len() {
        anyhow::bail!("configured Android APK changed while it was verified");
    }
    #[cfg(unix)]
    if after.dev() != opened.dev() || after.ino() != opened.ino() {
        anyhow::bail!("configured Android APK identity changed while it was verified");
    }

    Ok(AndroidApk {
        bytes: Bytes::from(bytes),
        sha256: config.android_apk_sha256.clone(),
        version_name: config.android_apk_version_name.clone(),
        version_code: config.android_apk_version_code,
        download_filename: format!("training-sensors-{}.apk", config.android_apk_version_name),
    })
}

fn validate_apk_metadata(
    path: &StdPath,
    sha256: &str,
    version_name: &str,
    version_code: u64,
) -> Result<()> {
    if !path.is_absolute() {
        anyhow::bail!("SENSORS_ANDROID_APK_PATH must be absolute");
    }
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        anyhow::bail!("SENSORS_ANDROID_APK_SHA256 must be 64 lowercase hexadecimal characters");
    }
    if version_name.is_empty()
        || version_name.len() > 64
        || !version_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        anyhow::bail!("SENSORS_ANDROID_APK_VERSION_NAME contains unsafe characters");
    }
    if version_code == 0 {
        anyhow::bail!("SENSORS_ANDROID_APK_VERSION_CODE must be positive");
    }
    Ok(())
}

fn router_from_state(state: Arc<CloudState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/assetlinks.json", get(assetlinks))
        .route("/", get(home))
        .route("/login", get(login_page))
        .route("/invite", get(login_page))
        .route("/forgot-password", get(login_page))
        .route("/reset-password", get(login_page))
        .route("/enroll", get(enroll_page))
        .route("/downloads/training-sensors.apk", get(download_android_apk))
        .route(
            "/api/v1/auth/password/register",
            post(password_register).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/auth/invitation",
            post(invitation_details).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/auth/password/login",
            post(password_login).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/auth/password-reset/request",
            post(password_reset_request).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/auth/password-reset/finish",
            post(password_reset_finish).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route("/api/v1/auth/browser-refresh", post(browser_refresh))
        .route("/api/v1/auth/logout", post(logout))
        .route("/api/v1/me", get(me))
        .route(
            "/api/v1/device-enrollments",
            post(create_device_enrollment).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/device-enrollments/exchange",
            post(exchange_device_enrollment).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/device-enrollments/exchange-macos",
            post(exchange_mac_device_enrollment)
                .layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/device-enrollments/preview",
            post(preview_device_enrollment).layer(DefaultBodyLimit::max(AUTH_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/ingest/batches",
            post(ingest_batch).layer(DefaultBodyLimit::max(INGEST_EXTRACTOR_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v1/seca/batches",
            post(seca_ingest).layer(DefaultBodyLimit::max(super::seca::MAX_BYTES + 1)),
        )
        .route("/admin/seca", get(seca_page))
        .route("/api/v1/admin/seca/sessions", get(seca_sessions))
        .route(
            "/api/v1/users/{user_id}/seca/{session_id}/snapshot",
            get(seca_snapshot),
        )
        .route(
            "/api/v1/users/{user_id}/seca/{session_id}/chunks/{index}",
            get(seca_chunk),
        )
        .route("/api/v1/admin/users", get(admin_users))
        .route("/api/v1/admin/installations", get(admin_installations))
        .route(
            "/api/v1/admin/overview/snapshot",
            get(admin_overview_snapshot),
        )
        .route("/api/v1/admin/overview/events", get(admin_overview_events))
        .route("/admin", get(admin_overview))
        .route(
            "/api/v1/admin/invitations",
            get(admin_invitations).post(admin_invitation),
        )
        .route(
            "/api/v1/admin/invitations/{invitation_id}/revoke",
            post(admin_revoke_invitation),
        )
        .route(
            "/api/v1/admin/invitations/{invitation_id}/resend",
            post(admin_resend_invitation),
        )
        .route(
            "/api/v1/admin/users/{user_id}/disabled",
            post(admin_disable_user),
        )
        .route(
            "/api/v1/admin/device-credentials/{credential_id}/revoke",
            post(admin_revoke_device_credential),
        )
        .route("/api/v1/users/{user_id}/devices", get(user_devices))
        .route(
            "/api/v1/users/{user_id}/sessions/{family_id}/revoke",
            post(revoke_user_browser_session),
        )
        .route(
            "/api/v1/users/{user_id}/installations/{installation_id}/revoke",
            post(revoke_user_installation),
        )
        .route("/dashboard/{user_id}", get(dashboard_html))
        .route("/dashboard/{user_id}/live-worker.js", get(dashboard_worker))
        .route(
            "/api/v1/users/{user_id}/dashboard/status",
            get(dashboard_status),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/latest",
            get(dashboard_latest),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/events",
            get(dashboard_events),
        )
        .route(
            "/api/v1/users/{user_id}/dashboard/marker",
            post(dashboard_marker),
        )
        .layer(DefaultBodyLimit::max(512 * 1024))
        .with_state(state)
}

/// Serves the cached integrity verdict, running the expensive
/// quick_check+schema read at most once per DB_CHECK_CACHE_MS. The very
/// first health request after startup performs the check inline (there is
/// nothing to serve yet); later requests refresh in the background and keep
/// answering from cache, so /healthz stays fast even when the check itself
/// takes seconds on a cold page cache. A genuine corruption still flips the
/// verdict within one cache period, and failures are logged loudly.
async fn cached_db_health(state: &Arc<CloudState>) -> (bool, i64) {
    let now = Instant::now();
    let (serve_cached, must_check_inline) = {
        let Ok(mut cache) = state.db_health_cache.lock() else {
            return (false, 0);
        };
        match cache.checked_at {
            None if cache.refreshing => (None, false),
            None => {
                cache.refreshing = true;
                (None, true)
            }
            Some(checked_at) => {
                let stale = now.duration_since(checked_at).as_millis() as u64 >= DB_CHECK_CACHE_MS;
                let start_refresh = stale && !cache.refreshing;
                if start_refresh {
                    cache.refreshing = true;
                }
                (Some((cache.db_ok, cache.schema_version)), start_refresh)
            }
        }
    };
    match (serve_cached, must_check_inline) {
        (Some(cached), true) => {
            let refresh_state = state.clone();
            tokio::task::spawn_blocking(move || {
                // Drop guard: a panic inside the check must not leave
                // `refreshing` stuck true forever (which would freeze the
                // cached verdict permanently).
                struct RefreshingReset(Arc<CloudState>);
                impl Drop for RefreshingReset {
                    fn drop(&mut self) {
                        if let Ok(mut cache) = self.0.db_health_cache.lock() {
                            cache.refreshing = false;
                        }
                    }
                }
                let _reset = RefreshingReset(refresh_state.clone());
                let result = refresh_state
                    .db
                    .quick_check()
                    .and_then(|_| refresh_state.db.schema_version());
                if let Err(error) = &result {
                    eprintln!("cloud health database refresh failure: {error:#}");
                }
                store_db_health(&refresh_state, result.map_err(|_| ()));
            });
            cached
        }
        (Some(cached), false) => cached,
        (None, true) => {
            let check_state = state.clone();
            let result = tokio::task::spawn_blocking(move || {
                let result = check_state
                    .db
                    .quick_check()
                    .and_then(|_| check_state.db.schema_version());
                store_db_health(&check_state, result.as_ref().copied().map_err(|_| ()));
                result
            })
            .await;
            match result {
                Ok(Ok(schema_version)) => (true, schema_version),
                Ok(Err(error)) => {
                    eprintln!("cloud health database failure: {error:#}");
                    (false, 0)
                }
                Err(error) => {
                    eprintln!("cloud health database task failed: {error}");
                    if let Ok(mut cache) = state.db_health_cache.lock() {
                        cache.refreshing = false;
                    }
                    (false, 0)
                }
            }
        }
        (None, false) => (false, 0),
    }
}

fn store_db_health(state: &Arc<CloudState>, result: std::result::Result<i64, ()>) {
    if let Ok(mut cache) = state.db_health_cache.lock() {
        cache.checked_at = Some(Instant::now());
        cache.refreshing = false;
        match result {
            Ok(schema_version) => {
                cache.db_ok = true;
                cache.schema_version = schema_version;
            }
            Err(()) => {
                cache.db_ok = false;
                cache.schema_version = 0;
            }
        }
    }
}

async fn healthz(State(state): State<Arc<CloudState>>) -> Response {
    let (db_ok, schema_version) = cached_db_health(&state).await;
    let archive = archive_health_snapshot(&state);
    let checkpoint = checkpoint_health_snapshot(&state);
    let ingest_timing = state.ingest.timing_snapshot();
    let schema_ok = schema_version == CURRENT_SCHEMA_VERSION;
    if db_ok && !schema_ok {
        eprintln!(
            "cloud health schema mismatch: expected {CURRENT_SCHEMA_VERSION}, found {schema_version}"
        );
    }
    let ok = db_ok && schema_ok && archive.state == "ok" && checkpoint.state == "ok";
    (
        if ok {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "ok": ok,
            "service": "training_sensors_cloud",
            "schema_version": 1,
            "database_schema_version": schema_version,
            "database_schema_ok": schema_ok,
            "database_ok": db_ok,
            "archive": archive,
            "checkpoint": checkpoint,
            "ingest_timing": ingest_timing,
        })),
    )
        .into_response()
}

async fn assetlinks(State(state): State<Arc<CloudState>>) -> Response {
    let mut response = Json(state.assetlinks.clone()).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=300"),
    );
    response
}

async fn home(State(state): State<Arc<CloudState>>, headers: HeaderMap) -> Response {
    match session(&state, &headers).await {
        Ok(session) => {
            let target = format!("/dashboard/{}", session.user.id);
            let admin = if session.user.is_admin {
                "<p><a href=\"/admin\">Admin overview</a></p>"
            } else {
                ""
            };
            let apk = &state.android_apk;
            no_store_html(&format!(
                "<!doctype html><meta charset=utf-8><meta name=viewport content=\"width=device-width,initial-scale=1\"><title>Training Sensors</title><style>body{{font:16px system-ui;max-width:42rem;margin:3rem auto;padding:1rem}}button,a{{font:inherit}}button,.download{{padding:.65rem;margin:.25rem}}section{{margin:2rem 0}}code{{overflow-wrap:anywhere}}#enrollment svg{{max-width:320px;height:auto}}.muted{{color:#667}}</style><h1>Training Sensors</h1><p>Signed in as {}</p><p><a href=\"{}\">Open dashboard</a></p>{}<section id=installAndroid><h2>1. Install the Android app</h2><p><a id=downloadAndroidApp class=download href=\"/downloads/training-sensors.apk\" download=\"{}\">Download signed private-beta APK</a></p><p class=muted>Version {} (code {}). SHA-256: <code>{}</code></p><p>On your Android phone, open the download and tap <strong>Install</strong> (or <strong>Update</strong>). If Android blocks it, open the offered Settings screen, allow this browser to install unknown apps, return and install. You can turn that permission off again afterwards. After installation, return to this page and continue to step 2.</p><p class=muted>Setting up a phone from this computer? Skip the download here: choose <strong>Connect a phone with QR</strong> below and scan the code with the phone camera. The phone browser will guide the download, installation and connection &mdash; no sign-in needed on the phone.</p></section><section id=connectAndroid><h2>2. Connect the Android collector</h2><p><button id=connectThisPhone>Connect this phone</button><button id=showEnrollmentQr>Connect a phone with QR</button></p><div id=enrollment class=muted></div></section><section id=connectMac><h2>Connect a macOS USB collector</h2><button id=connectMacCollector>Create Mac connection link</button><div id=macEnrollment class=muted></div></section><section id=yourDevices><h2>Your devices</h2><div id=devices class=muted data-user=\"{}\">loading…</div></section><p><button id=logout>Sign out</button></p>{}{}{}",
                escape_html(&session.user.email),
                target,
                admin,
                escape_html(&apk.download_filename),
                escape_html(&apk.version_name),
                apk.version_code,
                apk.sha256,
                session.user.id,
                HOME_ENROLLMENT_JS,
                BROWSER_SESSION_JS,
                HOME_DEVICES_JS,
            ))
        }
        Err(error) if error.status == StatusCode::UNAUTHORIZED => {
            no_store_html(SESSION_BOOTSTRAP_HTML)
        }
        Err(_) => response::Redirect::temporary("/login").into_response(),
    }
}

async fn login_page() -> Response {
    no_store_html(LOGIN_HTML)
}

/// Signed-in devices must never see a bare 401 on a page load
/// (docs/session_persistence_decision_20260808.md): the year-long refresh
/// cookie can outlive the access cookie, so an unauthorized HTML navigation
/// gets a non-sensitive recovery page even if its request has no cookies:
/// SameSite=Strict can withhold all cookies on an external app/link navigation.
/// Once loaded on our origin, the page checks the browser session and only
/// refreshes it if expired. Browsers without a session go to /login.
/// FORBIDDEN and other statuses pass through untouched so a wrong-user link
/// cannot refresh-reload forever.
const SESSION_BOOTSTRAP_HTML: &str = r#"<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Training Sensors</title><style>body{font:16px system-ui;max-width:38rem;margin:4rem auto;padding:1rem;color:#667}</style><p>Signing you back in&hellip;</p><script>
(async()=>{
function cookieValue(name){const p=document.cookie.split(';').map(v=>v.trim()).find(v=>v.startsWith(name+'='));return p?decodeURIComponent(p.slice(name.length+1)):''}
async function request(url,options={}){const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),10000);try{return await fetch(url,{...options,credentials:'same-origin',cache:'no-store',signal:controller.signal})}finally{clearTimeout(timer)}}
try{
const csrf=cookieValue('sensors_csrf');
if(!csrf){location.replace('/login');return}
const current=await request('/api/v1/me');
if(!current.ok){
if(current.status!==401)throw new Error('session '+current.status);
const refreshed=await request('/api/v1/auth/browser-refresh',{method:'POST',headers:{'x-csrf-token':csrf}});
if(!refreshed.ok)throw new Error('refresh '+refreshed.status);
}
location.reload();
}catch(e){location.replace('/login')}
})();
</script>"#;

fn html_error_or_session_bootstrap(error: ApiError) -> Response {
    if error.status == StatusCode::UNAUTHORIZED {
        return no_store_html(SESSION_BOOTSTRAP_HTML);
    }
    error.into_response()
}

async fn enroll_page() -> Response {
    no_store_html(ENROLL_FALLBACK_HTML)
}

async fn download_android_apk(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    if let Err(error) = session(&state, &headers).await {
        // Phone-browser bootstrap: a live enrollment-preview ticket
        // authorizes the download without a signed-in phone session.
        if !apk_ticket_authorizes(&state, &headers) {
            let mut response = error.into_response();
            mark_apk_download_response(&mut response);
            return Ok(response);
        }
    }
    let apk = &state.android_apk;
    let mut response = Response::new(Body::from(apk.bytes.clone()));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.android.package-archive"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&apk.bytes.len().to_string())
            .map_err(|_| internal(anyhow!("Android APK length is invalid")))?,
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            apk.download_filename
        ))
        .map_err(|_| internal(anyhow!("Android APK download filename is invalid")))?,
    );
    headers.insert(
        "x-checksum-sha256",
        HeaderValue::from_str(&apk.sha256)
            .map_err(|_| internal(anyhow!("Android APK checksum header is invalid")))?,
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert("accept-ranges", HeaderValue::from_static("none"));
    mark_apk_download_response(&mut response);
    Ok(response)
}

fn mark_apk_download_response(response: &mut Response) {
    mark_no_store(response);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-transform"),
    );
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvitationDetailsRequest {
    invitation_token: String,
}

async fn invitation_details(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<InvitationDetailsRequest>,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let invitation_state = state.clone();
    let invitation = run_blocking_public("invitation lookup task failed", move || {
        invitation_state
            .auth
            .invitation_details(&request.invitation_token, &client_key, unix_ms())
    })
    .await?;
    Ok(no_store_json(json!({"email": invitation.email})))
}

async fn password_register(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<PasswordRegisterRequest>,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let user_agent = sanitized_user_agent(&headers);
    let password_permit = password_work_permit(&state)?;
    let register_state = state.clone();
    let tokens = run_blocking_public("password registration task failed", move || {
        let _password_permit = password_permit;
        register_state.auth.register_password(
            request,
            &client_key,
            user_agent.as_deref(),
            unix_ms(),
        )
    })
    .await?;
    token_response(tokens)
}

async fn password_login(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<PasswordLoginRequest>,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let user_agent = sanitized_user_agent(&headers);
    let password_permit = password_work_permit(&state)?;
    let login_state = state.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _password_permit = password_permit;
        login_state
            .auth
            .login_password(request, &client_key, user_agent.as_deref(), unix_ms())
    })
    .await
    .map_err(|error| internal(anyhow!(error).context("password login task failed")))?;
    match result {
        Ok(tokens) => token_response(tokens),
        Err(PasswordLoginError::InvalidCredentials) => Err(ApiError::with_code(
            StatusCode::UNAUTHORIZED,
            "invalid_credentials",
            "email or password is incorrect",
        )),
        Err(PasswordLoginError::RateLimited) => Err(ApiError::with_code(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "too many attempts; try again later",
        )),
        Err(PasswordLoginError::Internal(error)) => Err(internal(error)),
    }
}

async fn password_reset_request(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<PasswordResetRequest>,
) -> ApiResult<Response> {
    // Keep the public success path on a common floor. Provider delivery is
    // detached below, so neither provider latency nor the presence of an
    // account is exposed in this response.
    let response_not_before = Instant::now() + Duration::from_millis(75);
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let reset_state = state.clone();
    let result = tokio::task::spawn_blocking(move || {
        reset_state
            .auth
            .request_password_reset(request, &client_key, unix_ms())
    })
    .await
    .map_err(|error| internal(anyhow!(error).context("password reset-request task failed")))?;
    let delivery = match result {
        Ok(delivery) => delivery,
        Err(PasswordResetRequestError::RateLimited) => {
            return Err(ApiError::with_code(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many attempts; try again later",
            ));
        }
        Err(PasswordResetRequestError::Internal(error)) => return Err(internal(error)),
    };
    if let Some(delivery) = delivery {
        let reset_url = format!(
            "{}/reset-password#{}",
            state.public_origin, delivery.reset_token
        );
        let delivery_state = state.clone();
        tokio::spawn(async move {
            let request_id = Uuid::new_v4();
            let Ok(_permit) = delivery_state
                .email_delivery_limit
                .clone()
                .acquire_owned()
                .await
            else {
                eprintln!("password reset delivery queue closed request_id={request_id}");
                return;
            };
            if let Err(error) = send_password_reset_email(
                &delivery_state.resend,
                &delivery.email,
                &reset_url,
                delivery.expires_at_unix_ms,
            )
            .await
            {
                eprintln!("password reset delivery failed request_id={request_id}: {error:#}");
            }
        });
    }
    tokio::time::sleep_until(tokio::time::Instant::from_std(response_not_before)).await;
    Ok(no_store_json(json!({
        "ok": true,
        "message": "If that account is active, a reset email has been sent."
    })))
}

async fn password_reset_finish(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<PasswordResetFinishRequest>,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let user_agent = sanitized_user_agent(&headers);
    let password_permit = password_work_permit(&state)?;
    let reset_state = state.clone();
    let tokens = run_blocking_public("password reset-finish task failed", move || {
        let _password_permit = password_permit;
        reset_state.auth.finish_password_reset(
            request,
            &client_key,
            user_agent.as_deref(),
            unix_ms(),
        )
    })
    .await?;
    token_response(tokens)
}

async fn create_device_enrollment(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let current = mutation_session(&state, &headers).await?;
    let enrollment_state = state.clone();
    let user = current.user;
    let session_id = current.session_id;
    let enrollment = run_blocking_internal("device enrollment-create task failed", move || {
        enrollment_state
            .auth
            .create_enrollment(&user, session_id, unix_ms())
    })
    .await?;
    let enrollment_url = format!(
        "{}/enroll#{}",
        state.public_origin, enrollment.enrollment_token
    );
    let qr = QrCode::new(enrollment_url.as_bytes())
        .map_err(|error| internal(anyhow!("encode enrollment QR: {error}")))?;
    let qr_svg = qr
        .render::<svg::Color<'_>>()
        .min_dimensions(256, 256)
        .quiet_zone(true)
        .build();
    Ok(no_store_json(json!({
        "enrollment_url": enrollment_url,
        "expires_at_unix_ms": enrollment.expires_at_unix_ms,
        "qr_svg": qr_svg,
    })))
}

async fn exchange_device_enrollment(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<EnrollmentExchangeRequest>,
) -> ApiResult<Response> {
    let client_key = auth_client_key(&headers);
    let exchange_state = state.clone();
    let credential = run_blocking_public("device enrollment-exchange task failed", move || {
        exchange_state
            .auth
            .exchange_enrollment(request, &client_key, unix_ms())
    })
    .await?;
    Ok(no_store_json(serde_json::to_value(credential).map_err(
        |error| internal(anyhow!(error).context("serialize device credential response")),
    )?))
}

async fn exchange_mac_device_enrollment(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<MacEnrollmentExchangeRequest>,
) -> ApiResult<Response> {
    let client_key = auth_client_key(&headers);
    let credential = run_blocking_public("macOS enrollment exchange task failed", move || {
        state
            .auth
            .exchange_mac_enrollment(request, &client_key, unix_ms())
    })
    .await?;
    Ok(no_store_json(
        serde_json::to_value(credential).map_err(|error| internal(error.into()))?,
    ))
}

const APK_TICKET_CAP: usize = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrollmentPreviewRequest {
    enrollment_token: String,
}

/// Phone-browser bootstrap: validates an enrollment link without consuming
/// its one-use exchange and, on success, mints a ticket cookie that
/// authorizes exactly the APK download for the link's remaining lifetime.
async fn preview_device_enrollment(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<EnrollmentPreviewRequest>,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let client_key = auth_client_key(&headers);
    let preview_state = state.clone();
    let token = request.enrollment_token;
    let enrollment_hash = hash_secret(&token);
    let expires_at_unix_ms =
        run_blocking_public("device enrollment-preview task failed", move || {
            preview_state
                .auth
                .preview_enrollment(&token, &client_key, unix_ms())
        })
        .await?;
    let ticket = random_token();
    {
        let mut tickets = state
            .apk_tickets
            .lock()
            .map_err(|_| internal(anyhow!("APK ticket mutex was poisoned")))?;
        let now_ms = unix_ms();
        tickets.retain(|_, entry| entry.1 > now_ms);
        if tickets.len() >= APK_TICKET_CAP && !tickets.contains_key(&enrollment_hash) {
            // Evict the soonest-expiring enrollment slot rather than
            // refusing new onboarding attempts.
            if let Some(oldest) = tickets
                .iter()
                .min_by_key(|(_, entry)| entry.1)
                .map(|(key, _)| key.clone())
            {
                tickets.remove(&oldest);
            }
        }
        tickets.insert(enrollment_hash, (hash_secret(&ticket), expires_at_unix_ms));
    }
    let apk = &state.android_apk;
    let mut response = no_store_json(json!({
        "expires_at_unix_ms": expires_at_unix_ms,
        "apk": {
            "version_name": apk.version_name,
            "version_code": apk.version_code,
            "sha256": apk.sha256,
        },
    }));
    let max_age_seconds = ((expires_at_unix_ms - unix_ms()).max(0) / 1_000).max(1);
    let ticket_cookie = format!(
        "sensors_apk_ticket={ticket}; Path=/downloads; Secure; HttpOnly; SameSite=Lax; Max-Age={max_age_seconds}"
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&ticket_cookie)
            .map_err(|_| internal(anyhow!("invalid APK ticket cookie")))?,
    );
    Ok(response)
}

/// True when the request carries a live enrollment-preview APK ticket. The
/// ticket authorizes only the APK artifact and is never consumed, so
/// HEAD/Range/retry requests keep working for its bounded lifetime.
fn apk_ticket_authorizes(state: &Arc<CloudState>, headers: &HeaderMap) -> bool {
    let Some(ticket) = cookie_value(headers, "sensors_apk_ticket") else {
        return false;
    };
    if ticket.is_empty() || ticket.len() > 128 {
        return false;
    }
    let Ok(tickets) = state.apk_tickets.lock() else {
        return false;
    };
    let ticket_hash = hash_secret(ticket);
    let now_ms = unix_ms();
    tickets
        .values()
        .any(|(hash, expiry)| *hash == ticket_hash && *expiry > now_ms)
}

async fn browser_refresh(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let refresh_token = cookie_value(&headers, "sensors_refresh")
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::UNAUTHORIZED,
                "browser refresh session is missing",
            )
        })?
        .to_string();
    let rotation_id = cookie_value(&headers, "sensors_rotation")
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| {
            ApiError::new(StatusCode::UNAUTHORIZED, "browser refresh state is missing")
        })?;
    let csrf = csrf_header(&headers)?.to_string();
    let csrf_state = state.clone();
    let refresh_token_for_csrf = refresh_token.clone();
    if !run_blocking_internal("browser refresh CSRF task failed", move || {
        csrf_state
            .auth
            .refresh_csrf_matches(&refresh_token_for_csrf, &csrf, rotation_id, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "browser refresh CSRF token is invalid",
        ));
    }
    let tokens = refresh_tokens(
        &state,
        RefreshRequest {
            refresh_token: refresh_token.to_string(),
            rotation_id,
        },
        sanitized_user_agent(&headers),
    )
    .await?;
    token_response(tokens)
}

async fn refresh_tokens(
    state: &Arc<CloudState>,
    request: RefreshRequest,
    user_agent: Option<String>,
) -> ApiResult<TokenResponse> {
    let refresh_state = state.clone();
    let result = tokio::task::spawn_blocking(move || {
        refresh_state
            .auth
            .refresh(request, user_agent.as_deref(), unix_ms())
    })
    .await
    .map_err(|error| internal(anyhow!(error).context("refresh task failed")))?;
    match result {
        Ok(tokens) => Ok(tokens),
        Err(RefreshServiceError::Terminal) => Err(ApiError::with_code(
            StatusCode::UNAUTHORIZED,
            "refresh_terminal",
            "refresh token is invalid, expired, or revoked",
        )),
        Err(RefreshServiceError::Internal(error)) => Err(internal(error)),
    }
}

async fn logout(State(state): State<Arc<CloudState>>, headers: HeaderMap) -> ApiResult<Response> {
    require_exact_origin(&state, &headers)?;
    let csrf = csrf_header(&headers)?.to_string();
    if let Ok(current) = session(&state, &headers).await {
        if crate::cloud::db::hash_secret(&csrf) != current.csrf_hash {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "CSRF token is invalid",
            ));
        }
        let revoke_state = state.clone();
        run_blocking_internal("browser session-family revoke task failed", move || {
            revoke_state
                .auth
                .revoke_session_family(current.session_id, unix_ms())
        })
        .await?;
    } else {
        let refresh = cookie_value(&headers, "sensors_refresh")
            .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "session is missing"))?
            .to_string();
        let rotation_id = cookie_value(&headers, "sensors_rotation")
            .and_then(|value| Uuid::parse_str(value).ok())
            .unwrap_or_else(Uuid::nil);
        let csrf_state = state.clone();
        let refresh_for_csrf = refresh.clone();
        if !run_blocking_internal("logout refresh CSRF task failed", move || {
            csrf_state
                .auth
                .refresh_csrf_matches(&refresh_for_csrf, &csrf, rotation_id, unix_ms())
        })
        .await?
        {
            return Err(ApiError::new(
                StatusCode::FORBIDDEN,
                "CSRF token is invalid",
            ));
        }
        let revoke_state = state.clone();
        run_blocking_internal("refresh-family revoke task failed", move || {
            revoke_state.auth.revoke_refresh_family(&refresh, unix_ms())
        })
        .await?;
    }
    let mut response = Json(json!({"ok": true})).into_response();
    clear_browser_cookies(&mut response);
    Ok(response)
}

async fn me(State(state): State<Arc<CloudState>>, headers: HeaderMap) -> ApiResult<Json<Value>> {
    let current = session(&state, &headers).await?;
    Ok(Json(json!({"user": current.user})))
}

struct IngestHandlerTimingGuard {
    state: Arc<CloudState>,
    activity: Arc<CheckpointActivity>,
    started_ns: u128,
    activity_before: super::ingest::ActivitySnapshot,
    attempt_seq: u64,
    http_auth: Option<HttpPhaseTiming>,
    http_decode_checksum: Option<HttpPhaseTiming>,
    blocking_queue: Option<HttpPhaseTiming>,
    outcome: &'static str,
}

impl IngestHandlerTimingGuard {
    fn new(state: Arc<CloudState>) -> Self {
        let activity = state.ingest.checkpoint_activity();
        let activity_before = activity.snapshot();
        Self {
            started_ns: state.ingest.timing_now_ns(),
            state,
            activity,
            activity_before,
            attempt_seq: 0,
            http_auth: None,
            http_decode_checksum: None,
            blocking_queue: None,
            outcome: "failure",
        }
    }
}

fn format_ingest_handler_timing(
    attempt_seq: u64,
    outcome: &str,
    http_auth: Option<HttpPhaseTiming>,
    http_decode_checksum: Option<HttpPhaseTiming>,
    blocking_queue: Option<HttpPhaseTiming>,
    handler_total: HttpPhaseTiming,
) -> Option<String> {
    let handler_total_us = u64::try_from(handler_total.elapsed_ns / 1_000).unwrap_or(u64::MAX);
    let slow = handler_total_us > SLOW_INGEST_US;
    let sampled = !slow && attempt_seq.is_multiple_of(NORMAL_INGEST_LOG_SAMPLE);
    if !slow && !sampled {
        return None;
    }
    let phase_fields = |phase: Option<HttpPhaseTiming>| {
        (
            phase.is_some(),
            phase
                .map(|value| u64::try_from(value.elapsed_ns / 1_000).unwrap_or(u64::MAX))
                .unwrap_or(0),
            phase.map(|value| value.checkpoint_overlap).unwrap_or(false),
        )
    };
    let (http_auth_present, http_auth_us, http_auth_overlap) = phase_fields(http_auth);
    let (http_decode_present, http_decode_us, http_decode_overlap) =
        phase_fields(http_decode_checksum);
    let (blocking_queue_present, blocking_queue_us, blocking_queue_overlap) =
        phase_fields(blocking_queue);
    Some(format!(
        "cloud_ingest_handler_timing attempt_seq={attempt_seq} outcome={outcome} http_auth_present={http_auth_present} http_auth_us={http_auth_us} http_auth_checkpoint_overlap={http_auth_overlap} http_decode_checksum_present={http_decode_present} http_decode_checksum_us={http_decode_us} http_decode_checksum_checkpoint_overlap={http_decode_overlap} blocking_queue_present={blocking_queue_present} blocking_queue_us={blocking_queue_us} blocking_queue_checkpoint_overlap={blocking_queue_overlap} handler_total_us={handler_total_us} handler_total_checkpoint_overlap={}",
        handler_total.checkpoint_overlap,
    ))
}

impl Drop for IngestHandlerTimingGuard {
    fn drop(&mut self) {
        let handler_elapsed_ns = self.state.ingest.timing_elapsed_ns(self.started_ns);
        let handler_total = self.state.ingest.record_handler_total(
            handler_elapsed_ns,
            self.activity_before,
            self.activity.snapshot(),
        );
        if let Some(log_line) = format_ingest_handler_timing(
            self.attempt_seq,
            self.outcome,
            self.http_auth,
            self.http_decode_checksum,
            self.blocking_queue,
            handler_total,
        ) {
            eprintln!("{log_line}");
        }
    }
}

async fn seca_ingest(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let permit = state
        .ingest_admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "server is busy; retry shortly",
            )
        })?;
    let credential = device_credential(&headers)
        .ok_or_else(|| device_reconnect_error(StatusCode::UNAUTHORIZED))?;
    let current = authenticate_device_blocking(&state, credential.to_string())
        .await?
        .ok_or_else(|| device_reconnect_error(StatusCode::UNAUTHORIZED))?;
    if body.len() > super::seca::MAX_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "seca chunk exceeds byte limit",
        ));
    }
    let batch: super::seca::Batch =
        serde_json::from_slice(&body).map_err(|e| bad_request(e.into()))?;
    if batch.installation_id != current.installation_id {
        return Err(device_reconnect_error(StatusCode::FORBIDDEN));
    }
    let checksum = hex::encode(sha2::Sha256::digest(&body));
    let mut ack = tokio::task::spawn_blocking(move || {
        // Keep the permit through disk work even if the HTTP client disconnects.
        let _permit = permit;
        state
            .seca
            .ingest(current.user.id, current.installation_id, batch, unix_ms())
    })
    .await
    .map_err(|e| internal(e.into()))?
    .map_err(|error| {
        if error.chain().any(|cause| cause.is::<std::io::Error>()) {
            internal(error)
        } else if error.to_string().contains("conflicting seca chunk")
            || error.to_string().contains("sequence gap")
        {
            ApiError::new(StatusCode::CONFLICT, error.to_string())
        } else {
            bad_request(error)
        }
    })?;
    // ACK the exact transport body. Canonical archive bytes can differ in JSON
    // whitespace/key order; the phone verifies this digest before deleting its file.
    ack.sha256 = checksum;
    Ok(no_store_json(
        serde_json::to_value(ack).map_err(|e| internal(e.into()))?,
    ))
}

async fn seca_page(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    require_admin(&state, &headers).await?;
    Ok(no_store_html(include_str!("seca.html")))
}

async fn seca_sessions(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    require_admin(&state, &headers).await?;
    let result =
        run_blocking_internal("seca sessions task failed", move || state.seca.sessions()).await?;
    Ok(no_store_json(json!({"sessions": result})))
}

#[derive(Deserialize)]
struct SecaSnapshotQuery {
    end: Option<u64>,
}

async fn seca_snapshot(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path((user, session)): Path<(Uuid, Uuid)>,
    Query(query): Query<SecaSnapshotQuery>,
) -> ApiResult<Response> {
    authorize_user(&state, &headers, user).await?;
    let permit = state
        .ingest_admission
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "server is busy; retry shortly",
            )
        })?;
    let result = run_blocking_seca_read(move || {
        let _permit = permit;
        state.seca.snapshot(user, session, query.end)
    })
    .await?;
    Ok(no_store_json(result))
}

async fn run_blocking_seca_read<T, F>(operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| internal(e.into()))?
        .map_err(|e| {
            if e.is::<super::seca::MissingRecording>() {
                ApiError::new(StatusCode::NOT_FOUND, "seca recording unavailable")
            } else {
                internal(e)
            }
        })
}

async fn seca_chunk(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path((user, session, index)): Path<(Uuid, Uuid, u64)>,
) -> ApiResult<Response> {
    authorize_user(&state, &headers, user).await?;
    let bytes = run_blocking_seca_read(move || state.seca.chunk(user, session, index)).await?;
    let mut response = bytes.into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"seca-{session}-{index}.json\""
        ))
        .map_err(|e| internal(e.into()))?,
    );
    Ok(response)
}

async fn ingest_batch(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    // Bounded admission before any work: refusing load is honest and safe
    // (the batch stays in the phone's durable queue and retries
    // idempotently), while unbounded server-side buffering is what melted
    // the VM on 9 Aug 2026.
    let Ok(_ingest_permit) = state.ingest_admission.clone().try_acquire_owned() else {
        let rejected = state
            .ingest_rejections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if rejected == 1 || rejected % 100 == 0 {
            eprintln!(
                "cloud ingest backpressure: {rejected} batch requests refused with 429 so far"
            );
        }
        let mut response = ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "server is busy; retry shortly",
        )
        .into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
        return Ok(response);
    };
    let mut handler_timing = IngestHandlerTimingGuard::new(state.clone());
    let attempt_seq = state.ingest.reserve_ingest_attempt_sequence();
    handler_timing.attempt_seq = attempt_seq;
    let checkpoint_activity = state.ingest.checkpoint_activity();
    let auth_started_ns = state.ingest.timing_now_ns();
    let auth_activity_before = checkpoint_activity.snapshot();
    let current = match device_credential(&headers) {
        Some(credential) => authenticate_device_blocking(&state, credential.to_string())
            .await?
            .ok_or_else(|| device_reconnect_error(StatusCode::UNAUTHORIZED)),
        None => Err(device_reconnect_error(StatusCode::UNAUTHORIZED)),
    };
    handler_timing.http_auth = Some(state.ingest.record_http_auth(
        state.ingest.timing_elapsed_ns(auth_started_ns),
        auth_activity_before,
        checkpoint_activity.snapshot(),
    ));
    let current = current?;
    let decode_started_ns = state.ingest.timing_now_ns();
    let decode_activity_before = checkpoint_activity.snapshot();
    let decoded = (|| -> ApiResult<(IngestBatchRequest, String, usize)> {
        if body.len() > super::ingest::MAX_INGEST_JSON_BYTES {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "ingest request exceeds encoded byte limit",
            ));
        }
        let checksum = hex::encode(sha2::Sha256::digest(&body));
        let request: IngestBatchRequest = serde_json::from_slice(&body)
            .context("decode ingest request JSON")
            .map_err(bad_request)?;
        Ok((request, checksum, body.len()))
    })();
    handler_timing.http_decode_checksum = Some(state.ingest.record_http_decode_checksum(
        state.ingest.timing_elapsed_ns(decode_started_ns),
        decode_activity_before,
        checkpoint_activity.snapshot(),
    ));
    let (request, checksum, encoded_bytes) = decoded?;
    if request.installation_id != current.installation_id {
        return Err(device_reconnect_error(StatusCode::FORBIDDEN));
    }
    let installation_id = current.installation_id;
    let user = current.user;
    let timing_state = state.clone();
    let queue_started_ns = state.ingest.timing_now_ns();
    let queue_activity_before = checkpoint_activity.snapshot();
    let queue_observation = Arc::new(Mutex::new(None));
    let closure_observation = queue_observation.clone();
    let closure_activity = checkpoint_activity.clone();
    let response = run_blocking_ingest(move || {
        if let Ok(mut observation) = closure_observation.lock() {
            *observation = Some((
                state.ingest.timing_elapsed_ns(queue_started_ns),
                closure_activity.snapshot(),
            ));
        }
        state.ingest.ingest_with_attempt_sequence(
            &user,
            installation_id,
            request,
            checksum,
            encoded_bytes,
            attempt_seq,
        )
    })
    .await;
    let queue = queue_observation
        .lock()
        .ok()
        .and_then(|observation| *observation);
    if let Some((queue_ns, queue_activity_after)) = queue {
        handler_timing.blocking_queue = Some(timing_state.ingest.record_blocking_queue(
            queue_ns,
            queue_activity_before,
            queue_activity_after,
        ));
    }
    let response = response?;
    let attempt_seq = response.instrumentation_attempt_seq;
    handler_timing.outcome = "success";
    let mut response = Json(response).into_response();
    if let Ok(value) = HeaderValue::from_str(&attempt_seq.to_string()) {
        response
            .headers_mut()
            .insert("x-sensors-ingest-attempt-seq", value);
    }
    Ok(response)
}

async fn run_blocking_ingest<F>(operation: F) -> ApiResult<super::ingest::IngestBatchResponse>
where
    F: FnOnce() -> Result<super::ingest::IngestBatchResponse> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| internal(anyhow!(error).context("cloud ingest blocking task failed")))?
        .map_err(bad_request)
}

async fn admin_users(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_admin(&state, &headers).await?;
    Ok(Json(json!({"users": list_users_blocking(&state).await?})))
}

#[derive(Clone, Debug, Serialize)]
struct AdminOverviewUserSnapshot {
    user: User,
    status: LiveStatus,
    ecg: Vec<LiveEcg>,
    latest_sequence: u64,
}

#[derive(Clone, Debug, Serialize)]
struct AdminOverviewSnapshot {
    schema_version: u16,
    archive: ArchiveHealth,
    users: Vec<AdminOverviewUserSnapshot>,
}

#[derive(Clone, Debug, Serialize)]
struct AdminOverviewUserDelta {
    user_id: Uuid,
    batch: LiveDeltaBatch,
}

#[derive(Clone, Debug, Serialize)]
struct AdminOverviewDelta {
    schema_version: u16,
    heartbeat: bool,
    archive: ArchiveHealth,
    users: Vec<AdminOverviewUserDelta>,
}

fn overview_snapshot(
    state: &Arc<CloudState>,
    listed_users: Vec<User>,
    clients: Option<&CloudSseClients>,
) -> Result<AdminOverviewSnapshot> {
    let mut users = Vec::new();
    for user in listed_users {
        let runtime = ensure_dashboard_runtime_for_known_user(state, user.id)?;
        if let Some(clients) = clients {
            clients.track(user.id, runtime.clone())?;
        }
        let snapshot = runtime
            .lock()
            .map_err(|_| anyhow!("dashboard lock poisoned"))?
            .snapshot();
        let ecg_start = snapshot.ecg.len().saturating_sub(1_300);
        users.push(AdminOverviewUserSnapshot {
            user,
            status: snapshot.status,
            ecg: snapshot.ecg[ecg_start..].to_vec(),
            latest_sequence: snapshot.latest_seq,
        });
    }
    Ok(AdminOverviewSnapshot {
        schema_version: 1,
        archive: archive_health_snapshot(state),
        users,
    })
}

async fn admin_overview_snapshot(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Json<AdminOverviewSnapshot>> {
    require_admin(&state, &headers).await?;
    let users = list_users_blocking(&state).await?;
    Ok(Json(
        overview_snapshot(&state, users, None).map_err(internal)?,
    ))
}

async fn admin_overview_events(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>> {
    require_admin(&state, &headers).await?;
    let token = auth_token(&headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "authentication required"))?
        .to_string();
    let clients = Arc::new(CloudSseClients::default());
    let stream = stream::unfold(
        (
            state,
            token,
            HashMap::<Uuid, u64>::new(),
            0_u64,
            clients,
            false,
        ),
        |(state, token, mut checkpoints, mut ticks, clients, initialized)| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let still_admin = authenticate_access_blocking(&state, token.clone())
                .await
                .ok()
                .flatten()
                .is_some_and(|current| current.user.is_admin);
            if !still_admin {
                return None;
            }

            let users = match list_users_blocking(&state).await {
                Ok(users) => users,
                Err(error) => {
                    eprintln!(
                        "admin overview user listing failed request_id={}",
                        error.request_id
                    );
                    return None;
                }
            };
            // A new connection must not replay the retained 20,000-item ring
            // from sequence zero after the browser already has current data.
            // Capture each cursor under the same runtime lock as its snapshot,
            // so the following deltas include every newer change without a gap.
            if !initialized
                || users.len() != checkpoints.len()
                || users.iter().any(|user| !checkpoints.contains_key(&user.id))
            {
                let snapshot = match overview_snapshot(&state, users, Some(&clients)) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        eprintln!("admin overview initial snapshot failed: {error:#}");
                        return None;
                    }
                };
                checkpoints = snapshot
                    .users
                    .iter()
                    .map(|entry| (entry.user.id, entry.latest_sequence))
                    .collect();
                let event = match Event::default()
                    .event("overview_snapshot")
                    .json_data(&snapshot)
                {
                    Ok(event) => event,
                    Err(error) => {
                        clients.note_write_error(format!(
                            "overview snapshot serialization failed: {error}"
                        ));
                        return None;
                    }
                };
                return Some((Ok(event), (state, token, checkpoints, 0, clients, true)));
            }
            ticks = ticks.saturating_add(1);
            let heartbeat = ticks >= ADMIN_OVERVIEW_HEARTBEAT_TICKS;
            if heartbeat {
                ticks = 0;
            }
            let mut deltas = Vec::new();
            let mut remaining = 4_000_usize;
            for user in users {
                if remaining == 0 {
                    break;
                }
                let Ok(runtime) = ensure_dashboard_runtime_for_known_user(&state, user.id) else {
                    continue;
                };
                if let Err(error) = clients.track(user.id, runtime.clone()) {
                    eprintln!("admin overview SSE accounting failed: {error:#}");
                    continue;
                }
                let after = checkpoints.get(&user.id).copied().unwrap_or(0);
                let limit = remaining.min(ADMIN_OVERVIEW_USER_BATCH_LIMIT);
                let batch = match runtime.lock() {
                    Ok(mut runtime) => {
                        let batch = runtime.delta_batch_after(after, limit);
                        if !batch.items.is_empty() || batch.overflow {
                            runtime.note_sse_batch(batch.items.len(), batch.overflow);
                        }
                        batch
                    }
                    Err(_) => continue,
                };
                checkpoints.insert(user.id, batch.to_seq);
                remaining = remaining.saturating_sub(batch.items.len());
                if !batch.items.is_empty() || batch.overflow || heartbeat {
                    deltas.push(AdminOverviewUserDelta {
                        user_id: user.id,
                        batch,
                    });
                }
            }
            let payload = AdminOverviewDelta {
                schema_version: 1,
                heartbeat,
                archive: archive_health_snapshot(&state),
                users: deltas,
            };
            let event = match Event::default().event("overview").json_data(&payload) {
                Ok(event) => event,
                Err(error) => {
                    clients.note_write_error(format!("overview serialization failed: {error}"));
                    Event::default()
                        .event("error")
                        .data(format!("overview serialization failed: {error}"))
                }
            };
            Some((Ok(event), (state, token, checkpoints, ticks, clients, true)))
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn admin_overview(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    if let Err(error) = require_admin(&state, &headers).await {
        return Ok(html_error_or_session_bootstrap(error));
    }
    let users = list_users_blocking(&state).await?;
    let rows = users
        .iter()
        .map(|user| {
            format!(
                "<article data-user=\"{}\" data-disabled=\"{}\"><h2>{} <span class=muted>&lt;{}&gt;{}</span></h2><p><a href=\"/dashboard/{}\">Complete dashboard</a> · <span class=status>loading…</span> · <button class=disable-user>{}</button></p><div class=installations>loading connected app installations…</div><div class=sessions>loading browser sessions…</div><div class=details>waiting for multiplexed live status…</div><canvas width=1300 height=150></canvas></article>",
                user.id,
                user.disabled,
                escape_html(&user.display_name),
                escape_html(&user.email),
                if user.is_admin { " · admin" } else { "" },
                user.id,
                if user.disabled { "Enable user" } else { "Disable user" },
            )
        })
        .collect::<String>();
    Ok(Html(ADMIN_HTML.replace("<!--USERS-->", &rows)).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InvitationRequest {
    email: String,
}

async fn admin_invitations(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    require_admin(&state, &headers).await?;
    let invitation_state = state.clone();
    let invitations = run_blocking_internal("admin invitation-list task failed", move || {
        invitation_state.db.list_invitations()
    })
    .await?;
    Ok(Json(json!({
        "invitations": invitations
    })))
}

async fn admin_invitation(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Json(request): Json<InvitationRequest>,
) -> ApiResult<Json<Value>> {
    let admin = require_admin_mutation(&state, &headers).await?;
    let email = normalize_email(&request.email).map_err(bad_request)?;
    let token = random_token();
    let now = unix_ms();
    let expires = now.saturating_add(INVITATION_LIFETIME_MS);
    let invitation_state = state.clone();
    let invitation_email = email.clone();
    let invitation_token = token.clone();
    let invitation_id =
        run_blocking_bad_request("admin invitation-create task failed", move || {
            invitation_state.db.issue_invitation(
                &invitation_email,
                &invitation_token,
                admin.user.id,
                now,
                expires,
            )
        })
        .await?;
    let registration_url = format!("{}/invite#{token}", state.public_origin);
    if let Err(error) =
        send_invitation_email(&state.resend, &email, &registration_url, expires).await
    {
        eprintln!("invitation {invitation_id} remains valid after Resend failure: {error:#}");
        return Err(ApiError::new(
            StatusCode::BAD_GATEWAY,
            "invitation remains valid, but email delivery failed; use admin resend",
        ));
    }
    Ok(Json(json!({
        "invitation_id": invitation_id,
        "email": email,
        "expires_at_unix_ms": expires,
        "email_delivery": "accepted"
    })))
}

async fn admin_revoke_invitation(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(invitation_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    require_admin_mutation(&state, &headers).await?;
    let invitation_state = state.clone();
    if !run_blocking_internal("admin invitation-revoke task failed", move || {
        invitation_state
            .db
            .revoke_invitation(invitation_id, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "invitation is missing, consumed, or already revoked",
        ));
    }
    Ok(Json(json!({"ok": true, "invitation_id": invitation_id})))
}

async fn admin_resend_invitation(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(invitation_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    let admin = require_admin_mutation(&state, &headers).await?;
    let lookup_state = state.clone();
    let old = run_blocking_internal("admin invitation-lookup task failed", move || {
        lookup_state.db.invitation_by_id(invitation_id)
    })
    .await?
    .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "invitation not found"))?;
    if old.consumed_unix_ms.is_some() || old.revoked_unix_ms.is_some() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "only an active or expired unconsumed invitation can be replaced",
        ));
    }
    let token = random_token();
    let now = unix_ms();
    let expires = now.saturating_add(INVITATION_LIFETIME_MS);
    let replacement_state = state.clone();
    let replacement_email = old.email.clone();
    let replacement_token = token.clone();
    let replacement_id =
        run_blocking_bad_request("admin replacement-invitation task failed", move || {
            replacement_state.db.issue_invitation(
                &replacement_email,
                &replacement_token,
                admin.user.id,
                now,
                expires,
            )
        })
        .await?;
    let registration_url = format!("{}/invite#{token}", state.public_origin);
    if let Err(error) =
        send_invitation_email(&state.resend, &old.email, &registration_url, expires).await
    {
        eprintln!(
            "replacement invitation {replacement_id} remains valid after Resend failure: {error:#}"
        );
        return Err(ApiError::new(
            StatusCode::BAD_GATEWAY,
            "replacement invitation remains valid, but email delivery failed",
        ));
    }
    let revoke_state = state.clone();
    run_blocking_internal("replaced-invitation revoke task failed", move || {
        revoke_state.db.revoke_invitation(invitation_id, now)
    })
    .await?;
    Ok(Json(json!({
        "ok": true,
        "replaced_invitation_id": invitation_id,
        "invitation_id": replacement_id,
        "email": old.email,
        "expires_at_unix_ms": expires,
        "email_delivery": "accepted"
    })))
}

async fn admin_installations(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    require_admin(&state, &headers).await?;
    let installation_state = state.clone();
    let installations = run_blocking_internal("admin installation-list task failed", move || {
        installation_state.db.list_installations()
    })
    .await?;
    Ok(no_store_json(json!({"installations": installations})))
}

async fn admin_revoke_device_credential(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(credential_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    require_admin_mutation(&state, &headers).await?;
    let installation_state = state.clone();
    if !run_blocking_internal("admin device-credential revoke task failed", move || {
        installation_state
            .db
            .revoke_device_credential(credential_id, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "device credential not found",
        ));
    }
    Ok(Json(json!({"ok": true, "credential_id": credential_id})))
}

#[derive(Clone)]
struct ResendConfig {
    api_key: String,
    from: String,
    reply_to: String,
}

async fn send_invitation_email(
    config: &ResendConfig,
    to: &str,
    registration_url: &str,
    _expires_unix_ms: i64,
) -> Result<()> {
    send_email(
        config,
        to,
        "Your Training Sensors invitation",
        &format!(
            "Open this one-use invitation to choose your password:\n\n{registration_url}\n\nThis link works for 7 days. Opening the link does not consume it; successful registration does."
        ),
    )
    .await
}

async fn send_password_reset_email(
    config: &ResendConfig,
    to: &str,
    reset_url: &str,
    _expires_unix_ms: i64,
) -> Result<()> {
    send_email(
        config,
        to,
        "Reset your Training Sensors password",
        &format!(
            "Open this one-use link to choose a new password:\n\n{reset_url}\n\nThis link works for one hour. If you did not request this, you can ignore this message."
        ),
    )
    .await
}

async fn send_email(config: &ResendConfig, to: &str, subject: &str, text: &str) -> Result<()> {
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("build email HTTP client")?
        .post("https://api.resend.com/emails")
        .bearer_auth(&config.api_key)
        .json(&json!({
            "from": config.from,
            "to": [to],
            "reply_to": config.reply_to,
            "subject": subject,
            "text": text,
        }))
        .send()
        .await
        .context("send email-provider request")?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(anyhow!("email provider returned {status}"));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DisableUserRequest {
    disabled: bool,
}

async fn admin_disable_user(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(request): Json<DisableUserRequest>,
) -> ApiResult<Json<Value>> {
    let admin = require_admin_mutation(&state, &headers).await?;
    if admin.user.id == user_id && request.disabled {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "admin cannot disable its own active account",
        ));
    }
    let disable_state = state.clone();
    if !run_blocking_internal("admin user-disable task failed", move || {
        disable_state
            .db
            .set_user_disabled(user_id, request.disabled, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "user not found"));
    }
    Ok(Json(
        json!({"ok": true, "user_id": user_id, "disabled": request.disabled}),
    ))
}

async fn dashboard_html(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Response> {
    if let Err(error) = authorize_user(&state, &headers, user_id).await {
        return Ok(html_error_or_session_bootstrap(error));
    }
    let base = format!("/api/v1/users/{user_id}/dashboard");
    let worker = format!("/dashboard/{user_id}/live-worker.js");
    let html = DASHBOARD_HTML
        .replace("/live-worker.js", &worker)
        .replace("/api/status", &format!("{base}/status"))
        .replace("/api/latest", &format!("{base}/latest"))
        .replace("/api/marker", &format!("{base}/marker"))
        .replace("/events", &format!("{base}/events"));
    let html = format!("{html}{BROWSER_SESSION_JS}");
    Ok(Html(html).into_response())
}

async fn dashboard_worker(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Response> {
    authorize_user(&state, &headers, user_id).await?;
    let body = LIVE_WORKER_JS.replace(
        "/events",
        &format!("/api/v1/users/{user_id}/dashboard/events"),
    );
    Ok((
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        body,
    )
        .into_response())
}

async fn dashboard_status(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    authorize_user(&state, &headers, user_id).await?;
    let runtime = dashboard_runtime(&state, user_id).await?;
    let status = {
        let mut runtime = runtime
            .lock()
            .map_err(|_| internal(anyhow!("dashboard lock poisoned")))?;
        runtime.poll_once().map_err(internal)?;
        runtime.status()
    };
    Ok(Json(
        dashboard_payload_with_archive(status, archive_health_snapshot(&state), false)
            .map_err(internal)?,
    ))
}

async fn dashboard_latest(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Json<Value>> {
    authorize_user(&state, &headers, user_id).await?;
    let runtime = dashboard_runtime(&state, user_id).await?;
    let snapshot = {
        let mut runtime = runtime
            .lock()
            .map_err(|_| internal(anyhow!("dashboard lock poisoned")))?;
        runtime.poll_once().map_err(internal)?;
        runtime.snapshot()
    };
    Ok(Json(
        dashboard_payload_with_archive(snapshot, archive_health_snapshot(&state), true)
            .map_err(internal)?,
    ))
}

async fn dashboard_events(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Query(query): Query<DashboardEventsQuery>,
) -> ApiResult<Sse<impl futures::Stream<Item = std::result::Result<Event, Infallible>>>> {
    authorize_user(&state, &headers, user_id).await?;
    let token = auth_token(&headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "authentication required"))?
        .to_string();
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let resume_seq = query.since.or(last_event_id).unwrap_or(0);
    let runtime = dashboard_runtime(&state, user_id).await?;
    let clients = Arc::new(CloudSseClients::default());
    clients.track(user_id, runtime.clone()).map_err(internal)?;
    let stream = stream::unfold(
        (runtime, resume_seq, state, token, clients),
        move |(runtime, last_seq, state, token, clients)| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let still_authorized = authenticate_access_blocking(&state, token.clone())
                .await
                .ok()
                .flatten()
                .is_some_and(|current| current.user.id == user_id || current.user.is_admin);
            if !still_authorized {
                return None;
            }
            let (event, next_seq) = match runtime.lock() {
                Ok(mut locked) => {
                    let _ = locked.poll_once();
                    let batch = locked.delta_batch_after(last_seq, 4_000);
                    let next = batch.to_seq;
                    if !batch.items.is_empty() || batch.overflow {
                        locked.note_sse_batch(batch.items.len(), batch.overflow);
                    }
                    let event = Event::default()
                        .event("batch")
                        .id(next.to_string())
                        .json_data(batch)
                        .unwrap_or_else(|error| {
                            locked.note_sse_write_error(format!(
                                "cloud dashboard serialization failed: {error}"
                            ));
                            Event::default().event("error").data("serialization failed")
                        });
                    (event, next)
                }
                Err(_) => (
                    Event::default()
                        .event("error")
                        .data("dashboard unavailable"),
                    last_seq,
                ),
            };
            Some((Ok(event), (runtime, next_seq, state, token, clients)))
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(2))))
}

#[derive(Default, Deserialize)]
struct DashboardEventsQuery {
    since: Option<u64>,
}

async fn dashboard_marker(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(request): Json<MarkerPostRequest>,
) -> ApiResult<Json<impl Serialize>> {
    authorize_user_mutation(&state, &headers, user_id).await?;
    let runtime = dashboard_runtime(&state, user_id).await?;
    let response = runtime
        .lock()
        .map_err(|_| internal(anyhow!("dashboard lock poisoned")))?
        .post_marker(request)
        .map_err(internal)?;
    Ok(Json(response))
}

/// "Your devices" listing: the target user's signed-in browser session
/// families and app installations, self-or-admin scoped like the dashboard
/// routes. Token hashes never appear in the response; `current` marks the
/// caller's own rotation family so the UI can label "this browser".
async fn user_devices(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Response> {
    let caller = authorize_user(&state, &headers, user_id).await?;
    let caller_session_id = caller.session_id;
    let db_state = state.clone();
    let (devices, installations, caller_family) =
        run_blocking_internal("user device-list task failed", move || {
            let now_ms = unix_ms();
            let devices = db_state.db.list_browser_session_devices(user_id, now_ms)?;
            let installations = db_state.db.list_installations_for_user(user_id)?;
            let caller_family = db_state.db.session_family_root(caller_session_id)?;
            Ok((devices, installations, caller_family))
        })
        .await?;
    let browser_sessions = devices
        .into_iter()
        .map(|device| {
            let current = caller_family == Some(device.family_id);
            let mut value = serde_json::to_value(&device)
                .context("serialize browser session device")
                .map_err(internal)?;
            if let Some(object) = value.as_object_mut() {
                object.insert("current".to_string(), Value::Bool(current));
            }
            Ok(value)
        })
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(no_store_json(json!({
        "browser_sessions": browser_sessions,
        "installations": installations,
    })))
}

/// Per-device sign-out for the "Your devices" view. 404 unless the family is
/// a browser session family belonging to the target user; revoking your own
/// current family is a normal sign-out.
async fn revoke_user_browser_session(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path((user_id, family_id)): Path<(Uuid, Uuid)>,
) -> ApiResult<Response> {
    authorize_user_mutation(&state, &headers, user_id).await?;
    let revoke_state = state.clone();
    if !run_blocking_internal("user session-family revoke task failed", move || {
        revoke_state
            .db
            .revoke_session_family_for_user(family_id, user_id, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "browser session not found",
        ));
    }
    Ok(no_store_json(json!({"ok": true})))
}

/// "Disconnect app" for the "Your devices" view. 404 unless the installation
/// belongs to the target user; afterwards the phone's next upload gets the
/// established `device_reconnect_required` 401 while local collection
/// continues, and re-enrolling restores upload.
async fn revoke_user_installation(
    State(state): State<Arc<CloudState>>,
    headers: HeaderMap,
    Path((user_id, installation_id)): Path<(Uuid, Uuid)>,
) -> ApiResult<Response> {
    authorize_user_mutation(&state, &headers, user_id).await?;
    let revoke_state = state.clone();
    if !run_blocking_internal("user installation revoke task failed", move || {
        revoke_state
            .db
            .revoke_installation_for_user(installation_id, user_id, unix_ms())
    })
    .await?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "installation not found",
        ));
    }
    Ok(no_store_json(json!({"ok": true})))
}

async fn dashboard_runtime(
    state: &Arc<CloudState>,
    user_id: Uuid,
) -> ApiResult<Arc<Mutex<LiveRuntime>>> {
    require_dashboard_target(state, user_id).await?;
    if let Some(runtime) = dashboard_runtime_if_initialized(state, user_id).map_err(internal)? {
        return Ok(runtime);
    }
    ensure_dashboard_runtime_for_known_user(state, user_id).map_err(internal)
}

fn dashboard_runtime_if_initialized(
    state: &Arc<CloudState>,
    user_id: Uuid,
) -> Result<Option<Arc<Mutex<LiveRuntime>>>> {
    if let Some(runtime) = state
        .dashboards
        .lock()
        .map_err(|_| anyhow!("dashboard registry lock poisoned"))?
        .get(&user_id)
        .cloned()
    {
        return Ok(Some(runtime));
    }
    Ok(None)
}

fn ensure_dashboard_runtime_for_known_user(
    state: &Arc<CloudState>,
    user_id: Uuid,
) -> Result<Arc<Mutex<LiveRuntime>>> {
    if let Some(runtime) = dashboard_runtime_if_initialized(state, user_id)? {
        return Ok(runtime);
    }

    // Critical startup can touch WAL. Do it without holding the shared tenant
    // registry so one user's restart recovery cannot block another dashboard.
    let mut config = LiveServerConfig::new(state.data_dir.clone(), 0);
    config.participant_id = user_id.to_string();
    // Bounded lookback for cloud tenants: memory scales with this window per
    // active runtime (the 10 h single-user default needed gigabytes per
    // tenant); longer lookback belongs to the archive/replay path.
    config.history_window = CLOUD_HISTORY_WINDOW;
    let runtime = Arc::new(Mutex::new(LiveRuntime::new(config.clone())?));
    let mut dashboards = state
        .dashboards
        .lock()
        .map_err(|_| anyhow!("dashboard registry lock poisoned"))?;
    if let Some(runtime) = dashboards.get(&user_id) {
        return Ok(runtime.clone());
    }
    dashboards.insert(user_id, runtime.clone());
    drop(dashboards);
    start_runtime_tasks(runtime.clone(), config);
    Ok(runtime)
}

async fn authorize_user(
    state: &Arc<CloudState>,
    headers: &HeaderMap,
    target: Uuid,
) -> ApiResult<SessionUser> {
    let current = session(state, headers).await?;
    if current.user.id != target && !current.user.is_admin {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "dashboard access denied",
        ));
    }
    require_dashboard_target(state, target).await?;
    Ok(current)
}

async fn authorize_user_mutation(
    state: &Arc<CloudState>,
    headers: &HeaderMap,
    target: Uuid,
) -> ApiResult<SessionUser> {
    let current = mutation_session(state, headers).await?;
    if current.user.id != target && !current.user.is_admin {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "dashboard access denied",
        ));
    }
    require_dashboard_target(state, target).await?;
    Ok(current)
}

async fn require_dashboard_target(state: &Arc<CloudState>, target: Uuid) -> ApiResult<()> {
    let lookup_state = state.clone();
    let available = run_blocking_internal("dashboard user-lookup task failed", move || {
        Ok(lookup_state
            .db
            .user_by_id(target)?
            .is_some_and(|user| user.retired_unix_ms.is_none()))
    })
    .await?;
    if !available {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "dashboard access denied",
        ));
    }
    Ok(())
}

async fn require_admin(state: &Arc<CloudState>, headers: &HeaderMap) -> ApiResult<SessionUser> {
    let current = session(state, headers).await?;
    if !current.user.is_admin {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "admin access required",
        ));
    }
    Ok(current)
}

async fn require_admin_mutation(
    state: &Arc<CloudState>,
    headers: &HeaderMap,
) -> ApiResult<SessionUser> {
    let current = mutation_session(state, headers).await?;
    if !current.user.is_admin {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "admin access required",
        ));
    }
    Ok(current)
}

async fn session(state: &Arc<CloudState>, headers: &HeaderMap) -> ApiResult<SessionUser> {
    let token = auth_token(headers)
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "authentication required"))?
        .to_string();
    authenticate_access_blocking(state, token)
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "session is invalid or expired"))
}

async fn authenticate_device_blocking(
    state: &Arc<CloudState>,
    credential: String,
) -> ApiResult<Option<DevicePrincipal>> {
    let state = state.clone();
    run_blocking_internal("cloud device authentication task failed", move || {
        state.auth.authenticate_device(&credential, unix_ms())
    })
    .await
}

async fn authenticate_access_blocking(
    state: &Arc<CloudState>,
    token: String,
) -> ApiResult<Option<SessionUser>> {
    let state = state.clone();
    run_blocking_internal(
        "cloud access-session authentication task failed",
        move || state.auth.authenticate_access(&token, unix_ms()),
    )
    .await
}

async fn list_users_blocking(state: &Arc<CloudState>) -> ApiResult<Vec<User>> {
    let state = state.clone();
    run_blocking_internal("cloud user-listing task failed", move || {
        state.db.list_users()
    })
    .await
}

async fn run_blocking_internal<T, F>(context: &'static str, operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| internal(anyhow!(error).context(context)))?
        .map_err(|error| internal(error.context(context)))
}

async fn run_blocking_bad_request<T, F>(context: &'static str, operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| internal(anyhow!(error).context(context)))?
        .map_err(bad_request)
}

async fn run_blocking_public<T, F>(context: &'static str, operation: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> std::result::Result<T, PublicAuthError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| internal(anyhow!(error).context(context)))?
        .map_err(|error| match error {
            PublicAuthError::Invalid => ApiError::new(
                StatusCode::BAD_REQUEST,
                "request is invalid, expired, or unavailable",
            ),
            PublicAuthError::InvitationTerminal => ApiError::with_code(
                StatusCode::BAD_REQUEST,
                "invalid_invitation",
                "invitation is invalid, expired, or unavailable",
            ),
            PublicAuthError::PasswordResetTerminal => ApiError::with_code(
                StatusCode::BAD_REQUEST,
                "password_reset_terminal",
                "password reset link is invalid, expired, or already used",
            ),
            PublicAuthError::RateLimited => ApiError::with_code(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many attempts; try again later",
            ),
            PublicAuthError::EnrollmentTerminal => ApiError::with_code(
                StatusCode::BAD_REQUEST,
                "enrollment_link_terminal",
                "connection link is invalid, expired, or already used",
            ),
            PublicAuthError::Internal(error) => internal(error.context(context)),
        })
}

fn password_work_permit(state: &Arc<CloudState>) -> ApiResult<tokio::sync::OwnedSemaphorePermit> {
    state
        .password_work_limit
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::with_code(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "password service is busy; try again later",
            )
        })
}

fn auth_token(headers: &HeaderMap) -> Option<&str> {
    cookie_value(headers, "sensors_access")
}

async fn mutation_session(state: &Arc<CloudState>, headers: &HeaderMap) -> ApiResult<SessionUser> {
    require_exact_origin(state, headers)?;
    let current = session(state, headers).await?;
    let csrf = csrf_header(headers)?;
    if crate::cloud::db::hash_secret(csrf) != current.csrf_hash {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "CSRF token is invalid",
        ));
    }
    Ok(current)
}

fn require_exact_origin(state: &Arc<CloudState>, headers: &HeaderMap) -> ApiResult<()> {
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::new(StatusCode::FORBIDDEN, "Origin header is required"))?;
    if origin != state.public_origin {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "request Origin is not the relying-party origin",
        ));
    }
    Ok(())
}

fn csrf_header(headers: &HeaderMap) -> ApiResult<&str> {
    headers
        .get("x-csrf-token")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::new(StatusCode::FORBIDDEN, "CSRF token is required"))
}

/// Browser identity label stored per session for the "Your devices" view:
/// the User-Agent header with control characters stripped, truncated to 256
/// bytes on a character boundary. Untrusted display data, never parsed
/// server-side.
const USER_AGENT_MAX_BYTES: usize = 256;

fn sanitized_user_agent(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::USER_AGENT)?.to_str().ok()?;
    let mut cleaned: String = raw
        .chars()
        .filter(|character| !character.is_control())
        .collect();
    while cleaned.len() > USER_AGENT_MAX_BYTES {
        cleaned.pop();
    }
    (!cleaned.is_empty()).then_some(cleaned)
}

fn device_credential(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Device ")
        .filter(|token| !token.is_empty())
}

fn device_reconnect_error(status: StatusCode) -> ApiError {
    ApiError::with_code(
        status,
        "device_reconnect_required",
        "device must be connected again",
    )
}

fn auth_client_key(headers: &HeaderMap) -> String {
    headers
        // Caddy must overwrite this header from its connection peer before
        // proxying to the loopback-only Rust listener. Caller-controlled
        // forwarding headers are deliberately ignored.
        .get("x-sensors-client-ip")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 64)
        .and_then(|value| value.parse::<std::net::IpAddr>().ok())
        .map(|address| address.to_string())
        .unwrap_or_else(|| SHARED_AUTH_CLIENT_KEY.to_string())
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    for part in headers.get(header::COOKIE)?.to_str().ok()?.split(';') {
        let (key, value) = part.trim().split_once('=')?;
        if key == name {
            return Some(value);
        }
    }
    None
}

fn token_response(tokens: TokenResponse) -> ApiResult<Response> {
    let now_ms = unix_ms();
    let access_cookie = format!(
        "sensors_access={}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={}",
        tokens.access_token,
        (tokens.access_expires_at_unix_ms - now_ms).max(0) / 1_000
    );
    let refresh_cookie = format!(
        "sensors_refresh={}; Path=/api/v1/auth; Secure; HttpOnly; SameSite=Strict; Max-Age={}",
        tokens.refresh_token,
        (tokens.refresh_expires_at_unix_ms - now_ms).max(0) / 1_000
    );
    let rotation_cookie = format!(
        "sensors_rotation={}; Path=/api/v1/auth; Secure; HttpOnly; SameSite=Strict; Max-Age={}",
        Uuid::new_v4(),
        (tokens.refresh_expires_at_unix_ms - now_ms).max(0) / 1_000
    );
    let csrf_cookie = format!(
        "sensors_csrf={}; Path=/; Secure; SameSite=Strict; Max-Age={}",
        tokens.csrf_token,
        (tokens.refresh_expires_at_unix_ms - now_ms).max(0) / 1_000
    );
    let mut response = Json(json!({
        "ok": true,
        "access_expires_at_unix_ms": tokens.access_expires_at_unix_ms,
        "refresh_expires_at_unix_ms": tokens.refresh_expires_at_unix_ms
    }))
    .into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&access_cookie).map_err(|_| {
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid session cookie")
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&refresh_cookie).map_err(|_| {
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid refresh cookie")
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&rotation_cookie).map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid refresh state cookie",
            )
        })?,
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&csrf_cookie)
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid CSRF cookie"))?,
    );
    mark_no_store(&mut response);
    Ok(response)
}

fn mark_no_store(response: &mut Response) {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response.headers_mut().insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}

fn no_store_html(html: &str) -> Response {
    let mut response = Html(html.to_string()).into_response();
    mark_no_store(&mut response);
    response
}

fn no_store_json(value: Value) -> Response {
    let mut response = Json(value).into_response();
    mark_no_store(&mut response);
    response
}

fn clear_browser_cookies(response: &mut Response) {
    for cookie in [
        "sensors_access=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0",
        "sensors_csrf=; Path=/; Secure; SameSite=Strict; Max-Age=0",
        "sensors_refresh=; Path=/api/v1/auth; Secure; HttpOnly; SameSite=Strict; Max-Age=0",
        "sensors_rotation=; Path=/api/v1/auth; Secure; HttpOnly; SameSite=Strict; Max-Age=0",
    ] {
        response
            .headers_mut()
            .append(header::SET_COOKIE, HeaderValue::from_static(cookie));
    }
}

type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    request_id: Uuid,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        let code = match status {
            StatusCode::UNAUTHORIZED => "authentication_required",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::CONFLICT => "conflict",
            StatusCode::PAYLOAD_TOO_LARGE => "payload_too_large",
            StatusCode::BAD_REQUEST => "invalid_request",
            _ => "request_failed",
        };
        Self::with_code(status, code, message)
    }

    fn with_code(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            request_id: Uuid::new_v4(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({
                "ok": false,
                "code": self.code,
                "error": self.message,
                "request_id": self.request_id,
            })),
        )
            .into_response();
        if let Ok(value) = HeaderValue::from_str(&self.request_id.to_string()) {
            response.headers_mut().insert("x-request-id", value);
        }
        response
    }
}

fn bad_request(error: anyhow::Error) -> ApiError {
    let api = ApiError::new(StatusCode::BAD_REQUEST, "request is invalid");
    eprintln!(
        "cloud invalid request request_id={}: {error:#}",
        api.request_id
    );
    api
}

fn internal(error: anyhow::Error) -> ApiError {
    let api = ApiError::with_code(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "internal server error",
    );
    eprintln!(
        "cloud internal error request_id={}: {error:#}",
        api.request_id
    );
    api
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .with_context(|| format!("required environment variable {name} is not configured"))
}

fn unix_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

const LOGIN_HTML: &str = r#"<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Training Sensors account</title>
<style>body{font:16px system-ui;max-width:38rem;margin:4rem auto;padding:1rem}input,button{font:inherit;padding:.65rem;margin:.3rem 0;width:100%;box-sizing:border-box}label{display:block;margin-top:.7rem}.status{white-space:pre-wrap;color:#922}.muted{color:#667}.show{display:flex;align-items:center;gap:.4rem;margin-top:.3rem}.show input{width:auto;margin:0}</style>
<h1>Training Sensors</h1>
<section id=loginPanel hidden><h2>Sign in</h2><form id=loginForm><label>Email<input id=loginEmail required type=email maxlength=254 autocomplete=username></label><label>Password<input id=loginPassword required type=password autocomplete=current-password></label><label class="show muted"><input id=loginShow type=checkbox> Show password</label><button>Sign in</button></form><p><a href="/forgot-password">Forgotten your password?</a></p></section>
<section id=invitePanel hidden><h2>Accept invitation</h2><p class=muted>This invitation is for:</p><p><output id=inviteEmail>checking invitation…</output></p><form id=registerForm><p class=muted>Passwords must be 12–128 UTF-8 bytes.</p><label>Choose password<input id=registerPassword required type=password autocomplete=new-password></label><label>Confirm password<input id=registerConfirm required type=password autocomplete=new-password></label><label class="show muted"><input id=registerShow type=checkbox> Show passwords</label><button>Create account</button></form></section>
<section id=forgotPanel hidden><h2>Reset password</h2><p>Enter your email. The response is the same whether or not an active account exists.</p><form id=forgotForm><label>Email<input id=forgotEmail required type=email maxlength=254 autocomplete=username></label><button>Send reset link</button></form><p><a href="/login">Back to sign in</a></p></section>
<section id=resetPanel hidden><h2>Choose a new password</h2><form id=resetForm><p class=muted>Passwords must be 12–128 UTF-8 bytes.</p><label>New password<input id=resetPassword required type=password autocomplete=new-password></label><label>Confirm password<input id=resetConfirm required type=password autocomplete=new-password></label><label class="show muted"><input id=resetShow type=checkbox> Show passwords</label><button>Reset password</button></form></section>
<p id=status class=status role=status></p>
<script>
const status=document.querySelector('#status'),path=location.pathname,secret=location.hash.length>1?location.hash.slice(1):'';if(location.hash)history.replaceState(null,'',path);
async function call(url,body){const r=await fetch(url,{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'},body:JSON.stringify(body)}),j=await r.json().catch(()=>({}));if(!r.ok)throw Error(j.error||`request failed (${r.status})`);return j}
function same(a,b){if(a!==b)throw Error('Passwords do not match');return a}
function validPassword(value){const bytes=new TextEncoder().encode(value).length;if(bytes<12||bytes>128||value.includes('\0'))throw Error('Password must be 12–128 UTF-8 bytes and contain no NUL character.');return value}
function showToggle(id,...fields){const box=document.querySelector(id);box.onchange=()=>fields.forEach(f=>document.querySelector(f).type=box.checked?'text':'password')}
showToggle('#loginShow','#loginPassword');showToggle('#registerShow','#registerPassword','#registerConfirm');showToggle('#resetShow','#resetPassword','#resetConfirm');
async function submit(form,work){form.onsubmit=async e=>{e.preventDefault();status.textContent='Working…';try{await work();location='/'}catch(error){status.textContent=error.message||String(error)}}}
if(path==='/invite'){document.querySelector('#invitePanel').hidden=false;if(!secret){status.textContent='This invitation link is incomplete.';document.querySelector('#registerForm').hidden=true}else{call('/api/v1/auth/invitation',{invitation_token:secret}).then(j=>document.querySelector('#inviteEmail').textContent=j.email).catch(e=>{status.textContent=e.message;document.querySelector('#registerForm').hidden=true})}submit(document.querySelector('#registerForm'),()=>call('/api/v1/auth/password/register',{invitation_token:secret,password:validPassword(same(document.querySelector('#registerPassword').value,document.querySelector('#registerConfirm').value))}))}
else if(path==='/forgot-password'){document.querySelector('#forgotPanel').hidden=false;document.querySelector('#forgotForm').onsubmit=async e=>{e.preventDefault();status.textContent='Working…';try{const j=await call('/api/v1/auth/password-reset/request',{email:document.querySelector('#forgotEmail').value});status.textContent=j.message}catch(error){status.textContent=error.message||String(error)}}}
else if(path==='/reset-password'){document.querySelector('#resetPanel').hidden=false;if(!secret){status.textContent='This reset link is incomplete.';document.querySelector('#resetForm').hidden=true}submit(document.querySelector('#resetForm'),()=>call('/api/v1/auth/password-reset/finish',{reset_token:secret,password:validPassword(same(document.querySelector('#resetPassword').value,document.querySelector('#resetConfirm').value))}))}
else{document.querySelector('#loginPanel').hidden=false;submit(document.querySelector('#loginForm'),()=>call('/api/v1/auth/password/login',{email:document.querySelector('#loginEmail').value,password:validPassword(document.querySelector('#loginPassword').value)}))}
</script>"#;

const ENROLL_FALLBACK_HTML: &str = r#"<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Open Training Sensors</title><style>body{font:16px system-ui;max-width:38rem;margin:3rem auto;padding:1rem}a.download{display:inline-block;padding:.65rem;border:1px solid #889;border-radius:.4rem}.muted{color:#667}code{overflow-wrap:anywhere}</style><h1>Connect Training Sensors</h1><div id="steps" class="muted">Checking this connection link&hellip;</div><script>
const host=document.querySelector('#steps');
function para(text,muted){const p=document.createElement('p');p.textContent=text;if(muted)p.className='muted';return p}
function heading(text){const h=document.createElement('h2');h.textContent=text;return h}
async function boot(){
const token=location.hash.slice(1);
if(!token){host.textContent='';host.append(
para('This page opens from a Training Sensors connection link or QR code.'),
para('On your signed-in device, choose "Connect a phone with QR" and scan the code with this phone.'));return}
let j,r=null;
try{
r=await fetch('/api/v1/device-enrollments/preview',{method:'POST',cache:'no-store',headers:{'content-type':'application/json'},body:JSON.stringify({enrollment_token:token})});
}catch(e){r=null}
if(!r||r.status===429){host.textContent='';host.append(
para('Could not check this connection link right now.'),
para('Wait a moment and reload this page. The link itself may still be valid.'));return}
j=await r.json().catch(()=>({}));
if(!r.ok){host.textContent='';host.append(
para('This connection link is invalid or has expired.'),
para('On your signed-in device, press "Regenerate QR code", then scan the new code with this phone.'));return}
host.textContent='';
const download=document.createElement('a');download.className='download';download.href='/downloads/training-sensors.apk';download.setAttribute('download','training-sensors.apk');download.textContent='Download the Training Sensors app (APK)';
const downloadRow=document.createElement('p');downloadRow.append(download);
const meta=document.createElement('p');meta.className='muted';const metaCode=document.createElement('code');metaCode.textContent=String(j.apk&&j.apk.sha256||'');meta.append('Version '+String(j.apk&&j.apk.version_name||'')+' (code '+String(j.apk&&j.apk.version_code||'')+'). SHA-256: ',metaCode);
host.append(
heading('1. Install the app'),
downloadRow,
meta,
para('Open the finished download and tap Install. If Android blocks it, open the offered Settings screen, allow this browser to install unknown apps, then return and install.'),
heading('2. Connect'),
para('After installing, scan the QR code again. Android will open the Training Sensors app to finish connecting. No sign-in is needed on this phone.'),
para('This one-use link expires '+new Date(Number(j.expires_at_unix_ms)).toLocaleString()+'. If it expires, press "Regenerate QR code" on your signed-in device and scan again.',true));
}
boot();
</script>"#;

const HOME_ENROLLMENT_JS: &str = r#"<script>
async function createEnrollment(){const r=await fetch('/api/v1/device-enrollments',{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json','x-csrf-token':sensorsCookie('sensors_csrf')},body:'{}'}),j=await r.json().catch(()=>({}));if(!r.ok)throw Error(j.error||`request failed (${r.status})`);return j}
function expiryText(value){return `This one-use link expires ${new Date(Number(value)).toLocaleString()}.`}
document.querySelector('#connectThisPhone').onclick=async()=>{const host=document.querySelector('#enrollment');host.textContent='Creating secure connection…';try{const j=await createEnrollment(),p=document.createElement('p'),a=document.createElement('a');a.href=j.enrollment_url;a.textContent='Open the Training Sensors app';p.append(a);host.textContent='';host.append(p,document.createTextNode(expiryText(j.expires_at_unix_ms)))}catch(e){host.textContent=e.message||String(e)}};
document.querySelector('#showEnrollmentQr').onclick=async()=>{const host=document.querySelector('#enrollment');host.textContent='Creating secure QR code…';try{const j=await createEnrollment(),qr=document.createElement('div'),p=document.createElement('p'),again=document.createElement('button');qr.innerHTML=j.qr_svg;p.textContent=expiryText(j.expires_at_unix_ms);again.textContent='Regenerate QR code';again.onclick=()=>document.querySelector('#showEnrollmentQr').click();host.textContent='';host.append(qr,p,again)}catch(e){host.textContent=e.message||String(e)}};
document.querySelector('#connectMacCollector').onclick=async()=>{const host=document.querySelector('#macEnrollment');host.textContent='Creating Mac connection link…';try{const j=await createEnrollment(),p=document.createElement('p'),input=document.createElement('input');input.type='text';input.readOnly=true;input.value=j.enrollment_url;input.setAttribute('aria-label','One-use Mac connection link');input.style.width='100%';input.onclick=()=>input.select();p.textContent='Run scripts/seca_enroll.py on your Mac and paste this link into its hidden prompt. '+expiryText(j.expires_at_unix_ms);host.textContent='';host.append(p,input)}catch(e){host.textContent=e.message||String(e)}};

</script>"#;

const BROWSER_SESSION_JS: &str = r#"<script>
function sensorsCookie(name){const p=document.cookie.split(';').map(v=>v.trim()).find(v=>v.startsWith(name+'='));return p?decodeURIComponent(p.slice(name.length+1)):''}
async function sensorsRefresh(hop=0){const r=await fetch('/api/v1/auth/browser-refresh',{method:'POST',headers:{'x-csrf-token':sensorsCookie('sensors_csrf')}});if(!r.ok){if(r.status===401||r.status===403)location='/login';throw Error('session refresh '+r.status)}const j=await r.json();if(Number(j.access_expires_at_unix_ms)<=Date.now()&&hop<2)return sensorsRefresh(hop+1);return j}
async function sensorsLogout(){try{await fetch('/api/v1/auth/logout',{method:'POST',headers:{'content-type':'application/json','x-csrf-token':sensorsCookie('sensors_csrf')},body:'{}'})}finally{location='/login'}}
if(document.querySelector('#logout'))document.querySelector('#logout').onclick=sensorsLogout;
setInterval(()=>sensorsRefresh().catch(console.error),10*60*1000);
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')sensorsRefresh().catch(console.error)});
</script>"#;

// "Your devices" section on the account home page. Rows are built with
// createElement/textContent only: user_agent and phone labels are untrusted
// data and must never be interpolated as HTML.
const HOME_DEVICES_JS: &str = r#"<script>
function sensorsDeviceLabel(ua){ua=String(ua||'');const platform=ua.includes('Android')?'Android':(ua.includes('iPhone')||ua.includes('iPad'))?'iPhone/iPad':ua.includes('Mac')?'Mac':ua.includes('Windows')?'Windows':ua.includes('Linux')?'Linux':'Browser';const brand=ua.includes('Firefox')?'Firefox':ua.includes('Edg')?'Edge':ua.includes('Chrome')?'Chrome':ua.includes('Safari')?'Safari':'';return brand?`${platform} · ${brand}`:platform}
async function sensorsRevokeDevice(url){const r=await fetch(url,{method:'POST',credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json','x-csrf-token':sensorsCookie('sensors_csrf')},body:'{}'}),j=await r.json().catch(()=>({}));if(!r.ok)throw Error(j.error||`request failed (${r.status})`);return j}
function sensorsDeviceRow(text,buttonLabel,revoke){const row=document.createElement('p'),label=document.createElement('span'),button=document.createElement('button');label.textContent=`${text} `;button.textContent=buttonLabel;button.onclick=async()=>{button.disabled=true;try{await revoke()}catch(e){button.disabled=false;button.textContent=`failed: ${e.message||e}`}};row.append(label,button);return row}
async function sensorsLoadDevices(){const host=document.querySelector('#devices'),user=host.dataset.user;try{
const r=await fetch(`/api/v1/users/${user}/devices`,{credentials:'same-origin',cache:'no-store'}),j=await r.json().catch(()=>({}));
if(!r.ok)throw Error(j.error||`request failed (${r.status})`);
host.textContent='';
for(const item of (j.installations||[]).filter(entry=>!entry.revoked_unix_ms)){
const label=item.display_label||[item.android_manufacturer,item.android_model].filter(Boolean).join(' ')||'Android phone';
const connected=new Date(Number(item.enrolled_unix_ms||item.created_unix_ms)).toLocaleString();
const upload=item.last_seen_unix_ms?new Date(Number(item.last_seen_unix_ms)).toLocaleString():'never';
host.append(sensorsDeviceRow(`${label} · connected ${connected} · last upload ${upload}`,'Disconnect app',async()=>{await sensorsRevokeDevice(`/api/v1/users/${user}/installations/${item.id}/revoke`);await sensorsLoadDevices()}))}
for(const item of j.browser_sessions||[]){
const label=sensorsDeviceLabel(item.user_agent)+(item.current?' (this browser)':'');
const signedIn=new Date(Number(item.created_unix_ms)).toLocaleString(),lastActive=new Date(Number(item.last_active_unix_ms)).toLocaleString();
host.append(sensorsDeviceRow(`${label} · signed in ${signedIn} · last active ${lastActive}`,'Sign out',async()=>{await sensorsRevokeDevice(`/api/v1/users/${user}/sessions/${item.family_id}/revoke`);if(item.current){location='/login';return}await sensorsLoadDevices()}))}
if(!host.childElementCount)host.textContent='No signed-in devices.';
}catch(e){host.textContent=`device list unavailable (${e.message||e})`}}
sensorsLoadDevices();
</script>"#;

const ADMIN_HTML: &str = r#"<!doctype html>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Training Sensors admin</title>
<style>body{font:15px system-ui;margin:1rem;background:#f5f6f8}header{display:flex;gap:1rem;align-items:center}article{background:white;border:1px solid #ccd3dd;border-radius:8px;padding:.8rem;margin:.8rem 0}h2{margin:.1rem 0;font-size:1rem}.muted{font-weight:400;color:#687384}.details{font:13px ui-monospace,monospace;margin:.35rem 0;color:#334}canvas{width:100%;height:150px;background:#081119}.fresh{color:#08752c}.stale{color:#a02920}.replay{color:#8a4dc2}input,button{font:inherit;padding:.5rem}</style>
<header><h1>Connected users</h1><a href="/">Home</a><a href="/admin/seca">12-lead ECG</a><button id=logout>Sign out</button></header>
<form id=invite><input id=inviteEmail type=email required placeholder="Exact invitation email"><button>Send 7-day invitation</button> <span id=inviteStatus></span></form>
<main><!--USERS--></main>
<section><h2>Invitations</h2><div id=invitations>loading…</div></section>
<script>
function cookie(name){const part=document.cookie.split(';').map(v=>v.trim()).find(v=>v.startsWith(name+'='));return part?decodeURIComponent(part.slice(name.length+1)):''}
async function mutate(url,body){const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json','x-csrf-token':cookie('sensors_csrf')},body:JSON.stringify(body??{})});const j=await r.json();if(!r.ok)throw Error(j.error||r.status);return j}
const nsms=v=>Number(BigInt(v)/1000000n),live=new Map(),OVERVIEW_PENDING_LIMIT=64,overviewPending=[];let overviewSource=null,overviewRefreshPromise=null,overviewRetryTimer=null,overviewRetryDelayMs=1000,overviewPendingOverflow=false,overviewSnapshotVersion=0,overviewRefreshVersion=null;
function age(v){return v==null?Infinity:Math.max(0,Date.now()-nsms(v))}function fmtAge(ms){return Number.isFinite(ms)?`${(ms/1000).toFixed(1)}s`:'—'}
function fmtUnixMs(v){const n=Number(v);return Number.isFinite(n)&&n>0?new Date(n).toLocaleString():'—'}
function draw(card,data){const c=card.querySelector('canvas'),x=c.getContext('2d'),rows=data.ecg||[],st=data.status||{},ar=st.archive||{};x.clearRect(0,0,c.width,c.height);x.strokeStyle=st.replay?'#c084fc':'#47dc77';x.beginPath();if(rows.length){let lo=Math.min(...rows.map(r=>r.microvolts)),hi=Math.max(...rows.map(r=>r.microvolts));if(lo===hi){lo--;hi++}rows.forEach((r,i)=>{const px=i*c.width/Math.max(1,rows.length-1),py=c.height-(r.microvolts-lo)*c.height/(hi-lo);i?x.lineTo(px,py):x.moveTo(px,py)});x.stroke()}const ecgAge=age(st.latest_ecg_unix_ns),s=card.querySelector('.status');s.className=`status ${st.replay?'replay':st.offline||ecgAge>=5000?'stale':'fresh'}`;s.textContent=`${st.connection_state||'offline'} · ECG ${fmtAge(ecgAge)} · ${st.latest_rssi_dbm??'—'} dBm`;card.querySelector('.details').textContent=`connected ${!!st.phone_connected} · collecting ${!!st.phone_collecting} · uploading ${!!st.phone_uploading} · server upload recent ${!!st.phone_upload_recent} (${fmtAge(age(st.latest_upload_received_unix_ns))}) · replay ${!!st.replay} · offline ${!!st.offline} · ECG/ACC/HR ${fmtAge(age(st.latest_ecg_unix_ns))}/${fmtAge(age(st.latest_acc_unix_ns))}/${fmtAge(age(st.latest_hr_unix_ns))} · contact ${st.latest_contact_detected===true?'OK':st.latest_contact_supported===false?'unsupported':'not OK/unknown'} · RSSI ${st.latest_rssi_dbm??'—'} dBm · H10 battery ${st.battery_latest_percent??'—'}% · phone ${st.phone_battery_percent??'—'}% · backlog ${st.phone_backlog_records??'—'} rec / ${st.phone_backlog_bytes??'—'} B · catch-up/future skipped ${st.catch_up_records_skipped_live??0}/${st.future_records_skipped_live??0} · archive ${ar.pending?'pending':ar.state||'unknown'} (success ${fmtUnixMs(ar.last_success_unix_ms)}, failure ${fmtUnixMs(ar.last_failure_unix_ms)}, consecutive ${ar.consecutive_failures??0}, segments/files ${ar.last_archived_segments??0}/${ar.last_output_files??0}, error ${ar.error||'none'}) · disconnect ${st.last_disconnect||'none'} · error ${st.last_collector_error||st.last_upload_error||'none'}`}
function drawAll(){for(const [id,data] of live){const card=document.querySelector(`[data-user="${id}"]`);if(card)draw(card,data)}}
function overviewStreamId(value){return typeof value?.stream_id==='string'&&value.stream_id?value.stream_id:null}
function queueOverviewPayload(payload){overviewPending.push(payload);while(overviewPending.length>OVERVIEW_PENDING_LIMIT){overviewPending.shift();overviewPendingOverflow=true}}
function drainOverviewPending(){const pending=overviewPending.splice(0),overflow=overviewPendingOverflow;overviewPendingOverflow=false;for(const payload of pending)applyOverview(payload,true);if(overflow||overviewPendingOverflow||overviewPending.length)scheduleOverviewRefresh()}
function applyOverviewSnapshot(j){overviewSnapshotVersion++;live.clear();for(const item of j.users){const status=item.status||{};live.set(item.user.id,{ecg:item.ecg||[],status:{...status,archive:j.archive||{}},latestSeq:Number(item.latest_sequence||0),streamId:overviewStreamId(status)})}drainOverviewPending();drawAll()}
async function refreshOverview(){const version=overviewSnapshotVersion,r=await fetch('/api/v1/admin/overview/snapshot',{cache:'no-store'}),j=await r.json();if(!r.ok)throw Error(j.error||r.status);if(version!==overviewSnapshotVersion)return;applyOverviewSnapshot(j)}
function scheduleOverviewRefresh(){if(overviewRetryTimer)return;const delay=overviewRetryDelayMs;overviewRetryTimer=setTimeout(()=>{overviewRetryTimer=null;requestOverviewRefresh()},delay);overviewRetryDelayMs=Math.min(30000,overviewRetryDelayMs*2)}
function requestOverviewRefresh(){if(overviewRefreshPromise||overviewRetryTimer)return;const version=overviewSnapshotVersion;overviewRefreshVersion=version;overviewRefreshPromise=refreshOverview().then(()=>{overviewRetryDelayMs=1000}).catch(error=>{console.error(error);if(version===overviewSnapshotVersion)scheduleOverviewRefresh()}).finally(()=>{overviewRefreshPromise=null;if(version!==overviewSnapshotVersion){drainOverviewPending();drawAll()}})}
function overviewUpdateMatches(update){const current=live.get(update.user_id),batch=update.batch||{},batchStreamId=overviewStreamId(batch)||overviewStreamId(batch.status);return !!(current?.streamId&&batchStreamId===current.streamId)}
function applyOverview(payload,fromPending=false){if(overviewRefreshPromise&&overviewRefreshVersion===overviewSnapshotVersion&&!fromPending){queueOverviewPayload(payload);return}const updates=payload.users||[];if(!fromPending&&updates.some(update=>!overviewUpdateMatches(update))){queueOverviewPayload(payload);requestOverviewRefresh();return}const applicableUpdates=fromPending?updates.filter(overviewUpdateMatches):updates;for(const current of live.values())current.status.archive=payload.archive||current.status.archive||{};for(const update of applicableUpdates){const current=live.get(update.user_id),batch=update.batch||{};if(batch.overflow){if(fromPending)overviewPendingOverflow=true;else requestOverviewRefresh();continue}current.status={...(batch.status||current.status),archive:payload.archive||current.status.archive||{}};for(const item of batch.items||[]){if(Number(item.seq)<=current.latestSeq)continue;if(item.kind==='ecg'){current.ecg.push(item.row);while(current.ecg.length>1300)current.ecg.shift()}current.latestSeq=Number(item.seq)}live.set(update.user_id,current)}drawAll()}
async function startOverview(){overviewSource=new EventSource('/api/v1/admin/overview/events');overviewSource.addEventListener('overview_snapshot',e=>{try{applyOverviewSnapshot(JSON.parse(e.data))}catch(error){console.error(error);requestOverviewRefresh()}});overviewSource.addEventListener('overview',e=>{try{applyOverview(JSON.parse(e.data))}catch(error){console.error(error)}});overviewSource.onerror=()=>{for(const card of document.querySelectorAll('[data-user]'))card.querySelector('.status').textContent='multiplexed feed reconnecting…'}}
startOverview().catch(e=>{for(const card of document.querySelectorAll('[data-user]'))card.querySelector('.status').textContent=`overview unavailable (${e})`});setInterval(drawAll,1000);
for(const button of document.querySelectorAll('.disable-user'))button.onclick=async()=>{const card=button.closest('[data-user]'),disabled=card.dataset.disabled==='true';try{await mutate(`/api/v1/admin/users/${card.dataset.user}/disabled`,{disabled:!disabled});location.reload()}catch(e){button.textContent=`failed: ${e}`}}
async function loadInvitations(){const host=document.querySelector('#invitations');try{const r=await fetch('/api/v1/admin/invitations',{cache:'no-store'}),j=await r.json();if(!r.ok)throw Error(j.error||r.status);host.textContent='';for(const invite of j.invitations){const row=document.createElement('p');const state=invite.consumed_unix_ms?'consumed':invite.revoked_unix_ms?'revoked':invite.expires_unix_ms<Date.now()?'expired':'active';const label=document.createElement('span');label.textContent=`${invite.email} · ${state} · expires ${new Date(invite.expires_unix_ms).toLocaleString()} `;row.append(label);if(state==='active'||state==='expired'){for(const [action,text] of [['revoke','Revoke'],['resend','Resend']]){const b=document.createElement('button');b.textContent=text;b.onclick=async()=>{try{await mutate(`/api/v1/admin/invitations/${invite.id}/${action}`,{});await loadInvitations()}catch(e){b.textContent=`failed: ${e}`}};row.append(b)}}host.append(row)}if(!j.invitations.length)host.textContent='No invitations.'}catch(e){host.textContent=`unavailable (${e})`}}
async function loadInstallations(){try{const r=await fetch('/api/v1/admin/installations',{cache:'no-store'}),j=await r.json();if(!r.ok)throw Error(j.error||r.status);for(const card of document.querySelectorAll('[data-user]')){const host=card.querySelector('.installations'),items=j.installations.filter(i=>i.user_id===card.dataset.user);host.textContent='';if(!items.length){host.textContent='No connected app installation.';continue}for(const item of items){const box=document.createElement('p'),model=[item.android_manufacturer,item.android_model].filter(Boolean).join(' ')||'legacy Android installation',label=document.createElement('span');label.textContent=`${item.display_label||model} · app ${item.app_version_name||'unknown'} · last seen ${fmtUnixMs(item.last_seen_unix_ms)} · installation ${item.revoked_unix_ms?'revoked':'enrolled'} `;box.append(label);if(!item.credentials.length)box.append(document.createTextNode('· reconnect required'));for(const credential of item.credentials){const active=!item.revoked_unix_ms&&!credential.revoked_unix_ms,detail=document.createElement('span');detail.textContent=` · credential ${active?'active':'revoked'} (created ${fmtUnixMs(credential.created_unix_ms)}) `;box.append(detail);if(active){const b=document.createElement('button');b.textContent='Revoke app credential';b.onclick=async()=>{try{await mutate(`/api/v1/admin/device-credentials/${credential.id}/revoke`,{});await loadInstallations()}catch(e){b.textContent=`failed: ${e}`}};box.append(b)}}host.append(box)}}}catch(e){for(const host of document.querySelectorAll('.installations'))host.textContent=`installation list unavailable (${e})`}}
document.querySelector('#invite').onsubmit=async e=>{e.preventDefault();const s=document.querySelector('#inviteStatus');s.textContent='sending…';try{await mutate('/api/v1/admin/invitations',{email:document.querySelector('#inviteEmail').value});s.textContent='sent; expires in 24 hours';await loadInvitations()}catch(e){s.textContent=`failed: ${e}`}}
document.querySelector('#logout').onclick=async()=>{try{await mutate('/api/v1/auth/logout',{});}finally{location='/login'}};
function deviceLabel(ua){ua=String(ua||'');const platform=ua.includes('Android')?'Android':(ua.includes('iPhone')||ua.includes('iPad'))?'iPhone/iPad':ua.includes('Mac')?'Mac':ua.includes('Windows')?'Windows':ua.includes('Linux')?'Linux':'Browser';const brand=ua.includes('Firefox')?'Firefox':ua.includes('Edg')?'Edge':ua.includes('Chrome')?'Chrome':ua.includes('Safari')?'Safari':'';return brand?`${platform} · ${brand}`:platform}
async function loadBrowserSessions(){for(const card of document.querySelectorAll('[data-user]')){const host=card.querySelector('.sessions');try{const r=await fetch(`/api/v1/users/${card.dataset.user}/devices`,{cache:'no-store'}),j=await r.json();if(!r.ok)throw Error(j.error||r.status);host.textContent='';for(const item of j.browser_sessions||[]){const row=document.createElement('p'),label=document.createElement('span');label.textContent=`${deviceLabel(item.user_agent)} · signed in ${fmtUnixMs(item.created_unix_ms)} · last active ${fmtUnixMs(item.last_active_unix_ms)} `;row.append(label);const b=document.createElement('button');b.textContent='Sign out device';b.onclick=async()=>{try{await mutate(`/api/v1/users/${card.dataset.user}/sessions/${item.family_id}/revoke`,{});await loadBrowserSessions()}catch(e){b.textContent=`failed: ${e}`}};row.append(b);host.append(row)}if(!(j.browser_sessions||[]).length)host.textContent='No signed-in browser sessions.'}catch(e){host.textContent=`browser sessions unavailable (${e})`}}}
loadInvitations();
loadInstallations();
loadBrowserSessions();
</script>
<script>
async function refreshSession(hop=0){const j=await mutate('/api/v1/auth/browser-refresh',{});if(Number(j.access_expires_at_unix_ms)<=Date.now()&&hop<2)return refreshSession(hop+1);return j}
setInterval(()=>refreshSession().catch(()=>location='/login'),10*60*1000);
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')refreshSession().catch(()=>location='/login')});
</script>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::db::{NewRegistration, NewSession};
    use crate::cloud::ingest::IngestRecord;
    use crate::wal::{GattNotificationBody, WalBody, WalRecordKind};
    use axum::body::Body;
    use axum::http::Request;
    use futures::StreamExt;
    use std::fs;
    use tower::ServiceExt;

    const TEST_ANDROID_APK_BYTES: &[u8] = b"signed-private-beta-apk-test-fixture";

    fn response_cookie(response: &Response, name: &str) -> Option<String> {
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .find_map(|value| {
                let value = value.to_str().ok()?;
                value
                    .strip_prefix(&format!("{name}="))
                    .and_then(|rest| rest.split(';').next())
                    .map(str::to_string)
            })
    }

    fn decode_qr_svg(svg: &str) -> String {
        fn take_number(bytes: &[u8], cursor: &mut usize) -> usize {
            let start = *cursor;
            while *cursor < bytes.len() && bytes[*cursor].is_ascii_digit() {
                *cursor += 1;
            }
            assert!(*cursor > start, "SVG QR path number is missing");
            std::str::from_utf8(&bytes[start..*cursor])
                .unwrap()
                .parse()
                .unwrap()
        }

        fn expect(bytes: &[u8], cursor: &mut usize, expected: u8) {
            assert_eq!(bytes.get(*cursor), Some(&expected));
            *cursor += 1;
        }

        let svg_body = svg.split_once("<svg ").unwrap().1;
        let width: usize = svg_body
            .split_once("width=\"")
            .unwrap()
            .1
            .split_once('"')
            .unwrap()
            .0
            .parse()
            .unwrap();
        let height: usize = svg_body
            .split_once("height=\"")
            .unwrap()
            .1
            .split_once('"')
            .unwrap()
            .0
            .parse()
            .unwrap();
        assert_eq!(width, height);
        let path = svg
            .split_once(" d=\"")
            .unwrap()
            .1
            .split_once('"')
            .unwrap()
            .0;
        let bytes = path.as_bytes();
        let mut cursor = 0;
        let mut dark = vec![false; width * height];
        while cursor < bytes.len() {
            expect(bytes, &mut cursor, b'M');
            let left = take_number(bytes, &mut cursor);
            expect(bytes, &mut cursor, b' ');
            let top = take_number(bytes, &mut cursor);
            expect(bytes, &mut cursor, b'h');
            let rectangle_width = take_number(bytes, &mut cursor);
            expect(bytes, &mut cursor, b'v');
            let rectangle_height = take_number(bytes, &mut cursor);
            expect(bytes, &mut cursor, b'H');
            assert_eq!(take_number(bytes, &mut cursor), left);
            expect(bytes, &mut cursor, b'V');
            assert_eq!(take_number(bytes, &mut cursor), top);
            assert!(left + rectangle_width <= width);
            assert!(top + rectangle_height <= height);
            for y in top..top + rectangle_height {
                for x in left..left + rectangle_width {
                    dark[y * width + x] = true;
                }
            }
        }

        let scale = 8;
        let mut prepared =
            rqrr::PreparedImage::prepare_from_bitmap(width * scale, height * scale, |x, y| {
                dark[(y / scale) * width + (x / scale)]
            });
        let grids = prepared.detect_grids();
        assert_eq!(
            grids.len(),
            1,
            "rendered enrollment SVG must contain one QR code"
        );
        grids[0].decode().unwrap().1
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("training-sensors-web-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_config(dir: &TestDir) -> CloudConfig {
        let android_apk_path = dir.0.join("approved-private-beta.apk");
        fs::write(&android_apk_path, TEST_ANDROID_APK_BYTES).unwrap();
        CloudConfig {
            data_dir: dir.0.join("data"),
            db_path: dir.0.join("cloud.sqlite3"),
            port: 0,
            public_origin: "https://example.com".to_string(),
            admin_email: "admin@example.com".to_string(),
            android_package: "com.vineyardfinance.sensors".to_string(),
            android_cert_sha256: "AA:BB".to_string(),
            android_apk_path,
            android_apk_sha256: hex::encode(sha2::Sha256::digest(TEST_ANDROID_APK_BYTES)),
            android_apk_version_name: "0.2.2".to_string(),
            android_apk_version_code: 4,
            resend_api_key: "test".to_string(),
            resend_from: "Test <test@example.com>".to_string(),
            resend_reply_to: "reply@example.com".to_string(),
            token_derivation_secret: "test-only-token-derivation-secret-at-least-32-bytes"
                .to_string(),
        }
    }

    #[test]
    fn cloud_config_debug_redacts_credentials_and_operator_identity() {
        let dir = TestDir::new();
        let sentinel = "sentinel-cloud-config-secret";
        let mut config = test_config(&dir);
        config.admin_email = sentinel.to_string();
        config.android_cert_sha256 = sentinel.to_string();
        config.resend_api_key = sentinel.to_string();
        config.resend_from = sentinel.to_string();
        config.resend_reply_to = sentinel.to_string();
        config.token_derivation_secret = sentinel.to_string();

        let debug = format!("{config:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(sentinel));
    }

    #[test]
    fn android_apk_configuration_fails_closed_for_unsafe_or_unapproved_artifacts() {
        let dir = TestDir::new();

        let mut relative = test_config(&dir);
        relative.android_apk_path = PathBuf::from("relative.apk");
        assert!(build_router(&relative).is_err());

        let mut missing = test_config(&dir);
        missing.android_apk_path = dir.0.join("missing.apk");
        assert!(build_router(&missing).is_err());

        let mut directory = test_config(&dir);
        directory.android_apk_path = dir.0.clone();
        assert!(build_router(&directory).is_err());

        let mut mismatched = test_config(&dir);
        mismatched.android_apk_sha256 = "0".repeat(64);
        assert!(build_router(&mismatched).is_err());

        let mut malformed_hash = test_config(&dir);
        malformed_hash.android_apk_sha256 = "A".repeat(64);
        assert!(build_router(&malformed_hash).is_err());

        let mut unsafe_version = test_config(&dir);
        unsafe_version.android_apk_version_name = "0.2.2\"><script>".to_string();
        assert!(build_router(&unsafe_version).is_err());

        let mut no_version = test_config(&dir);
        no_version.android_apk_version_code = 0;
        assert!(build_router(&no_version).is_err());

        let empty_path = dir.0.join("empty.apk");
        fs::write(&empty_path, []).unwrap();
        let mut empty = test_config(&dir);
        empty.android_apk_path = empty_path;
        empty.android_apk_sha256 = hex::encode(sha2::Sha256::digest([]));
        assert!(build_router(&empty).is_err());

        let oversized_path = dir.0.join("oversized.apk");
        let oversized = fs::File::create(&oversized_path).unwrap();
        oversized.set_len(MAX_ANDROID_APK_BYTES + 1).unwrap();
        let mut oversized_config = test_config(&dir);
        oversized_config.android_apk_path = oversized_path;
        assert!(build_router(&oversized_config).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn android_apk_configuration_rejects_a_symlink_final_component() {
        use std::os::unix::fs::symlink;

        let dir = TestDir::new();
        let mut config = test_config(&dir);
        let symlink_path = dir.0.join("artifact-link.apk");
        symlink(&config.android_apk_path, &symlink_path).unwrap();
        config.android_apk_path = symlink_path;
        assert!(build_router(&config).is_err());
    }

    #[tokio::test]
    async fn one_use_token_terminal_errors_have_stable_public_codes() {
        let invitation = run_blocking_public::<(), _>("test invitation terminal", || {
            Err(PublicAuthError::InvitationTerminal)
        })
        .await
        .unwrap_err();
        assert_eq!(invitation.status, StatusCode::BAD_REQUEST);
        assert_eq!(invitation.code, "invalid_invitation");

        let reset = run_blocking_public::<(), _>("test password reset terminal", || {
            Err(PublicAuthError::PasswordResetTerminal)
        })
        .await
        .unwrap_err();
        assert_eq!(reset.status, StatusCode::BAD_REQUEST);
        assert_eq!(reset.code, "password_reset_terminal");

        let internal = run_blocking_public::<(), _>("test true internal", || {
            Err(PublicAuthError::Internal(anyhow!(
                "injected database I/O failure"
            )))
        })
        .await
        .unwrap_err();
        assert_eq!(internal.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(internal.code, "internal_error");
    }

    fn test_ingest_response() -> super::super::ingest::IngestBatchResponse {
        super::super::ingest::IngestBatchResponse {
            schema_version: 1,
            installation_id: Uuid::nil(),
            batch_id: Uuid::nil(),
            accepted_records: 0,
            durably_committed_through_seq: -1,
            missing_ranges: Vec::new(),
            server_receive_unix_ms: 0,
            batch_checksum_sha256: "test".to_string(),
            encoded_bytes: 0,
            record_count: 0,
            instrumentation_attempt_seq: 0,
        }
    }

    async fn response_json(response: Response) -> Value {
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn create_password_user(
        state: &Arc<CloudState>,
        email: &str,
        password: &str,
        now_ms: i64,
    ) -> (TokenResponse, SessionUser) {
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let invitation = random_token();
        state
            .db
            .issue_invitation(email, &invitation, admin.id, now_ms, i64::MAX)
            .unwrap();
        let tokens = state
            .auth
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: password.to_string(),
                },
                "test-setup-client",
                None,
                now_ms + 1,
            )
            .unwrap();
        let current = state
            .db
            .authenticate_access(&tokens.access_token, now_ms + 2)
            .unwrap()
            .unwrap();
        (tokens, current)
    }

    fn enrollment_exchange_json(
        enrollment_token: &str,
        installation_id: Uuid,
        exchange_id: Uuid,
    ) -> Value {
        json!({
            "enrollment_token": enrollment_token,
            "installation_id": installation_id,
            "exchange_id": exchange_id,
            "display_label": "Galaxy A54",
            "android_manufacturer": "Samsung",
            "android_model": "SM-A546B",
            "android_sdk": 34,
            "app_package": "com.vineyardfinance.sensors",
            "app_version_name": "1.0",
            "app_version_code": 1
        })
    }

    fn valid_ingest_request(installation_id: Uuid, now_ms: i64) -> IngestBatchRequest {
        let mut payload = vec![0];
        payload.extend_from_slice(&1_000_000_000_u64.to_le_bytes());
        payload.extend_from_slice(&[0, 1, 0, 0]);
        IngestBatchRequest {
            schema_version: 1,
            installation_id,
            batch_id: Uuid::new_v4(),
            source: "chest_h10".to_string(),
            capture_session_id: Uuid::new_v4(),
            writer_epoch_id: Uuid::new_v4(),
            first_source_seq: 0,
            last_source_seq: 0,
            records: vec![IngestRecord {
                record_id: Uuid::new_v4(),
                source_seq: 0,
                connection_id: Some(Uuid::new_v4()),
                collector_unix_ns: now_ms.saturating_mul(1_000_000),
                collector_monotonic_ns: 1,
                physical_device_id: Some("h10".to_string()),
                ble_peripheral_id: Some("polar".to_string()),
                kind: WalRecordKind::GattNotification,
                body: serde_json::to_value(WalBody::GattNotification(
                    GattNotificationBody::from_payload(
                        "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
                        "pmd_data",
                        &payload,
                    )
                    .unwrap(),
                ))
                .unwrap(),
            }],
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_ingest_and_auth_waiters_do_not_starve_async_runtime() {
        let database_gate = Arc::new(Mutex::new(()));
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (ingest_started_tx, ingest_started_rx) = tokio::sync::oneshot::channel();
        let ingest_gate = database_gate.clone();
        let ingest_release = release.clone();
        let ingest = tokio::spawn(async move {
            run_blocking_ingest(move || {
                let _database = ingest_gate.lock().expect("lock test database gate");
                let _ = ingest_started_tx.send(());
                let (released, wake) = &*ingest_release;
                let mut released = released.lock().expect("lock release flag");
                while !*released {
                    released = wake.wait(released).expect("wait for test release");
                }
                Ok(test_ingest_response())
            })
            .await
        });
        ingest_started_rx.await.expect("blocking ingest started");

        // A watchdog bounds the regression if blocking work is accidentally
        // moved back onto both Tokio workers: it releases the fake database
        // after two seconds, allowing the test to fail instead of deadlocking.
        let watchdog_release = release.clone();
        let (watchdog_cancel_tx, watchdog_cancel_rx) = std::sync::mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(2))
                .is_err()
            {
                let (released, wake) = &*watchdog_release;
                *released.lock().expect("lock watchdog release flag") = true;
                wake.notify_all();
            }
        });

        let responsiveness_started = std::time::Instant::now();
        let (second_ingest_started_tx, second_ingest_started_rx) = tokio::sync::oneshot::channel();
        let second_ingest_gate = database_gate.clone();
        let second_ingest = tokio::spawn(async move {
            run_blocking_ingest(move || {
                let _ = second_ingest_started_tx.send(());
                let _database = second_ingest_gate.lock().expect("lock test database gate");
                Ok(test_ingest_response())
            })
            .await
        });
        let (first_auth_started_tx, first_auth_started_rx) = tokio::sync::oneshot::channel();
        let first_auth_gate = database_gate.clone();
        let first_auth = tokio::spawn(async move {
            run_blocking_internal("test access authentication failed", move || {
                let _ = first_auth_started_tx.send(());
                let _database = first_auth_gate.lock().expect("lock test database gate");
                Ok(())
            })
            .await
        });
        let (second_auth_started_tx, second_auth_started_rx) = tokio::sync::oneshot::channel();
        let second_auth_gate = database_gate.clone();
        let second_auth = tokio::spawn(async move {
            run_blocking_internal("test access authentication failed", move || {
                let _ = second_auth_started_tx.send(());
                let _database = second_auth_gate.lock().expect("lock test database gate");
                Ok(())
            })
            .await
        });

        second_ingest_started_rx
            .await
            .expect("second blocking ingest waiter started");
        first_auth_started_rx
            .await
            .expect("first auth waiter started");
        second_auth_started_rx
            .await
            .expect("second auth waiter started");
        let timer = tokio::time::timeout(
            Duration::from_millis(500),
            tokio::time::sleep(Duration::from_millis(20)),
        )
        .await;
        assert!(
            timer.is_ok(),
            "health-like timer was starved by blocking ingest/auth waiters"
        );
        assert!(
            responsiveness_started.elapsed() < Duration::from_millis(500),
            "Tokio workers resumed only after the two-second watchdog"
        );

        let (released, wake) = &*release;
        *released.lock().expect("lock release flag") = true;
        wake.notify_all();
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("join test watchdog");
        ingest
            .await
            .expect("join ingest task")
            .expect("ingest result");
        second_ingest
            .await
            .expect("join second ingest task")
            .expect("second ingest result");
        first_auth
            .await
            .expect("join first auth task")
            .expect("first auth result");
        second_auth
            .await
            .expect("join second auth task")
            .expect("second auth result");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn authenticated_status_does_not_wait_for_durable_writer_and_observes_revoke_commit() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let now = unix_ms();
        let session_id = state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "contention-access",
                refresh_token: "contention-refresh",
                csrf_token: "contention-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: now + 60_000,
                refresh_expires_ms: now + 120_000,
            })
            .unwrap();

        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let revocation_db = state.db.clone();
        let revocation = tokio::task::spawn_blocking(move || {
            revocation_db.hold_uncommitted_session_revocation(
                session_id,
                now + 1,
                started_tx,
                release_rx,
            )
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)))
            .await
            .expect("join revocation-start waiter")
            .expect("writer transaction started");

        let status_path = format!("/api/v1/users/{}/dashboard/status", admin.id);
        let first = app.clone().oneshot(
            Request::get(&status_path)
                .header(header::COOKIE, "sensors_access=contention-access")
                .body(Body::empty())
                .unwrap(),
        );
        let second = app.clone().oneshot(
            Request::get(&status_path)
                .header(header::COOKIE, "sensors_access=contention-access")
                .body(Body::empty())
                .unwrap(),
        );
        let prompt_reads = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(first, second)
        })
        .await;

        // Always release the real writer transaction before asserting so a
        // regression fails promptly instead of leaving blocking tasks behind.
        release_tx.send(()).expect("release writer transaction");
        revocation
            .await
            .expect("join writer transaction")
            .expect("commit writer transaction");
        let (first, second) =
            prompt_reads.expect("authenticated status waited for the durable writer mutex");
        assert_eq!(first.unwrap().status(), StatusCode::OK);
        assert_eq!(second.unwrap().status(), StatusCode::OK);

        let revoked = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::get(&status_path)
                    .header(header::COOKIE, "sensors_access=contention-access")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .expect("authentication did not observe the committed revocation")
        .unwrap();
        assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoint_mutex_does_not_block_authenticated_read_or_writer_http_work() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let now = unix_ms();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "checkpoint-isolation-access",
                refresh_token: "checkpoint-isolation-refresh",
                csrf_token: "checkpoint-isolation-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: now + 60_000,
                refresh_expires_ms: now + 120_000,
            })
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (watchdog_cancel_tx, watchdog_cancel_rx) = std::sync::mpsc::sync_channel(1);
        let watchdog_release = release_tx.clone();
        let watchdog = std::thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(2))
                .is_err()
            {
                let _ = watchdog_release.send(());
            }
        });
        let checkpoint_db = state.db.clone();
        let checkpoint = tokio::task::spawn_blocking(move || {
            checkpoint_db.hold_checkpoint_connection(started_tx, release_rx)
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("join checkpoint-start waiter")
            .expect("checkpoint connection held");

        let status_path = format!("/api/v1/users/{}/dashboard/status", admin.id);
        let status = tokio::time::timeout(
            Duration::from_secs(1),
            app.clone().oneshot(
                Request::get(status_path)
                    .header(header::COOKIE, "sensors_access=checkpoint-isolation-access")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await;
        let logout = tokio::time::timeout(
            Duration::from_secs(1),
            app.oneshot(
                Request::post("/api/v1/auth/logout")
                    .header(
                        header::COOKIE,
                        "sensors_access=checkpoint-isolation-access; sensors_csrf=checkpoint-isolation-csrf",
                    )
                    .header(header::ORIGIN, &config.public_origin)
                    .header("x-csrf-token", "checkpoint-isolation-csrf")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await;

        // Release the held connection and cancel its watchdog before any
        // timing/status assertion can panic.
        let _ = release_tx.send(());
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("join checkpoint watchdog");
        checkpoint
            .await
            .expect("join checkpoint holder")
            .expect("release checkpoint holder");
        assert_eq!(
            status
                .expect("authenticated status waited for checkpoint mutex")
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            logout
                .expect("writer logout waited for checkpoint mutex")
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    #[test]
    fn checkpoint_health_bounds_stuck_attempts_and_logical_backlog() {
        let health = Arc::new(Mutex::new(CheckpointHealth::default()));
        assert!(begin_checkpoint_attempt(&health, 1_000).unwrap());
        assert!(!begin_checkpoint_attempt(&health, 2_000).unwrap());
        let mut stuck = health.lock().unwrap().clone();
        evaluate_checkpoint_health(&mut stuck, 31_001);
        assert_eq!(stuck.state, "error");
        assert_eq!(
            stuck.error,
            Some("checkpoint attempt stuck; inspect service logs")
        );

        note_checkpoint_result(
            &health,
            32_000,
            12,
            112,
            PassiveCheckpointResult {
                busy: true,
                log_frames: 1_001,
                checkpointed_frames: 0,
                wal_bytes: 10_000_000,
                checkpoint_operation_ns: 90_000,
                wal_stat_ns: 10_000,
            },
        );
        let excessive = health.lock().unwrap().clone();
        assert_eq!(excessive.state, "error");
        assert_eq!(excessive.outstanding_frames, 1_001);

        note_checkpoint_result(
            &health,
            33_000,
            13,
            113,
            PassiveCheckpointResult {
                busy: false,
                log_frames: 10,
                checkpointed_frames: 10,
                wal_bytes: 10_000_000,
                checkpoint_operation_ns: 90_000,
                wal_stat_ns: 10_000,
            },
        );
        let complete = health.lock().unwrap().clone();
        assert_eq!(complete.state, "ok");
        assert!(!complete.pending);
        assert_eq!(complete.last_success_unix_ms, Some(33_000));
        assert_eq!(complete.last_full_completion_unix_ms, Some(33_000));

        note_checkpoint_result(
            &health,
            34_000,
            14,
            114,
            PassiveCheckpointResult {
                busy: true,
                log_frames: 1_000,
                checkpointed_frames: 0,
                wal_bytes: u64::MAX,
                checkpoint_operation_ns: 90_000,
                wal_stat_ns: 10_000,
            },
        );
        let mut at_limit = health.lock().unwrap().clone();
        evaluate_checkpoint_health(&mut at_limit, 94_000);
        assert_eq!(at_limit.state, "ok");
        evaluate_checkpoint_health(&mut at_limit, 94_001);
        assert_eq!(at_limit.state, "error");
        assert_eq!(
            at_limit.error,
            Some("checkpoint backlog stale; inspect service logs")
        );

        note_checkpoint_failure(&health, 95_000, Some(5), 15);
        assert_eq!(health.lock().unwrap().state, "error");
        note_checkpoint_result(
            &health,
            96_000,
            16,
            116,
            PassiveCheckpointResult {
                busy: false,
                log_frames: 0,
                checkpointed_frames: 0,
                wal_bytes: u64::MAX,
                checkpoint_operation_ns: 90_000,
                wal_stat_ns: 10_000,
            },
        );
        let recovered = health.lock().unwrap().clone();
        assert_eq!(recovered.state, "ok");
        assert_eq!(recovered.consecutive_failures, 0);
        assert_eq!(recovered.wal_bytes, u64::MAX);
    }

    #[tokio::test]
    async fn checkpoint_failure_is_visible_in_health() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        note_checkpoint_failure(&state.checkpoint_health, 123_456, None, 17);

        let response = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], false);
        assert_eq!(json["checkpoint"]["state"], "error");
        assert_eq!(json["checkpoint"]["last_failure_unix_ms"], 123_456);
        assert_eq!(
            json["checkpoint"]["error"],
            "checkpoint failure; inspect service logs"
        );
    }

    #[tokio::test]
    async fn health_fails_when_authoritative_database_schema_version_is_missing() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, _) = build_router_and_state(&config).unwrap();
        rusqlite::Connection::open(&config.db_path)
            .unwrap()
            .execute(
                "DELETE FROM schema_migrations WHERE version=?1",
                [CURRENT_SCHEMA_VERSION],
            )
            .unwrap();

        let response = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let health = response_json(response).await;
        assert_eq!(health["schema_version"], 1);
        assert_eq!(health["database_schema_version"], 0);
        assert_eq!(health["database_schema_ok"], false);
        assert_eq!(health["database_ok"], true);
        assert_eq!(health["ok"], false);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn periodic_checkpoint_coalesces_slow_attempt_and_runs_again_for_later_writes() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (_, state) = build_router_and_state(&config).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (watchdog_cancel_tx, watchdog_cancel_rx) = std::sync::mpsc::sync_channel(1);
        let watchdog_release = release_tx.clone();
        let watchdog = std::thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(2))
                .is_err()
            {
                let _ = watchdog_release.send(());
            }
        });
        let held_db = state.db.clone();
        let holder = tokio::task::spawn_blocking(move || {
            held_db.hold_checkpoint_connection(started_tx, release_rx)
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(1)))
            .await
            .expect("join checkpoint-start waiter")
            .expect("checkpoint connection held");
        let worker = start_checkpoint_worker_with_interval(
            state.db.clone(),
            state.checkpoint_health.clone(),
            state.ingest.checkpoint_activity(),
            Duration::from_millis(10),
        );

        let health = state.checkpoint_health.clone();
        let observed_in_flight = tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                if health.lock().unwrap().in_flight_started_unix_ms.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let writer_db = state.db.clone();
        let write_during_attempt = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::task::spawn_blocking(move || writer_db.ensure_admin("tail-a@example.com", 10)),
        )
        .await;

        // Always release the connection and cancel its watchdog before
        // checking any timer or writer result.
        let _ = release_tx.send(());
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("join checkpoint watchdog");
        holder
            .await
            .expect("join checkpoint holder")
            .expect("release checkpoint holder");
        observed_in_flight.expect("periodic checkpoint never started");
        write_during_attempt
            .expect("writer waited for checkpoint connection")
            .expect("join writer during checkpoint")
            .expect("write during checkpoint");

        let first_success = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(value) = state.checkpoint_health.lock().unwrap().last_success_unix_ms {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("first coalesced checkpoint did not finish");
        let later_db = state.db.clone();
        tokio::task::spawn_blocking(move || later_db.ensure_admin("tail-b@example.com", 20))
            .await
            .expect("join later writer")
            .expect("later write");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let health = state.checkpoint_health.lock().unwrap().clone();
                if health
                    .last_success_unix_ms
                    .is_some_and(|value| value > first_success)
                    && health.last_full_completion_unix_ms == health.last_success_unix_ms
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("next periodic checkpoint did not handle later write");
        worker.abort();
    }

    #[tokio::test]
    async fn blocking_ingest_join_failure_is_opaque_internal_error() {
        let error = run_blocking_ingest(|| panic!("private blocking-task panic detail"))
            .await
            .expect_err("blocking-task panic must become an API error");
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.code, "internal_error");
        assert_eq!(error.message, "internal server error");
        let request_id = error.request_id;
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.headers().get("x-request-id").unwrap(),
            request_id.to_string().as_str()
        );
    }

    #[test]
    fn device_and_cookie_parsing_are_strict_and_separate() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer abc"),
        );
        assert_eq!(device_credential(&headers), None);
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Device device-secret"),
        );
        assert_eq!(device_credential(&headers), Some("device-secret"));
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("x=1; sensors_access=xyz"),
        );
        assert_eq!(cookie_value(&headers, "sensors_access"), Some("xyz"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("bearer abc"),
        );
        assert_eq!(device_credential(&headers), None);
    }

    #[test]
    fn handler_timing_log_attributes_slow_failure_and_correlated_normal_success() {
        let slow_failure = format_ingest_handler_timing(
            1,
            "failure",
            Some(HttpPhaseTiming {
                elapsed_ns: 250_001_000,
                checkpoint_overlap: true,
            }),
            None,
            None,
            HttpPhaseTiming {
                elapsed_ns: 250_002_000,
                checkpoint_overlap: true,
            },
        )
        .expect("slow pre-ingest failure must emit a timing line");
        for required in [
            "attempt_seq=1 outcome=failure",
            "http_auth_present=true http_auth_us=250001 http_auth_checkpoint_overlap=true",
            "http_decode_checksum_present=false http_decode_checksum_us=0 http_decode_checksum_checkpoint_overlap=false",
            "blocking_queue_present=false blocking_queue_us=0 blocking_queue_checkpoint_overlap=false",
            "handler_total_us=250002 handler_total_checkpoint_overlap=true",
        ] {
            assert!(
                slow_failure.contains(required),
                "missing handler timing fields: {required}"
            );
        }
        for secret in [
            "private-token",
            "private@example.com",
            "550e8400-e29b-41d4-a716-446655440000",
            "private-checksum",
        ] {
            assert!(!slow_failure.contains(secret));
        }

        let sampled_success = format_ingest_handler_timing(
            128,
            "success",
            Some(HttpPhaseTiming {
                elapsed_ns: 1_000,
                checkpoint_overlap: false,
            }),
            Some(HttpPhaseTiming {
                elapsed_ns: 2_000,
                checkpoint_overlap: true,
            }),
            Some(HttpPhaseTiming {
                elapsed_ns: 3_000,
                checkpoint_overlap: false,
            }),
            HttpPhaseTiming {
                elapsed_ns: 7_000,
                checkpoint_overlap: true,
            },
        )
        .expect("shared ordinal 128 must sample the successful handler");
        assert!(sampled_success.contains("attempt_seq=128 outcome=success"));
        assert!(
            sampled_success.contains(
                "http_auth_present=true http_auth_us=1 http_auth_checkpoint_overlap=false"
            )
        );
        assert!(sampled_success.contains(
            "http_decode_checksum_present=true http_decode_checksum_us=2 http_decode_checksum_checkpoint_overlap=true"
        ));
        assert!(sampled_success.contains(
            "blocking_queue_present=true blocking_queue_us=3 blocking_queue_checkpoint_overlap=false"
        ));
        assert!(
            format_ingest_handler_timing(
                127,
                "success",
                None,
                None,
                None,
                HttpPhaseTiming {
                    elapsed_ns: 7_000,
                    checkpoint_overlap: false,
                },
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn mac_enrollment_http_preserves_android_identity_and_owner_boundaries() {
        let dir = TestDir::new();
        let (app, state) = build_router_and_state(&test_config(&dir)).unwrap();
        let now = unix_ms();
        let (tokens, wearer) =
            create_password_user(&state, "mac-owner@example.com", "mac-owner-password", now);
        let (_, other) =
            create_password_user(&state, "mac-other@example.com", "mac-other-password", now);
        let enrollment = state
            .auth
            .create_enrollment(&wearer.user, wearer.session_id, now)
            .unwrap();
        let installation = Uuid::new_v4();
        let mut body = json!({"enrollment_token":enrollment.enrollment_token,"installation_id":installation,
            "exchange_id":Uuid::new_v4(),"display_label":"My Mac","client_version":"1.0"});
        let post = |body: Value| {
            Request::post("/api/v1/device-enrollments/exchange-macos")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = app.clone().oneshot(post(body.clone())).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let first = response_json(response).await;
        let retry = response_json(app.clone().oneshot(post(body.clone())).await.unwrap()).await;
        assert_eq!(first, retry);
        let home = app
            .clone()
            .oneshot(
                Request::get("/")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", tokens.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let home = axum::body::to_bytes(home.into_body(), 1_000_000)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&home).contains("connectMacCollector"));
        let other_enrollment = state
            .auth
            .create_enrollment(&other.user, other.session_id, now)
            .unwrap();
        body["enrollment_token"] = json!(other_enrollment.enrollment_token);
        body["exchange_id"] = json!(Uuid::new_v4());
        assert_eq!(
            app.clone()
                .oneshot(post(body.clone()))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        let phone = Uuid::new_v4();
        state
            .db
            .insert_test_device_credential(
                wearer.user.id,
                phone,
                Uuid::new_v4(),
                "unchanged-phone",
                now,
            )
            .unwrap();
        let fresh = state
            .auth
            .create_enrollment(&wearer.user, wearer.session_id, now)
            .unwrap();
        body["enrollment_token"] = json!(fresh.enrollment_token);
        body["installation_id"] = json!(phone);
        assert_eq!(
            app.oneshot(post(body)).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert!(
            state
                .auth
                .authenticate_device("unchanged-phone", now)
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn seca_http_auth_durable_ack_conflict_and_cross_user_separation() {
        use base64::Engine;
        let dir = TestDir::new();
        let (app, state) = build_router_and_state(&test_config(&dir)).unwrap();
        let now = unix_ms();
        let (tokens, wearer) =
            create_password_user(&state, "seca@example.com", "seca-password", now);
        let (other_tokens, _) =
            create_password_user(&state, "other-seca@example.com", "other-password", now);
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "seca-admin",
                refresh_token: "seca-admin-refresh",
                csrf_token: "seca-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let installation = Uuid::new_v4();
        let session = Uuid::new_v4();
        state
            .db
            .insert_test_device_credential(
                wearer.user.id,
                installation,
                Uuid::new_v4(),
                "seca-device",
                now,
            )
            .unwrap();
        let packet = crate::seca::tests::frame(&crate::seca::tests::group(8), 1);
        let body = json!({"schema_version":1,"installation_id":installation,"session_id":session,
            "batch_index":0,"device_id":"ct331","sample_rate_hz":4000,"gain":6,"transport":"bluetooth","contact_detection":false,
            "records":[{"unix_ns":now*1_000_000,"monotonic_ns":1,"direction":"rx","acquisition":true,
            "payload_b64":base64::engine::general_purpose::STANDARD.encode(packet)}]});
        // Pretty JSON and omitted optional end_reason deliberately differ from archive canonical bytes.
        let encoded = serde_json::to_string_pretty(&body).unwrap();
        let post_batch = |auth: &str, bytes: String| {
            Request::post("/api/v1/seca/batches")
                .header(header::AUTHORIZATION, auth)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(bytes))
                .unwrap()
        };
        let response = app
            .clone()
            .oneshot(post_batch("Bearer seca-device", encoded.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = app
            .clone()
            .oneshot(post_batch("Device seca-device", encoded.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response_json(response).await["sha256"],
            hex::encode(sha2::Sha256::digest(encoded.as_bytes()))
        );
        let response = app
            .clone()
            .oneshot(post_batch("Device seca-device", encoded.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut conflicting = body.clone();
        conflicting["records"][0]["monotonic_ns"] = json!(2);
        let response = app
            .clone()
            .oneshot(post_batch("Device seca-device", conflicting.to_string()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let mut wrong = body.clone();
        wrong["installation_id"] = json!(Uuid::new_v4());
        assert_eq!(
            app.clone()
                .oneshot(post_batch("Device seca-device", wrong.to_string()))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let mut corrupt = body.clone();
        corrupt["batch_index"] = json!(1);
        corrupt["records"][0]["payload_b64"] = json!("AQ==");
        assert_eq!(
            app.clone()
                .oneshot(post_batch("Device seca-device", corrupt.to_string()))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let snapshot_path = format!("/api/v1/users/{}/seca/{session}/snapshot", wearer.user.id);
        let get = |path: &str, token: &str| {
            Request::get(path)
                .header(header::COOKIE, format!("sensors_access={token}"))
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(get(&snapshot_path, &other_tokens.access_token))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            app.clone()
                .oneshot(get("/api/v1/admin/seca/sessions", &tokens.access_token))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let response = app
            .clone()
            .oneshot(get(&snapshot_path, &tokens.access_token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_json(response).await["summary"]["decode_errors"], 1);
        let response = app
            .clone()
            .oneshot(get("/api/v1/admin/seca/sessions", "seca-admin"))
            .await
            .unwrap();
        assert_eq!(
            response_json(response).await["sessions"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let raw_path = format!("/api/v1/users/{}/seca/{session}/chunks/0", wearer.user.id);
        assert_eq!(
            app.clone()
                .oneshot(get(&raw_path, &other_tokens.access_token))
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        let raw = app
            .clone()
            .oneshot(get(&raw_path, &tokens.access_token))
            .await
            .unwrap();
        assert_eq!(raw.status(), StatusCode::OK);
        assert_eq!(raw.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            app.clone()
                .oneshot(get("/admin/seca", "seca-admin"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let mut permits = Vec::new();
        for _ in 0..MAX_ADMITTED_INGEST {
            permits.push(state.ingest_admission.clone().try_acquire_owned().unwrap());
        }
        assert_eq!(
            app.clone()
                .oneshot(post_batch("Device seca-device", encoded))
                .await
                .unwrap()
                .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        drop(permits);
        assert_eq!(
            app.oneshot(post_batch(
                "Device seca-device",
                " ".repeat(super::super::seca::MAX_BYTES + 1)
            ))
            .await
            .unwrap()
            .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn ingest_backpressure_refuses_loudly_and_startup_builds_no_runtimes() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();

        // Lazy policy: startup builds zero per-user runtimes regardless of
        // registered users (the 9 Aug OOM was startup history load).
        create_password_user(
            &state,
            "lazy-runtime@example.com",
            "lazy-password-1",
            unix_ms(),
        );
        assert!(state.dashboards.lock().unwrap().is_empty());

        // Exhaust admission: the next batch is refused with an honest 429
        // and a Retry-After, before any auth or decode work.
        let mut permits = Vec::new();
        for _ in 0..MAX_ADMITTED_INGEST {
            permits.push(state.ingest_admission.clone().try_acquire_owned().unwrap());
        }
        let denied = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(denied.headers().get(header::RETRY_AFTER).unwrap(), "5");
        assert_eq!(
            state
                .ingest_rejections
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Releasing admission lets the same request advance past the gate
        // (to the device-credential check, 401 here).
        drop(permits);
        let admitted = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(admitted.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ingest_response_exposes_attempt_header_and_health_has_bounded_timings() {
        let dir = TestDir::new();
        let (app, state) = build_router_and_state(&test_config(&dir)).unwrap();
        let user = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let installation_id = Uuid::new_v4();
        let now = unix_ms();
        state
            .db
            .create_session(NewSession {
                user_id: user.id,
                installation_id: Some(installation_id),
                access_token: "timing-access",
                refresh_token: "timing-refresh",
                csrf_token: "timing-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: now + 60_000,
                refresh_expires_ms: now + 120_000,
            })
            .unwrap();
        state
            .db
            .insert_test_device_credential(
                user.id,
                installation_id,
                Uuid::new_v4(),
                "timing-device-credential",
                now,
            )
            .unwrap();
        let mut pmd_payload = vec![0];
        pmd_payload.extend_from_slice(&1_000_000_000_u64.to_le_bytes());
        pmd_payload.extend_from_slice(&[0, 1, 0, 0]);
        let request = IngestBatchRequest {
            schema_version: 1,
            installation_id,
            batch_id: Uuid::new_v4(),
            source: "chest_h10".to_string(),
            capture_session_id: Uuid::new_v4(),
            writer_epoch_id: Uuid::new_v4(),
            first_source_seq: 0,
            last_source_seq: 0,
            records: vec![IngestRecord {
                record_id: Uuid::new_v4(),
                source_seq: 0,
                connection_id: Some(Uuid::new_v4()),
                collector_unix_ns: now.saturating_mul(1_000_000),
                collector_monotonic_ns: 1,
                physical_device_id: Some("h10".to_string()),
                ble_peripheral_id: Some("polar".to_string()),
                kind: WalRecordKind::GattNotification,
                body: serde_json::to_value(WalBody::GattNotification(
                    GattNotificationBody::from_payload(
                        "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
                        "pmd_data",
                        &pmd_payload,
                    )
                    .unwrap(),
                ))
                .unwrap(),
            }],
        };
        let pretraffic = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let pretraffic_body = axum::body::to_bytes(pretraffic.into_body(), 64 * 1024)
            .await
            .unwrap();
        let pretraffic_json: Value = serde_json::from_slice(&pretraffic_body).unwrap();
        assert_eq!(
            pretraffic_json["ingest_timing"]["ingest_total"]["bucket_upper_us"][0],
            1_000
        );
        assert_eq!(
            pretraffic_json["ingest_timing"]["ingest_total"]["buckets"],
            json!([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        );
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(header::AUTHORIZATION, "Device timing-device-credential")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-sensors-ingest-attempt-seq")
                .unwrap(),
            "1"
        );
        let response_body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        assert!(!String::from_utf8_lossy(&response_body).contains("instrumentation_attempt_seq"));

        let health = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let health_body = axum::body::to_bytes(health.into_body(), 64 * 1024)
            .await
            .unwrap();
        let health_json: Value = serde_json::from_slice(&health_body).unwrap();
        assert_eq!(health_json["ingest_timing"]["attempts"], 1);
        assert_eq!(health_json["ingest_timing"]["successes"], 1);
        assert_eq!(health_json["ingest_timing"]["http_auth"]["count"], 1);
        assert_eq!(
            health_json["ingest_timing"]["http_decode_checksum"]["count"],
            1
        );
        assert_eq!(health_json["ingest_timing"]["blocking_queue"]["count"], 1);
        assert_eq!(health_json["ingest_timing"]["handler_total"]["count"], 1);
        assert_eq!(
            health_json["ingest_timing"]["ingest_total"]["buckets"]
                .as_array()
                .unwrap()
                .len(),
            12
        );
        let health_text = String::from_utf8_lossy(&health_body);
        assert!(!health_text.contains("timing-access"));
        assert!(!health_text.contains(&installation_id.to_string()));
        assert!(!health_text.contains(&request.batch_id.to_string()));
    }

    #[tokio::test]
    async fn preingest_failures_have_unique_internal_ordinals_and_no_header_or_secret_leak() {
        let dir = TestDir::new();
        let (app, state) = build_router_and_state(&test_config(&dir)).unwrap();
        let user = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let installation_id = Uuid::new_v4();
        let now = unix_ms();
        state
            .db
            .create_session(NewSession {
                user_id: user.id,
                installation_id: Some(installation_id),
                access_token: "preingest-secret-access",
                refresh_token: "preingest-secret-refresh",
                csrf_token: "preingest-secret-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: now + 60_000,
                refresh_expires_ms: now + 120_000,
            })
            .unwrap();
        state
            .db
            .insert_test_device_credential(
                user.id,
                installation_id,
                Uuid::new_v4(),
                "preingest-secret-device-credential",
                now,
            )
            .unwrap();
        let cases = vec![
            (None, Body::from("{}"), StatusCode::UNAUTHORIZED),
            (
                Some("Device preingest-secret-device-credential"),
                Body::from("{"),
                StatusCode::BAD_REQUEST,
            ),
            (
                Some("Device preingest-secret-device-credential"),
                Body::from(vec![b'x'; super::super::ingest::MAX_INGEST_JSON_BYTES + 1]),
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ];
        for (authorization, body, expected_status) in cases {
            let mut request = Request::post("/api/v1/ingest/batches")
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(authorization) = authorization {
                request = request.header(header::AUTHORIZATION, authorization);
            }
            let response = app
                .clone()
                .oneshot(request.body(body).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            assert!(
                response
                    .headers()
                    .get("x-sensors-ingest-attempt-seq")
                    .is_none()
            );
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let text = String::from_utf8_lossy(&body);
            for secret in [
                "preingest-secret-access",
                "preingest-secret-refresh",
                "preingest-secret-csrf",
                "preingest-secret-device-credential",
                "admin@example.com",
                &installation_id.to_string(),
            ] {
                assert!(!text.contains(secret));
            }
        }
        let extractor_rejection = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        "Device preingest-secret-device-credential",
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![
                        b'x';
                        super::super::ingest::MAX_INGEST_JSON_BYTES
                            + 2
                    ]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(extractor_rejection.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            extractor_rejection
                .headers()
                .get("x-sensors-ingest-attempt-seq")
                .is_none()
        );
        let non_ingest_global_rejection = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/users/{}/dashboard/marker", Uuid::nil()))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(vec![
                        b'x';
                        super::super::ingest::MAX_INGEST_JSON_BYTES
                            + 1
                    ]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            non_ingest_global_rejection.status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(state.ingest.reserve_ingest_attempt_sequence(), 4);
        let health = state.ingest.timing_snapshot();
        assert_eq!(health.attempts, 0, "core ingest never began");
        assert_eq!(health.handler_total.count, 3);
        assert_eq!(health.http_auth.count, 3);
        assert_eq!(health.http_decode_checksum.count, 2);
        assert_eq!(health.blocking_queue.count, 0);
    }

    #[test]
    fn dashboard_customization_keeps_each_user_on_scoped_endpoints() {
        let id = Uuid::new_v4();
        let base = format!("/api/v1/users/{id}/dashboard");
        let html = DASHBOARD_HTML
            .replace("/api/status", &format!("{base}/status"))
            .replace("/api/latest", &format!("{base}/latest"))
            .replace("/events", &format!("{base}/events"));
        assert!(html.contains(&format!("{base}/status")));
        assert!(html.contains(&format!("{base}/latest")));
        assert!(html.contains(&format!("{base}/events")));
        assert!(!html.contains("fetch('/api/latest'"));
        assert!(!html.contains("fetch('/api/status'"));
        for required in [
            "Cloud archive:",
            "htmlText(archive.error",
            "archive.last_success_unix_ms",
            "archive.last_failure_unix_ms",
            "archive.consecutive_failures",
            "refreshStatus()",
        ] {
            assert!(
                html.contains(required),
                "missing archive UI field {required}"
            );
        }
    }

    #[tokio::test]
    async fn public_auth_routes_have_a_small_request_body_limit() {
        let dir = TestDir::new();
        let app = build_router(&test_config(&dir)).unwrap();
        for route in [
            "/api/v1/auth/password/login",
            "/api/v1/auth/password/register",
            "/api/v1/auth/password-reset/request",
            "/api/v1/auth/password-reset/finish",
            "/api/v1/auth/invitation",
            "/api/v1/device-enrollments/exchange",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::post(route)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("x".repeat(AUTH_BODY_LIMIT_BYTES + 1)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        }
    }

    #[tokio::test]
    async fn dashboard_routes_enforce_self_or_admin_and_health_is_public() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let app = build_router(&config).unwrap();
        let db = CloudDb::open(&config.db_path).unwrap();
        let admin = db.user_by_email("admin@example.com").unwrap().unwrap();
        let invitation = db
            .issue_invitation("user@example.com", "invite", admin.id, 1, i64::MAX)
            .unwrap();
        let user = db
            .consume_invitation_and_create_user(NewRegistration {
                invitation_id: invitation,
                requested_user_id: Uuid::new_v4(),
                email: "user@example.com",
                display_name: "User",
                credential_id: "fake",
                passkey_json: r#"{"cred":{"cred_id":"fake"}}"#,
                now_ms: 2,
            })
            .unwrap();
        db.create_session(NewSession {
            user_id: user.id,
            installation_id: None,
            access_token: "user-access",
            refresh_token: "user-refresh",
            csrf_token: "user-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "admin-access",
            refresh_token: "admin-refresh",
            csrf_token: "admin-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();

        let health = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);
        let health = response_json(health).await;
        assert_eq!(health["schema_version"], 1);
        assert_eq!(health["database_schema_version"], CURRENT_SCHEMA_VERSION);
        assert_eq!(health["database_schema_ok"], true);
        assert_eq!(health["database_ok"], true);

        let own = app
            .clone()
            .oneshot(
                Request::get(format!("/dashboard/{}", user.id))
                    .header(header::COOKIE, "sensors_access=user-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(own.status(), StatusCode::OK);

        let missing_target = Uuid::new_v4();
        for (existing_path, missing_path) in [
            (
                format!("/dashboard/{}", admin.id),
                format!("/dashboard/{missing_target}"),
            ),
            (
                format!("/dashboard/{}/live-worker.js", admin.id),
                format!("/dashboard/{missing_target}/live-worker.js"),
            ),
            (
                format!("/api/v1/users/{}/dashboard/status", admin.id),
                format!("/api/v1/users/{missing_target}/dashboard/status"),
            ),
            (
                format!("/api/v1/users/{}/dashboard/latest", admin.id),
                format!("/api/v1/users/{missing_target}/dashboard/latest"),
            ),
            (
                format!("/api/v1/users/{}/dashboard/events?since=0", admin.id),
                format!("/api/v1/users/{missing_target}/dashboard/events?since=0"),
            ),
        ] {
            let mut bodies = Vec::new();
            for path in [existing_path, missing_path] {
                let denied = app
                    .clone()
                    .oneshot(
                        Request::get(&path)
                            .header(header::COOKIE, "sensors_access=user-access")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{path}");
                let mut body = response_json(denied).await;
                body.as_object_mut().unwrap().remove("request_id");
                assert_eq!(
                    body,
                    json!({
                        "ok": false,
                        "code": "forbidden",
                        "error": "dashboard access denied"
                    }),
                    "{path}"
                );
                bodies.push(body);
            }
            assert_eq!(bodies[0], bodies[1]);
        }

        let mut marker_bodies = Vec::new();
        for target in [admin.id, missing_target] {
            let path = format!("/api/v1/users/{target}/dashboard/marker");
            let denied = app
                .clone()
                .oneshot(
                    Request::post(&path)
                        .header(
                            header::COOKIE,
                            "sensors_access=user-access; sensors_csrf=user-csrf",
                        )
                        .header(header::ORIGIN, &config.public_origin)
                        .header("x-csrf-token", "user-csrf")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(r#"{"label":"cross-user denial"}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{path}");
            let mut body = response_json(denied).await;
            body.as_object_mut().unwrap().remove("request_id");
            marker_bodies.push(body);
        }
        assert_eq!(marker_bodies[0], marker_bodies[1]);
        assert_eq!(
            marker_bodies[0],
            json!({
                "ok": false,
                "code": "forbidden",
                "error": "dashboard access denied"
            })
        );

        let admin_view = app
            .oneshot(
                Request::get(format!("/dashboard/{}", user.id))
                    .header(header::COOKIE, "sensors_access=admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(admin_view.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_overview_is_one_multiplexed_feed_and_mutations_require_csrf() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let app = build_router(&config).unwrap();
        let db = CloudDb::open(&config.db_path).unwrap();
        let admin = db.user_by_email("admin@example.com").unwrap().unwrap();
        let invitation = db
            .issue_invitation("second@example.com", "invite", admin.id, 1, i64::MAX)
            .unwrap();
        let user = db
            .consume_invitation_and_create_user(NewRegistration {
                invitation_id: invitation,
                requested_user_id: Uuid::new_v4(),
                email: "second@example.com",
                display_name: "Second User",
                credential_id: "fake-second",
                passkey_json: r#"{"cred":{"cred_id":"fake-second"}}"#,
                now_ms: 2,
            })
            .unwrap();
        db.create_session(NewSession {
            user_id: user.id,
            installation_id: None,
            access_token: "second-access",
            refresh_token: "second-refresh",
            csrf_token: "second-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "overview-admin-access",
            refresh_token: "overview-admin-refresh",
            csrf_token: "overview-admin-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();

        let denied = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/overview/snapshot")
                    .header(header::COOKIE, "sensors_access=second-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        tokio::time::sleep(Duration::from_millis(20)).await;
        let snapshot = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/overview/snapshot")
                    .header(header::COOKIE, "sensors_access=overview-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        let snapshot_body = axum::body::to_bytes(snapshot.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let snapshot_json: Value = serde_json::from_slice(&snapshot_body).unwrap();
        assert_eq!(
            snapshot_json["users"].as_array().map(Vec::len),
            Some(2),
            "admin snapshot contains one isolated entry per user"
        );
        assert!(
            snapshot_json["users"]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| {
                    matches!(
                        entry["status"]["history_load_state"].as_str(),
                        Some("running" | "complete")
                    )
                })
        );

        let feed = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/overview/events")
                    .header(header::COOKIE, "sensors_access=overview-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(feed.status(), StatusCode::OK);
        assert_eq!(
            feed.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );

        let missing_csrf = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/admin/users/{}/disabled", user.id))
                    .header(header::COOKIE, "sensors_access=overview-admin-access")
                    .header(header::ORIGIN, "https://example.com")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"disabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);
        let valid_csrf = app
            .oneshot(
                Request::post(format!("/api/v1/admin/users/{}/disabled", user.id))
                    .header(header::COOKIE, "sensors_access=overview-admin-access")
                    .header(header::ORIGIN, "https://example.com")
                    .header("x-csrf-token", "overview-admin-csrf")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"disabled":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(valid_csrf.status(), StatusCode::OK);

        assert!(ADMIN_HTML.contains("new EventSource('/api/v1/admin/overview/events')"));
        assert!(ADMIN_HTML.contains("/api/v1/admin/overview/snapshot"));
        assert!(!ADMIN_HTML.contains("/dashboard/latest"));
        for required in [
            "phone_backlog_records",
            "phone_uploading",
            "st.replay",
            "st.offline",
            "latest_acc_unix_ns",
            "latest_hr_unix_ns",
            "ar.last_success_unix_ms",
            "ar.last_failure_unix_ms",
            "ar.consecutive_failures",
            "ar.last_archived_segments",
            "ar.last_output_files",
            "ar.error",
            "'resend','Resend'",
            "'revoke','Revoke'",
        ] {
            assert!(
                ADMIN_HTML.contains(required),
                "missing admin UI field {required}"
            );
        }
    }

    #[tokio::test]
    async fn cloud_sse_endpoints_preserve_connection_batch_and_disconnect_diagnostics() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let app = build_router(&config).unwrap();
        let db = CloudDb::open(&config.db_path).unwrap();
        let admin = db.user_by_email("admin@example.com").unwrap().unwrap();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "sse-admin-access",
            refresh_token: "sse-admin-refresh",
            csrf_token: "sse-admin-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();

        let marker = app
            .clone()
            .oneshot(
                Request::post(format!("/api/v1/users/{}/dashboard/marker", admin.id))
                    .header(
                        header::COOKIE,
                        "sensors_access=sse-admin-access; sensors_csrf=sse-admin-csrf",
                    )
                    .header(header::ORIGIN, "https://example.com")
                    .header("x-csrf-token", "sse-admin-csrf")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"label":"SSE diagnostic fixture"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(marker.status(), StatusCode::OK);

        let feed = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/users/{}/dashboard/events?since=0",
                    admin.id
                ))
                .header(header::COOKIE, "sensors_access=sse-admin-access")
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(feed.status(), StatusCode::OK);

        let connected = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/dashboard/status", admin.id))
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let connected_json: Value = serde_json::from_slice(
            &axum::body::to_bytes(connected.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(connected_json["sse_connected_clients"].as_u64(), Some(1));

        let mut body = feed.into_body().into_data_stream();
        let first_event = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("cloud dashboard SSE event timeout")
            .expect("cloud dashboard SSE ended")
            .expect("cloud dashboard SSE body error");
        assert!(
            String::from_utf8_lossy(&first_event).contains("event: batch"),
            "unexpected SSE payload: {}",
            String::from_utf8_lossy(&first_event)
        );

        let after_batch = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/dashboard/status", admin.id))
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let after_batch_json: Value = serde_json::from_slice(
            &axum::body::to_bytes(after_batch.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(after_batch_json["sse_batches_sent"].as_u64().unwrap_or(0) >= 1);
        assert!(after_batch_json["sse_rows_sent"].as_u64().unwrap_or(0) >= 1);
        drop(body);
        tokio::task::yield_now().await;

        let disconnected = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/dashboard/status", admin.id))
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let disconnected_json: Value = serde_json::from_slice(
            &axum::body::to_bytes(disconnected.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(disconnected_json["sse_connected_clients"].as_u64(), Some(0));

        let overview = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/overview/events")
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut overview_body = overview.into_body().into_data_stream();
        tokio::time::timeout(Duration::from_secs(2), overview_body.next())
            .await
            .expect("admin overview SSE timeout")
            .expect("admin overview SSE ended")
            .expect("admin overview SSE body error");
        let overview_connected = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/dashboard/status", admin.id))
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let overview_connected_json: Value = serde_json::from_slice(
            &axum::body::to_bytes(overview_connected.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            overview_connected_json["sse_connected_clients"].as_u64(),
            Some(1)
        );
        drop(overview_body);
        tokio::task::yield_now().await;
        let overview_disconnected = app
            .oneshot(
                Request::get(format!("/api/v1/users/{}/dashboard/status", admin.id))
                    .header(header::COOKIE, "sensors_access=sse-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let overview_disconnected_json: Value = serde_json::from_slice(
            &axum::body::to_bytes(overview_disconnected.into_body(), 1024 * 1024)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            overview_disconnected_json["sse_connected_clients"].as_u64(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn admin_overview_starts_and_reconnects_at_current_snapshot_with_full_delta_ring() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let now = unix_ms();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "overview-startup-access",
                refresh_token: "overview-startup-refresh",
                csrf_token: "overview-startup-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let installation = Uuid::new_v4();
        state
            .db
            .insert_test_device_credential(
                admin.id,
                installation,
                Uuid::new_v4(),
                "overview-startup-device",
                now,
            )
            .unwrap();
        let mut request = valid_ingest_request(installation, now);
        let template = request.records[0].clone();
        // Real PMD replay produces >20,000 deltas, filling the retained ring.
        // The former zero cursor took ~40 x 200 ms to reach the live edge.
        request.records = (0..300)
            .map(|index| {
                let mut record = template.clone();
                record.record_id = Uuid::new_v4();
                record.source_seq = index;
                let mut payload = vec![0];
                payload.extend_from_slice(
                    &(1_000_000_000_u64 + u64::try_from(index).unwrap() * 600_000_000)
                        .to_le_bytes(),
                );
                payload.push(0);
                for _ in 0..73 {
                    payload.extend_from_slice(&[1, 0, 0]);
                }
                record.body = serde_json::to_value(WalBody::GattNotification(
                    GattNotificationBody::from_payload(
                        "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
                        "pmd_data",
                        &payload,
                    )
                    .unwrap(),
                ))
                .unwrap();
                record
            })
            .collect();
        request.last_source_seq = 299;
        for records in request.records.chunks(150) {
            let mut chunk = request.clone();
            chunk.batch_id = Uuid::new_v4();
            chunk.records = records.to_vec();
            chunk.first_source_seq = records.first().unwrap().source_seq;
            chunk.last_source_seq = records.last().unwrap().source_seq;
            let encoded = serde_json::to_vec(&chunk).unwrap();
            state
                .ingest
                .ingest(
                    &admin,
                    installation,
                    chunk,
                    hex::encode(sha2::Sha256::digest(&encoded)),
                    encoded.len(),
                )
                .unwrap();
        }
        let runtime = ensure_dashboard_runtime_for_known_user(&state, admin.id).unwrap();
        let initial_seq = runtime.lock().unwrap().snapshot().latest_seq;
        assert!(
            initial_seq > 20_000,
            "fixture must fill the live ring: {initial_seq}"
        );

        // Repeat with a new SSE connection to cover EventSource reconnect too.
        for offset in 0..2 {
            let response = app
                .clone()
                .oneshot(
                    Request::get("/api/v1/admin/overview/events")
                        .header(header::COOKIE, "sensors_access=overview-startup-access")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let mut body = response.into_body().into_data_stream();
            let frame = tokio::time::timeout(Duration::from_secs(2), body.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame = std::str::from_utf8(&frame).unwrap();
            assert!(frame.starts_with("event: overview_snapshot\n"));
            let snapshot: Value =
                serde_json::from_str(frame.split_once("data: ").unwrap().1.trim()).unwrap();
            let user = &snapshot["users"][0];
            assert_eq!(user["user"]["id"], admin.id.to_string());
            let cursor = user["latest_sequence"].as_u64().unwrap();
            assert!(cursor >= initial_seq);
            assert_eq!(user["ecg"].as_array().unwrap().len(), 1300);

            let mut fresh = request.clone();
            fresh.batch_id = Uuid::new_v4();
            fresh.first_source_seq = 300 + offset;
            fresh.last_source_seq = fresh.first_source_seq;
            let mut record = template.clone();
            record.record_id = Uuid::new_v4();
            record.source_seq = fresh.first_source_seq;
            record.collector_unix_ns = unix_ms() * 1_000_000;
            fresh.records = vec![record];
            let encoded = serde_json::to_vec(&fresh).unwrap();
            state
                .ingest
                .ingest(
                    &admin,
                    installation,
                    fresh,
                    hex::encode(sha2::Sha256::digest(&encoded)),
                    encoded.len(),
                )
                .unwrap();
            runtime.lock().unwrap().poll_once().unwrap();
            let newest = runtime.lock().unwrap().snapshot().latest_seq;
            assert!(newest > cursor);
            let frame = tokio::time::timeout(Duration::from_secs(2), body.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let frame = std::str::from_utf8(&frame).unwrap();
            assert!(frame.starts_with("event: overview\n"));
            let delta: Value =
                serde_json::from_str(frame.split_once("data: ").unwrap().1.trim()).unwrap();
            let batch = &delta["users"][0]["batch"];
            assert_eq!(batch["from_seq"].as_u64(), Some(cursor));
            assert_eq!(
                batch["to_seq"].as_u64(),
                Some(newest),
                "first delta reaches new data immediately"
            );
            assert_eq!(batch["overflow"], false);
            assert!(
                batch["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|item| item["seq"].as_u64().unwrap() > cursor)
            );
            assert!(
                batch["items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item["kind"] == "ecg")
            );
        }
    }

    #[tokio::test]
    async fn browser_refresh_uses_http_only_cookie_after_access_expiry() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let app = build_router(&config).unwrap();
        let db = CloudDb::open(&config.db_path).unwrap();
        let admin = db.user_by_email("admin@example.com").unwrap().unwrap();
        let now = unix_ms();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "expired-browser-access",
            refresh_token: "browser-refresh-secret",
            csrf_token: "browser-csrf",
            user_agent: None,
            now_ms: now - 10_000,
            access_expires_ms: now - 1,
            refresh_expires_ms: now + 60_000,
        })
        .unwrap();
        let rotation = Uuid::new_v4();
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/auth/browser-refresh")
                    .header(header::ORIGIN, "https://example.com")
                    .header("x-csrf-token", "browser-csrf")
                    .header(
                        header::COOKIE,
                        format!(
                            "sensors_refresh=browser-refresh-secret; sensors_rotation={rotation}; sensors_csrf=browser-csrf"
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let new_access = response_cookie(&response, "sensors_access").unwrap();
        let new_refresh = response_cookie(&response, "sensors_refresh").unwrap();
        let retry = app
            .clone()
            .oneshot(
                Request::post("/api/v1/auth/browser-refresh")
                    .header(header::ORIGIN, "https://example.com")
                    .header("x-csrf-token", "browser-csrf")
                    .header(
                        header::COOKIE,
                        format!(
                            "sensors_refresh=browser-refresh-secret; sensors_rotation={rotation}; sensors_csrf=browser-csrf"
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        assert_eq!(
            response_cookie(&retry, "sensors_access").unwrap(),
            new_access
        );
        assert_eq!(
            response_cookie(&retry, "sensors_refresh").unwrap(),
            new_refresh
        );
        assert_ne!(new_refresh, "browser-refresh-secret");
        assert!(
            response
                .headers()
                .get_all(header::SET_COOKIE)
                .iter()
                .any(|value| {
                    value
                        .to_str()
                        .unwrap_or_default()
                        .starts_with("sensors_refresh=")
                        && value.to_str().unwrap_or_default().contains("HttpOnly")
                })
        );
        assert!(
            db.authenticate_access(&new_access, unix_ms())
                .unwrap()
                .is_some()
        );
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            !body.contains(&new_refresh),
            "refresh token must never be exposed to browser JS"
        );
    }

    #[tokio::test]
    async fn archive_failure_is_visible_in_health_user_and_admin_diagnostics() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let archive_health = Arc::new(Mutex::new(ArchiveHealth::default()));
        let app = build_router_with_archive_health(&config, archive_health.clone()).unwrap();
        note_archive_failure(&archive_health);
        archive_health.lock().unwrap().error =
            Some(r#"<img src=x onerror="globalThis.pwned=true">"#);
        let db = CloudDb::open(&config.db_path).unwrap();
        let admin = db.user_by_email("admin@example.com").unwrap().unwrap();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "archive-admin-access",
            refresh_token: "archive-admin-refresh",
            csrf_token: "archive-admin-csrf",
            user_agent: None,
            now_ms: 3,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();

        let response = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["ok"], false);
        assert_eq!(json["archive"]["state"], "error");
        assert_eq!(json["service"], "training_sensors_cloud");

        for path in [
            format!("/api/v1/users/{}/dashboard/status", admin.id),
            format!("/api/v1/users/{}/dashboard/latest", admin.id),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(path)
                        .header(header::COOKIE, "sensors_access=archive-admin-access")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap();
            let json: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["archive"]["state"], "error");
            assert_eq!(
                json["archive"]["error"],
                r#"<img src=x onerror="globalThis.pwned=true">"#
            );
            if json.get("status").is_some() {
                assert_eq!(json["status"]["archive"], json["archive"]);
            }
        }

        let feed = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/overview/events")
                    .header(header::COOKIE, "sensors_access=archive-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(feed.status(), StatusCode::OK);
        let mut feed_body = feed.into_body().into_data_stream();
        let event = tokio::time::timeout(Duration::from_secs(2), feed_body.next())
            .await
            .expect("admin archive overview SSE timeout")
            .expect("admin archive overview SSE ended")
            .expect("admin archive overview SSE body error");
        let event = String::from_utf8_lossy(&event);
        assert!(
            event.contains("\"archive\""),
            "archive omitted from SSE: {event}"
        );
        assert!(
            event.contains("\"consecutive_failures\":1"),
            "archive failure count omitted from SSE: {event}"
        );

        let response = app
            .oneshot(
                Request::get("/api/v1/admin/overview/snapshot")
                    .header(header::COOKIE, "sensors_access=archive-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["archive"]["state"], "error");
        assert_eq!(json["archive"]["consecutive_failures"], 1);
        assert_eq!(json["users"].as_array().map(Vec::len), Some(1));
    }

    #[tokio::test]
    async fn enrollment_http_preserves_retries_and_exposes_only_stable_terminal_state() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, current) = create_password_user(
            &state,
            "enroll-http@example.com",
            "enrollment-password",
            now,
        );
        let installation_id = Uuid::new_v4();

        let enrollment = state
            .auth
            .create_enrollment(&current.user, current.session_id, now + 3)
            .unwrap();
        let request = enrollment_exchange_json(
            &enrollment.enrollment_token,
            installation_id,
            Uuid::new_v4(),
        );
        state.db.fail_next_enrollment_exchange_before_commit();
        let failed = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response_json(failed).await["code"], "internal_error");
        let retried = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retried.status(), StatusCode::OK);
        let first = response_json(retried).await;

        let ambiguous = state
            .auth
            .create_enrollment(&current.user, current.session_id, now + 4)
            .unwrap();
        let ambiguous_request =
            enrollment_exchange_json(&ambiguous.enrollment_token, installation_id, Uuid::new_v4());
        state.db.fail_next_enrollment_exchange_after_commit();
        let failed = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ambiguous_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let retried = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ambiguous_request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retried.status(), StatusCode::OK);
        let recovered = response_json(retried).await;
        assert_ne!(first["credential_id"], recovered["credential_id"]);

        let mut changed_replay = ambiguous_request.clone();
        changed_replay["exchange_id"] = json!(Uuid::new_v4());
        let terminal = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&changed_replay).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(terminal.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_json(terminal).await["code"],
            "enrollment_link_terminal"
        );

        let malformed_enrollment = state
            .auth
            .create_enrollment(&current.user, current.session_id, now + 5)
            .unwrap();
        let mut malformed = enrollment_exchange_json(
            &malformed_enrollment.enrollment_token,
            installation_id,
            Uuid::new_v4(),
        );
        malformed["android_sdk"] = json!(0);
        let invalid = app
            .oneshot(
                Request::post("/api/v1/device-enrollments/exchange")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&malformed).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response_json(invalid).await["code"], "invalid_request");
    }

    #[tokio::test]
    async fn browser_and_device_credentials_are_strictly_route_scoped_and_revocable() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser, current) =
            create_password_user(&state, "scope@example.com", "route-scope-password", now);
        let installation_id = Uuid::new_v4();
        let enrollment = state
            .auth
            .create_enrollment(&current.user, current.session_id, now + 3)
            .unwrap();
        let exchange_value = enrollment_exchange_json(
            &enrollment.enrollment_token,
            installation_id,
            Uuid::new_v4(),
        );
        let credential = state
            .auth
            .exchange_enrollment(
                serde_json::from_value(exchange_value).unwrap(),
                "scope-client",
                now + 4,
            )
            .unwrap();
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "scope-admin-access",
                refresh_token: "scope-admin-refresh",
                csrf_token: "scope-admin-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();

        for path in [
            "/api/v1/me".to_string(),
            format!("/dashboard/{}", current.user.id),
            "/api/v1/admin/users".to_string(),
        ] {
            let bearer = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {}", browser.access_token),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if path.starts_with("/dashboard/") {
                assert_eq!(bearer.status(), StatusCode::OK, "{path}");
                let body = axum::body::to_bytes(bearer.into_body(), 128 * 1024)
                    .await
                    .unwrap();
                assert_eq!(body.as_ref(), SESSION_BOOTSTRAP_HTML.as_bytes());
            } else {
                assert_eq!(bearer.status(), StatusCode::UNAUTHORIZED, "{path}");
            }
            let device = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(
                            header::AUTHORIZATION,
                            format!("Device {}", credential.device_credential),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            if path.starts_with("/dashboard/") {
                assert_eq!(device.status(), StatusCode::OK, "{path}");
                let body = axum::body::to_bytes(device.into_body(), 128 * 1024)
                    .await
                    .unwrap();
                assert_eq!(body.as_ref(), SESSION_BOOTSTRAP_HTML.as_bytes());
            } else {
                assert_eq!(device.status(), StatusCode::UNAUTHORIZED, "{path}");
            }
        }

        let mutation_cases = [
            ("/api/v1/auth/logout".to_string(), json!({})),
            ("/api/v1/device-enrollments".to_string(), json!({})),
            (
                format!("/api/v1/users/{}/dashboard/marker", current.user.id),
                json!({"label":"scope test"}),
            ),
            (
                format!("/api/v1/admin/users/{}/disabled", current.user.id),
                json!({"disabled":false}),
            ),
            (
                format!(
                    "/api/v1/admin/device-credentials/{}/revoke",
                    credential.credential_id
                ),
                json!({}),
            ),
        ];
        for (path, body) in mutation_cases {
            for authorization in [
                format!("Bearer {}", browser.access_token),
                format!("Device {}", credential.device_credential),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::post(&path)
                            .header(header::AUTHORIZATION, authorization)
                            .header(header::ORIGIN, &config.public_origin)
                            .header("x-csrf-token", "scope-admin-csrf")
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(Body::from(serde_json::to_vec(&body).unwrap()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
            }
        }

        let ingest = valid_ingest_request(installation_id, now + 5);
        let cookie_denied = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser.access_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cookie_denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(cookie_denied).await["code"],
            "device_reconnect_required"
        );
        let mut wrong_installation = ingest.clone();
        wrong_installation.installation_id = Uuid::new_v4();
        let mismatch = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        format!("Device {}", credential.device_credential),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&wrong_installation).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mismatch.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response_json(mismatch).await["code"],
            "device_reconnect_required"
        );
        let accepted = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        format!("Device {}", credential.device_credential),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);

        let installations = app
            .clone()
            .oneshot(
                Request::get("/api/v1/admin/installations")
                    .header(header::COOKIE, "sensors_access=scope-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(installations.status(), StatusCode::OK);
        let installations_text = response_json(installations).await.to_string();
        assert!(!installations_text.contains(&credential.device_credential));
        assert!(!installations_text.contains("credential_hash"));

        let revoked = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/api/v1/admin/device-credentials/{}/revoke",
                    credential.credential_id
                ))
                .header(
                    header::COOKIE,
                    "sensors_access=scope-admin-access; sensors_csrf=scope-admin-csrf",
                )
                .header(header::ORIGIN, &config.public_origin)
                .header("x-csrf-token", "scope-admin-csrf")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::OK);
        let denied = app
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        format!("Device {}", credential.device_credential),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(denied).await["code"],
            "device_reconnect_required"
        );
    }

    #[tokio::test]
    async fn retired_dashboard_target_is_denied_with_cached_and_cold_state() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser, current) =
            create_password_user(&state, "retired@example.com", "retirement-password", now);
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "retirement-admin-access",
                refresh_token: "retirement-admin-refresh",
                csrf_token: "retirement-admin-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let cached = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/api/v1/users/{}/dashboard/status",
                    current.user.id
                ))
                .header(
                    header::COOKIE,
                    format!("sensors_access={}", browser.access_token),
                )
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cached.status(), StatusCode::OK);
        assert!(
            state
                .dashboards
                .lock()
                .unwrap()
                .contains_key(&current.user.id)
        );
        assert_eq!(
            state
                .db
                .set_users_retired_exact(
                    &[(current.user.id, current.user.email.as_str())],
                    admin.id,
                    &admin.email,
                    "deterministic test retirement",
                    true,
                    now + 10,
                )
                .unwrap(),
            1
        );

        for path in [
            format!("/dashboard/{}", current.user.id),
            format!("/dashboard/{}/live-worker.js", current.user.id),
            format!("/api/v1/users/{}/dashboard/status", current.user.id),
            format!("/api/v1/users/{}/dashboard/latest", current.user.id),
            format!("/api/v1/users/{}/dashboard/events?since=0", current.user.id),
        ] {
            let denied = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(header::COOKIE, "sensors_access=retirement-admin-access")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::FORBIDDEN, "{path}");
        }
        let marker = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/api/v1/users/{}/dashboard/marker",
                    current.user.id
                ))
                .header(
                    header::COOKIE,
                    "sensors_access=retirement-admin-access; sensors_csrf=retirement-admin-csrf",
                )
                .header(header::ORIGIN, &config.public_origin)
                .header("x-csrf-token", "retirement-admin-csrf")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"label":"must fail"}"#))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(marker.status(), StatusCode::FORBIDDEN);

        drop(app);
        drop(state);
        let (cold_app, cold_state) = build_router_and_state(&config).unwrap();
        assert!(
            !cold_state
                .dashboards
                .lock()
                .unwrap()
                .contains_key(&current.user.id)
        );
        let cold = cold_app
            .oneshot(
                Request::get(format!("/dashboard/{}", current.user.id))
                    .header(header::COOKIE, "sensors_access=retirement-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cold.status(), StatusCode::FORBIDDEN);
        assert!(cold_state.db.quick_check().is_ok());
    }

    #[test]
    fn client_ip_bucket_accepts_only_canonical_ip_addresses_from_the_private_header() {
        let mut first = HeaderMap::new();
        first.insert("cf-connecting-ip", HeaderValue::from_static("198.51.100.1"));
        first.insert("x-forwarded-for", HeaderValue::from_static("198.51.100.2"));
        let mut second = HeaderMap::new();
        second.insert(
            "cf-connecting-ip",
            HeaderValue::from_static("203.0.113.200"),
        );
        second.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.201"));
        assert_eq!(auth_client_key(&first), SHARED_AUTH_CLIENT_KEY);
        assert_eq!(auth_client_key(&first), auth_client_key(&second));
        first.insert(
            "x-sensors-client-ip",
            HeaderValue::from_static("192.0.2.10"),
        );
        assert_eq!(auth_client_key(&first), "192.0.2.10");

        first.insert(
            "x-sensors-client-ip",
            HeaderValue::from_static("2001:0db8:0000:0000:0000:ff00:0042:8329"),
        );
        assert_eq!(auth_client_key(&first), "2001:db8::ff00:42:8329");

        for invalid in [
            "not-an-address".to_string(),
            "192.0.2.10, 198.51.100.4".to_string(),
            "192.0.2.0/24".to_string(),
            String::new(),
            "1".repeat(65),
        ] {
            first.insert(
                "x-sensors-client-ip",
                HeaderValue::from_str(&invalid).unwrap(),
            );
            assert_eq!(auth_client_key(&first), SHARED_AUTH_CLIENT_KEY);
        }
    }

    #[tokio::test]
    async fn password_reset_public_response_has_common_timing_and_shape() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        create_password_user(
            &state,
            "reset-timing@example.com",
            "reset-timing-password",
            now,
        );
        let request = |email: &str| {
            Request::post("/api/v1/auth/password-reset/request")
                .header(header::ORIGIN, &config.public_origin)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({"email": email})).unwrap(),
                ))
                .unwrap()
        };
        let known_started = Instant::now();
        let known = app
            .clone()
            .oneshot(request("reset-timing@example.com"))
            .await
            .unwrap();
        let known_elapsed = known_started.elapsed();
        let unknown_started = Instant::now();
        let unknown = app.oneshot(request("absent@example.com")).await.unwrap();
        let unknown_elapsed = unknown_started.elapsed();
        assert_eq!(known.status(), StatusCode::OK);
        assert_eq!(unknown.status(), StatusCode::OK);
        assert_eq!(response_json(known).await, response_json(unknown).await);
        assert!(known_elapsed >= Duration::from_millis(65));
        assert!(unknown_elapsed >= Duration::from_millis(65));
        assert!(known_elapsed < Duration::from_millis(500));
        assert!(unknown_elapsed < Duration::from_millis(500));
        assert!(known_elapsed.abs_diff(unknown_elapsed) < Duration::from_millis(100));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_password_capacity_does_not_occupy_blocking_ingest_workers() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let first = state
            .password_work_limit
            .clone()
            .try_acquire_owned()
            .unwrap();
        let second = state
            .password_work_limit
            .clone()
            .try_acquire_owned()
            .unwrap();
        let mut requests = Vec::new();
        for index in 0..32 {
            let app = app.clone();
            let origin = config.public_origin.clone();
            requests.push(tokio::spawn(async move {
                app.oneshot(
                    Request::post("/api/v1/auth/password/login")
                        .header(header::ORIGIN, origin)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&json!({
                                "email": format!("busy-{index}@example.com"),
                                "password": "not-a-real-password"
                            }))
                            .unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
            }));
        }
        let ingest = tokio::time::timeout(
            Duration::from_millis(500),
            run_blocking_ingest(|| Ok(test_ingest_response())),
        )
        .await
        .expect("blocking ingest was starved by rejected password work")
        .unwrap();
        assert_eq!(ingest.schema_version, 1);
        for request in requests {
            assert_eq!(request.await.unwrap(), StatusCode::TOO_MANY_REQUESTS);
        }
        drop((first, second));
    }

    #[test]
    fn web_login_page_offers_show_password_toggles() {
        assert_eq!(LOGIN_HTML.matches("class=\"show muted\"").count(), 3);
        // The toggles are wired unconditionally at page load, so every referenced id must
        // exist in the markup or the whole login page dies with a TypeError.
        for id in ["id=loginShow", "id=registerShow", "id=resetShow"] {
            assert!(LOGIN_HTML.contains(id), "missing {id} in LOGIN_HTML");
        }
        assert!(LOGIN_HTML.contains("function showToggle"));
        assert!(LOGIN_HTML.contains("showToggle('#loginShow','#loginPassword')"));
        assert!(
            LOGIN_HTML
                .contains("showToggle('#registerShow','#registerPassword','#registerConfirm')")
        );
        assert!(LOGIN_HTML.contains("showToggle('#resetShow','#resetPassword','#resetConfirm')"));
    }

    #[test]
    fn web_password_policy_uses_utf8_bytes_at_unicode_boundaries() {
        assert!(LOGIN_HTML.contains("new TextEncoder().encode(value).length"));
        assert!(LOGIN_HTML.contains("value.includes('\\0')"));
        assert_eq!(LOGIN_HTML.matches("validPassword(").count(), 4);
        assert!(!LOGIN_HTML.contains("minlength="));
        assert!(!LOGIN_HTML.contains("maxlength=128"));

        for valid in [
            "a".repeat(12),
            "é".repeat(6),
            "🙂".repeat(3),
            "🙂".repeat(32),
        ] {
            assert!(
                crate::cloud::auth::validate_password(&valid).is_ok(),
                "expected {} UTF-8 bytes to be valid",
                valid.len()
            );
        }
        for invalid in [
            "a".repeat(11),
            "é".repeat(5),
            "🙂".repeat(33),
            "valid-password\0bad".to_string(),
        ] {
            assert!(
                crate::cloud::auth::validate_password(&invalid).is_err(),
                "expected {} UTF-8 bytes to be invalid",
                invalid.len()
            );
        }
    }

    #[tokio::test]
    async fn expired_page_loads_run_the_session_bootstrap_instead_of_bare_401() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, wearer) = create_password_user(
            &state,
            "bootstrap-wearer@example.com",
            "bootstrap-password-1",
            now,
        );
        let (_, other) = create_password_user(
            &state,
            "bootstrap-other@example.com",
            "bootstrap-password-2",
            now,
        );
        state
            .db
            .create_session(NewSession {
                user_id: wearer.user.id,
                installation_id: None,
                access_token: "bootstrap-live-access",
                refresh_token: "bootstrap-live-refresh",
                csrf_token: "bootstrap-live-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let body_text = |response: Response| async move {
            String::from_utf8_lossy(
                &axum::body::to_bytes(response.into_body(), 128 * 1024)
                    .await
                    .unwrap(),
            )
            .to_string()
        };

        // An expired access cookie on a device that signed in before (CSRF
        // cookie present) gets the refresh bootstrap page, not a bare 401.
        for path in [
            format!("/dashboard/{}", wearer.user.id),
            "/admin".to_string(),
            "/".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(&path)
                        .header(
                            header::COOKIE,
                            "sensors_access=expired-or-garbage; sensors_csrf=still-here",
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let text = body_text(response).await;
            assert!(text.contains("Signing you back in"), "{path}");
            assert!(text.contains("browser-refresh"), "{path}");
        }

        // An external app/link can withhold ALL Strict cookies from the initial
        // navigation. Only return the public recovery shell: the subsequent
        // same-origin session check still has to authenticate the browser.
        for path in [
            format!("/dashboard/{}", wearer.user.id),
            "/admin".to_string(),
            "/".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(&path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            let text = body_text(response).await;
            assert_eq!(text, SESSION_BOOTSTRAP_HTML, "{path}");
            assert!(!text.contains("bootstrap-wearer@example.com"));
        }
        // Recovery is an HTML-only behavior, never an authorization bypass for
        // private API data or APK downloads.
        for path in [
            "/api/v1/me".to_string(),
            "/api/v1/admin/users".to_string(),
            format!("/api/v1/users/{}/dashboard/latest", wearer.user.id),
            format!("/api/v1/users/{}/dashboard/status", wearer.user.id),
            "/downloads/training-sensors.apk".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(&path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        }

        // A wrong-user page with a VALID session must stay 403 — the
        // bootstrap must never turn an authorization failure into a
        // refresh-reload loop.
        let forbidden = app
            .clone()
            .oneshot(
                Request::get(format!("/dashboard/{}", other.user.id))
                    .header(
                        header::COOKIE,
                        "sensors_access=bootstrap-live-access; sensors_csrf=bootstrap-live-csrf",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
        let text = body_text(forbidden).await;
        assert!(!text.contains("Signing you back in"));

        // The invitation surfaces carry the humane 7-day copy.
        let admin_page = app
            .clone()
            .oneshot(
                Request::get("/admin")
                    .header(
                        header::COOKIE,
                        format!(
                            "sensors_access={}; sensors_csrf={}",
                            "bootstrap-live-access", "bootstrap-live-csrf"
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // The wearer is not an admin: expect the bootstrap-free 403 here too.
        assert_eq!(admin_page.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn account_and_enrollment_pages_are_no_store_and_share_one_fragment_link() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        for path in [
            "/login",
            "/invite",
            "/forgot-password",
            "/reset-password",
            "/enroll",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store"
            );
            assert_eq!(
                response.headers().get(header::REFERRER_POLICY).unwrap(),
                "no-referrer"
            );
            let body = axum::body::to_bytes(response.into_body(), 128 * 1024)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&body).contains("passkey"));
        }
        let now = unix_ms();
        let (browser, _) =
            create_password_user(&state, "page@example.com", "page-enrollment-password", now);
        let enrollment = app
            .oneshot(
                Request::post("/api/v1/device-enrollments")
                    .header(
                        header::COOKIE,
                        format!(
                            "sensors_access={}; sensors_csrf={}",
                            browser.access_token, browser.csrf_token
                        ),
                    )
                    .header(header::ORIGIN, &config.public_origin)
                    .header("x-csrf-token", &browser.csrf_token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(enrollment.status(), StatusCode::OK);
        assert_eq!(
            enrollment.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let enrollment = response_json(enrollment).await;
        let link = enrollment["enrollment_url"].as_str().unwrap();
        assert!(link.starts_with("https://example.com/enroll#"));
        assert!(!link.contains('?'));
        let qr_svg = enrollment["qr_svg"].as_str().unwrap();
        assert!(qr_svg.contains("<svg"));
        assert_eq!(decode_qr_svg(qr_svg), link);
        assert!(enrollment.get("device_credential").is_none());
        assert!(HOME_ENROLLMENT_JS.contains("/api/v1/device-enrollments"));
        assert!(HOME_ENROLLMENT_JS.contains("connectThisPhone"));
        assert!(HOME_ENROLLMENT_JS.contains("showEnrollmentQr"));
    }

    #[tokio::test]
    async fn enrollment_preview_bootstraps_apk_download_without_consuming_the_link() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, wearer) = create_password_user(
            &state,
            "qr-wearer@example.com",
            "qr-bootstrap-password",
            now,
        );
        let session_id = state
            .db
            .create_session(NewSession {
                user_id: wearer.user.id,
                installation_id: None,
                access_token: "qr-wearer-access",
                refresh_token: "qr-wearer-refresh",
                csrf_token: "qr-wearer-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let enrollment = state
            .auth
            .create_enrollment(&wearer.user, session_id, now)
            .unwrap();
        let preview = |token: String| {
            let app = app.clone();
            async move {
                app.oneshot(
                    Request::post("/api/v1/device-enrollments/preview")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::ORIGIN, "https://example.com")
                        .body(Body::from(json!({ "enrollment_token": token }).to_string()))
                        .unwrap(),
                )
                .await
                .unwrap()
            }
        };

        // The relying-party origin is mandatory for the public preview
        // endpoint, matching the sibling public lookup convention.
        let no_origin = app
            .clone()
            .oneshot(
                Request::post("/api/v1/device-enrollments/preview")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "enrollment_token": "x" }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_origin.status(), StatusCode::FORBIDDEN);

        // Malformed tokens: generic client error, no ticket cookie.
        let denied = preview("not-a-valid-token".to_string()).await;
        assert!(denied.status().is_client_error());
        assert!(denied.headers().get(header::SET_COOKIE).is_none());

        // A valid link previews without being consumed and mints the ticket.
        let ok = preview(enrollment.enrollment_token.clone()).await;
        assert_eq!(ok.status(), StatusCode::OK);
        let cookie = ok
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(cookie.starts_with("sensors_apk_ticket="));
        for attribute in ["Path=/downloads", "HttpOnly", "SameSite=Lax", "Secure"] {
            assert!(cookie.contains(attribute), "{attribute}: {cookie}");
        }
        let ticket = cookie
            .strip_prefix("sensors_apk_ticket=")
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string();
        let body = axum::body::to_bytes(ok.into_body(), 64 * 1024)
            .await
            .unwrap();
        let preview_json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            preview_json["expires_at_unix_ms"],
            json!(enrollment.expires_at_unix_ms)
        );
        assert_eq!(
            preview_json["apk"]["sha256"],
            json!(state.android_apk.sha256)
        );
        assert_eq!(
            preview_json["apk"]["version_name"],
            json!(state.android_apk.version_name)
        );
        assert_eq!(
            preview_json["apk"]["version_code"],
            json!(state.android_apk.version_code)
        );

        // The ticket authorizes exactly the APK download with no session, and
        // is not consumed by a download (a retry keeps working).
        for _ in 0..2 {
            let download = app
                .clone()
                .oneshot(
                    Request::get("/downloads/training-sensors.apk")
                        .header(header::COOKIE, format!("sensors_apk_ticket={ticket}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(download.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(download.into_body(), 16 * 1024)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), TEST_ANDROID_APK_BYTES);
        }
        let wrong = app
            .clone()
            .oneshot(
                Request::get("/downloads/training-sensors.apk")
                    .header(
                        header::COOKIE,
                        format!("sensors_apk_ticket={}", random_token()),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        // Preview left the one-use exchange fully intact.
        let device = state
            .auth
            .exchange_enrollment(
                serde_json::from_value(enrollment_exchange_json(
                    &enrollment.enrollment_token,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                ))
                .unwrap(),
                "qr-bootstrap-scope",
                now + 5,
            )
            .unwrap();
        assert!(!device.device_credential.is_empty());

        // A consumed link no longer previews and mints no new ticket.
        let consumed = preview(enrollment.enrollment_token.clone()).await;
        assert!(consumed.status().is_client_error());
        assert!(consumed.headers().get(header::SET_COOKIE).is_none());

        // An expired link is rejected by the same validation.
        assert!(
            state
                .auth
                .preview_enrollment(
                    &enrollment.enrollment_token,
                    "qr-expired-check",
                    enrollment.expires_at_unix_ms + 1,
                )
                .is_err()
        );

        // An expired ticket no longer authorizes the download.
        let stale_ticket = random_token();
        state.apk_tickets.lock().unwrap().insert(
            hash_secret("stale-enrollment-slot"),
            (hash_secret(&stale_ticket), unix_ms() - 1_000),
        );
        let stale = app
            .clone()
            .oneshot(
                Request::get("/downloads/training-sensors.apk")
                    .header(header::COOKIE, format!("sensors_apk_ticket={stale_ticket}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::UNAUTHORIZED);

        // The guided bootstrap page carries the phone flow markers.
        let page = app
            .clone()
            .oneshot(Request::get("/enroll").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(page.status(), StatusCode::OK);
        let page_body = axum::body::to_bytes(page.into_body(), 128 * 1024)
            .await
            .unwrap();
        let page_text = String::from_utf8_lossy(&page_body);
        for marker in [
            "Connect Training Sensors",
            "/api/v1/device-enrollments/preview",
            "/downloads/training-sensors.apk",
            "Regenerate QR code",
        ] {
            assert!(page_text.contains(marker), "{marker}");
        }
    }

    #[tokio::test]
    async fn authenticated_onboarding_serves_only_the_pinned_private_beta_apk() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, wearer_current) = create_password_user(
            &state,
            "apk-wearer@example.com",
            "private-beta-password",
            now,
        );
        let admin = state
            .db
            .user_by_email("admin@example.com")
            .unwrap()
            .unwrap();
        let wearer_session_id = state
            .db
            .create_session(NewSession {
                user_id: wearer_current.user.id,
                installation_id: None,
                access_token: "apk-wearer-access",
                refresh_token: "apk-wearer-refresh",
                csrf_token: "apk-wearer-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        state
            .db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "apk-admin-access",
                refresh_token: "apk-admin-refresh",
                csrf_token: "apk-admin-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();

        let expired_user = create_password_user(
            &state,
            "expired-apk@example.com",
            "expired-private-beta-password",
            now,
        )
        .1
        .user;
        state
            .db
            .create_session(NewSession {
                user_id: expired_user.id,
                installation_id: None,
                access_token: "expired-apk-access",
                refresh_token: "expired-apk-refresh",
                csrf_token: "expired-apk-csrf",
                user_agent: None,
                now_ms: now - 2_000,
                access_expires_ms: now - 1_000,
                refresh_expires_ms: now + 120_000,
            })
            .unwrap();
        let (disabled_tokens, disabled_user) = create_password_user(
            &state,
            "disabled-apk@example.com",
            "disabled-private-beta-password",
            now,
        );
        state
            .db
            .set_user_disabled(disabled_user.user.id, true, now + 10)
            .unwrap();
        let (retired_tokens, retired_user) = create_password_user(
            &state,
            "retired-apk@example.com",
            "retired-private-beta-password",
            now,
        );
        state
            .db
            .set_users_retired_exact(
                &[(retired_user.user.id, retired_user.user.email.as_str())],
                admin.id,
                &admin.email,
                "private-beta download authorization test",
                true,
                now + 11,
            )
            .unwrap();
        let enrollment = state
            .auth
            .create_enrollment(&wearer_current.user, wearer_session_id, now + 12)
            .unwrap();
        let device = state
            .auth
            .exchange_enrollment(
                serde_json::from_value(enrollment_exchange_json(
                    &enrollment.enrollment_token,
                    Uuid::new_v4(),
                    Uuid::new_v4(),
                ))
                .unwrap(),
                "apk-download-route-scope",
                now + 13,
            )
            .unwrap();

        // Namespace replacement after startup cannot substitute bytes for the
        // already verified artifact retained by this process.
        let original_path = config.android_apk_path.with_extension("approved");
        fs::rename(&config.android_apk_path, &original_path).unwrap();
        fs::write(&config.android_apk_path, b"tampered-after-startup").unwrap();

        let denied_headers = [
            ("unauthenticated", None),
            (
                "bearer access token",
                Some("Bearer apk-wearer-access".to_string()),
            ),
            (
                "valid device credential",
                Some(format!("Device {}", device.device_credential)),
            ),
        ];
        for (case, authorization) in denied_headers {
            let mut request = Request::get("/downloads/training-sensors.apk");
            if let Some(authorization) = authorization {
                request = request.header(header::AUTHORIZATION, authorization);
            }
            let denied = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED, "{case}");
            assert_eq!(
                denied.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store, no-transform",
                "{case}"
            );
            assert_eq!(
                denied.headers().get("x-content-type-options").unwrap(),
                "nosniff",
                "{case}"
            );
            let body = axum::body::to_bytes(denied.into_body(), 16 * 1024)
                .await
                .unwrap();
            assert!(
                !body
                    .windows(TEST_ANDROID_APK_BYTES.len())
                    .any(|window| window == TEST_ANDROID_APK_BYTES),
                "{case}"
            );
        }
        for (case, access_token) in [
            ("expired session", "expired-apk-access"),
            ("disabled user", disabled_tokens.access_token.as_str()),
            ("retired user", retired_tokens.access_token.as_str()),
        ] {
            let denied = app
                .clone()
                .oneshot(
                    Request::get("/downloads/training-sensors.apk")
                        .header(header::COOKIE, format!("sensors_access={access_token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED, "{case}");
            assert_eq!(
                denied.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store, no-transform",
                "{case}"
            );
            let body = axum::body::to_bytes(denied.into_body(), 16 * 1024)
                .await
                .unwrap();
            assert!(
                !body
                    .windows(TEST_ANDROID_APK_BYTES.len())
                    .any(|window| window == TEST_ANDROID_APK_BYTES),
                "{case}"
            );
        }

        for access_token in ["apk-wearer-access", "apk-admin-access"] {
            let response = app
                .clone()
                .oneshot(
                    Request::get("/downloads/training-sensors.apk")
                        .header(header::COOKIE, format!("sensors_access={access_token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/vnd.android.package-archive"
            );
            assert_eq!(
                response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
                "attachment; filename=\"training-sensors-0.2.2.apk\""
            );
            assert_eq!(
                response.headers().get(header::CONTENT_LENGTH).unwrap(),
                TEST_ANDROID_APK_BYTES.len().to_string().as_str()
            );
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store, no-transform"
            );
            assert_eq!(response.headers().get(header::PRAGMA).unwrap(), "no-cache");
            assert_eq!(
                response.headers().get(header::REFERRER_POLICY).unwrap(),
                "no-referrer"
            );
            assert_eq!(
                response.headers().get("x-content-type-options").unwrap(),
                "nosniff"
            );
            assert_eq!(response.headers().get("x-frame-options").unwrap(), "DENY");
            assert_eq!(response.headers().get("accept-ranges").unwrap(), "none");
            assert_eq!(
                response.headers().get("content-security-policy").unwrap(),
                "default-src 'none'; sandbox"
            );
            assert_eq!(
                response.headers().get("x-checksum-sha256").unwrap(),
                config.android_apk_sha256.as_str()
            );
            let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(body.as_ref(), TEST_ANDROID_APK_BYTES);
        }

        let head = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/downloads/training-sensors.apk")
                    .header(header::COOKIE, "sensors_access=apk-wearer-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        assert_eq!(
            head.headers().get(header::CONTENT_LENGTH).unwrap(),
            TEST_ANDROID_APK_BYTES.len().to_string().as_str()
        );
        assert_eq!(
            head.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/vnd.android.package-archive"
        );
        assert_eq!(
            head.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store, no-transform"
        );
        assert_eq!(head.headers().get("accept-ranges").unwrap(), "none");
        assert!(
            axum::body::to_bytes(head.into_body(), 1024)
                .await
                .unwrap()
                .is_empty()
        );

        // Ranges are deliberately unsupported for this small private-beta
        // artifact: a Range request receives one complete, verified 200 body.
        let ranged = app
            .clone()
            .oneshot(
                Request::get("/downloads/training-sensors.apk")
                    .header(header::COOKIE, "sensors_access=apk-wearer-access")
                    .header(header::RANGE, "bytes=0-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ranged.status(), StatusCode::OK);
        assert_eq!(ranged.headers().get("accept-ranges").unwrap(), "none");
        let ranged = axum::body::to_bytes(ranged.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(ranged.as_ref(), TEST_ANDROID_APK_BYTES);

        let home = app
            .oneshot(
                Request::get("/")
                    .header(header::COOKIE, "sensors_access=apk-wearer-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(home.status(), StatusCode::OK);
        let home = axum::body::to_bytes(home.into_body(), 256 * 1024)
            .await
            .unwrap();
        let home = String::from_utf8(home.to_vec()).unwrap();
        let download_position = home.find("id=downloadAndroidApp").unwrap();
        let phone_position = home.find("id=connectThisPhone").unwrap();
        let qr_position = home.find("id=showEnrollmentQr").unwrap();
        assert!(download_position < phone_position && phone_position < qr_position);
        assert!(home.contains("/downloads/training-sensors.apk"));
        assert!(home.contains("Version 0.2.2 (code 4)"));
        assert!(home.contains(&config.android_apk_sha256));
        assert!(home.contains("allow this browser to install unknown apps"));
        assert!(home.contains("After installation, return to this page and continue to step 2"));
        assert!(home.contains("Setting up a phone from this computer?"));
        assert!(home.contains("scan the code with the phone camera"));
        assert!(!home.contains("/downloads/training-sensors.apk?"));
        assert!(!home.contains("/downloads/training-sensors.apk#"));
        assert!(!home.contains("device_credential"));
    }

    fn write_canonical_batch(config: &CloudConfig, email: &str) -> (User, IngestBatchRequest) {
        let (_, state) = build_router_and_state(config).unwrap();
        let now = unix_ms();
        let (_, current) = create_password_user(&state, email, "replay-password", now);
        let request = valid_ingest_request(Uuid::new_v4(), now + 3);
        state
            .db
            .insert_test_device_credential(
                current.user.id,
                request.installation_id,
                Uuid::new_v4(),
                "reconciliation-test-device-credential",
                now + 2,
            )
            .unwrap();
        let encoded = serde_json::to_vec(&request).unwrap();
        let checksum = hex::encode(sha2::Sha256::digest(&encoded));
        state
            .ingest
            .ingest(
                &current.user,
                request.installation_id,
                request.clone(),
                checksum,
                encoded.len(),
            )
            .unwrap();
        (current.user, request)
    }

    fn delete_canonical_receipt(config: &CloudConfig, user: &User, request: &IngestBatchRequest) {
        let db = CloudDb::open(&config.db_path).unwrap();
        db.delete_batch_receipts(user.id, request.installation_id, request.batch_id)
            .unwrap();
    }

    #[tokio::test]
    async fn retired_existing_wal_parents_recover_receipt_and_survive_two_cold_starts() {
        let retained_dir = TestDir::new();
        let retained_config = test_config(&retained_dir);
        let (user, request) = write_canonical_batch(&retained_config, "retained@example.com");
        delete_canonical_receipt(&retained_config, &user, &request);
        let retained_installation = {
            let db = CloudDb::open(&retained_config.db_path).unwrap();
            let protected = db.user_by_email("admin@example.com").unwrap().unwrap();
            assert_eq!(
                db.set_users_retired_exact(
                    &[(user.id, user.email.as_str())],
                    protected.id,
                    &protected.email,
                    "historical replay fixture",
                    true,
                    unix_ms(),
                )
                .unwrap(),
                1
            );
            drop(db);
            rusqlite::Connection::open(&retained_config.db_path)
                .unwrap()
                .query_row(
                    "SELECT i.last_seen_unix_ms,i.revoked_unix_ms,c.last_used_unix_ms,c.revoked_unix_ms \
                     FROM installations i JOIN device_credentials c ON c.installation_id=i.id \
                     WHERE i.id=?1",
                    [request.installation_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                        ))
                    },
                )
                .unwrap()
        };
        for cold_start in 1..=2 {
            let (_, state) = build_router_and_state(&retained_config).unwrap();
            assert!(
                state
                    .db
                    .stored_batch(user.id, request.installation_id, request.batch_id)
                    .unwrap()
                    .is_some(),
                "receipt missing after cold start {cold_start}"
            );
            let installation = rusqlite::Connection::open(&retained_config.db_path)
                .unwrap()
                .query_row(
                    "SELECT i.last_seen_unix_ms,i.revoked_unix_ms,c.last_used_unix_ms,c.revoked_unix_ms \
                     FROM installations i JOIN device_credentials c ON c.installation_id=i.id \
                     WHERE i.id=?1",
                    [request.installation_id.to_string()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(installation, retained_installation);
            state.db.quick_check().unwrap();
        }
    }

    #[tokio::test]
    async fn missing_wal_user_parent_fails_cold_start_without_recreating_it() {
        let missing_dir = TestDir::new();
        let missing_config = test_config(&missing_dir);
        let (missing_user, missing_request) =
            write_canonical_batch(&missing_config, "missing-user-parent@example.com");
        delete_canonical_receipt(&missing_config, &missing_user, &missing_request);
        {
            let connection = rusqlite::Connection::open(&missing_config.db_path).unwrap();
            connection
                .pragma_update(None, "foreign_keys", "OFF")
                .unwrap();
            connection
                .execute(
                    "DELETE FROM users WHERE id=?1",
                    [missing_user.id.to_string()],
                )
                .unwrap();
        }
        let error = match build_router_and_state(&missing_config) {
            Ok(_) => panic!("missing WAL user parent must prevent startup"),
            Err(error) => error,
        };
        let detail = format!("{error:#}");
        assert!(
            detail.contains(&format!(
                "WAL reconciliation parent user {} is missing",
                missing_user.id
            )),
            "unexpected missing-user error: {detail}"
        );
        let connection = rusqlite::Connection::open(&missing_config.db_path).unwrap();
        let users: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM users WHERE id=?1",
                [missing_user.id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(users, 0);
    }

    #[tokio::test]
    async fn missing_wal_installation_parent_fails_cold_start_without_recreating_it() {
        let missing_dir = TestDir::new();
        let missing_config = test_config(&missing_dir);
        let (user, request) =
            write_canonical_batch(&missing_config, "missing-installation-parent@example.com");
        delete_canonical_receipt(&missing_config, &user, &request);
        {
            let connection = rusqlite::Connection::open(&missing_config.db_path).unwrap();
            connection
                .pragma_update(None, "foreign_keys", "OFF")
                .unwrap();
            connection
                .execute(
                    "DELETE FROM installations WHERE id=?1",
                    [request.installation_id.to_string()],
                )
                .unwrap();
        }
        let error = match build_router_and_state(&missing_config) {
            Ok(_) => panic!("missing WAL installation parent must prevent startup"),
            Err(error) => error,
        };
        let detail = format!("{error:#}");
        assert!(
            detail.contains(&format!(
                "WAL reconciliation parent installation {} is missing",
                request.installation_id
            )),
            "unexpected missing-installation error: {detail}"
        );
        let connection = rusqlite::Connection::open(&missing_config.db_path).unwrap();
        let installations: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM installations WHERE id=?1",
                [request.installation_id.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(installations, 0);
    }
    #[tokio::test]
    async fn browser_session_devices_group_rotations_and_track_the_latest_user_agent() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, current) = create_password_user(
            &state,
            "grouping@example.com",
            "rotation-grouping-password",
            now,
        );
        // Retire the registration-time family so exactly one device remains.
        assert!(
            state
                .db
                .revoke_session_family(current.session_id, now + 3)
                .unwrap()
        );

        let login_ms = now + 10;
        let first = state
            .auth
            .login_password(
                PasswordLoginRequest {
                    email: "grouping@example.com".to_string(),
                    password: "rotation-grouping-password".to_string(),
                },
                "grouping-client",
                Some("Mozilla/5.0 (Macintosh) Chrome/126 Safari/537.36"),
                login_ms,
            )
            .unwrap();
        let second = state
            .auth
            .refresh(
                RefreshRequest {
                    refresh_token: first.refresh_token.clone(),
                    rotation_id: Uuid::new_v4(),
                },
                Some("Mozilla/5.0 (Macintosh) Chrome/127 Safari/537.36"),
                now + 20,
            )
            .unwrap();
        let last_rotation_ms = now + 30;
        let final_user_agent = "Mozilla/5.0 (X11; Linux x86_64) Firefox/128.0";
        let third = state
            .auth
            .refresh(
                RefreshRequest {
                    refresh_token: second.refresh_token.clone(),
                    rotation_id: Uuid::new_v4(),
                },
                Some(final_user_agent),
                last_rotation_ms,
            )
            .unwrap();

        let response = app
            .oneshot(
                Request::get(format!("/api/v1/users/{}/devices", current.user.id))
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", third.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let devices = response_json(response).await;
        let sessions = devices["browser_sessions"].as_array().unwrap();
        assert_eq!(
            sessions.len(),
            1,
            "one rotation family must collapse to one device row: {devices}"
        );
        assert_eq!(sessions[0]["created_unix_ms"], json!(login_ms));
        assert_eq!(sessions[0]["last_active_unix_ms"], json!(last_rotation_ms));
        assert_eq!(sessions[0]["user_agent"], json!(final_user_agent));
        assert_eq!(sessions[0]["current"], json!(true));
        assert_eq!(devices["installations"], json!([]));
    }

    #[tokio::test]
    async fn your_devices_view_is_scoped_to_self_or_admin_and_leaks_no_secrets() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser_a, current_a) =
            create_password_user(&state, "owner-a@example.com", "scope-a-password", now);
        let (browser_b, current_b) =
            create_password_user(&state, "owner-b@example.com", "scope-b-password", now);
        let installation_b = Uuid::new_v4();
        let enrollment_b = state
            .auth
            .create_enrollment(&current_b.user, current_b.session_id, now + 3)
            .unwrap();
        let credential_b = state
            .auth
            .exchange_enrollment(
                serde_json::from_value(enrollment_exchange_json(
                    &enrollment_b.enrollment_token,
                    installation_b,
                    Uuid::new_v4(),
                ))
                .unwrap(),
                "scope-b-phone",
                now + 4,
            )
            .unwrap();
        let family_b = state
            .db
            .session_family_root(current_b.session_id)
            .unwrap()
            .unwrap();
        state
            .db
            .create_session(NewSession {
                user_id: state
                    .db
                    .user_by_email("admin@example.com")
                    .unwrap()
                    .unwrap()
                    .id,
                installation_id: None,
                access_token: "your-devices-admin-access",
                refresh_token: "your-devices-admin-refresh",
                csrf_token: "your-devices-admin-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();

        // Cross-user listing is forbidden for non-admins.
        let cross_list = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/devices", current_b.user.id))
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser_a.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_list.status(), StatusCode::FORBIDDEN);

        // A's mutation on B's path is forbidden before any DB lookup.
        let cross_path = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/api/v1/users/{}/sessions/{family_b}/revoke",
                    current_b.user.id
                ))
                .header(
                    header::COOKIE,
                    format!(
                        "sensors_access={}; sensors_csrf={}",
                        browser_a.access_token, browser_a.csrf_token
                    ),
                )
                .header(header::ORIGIN, &config.public_origin)
                .header("x-csrf-token", &browser_a.csrf_token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_path.status(), StatusCode::FORBIDDEN);

        // B's identifiers under A's own path 404 without touching B.
        for path in [
            format!(
                "/api/v1/users/{}/sessions/{family_b}/revoke",
                current_a.user.id
            ),
            format!(
                "/api/v1/users/{}/installations/{installation_b}/revoke",
                current_a.user.id
            ),
        ] {
            let smuggled = app
                .clone()
                .oneshot(
                    Request::post(&path)
                        .header(
                            header::COOKIE,
                            format!(
                                "sensors_access={}; sensors_csrf={}",
                                browser_a.access_token, browser_a.csrf_token
                            ),
                        )
                        .header(header::ORIGIN, &config.public_origin)
                        .header("x-csrf-token", &browser_a.csrf_token)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(smuggled.status(), StatusCode::NOT_FOUND, "{path}");
            assert_eq!(response_json(smuggled).await["code"], "not_found", "{path}");
        }
        assert!(
            state
                .db
                .authenticate_access(&browser_b.access_token, unix_ms())
                .unwrap()
                .is_some(),
            "failed cross-user revocations must not sign B out"
        );

        // The admin sees B's devices (never marked current) without secrets.
        let admin_list = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/devices", current_b.user.id))
                    .header(header::COOKIE, "sensors_access=your-devices-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(admin_list.status(), StatusCode::OK);
        let admin_view = response_json(admin_list).await;
        let sessions = admin_view["browser_sessions"].as_array().unwrap();
        assert!(!sessions.is_empty());
        assert!(
            sessions
                .iter()
                .all(|session| session["current"] == json!(false))
        );
        assert_eq!(
            admin_view["installations"][0]["id"],
            json!(installation_b.to_string())
        );
        let admin_text = admin_view.to_string();
        assert!(!admin_text.contains("hash"));
        assert!(!admin_text.contains(&browser_b.access_token));
        assert!(!admin_text.contains(&browser_b.refresh_token));
        assert!(!admin_text.contains(&browser_b.csrf_token));
        assert!(!admin_text.contains(&credential_b.device_credential));

        // The admin can revoke both device kinds for any user.
        for path in [
            format!(
                "/api/v1/users/{}/sessions/{family_b}/revoke",
                current_b.user.id
            ),
            format!(
                "/api/v1/users/{}/installations/{installation_b}/revoke",
                current_b.user.id
            ),
        ] {
            let revoked = app
                .clone()
                .oneshot(
                    Request::post(&path)
                        .header(
                            header::COOKIE,
                            "sensors_access=your-devices-admin-access; \
                             sensors_csrf=your-devices-admin-csrf",
                        )
                        .header(header::ORIGIN, &config.public_origin)
                        .header("x-csrf-token", "your-devices-admin-csrf")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(revoked.status(), StatusCode::OK, "{path}");
            assert_eq!(response_json(revoked).await["ok"], json!(true), "{path}");
        }
        assert!(
            state
                .db
                .authenticate_access(&browser_b.access_token, unix_ms())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn session_family_revoke_signs_out_that_browser_and_only_that_browser() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser_first, current) = create_password_user(
            &state,
            "two-browsers@example.com",
            "family-revoke-password",
            now,
        );
        let family_first = state
            .db
            .session_family_root(current.session_id)
            .unwrap()
            .unwrap();
        let browser_second = state
            .auth
            .login_password(
                PasswordLoginRequest {
                    email: "two-browsers@example.com".to_string(),
                    password: "family-revoke-password".to_string(),
                },
                "second-browser-client",
                Some("Mozilla/5.0 (Windows NT 10.0) Edg/126.0"),
                now + 5,
            )
            .unwrap();
        let family_second = state
            .db
            .session_family_root(
                state
                    .db
                    .authenticate_access(&browser_second.access_token, now + 6)
                    .unwrap()
                    .unwrap()
                    .session_id,
            )
            .unwrap()
            .unwrap();

        let listed = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/devices", current.user.id))
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser_first.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed = response_json(listed).await;
        let sessions = listed["browser_sessions"].as_array().unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(
            sessions[0]["family_id"],
            json!(family_second.to_string()),
            "newest activity must sort first: {listed}"
        );
        assert_eq!(sessions[0]["current"], json!(false));
        assert_eq!(sessions[1]["family_id"], json!(family_first.to_string()));
        assert_eq!(sessions[1]["current"], json!(true));

        let revoke = |family: Uuid| {
            let app = app.clone();
            let path = format!("/api/v1/users/{}/sessions/{family}/revoke", current.user.id);
            let cookies = format!(
                "sensors_access={}; sensors_csrf={}",
                browser_first.access_token, browser_first.csrf_token
            );
            let csrf = browser_first.csrf_token.clone();
            let origin = config.public_origin.clone();
            async move {
                app.oneshot(
                    Request::post(&path)
                        .header(header::COOKIE, cookies)
                        .header(header::ORIGIN, origin)
                        .header("x-csrf-token", csrf)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap()
            }
        };

        let other_revoked = revoke(family_second).await;
        assert_eq!(other_revoked.status(), StatusCode::OK);
        assert_eq!(response_json(other_revoked).await["ok"], json!(true));
        let second_denied = app
            .clone()
            .oneshot(
                Request::get("/api/v1/me")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser_second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_denied.status(), StatusCode::UNAUTHORIZED);
        let first_still_signed_in = app
            .clone()
            .oneshot(
                Request::get("/api/v1/me")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser_first.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_still_signed_in.status(), StatusCode::OK);

        // Revoking your own current family is a normal sign-out.
        let own_revoked = revoke(family_first).await;
        assert_eq!(own_revoked.status(), StatusCode::OK);
        let first_denied = app
            .oneshot(
                Request::get("/api/v1/me")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser_first.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_denied.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn installation_revoke_disconnects_the_phone_with_the_established_reconnect_error() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser, current) =
            create_password_user(&state, "lost-phone@example.com", "lost-phone-password", now);
        let installation_id = Uuid::new_v4();
        let enrollment = state
            .auth
            .create_enrollment(&current.user, current.session_id, now + 3)
            .unwrap();
        let credential = state
            .auth
            .exchange_enrollment(
                serde_json::from_value(enrollment_exchange_json(
                    &enrollment.enrollment_token,
                    installation_id,
                    Uuid::new_v4(),
                ))
                .unwrap(),
                "lost-phone-client",
                now + 4,
            )
            .unwrap();
        let ingest = valid_ingest_request(installation_id, now + 5);
        let uploaded = app
            .clone()
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        format!("Device {}", credential.device_credential),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(uploaded.status(), StatusCode::OK);

        let listed = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/users/{}/devices", current.user.id))
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed = response_json(listed).await;
        assert_eq!(
            listed["installations"][0]["id"],
            json!(installation_id.to_string())
        );
        assert!(
            listed["installations"][0]["last_seen_unix_ms"]
                .as_i64()
                .unwrap()
                >= now,
            "the accepted upload must advance last_seen: {listed}"
        );
        assert_eq!(listed["installations"][0]["revoked_unix_ms"], json!(null));

        let revoked = app
            .clone()
            .oneshot(
                Request::post(format!(
                    "/api/v1/users/{}/installations/{installation_id}/revoke",
                    current.user.id
                ))
                .header(
                    header::COOKIE,
                    format!(
                        "sensors_access={}; sensors_csrf={}",
                        browser.access_token, browser.csrf_token
                    ),
                )
                .header(header::ORIGIN, &config.public_origin)
                .header("x-csrf-token", &browser.csrf_token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(revoked.status(), StatusCode::OK);
        assert_eq!(response_json(revoked).await["ok"], json!(true));

        let denied = app
            .oneshot(
                Request::post("/api/v1/ingest/batches")
                    .header(
                        header::AUTHORIZATION,
                        format!("Device {}", credential.device_credential),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&ingest).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(denied).await["code"],
            "device_reconnect_required"
        );
    }

    #[tokio::test]
    async fn login_user_agent_is_stored_truncated_and_stripped_of_control_characters() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (_, current) =
            create_password_user(&state, "user-agent@example.com", "user-agent-password", now);
        let login = |user_agent: String| {
            let app = app.clone();
            let origin = config.public_origin.clone();
            async move {
                app.oneshot(
                    Request::post("/api/v1/auth/password/login")
                        .header(header::ORIGIN, origin)
                        .header(header::USER_AGENT, user_agent)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&json!({
                                "email": "user-agent@example.com",
                                "password": "user-agent-password",
                            }))
                            .unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap()
            }
        };

        let oversized = login("a".repeat(1_000)).await;
        assert_eq!(oversized.status(), StatusCode::OK);
        let tabbed = login("Tab\tSeparated Agent".to_string()).await;
        assert_eq!(tabbed.status(), StatusCode::OK);

        let devices = state
            .db
            .list_browser_session_devices(current.user.id, unix_ms())
            .unwrap();
        let stored: Vec<&str> = devices
            .iter()
            .filter_map(|device| device.user_agent.as_deref())
            .collect();
        assert!(
            stored.contains(&"a".repeat(256).as_str()),
            "oversized user agents must be stored truncated to 256 bytes: {stored:?}"
        );
        assert!(
            stored.contains(&"TabSeparated Agent"),
            "control characters must be stripped: {stored:?}"
        );
        assert!(stored.iter().all(|value| value.len() <= 256));
    }

    #[tokio::test]
    async fn home_and_admin_pages_pin_the_your_devices_ui() {
        let dir = TestDir::new();
        let config = test_config(&dir);
        let (app, state) = build_router_and_state(&config).unwrap();
        let now = unix_ms();
        let (browser, current) = create_password_user(
            &state,
            "devices-page@example.com",
            "devices-page-password",
            now,
        );
        let home = app
            .clone()
            .oneshot(
                Request::get("/")
                    .header(
                        header::COOKIE,
                        format!("sensors_access={}", browser.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(home.status(), StatusCode::OK);
        let home = axum::body::to_bytes(home.into_body(), 256 * 1024)
            .await
            .unwrap();
        let home = String::from_utf8(home.to_vec()).unwrap();
        assert!(home.contains("Your devices"));
        assert!(home.contains(&format!(
            "id=devices class=muted data-user=\"{}\"",
            current.user.id
        )));
        assert!(home.contains("sensorsLoadDevices()"));
        assert!(HOME_DEVICES_JS.contains("Disconnect app"));
        assert!(HOME_DEVICES_JS.contains("(this browser)"));
        assert!(HOME_DEVICES_JS.contains("createElement"));
        assert!(
            !HOME_DEVICES_JS.contains("innerHTML"),
            "untrusted device labels must never be rendered as HTML"
        );

        state
            .db
            .create_session(NewSession {
                user_id: state
                    .db
                    .user_by_email("admin@example.com")
                    .unwrap()
                    .unwrap()
                    .id,
                installation_id: None,
                access_token: "devices-page-admin-access",
                refresh_token: "devices-page-admin-refresh",
                csrf_token: "devices-page-admin-csrf",
                user_agent: None,
                now_ms: now,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let admin = app
            .oneshot(
                Request::get("/admin")
                    .header(header::COOKIE, "sensors_access=devices-page-admin-access")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(admin.status(), StatusCode::OK);
        let admin = axum::body::to_bytes(admin.into_body(), 256 * 1024)
            .await
            .unwrap();
        let admin = String::from_utf8(admin.to_vec()).unwrap();
        assert!(admin.contains("<div class=sessions>"));
        assert!(admin.contains("Sign out device"));
        assert!(admin.contains("loadBrowserSessions()"));
    }
}
