use crate::config::{BATTERY_LEVEL_UUID, HR_MEASUREMENT_UUID, PMD_CONTROL_UUID, PMD_DATA_UUID};
use crate::hr::{HrData, handle_hr};
use crate::pmd::{PmdParser, measurement_name};
use crate::process_lock::SourceWriterLock;
use crate::records::{PmdFrameTiming, RECORD_SCHEMA_VERSION, RecordMeta};
use crate::sink::{H10_WAL_SOURCE, RecordSink, WAL_PARTICIPANT_ID};
use crate::wal::{
    ManualMarkerBody, RecoveryEventBody, SegmentClosedBody, SegmentHeader, WAL_FRAME_HEADER_LEN,
    WAL_HEADER_LEN, WAL_MAX_PAYLOAD_LEN, WalBody, WalFrameRecord, WalPathConfig, WalReadMode,
    WalRecord, WalRecordContext, WalRecoveryReport, WalSegmentManager, WalSegmentManagerOptions,
    WalTailIssue, WalWriterOptions, crc32c, decode_record_payload, format_date_partition,
    logical_wal_records, recover_stale_open_segments,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{MissedTickBehavior, interval};
use uuid::Uuid;

pub const MANUAL_EVENTS_SOURCE: &str = "manual_events";
pub const REPLAY_H10_SOURCE: &str = "replay_h10";
const LIVE_WRITER_ID: &str = "training_sensors live";
const DEFAULT_LIVE_WINDOW: Duration = Duration::from_secs(20);
const DEFAULT_HISTORY_WINDOW: Duration = Duration::from_secs(10 * 60 * 60);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);
const ECG_LIMIT: usize = 2_000;
// 200 Hz ACC: 2_600 samples is ~13 s, enough to always fill the 10 s live sweep
// window (10 s = 2_000 samples) with margin, mirroring the ECG buffer's headroom.
const ACC_LIMIT: usize = 2_600;
const HR_LIMIT: usize = 300;
const RR_LIMIT: usize = 1_000;
const BATTERY_LIMIT: usize = 20;
// Battery-low banner latch (docs/battery_low_banner_20260612.md): a reading at or
// below the trigger trips the banner; only a later reading at or above the clear
// threshold (fresh CR2025 reads 100%) clears it. Aging-cell rest rebounds top out
// well below 90%, so they intentionally do not clear the banner.
const BATTERY_LOW_TRIGGER_PERCENT: u8 = 30;
const BATTERY_REPLACED_CLEAR_PERCENT: u8 = 90;
const HEALTH_LIMIT: usize = 1_000;
const TIMING_LIMIT: usize = 2_000;
const EVENT_LIMIT: usize = 2_000;
const DELTA_LIMIT: usize = 20_000;
const SSE_BATCH_LIMIT: usize = 4_000;
const SSE_HEARTBEAT_TICKS: u64 = 10;
const HTTP_READ_LIMIT: usize = 256 * 1024;
const WAL_TS_FILENAME_LEN: usize = 26;
const LIVE_INDEX_SCHEMA_VERSION: u16 = 1;
const LIVE_INDEX_MIN_INTERVAL_NS: i64 = 1_000_000_000;
const PMD_HISTORY_GAP_THRESHOLD_MS: f64 = 20.0;
const EPOCH_2000_UNIX_NS_I128: i128 = 946_684_800_000_000_000;
const PHONE_OFFLINE_AFTER_NS: u128 = 15_000_000_000;
const UPLOAD_ACTIVE_AFTER_NS: u128 = 10_000_000_000;
const LIVE_FUTURE_TOLERANCE_NS: i64 = 5_000_000_000;

#[derive(Clone, Debug)]
pub struct LiveServerConfig {
    pub data_dir: PathBuf,
    pub participant_id: String,
    pub port: u16,
    pub live_window: Duration,
    pub history_window: Duration,
    pub poll_interval: Duration,
    pub enable_markers: bool,
}

impl LiveServerConfig {
    pub fn new(data_dir: PathBuf, port: u16) -> Self {
        Self {
            data_dir,
            participant_id: WAL_PARTICIPANT_ID.to_string(),
            port,
            live_window: DEFAULT_LIVE_WINDOW,
            history_window: DEFAULT_HISTORY_WINDOW,
            poll_interval: DEFAULT_POLL_INTERVAL,
            enable_markers: true,
        }
    }
}

pub async fn run(config: LiveServerConfig) -> Result<()> {
    if config.live_window < Duration::from_secs(12) {
        bail!(
            "--live-window-seconds must be at least 12 so the 10s ECG sweep has H10 batching margin"
        );
    }
    if config.history_window < config.live_window {
        bail!("--history-window-hours must cover at least the live window");
    }
    let bind_addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .with_context(|| format!("bind live dashboard server on {bind_addr}"))?;
    let actual_addr = listener
        .local_addr()
        .context("read live server bind address")?;
    let runtime = Arc::new(Mutex::new(LiveRuntime::new(config.clone())?));
    start_tailer_task(runtime.clone(), config.clone());
    println!(
        "live dashboard listening at http://{} using WAL under {}",
        actual_addr,
        config.data_dir.display()
    );
    start_history_task(runtime.clone(), config.clone());

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _) = accept.context("accept live dashboard connection")?;
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, runtime).await {
                        eprintln!("live dashboard connection failed: {err:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("wait for Ctrl-C")?;
                if let Ok(mut runtime) = runtime.lock()
                    && let Err(err) = runtime.shutdown()
                {
                    eprintln!("live dashboard manual-events shutdown failed: {err:#}");
                }
                break;
            }
        }
    }
    Ok(())
}

/// Start the same bounded tail/history workers used by the local live server.
/// Cloud runtime registries call this once for every tenant so status/history is
/// restored after restart without waiting for a browser request.
pub(crate) fn start_runtime_tasks(runtime: Arc<Mutex<LiveRuntime>>, config: LiveServerConfig) {
    start_tailer_task(runtime.clone(), config.clone());
    start_history_task(runtime, config);
}

fn start_tailer_task(runtime: Arc<Mutex<LiveRuntime>>, config: LiveServerConfig) {
    tokio::spawn(async move {
        let mut tick = interval(config.poll_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let poll_result = runtime
                .lock()
                .map_err(|_| anyhow!("live runtime lock poisoned"))
                .and_then(|mut runtime| runtime.poll_once());
            if let Err(err) = poll_result {
                eprintln!("live WAL tail poll failed: {err:#}");
            }
        }
    });
}

fn start_history_task(runtime: Arc<Mutex<LiveRuntime>>, config: LiveServerConfig) {
    if let Ok(mut runtime) = runtime.lock() {
        runtime.mark_history_running();
    }
    tokio::spawn(async move {
        let scan_config = config.clone();
        let scan_result = tokio::task::spawn_blocking(move || scan_history(scan_config)).await;
        match scan_result {
            Ok(Ok(result)) => {
                if let Ok(mut runtime) = runtime.lock() {
                    runtime.apply_history_result(result);
                }
            }
            Ok(Err(err)) => {
                if let Ok(mut runtime) = runtime.lock() {
                    runtime.mark_history_error(format!("{err:#}"));
                }
                eprintln!("live history scan failed: {err:#}");
            }
            Err(err) => {
                if let Ok(mut runtime) = runtime.lock() {
                    runtime.mark_history_error(format!("history task join error: {err}"));
                }
                eprintln!("live history scan join failed: {err}");
            }
        }
    });
}

#[derive(Debug)]
pub(crate) struct LiveRuntime {
    stream_id: Uuid,
    config: LiveServerConfig,
    state: LiveState,
    tailer: WalTailer,
    marker_writer: Option<ManualMarkerWriter>,
    marker_status: String,
    critical: CriticalLoadMetrics,
    history: HistoryLoadMetrics,
    index: IndexBuildMetrics,
    sse: SseMetrics,
}

impl LiveRuntime {
    pub(crate) fn new(config: LiveServerConfig) -> Result<Self> {
        Self::new_at(config, unix_ns_i64_lossy())
    }

    fn new_at(config: LiveServerConfig, now_unix_ns: i64) -> Result<Self> {
        Self::new_at_with_stream_id(config, now_unix_ns, Uuid::new_v4())
    }

    fn new_at_with_stream_id(
        config: LiveServerConfig,
        now_unix_ns: i64,
        stream_id: Uuid,
    ) -> Result<Self> {
        let (marker_writer, marker_status, _recovery_reports) = if config.enable_markers {
            match ManualMarkerWriter::try_new(&config.data_dir, &config.participant_id)? {
                Some((writer, reports)) => {
                    let status = if reports.is_empty() {
                        "enabled".to_string()
                    } else {
                        format!(
                            "enabled; recovered {} stale manual WAL item(s)",
                            reports.len()
                        )
                    };
                    (Some(writer), status, reports)
                }
                None => (
                    None,
                    "read-only: manual-events writer lock is held by another process".to_string(),
                    Vec::new(),
                ),
            }
        } else {
            (None, "disabled by configuration".to_string(), Vec::new())
        };

        let mut runtime = Self {
            stream_id,
            config,
            state: LiveState::default(),
            tailer: WalTailer::default(),
            marker_writer,
            marker_status,
            critical: CriticalLoadMetrics::default(),
            history: HistoryLoadMetrics::not_started(),
            index: IndexBuildMetrics::default(),
            sse: SseMetrics::default(),
        };
        runtime.load_critical_window_at(now_unix_ns)?;
        Ok(runtime)
    }

    fn load_critical_window_at(&mut self, now_unix_ns: i64) -> Result<usize> {
        let start = Instant::now();
        let live_cutoff = cutoff_ns(now_unix_ns, self.config.live_window);
        let result = self.tailer.load_critical_window(
            &self.config.data_dir,
            &self.config.participant_id,
            CaptureTimeWindow {
                cutoff_unix_ns: live_cutoff,
                now_unix_ns,
            },
            &mut self.state,
            &mut self.index,
        );
        self.critical.elapsed_ms = Some(duration_millis_u64(start.elapsed()));
        match result {
            Ok(outcome) => {
                self.critical.records_processed = outcome.records_processed;
                self.critical.index_state = outcome.index_state;
                self.critical.start_offset = outcome.start_offset;
                self.critical.start_path = outcome.start_path;
                Ok(usize::try_from(outcome.records_processed).unwrap_or(usize::MAX))
            }
            Err(err) => {
                self.critical.index_state = "error".to_string();
                self.critical.last_error = Some(format!("{err:#}"));
                Err(err)
            }
        }
    }

    pub(crate) fn poll_once(&mut self) -> Result<usize> {
        let now = unix_ns_i64_lossy();
        let live_cutoff = cutoff_ns(now, self.config.live_window);
        let event_cutoff = cutoff_ns(now, self.config.history_window);
        self.state.trim_before(
            u128::try_from(live_cutoff).unwrap_or(0),
            u128::try_from(event_cutoff).unwrap_or(0),
        );
        self.tailer.poll_live_once(
            &self.config.data_dir,
            &self.config.participant_id,
            CaptureTimeWindow {
                cutoff_unix_ns: live_cutoff,
                now_unix_ns: now,
            },
            &mut self.state,
            &mut self.index,
            &[H10_WAL_SOURCE, REPLAY_H10_SOURCE, MANUAL_EVENTS_SOURCE],
        )
    }

    pub(crate) fn snapshot(&self) -> DashboardSnapshot {
        self.state.snapshot(self.status())
    }

    pub(crate) fn delta_batch_after(&self, after_seq: u64, max_items: usize) -> LiveDeltaBatch {
        self.state
            .delta_batch_after(after_seq, max_items, self.status())
    }

    pub(crate) fn status(&self) -> LiveStatus {
        self.state.status(LiveStatusContext {
            stream_id: self.stream_id,
            config: &self.config,
            marker_enabled: self.marker_writer.is_some(),
            marker_status: &self.marker_status,
            tailer: &self.tailer,
            critical: &self.critical,
            history: &self.history,
            index: &self.index,
            sse: &self.sse,
        })
    }

    fn mark_history_running(&mut self) {
        self.history.mark_running();
    }

    fn mark_history_error(&mut self, error: String) {
        self.history.mark_error(error);
    }

    fn apply_history_result(&mut self, mut result: HistoryScanResult) {
        self.tailer
            .parser
            .timezone_offsets
            .extend(result.timezone_offsets.clone());
        self.state
            .connection_timezone_offsets
            .extend(result.timezone_offsets);
        truncate_to_newest_events(&mut result.events, EVENT_LIMIT);
        for event in &result.events {
            self.state.phone.note_event(
                event.collector_unix_ns,
                &event.event,
                &event.detail,
                &event.fields,
            );
        }
        self.state.merge_history_events(result.events);
        self.history.apply_complete(result.metrics);
        self.index.merge(result.index);
    }

    pub(crate) fn sse_client_connected(&mut self) {
        self.sse.connected_clients = self.sse.connected_clients.saturating_add(1);
    }

    pub(crate) fn sse_client_disconnected(&mut self) {
        self.sse.connected_clients = self.sse.connected_clients.saturating_sub(1);
    }

    pub(crate) fn note_sse_batch(&mut self, rows: usize, overflow: bool) {
        self.sse.batches_sent = self.sse.batches_sent.saturating_add(1);
        self.sse.rows_sent = self
            .sse
            .rows_sent
            .saturating_add(u64::try_from(rows).unwrap_or(u64::MAX));
        self.sse.last_batch_size = rows;
        if overflow {
            self.sse.overflow_resync_count = self.sse.overflow_resync_count.saturating_add(1);
        }
    }

    pub(crate) fn note_sse_write_error(&mut self, error: String) {
        self.sse.last_write_error = Some(error);
    }

    pub(crate) fn post_marker(&mut self, request: MarkerPostRequest) -> Result<MarkerPostResponse> {
        let writer = self
            .marker_writer
            .as_mut()
            .ok_or_else(|| anyhow!("marker posting unavailable: {}", self.marker_status))?;
        writer.append_marker(request)
    }

    fn shutdown(&mut self) -> Result<()> {
        if let Some(writer) = self.marker_writer.as_mut() {
            writer.close("writer_shutdown")?;
        }
        Ok(())
    }
}

fn cutoff_ns(now_unix_ns: i64, window: Duration) -> i64 {
    let window_ns = i64::try_from(window.as_nanos()).unwrap_or(i64::MAX);
    now_unix_ns.saturating_sub(window_ns)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
struct CriticalLoadMetrics {
    elapsed_ms: Option<u64>,
    records_processed: u64,
    index_state: String,
    start_offset: Option<u64>,
    start_path: Option<String>,
    last_error: Option<String>,
}

impl Default for CriticalLoadMetrics {
    fn default() -> Self {
        Self {
            elapsed_ms: None,
            records_processed: 0,
            index_state: "not_started".to_string(),
            start_offset: None,
            start_path: None,
            last_error: None,
        }
    }
}

#[derive(Clone, Debug)]
struct HistoryLoadMetrics {
    state: String,
    started_at: Option<Instant>,
    elapsed_ms: Option<u64>,
    records_scanned: u64,
    records_matched: u64,
    oldest_loaded_unix_ns: Option<i64>,
    newest_loaded_unix_ns: Option<i64>,
    last_error: Option<String>,
    rssi_latest_dbm: Option<f64>,
    rssi_min_dbm: Option<f64>,
    rssi_median_dbm: Option<f64>,
    disconnect_count: u64,
    watchdog_error_timeout_count: u64,
    pmd_gap_gt20ms_count: u64,
    pmd_max_positive_gap_ms: Option<f64>,
    battery_latch: BatteryLatch,
}

impl HistoryLoadMetrics {
    fn not_started() -> Self {
        Self {
            state: "not_started".to_string(),
            started_at: None,
            elapsed_ms: None,
            records_scanned: 0,
            records_matched: 0,
            oldest_loaded_unix_ns: None,
            newest_loaded_unix_ns: None,
            last_error: None,
            rssi_latest_dbm: None,
            rssi_min_dbm: None,
            rssi_median_dbm: None,
            disconnect_count: 0,
            watchdog_error_timeout_count: 0,
            pmd_gap_gt20ms_count: 0,
            pmd_max_positive_gap_ms: None,
            battery_latch: BatteryLatch::default(),
        }
    }

    fn mark_running(&mut self) {
        self.state = "running".to_string();
        self.started_at = Some(Instant::now());
        self.elapsed_ms = Some(0);
        self.last_error = None;
    }

    fn mark_error(&mut self, error: String) {
        self.state = "error".to_string();
        self.elapsed_ms = self
            .started_at
            .map(|start| duration_millis_u64(start.elapsed()));
        self.last_error = Some(error);
    }

    fn apply_complete(&mut self, metrics: HistoryScanMetrics) {
        self.state = "complete".to_string();
        self.elapsed_ms = Some(metrics.elapsed_ms);
        self.records_scanned = metrics.records_scanned;
        self.records_matched = metrics.records_matched;
        self.oldest_loaded_unix_ns = metrics.oldest_loaded_unix_ns;
        self.newest_loaded_unix_ns = metrics.newest_loaded_unix_ns;
        self.last_error = None;
        self.rssi_latest_dbm = metrics.rssi_latest_dbm;
        self.rssi_min_dbm = metrics.rssi_min_dbm;
        self.rssi_median_dbm = metrics.rssi_median_dbm;
        self.disconnect_count = metrics.disconnect_count;
        self.watchdog_error_timeout_count = metrics.watchdog_error_timeout_count;
        self.pmd_gap_gt20ms_count = metrics.pmd_gap_gt20ms_count;
        self.pmd_max_positive_gap_ms = metrics.pmd_max_positive_gap_ms;
        self.battery_latch = metrics.battery_latch;
    }
}

#[derive(Clone, Debug)]
struct IndexBuildMetrics {
    state: String,
    points_written: u64,
    last_error: Option<String>,
}

impl Default for IndexBuildMetrics {
    fn default() -> Self {
        Self {
            state: "not_started".to_string(),
            points_written: 0,
            last_error: None,
        }
    }
}

impl IndexBuildMetrics {
    fn note_written(&mut self, points: usize) {
        self.state = "running".to_string();
        self.points_written = self
            .points_written
            .saturating_add(u64::try_from(points).unwrap_or(u64::MAX));
        self.last_error = None;
    }

    fn note_complete(&mut self) {
        if self.last_error.is_none() {
            self.state = "complete".to_string();
        }
    }

    fn note_error(&mut self, error: String) {
        self.state = "error".to_string();
        self.last_error = Some(error);
    }

    fn merge(&mut self, other: IndexBuildMetrics) {
        self.points_written = self.points_written.saturating_add(other.points_written);
        if other.last_error.is_some() {
            self.state = other.state;
            self.last_error = other.last_error;
        } else if other.state == "complete" || self.state == "not_started" {
            self.state = other.state;
        }
    }
}

#[derive(Default, Clone, Debug)]
struct SseMetrics {
    connected_clients: u64,
    batches_sent: u64,
    rows_sent: u64,
    overflow_resync_count: u64,
    last_batch_size: usize,
    last_write_error: Option<String>,
}

#[derive(Debug)]
struct CriticalLoadOutcome {
    records_processed: u64,
    index_state: String,
    start_offset: Option<u64>,
    start_path: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct CaptureTimeWindow {
    cutoff_unix_ns: i64,
    now_unix_ns: i64,
}

#[derive(Default, Debug)]
struct WalTailer {
    processed_offsets_by_segment: HashMap<Uuid, u64>,
    parser: WalLiveParser,
    segments_read: u64,
    records_processed: u64,
    last_poll_elapsed_ms: Option<u64>,
    last_poll_records_processed: usize,
    last_error: Option<String>,
}

impl WalTailer {
    fn load_critical_window(
        &mut self,
        data_dir: &Path,
        participant_id: &str,
        window: CaptureTimeWindow,
        state: &mut LiveState,
        index_metrics: &mut IndexBuildMetrics,
    ) -> Result<CriticalLoadOutcome> {
        let result =
            self.load_critical_window_inner(data_dir, participant_id, window, state, index_metrics);
        match &result {
            Ok(_) => self.last_error = None,
            Err(err) => self.last_error = Some(format!("{err:#}")),
        }
        result
    }

    fn load_critical_window_inner(
        &mut self,
        data_dir: &Path,
        participant_id: &str,
        window: CaptureTimeWindow,
        state: &mut LiveState,
        index_metrics: &mut IndexBuildMetrics,
    ) -> Result<CriticalLoadOutcome> {
        let mut files = find_critical_wal_files(
            data_dir,
            participant_id,
            H10_WAL_SOURCE,
            window.cutoff_unix_ns,
        )?;
        files.extend(find_critical_wal_files(
            data_dir,
            participant_id,
            REPLAY_H10_SOURCE,
            window.cutoff_unix_ns,
        )?);
        files.sort_by(|left, right| {
            left.start_unix_ns
                .cmp(&right.start_unix_ns)
                .then(left.path.cmp(&right.path))
        });
        if files.is_empty() {
            return Ok(CriticalLoadOutcome {
                records_processed: 0,
                index_state: "not_needed".to_string(),
                start_offset: None,
                start_path: None,
            });
        }

        let newest_path = files.last().map(|file| file.path.clone());
        let mut total_processed = 0_u64;
        let mut index_state = "not_needed".to_string();
        let mut first_start_offset = None;
        let mut first_start_path = None;
        for file in files {
            let source = wal_source_for_path(&file.path);
            let header = read_wal_header(&file.path)
                .with_context(|| format!("read critical WAL header {}", file.path.display()))?;
            let (mut start_offset, mut state_for_file) = choose_critical_start_offset(
                data_dir,
                participant_id,
                source,
                &file,
                &header,
                window.cutoff_unix_ns,
            );
            let is_newest_candidate = newest_path.as_ref() == Some(&file.path);
            if state_for_file == "missing_or_stale_fallback" && !is_newest_candidate {
                index_state = merge_critical_index_state(&index_state, &state_for_file);
                continue;
            }
            let mode = if file.is_open {
                WalReadMode::TailTolerant
            } else {
                WalReadMode::Strict
            };
            let (read, force_rebuild_index) = read_critical_wal_with_index_fallback(
                &file,
                mode,
                &header,
                start_offset,
                &mut state_for_file,
                &mut start_offset,
            )?;
            index_state = merge_critical_index_state(&index_state, &state_for_file);
            first_start_offset.get_or_insert(start_offset);
            first_start_path.get_or_insert_with(|| file.path.display().to_string());
            self.segments_read = self.segments_read.saturating_add(1);
            let decoded_records = read.records.len();
            let _admitted_records = self.process_live_read(&file, &read, window, state)?;
            total_processed =
                total_processed.saturating_add(u64::try_from(decoded_records).unwrap_or(u64::MAX));
            self.processed_offsets_by_segment
                .insert(read.header.segment_id, read.valid_end_offset);
            if force_rebuild_index {
                delete_live_index(data_dir, participant_id, source, &read.header);
            }
            best_effort_update_live_index(
                data_dir,
                participant_id,
                source,
                &file,
                &read,
                index_metrics,
            );
        }
        Ok(CriticalLoadOutcome {
            records_processed: total_processed,
            index_state,
            start_offset: first_start_offset,
            start_path: first_start_path,
        })
    }

    fn poll_live_once(
        &mut self,
        data_dir: &Path,
        participant_id: &str,
        window: CaptureTimeWindow,
        state: &mut LiveState,
        index_metrics: &mut IndexBuildMetrics,
        sources: &[&str],
    ) -> Result<usize> {
        let start = Instant::now();
        let result = self.poll_live_once_inner(
            data_dir,
            participant_id,
            window,
            state,
            index_metrics,
            sources,
        );
        self.last_poll_elapsed_ms = Some(duration_millis_u64(start.elapsed()));
        match &result {
            Ok(processed) => {
                self.last_poll_records_processed = *processed;
                self.last_error = None;
            }
            Err(err) => {
                self.last_poll_records_processed = 0;
                self.last_error = Some(format!("{err:#}"));
            }
        }
        result
    }

    fn poll_live_once_inner(
        &mut self,
        data_dir: &Path,
        participant_id: &str,
        window: CaptureTimeWindow,
        state: &mut LiveState,
        index_metrics: &mut IndexBuildMetrics,
        sources: &[&str],
    ) -> Result<usize> {
        let mut processed = 0usize;
        for source in sources {
            let files =
                find_recent_wal_files(data_dir, participant_id, source, window.cutoff_unix_ns)?;
            for file in files {
                let mode = if file.is_open {
                    WalReadMode::TailTolerant
                } else {
                    WalReadMode::Strict
                };
                let read = read_wal_records_incremental(
                    &file.path,
                    mode,
                    &self.processed_offsets_by_segment,
                )
                .with_context(|| format!("read live WAL {}", file.path.display()))?;
                self.segments_read = self.segments_read.saturating_add(1);
                let segment_id = read.header.segment_id;
                if *source == H10_WAL_SOURCE {
                    best_effort_update_live_index(
                        data_dir,
                        participant_id,
                        source,
                        &file,
                        &read,
                        index_metrics,
                    );
                }
                let source_processed = self.process_live_read(&file, &read, window, state)?;
                processed = processed.saturating_add(source_processed);
                self.processed_offsets_by_segment
                    .insert(segment_id, read.valid_end_offset);
            }
        }
        Ok(processed)
    }

    fn process_live_read(
        &mut self,
        file: &WalFileInfo,
        read: &crate::wal::WalReadResult,
        window: CaptureTimeWindow,
        state: &mut LiveState,
    ) -> Result<usize> {
        let mut processed = 0usize;
        for frame in &read.records {
            state.note_cloud_batch_received(&frame.record, window);
            // A cloud envelope is timestamped when the server receives it. Its
            // logical children retain the phone capture timestamps and can be
            // many hours old after an outage, so the envelope timestamp must
            // never decide live eligibility.
            for record in logical_wal_records(&frame.record) {
                if record.collector_unix_ns < window.cutoff_unix_ns {
                    state.note_catch_up_record(record);
                    continue;
                }
                if record.collector_unix_ns
                    > window.now_unix_ns.saturating_add(LIVE_FUTURE_TOLERANCE_NS)
                {
                    state.note_future_record(record);
                    continue;
                }
                if let Err(error) = self.parser.process_record(record, state) {
                    let detail = format!(
                        "record_id={} kind={} path={} error={error:#}",
                        record.record_id,
                        record.kind.as_str(),
                        file.path.display()
                    );
                    eprintln!("quarantined malformed live WAL record: {detail}");
                    state.push_event(quarantined_record_event(record, "live", detail));
                    // A malformed compressed PMD frame may have mutated parser
                    // state before failing. Reset only that state; connection
                    // identity/timezone mappings and monotonic counters remain.
                    self.parser.pmd = PmdParser::default();
                }
                processed = processed.saturating_add(1);
                self.records_processed = self.records_processed.saturating_add(1);
            }
        }
        Ok(processed)
    }
}

fn read_wal_header(path: &Path) -> Result<SegmentHeader> {
    let mut file = File::open(path).with_context(|| format!("open WAL {}", path.display()))?;
    let mut header_bytes = [0_u8; WAL_HEADER_LEN];
    let header_read = read_exact_or_partial_live(&mut file, &mut header_bytes)
        .with_context(|| format!("read WAL header from {}", path.display()))?;
    if header_read != WAL_HEADER_LEN {
        bail!("WAL header is incomplete: got {header_read} bytes, expected {WAL_HEADER_LEN}");
    }
    SegmentHeader::decode(&header_bytes)
        .with_context(|| format!("decode WAL header from {}", path.display()))
}

fn find_critical_wal_files(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    live_cutoff_unix_ns: i64,
) -> Result<Vec<WalFileInfo>> {
    let files = find_recent_wal_files(data_dir, participant_id, source, live_cutoff_unix_ns)?;
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    let newest_start = files
        .iter()
        .map(|file| file.start_unix_ns)
        .max()
        .unwrap_or(i64::MIN);
    for file in files {
        let overlaps = file.start_unix_ns >= live_cutoff_unix_ns
            || file
                .end_unix_ns
                .map(|end| end >= live_cutoff_unix_ns)
                .unwrap_or(file.start_unix_ns == newest_start);
        if overlaps || file.start_unix_ns == newest_start {
            selected.push(file);
        }
    }
    selected.sort_by(|left, right| {
        left.start_unix_ns
            .cmp(&right.start_unix_ns)
            .then(left.end_unix_ns.cmp(&right.end_unix_ns))
            .then(left.path.cmp(&right.path))
    });
    Ok(selected)
}

fn wal_source_for_path(path: &Path) -> &'static str {
    if path
        .components()
        .any(|component| component.as_os_str().to_str() == Some("source=replay_h10"))
    {
        REPLAY_H10_SOURCE
    } else {
        H10_WAL_SOURCE
    }
}

fn merge_critical_index_state(current: &str, next: &str) -> String {
    if current == "missing_or_stale_fallback" || next == "missing_or_stale_fallback" {
        "missing_or_stale_fallback".to_string()
    } else if current == "used" || next == "used" {
        "used".to_string()
    } else if current == "error" || next == "error" {
        "error".to_string()
    } else {
        "not_needed".to_string()
    }
}

fn choose_critical_start_offset(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    file: &WalFileInfo,
    header: &SegmentHeader,
    live_cutoff_unix_ns: i64,
) -> (u64, String) {
    if file.start_unix_ns >= live_cutoff_unix_ns {
        return (WAL_HEADER_LEN as u64, "not_needed".to_string());
    }
    match load_live_index(data_dir, participant_id, source, file, header) {
        Ok(Some(index)) => {
            if let Some(point) = index
                .points
                .iter()
                .filter(|point| point.collector_unix_ns <= live_cutoff_unix_ns)
                .max_by_key(|point| point.collector_unix_ns)
            {
                (
                    point.frame_offset.max(WAL_HEADER_LEN as u64),
                    "used".to_string(),
                )
            } else {
                (
                    WAL_HEADER_LEN as u64,
                    "missing_or_stale_fallback".to_string(),
                )
            }
        }
        Ok(None) | Err(_) => (
            WAL_HEADER_LEN as u64,
            "missing_or_stale_fallback".to_string(),
        ),
    }
}

fn read_critical_wal_with_index_fallback(
    file: &WalFileInfo,
    mode: WalReadMode,
    header: &SegmentHeader,
    start_offset: u64,
    state_for_file: &mut String,
    effective_start_offset: &mut u64,
) -> Result<(crate::wal::WalReadResult, bool)> {
    let mut offsets = HashMap::new();
    offsets.insert(header.segment_id, start_offset);
    let read_result = read_wal_records_incremental(&file.path, mode, &offsets)
        .with_context(|| format!("read critical WAL {}", file.path.display()));
    match read_result {
        Ok(read)
            if *state_for_file == "used"
                && read.tail_issue.is_some()
                && read.records.is_empty() =>
        {
            *state_for_file = "missing_or_stale_fallback".to_string();
            *effective_start_offset = WAL_HEADER_LEN as u64;
            let fallback = read_critical_wal_from_header(file, mode).with_context(|| {
                format!(
                    "indexed critical WAL read from {} produced only a tail issue; fallback also failed",
                    file.path.display()
                )
            })?;
            Ok((fallback, true))
        }
        Ok(read) => Ok((read, false)),
        Err(err) if *state_for_file == "used" => {
            *state_for_file = "missing_or_stale_fallback".to_string();
            *effective_start_offset = WAL_HEADER_LEN as u64;
            let fallback = read_critical_wal_from_header(file, mode).with_context(|| {
                format!(
                    "indexed critical WAL read from {} failed ({err:#}); fallback also failed",
                    file.path.display()
                )
            })?;
            Ok((fallback, true))
        }
        Err(err) => Err(err),
    }
}

fn read_critical_wal_from_header(
    file: &WalFileInfo,
    mode: WalReadMode,
) -> Result<crate::wal::WalReadResult> {
    read_wal_records_incremental(&file.path, mode, &HashMap::new())
        .with_context(|| format!("read critical WAL fallback {}", file.path.display()))
}

fn read_wal_records_incremental(
    path: &Path,
    mode: WalReadMode,
    processed_offsets_by_segment: &HashMap<Uuid, u64>,
) -> Result<crate::wal::WalReadResult> {
    let mut file = File::open(path).with_context(|| format!("open WAL {}", path.display()))?;
    let file_len = file
        .metadata()
        .with_context(|| format!("stat WAL {}", path.display()))?
        .len();
    let mut header_bytes = [0_u8; WAL_HEADER_LEN];
    let header_read = read_exact_or_partial_live(&mut file, &mut header_bytes)
        .with_context(|| format!("read WAL header from {}", path.display()))?;
    if header_read != WAL_HEADER_LEN {
        bail!("WAL header is incomplete: got {header_read} bytes, expected {WAL_HEADER_LEN}");
    }
    let header = SegmentHeader::decode(&header_bytes)
        .with_context(|| format!("decode WAL header from {}", path.display()))?;
    let requested_start = processed_offsets_by_segment
        .get(&header.segment_id)
        .copied()
        .unwrap_or(WAL_HEADER_LEN as u64)
        .max(WAL_HEADER_LEN as u64);
    let mut offset = requested_start.min(file_len);
    file.seek(SeekFrom::Start(offset))
        .with_context(|| format!("seek WAL {} to offset {offset}", path.display()))?;

    let mut records = Vec::new();
    let mut valid_end_offset = offset;
    let mut tail_issue = None;

    loop {
        let mut frame_header = [0_u8; WAL_FRAME_HEADER_LEN];
        let frame_header_read = read_exact_or_partial_live(&mut file, &mut frame_header)
            .with_context(|| format!("read WAL frame header at offset {offset}"))?;
        if frame_header_read == 0 {
            break;
        }
        if frame_header_read != WAL_FRAME_HEADER_LEN {
            let issue = WalTailIssue::IncompleteFrameHeader {
                offset,
                bytes_present: frame_header_read,
            };
            if handle_live_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }

        let payload_len = u32::from_le_bytes(
            frame_header[0..4]
                .try_into()
                .map_err(|_| anyhow!("payload_len frame-header slice length mismatch"))?,
        );
        let expected_crc32c = u32::from_le_bytes(
            frame_header[4..8]
                .try_into()
                .map_err(|_| anyhow!("payload_crc32c frame-header slice length mismatch"))?,
        );
        if payload_len == 0 {
            let issue = WalTailIssue::InvalidPayloadLen {
                offset,
                payload_len,
            };
            if handle_live_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }
        if usize::try_from(payload_len)
            .map(|len| len > WAL_MAX_PAYLOAD_LEN)
            .unwrap_or(true)
        {
            let issue = WalTailIssue::ExcessivePayloadLen {
                offset,
                payload_len,
                max_payload_len: WAL_MAX_PAYLOAD_LEN,
            };
            if handle_live_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }

        let payload_len_usize =
            usize::try_from(payload_len).context("payload_len does not fit usize")?;
        let mut payload = vec![0_u8; payload_len_usize];
        let payload_read = read_exact_or_partial_live(&mut file, &mut payload)
            .with_context(|| format!("read WAL payload at offset {offset}"))?;
        if payload_read != payload_len_usize {
            let issue = WalTailIssue::IncompletePayload {
                offset,
                payload_len,
                bytes_present: payload_read,
            };
            if handle_live_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }

        let actual_crc32c = crc32c(&payload);
        if actual_crc32c != expected_crc32c {
            let issue = WalTailIssue::ChecksumMismatch {
                offset,
                payload_len,
                expected_crc32c,
                actual_crc32c,
            };
            if handle_live_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }

        let record = decode_record_payload(&payload)
            .with_context(|| format!("decode WAL payload at frame offset {offset}"))?;
        records.push(WalFrameRecord {
            frame_offset: offset,
            payload_len,
            payload_crc32c: expected_crc32c,
            payload,
            record,
        });
        offset = offset
            .checked_add(WAL_FRAME_HEADER_LEN as u64)
            .and_then(|value| value.checked_add(u64::from(payload_len)))
            .ok_or_else(|| anyhow!("WAL offset overflow"))?;
        valid_end_offset = offset;
    }

    Ok(crate::wal::WalReadResult {
        header,
        records,
        valid_end_offset,
        file_len,
        tail_issue,
    })
}

fn read_exact_or_partial_live<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("read WAL bytes"),
        }
    }
    Ok(total)
}

fn handle_live_tail_issue(mode: WalReadMode, issue: &WalTailIssue) -> Result<bool> {
    match mode {
        WalReadMode::Strict => bail!("invalid WAL frame in strict mode: {issue:?}"),
        WalReadMode::TailTolerant => Ok(true),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct LiveWalIndexFile {
    schema_version: u16,
    participant_id: String,
    source: String,
    segment_id: Uuid,
    segment_start_unix_ns: i64,
    wal_file_name: String,
    wal_header_crc32c: u32,
    points: Vec<LiveWalIndexPoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct LiveWalIndexPoint {
    segment_id: Uuid,
    frame_offset: u64,
    collector_unix_ns: i64,
    record_id: Uuid,
    record_kind: String,
    file_len_at_index: u64,
}

fn live_index_path(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    header: &SegmentHeader,
) -> Result<PathBuf> {
    Ok(data_dir
        .join("live_index")
        .join(format!("participant_id={participant_id}"))
        .join(format!("source={source}"))
        .join(format!(
            "date={}",
            format_date_partition(header.segment_start_unix_ns)?
        ))
        .join(format!(
            "wal-{}.live_index.json",
            header.segment_id.hyphenated()
        )))
}

fn live_index_header_crc32c(header: &SegmentHeader) -> u32 {
    crc32c(&header.encode())
}

fn load_live_index(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    file: &WalFileInfo,
    header: &SegmentHeader,
) -> Result<Option<LiveWalIndexFile>> {
    let path = live_index_path(data_dir, participant_id, source, header)?;
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path)
        .with_context(|| format!("read live WAL index {}", path.display()))?;
    let index: LiveWalIndexFile = serde_json::from_str(&text)
        .with_context(|| format!("decode live WAL index {}", path.display()))?;
    let wal_len = fs::metadata(&file.path)
        .with_context(|| format!("stat indexed WAL {}", file.path.display()))?
        .len();
    if index.schema_version != LIVE_INDEX_SCHEMA_VERSION
        || index.participant_id != participant_id
        || index.source != source
        || index.segment_id != header.segment_id
        || index.segment_start_unix_ns != header.segment_start_unix_ns
        || index.wal_header_crc32c != live_index_header_crc32c(header)
        || index.points.iter().any(|point| {
            point.segment_id != header.segment_id
                || point.frame_offset < WAL_HEADER_LEN as u64
                || point.frame_offset >= wal_len
                || point.file_len_at_index > wal_len
        })
    {
        return Ok(None);
    }
    Ok(Some(index))
}

fn best_effort_update_live_index(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    file: &WalFileInfo,
    read: &crate::wal::WalReadResult,
    metrics: &mut IndexBuildMetrics,
) {
    if read.records.is_empty() {
        return;
    }
    match update_live_index(data_dir, participant_id, source, file, read) {
        Ok(points_written) => {
            if points_written > 0 {
                metrics.note_written(points_written);
            }
        }
        Err(err) => metrics.note_error(format!("{err:#}")),
    }
}

fn update_live_index(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    file: &WalFileInfo,
    read: &crate::wal::WalReadResult,
) -> Result<usize> {
    let mut existing_points =
        match load_live_index(data_dir, participant_id, source, file, &read.header) {
            Ok(Some(index)) => index.points,
            Ok(None) | Err(_) => Vec::new(),
        };
    existing_points.sort_by_key(|point| point.frame_offset);
    existing_points.dedup_by_key(|point| point.frame_offset);
    let mut last_indexed_ts = existing_points
        .last()
        .map(|point| point.collector_unix_ns)
        .unwrap_or(i64::MIN);
    let mut new_points = Vec::new();
    for frame in &read.records {
        if frame.record.collector_unix_ns
            < last_indexed_ts.saturating_add(LIVE_INDEX_MIN_INTERVAL_NS)
            && (!existing_points.is_empty() || !new_points.is_empty())
        {
            continue;
        }
        let point = LiveWalIndexPoint {
            segment_id: read.header.segment_id,
            frame_offset: frame.frame_offset,
            collector_unix_ns: frame.record.collector_unix_ns,
            record_id: frame.record.record_id,
            record_kind: frame.record.kind.as_str().to_string(),
            file_len_at_index: read.file_len,
        };
        last_indexed_ts = point.collector_unix_ns;
        new_points.push(point);
    }
    if new_points.is_empty() {
        return Ok(0);
    }
    let written = new_points.len();
    existing_points.extend(new_points);
    existing_points.sort_by_key(|point| point.frame_offset);
    existing_points.dedup_by_key(|point| point.frame_offset);
    let wal_file_name = file
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("")
        .to_string();
    let index = LiveWalIndexFile {
        schema_version: LIVE_INDEX_SCHEMA_VERSION,
        participant_id: participant_id.to_string(),
        source: source.to_string(),
        segment_id: read.header.segment_id,
        segment_start_unix_ns: read.header.segment_start_unix_ns,
        wal_file_name,
        wal_header_crc32c: live_index_header_crc32c(&read.header),
        points: existing_points,
    };
    write_live_index(data_dir, participant_id, source, &read.header, &index)?;
    Ok(written)
}

fn delete_live_index(data_dir: &Path, participant_id: &str, source: &str, header: &SegmentHeader) {
    if let Ok(path) = live_index_path(data_dir, participant_id, source, header) {
        let _ = fs::remove_file(path);
    }
}

fn write_live_index(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    header: &SegmentHeader,
    index: &LiveWalIndexFile,
) -> Result<()> {
    let path = live_index_path(data_dir, participant_id, source, header)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("live index path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create live index dir {}", parent.display()))?;
    let tmp = parent.join(format!(
        "{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("live_index.json"),
        std::process::id(),
        Uuid::new_v4().hyphenated()
    ));
    let json = serde_json::to_vec(index).context("serialize live WAL index")?;
    fs::write(&tmp, json).with_context(|| format!("write live WAL index tmp {}", tmp.display()))?;
    fs::rename(&tmp, &path).with_context(|| {
        format!(
            "atomically rename live WAL index tmp {} to {}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

#[derive(Default, Debug)]
struct WalLiveParser {
    pmd: PmdParser,
    connection_ids: ConnectionIdMapper,
    timezone_offsets: BTreeMap<Uuid, i32>,
    pmd_frames: u64,
    hr_frames: u64,
}

impl WalLiveParser {
    fn process_record(&mut self, record: &WalRecord, state: &mut LiveState) -> Result<()> {
        state.note_source(record);
        if let WalBody::PsftpSetLocalTimeAttempt(body) = &record.body
            && let Some(connection_id) = record.connection_id
        {
            self.timezone_offsets
                .insert(connection_id, body.tz_offset_minutes);
            state
                .connection_timezone_offsets
                .insert(connection_id, body.tz_offset_minutes);
        }
        let meta = record_meta_from_wal(record, &mut self.connection_ids)?;
        let timezone_offset_minutes = record
            .connection_id
            .and_then(|id| self.timezone_offsets.get(&id).copied());
        let mut sink = LiveRecordSink {
            state,
            current_meta: meta,
            connection_uuid: record.connection_id,
            timezone_offset_minutes,
            pmd_frames: self.pmd_frames,
            hr_frames: self.hr_frames,
        };
        route_live_wal_record(record, &mut self.pmd, &mut sink)?;
        self.pmd_frames = sink.pmd_frames;
        self.hr_frames = sink.hr_frames;
        Ok(())
    }
}

#[derive(Default, Debug)]
struct ConnectionIdMapper {
    ids: BTreeMap<Uuid, u32>,
}

impl ConnectionIdMapper {
    fn map(&mut self, id: Option<Uuid>) -> u32 {
        let Some(id) = id else {
            return 0;
        };
        if let Some(mapped) = self.ids.get(&id) {
            return *mapped;
        }
        let next = u32::try_from(self.ids.len().saturating_add(1)).unwrap_or(u32::MAX);
        self.ids.insert(id, next);
        next
    }
}

fn record_meta_from_wal(
    record: &WalRecord,
    connection_ids: &mut ConnectionIdMapper,
) -> Result<RecordMeta> {
    let collector_unix_ns = u128::try_from(record.collector_unix_ns)
        .with_context(|| format!("negative collector_unix_ns in record {}", record.record_id))?;
    Ok(RecordMeta {
        schema_version: RECORD_SCHEMA_VERSION,
        sensor_id: record
            .physical_device_id
            .clone()
            .unwrap_or_else(|| record.source.clone()),
        session_id: record.session_id.to_string(),
        connection_id: connection_ids.map(record.connection_id),
        collector_unix_ns,
        collector_monotonic_ns: u128::from(record.collector_monotonic_ns),
    })
}

fn route_live_operational_wal_record<S: RecordSink>(
    record: &WalRecord,
    sink: &mut S,
) -> Result<bool> {
    match &record.body {
        WalBody::CollectorEvent(body) => {
            let detail = if body.fields.is_empty() {
                body.detail.clone()
            } else {
                format!(
                    "{} fields={}",
                    body.detail,
                    serde_json::to_string(&body.fields)?
                )
            };
            sink.event(&body.event, &detail)?;
        }
        WalBody::RssiReadError(body) => {
            sink.event(
                "rssi_read_error",
                &format!(
                    "peripheral_id={} method={} elapsed_ms={} error={}",
                    body.peripheral_id, body.method, body.elapsed_ms, body.error
                ),
            )?;
        }
        WalBody::CentralEvent(body) => {
            sink.event(
                "central_event",
                &format!(
                    "event_name={} peripheral_id={} detail={} rssi_dbm={:?}",
                    body.event_name, body.peripheral_id, body.detail, body.rssi_dbm
                ),
            )?;
        }
        WalBody::ManualMarker(body) => {
            sink.event(
                "manual_marker",
                &format!(
                    "event_id={} event_unix_ns={} label={} note={:?}",
                    body.event_id, body.event_unix_ns, body.label, body.note
                ),
            )?;
        }
        WalBody::SegmentClosed(SegmentClosedBody { reason, .. }) => {
            sink.event("segment_closed", reason)?;
        }
        WalBody::SegmentStarted(body) => {
            sink.event("segment_started", &body.rotation_reason)?;
        }
        WalBody::SessionStarted(body) => {
            sink.event("session_started", &body.started_reason)?;
        }
        WalBody::SessionEnded(body) => {
            sink.event(
                "session_ended",
                &format!("{} {}", body.reason, body.message),
            )?;
        }
        WalBody::RecoveryEvent(body) => {
            sink.event(
                "recovery_event",
                &format!(
                    "action={} path={} truncated_tail_bytes={} message={}",
                    body.action, body.recovered_path, body.truncated_tail_bytes, body.message
                ),
            )?;
        }
        WalBody::ConnectAttempt(body) => {
            sink.event(
                "connect_attempt",
                &format!("peripheral_id={}", body.peripheral_id),
            )?;
        }
        WalBody::ConnectResult(body) => {
            sink.event(
                "connect_result",
                &format!(
                    "connected={} elapsed_ms={}",
                    body.connected, body.elapsed_ms
                ),
            )?;
        }
        WalBody::ConnectError(body) => {
            sink.event(
                "connect_error",
                &format!(
                    "stage={} elapsed_ms={} error={}",
                    body.stage, body.elapsed_ms, body.error
                ),
            )?;
        }
        WalBody::DisconnectAttempt(body) => {
            sink.event("disconnect_attempt", &body.reason)?;
        }
        WalBody::DisconnectResult(body) => {
            sink.event(
                "disconnect_result",
                &format!("reason={} elapsed_ms={}", body.reason, body.elapsed_ms),
            )?;
        }
        WalBody::DisconnectError(body) => {
            sink.event(
                "disconnect_error",
                &format!("reason={} error={}", body.reason, body.error),
            )?;
        }
        WalBody::ScanAttempt(body) => {
            sink.event("scan_attempt", &format!("target={}", body.target))?;
        }
        WalBody::ScanResult(body) if body.matched => {
            sink.event(
                "scan_result",
                &format!(
                    "matched peripheral_id={} rssi_dbm={:?}",
                    body.peripheral_id, body.rssi_dbm
                ),
            )?;
        }
        WalBody::ScanError(body) => {
            sink.event(
                "scan_error",
                &format!("stage={} error={}", body.stage, body.error),
            )?;
        }
        WalBody::ServiceDiscoverySummary(body) => {
            sink.event(
                "service_discovery_summary",
                &format!(
                    "peripheral_id={} service_count={}",
                    body.peripheral_id, body.service_count
                ),
            )?;
        }
        WalBody::SubscriptionError(body) => {
            sink.event(
                "subscription_error",
                &format!(
                    "characteristic_uuid={} error={}",
                    body.characteristic_uuid, body.error
                ),
            )?;
        }
        WalBody::GattReadError(body) => {
            sink.event(
                "gatt_read_error",
                &format!("reason={} error={}", body.reason, body.error),
            )?;
        }
        WalBody::GattWriteError(body) => {
            sink.event(
                "gatt_write_error",
                &format!(
                    "characteristic_uuid={} error={}",
                    body.characteristic_uuid, body.error
                ),
            )?;
        }
        WalBody::PsftpSetLocalTimeAttempt(body) => {
            sink.event(
                "psftp_set_local_time_attempt",
                &format!(
                    "operation_id={} local_time={} tz_offset_minutes={} dst_offset_minutes={:?}",
                    body.operation_id,
                    body.local_time_iso8601,
                    body.tz_offset_minutes,
                    body.dst_offset_minutes
                ),
            )?;
        }
        WalBody::PsftpSetLocalTimeResult(body) => {
            sink.event(
                if body.success {
                    "psftp_set_local_time_success"
                } else {
                    "psftp_set_local_time_error"
                },
                &format!(
                    "status_code={:?} error={:?} elapsed_ms={}",
                    body.status_code, body.error, body.elapsed_ms
                ),
            )?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn route_live_wal_record(
    record: &WalRecord,
    parser: &mut PmdParser,
    sink: &mut LiveRecordSink<'_>,
) -> Result<()> {
    if let WalBody::CollectorEvent(body) = &record.body {
        let detail = collector_event_detail(body)?;
        sink.state.push_event(LiveEvent {
            collector_unix_ns: sink.current_meta.collector_unix_ns,
            event: body.event.clone(),
            detail,
            session: sink.current_meta.session_id.clone(),
            severity: Some(body.severity.clone()),
            fields: body.fields.clone(),
        });
        return Ok(());
    }
    if route_live_operational_wal_record(record, sink)? {
        return Ok(());
    }
    match &record.body {
        WalBody::GattNotification(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_notification")?;
            let routed = route_live_gatt_payload(
                sink.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                &body.description,
                &payload,
                parser,
                sink,
            )?;
            if routed.pmd_frame {
                sink.pmd_frames = sink.pmd_frames.saturating_add(1);
            }
            if routed.hr_frame {
                sink.hr_frames = sink.hr_frames.saturating_add(1);
            }
        }
        WalBody::GattReadResponse(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_read_response")?;
            if is_battery_characteristic(&body.characteristic_uuid, &body.reason)
                && let Some(level) = payload.first()
            {
                sink.battery(sink.current_meta.collector_unix_ns, *level, &body.reason)?;
            }
        }
        WalBody::RssiSample(body) => {
            sink.health(
                sink.current_meta.collector_unix_ns,
                Some(body.rssi_dbm),
                sink.pmd_frames,
                sink.hr_frames,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn collector_event_detail(body: &crate::wal::CollectorEventBody) -> Result<String> {
    if body.fields.is_empty() {
        Ok(body.detail.clone())
    } else {
        Ok(format!(
            "{} fields={}",
            body.detail,
            serde_json::to_string(&body.fields)?
        ))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GattPayloadRoute {
    pmd_frame: bool,
    hr_frame: bool,
}

fn route_live_gatt_payload<S: RecordSink>(
    collector_ns: u128,
    characteristic_uuid: &str,
    description: &str,
    payload: &[u8],
    parser: &mut PmdParser,
    sink: &mut S,
) -> Result<GattPayloadRoute> {
    let uuid = characteristic_uuid.to_ascii_lowercase();
    if uuid == PMD_DATA_UUID {
        parser.handle_pmd_data(collector_ns, payload, sink)?;
        Ok(GattPayloadRoute {
            pmd_frame: true,
            hr_frame: false,
        })
    } else if uuid == PMD_CONTROL_UUID {
        parser.handle_control(collector_ns, payload, sink)?;
        Ok(GattPayloadRoute::default())
    } else if uuid == HR_MEASUREMENT_UUID {
        handle_hr(collector_ns, payload, sink)?;
        Ok(GattPayloadRoute {
            pmd_frame: false,
            hr_frame: true,
        })
    } else if is_battery_characteristic(&uuid, description) {
        if let Some(level) = payload.first() {
            sink.battery(collector_ns, *level, description)?;
        }
        Ok(GattPayloadRoute::default())
    } else {
        Ok(GattPayloadRoute::default())
    }
}

fn decode_payload_b64(encoded: &str, expected_len: u32, label: &str) -> Result<Vec<u8>> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .with_context(|| format!("decode {label} payload"))?;
    let expected = usize::try_from(expected_len).context("payload_len does not fit usize")?;
    if decoded.len() != expected {
        bail!(
            "{label} decoded payload length {} does not match payload_len {expected}",
            decoded.len()
        );
    }
    Ok(decoded)
}

fn is_battery_characteristic(characteristic_uuid: &str, reason_or_description: &str) -> bool {
    characteristic_uuid.eq_ignore_ascii_case(BATTERY_LEVEL_UUID)
        || reason_or_description == "battery_level"
}

struct LiveRecordSink<'a> {
    state: &'a mut LiveState,
    current_meta: RecordMeta,
    connection_uuid: Option<Uuid>,
    timezone_offset_minutes: Option<i32>,
    pmd_frames: u64,
    hr_frames: u64,
}

impl crate::sink::RecordSink for LiveRecordSink<'_> {
    fn session_dir(&self) -> &Path {
        Path::new("")
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        self.state.push_event(LiveEvent {
            collector_unix_ns: self.current_meta.collector_unix_ns,
            event: event.to_string(),
            detail: detail.to_string(),
            session: self.current_meta.session_id.clone(),
            severity: None,
            fields: BTreeMap::new(),
        });
        Ok(())
    }

    fn raw_gatt(
        &mut self,
        _collector_ns: u128,
        _characteristic_uuid: &str,
        _direction: &str,
        _operation: &str,
        _payload: &[u8],
        _detail: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        self.state.push_timing(LiveTiming {
            i: 0,
            collector_unix_ns: timing.collector_ns,
            measurement: measurement_name(timing.measurement_type).to_string(),
            measurement_type: timing.measurement_type,
            frame_type: timing.frame_type,
            compressed: timing.compressed,
            device_frame_ts_ns: timing.device_frame_ts,
            payload_len: u32::try_from(timing.payload_len).unwrap_or(u32::MAX),
            sample_count: u32::try_from(timing.sample_count).unwrap_or(u32::MAX),
            collector_delta_ms: timing.collector_delta_ms,
            device_delta_ms: timing.device_delta_ms,
            expected_frame_delta_ms: Some(timing.expected_frame_delta_ms),
            gap_ms: timing.gap_ms,
            connection_id: self.current_meta.connection_id,
            connection_uuid: self.connection_uuid,
            tz_offset_minutes: self.timezone_offset_minutes,
            packet_delay_ms: packet_delay_ms(
                timing.collector_ns,
                timing.device_frame_ts,
                self.timezone_offset_minutes,
            ),
        });
        Ok(())
    }

    fn ecg(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        microvolts: i32,
    ) -> Result<()> {
        self.state.push_ecg(LiveEcg {
            i: 0,
            collector_unix_ns: collector_ns,
            device_ts_ns: device_ts,
            sample_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
            microvolts,
        });
        Ok(())
    }

    fn acc(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        x: i32,
        y: i32,
        z: i32,
    ) -> Result<()> {
        self.state.push_acc(LiveAcc {
            i: 0,
            collector_unix_ns: collector_ns,
            device_ts_ns: device_ts,
            sample_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
            x: -x,
            raw_x: x,
            y,
            z,
        });
        Ok(())
    }

    fn hr(&mut self, collector_ns: u128, data: &HrData, _raw: &[u8]) -> Result<()> {
        self.state.push_hr(LiveHr {
            i: 0,
            collector_unix_ns: collector_ns,
            bpm: data.hr_bpm,
            contact_supported: data.sensor_contact_supported,
            contact_detected: data.sensor_contact_detected,
            rr_ms: data.rr_ms.clone(),
            connection_id: self.current_meta.connection_id,
            connection_uuid: self.connection_uuid,
            tz_offset_minutes: self.timezone_offset_minutes,
        });
        for (index, rr_ms) in data.rr_ms.iter().enumerate() {
            self.state.push_rr(LiveRr {
                i: 0,
                collector_unix_ns: collector_ns,
                rr_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
                rr_ms: *rr_ms,
            });
        }
        Ok(())
    }

    fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()> {
        self.state.push_battery(LiveBattery {
            collector_unix_ns: collector_ns,
            battery_percent: level,
            reason: reason.to_string(),
        });
        Ok(())
    }

    fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        self.state.push_health(LiveHealth {
            i: 0,
            collector_unix_ns: collector_ns,
            rssi: rssi.map(f64::from),
            pmd_frames,
            hr_frames,
        });
        Ok(())
    }
}

#[derive(Default, Debug)]
struct HistoryScanMetrics {
    elapsed_ms: u64,
    records_scanned: u64,
    records_matched: u64,
    oldest_loaded_unix_ns: Option<i64>,
    newest_loaded_unix_ns: Option<i64>,
    rssi_latest_dbm: Option<f64>,
    rssi_min_dbm: Option<f64>,
    rssi_median_dbm: Option<f64>,
    disconnect_count: u64,
    watchdog_error_timeout_count: u64,
    pmd_gap_gt20ms_count: u64,
    pmd_max_positive_gap_ms: Option<f64>,
    battery_latch: BatteryLatch,
}

#[derive(Default, Debug)]
struct HistoryScanResult {
    events: Vec<LiveEvent>,
    metrics: HistoryScanMetrics,
    index: IndexBuildMetrics,
    timezone_offsets: BTreeMap<Uuid, i32>,
}

#[derive(Debug)]
struct HistoryRecordSink<'a> {
    events: &'a mut Vec<LiveEvent>,
    metrics: &'a mut HistoryScanMetrics,
    current_meta: RecordMeta,
}

impl RecordSink for HistoryRecordSink<'_> {
    fn session_dir(&self) -> &Path {
        Path::new("")
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        self.events.push(LiveEvent {
            collector_unix_ns: self.current_meta.collector_unix_ns,
            event: event.to_string(),
            detail: detail.to_string(),
            session: self.current_meta.session_id.clone(),
            severity: None,
            fields: BTreeMap::new(),
        });
        if event_contains_disconnect(event, detail) {
            self.metrics.disconnect_count = self.metrics.disconnect_count.saturating_add(1);
        }
        if event_contains_watchdog_error_timeout(event, detail) {
            self.metrics.watchdog_error_timeout_count =
                self.metrics.watchdog_error_timeout_count.saturating_add(1);
        }
        Ok(())
    }

    fn raw_gatt(
        &mut self,
        _collector_ns: u128,
        _characteristic_uuid: &str,
        _direction: &str,
        _operation: &str,
        _payload: &[u8],
        _detail: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        if let Some(gap_ms) = timing
            .gap_ms
            .filter(|gap| *gap > PMD_HISTORY_GAP_THRESHOLD_MS)
        {
            self.metrics.pmd_gap_gt20ms_count = self.metrics.pmd_gap_gt20ms_count.saturating_add(1);
            self.metrics.pmd_max_positive_gap_ms = Some(
                self.metrics
                    .pmd_max_positive_gap_ms
                    .map(|current| current.max(gap_ms))
                    .unwrap_or(gap_ms),
            );
        }
        Ok(())
    }

    fn ecg(
        &mut self,
        _collector_ns: u128,
        _device_ts: u64,
        _index: usize,
        _microvolts: i32,
    ) -> Result<()> {
        Ok(())
    }

    fn acc(
        &mut self,
        _collector_ns: u128,
        _device_ts: u64,
        _index: usize,
        _x: i32,
        _y: i32,
        _z: i32,
    ) -> Result<()> {
        Ok(())
    }

    fn hr(&mut self, _collector_ns: u128, _data: &HrData, _raw: &[u8]) -> Result<()> {
        Ok(())
    }

    fn battery(&mut self, collector_ns: u128, level: u8, _reason: &str) -> Result<()> {
        self.metrics.battery_latch.note(collector_ns, level);
        Ok(())
    }

    fn health(
        &mut self,
        _collector_ns: u128,
        _rssi: Option<i16>,
        _pmd_frames: u64,
        _hr_frames: u64,
    ) -> Result<()> {
        Ok(())
    }
}

fn event_contains_disconnect(event: &str, detail: &str) -> bool {
    let text = format!("{} {}", event, detail).to_ascii_lowercase();
    text.contains("disconnect") || text.contains("disconnected")
}

fn event_contains_watchdog_error_timeout(event: &str, detail: &str) -> bool {
    let text = format!("{} {}", event, detail).to_ascii_lowercase();
    text.contains("watchdog")
        || text.contains("timeout")
        || text.contains("error")
        || text.contains("failed")
}

fn truncate_to_newest_events(events: &mut Vec<LiveEvent>, limit: usize) {
    events.sort_by(|left, right| {
        left.collector_unix_ns
            .cmp(&right.collector_unix_ns)
            .then(left.event.cmp(&right.event))
            .then(left.detail.cmp(&right.detail))
    });
    if events.len() > limit {
        let drop_count = events.len() - limit;
        events.drain(0..drop_count);
    }
}

fn scan_history(config: LiveServerConfig) -> Result<HistoryScanResult> {
    scan_history_at(config, unix_ns_i64_lossy())
}

fn scan_history_at(config: LiveServerConfig, now_unix_ns: i64) -> Result<HistoryScanResult> {
    let started = Instant::now();
    let history_cutoff = cutoff_ns(now_unix_ns, config.history_window);
    let history_window = CaptureTimeWindow {
        cutoff_unix_ns: history_cutoff,
        now_unix_ns,
    };
    let mut result = HistoryScanResult::default();
    let mut rssi_values = Vec::new();
    scan_history_source(
        &config.data_dir,
        &config.participant_id,
        H10_WAL_SOURCE,
        history_window,
        &mut result,
        &mut rssi_values,
        true,
    )?;
    scan_history_source(
        &config.data_dir,
        &config.participant_id,
        REPLAY_H10_SOURCE,
        history_window,
        &mut result,
        &mut rssi_values,
        true,
    )?;
    scan_history_source(
        &config.data_dir,
        &config.participant_id,
        MANUAL_EVENTS_SOURCE,
        history_window,
        &mut result,
        &mut rssi_values,
        false,
    )?;
    result.events.sort_by(|left, right| {
        left.collector_unix_ns
            .cmp(&right.collector_unix_ns)
            .then(left.event.cmp(&right.event))
            .then(left.detail.cmp(&right.detail))
    });
    result.events.dedup_by(|left, right| {
        left.collector_unix_ns == right.collector_unix_ns
            && left.event == right.event
            && left.detail == right.detail
            && left.session == right.session
    });
    truncate_to_newest_events(&mut result.events, EVENT_LIMIT);
    if !rssi_values.is_empty() {
        rssi_values.sort_by(|left, right| left.total_cmp(right));
        result.metrics.rssi_min_dbm = rssi_values.first().copied();
        result.metrics.rssi_median_dbm = rssi_values.get(rssi_values.len() / 2).copied();
    }
    result.metrics.elapsed_ms = duration_millis_u64(started.elapsed());
    result.index.note_complete();
    Ok(result)
}

fn scan_history_source(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    window: CaptureTimeWindow,
    result: &mut HistoryScanResult,
    rssi_values: &mut Vec<f64>,
    build_index: bool,
) -> Result<()> {
    let files = find_recent_wal_files(data_dir, participant_id, source, window.cutoff_unix_ns)?;
    let mut parser = PmdParser::default();
    let mut connection_ids = ConnectionIdMapper::default();
    for file in files {
        let mode = if file.is_open {
            WalReadMode::TailTolerant
        } else {
            WalReadMode::Strict
        };
        let read = read_wal_records_incremental(&file.path, mode, &HashMap::new())
            .with_context(|| format!("read history WAL {}", file.path.display()))?;
        if build_index {
            best_effort_update_live_index(
                data_dir,
                participant_id,
                source,
                &file,
                &read,
                &mut result.index,
            );
        }
        for frame in &read.records {
            for record in logical_wal_records(&frame.record) {
                result.metrics.records_scanned = result.metrics.records_scanned.saturating_add(1);
                if record.collector_unix_ns < window.cutoff_unix_ns {
                    continue;
                }
                if record.collector_unix_ns
                    > window.now_unix_ns.saturating_add(LIVE_FUTURE_TOLERANCE_NS)
                {
                    continue;
                }
                result.metrics.records_matched = result.metrics.records_matched.saturating_add(1);
                result.metrics.oldest_loaded_unix_ns = Some(
                    result
                        .metrics
                        .oldest_loaded_unix_ns
                        .map(|current| current.min(record.collector_unix_ns))
                        .unwrap_or(record.collector_unix_ns),
                );
                result.metrics.newest_loaded_unix_ns = Some(
                    result
                        .metrics
                        .newest_loaded_unix_ns
                        .map(|current| current.max(record.collector_unix_ns))
                        .unwrap_or(record.collector_unix_ns),
                );
                if let Err(error) = process_history_record(
                    record,
                    &mut parser,
                    &mut connection_ids,
                    &mut result.events,
                    &mut result.metrics,
                    rssi_values,
                    &mut result.timezone_offsets,
                ) {
                    let detail = format!(
                        "record_id={} kind={} path={} error={error:#}",
                        record.record_id,
                        record.kind.as_str(),
                        file.path.display()
                    );
                    eprintln!("quarantined malformed history WAL record: {detail}");
                    result
                        .events
                        .push(quarantined_record_event(record, "history", detail));
                    parser = PmdParser::default();
                }
            }
        }
        std::thread::yield_now();
    }
    Ok(())
}

fn quarantined_record_event(record: &WalRecord, phase: &str, detail: String) -> LiveEvent {
    let mut fields = BTreeMap::new();
    fields.insert(
        "record_id".to_string(),
        Value::String(record.record_id.to_string()),
    );
    fields.insert(
        "record_kind".to_string(),
        Value::String(record.kind.as_str().to_string()),
    );
    fields.insert("source".to_string(), Value::String(record.source.clone()));
    fields.insert("phase".to_string(), Value::String(phase.to_string()));
    if let Some(source_seq) = record.origin_source_seq {
        fields.insert("origin_source_seq".to_string(), Value::from(source_seq));
    }
    LiveEvent {
        collector_unix_ns: u128::try_from(record.collector_unix_ns).unwrap_or(0),
        event: "wal_record_quarantined".to_string(),
        detail,
        session: record.session_id.to_string(),
        severity: Some("error".to_string()),
        fields,
    }
}

fn process_history_record(
    record: &WalRecord,
    parser: &mut PmdParser,
    connection_ids: &mut ConnectionIdMapper,
    events: &mut Vec<LiveEvent>,
    metrics: &mut HistoryScanMetrics,
    rssi_values: &mut Vec<f64>,
    timezone_offsets: &mut BTreeMap<Uuid, i32>,
) -> Result<()> {
    if let WalBody::PsftpSetLocalTimeAttempt(body) = &record.body
        && let Some(connection_id) = record.connection_id
    {
        timezone_offsets.insert(connection_id, body.tz_offset_minutes);
    }
    let meta = record_meta_from_wal(record, connection_ids)?;
    let mut sink = HistoryRecordSink {
        events,
        metrics,
        current_meta: meta,
    };
    if let WalBody::CollectorEvent(body) = &record.body {
        let detail = collector_event_detail(body)?;
        sink.events.push(LiveEvent {
            collector_unix_ns: sink.current_meta.collector_unix_ns,
            event: body.event.clone(),
            detail: detail.clone(),
            session: sink.current_meta.session_id.clone(),
            severity: Some(body.severity.clone()),
            fields: body.fields.clone(),
        });
        if event_contains_disconnect(&body.event, &detail) {
            sink.metrics.disconnect_count = sink.metrics.disconnect_count.saturating_add(1);
        }
        if event_contains_watchdog_error_timeout(&body.event, &detail) {
            sink.metrics.watchdog_error_timeout_count =
                sink.metrics.watchdog_error_timeout_count.saturating_add(1);
        }
        return Ok(());
    }
    if route_live_operational_wal_record(record, &mut sink)? {
        return Ok(());
    }
    match &record.body {
        WalBody::RssiSample(body) => {
            let rssi = f64::from(body.rssi_dbm);
            sink.metrics.rssi_latest_dbm = Some(rssi);
            rssi_values.push(rssi);
        }
        WalBody::GattNotification(body) => {
            let payload = decode_payload_b64(
                &body.payload_b64,
                body.payload_len,
                "history_gatt_notification",
            )?;
            let _ = route_live_gatt_payload(
                sink.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                &body.description,
                &payload,
                parser,
                &mut sink,
            )?;
        }
        WalBody::GattReadResponse(body) => {
            let payload = decode_payload_b64(
                &body.payload_b64,
                body.payload_len,
                "history_gatt_read_response",
            )?;
            if is_battery_characteristic(&body.characteristic_uuid, &body.reason)
                && let Some(level) = payload.first()
            {
                sink.battery(sink.current_meta.collector_unix_ns, *level, &body.reason)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct LiveStatusContext<'a> {
    stream_id: Uuid,
    config: &'a LiveServerConfig,
    marker_enabled: bool,
    marker_status: &'a str,
    tailer: &'a WalTailer,
    critical: &'a CriticalLoadMetrics,
    history: &'a HistoryLoadMetrics,
    index: &'a IndexBuildMetrics,
    sse: &'a SseMetrics,
}

/// Battery-banner latch inputs, tracked as monotonic maxima so the result is
/// independent of the order readings arrive in (history scan vs live tail), the
/// same way the pairing banner compares pairing-breakdown vs streaming-progress
/// timestamps.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct BatteryLatch {
    low_latest_unix_ns: Option<u128>,
    low_latest_percent: Option<u8>,
    replaced_latest_unix_ns: Option<u128>,
    latest_unix_ns: Option<u128>,
    latest_percent: Option<u8>,
}

impl BatteryLatch {
    fn note(&mut self, collector_unix_ns: u128, percent: u8) {
        if self
            .latest_unix_ns
            .map(|ns| collector_unix_ns >= ns)
            .unwrap_or(true)
        {
            self.latest_unix_ns = Some(collector_unix_ns);
            self.latest_percent = Some(percent);
        }
        if percent <= BATTERY_LOW_TRIGGER_PERCENT
            && self
                .low_latest_unix_ns
                .map(|ns| collector_unix_ns >= ns)
                .unwrap_or(true)
        {
            self.low_latest_unix_ns = Some(collector_unix_ns);
            self.low_latest_percent = Some(percent);
        }
        if percent >= BATTERY_REPLACED_CLEAR_PERCENT {
            self.replaced_latest_unix_ns = Some(
                self.replaced_latest_unix_ns
                    .map(|ns| ns.max(collector_unix_ns))
                    .unwrap_or(collector_unix_ns),
            );
        }
    }

    fn merged(&self, other: &BatteryLatch) -> BatteryLatch {
        let mut result = *self;
        if let (Some(ns), Some(percent)) = (other.latest_unix_ns, other.latest_percent)
            && result.latest_unix_ns.map(|own| ns >= own).unwrap_or(true)
        {
            result.latest_unix_ns = Some(ns);
            result.latest_percent = Some(percent);
        }
        if let (Some(ns), Some(percent)) = (other.low_latest_unix_ns, other.low_latest_percent)
            && result
                .low_latest_unix_ns
                .map(|own| ns >= own)
                .unwrap_or(true)
        {
            result.low_latest_unix_ns = Some(ns);
            result.low_latest_percent = Some(percent);
        }
        if let Some(ns) = other.replaced_latest_unix_ns {
            result.replaced_latest_unix_ns = Some(
                result
                    .replaced_latest_unix_ns
                    .map(|own| own.max(ns))
                    .unwrap_or(ns),
            );
        }
        result
    }

    fn low_active(&self) -> bool {
        match (self.low_latest_unix_ns, self.replaced_latest_unix_ns) {
            (Some(low), Some(replaced)) => low > replaced,
            (Some(_), None) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct PhoneOperationalState {
    last_event_unix_ns: Option<u128>,
    connection_updated_unix_ns: Option<u128>,
    connected: bool,
    collection_updated_unix_ns: Option<u128>,
    collecting: bool,
    upload_updated_unix_ns: Option<u128>,
    uploading: bool,
    health_updated_unix_ns: Option<u128>,
    health_fields: BTreeMap<String, Value>,
    backlog_records: Option<u64>,
    backlog_bytes: Option<u64>,
    local_free_bytes: Option<u64>,
    phone_battery_percent: Option<u8>,
    power_save_mode: Option<bool>,
    device_interactive: Option<bool>,
    reported_ecg_age_ms: Option<f64>,
    reported_acc_age_ms: Option<f64>,
    reported_hr_age_ms: Option<f64>,
    last_upload_attempt: Option<String>,
    last_upload_ack: Option<String>,
    last_upload_error: Option<String>,
    last_disconnect_unix_ns: Option<u128>,
    last_disconnect: Option<String>,
    last_error_unix_ns: Option<u128>,
    last_error: Option<String>,
}

impl PhoneOperationalState {
    fn note_event(
        &mut self,
        collector_unix_ns: u128,
        event: &str,
        detail: &str,
        fields: &BTreeMap<String, Value>,
    ) {
        let lower = event.to_ascii_lowercase();
        if is_phone_activity_event(&lower) {
            self.last_event_unix_ns = max_option_u128(self.last_event_unix_ns, collector_unix_ns);
        }

        if lower == "collector_health" && is_newer(self.health_updated_unix_ns, collector_unix_ns) {
            self.health_updated_unix_ns = Some(collector_unix_ns);
            self.health_fields = fields.clone();
            self.backlog_records = value_u64(fields.get("backlog_records"));
            self.backlog_bytes = value_u64(fields.get("backlog_bytes"));
            self.local_free_bytes = value_u64(fields.get("local_free_bytes"));
            self.phone_battery_percent = value_u64(fields.get("phone_battery_percent"))
                .and_then(|value| u8::try_from(value).ok());
            self.power_save_mode = value_bool(fields.get("power_save_mode"));
            self.device_interactive = value_bool(fields.get("device_interactive"));
            self.reported_ecg_age_ms = value_f64(fields.get("ecg_age_ms"));
            self.reported_acc_age_ms = value_f64(fields.get("acc_age_ms"));
            self.reported_hr_age_ms = value_f64(fields.get("hr_age_ms"));
            self.note_collecting(collector_unix_ns, true);
        }

        if lower == "connect_result" && detail.contains("connected=true")
            || lower == "pmd_stream_start_confirmed"
        {
            self.note_connected(collector_unix_ns, true);
        }
        if lower.contains("disconnect") || lower == "android_service_stopped" {
            self.note_connected(collector_unix_ns, false);
            if is_newer(self.last_disconnect_unix_ns, collector_unix_ns) {
                self.last_disconnect_unix_ns = Some(collector_unix_ns);
                self.last_disconnect = Some(format!("{event}: {detail}"));
            }
        }
        if lower == "android_service_started"
            || lower == "pmd_stream_start_confirmed"
            || lower == "collector_health"
        {
            self.note_collecting(collector_unix_ns, true);
        }
        if lower == "android_service_stopped" || lower == "android_service_fatal" {
            self.note_collecting(collector_unix_ns, false);
        }

        if lower == "upload_attempt" && is_newer(self.upload_updated_unix_ns, collector_unix_ns) {
            self.upload_updated_unix_ns = Some(collector_unix_ns);
            self.uploading = true;
            self.last_upload_attempt = Some(detail.to_string());
        } else if lower == "upload_durable_ack"
            && is_newer(self.upload_updated_unix_ns, collector_unix_ns)
        {
            self.upload_updated_unix_ns = Some(collector_unix_ns);
            self.uploading = false;
            self.last_upload_ack = Some(detail.to_string());
        } else if lower == "upload_error"
            && is_newer(self.upload_updated_unix_ns, collector_unix_ns)
        {
            self.upload_updated_unix_ns = Some(collector_unix_ns);
            self.uploading = false;
            self.last_upload_error = Some(detail.to_string());
        }

        if is_phone_activity_event(&lower)
            && (event_contains_watchdog_error_timeout(event, detail)
                || lower.contains("fatal")
                || lower.contains("critical"))
            && is_newer(self.last_error_unix_ns, collector_unix_ns)
        {
            self.last_error_unix_ns = Some(collector_unix_ns);
            self.last_error = Some(format!("{event}: {detail}"));
        }
    }

    fn note_connected(&mut self, collector_unix_ns: u128, connected: bool) {
        if is_newer(self.connection_updated_unix_ns, collector_unix_ns) {
            self.connection_updated_unix_ns = Some(collector_unix_ns);
            self.connected = connected;
        }
    }

    fn note_collecting(&mut self, collector_unix_ns: u128, collecting: bool) {
        if is_newer(self.collection_updated_unix_ns, collector_unix_ns) {
            self.collection_updated_unix_ns = Some(collector_unix_ns);
            self.collecting = collecting;
        }
    }
}

/// Only events emitted by the phone/collector are allowed to refresh phone
/// liveness. Manual markers and server-owned WAL/session/recovery lifecycle
/// events can be written while the phone is absent and must never make it look
/// online. Keeping this as an allow-list also prevents a future server event
/// from accidentally acquiring liveness semantics merely because it is new.
fn is_phone_activity_event(event: &str) -> bool {
    matches!(
        event,
        "collector_health"
            | "android_service_started"
            | "android_service_stopped"
            | "android_service_fatal"
            | "upload_attempt"
            | "upload_durable_ack"
            | "upload_error"
            | "connect_attempt"
            | "connect_result"
            | "connect_error"
            | "disconnect_attempt"
            | "disconnect_result"
            | "disconnect_error"
            | "scan_attempt"
            | "scan_result"
            | "scan_error"
            | "central_event"
            | "rssi_read_error"
            | "service_discovery_summary"
            | "subscription_attempt"
            | "subscription_result"
            | "subscription_error"
            | "gatt_read_error"
            | "gatt_write_error"
            | "psftp_set_local_time_attempt"
            | "psftp_set_local_time_success"
            | "psftp_set_local_time_error"
            | "pmd_stream_start_confirmed"
            | "pmd_stream_start_failed"
            | "pmd_stream_stop_confirmed"
            | "pmd_stream_stop_failed"
            | "pairing_breakdown_detected"
            | "wal_progress"
            | "watchdog_timeout"
            | "ecg_watchdog_timeout"
            | "acc_watchdog_timeout"
            | "hr_watchdog_timeout"
            | "pmd_watchdog_timeout"
            | "notification_watchdog_timeout"
    )
}

fn is_newer(current: Option<u128>, candidate: u128) -> bool {
    current.map(|value| candidate >= value).unwrap_or(true)
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64).or_else(|| {
        value
            .and_then(Value::as_i64)
            .and_then(|value| u64::try_from(value).ok())
    })
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64)
}

fn value_bool(value: Option<&Value>) -> Option<bool> {
    value.and_then(Value::as_bool)
}

fn record_has_source_activity(record: &WalRecord) -> bool {
    match &record.body {
        WalBody::CollectorEvent(body) => is_phone_activity_event(&body.event.to_ascii_lowercase()),
        WalBody::SegmentStarted(_)
        | WalBody::SegmentClosed(_)
        | WalBody::SessionStarted(_)
        | WalBody::SessionEnded(_)
        | WalBody::RecoveryEvent(_)
        | WalBody::ManualMarker(_)
        | WalBody::CloudIngestBatchV1(_) => false,
        _ => true,
    }
}

fn packet_delay_ms(
    collector_unix_ns: u128,
    device_frame_ts_ns: u64,
    tz_offset_minutes: Option<i32>,
) -> Option<f64> {
    let collector = i128::try_from(collector_unix_ns).ok()?;
    let offset = i128::from(tz_offset_minutes?)
        .checked_mul(60)?
        .checked_mul(1_000_000_000)?;
    let since_2000 = collector
        .checked_sub(EPOCH_2000_UNIX_NS_I128)?
        .checked_add(offset)?;
    Some((since_2000 - i128::from(device_frame_ts_ns)) as f64 / 1_000_000.0)
}

#[derive(Default, Debug)]
struct LiveState {
    ecg: VecDeque<LiveEcg>,
    acc: VecDeque<LiveAcc>,
    hr: VecDeque<LiveHr>,
    rr: VecDeque<LiveRr>,
    battery: VecDeque<LiveBattery>,
    battery_latch: BatteryLatch,
    health: VecDeque<LiveHealth>,
    timing: VecDeque<LiveTiming>,
    events: VecDeque<LiveEvent>,
    changes: VecDeque<LiveDeltaItem>,
    next_sequence: u64,
    last_sensor_record_unix_ns: Option<u128>,
    latest_ecg_unix_ns: Option<u128>,
    latest_acc_unix_ns: Option<u128>,
    latest_hr_unix_ns: Option<u128>,
    latest_health_unix_ns: Option<u128>,
    phone: PhoneOperationalState,
    latest_source_unix_ns: Option<u128>,
    latest_source: Option<String>,
    latest_upload_received_unix_ns: Option<u128>,
    catch_up_records_skipped_live: u64,
    latest_catch_up_record_unix_ns: Option<u128>,
    future_records_skipped_live: u64,
    latest_future_record_unix_ns: Option<u128>,
    connection_timezone_offsets: BTreeMap<Uuid, i32>,
}

impl LiveState {
    fn note_source(&mut self, record: &WalRecord) {
        if !record_has_source_activity(record) {
            return;
        }
        let Ok(timestamp) = u128::try_from(record.collector_unix_ns) else {
            return;
        };
        if is_newer(self.latest_source_unix_ns, timestamp) {
            self.latest_source_unix_ns = Some(timestamp);
            self.latest_source = Some(record.source.clone());
        }
    }

    fn note_catch_up_record(&mut self, record: &WalRecord) {
        self.catch_up_records_skipped_live = self.catch_up_records_skipped_live.saturating_add(1);
        if let Ok(timestamp) = u128::try_from(record.collector_unix_ns) {
            self.latest_catch_up_record_unix_ns =
                max_option_u128(self.latest_catch_up_record_unix_ns, timestamp);
        }
    }

    fn note_future_record(&mut self, record: &WalRecord) {
        self.future_records_skipped_live = self.future_records_skipped_live.saturating_add(1);
        if let Ok(timestamp) = u128::try_from(record.collector_unix_ns) {
            self.latest_future_record_unix_ns =
                max_option_u128(self.latest_future_record_unix_ns, timestamp);
        }
    }

    fn note_cloud_batch_received(&mut self, record: &WalRecord, window: CaptureTimeWindow) {
        let WalBody::CloudIngestBatchV1(batch) = &record.body else {
            return;
        };
        let Some(received_unix_ns) = batch.received_unix_ms.checked_mul(1_000_000) else {
            return;
        };
        if received_unix_ns < window.cutoff_unix_ns
            || received_unix_ns > window.now_unix_ns.saturating_add(LIVE_FUTURE_TOLERANCE_NS)
        {
            return;
        }
        if let Ok(timestamp) = u128::try_from(received_unix_ns) {
            self.latest_upload_received_unix_ns =
                max_option_u128(self.latest_upload_received_unix_ns, timestamp);
        }
    }

    fn push_ecg(&mut self, value: LiveEcg) {
        self.note_ts(value.collector_unix_ns);
        self.latest_ecg_unix_ns = max_option_u128(self.latest_ecg_unix_ns, value.collector_unix_ns);
        push_limited(&mut self.ecg, value.clone(), ECG_LIMIT);
        self.push_delta(LiveDeltaKind::Ecg(value));
    }

    fn push_acc(&mut self, value: LiveAcc) {
        self.note_ts(value.collector_unix_ns);
        self.latest_acc_unix_ns = max_option_u128(self.latest_acc_unix_ns, value.collector_unix_ns);
        push_limited(&mut self.acc, value.clone(), ACC_LIMIT);
        self.push_delta(LiveDeltaKind::Acc(value));
    }

    fn push_hr(&mut self, value: LiveHr) {
        self.note_ts(value.collector_unix_ns);
        self.latest_hr_unix_ns = max_option_u128(self.latest_hr_unix_ns, value.collector_unix_ns);
        push_limited(&mut self.hr, value.clone(), HR_LIMIT);
        self.push_delta(LiveDeltaKind::Hr(value));
    }

    fn push_rr(&mut self, value: LiveRr) {
        self.note_ts(value.collector_unix_ns);
        push_limited(&mut self.rr, value.clone(), RR_LIMIT);
        self.push_delta(LiveDeltaKind::Rr(value));
    }

    fn push_battery(&mut self, value: LiveBattery) {
        self.note_ts(value.collector_unix_ns);
        self.battery_latch
            .note(value.collector_unix_ns, value.battery_percent);
        push_limited(&mut self.battery, value.clone(), BATTERY_LIMIT);
        self.push_delta(LiveDeltaKind::Battery(value));
    }

    fn push_health(&mut self, value: LiveHealth) {
        self.note_ts(value.collector_unix_ns);
        self.latest_health_unix_ns =
            max_option_u128(self.latest_health_unix_ns, value.collector_unix_ns);
        push_limited(&mut self.health, value.clone(), HEALTH_LIMIT);
        self.push_delta(LiveDeltaKind::Health(value));
    }

    fn push_timing(&mut self, value: LiveTiming) {
        self.note_ts(value.collector_unix_ns);
        push_limited(&mut self.timing, value.clone(), TIMING_LIMIT);
        self.push_delta(LiveDeltaKind::Timing(value));
    }

    fn push_event(&mut self, value: LiveEvent) {
        // Events and manual markers are useful operational history, but they must
        // not make sensor-data freshness look healthy when ECG/ACC/HR/health
        // samples are stale.
        self.phone.note_event(
            value.collector_unix_ns,
            &value.event,
            &value.detail,
            &value.fields,
        );
        push_limited(&mut self.events, value.clone(), EVENT_LIMIT);
        self.push_delta(LiveDeltaKind::Event(value));
    }

    fn push_delta(&mut self, change: LiveDeltaKind) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.changes.push_back(LiveDeltaItem {
            seq: self.next_sequence,
            change,
        });
        while self.changes.len() > DELTA_LIMIT {
            self.changes.pop_front();
        }
    }

    fn delta_batch_after(
        &self,
        after_seq: u64,
        max_items: usize,
        status: LiveStatus,
    ) -> LiveDeltaBatch {
        let first_available_seq = self.changes.front().map(|item| item.seq);
        let overflow = first_available_seq
            .map(|first| after_seq < first.saturating_sub(1))
            .unwrap_or(false);
        let items = self
            .changes
            .iter()
            .filter(|item| item.seq > after_seq)
            .take(max_items)
            .cloned()
            .collect::<Vec<_>>();
        let to_seq = items
            .last()
            .map(|item| item.seq)
            .unwrap_or(after_seq.max(self.next_sequence));
        LiveDeltaBatch {
            schema_version: 1,
            stream_id: status.stream_id,
            from_seq: after_seq,
            to_seq,
            latest_seq: self.next_sequence,
            overflow,
            items,
            status,
        }
    }

    fn note_ts(&mut self, collector_unix_ns: u128) {
        self.last_sensor_record_unix_ns = Some(
            self.last_sensor_record_unix_ns
                .map(|current| current.max(collector_unix_ns))
                .unwrap_or(collector_unix_ns),
        );
    }

    fn trim_before(&mut self, live_cutoff: u128, event_cutoff: u128) {
        trim_deque(&mut self.ecg, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.acc, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.hr, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.rr, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.battery, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.health, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.timing, live_cutoff, |row| row.collector_unix_ns);
        trim_deque(&mut self.events, event_cutoff, |row| row.collector_unix_ns);
    }

    fn merge_history_events(&mut self, events: Vec<LiveEvent>) {
        let mut existing = self.events.iter().cloned().collect::<Vec<_>>();
        for event in events {
            if !existing.iter().any(|old| {
                old.collector_unix_ns == event.collector_unix_ns
                    && old.event == event.event
                    && old.detail == event.detail
                    && old.session == event.session
            }) {
                existing.push(event.clone());
                self.push_delta(LiveDeltaKind::Event(event));
            }
        }
        existing.sort_by(|left, right| {
            left.collector_unix_ns
                .cmp(&right.collector_unix_ns)
                .then(left.event.cmp(&right.event))
                .then(left.detail.cmp(&right.detail))
        });
        while existing.len() > EVENT_LIMIT {
            existing.remove(0);
        }
        self.events = existing.into();
    }

    fn snapshot(&self, status: LiveStatus) -> DashboardSnapshot {
        let mut ecg = self.ecg.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut ecg, |row, i| row.i = i);
        let mut acc = self.acc.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut acc, |row, i| row.i = i);
        let mut hr = self.hr.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut hr, |row, i| row.i = i);
        let mut rr = self.rr.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut rr, |row, i| row.i = i);
        let mut health = self.health.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut health, |row, i| row.i = i);
        let mut timing = self.timing.iter().cloned().collect::<Vec<_>>();
        set_indices(&mut timing, |row, i| row.i = i);
        let latest_seq = self.next_sequence;
        DashboardSnapshot {
            schema_version: 1,
            stream_id: status.stream_id,
            latest_seq,
            data_dir: status.data_dir.clone(),
            status,
            ecg,
            acc,
            latest_acc: self.acc.back().cloned(),
            hr,
            latest_hr: self.hr.back().cloned(),
            rr,
            latest_battery: self.battery.back().cloned(),
            latest_health: self.health.back().cloned(),
            health,
            timing,
            events: self.events.iter().cloned().collect(),
        }
    }

    fn status(&self, context: LiveStatusContext<'_>) -> LiveStatus {
        let now = u128::try_from(unix_ns_i64_lossy()).unwrap_or(0);
        let config = context.config;
        let tailer = context.tailer;
        let critical = context.critical;
        let history = context.history;
        let index = context.index;
        let sse = context.sse;
        let live_lag_ms = age_ms(now, self.last_sensor_record_unix_ns);
        let ecg_age_ms = age_ms(now, self.latest_ecg_unix_ns);
        let acc_age_ms = age_ms(now, self.latest_acc_unix_ns);
        let hr_age_ms = age_ms(now, self.latest_hr_unix_ns);
        let health_age_ms = age_ms(now, self.latest_health_unix_ns);
        let phone_activity = max_optional_timestamp(
            self.last_sensor_record_unix_ns,
            self.phone.last_event_unix_ns,
        );
        let offline = !age_ms(now, phone_activity)
            .is_some_and(|age| age <= PHONE_OFFLINE_AFTER_NS as f64 / 1_000_000.0);
        let recent_sensor = age_ms(now, self.last_sensor_record_unix_ns)
            .is_some_and(|age| age <= PHONE_OFFLINE_AFTER_NS as f64 / 1_000_000.0);
        let upload_receive_age_ms = age_ms(now, self.latest_upload_received_unix_ns);
        let phone_upload_recent = upload_receive_age_ms
            .is_some_and(|age| age <= PHONE_OFFLINE_AFTER_NS as f64 / 1_000_000.0);
        let replay = self.latest_source.as_deref() == Some(REPLAY_H10_SOURCE) && !offline;
        let connected = !offline && (self.phone.connected || recent_sensor);
        let collecting = !offline
            && (self.phone.collecting
                || ecg_age_ms.is_some_and(|age| age <= 5_000.0)
                || acc_age_ms.is_some_and(|age| age <= 5_000.0));
        let uploading = !offline
            && self.phone.uploading
            && age_ms(now, self.phone.upload_updated_unix_ns)
                .is_some_and(|age| age <= UPLOAD_ACTIVE_AFTER_NS as f64 / 1_000_000.0);
        let connection_state = if replay {
            "replay"
        } else if offline {
            "offline"
        } else if uploading {
            "uploading"
        } else if collecting {
            "collecting"
        } else if connected {
            "connected"
        } else {
            "idle"
        };
        // Live-tail and history-scan readings can arrive in either order; merging
        // the two monotonic-maxima trackers keeps the latch order-insensitive.
        let battery_latch = self.battery_latch.merged(&history.battery_latch);
        LiveStatus {
            schema_version: 1,
            stream_id: context.stream_id,
            data_dir: config.data_dir.display().to_string(),
            marker_enabled: context.marker_enabled,
            marker_status: context.marker_status.to_string(),
            live_window_seconds: config.live_window.as_secs_f64(),
            history_window_hours: config.history_window.as_secs_f64() / 3600.0,
            critical_load_elapsed_ms: critical.elapsed_ms,
            critical_wal_records_processed: critical.records_processed,
            critical_load_index_state: critical.index_state.clone(),
            critical_load_start_offset: critical.start_offset,
            critical_load_start_path: critical.start_path.clone(),
            critical_load_last_error: critical.last_error.clone(),
            history_load_state: history.state.clone(),
            history_load_elapsed_ms: history.elapsed_ms.or_else(|| {
                history
                    .started_at
                    .map(|start| duration_millis_u64(start.elapsed()))
            }),
            history_records_scanned: history.records_scanned,
            history_records_matched: history.records_matched,
            history_oldest_loaded_unix_ns: history.oldest_loaded_unix_ns,
            history_newest_loaded_unix_ns: history.newest_loaded_unix_ns,
            history_last_error: history.last_error.clone(),
            history_rssi_latest_dbm: history.rssi_latest_dbm,
            history_rssi_min_dbm: history.rssi_min_dbm,
            history_rssi_median_dbm: history.rssi_median_dbm,
            history_disconnect_count: history.disconnect_count,
            history_watchdog_error_timeout_count: history.watchdog_error_timeout_count,
            history_pmd_gap_gt20ms_count: history.pmd_gap_gt20ms_count,
            history_pmd_max_positive_gap_ms: history.pmd_max_positive_gap_ms,
            index_build_state: index.state.clone(),
            index_points_written: index.points_written,
            index_last_error: index.last_error.clone(),
            last_wal_poll_elapsed_ms: tailer.last_poll_elapsed_ms,
            last_wal_poll_records_processed: tailer.last_poll_records_processed,
            live_lag_ms,
            last_sensor_record_unix_ns: self.last_sensor_record_unix_ns,
            latest_sensor_record_unix_ns: self.last_sensor_record_unix_ns,
            latest_ecg_unix_ns: self.latest_ecg_unix_ns,
            latest_acc_unix_ns: self.latest_acc_unix_ns,
            latest_hr_unix_ns: self.latest_hr_unix_ns,
            latest_health_unix_ns: self.latest_health_unix_ns,
            latest_hr_bpm: self.hr.back().map(|row| row.bpm),
            latest_rssi_dbm: self.health.back().and_then(|row| row.rssi),
            latest_contact_supported: self.hr.back().map(|row| row.contact_supported),
            latest_contact_detected: self.hr.back().map(|row| row.contact_detected),
            ecg_freshness_age_ms: ecg_age_ms,
            acc_freshness_age_ms: acc_age_ms,
            hr_freshness_age_ms: hr_age_ms,
            health_freshness_age_ms: health_age_ms,
            connection_state: connection_state.to_string(),
            phone_connected: connected,
            phone_collecting: collecting,
            phone_uploading: uploading,
            phone_upload_recent,
            replay,
            offline,
            phone_health_unix_ns: self.phone.health_updated_unix_ns,
            phone_health_fields: self.phone.health_fields.clone(),
            phone_backlog_records: self.phone.backlog_records,
            phone_backlog_bytes: self.phone.backlog_bytes,
            phone_local_free_bytes: self.phone.local_free_bytes,
            phone_battery_percent: self.phone.phone_battery_percent,
            phone_power_save_mode: self.phone.power_save_mode,
            phone_device_interactive: self.phone.device_interactive,
            phone_reported_ecg_age_ms: self.phone.reported_ecg_age_ms,
            phone_reported_acc_age_ms: self.phone.reported_acc_age_ms,
            phone_reported_hr_age_ms: self.phone.reported_hr_age_ms,
            last_upload_attempt: self.phone.last_upload_attempt.clone(),
            last_upload_ack: self.phone.last_upload_ack.clone(),
            last_upload_error: self.phone.last_upload_error.clone(),
            last_disconnect_unix_ns: self.phone.last_disconnect_unix_ns,
            last_disconnect: self.phone.last_disconnect.clone(),
            last_collector_error_unix_ns: self.phone.last_error_unix_ns,
            last_collector_error: self.phone.last_error.clone(),
            latest_source: self.latest_source.clone(),
            latest_upload_received_unix_ns: self.latest_upload_received_unix_ns,
            upload_receive_freshness_age_ms: upload_receive_age_ms,
            catch_up_records_skipped_live: self.catch_up_records_skipped_live,
            latest_catch_up_record_unix_ns: self.latest_catch_up_record_unix_ns,
            future_records_skipped_live: self.future_records_skipped_live,
            latest_future_record_unix_ns: self.latest_future_record_unix_ns,
            current_tz_offset_minutes: self.timing.back().and_then(|row| {
                row.tz_offset_minutes.or_else(|| {
                    row.connection_uuid
                        .and_then(|id| self.connection_timezone_offsets.get(&id).copied())
                })
            }),
            connection_timezone_offsets: self
                .connection_timezone_offsets
                .iter()
                .map(|(id, offset)| (id.to_string(), *offset))
                .collect(),
            battery_low_active: battery_latch.low_active(),
            battery_low_trigger_percent: BATTERY_LOW_TRIGGER_PERCENT,
            battery_replaced_clear_percent: BATTERY_REPLACED_CLEAR_PERCENT,
            battery_low_latest_unix_ns: battery_latch.low_latest_unix_ns,
            battery_low_latest_percent: battery_latch.low_latest_percent,
            battery_replaced_latest_unix_ns: battery_latch.replaced_latest_unix_ns,
            battery_latest_unix_ns: battery_latch.latest_unix_ns,
            battery_latest_percent: battery_latch.latest_percent,
            ecg_count: self.ecg.len(),
            acc_count: self.acc.len(),
            hr_count: self.hr.len(),
            rr_count: self.rr.len(),
            event_count: self.events.len(),
            wal_segments_read: tailer.segments_read,
            wal_records_processed: tailer.records_processed,
            latest_sequence: self.next_sequence,
            first_available_sequence: self.changes.front().map(|item| item.seq),
            delta_buffer_len: self.changes.len(),
            last_tail_error: tailer.last_error.clone(),
            sse_connected_clients: sse.connected_clients,
            sse_batches_sent: sse.batches_sent,
            sse_rows_sent: sse.rows_sent,
            sse_overflow_resync_count: sse.overflow_resync_count,
            sse_last_batch_size: sse.last_batch_size,
            sse_last_write_error: sse.last_write_error.clone(),
        }
    }
}

fn push_limited<T>(buffer: &mut VecDeque<T>, value: T, limit: usize) {
    buffer.push_back(value);
    while buffer.len() > limit {
        buffer.pop_front();
    }
}

fn max_option_u128(current: Option<u128>, value: u128) -> Option<u128> {
    Some(current.map(|current| current.max(value)).unwrap_or(value))
}

fn max_optional_timestamp(left: Option<u128>, right: Option<u128>) -> Option<u128> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn age_ms(now: u128, timestamp: Option<u128>) -> Option<f64> {
    timestamp.and_then(|value| {
        let future_tolerance = u128::try_from(LIVE_FUTURE_TOLERANCE_NS).unwrap_or(0);
        if value > now.saturating_add(future_tolerance) {
            None
        } else {
            Some(now.saturating_sub(value) as f64 / 1_000_000.0)
        }
    })
}

fn trim_deque<T, F>(buffer: &mut VecDeque<T>, cutoff: u128, timestamp: F)
where
    F: Fn(&T) -> u128,
{
    while buffer
        .front()
        .map(|row| timestamp(row) < cutoff)
        .unwrap_or(false)
    {
        buffer.pop_front();
    }
}

fn set_indices<T, F>(rows: &mut [T], mut set: F)
where
    F: FnMut(&mut T, usize),
{
    for (index, row) in rows.iter_mut().enumerate() {
        set(row, index);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DashboardSnapshot {
    pub schema_version: u16,
    pub stream_id: Uuid,
    pub latest_seq: u64,
    pub data_dir: String,
    pub status: LiveStatus,
    pub ecg: Vec<LiveEcg>,
    pub acc: Vec<LiveAcc>,
    pub latest_acc: Option<LiveAcc>,
    pub hr: Vec<LiveHr>,
    pub latest_hr: Option<LiveHr>,
    pub rr: Vec<LiveRr>,
    pub latest_battery: Option<LiveBattery>,
    pub latest_health: Option<LiveHealth>,
    pub health: Vec<LiveHealth>,
    pub timing: Vec<LiveTiming>,
    pub events: Vec<LiveEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveStatus {
    pub schema_version: u16,
    pub stream_id: Uuid,
    pub data_dir: String,
    pub marker_enabled: bool,
    pub marker_status: String,
    pub live_window_seconds: f64,
    pub history_window_hours: f64,
    pub critical_load_elapsed_ms: Option<u64>,
    pub critical_wal_records_processed: u64,
    pub critical_load_index_state: String,
    pub critical_load_start_offset: Option<u64>,
    pub critical_load_start_path: Option<String>,
    pub critical_load_last_error: Option<String>,
    pub history_load_state: String,
    pub history_load_elapsed_ms: Option<u64>,
    pub history_records_scanned: u64,
    pub history_records_matched: u64,
    pub history_oldest_loaded_unix_ns: Option<i64>,
    pub history_newest_loaded_unix_ns: Option<i64>,
    pub history_last_error: Option<String>,
    pub history_rssi_latest_dbm: Option<f64>,
    pub history_rssi_min_dbm: Option<f64>,
    pub history_rssi_median_dbm: Option<f64>,
    pub history_disconnect_count: u64,
    pub history_watchdog_error_timeout_count: u64,
    pub history_pmd_gap_gt20ms_count: u64,
    pub history_pmd_max_positive_gap_ms: Option<f64>,
    pub index_build_state: String,
    pub index_points_written: u64,
    pub index_last_error: Option<String>,
    pub last_wal_poll_elapsed_ms: Option<u64>,
    pub last_wal_poll_records_processed: usize,
    pub live_lag_ms: Option<f64>,
    pub last_sensor_record_unix_ns: Option<u128>,
    pub latest_sensor_record_unix_ns: Option<u128>,
    pub latest_ecg_unix_ns: Option<u128>,
    pub latest_acc_unix_ns: Option<u128>,
    pub latest_hr_unix_ns: Option<u128>,
    pub latest_health_unix_ns: Option<u128>,
    pub latest_hr_bpm: Option<u16>,
    pub latest_rssi_dbm: Option<f64>,
    pub latest_contact_supported: Option<bool>,
    pub latest_contact_detected: Option<bool>,
    pub ecg_freshness_age_ms: Option<f64>,
    pub acc_freshness_age_ms: Option<f64>,
    pub hr_freshness_age_ms: Option<f64>,
    pub health_freshness_age_ms: Option<f64>,
    pub connection_state: String,
    pub phone_connected: bool,
    pub phone_collecting: bool,
    pub phone_uploading: bool,
    pub phone_upload_recent: bool,
    pub replay: bool,
    pub offline: bool,
    pub phone_health_unix_ns: Option<u128>,
    pub phone_health_fields: BTreeMap<String, Value>,
    pub phone_backlog_records: Option<u64>,
    pub phone_backlog_bytes: Option<u64>,
    pub phone_local_free_bytes: Option<u64>,
    pub phone_battery_percent: Option<u8>,
    pub phone_power_save_mode: Option<bool>,
    pub phone_device_interactive: Option<bool>,
    pub phone_reported_ecg_age_ms: Option<f64>,
    pub phone_reported_acc_age_ms: Option<f64>,
    pub phone_reported_hr_age_ms: Option<f64>,
    pub last_upload_attempt: Option<String>,
    pub last_upload_ack: Option<String>,
    pub last_upload_error: Option<String>,
    pub last_disconnect_unix_ns: Option<u128>,
    pub last_disconnect: Option<String>,
    pub last_collector_error_unix_ns: Option<u128>,
    pub last_collector_error: Option<String>,
    pub latest_source: Option<String>,
    pub latest_upload_received_unix_ns: Option<u128>,
    pub upload_receive_freshness_age_ms: Option<f64>,
    pub catch_up_records_skipped_live: u64,
    pub latest_catch_up_record_unix_ns: Option<u128>,
    pub future_records_skipped_live: u64,
    pub latest_future_record_unix_ns: Option<u128>,
    pub current_tz_offset_minutes: Option<i32>,
    pub connection_timezone_offsets: BTreeMap<String, i32>,
    pub battery_low_active: bool,
    pub battery_low_trigger_percent: u8,
    pub battery_replaced_clear_percent: u8,
    pub battery_low_latest_unix_ns: Option<u128>,
    pub battery_low_latest_percent: Option<u8>,
    pub battery_replaced_latest_unix_ns: Option<u128>,
    pub battery_latest_unix_ns: Option<u128>,
    pub battery_latest_percent: Option<u8>,
    pub ecg_count: usize,
    pub acc_count: usize,
    pub hr_count: usize,
    pub rr_count: usize,
    pub event_count: usize,
    pub wal_segments_read: u64,
    pub wal_records_processed: u64,
    pub latest_sequence: u64,
    pub first_available_sequence: Option<u64>,
    pub delta_buffer_len: usize,
    pub last_tail_error: Option<String>,
    pub sse_connected_clients: u64,
    pub sse_batches_sent: u64,
    pub sse_rows_sent: u64,
    pub sse_overflow_resync_count: u64,
    pub sse_last_batch_size: usize,
    pub sse_last_write_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveDeltaBatch {
    pub schema_version: u16,
    pub stream_id: Uuid,
    pub from_seq: u64,
    pub to_seq: u64,
    pub latest_seq: u64,
    pub overflow: bool,
    pub items: Vec<LiveDeltaItem>,
    pub status: LiveStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveDeltaItem {
    pub seq: u64,
    #[serde(flatten)]
    pub change: LiveDeltaKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "row", rename_all = "snake_case")]
pub enum LiveDeltaKind {
    Ecg(LiveEcg),
    Acc(LiveAcc),
    Hr(LiveHr),
    Rr(LiveRr),
    Battery(LiveBattery),
    Health(LiveHealth),
    Timing(LiveTiming),
    Event(LiveEvent),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveEcg {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub device_ts_ns: u64,
    pub sample_index_in_frame: u32,
    pub microvolts: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveAcc {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub device_ts_ns: u64,
    pub sample_index_in_frame: u32,
    pub x: i32,
    pub raw_x: i32,
    pub y: i32,
    pub z: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveHr {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub bpm: u16,
    pub contact_supported: bool,
    pub contact_detected: bool,
    pub rr_ms: Vec<f64>,
    pub connection_id: u32,
    pub connection_uuid: Option<Uuid>,
    pub tz_offset_minutes: Option<i32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveRr {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub rr_index_in_frame: u32,
    pub rr_ms: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveBattery {
    pub collector_unix_ns: u128,
    pub battery_percent: u8,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveHealth {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub rssi: Option<f64>,
    pub pmd_frames: u64,
    pub hr_frames: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LiveTiming {
    pub i: usize,
    pub collector_unix_ns: u128,
    pub measurement: String,
    pub measurement_type: u8,
    pub frame_type: u8,
    pub compressed: bool,
    pub device_frame_ts_ns: u64,
    pub payload_len: u32,
    pub sample_count: u32,
    pub collector_delta_ms: Option<f64>,
    pub device_delta_ms: Option<f64>,
    pub expected_frame_delta_ms: Option<f64>,
    pub gap_ms: Option<f64>,
    pub connection_id: u32,
    pub connection_uuid: Option<Uuid>,
    pub tz_offset_minutes: Option<i32>,
    pub packet_delay_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveEvent {
    pub collector_unix_ns: u128,
    pub event: String,
    pub detail: String,
    pub session: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug)]
struct ManualMarkerWriter {
    _lock: SourceWriterLock,
    manager: Option<WalSegmentManager>,
    base_context: WalRecordContext,
    started_at: Instant,
}

impl ManualMarkerWriter {
    fn try_new(
        data_dir: &Path,
        participant_id: &str,
    ) -> Result<Option<(Self, Vec<WalRecoveryReport>)>> {
        let Some(lock) =
            SourceWriterLock::try_acquire(data_dir, participant_id, MANUAL_EVENTS_SOURCE)?
        else {
            return Ok(None);
        };
        let path_config = WalPathConfig::new(data_dir, participant_id, MANUAL_EVENTS_SOURCE)?;
        let writer_epoch_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let base_context = WalRecordContext::new(
            participant_id,
            MANUAL_EVENTS_SOURCE,
            LIVE_WRITER_ID,
            writer_epoch_id,
            session_id,
        )?;
        let recovery_end_ns = unix_ns_i64_lossy();
        let recovery_context = base_context.clone().at_time(recovery_end_ns, 0);
        let recovery_reports =
            recover_stale_open_segments(&path_config, &recovery_context, recovery_end_ns)
                .context("recover stale manual-events WAL segments")?;
        let manager = WalSegmentManager::create(
            path_config,
            base_context.clone(),
            unix_ns_i64_lossy(),
            0,
            WalSegmentManagerOptions {
                writer_options: WalWriterOptions::default(),
                ..WalSegmentManagerOptions::default()
            },
        )
        .context("open manual-events WAL segment")?;
        let mut writer = Self {
            _lock: lock,
            manager: Some(manager),
            base_context,
            started_at: Instant::now(),
        };
        for report in &recovery_reports {
            writer.append_recovery_event(report)?;
        }
        Ok(Some((writer, recovery_reports)))
    }

    fn append_recovery_event(&mut self, report: &WalRecoveryReport) -> Result<()> {
        let now = unix_ns_i64_lossy();
        let monotonic = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let body = RecoveryEventBody {
            recovered_path: report.path.display().to_string(),
            action: report.action.as_str().to_string(),
            valid_records: report.valid_records,
            truncated_tail_bytes: report.truncated_tail_bytes,
            message: report.message.clone(),
        };
        let record = self
            .base_context
            .clone()
            .at_time(now, monotonic)
            .make_record(WalBody::RecoveryEvent(body));
        self.manager
            .as_mut()
            .ok_or_else(|| anyhow!("manual-events WAL writer is closed"))?
            .append_record(&record)
            .context("append manual-events recovery WAL record")?;
        Ok(())
    }

    fn append_marker(&mut self, request: MarkerPostRequest) -> Result<MarkerPostResponse> {
        let label = clean_marker_label(request.label);
        let note = request.note.and_then(clean_optional_string);
        let client_id = request.client_id.and_then(clean_optional_string);
        let client_event_id = request.client_event_id.and_then(clean_optional_string);
        let now = unix_ns_i64_lossy();
        let monotonic = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let event_id = Uuid::new_v4();
        let body = ManualMarkerBody {
            event_id,
            event_unix_ns: now,
            event_monotonic_ns: Some(monotonic),
            event_time_source: "dashboard_server_receive".to_string(),
            created_by: "dashboard".to_string(),
            label: label.clone(),
            note,
            client_id,
            client_event_id,
            dedupe_key: event_id.hyphenated().to_string(),
        };
        let record = self
            .base_context
            .clone()
            .at_time(now, monotonic)
            .make_record(WalBody::ManualMarker(body));
        let manager = self
            .manager
            .as_mut()
            .ok_or_else(|| anyhow!("manual-events WAL writer is closed"))?;
        manager
            .append_record(&record)
            .context("append manual marker WAL record")?;
        manager
            .sync_all()
            .context("fsync manual marker WAL before HTTP acknowledgement")?;
        let wal_path = manager
            .current_open_path()
            .map(|path| path.display().to_string());
        Ok(MarkerPostResponse {
            ok: true,
            event_id,
            label,
            collector_unix_ns: now,
            wal_path,
        })
    }

    fn close(&mut self, reason: &str) -> Result<()> {
        let Some(manager) = self.manager.take() else {
            return Ok(());
        };
        let end_ns = unix_ns_i64_lossy();
        let monotonic = u64::try_from(self.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        manager.close(end_ns, monotonic, reason)?;
        Ok(())
    }
}

fn clean_marker_label(label: Option<String>) -> String {
    label
        .and_then(clean_optional_string)
        .unwrap_or_else(|| "dashboard_marker".to_string())
}

fn clean_optional_string(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(512).collect())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct MarkerPostRequest {
    pub label: Option<String>,
    pub note: Option<String>,
    pub client_id: Option<String>,
    pub client_event_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarkerPostResponse {
    pub ok: bool,
    pub event_id: Uuid,
    pub label: String,
    pub collector_unix_ns: i64,
    pub wal_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WalFileInfo {
    path: PathBuf,
    start_unix_ns: i64,
    end_unix_ns: Option<i64>,
    is_open: bool,
}

fn find_recent_wal_files(
    data_dir: &Path,
    participant_id: &str,
    source: &str,
    cutoff_unix_ns: i64,
) -> Result<Vec<WalFileInfo>> {
    let path_config = WalPathConfig::new(data_dir, participant_id, source)?;
    let source_dir = path_config.source_dir();
    if !source_dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for date_entry in fs::read_dir(&source_dir)
        .with_context(|| format!("scan live WAL source dir {}", source_dir.display()))?
    {
        let date_entry = date_entry?;
        let date_path = date_entry.path();
        if !date_path.is_dir() {
            continue;
        }
        let Some(date_name) = date_path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !date_name.starts_with("date=") {
            continue;
        }
        for file_entry in fs::read_dir(&date_path)
            .with_context(|| format!("scan live WAL date dir {}", date_path.display()))?
        {
            let file_entry = file_entry?;
            let path = file_entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(info) = parse_wal_file_name(&path, name)? else {
                continue;
            };
            if info.is_open || info.end_unix_ns.unwrap_or(info.start_unix_ns) >= cutoff_unix_ns {
                out.push(info);
            }
        }
    }
    out.sort_by(|left, right| {
        left.start_unix_ns
            .cmp(&right.start_unix_ns)
            .then(left.end_unix_ns.cmp(&right.end_unix_ns))
            .then(left.path.cmp(&right.path))
    });
    Ok(out)
}

fn parse_wal_file_name(path: &Path, name: &str) -> Result<Option<WalFileInfo>> {
    if name.ends_with(".open.tmp") || name.ends_with(".archived.json") || name.ends_with(".tmp") {
        return Ok(None);
    }
    if let Some(stem) = name
        .strip_prefix("wal-")
        .and_then(|value| value.strip_suffix(".open"))
    {
        if !stem.is_ascii() || stem.len() < WAL_TS_FILENAME_LEN + 1 {
            return Ok(None);
        }
        let start = parse_wal_filename_timestamp(&stem[0..WAL_TS_FILENAME_LEN])?;
        return Ok(Some(WalFileInfo {
            path: path.to_path_buf(),
            start_unix_ns: start,
            end_unix_ns: None,
            is_open: true,
        }));
    }
    if let Some(stem) = name
        .strip_prefix("wal-")
        .and_then(|value| value.strip_suffix(".closed"))
    {
        if !stem.is_ascii() || stem.len() < WAL_TS_FILENAME_LEN * 2 + 2 {
            return Ok(None);
        }
        let start = parse_wal_filename_timestamp(&stem[0..WAL_TS_FILENAME_LEN])?;
        let end_offset = WAL_TS_FILENAME_LEN + 1;
        let end =
            parse_wal_filename_timestamp(&stem[end_offset..end_offset + WAL_TS_FILENAME_LEN])?;
        return Ok(Some(WalFileInfo {
            path: path.to_path_buf(),
            start_unix_ns: start,
            end_unix_ns: Some(end),
            is_open: false,
        }));
    }
    Ok(None)
}

fn parse_wal_filename_timestamp(text: &str) -> Result<i64> {
    if text.len() != WAL_TS_FILENAME_LEN
        || text.as_bytes().get(8) != Some(&b'T')
        || text.as_bytes().get(15) != Some(&b'.')
        || text.as_bytes().get(25) != Some(&b'Z')
    {
        bail!("invalid WAL filename timestamp {text}");
    }
    let year: i32 = text[0..4].parse()?;
    let month: u32 = text[4..6].parse()?;
    let day: u32 = text[6..8].parse()?;
    let hour: u32 = text[9..11].parse()?;
    let minute: u32 = text[11..13].parse()?;
    let second: u32 = text[13..15].parse()?;
    let nanos: u32 = text[16..25].parse()?;
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| anyhow!("invalid WAL timestamp date {text}"))?;
    let time = NaiveTime::from_hms_nano_opt(hour, minute, second, nanos)
        .ok_or_else(|| anyhow!("invalid WAL timestamp time {text}"))?;
    let dt = DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::new(date, time), Utc);
    dt.timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("WAL timestamp out of range {text}"))
}

async fn handle_connection(mut stream: TcpStream, runtime: Arc<Mutex<LiveRuntime>>) -> Result<()> {
    let request = read_http_request(&mut stream).await?;
    let path = request.path.split('?').next().unwrap_or(&request.path);
    match (request.method.as_str(), path) {
        ("GET", "/") => {
            write_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                DASHBOARD_HTML.as_bytes(),
            )
            .await
        }
        ("GET", "/live-worker.js") => {
            write_response(
                &mut stream,
                200,
                "text/javascript; charset=utf-8",
                LIVE_WORKER_JS.as_bytes(),
            )
            .await
        }
        ("GET", "/api/status") => {
            let status = runtime
                .lock()
                .map_err(|_| anyhow!("live runtime lock poisoned"))?
                .status();
            write_json(&mut stream, 200, &status).await
        }
        ("GET", "/api/latest") => {
            let snapshot = runtime
                .lock()
                .map_err(|_| anyhow!("live runtime lock poisoned"))?
                .snapshot();
            write_json(&mut stream, 200, &snapshot).await
        }
        ("GET", "/events") => write_sse(stream, runtime, &request.path).await,
        ("POST", "/api/marker") => {
            let marker_request = if request.body.is_empty() {
                MarkerPostRequest {
                    label: None,
                    note: None,
                    client_id: None,
                    client_event_id: None,
                }
            } else {
                serde_json::from_slice::<MarkerPostRequest>(&request.body)
                    .context("parse marker POST JSON")?
            };
            let post_result = runtime
                .lock()
                .map_err(|_| anyhow!("live runtime lock poisoned"))?
                .post_marker(marker_request);
            match post_result {
                Ok(response) => write_json(&mut stream, 200, &response).await,
                Err(err) => {
                    let body = serde_json::json!({"ok": false, "error": format!("{err:#}")});
                    write_json(&mut stream, 503, &body).await
                }
            }
        }
        _ => {
            let body = serde_json::json!({"ok": false, "error": "not found"});
            write_json(&mut stream, 404, &body).await
        }
    }
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut buf = Vec::new();
    let header_end;
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await.context("read HTTP request")?;
        if n == 0 {
            bail!("connection closed before HTTP headers");
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > HTTP_READ_LIMIT {
            bail!("HTTP request too large");
        }
        if let Some(pos) = find_header_end(&buf) {
            header_end = pos;
            break;
        }
    }
    let header_bytes = &buf[..header_end];
    let header_text = std::str::from_utf8(header_bytes).context("HTTP headers are not UTF-8")?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| anyhow!("missing HTTP request line"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| anyhow!("missing HTTP method"))?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| anyhow!("missing HTTP path"))?
        .to_string();
    let mut content_length = 0usize;
    for line in lines {
        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = value.trim().parse().context("parse Content-Length")?;
        } else if let Some(value) = line.strip_prefix("content-length:") {
            content_length = value.trim().parse().context("parse content-length")?;
        }
    }
    if content_length > HTTP_READ_LIMIT {
        bail!("HTTP body too large");
    }
    let body_start = header_end + 4;
    let mut body = buf.get(body_start..).unwrap_or_default().to_vec();
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = vec![0_u8; remaining.min(8192)];
        let n = stream.read(&mut chunk).await.context("read HTTP body")?;
        if n == 0 {
            bail!("connection closed before HTTP body complete");
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok(HttpRequest { method, path, body })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

async fn write_json<T: Serialize>(stream: &mut TcpStream, status: u16, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value).context("serialize JSON response")?;
    write_response(stream, status, "application/json", &body).await
}

async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
        body.len()
    );
    stream
        .write_all(header.as_bytes())
        .await
        .context("write HTTP response header")?;
    stream
        .write_all(body)
        .await
        .context("write HTTP response body")?;
    Ok(())
}

async fn write_sse(
    mut stream: TcpStream,
    runtime: Arc<Mutex<LiveRuntime>>,
    request_path: &str,
) -> Result<()> {
    {
        let mut runtime = runtime
            .lock()
            .map_err(|_| anyhow!("live runtime lock poisoned"))?;
        runtime.sse_client_connected();
    }
    let result = write_sse_inner(&mut stream, runtime.clone(), request_path).await;
    if let Err(err) = &result
        && let Ok(mut runtime) = runtime.lock()
    {
        runtime.note_sse_write_error(format!("{err:#}"));
    }
    if let Ok(mut runtime) = runtime.lock() {
        runtime.sse_client_disconnected();
    }
    result
}

async fn write_sse_inner(
    stream: &mut TcpStream,
    runtime: Arc<Mutex<LiveRuntime>>,
    request_path: &str,
) -> Result<()> {
    let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
    stream
        .write_all(header.as_bytes())
        .await
        .context("write SSE header")?;
    let mut last_seq = query_u64(request_path, "since").unwrap_or_else(|| {
        runtime
            .lock()
            .map(|runtime| runtime.status().latest_sequence)
            .unwrap_or(0)
    });
    let mut tick = interval(DEFAULT_POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut idle_ticks = SSE_HEARTBEAT_TICKS;
    loop {
        tick.tick().await;
        let batch = runtime
            .lock()
            .map_err(|_| anyhow!("live runtime lock poisoned"))?
            .delta_batch_after(last_seq, SSE_BATCH_LIMIT);
        if !batch.items.is_empty() || batch.overflow {
            last_seq = batch.to_seq;
            let rows = batch.items.len();
            let overflow = batch.overflow;
            if !write_sse_json_event(stream, "batch", Some(batch.to_seq), &batch).await? {
                break;
            }
            if let Ok(mut runtime) = runtime.lock() {
                runtime.note_sse_batch(rows, overflow);
            }
            idle_ticks = 0;
        } else {
            idle_ticks = idle_ticks.saturating_add(1);
            if idle_ticks >= SSE_HEARTBEAT_TICKS {
                let status = runtime
                    .lock()
                    .map_err(|_| anyhow!("live runtime lock poisoned"))?
                    .status();
                if !write_sse_json_event(stream, "status", Some(status.latest_sequence), &status)
                    .await?
                {
                    break;
                }
                idle_ticks = 0;
            }
        }
    }
    Ok(())
}

async fn write_sse_json_event<T: Serialize>(
    stream: &mut TcpStream,
    event_name: &str,
    event_id: Option<u64>,
    value: &T,
) -> Result<bool> {
    let data = serde_json::to_string(value).context("serialize SSE JSON event")?;
    let mut event = String::new();
    if let Some(id) = event_id {
        event.push_str(&format!("id: {id}\n"));
    }
    event.push_str(&format!("event: {event_name}\n"));
    event.push_str("data: ");
    event.push_str(&data.replace('\n', "\\n"));
    event.push_str("\n\n");
    match stream.write_all(event.as_bytes()).await {
        Ok(()) => Ok(true),
        Err(err)
            if err.kind() == ErrorKind::BrokenPipe || err.kind() == ErrorKind::ConnectionReset =>
        {
            Ok(false)
        }
        Err(err) => Err(err).context("write SSE event"),
    }
}

fn query_u64(path: &str, key: &str) -> Option<u64> {
    let query = path.split_once('?')?.1;
    for part in query.split('&') {
        let (name, value) = part.split_once('=').unwrap_or((part, ""));
        if name == key
            && let Ok(parsed) = value.parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

pub(crate) const LIVE_WORKER_JS: &str = r##"'use strict';

const ports = new Set();
const mismatchNotices = new Map();
const RECENT_BATCH_LIMIT = 256;
const FORCED_RECOVERY_COOLDOWN_MS = 5000;
const recentBatches = [];
let eventSource = null;
let reconnectTimer = null;
let reconnectDelayMs = 1000;
let requestedSince = 0;
let activeStreamId = null;
let transportState = 'connecting';
let lastForcedRecoveryStreamId = null;
let lastForcedRecoveryTarget = 0;
let lastForcedRecoveryAtMs = -Infinity;

function broadcast(message, exceptPort = null) {
  for (const port of [...ports]) {
    if (port === exceptPort) continue;
    try {
      port.postMessage(message);
    } catch (_) {
      ports.delete(port);
      mismatchNotices.delete(port);
    }
  }
  if (ports.size === 0) stopEventSourceIfUnused();
}

function noteEventId(event) {
  const sequence = Number(event.lastEventId);
  if (Number.isFinite(sequence)) requestedSince = Math.max(requestedSince, sequence);
}

function eventStreamDetails(parsed, latestSequenceField) {
  const streamId = typeof parsed?.stream_id === 'string' && parsed.stream_id ? parsed.stream_id : null;
  const latestSequence = Number(parsed?.[latestSequenceField]);
  if (!streamId || !Number.isFinite(latestSequence) || latestSequence < 0) return null;
  return {streamId, latestSequence};
}

function resetForStream(streamId, latestSequence) {
  const source = eventSource;
  eventSource = null;
  if (source) source.close();
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  reconnectDelayMs = 1000;
  requestedSince = latestSequence;
  activeStreamId = streamId;
  lastForcedRecoveryStreamId = streamId;
  lastForcedRecoveryTarget = 0;
  lastForcedRecoveryAtMs = -Infinity;
  recentBatches.length = 0;
  mismatchNotices.clear();
  transportState = 'reconnecting';
  broadcast({type:'resync', reason:'stream_generation_changed', streamId, since:latestSequence});
  broadcast({type:'transport', mode:'shared_worker', state:transportState});
  startEventSource();
}

function recoverSameStream(requestingPort, streamId, since, observedLatestSequence) {
  if (!streamId || streamId !== activeStreamId) {
    requestingPort.postMessage({type:'resync', reason:'worker_stream_mismatch'});
    return;
  }
  const cursor = Number(since);
  const observed = Number(observedLatestSequence);
  if (!Number.isFinite(cursor) || cursor < 0 || !Number.isFinite(observed) || observed < cursor) {
    console.error('dropping invalid same-generation stream recovery request');
    return;
  }
  const target = Math.max(cursor, observed);
  const nowMs = Date.now();
  const duplicateTarget = lastForcedRecoveryStreamId === streamId && target <= lastForcedRecoveryTarget;
  const insideCooldown = lastForcedRecoveryStreamId === streamId
    && nowMs - lastForcedRecoveryAtMs < FORCED_RECOVERY_COOLDOWN_MS;
  if (duplicateTarget || insideCooldown) return;

  lastForcedRecoveryStreamId = streamId;
  lastForcedRecoveryTarget = target;
  lastForcedRecoveryAtMs = nowMs;
  const source = eventSource;
  eventSource = null;
  if (source) source.close();
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  reconnectDelayMs = 1000;
  requestedSince = cursor;
  recentBatches.length = 0;
  mismatchNotices.clear();
  transportState = 'reconnecting';
  broadcast({type:'resync', reason:'same_generation_stall', streamId, since:cursor}, requestingPort);
  broadcast({type:'transport', mode:'shared_worker', state:transportState});
  startEventSource();
}

function acceptEventStream(parsed, latestSequenceField) {
  const details = eventStreamDetails(parsed, latestSequenceField);
  if (!details) {
    console.error('dropping live SSE event without a valid stream generation');
    return false;
  }
  if (activeStreamId === null) {
    activeStreamId = details.streamId;
  } else if (details.streamId !== activeStreamId) {
    resetForStream(details.streamId, details.latestSequence);
    return false;
  }
  return true;
}

function rememberBatch(event) {
  let parsed;
  try {
    parsed = JSON.parse(event.data);
  } catch (error) {
    console.error('dropping malformed live SSE batch', error);
    return null;
  }
  if (!acceptEventStream(parsed, 'latest_seq')) return null;
  const message = {
    type:'batch',
    data:event.data,
    fromSeq:Number(parsed.from_seq),
    toSeq:Number(parsed.to_seq)
  };
  if (!Number.isFinite(message.fromSeq) || !Number.isFinite(message.toSeq) || message.toSeq < message.fromSeq) {
    console.error('dropping live SSE batch with invalid sequence range');
    return null;
  }
  recentBatches.push(message);
  while (recentBatches.length > RECENT_BATCH_LIMIT) recentBatches.shift();
  return message;
}

function sendCatchup(port, since) {
  const oldest = recentBatches[0];
  if (oldest && since < oldest.fromSeq) {
    port.postMessage({type:'resync'});
    return;
  }
  for (const batch of recentBatches) {
    if (batch.toSeq > since) port.postMessage(batch);
  }
}

function scheduleReconnect() {
  if (reconnectTimer || ports.size === 0) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    startEventSource();
  }, reconnectDelayMs);
  reconnectDelayMs = Math.min(30000, reconnectDelayMs * 2);
}

function startEventSource() {
  if (eventSource || ports.size === 0) return;
  const source = new EventSource(`/events?since=${requestedSince}`);
  eventSource = source;
  source.onopen = () => {
    if (eventSource !== source) return;
    reconnectDelayMs = 1000;
    transportState = 'connected';
    broadcast({type:'transport', mode:'shared_worker', state:transportState});
  };
  source.addEventListener('batch', event => {
    if (eventSource !== source) return;
    const batch = rememberBatch(event);
    if (batch) {
      noteEventId(event);
      broadcast(batch);
    }
  });
  source.addEventListener('status', event => {
    if (eventSource !== source) return;
    let parsed;
    try {
      parsed = JSON.parse(event.data);
    } catch (error) {
      console.error('dropping malformed live SSE status', error);
      return;
    }
    if (!acceptEventStream(parsed, 'latest_sequence')) return;
    noteEventId(event);
    broadcast({type:'status', data:event.data});
  });
  source.onerror = () => {
    if (eventSource !== source) return;
    transportState = 'reconnecting';
    broadcast({type:'transport', mode:'shared_worker', state:transportState});
    source.close();
    eventSource = null;
    scheduleReconnect();
  };
}

function stopEventSourceIfUnused() {
  if (ports.size !== 0) return;
  if (eventSource) eventSource.close();
  eventSource = null;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  reconnectDelayMs = 1000;
  requestedSince = 0;
  activeStreamId = null;
  lastForcedRecoveryStreamId = null;
  lastForcedRecoveryTarget = 0;
  lastForcedRecoveryAtMs = -Infinity;
  recentBatches.length = 0;
  mismatchNotices.clear();
}

self.onconnect = event => {
  const port = event.ports[0];
  ports.add(port);
  port.onmessage = messageEvent => {
    const message = messageEvent.data || {};
    if (message.type === 'subscribe') {
      const sequence = Number(message.since);
      const since = Number.isFinite(sequence) ? Math.max(0, sequence) : 0;
      const subscribedStreamId = typeof message.streamId === 'string' && message.streamId ? message.streamId : null;
      if (!eventSource && !reconnectTimer) {
        requestedSince = since;
        if (activeStreamId === null && subscribedStreamId) activeStreamId = subscribedStreamId;
      }
      if (activeStreamId && subscribedStreamId && subscribedStreamId !== activeStreamId) {
        const mismatchKey = `${subscribedStreamId}->${activeStreamId}`;
        if (mismatchNotices.get(port) !== mismatchKey) {
          mismatchNotices.set(port, mismatchKey);
          port.postMessage({type:'resync', reason:'worker_stream_mismatch'});
        }
      } else {
        mismatchNotices.delete(port);
        sendCatchup(port, since);
      }
      port.postMessage({type:'transport', mode:'shared_worker', state:transportState});
      startEventSource();
    } else if (message.type === 'recover') {
      recoverSameStream(port, message.streamId, message.since, message.observedLatestSequence);
    } else if (message.type === 'disconnect') {
      ports.delete(port);
      mismatchNotices.delete(port);
      port.close();
      stopEventSourceIfUnused();
    }
  };
  port.onmessageerror = () => {
    ports.delete(port);
    mismatchNotices.delete(port);
    port.close();
    stopEventSourceIfUnused();
  };
  port.start();
};
"##;

pub(crate) const DASHBOARD_HTML: &str = r##"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Training sensors H10 dashboard</title>
<style>
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #111; color: #eee; }
header { padding: 12px 18px; background: #1d1d1d; display: flex; gap: 24px; align-items: baseline; flex-wrap: wrap; }
main { padding: 16px; display: grid; grid-template-columns: minmax(420px, 2fr) minmax(320px, 1fr); gap: 16px; }
.card { background: #1b1b1b; border: 1px solid #333; border-radius: 10px; padding: 14px; box-shadow: 0 2px 12px #0008; }
canvas { width: 100%; height: 360px; background: #050505; border-radius: 8px; }
canvas.shortCanvas { height: 230px; }
canvas.hrCanvas { height: 150px; margin-top: 6px; }
#accBars { display: flex; min-height: 290px; gap: 24px; align-items: center; justify-content: space-around; flex-wrap: wrap; }
.axisWidget { display: grid; grid-template-rows: 20px auto 20px 24px; justify-items: center; gap: 6px; }
.axisTop, .axisBottom, .axisSide { color: #bbb; font-size: 12px; line-height: 1; }
.barWrap { width: 70px; height: 220px; border: 1px solid #444; position: relative; background: #080808; border-radius: 6px; overflow: hidden; }
.hbarWrap { width: 220px; height: 70px; border: 1px solid #444; position: relative; background: #080808; border-radius: 6px; overflow: hidden; }
.zero { position: absolute; left: 0; right: 0; top: 50%; border-top: 1px dashed #666; }
.hzero { position: absolute; top: 0; bottom: 0; left: 50%; border-left: 1px dashed #666; }
.bar { position: absolute; left: 10px; right: 10px; background: #58a6ff; }
.hbar { position: absolute; top: 10px; bottom: 10px; background: #f2cc60; }
.hAxisRow { display: flex; align-items: center; gap: 8px; }
.label { text-align: center; margin-top: 0; font-weight: 700; }
.small { color: #aaa; font-size: 13px; }
.legend, .plotLegend { display: inline-flex; gap: 14px; margin: 6px 0 8px 12px; flex-wrap: wrap; }
.plotSummary { margin: 2px 0 8px 12px; }
.swatch { display: inline-block; width: 14px; height: 3px; vertical-align: middle; margin-right: 4px; }
.ok { color: #7ee787; }
.bad { color: #ff7b72; }
pre { white-space: pre-wrap; font-size: 12px; max-height: 420px; overflow: auto; }
button.marker { background: #238636; color: white; border: 1px solid #2ea043; border-radius: 8px; padding: 10px 14px; font-weight: 700; cursor: pointer; }
button.marker:disabled { opacity: 0.6; cursor: wait; }
#markerStatus { margin-left: 10px; }
@media (max-width: 900px) { main { grid-template-columns: 1fr; } }
#actionBanner, #batteryBanner, #linkBanner { display:none; margin: 12px 0 0; padding: 16px 20px; border-radius: 8px; background: #b00020; color: #fff; font-size: 19px; font-weight: 700; line-height: 1.45; border: 2px solid #ff5252; box-shadow: 0 0 0 2px rgba(255,82,82,0.25); }
#actionBanner .sub, #batteryBanner .sub, #linkBanner .sub { display:block; margin-top:6px; font-weight: 400; font-size: 13px; opacity: 0.95; }
#linkBanner.buffering { background: #7a5c00; border-color: #ffc107; box-shadow: 0 0 0 2px rgba(255,193,7,0.25); }
</style>
</head>
<body>
<header>
  <h2>Polar H10 live dashboard</h2>
  <div id="status">loading…</div>
  <div class="small" id="dataDir"></div>
  <div class="small" id="browserTransport">browser transport: starting</div>
</header>
<div id="actionBanner"></div>
<div id="batteryBanner"></div>
<div id="linkBanner"></div>
<main>
  <section class="card">
    <h3>ECG waveform <span class="small">10 s sweep; the previous sweep stays ahead of the cursor, with the oldest ~0.2 s blanked just ahead of it</span></h3>
    <canvas id="ecg" width="1200" height="420"></canvas>
    <div class="small plotSummary">Accelerometer, same 10 s sweep</div>
    <div class="plotLegend small">
      <span><span class="swatch" style="background:#58a6ff"></span>X: superior/inferior (displayed as −raw X)</span>
      <span><span class="swatch" style="background:#f2cc60"></span>Y: lateral</span>
      <span><span class="swatch" style="background:#a371f7"></span>Z: supine/prone</span>
      <span>Units: mG</span>
    </div>
    <canvas id="acc" width="1200" height="300"></canvas>
    <div class="small plotSummary">Heart rate, same 10 s sweep</div>
    <canvas id="hrSweep" class="hrCanvas" width="1200" height="170"></canvas>
    <div class="small plotSummary">R-R intervals, same 10 s sweep</div>
    <canvas id="rrSweep" class="hrCanvas" width="1200" height="170"></canvas>
  </section>
  <section class="card">
    <h3>Heart / connection</h3>
    <div id="hrBox">—</div>
    <h3>Accelerometer latest (mG) <span class="small">raw WAL axes; dashboard shows X inverted</span></h3>
    <div id="accBars"></div>
  </section>
  <section class="card">
    <h3>Bluetooth connection strength (RSSI)</h3>
    <div id="rssiSummary" class="small plotSummary">Latest RSSI: —</div>
    <canvas id="rssi" class="shortCanvas" width="1200" height="260"></canvas>
    <div class="legend small">
      <span><span class="swatch" style="background:#1f7a3a"></span>strong ≥ -50 dBm</span>
      <span><span class="swatch" style="background:#2f80a0"></span>good -65..-50</span>
      <span><span class="swatch" style="background:#b88717"></span>weak -80..-65</span>
      <span><span class="swatch" style="background:#ad3434"></span>unstable &lt; -80</span>
    </div>
  </section>
  <section class="card">
    <h3>Packet delay <span class="small">phone-local receive time − H10 local frame time, using that connection's PS-FTP offset; orange = rolling mean of last 1000 PMD frames</span></h3>
    <canvas id="packetDelay" class="shortCanvas" width="1200" height="260"></canvas>
    <div id="timingBox">Waiting for PMD timing rows from WAL…</div>
  </section>
  <section class="card">
    <h3>WAL / live health</h3>
    <div id="walHealth">waiting for live status…</div>
  </section>
  <section class="card">
    <h3>Experiment marker</h3>
    <p class="small">Writes canonical <code>source=manual_events</code> WAL records. The H10 collector WAL is tailed read-only by this dashboard.</p>
    <button id="markLeave" class="marker" onclick="markEvent('leave_building_start')">Mark: leaving building now</button><span id="markerStatus" class="small">not marked yet</span>
  </section>
  <section class="card">
    <h3>Recent events <span class="small">last ~10 hours of WAL collector and manual-marker events</span></h3>
    <pre id="events"></pre>
  </section>
</main>
<script>
const EPOCH_2000_UNIX_NS = 946_684_800_000_000_000;
const ROLLING_DELAY_PACKETS = 1000;
const LONG_DRIFT_WINDOW_NS = 60 * 60 * 1_000_000_000;
const RR_TIMELINE_MAX_AGE_NS = 30 * 60 * 1_000_000_000;
const RR_TIMELINE_RESET_GAP_NS = 10 * 1_000_000_000;
const LIMITS = {ecg: 2000, acc: 2600, hr: 300, rr: 1000, health: 1000, timing: 2000, events: 2000};
const RENDER_REFRESH_MS = 100;
const STREAM_PROGRESS_GRACE_MS = 1500;
let latestSeq = 0;
let currentStreamId = null;
let eventSource = null;
let liveTransport = null;
let browserTransportMode = 'starting';
let browserTransportState = 'starting';
let resyncInFlight = false;
let resyncInFlightReason = null;
let resyncRetryTimer = null;
let resyncRetryDelayMs = 1000;
let resyncRetryOwner = null;
let resyncFollowupReason = null;
let streamLagTimer = null;
let streamLagObservation = null;
let pendingTransportRecovery = null;
let lastEventsText = '';
// Monotonic latest timestamps for the ACTION-NEEDED banner. Tracked at module scope (not just from
// the current, display-capped events buffer) so a long storm cannot scroll the original
// pairing_breakdown_detected out and silently hide the banner. Only ever advance.
let lastPairingBreakdownNs = 0;
let lastStreamingProgressNs = 0;
// Same monotonic-latch idea for the sensor->phone link-loss banner: device-clock ns so a
// post-outage catch-up drain of old failure events cannot false-trigger it.
let lastLinkFailureNs = 0;
let lastLinkRecoveryNs = 0;
// Shared 10 s sweep frame used by every live plot (ECG, ACC, HR, RR) so they all
// scroll on exactly the same timeline with one aligned blue playhead.
let currentSweep = null;
let sweepAnchorNs = null;
let lastSweepLatestTs = 0;
let rrState = {dataKey: null, lastNotificationNs: 0, lastRrTsNs: null, points: []};
let state = emptyState();
function emptyState() { return {schema_version:1, latest_seq:0, data_dir:'', status:{}, ecg:[], acc:[], hr:[], rr:[], health:[], timing:[], events:[], latest_acc:null, latest_hr:null, latest_battery:null, latest_health:null}; }
function asNumber(v, fallback=0) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }
function connectionOffsetNs(row, status) { const direct=row?.tz_offset_minutes; const mapped=row?.connection_uuid ? status?.connection_timezone_offsets?.[row.connection_uuid] : null; const minutes=direct ?? mapped; return (minutes === null || minutes === undefined) ? null : Number(minutes) * 60 * 1_000_000_000; }
function fmtNs(ns) { const n = asNumber(ns); return n ? new Date(n / 1_000_000).toLocaleTimeString() : '—'; }
function fmtNsDateTime(ns) { const n = asNumber(ns); return n ? new Date(n / 1_000_000).toLocaleString() : '—'; }
function cloneIndexed(rows) { return (rows || []).map((row, i) => ({...row, i})); }
function pushLimited(array, row, limit) { array.push(row); while (array.length > limit) array.shift(); }
function eventKey(row) { return `${row.collector_unix_ns}|${row.session || ''}|${row.event || ''}|${row.detail || ''}`; }
function mergeEventRow(row) { const key = eventKey(row); if (!state.events.some(existing => eventKey(existing) === key)) state.events.push(row); state.events.sort((a,b) => asNumber(a.collector_unix_ns) - asNumber(b.collector_unix_ns) || String(a.event || '').localeCompare(String(b.event || '')) || String(a.detail || '').localeCompare(String(b.detail || ''))); while (state.events.length > LIMITS.events) state.events.shift(); }
function streamIdOf(value) { return typeof value?.stream_id === 'string' && value.stream_id ? value.stream_id : null; }
function clearStreamLagObservation() {
  if (streamLagTimer) clearTimeout(streamLagTimer);
  streamLagTimer = null;
  streamLagObservation = null;
}
function armStreamLagTimer() {
  if (streamLagTimer || !streamLagObservation) return;
  streamLagTimer = setTimeout(() => {
    streamLagTimer = null;
    const observation = streamLagObservation;
    if (!observation || observation.streamId !== currentStreamId || latestSeq >= observation.latestSequence) {
      clearStreamLagObservation();
      return;
    }
    pendingTransportRecovery = {...observation};
    refreshSnapshotAndResubscribe('stall');
  }, STREAM_PROGRESS_GRACE_MS);
}
function cancelStallOwnedRetry() {
  if (!resyncRetryTimer || resyncRetryOwner !== 'stall') return;
  clearTimeout(resyncRetryTimer);
  resyncRetryTimer = null;
  resyncRetryOwner = null;
  resyncRetryDelayMs = 1000;
}
function notePageCursorProgress(allowPendingRecoveryCancellation = true) {
  const recovery = pendingTransportRecovery;
  if (allowPendingRecoveryCancellation
      && recovery?.streamId === currentStreamId
      && latestSeq > recovery.pageSequence) {
    pendingTransportRecovery = null;
    cancelStallOwnedRetry();
  }
  if (!streamLagObservation) return;
  if (streamLagObservation.streamId !== currentStreamId || latestSeq >= streamLagObservation.latestSequence) {
    clearStreamLagObservation();
  } else if (latestSeq > streamLagObservation.pageSequence) {
    streamLagObservation.pageSequence = latestSeq;
    if (streamLagTimer) clearTimeout(streamLagTimer);
    streamLagTimer = null;
    armStreamLagTimer();
  }
}
function observeAuthoritativeProgress(status) {
  const streamId = streamIdOf(status);
  const serverLatestSequence = Number(status?.latest_sequence);
  if (!streamId || streamId !== currentStreamId || !Number.isFinite(serverLatestSequence) || serverLatestSequence <= latestSeq) {
    clearStreamLagObservation();
    return;
  }
  if (streamLagObservation?.streamId === streamId) {
    streamLagObservation.latestSequence = Math.max(streamLagObservation.latestSequence, serverLatestSequence);
    return;
  }
  clearStreamLagObservation();
  streamLagObservation = {streamId, latestSequence:serverLatestSequence, pageSequence:latestSeq};
  armStreamLagTimer();
}
function resetFromSnapshot(snapshot) {
  const streamId = streamIdOf(snapshot) || streamIdOf(snapshot.status);
  if (!streamId) throw new Error('dashboard snapshot is missing its live stream generation');
  state = {...emptyState(), ...snapshot};
  for (const key of ['ecg','acc','hr','rr','health','timing','events']) state[key] = cloneIndexed(state[key] || []);
  state.latest_acc = state.latest_acc || state.acc[state.acc.length - 1] || null;
  state.latest_hr = state.latest_hr || state.hr[state.hr.length - 1] || null;
  state.latest_health = state.latest_health || state.health[state.health.length - 1] || null;
  latestSeq = Number(snapshot.latest_seq || snapshot.status?.latest_sequence || 0);
  currentStreamId = streamId;
  notePageCursorProgress(false);
}
function setBrowserTransport(mode, transportState) {
  browserTransportMode = mode;
  browserTransportState = transportState;
  const host = document.getElementById('browserTransport');
  if (host) host.textContent = `browser transport: ${mode} (${transportState})`;
}
async function refreshSnapshot() {
  const res = await fetch('/api/latest', {cache:'no-store'});
  if (!res.ok) throw new Error(`/api/latest ${res.status}`);
  resetFromSnapshot(await res.json());
}
async function refreshStatus() {
  const res = await fetch('/api/status', {cache:'no-store'});
  if (!res.ok) throw new Error(`/api/status ${res.status}`);
  const next = await res.json();
  const nextStreamId = streamIdOf(next);
  if (currentStreamId && nextStreamId && nextStreamId !== currentStreamId) {
    refreshSnapshotAndResubscribe();
    return;
  }
  observeAuthoritativeProgress(next);
  const archive = next.archive || state.status?.archive;
  state.status = {...state.status, ...next};
  if (archive) state.status.archive = archive;
}
function applyBatch(batch) {
  const batchStreamId = streamIdOf(batch) || streamIdOf(batch.status);
  if (!batchStreamId || (currentStreamId && batchStreamId !== currentStreamId)) {
    refreshSnapshotAndResubscribe();
    return;
  }
  if (!currentStreamId) currentStreamId = batchStreamId;
  if (batch.overflow) { refreshSnapshotAndResubscribe(); return; }
  if (batch.status) {
    const archive = state.status?.archive;
    state.status = batch.status;
    if (archive) state.status.archive = archive;
  }
  for (const item of batch.items || []) {
    if (Number(item.seq) <= latestSeq) continue;
    const row = item.row;
    switch (item.kind) {
      case 'ecg': pushLimited(state.ecg, row, LIMITS.ecg); break;
      case 'acc': pushLimited(state.acc, row, LIMITS.acc); state.latest_acc = row; break;
      case 'hr': pushLimited(state.hr, row, LIMITS.hr); state.latest_hr = row; break;
      case 'rr': pushLimited(state.rr, row, LIMITS.rr); break;
      case 'battery': state.latest_battery = row; break;
      case 'health': pushLimited(state.health, row, LIMITS.health); state.latest_health = row; break;
      case 'timing': pushLimited(state.timing, row, LIMITS.timing); break;
      case 'event': mergeEventRow(row); break;
    }
    latestSeq = Number(item.seq);
  }
  notePageCursorProgress();
}

function handleTransportMessage(message) {
  try {
    if (message.type === 'batch') applyBatch(JSON.parse(message.data));
    else if (message.type === 'status') {
      const next = JSON.parse(message.data);
      const nextStreamId = streamIdOf(next);
      if (!nextStreamId || (currentStreamId && nextStreamId !== currentStreamId)) {
        refreshSnapshotAndResubscribe();
        return;
      }
      observeAuthoritativeProgress(next);
      const archive = state.status?.archive;
      state.status = next;
      if (archive) state.status.archive = archive;
    }
    else if (message.type === 'resync') refreshSnapshotAndResubscribe();
    else if (message.type === 'transport') {
      setBrowserTransport(message.mode || browserTransportMode, message.state || browserTransportState);
    }
  } catch (err) {
    console.error(err);
  }
}
function strongerResyncReason(left, right) {
  return left === 'general' || right === 'general' ? 'general' : 'stall';
}
function scheduleResyncRetry(reason) {
  if (resyncRetryTimer) {
    resyncRetryOwner = strongerResyncReason(resyncRetryOwner, reason);
    return;
  }
  const delay = resyncRetryDelayMs;
  resyncRetryOwner = reason;
  resyncRetryTimer = setTimeout(() => {
    resyncRetryTimer = null;
    const retryReason = resyncRetryOwner || 'general';
    resyncRetryOwner = null;
    refreshSnapshotAndResubscribe(retryReason);
  }, delay);
  resyncRetryDelayMs = Math.min(30000, resyncRetryDelayMs * 2);
}
async function refreshSnapshotAndResubscribe(reason = 'general') {
  if (resyncInFlight) {
    if (resyncInFlightReason === 'stall' && reason === 'general') {
      resyncFollowupReason = 'general';
    }
    return;
  }
  if (resyncRetryTimer) {
    resyncRetryOwner = strongerResyncReason(resyncRetryOwner, reason);
    return;
  }
  resyncInFlight = true;
  resyncInFlightReason = reason;
  let retry = false;
  try {
    await refreshSnapshot();
    const recovery = pendingTransportRecovery;
    const forceTransportRecovery = recovery?.streamId === currentStreamId;
    const deferTransportToGeneralFollowup = resyncFollowupReason === 'general';
    resyncRetryDelayMs = 1000;
    if (resyncRetryTimer) clearTimeout(resyncRetryTimer);
    resyncRetryTimer = null;
    resyncRetryOwner = null;
    if (!deferTransportToGeneralFollowup && liveTransport?.kind === 'shared_worker') {
      if (forceTransportRecovery) {
        liveTransport.port.postMessage({
          type:'recover',
          since:latestSeq,
          streamId:currentStreamId,
          observedLatestSequence:Math.max(latestSeq, recovery.latestSequence),
        });
      } else {
        liveTransport.port.postMessage({type:'subscribe', since:latestSeq, streamId:currentStreamId});
      }
    } else if (!deferTransportToGeneralFollowup && liveTransport?.kind === 'direct_sse') {
      startDirectSse('resynced');
    }
    pendingTransportRecovery = null;
  } catch (err) {
    setBrowserTransport(browserTransportMode, `resync failed: ${err}`);
    retry = true;
  } finally {
    resyncInFlight = false;
    resyncInFlightReason = null;
  }
  const followupReason = resyncFollowupReason;
  resyncFollowupReason = null;
  const nextReason = followupReason
    ? strongerResyncReason(reason, followupReason)
    : reason;
  if (retry) {
    if (nextReason === 'general' || pendingTransportRecovery) scheduleResyncRetry(nextReason);
  } else if (followupReason) {
    refreshSnapshotAndResubscribe(followupReason);
  }
}
function startDirectSse(reason='SharedWorker unavailable') {
  setBrowserTransport('direct_sse_fallback', reason);
  if (eventSource) eventSource.close();
  const source = new EventSource(`/events?since=${latestSeq}`);
  eventSource = source;
  source.onopen = () => {
    if (eventSource === source) setBrowserTransport('direct_sse_fallback', 'connected');
  };
  source.addEventListener('batch', event => {
    if (eventSource === source) handleTransportMessage({type:'batch', data:event.data});
  });
  source.addEventListener('status', event => {
    if (eventSource === source) handleTransportMessage({type:'status', data:event.data});
  });
  source.onerror = () => {
    if (eventSource === source) setBrowserTransport('direct_sse_fallback', 'reconnecting');
  };
  liveTransport = {kind:'direct_sse'};
}
function startSse() {
  if (!('SharedWorker' in window)) {
    startDirectSse('SharedWorker unsupported');
    return;
  }
  try {
    const worker = new SharedWorker('/live-worker.js?v=20260806', 'training-sensors-live-v4');
    const port = worker.port;
    liveTransport = {kind:'shared_worker', worker, port};
    setBrowserTransport('shared_worker', 'connecting');
    port.onmessage = event => handleTransportMessage(event.data || {});
    port.onmessageerror = () => setBrowserTransport('shared_worker', 'message error');
    worker.onerror = event => {
      event.preventDefault();
      if (liveTransport?.kind !== 'shared_worker') return;
      port.close();
      liveTransport = null;
      startDirectSse('SharedWorker failed');
    };
    port.start();
    port.postMessage({type:'subscribe', since:latestSeq, streamId:currentStreamId});
  } catch (err) {
    console.error(err);
    startDirectSse('SharedWorker startup failed');
  }
}
function prepareData() {
  const data = {...state};
  data.ecg = cloneIndexed(state.ecg);
  data.acc = cloneIndexed(state.acc);
  data.hr = cloneIndexed(state.hr).map(row => {
    const collectorNs = asNumber(row.collector_unix_ns);
    const offsetNs = connectionOffsetNs(row, state.status);
    return {...row, collector_unix_ns: collectorNs, local_ts_ns: collectorNs && offsetNs !== null ? collectorNs - EPOCH_2000_UNIX_NS + offsetNs : 0};
  });
  data.health = cloneIndexed(state.health).filter(row => row.rssi !== null && row.rssi !== undefined).map(row => ({...row, rssi: Number(row.rssi)}));
  data.timing = cloneIndexed(state.timing);
  data.events = cloneIndexed(state.events).sort((a,b) => asNumber(a.collector_unix_ns) - asNumber(b.collector_unix_ns));
  data.latest_hr = data.hr[data.hr.length - 1] || null;
  data.latest_acc = data.acc[data.acc.length - 1] || null;
  data.latest_health = data.health[data.health.length - 1] || null;
  data.latest_battery = state.latest_battery || null;
  data.rr = stableRrTimeline(data.status?.data_dir || data.data_dir || '', data.hr);
  data.packet_delay = packetDelaySeries(data.timing);
  data.timing_summary = estimateTiming(data.timing);
  return data;
}
function niceStep(range, targetTicks=5) {
  if (!Number.isFinite(range) || range <= 0) return 1;
  const rough = range / targetTicks;
  const exponent = Math.floor(Math.log10(rough));
  const fraction = rough / Math.pow(10, exponent);
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * Math.pow(10, exponent);
}
function niceTicks(min, max, targetTicks=5) {
  const step = niceStep(max - min, targetTicks);
  const start = Math.ceil(min / step) * step;
  const end = Math.floor(max / step) * step;
  const ticks = [];
  for (let v = start; v <= end + step * 0.001; v += step) ticks.push(Math.abs(v) < step * 1e-6 ? 0 : v);
  if (ticks.length < 2) return [min, max];
  return ticks;
}
function formatUnitTick(v, unit) { const abs = Math.abs(v); const decimals = abs < 10 && Math.abs(v - Math.round(v)) > 0.01 ? 1 : 0; return `${v.toFixed(decimals)} ${unit}`; }
function percentile(values, p) { if (!values.length) return null; const sorted=[...values].sort((a,b)=>a-b); const idx=Math.min(sorted.length-1, Math.max(0, Math.round((sorted.length-1)*p))); return sorted[idx]; }
function mean(values) { return values.reduce((a,b)=>a+b,0) / Math.max(1, values.length); }
function packetDelaySeries(timing) {
  const out = []; let rollingSum = 0; const window = [];
  for (const row of timing || []) {
    const collectorUnixNs = asNumber(row.collector_unix_ns); const deviceFrameTsNs = asNumber(row.device_frame_ts_ns);
    const offsetNs = connectionOffsetNs(row, state.status);
    if (!collectorUnixNs || !deviceFrameTsNs || offsetNs === null) continue;
    const collectorSince2000Ns = collectorUnixNs - EPOCH_2000_UNIX_NS + offsetNs;
    const delayMs = row.packet_delay_ms ?? ((collectorSince2000Ns - deviceFrameTsNs) / 1_000_000.0);
    window.push(delayMs); rollingSum += delayMs;
    if (window.length > ROLLING_DELAY_PACKETS) rollingSum -= window.shift();
    out.push({i: out.length, measurement: row.measurement || '', delay_ms: delayMs, rolling_avg_ms: rollingSum / window.length, gap_ms: row.gap_ms});
  }
  return out;
}
function fitH10Drift(points, label) {
  if (points.length < 10) return {label, points: points.length};
  const dev0 = points[0][0], col0 = points[0][1];
  const xs = points.map(p => (p[0] - dev0) / 1_000_000_000.0);
  const ys = points.map(p => (p[1] - col0) / 1_000_000_000.0);
  const xMean = mean(xs), yMean = mean(ys);
  const denom = xs.reduce((acc, x) => acc + (x - xMean) ** 2, 0);
  if (denom <= 0) return {label, points: points.length};
  const slope = xs.reduce((acc, x, i) => acc + (x - xMean) * (ys[i] - yMean), 0) / denom;
  const intercept = yMean - slope * xMean;
  const h10PpmVsCollector = ((1.0 / slope) - 1.0) * 1_000_000.0;
  const h10SecondsPerDay = h10PpmVsCollector * 86_400.0 / 1_000_000.0;
  const residualsMs = xs.map((x, i) => (ys[i] - (intercept + slope * x)) * 1000.0);
  return {label, points: points.length, window_seconds: ys[ys.length - 1] - ys[0], collector_per_h10_slope: slope, h10_ppm_vs_collector: h10PpmVsCollector, h10_seconds_per_day: h10SecondsPerDay, residual_p95_ms: percentile(residualsMs.map(Math.abs), 0.95)};
}
function estimateTiming(timing) {
  const points = (timing || []).filter(r => r.device_frame_ts_ns && r.collector_unix_ns).map(r => [asNumber(r.device_frame_ts_ns), asNumber(r.collector_unix_ns), r]);
  if (points.length < 4) return {points: points.length};
  const latestCollectorNs = points[points.length - 1][1];
  const longPoints = points.filter(p => latestCollectorNs - p[1] <= LONG_DRIFT_WINDOW_NS);
  const gapValues = points.map(p => p[2].gap_ms).filter(v => v !== null && v !== undefined).map(Number);
  const positiveGaps = gapValues.filter(g => g > 0);
  return {points: points.length, short_drift: fitH10Drift(points.slice(-ROLLING_DELAY_PACKETS), 'last 1000 PMD frames'), long_drift: fitH10Drift(longPoints, longPoints.length < points.length ? 'last 60 minutes' : 'session so far'), gap_latest_ms: gapValues.length ? gapValues[gapValues.length - 1] : null, gap_max_ms: positiveGaps.length ? Math.max(...positiveGaps) : 0.0};
}
function stableRrTimeline(dataKey, hr) {
  if (!hr.length) return [];
  const latestCollectorNs = asNumber(hr[hr.length - 1].collector_unix_ns);
  if (rrState.dataKey !== dataKey || latestCollectorNs < rrState.lastNotificationNs) rrState = {dataKey, lastNotificationNs: 0, lastRrTsNs: null, points: []};
  let points = rrState.points; let lastRrTsNs = rrState.lastRrTsNs; let lastNotificationNs = rrState.lastNotificationNs;
  for (const row of hr) {
    const collectorNs = asNumber(row.collector_unix_ns); if (!collectorNs || collectorNs <= lastNotificationNs) continue;
    const localTsNs = asNumber(row.local_ts_ns); const rrValues = row.rr_ms || [];
    if (rrValues.length && localTsNs) {
      const resetTimeline = lastRrTsNs === null || Math.abs(localTsNs - lastRrTsNs) > RR_TIMELINE_RESET_GAP_NS;
      if (resetTimeline) {
        let remainingAfterMs = rrValues.reduce((a,b)=>a+Number(b),0);
        for (const valueRaw of rrValues) { const value = Number(valueRaw); remainingAfterMs -= value; const ts = localTsNs - remainingAfterMs * 1_000_000.0; points.push({collector_unix_ns: collectorNs, local_ts_ns: ts, notify_local_ts_ns: localTsNs, rr_ms: value}); }
        lastRrTsNs = points[points.length - 1].local_ts_ns;
      } else {
        for (const valueRaw of rrValues) { const value = Number(valueRaw); lastRrTsNs += value * 1_000_000.0; points.push({collector_unix_ns: collectorNs, local_ts_ns: lastRrTsNs, notify_local_ts_ns: localTsNs, rr_ms: value}); }
      }
    }
    lastNotificationNs = collectorNs;
  }
  const latestLocalTsNs = asNumber(hr[hr.length - 1].local_ts_ns);
  if (latestLocalTsNs) points = points.filter(p => p.local_ts_ns >= latestLocalTsNs - RR_TIMELINE_MAX_AGE_NS);
  rrState.points = points; rrState.lastNotificationNs = lastNotificationNs; rrState.lastRrTsNs = lastRrTsNs;
  return points.slice(-1000).map((p, i) => ({...p, i}));
}
// Shared sweep helpers. All four live plots map a sample's x by its PHASE within
// the 10 s sweep (modulo), so the previous sweep stays visible ahead of the blue
// playhead and is overwritten only as the playhead passes over each column —
// classic patient-monitor behaviour, redrawn statelessly from the ring buffers.
const SWEEP_NS = 10_000_000_000;          // 10 s sweep window, shared by all plots
const BLANK_AHEAD_NS = 200_000_000;       // hide the oldest ~0.2 s of each sweep — the blank "erase gap" just ahead of the cursor
const ECG_GAP_BREAK_NS = 150_000_000;     // 150 ms: break ECG/ACC trace across a dropout
const VALUE_GAP_BREAK_NS = 3_000_000_000; // 3 s: break the sparse HR/RR line across reconnects
const REGRESSION_NS = 1_000_000_000;      // >1 s backward device-time jump = clock reset / new generation
const PMD_STALE_NS = 2_500_000_000;       // ECG/ACC: warn if no new sample for 2.5 s (gap > ~2 s = broken stream)
const VALUE_STALE_NS = 5_000_000_000;     // HR/RR (~1 Hz): warn if no new sample for 5 s
const PLAYHEAD_COLOR = '#bcd9ff';          // light-blue sweep cursor, distinct from every trace colour
// Buffers are append-only in arrival order, so the tail is the newest-arrived sample. Using the tail
// (not the max over the whole buffer) means a backward device-clock reset DROPS the latest timestamp
// and triggers a re-anchor, instead of pinning the sweep to stale pre-reset samples.
function tailTs(rows, key) { const a = rows || []; return a.length ? asNumber(a[a.length - 1][key]) : 0; }
function headTs(rows, key) { const a = rows || []; return a.length ? asNumber(a[0][key]) : 0; }
function updateSweepFrame(latestTs, oldestTs) {
  if (!latestTs) return currentSweep;
  // Anchor the phase grid on the OLDEST buffered sample, not latestTs: otherwise the seconds of
  // history already in the ring at first paint (and after every re-anchor) would wrap onto the
  // right of the plot and look like a previous sweep that never happened.
  if (sweepAnchorNs === null || latestTs < lastSweepLatestTs) sweepAnchorNs = oldestTs || latestTs;
  lastSweepLatestTs = latestTs;
  const cycle = Math.floor(Math.max(0, latestTs - sweepAnchorNs) / SWEEP_NS);
  const startNs = sweepAnchorNs + cycle * SWEEP_NS;
  currentSweep = {startNs, endNs: startNs + SWEEP_NS};
  return currentSweep;
}
function sweepPhaseNs(ts, startNs) { const d = (asNumber(ts) - startNs) % SWEEP_NS; return d < 0 ? d + SWEEP_NS : d; }
function sweepXForTs(ts, startNs, plotLeft, plotW) { return plotLeft + sweepPhaseNs(ts, startNs) * plotW / SWEEP_NS; }
// Each plot draws its cursor at ITS OWN newest sample (the shared startNs still keeps all four plots
// time-aligned), so a stream whose latest BLE batch lags the others is never stranded on the right at
// a wrap; instead its own cursor sits where its trace ends.
function drawPlayhead(ctx, ts, startNs, plotLeft, plotW, plotTop, plotBottom) { if (!ts) return; const x = sweepXForTs(ts, startNs, plotLeft, plotW); ctx.strokeStyle = PLAYHEAD_COLOR; ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(x, plotTop); ctx.lineTo(x, plotBottom); ctx.stroke(); }
// Wall-clock age since the newest sample was RECEIVED (collector_unix_ns), independent of the H10
// device clock — so device-clock skew never false-alarms nor delays the staleness warning.
function recvAgeNs(rows) { const r = tailTs(rows, 'collector_unix_ns'); return r ? Date.now() * 1_000_000 - r : 0; }
// The current "generation" is the suffix of the (append-only, arrival-order) buffer after the last
// backward device-time jump (a clock reset). Restricting the sweep window to it drops stale pre-reset
// samples that would otherwise be drawn as if current and silently hide the reset.
function currentGenStart(rows, tsKey) { const a = rows || []; let start = 0; for (let i = 1; i < a.length; i++) { if (asNumber(a[i][tsKey]) < asNumber(a[i-1][tsKey]) - REGRESSION_NS) start = i; } return start; }
function genHeadTs(rows, tsKey) { const a = rows || []; return a.length ? asNumber(a[currentGenStart(a, tsKey)][tsKey]) : 0; }
// Window = the current generation within (latest - SWEEP_NS + BLANK_AHEAD_NS, latest]. Dropping the
// oldest BLANK_AHEAD_NS leaves a blank "erase gap" just ahead of the cursor (the about-to-be-overwritten
// oldest second is not drawn), the classic patient-monitor look.
function sweepWindow(rows, tsKey, latest) { const a = rows || []; const start = currentGenStart(a, tsKey); const lo = latest - SWEEP_NS + BLANK_AHEAD_NS; const out = []; for (let i = start; i < a.length; i++) { const ts = asNumber(a[i][tsKey]); if (ts > lo && ts <= latest) out.push(a[i]); } return out; }
// Loud, on-plot staleness warning so a stopped/gapped stream is never mistaken for live data.
function drawStaleBadge(ctx, ageNs, label, plotRight, plotTop) { ctx.fillStyle = '#ffb000'; ctx.font = '15px monospace'; ctx.textAlign = 'right'; ctx.fillText(`⚠ no new ${label} for ${(ageNs/1e9).toFixed(1)} s`, plotRight - 6, plotTop + 16); ctx.textAlign = 'left'; }
// Draw one retained polyline. Samples must be in time order. Starts a new sub-path
// at the once-per-window phase wrap (else a line would shoot right edge -> left) and
// across any time gap bigger than gapBreakNs (so dropouts are not stitched over).
function drawRetainedPath(ctx, samples, tsKey, valKey, startNs, plotLeft, plotW, yFor, color, lineWidth, gapBreakNs) {
  if (!samples.length) return;
  ctx.strokeStyle = color; ctx.lineWidth = lineWidth; ctx.beginPath();
  let prevTs = null, prevPhase = null, penDown = false;
  for (const row of samples) {
    const ts = asNumber(row[tsKey]); const phase = sweepPhaseNs(ts, startNs);
    const x = plotLeft + phase * plotW / SWEEP_NS; const y = yFor(asNumber(row[valKey]));
    const wrap = prevPhase !== null && phase < prevPhase;
    const gap = prevTs !== null && (ts - prevTs > gapBreakNs || ts < prevTs);
    if (!penDown || wrap || gap) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    penDown = true; prevTs = ts; prevPhase = phase;
  }
  ctx.stroke();
}
function drawEmptyEcg(canvas, message) { const ctx = canvas.getContext('2d'); ctx.clearRect(0,0,canvas.width,canvas.height); ctx.fillStyle = '#050505'; ctx.fillRect(0,0,canvas.width,canvas.height); ctx.fillStyle = '#aaa'; ctx.font = '22px monospace'; ctx.fillText(message, 18, 42); }
function drawSweepValueCanvas(canvas, points, key, unit, color) {
  const ctx = canvas.getContext('2d'); const w = canvas.width, h = canvas.height; const margin = {left:105,right:20,top:14,bottom:34}; const plotW=w-margin.left-margin.right; const plotH=h-margin.top-margin.bottom; const plotLeft=margin.left, plotRight=margin.left+plotW, plotTop=margin.top, plotBottom=margin.top+plotH;
  ctx.clearRect(0,0,w,h); ctx.fillStyle = '#050505'; ctx.fillRect(0,0,w,h); if (!currentSweep) { ctx.fillStyle = '#aaa'; ctx.font = '18px monospace'; ctx.fillText('Waiting for ECG sweep…', 18, 34); return; }
  const {startNs} = currentSweep; const latestTs = tailTs(points, 'local_ts_ns'); const label = key === 'bpm' ? 'HR' : 'R-R'; const visible = sweepWindow(points, 'local_ts_ns', latestTs).filter(row => Number.isFinite(asNumber(row[key], NaN)));
  const vals = visible.map(row => asNumber(row[key])); const defaultRange = key === 'bpm' ? {min:60,max:180} : key === 'rr_ms' ? {min:60000/180,max:60000/60} : null; let min, max;
  if (defaultRange) { const dataMin = vals.length ? Math.min(...vals) : defaultRange.min; const dataMax = vals.length ? Math.max(...vals) : defaultRange.max; min = Math.min(defaultRange.min, dataMin); max = Math.max(defaultRange.max, dataMax); const pad = Math.max((max-min)*0.04, unit === 'bpm' ? 1 : 5); if (dataMin < defaultRange.min) min -= pad; if (dataMax > defaultRange.max) max += pad; }
  else { if (!vals.length) vals.push(0, 1); min = Math.min(...vals); max = Math.max(...vals); if (min === max) { min -= 1; max += 1; } const pad = Math.max((max-min)*0.12, 1); min -= pad; max += pad; }
  const yFor = v => plotBottom - (v - min) * plotH / (max - min);
  let yTicks = niceTicks(min, max, 3); if (defaultRange && Math.abs(min-defaultRange.min)<1e-6 && Math.abs(max-defaultRange.max)<1e-6) yTicks = key === 'bpm' ? [60,90,120,150,180] : [60000/180,60000/120,60000/90,60000/60];
  ctx.strokeStyle = '#333'; ctx.lineWidth = 1; ctx.fillStyle = '#b8b8b8'; ctx.font = '14px monospace'; ctx.textAlign = 'right'; for (const tick of yTicks) { const y=yFor(tick); ctx.beginPath(); ctx.moveTo(plotLeft,y); ctx.lineTo(plotRight,y); ctx.stroke(); ctx.fillText(formatUnitTick(tick, unit), plotLeft-10, y+5); }
  ctx.textAlign = 'center'; ctx.fillStyle = '#999'; ctx.font = '13px monospace'; for (const seconds of [0,2.5,5,7.5,10]) { const x=plotLeft + seconds*plotW/10; ctx.fillText(`${seconds.toFixed(seconds % 1 ? 1 : 0)} s`, x, h-12); }
  ctx.strokeStyle = '#555'; ctx.lineWidth = 1; ctx.strokeRect(plotLeft, plotTop, plotW, plotH);
  drawRetainedPath(ctx, visible, 'local_ts_ns', key, startNs, plotLeft, plotW, yFor, color, 2, VALUE_GAP_BREAK_NS);
  ctx.fillStyle=color; for (const row of visible) { const x=sweepXForTs(row.local_ts_ns, startNs, plotLeft, plotW); const y=yFor(asNumber(row[key])); ctx.beginPath(); ctx.arc(x,y,4,0,2*Math.PI); ctx.fill(); }
  drawPlayhead(ctx, latestTs, startNs, plotLeft, plotW, plotTop, plotBottom);
  if (!latestTs) { ctx.fillStyle='#aaa'; ctx.font='15px monospace'; ctx.textAlign='left'; ctx.fillText(`Waiting for ${label} samples…`, plotLeft + 6, plotTop + 16); }
  else { const age = recvAgeNs(points); if (age > VALUE_STALE_NS) drawStaleBadge(ctx, age, label, plotRight, plotTop); }
}
function drawEcg(canvas, ecg) {
  const ctx = canvas.getContext('2d'); const w=canvas.width, h=canvas.height; const margin={left:105,right:20,top:18,bottom:46}; const plotW=w-margin.left-margin.right; const plotH=h-margin.top-margin.bottom; const plotLeft=margin.left, plotRight=margin.left+plotW, plotTop=margin.top, plotBottom=margin.top+plotH;
  if (!currentSweep) { drawEmptyEcg(canvas, 'Waiting for ECG samples…'); return; }
  const {startNs} = currentSweep; const latestTs = tailTs(ecg, 'device_ts_ns'); if (!latestTs) { drawEmptyEcg(canvas, 'Waiting for ECG timestamps…'); return; }
  const visible = sweepWindow(ecg, 'device_ts_ns', latestTs);
  const vals = visible.map(r => asNumber(r.microvolts)); if (!vals.length) { drawEmptyEcg(canvas, 'Waiting for ECG samples in current sweep…'); return; }
  let min=Math.min(...vals), max=Math.max(...vals); if (min===max) { min-=1; max+=1; } const pad=Math.max((max-min)*0.08,1); min-=pad; max+=pad; const yFor=v=>plotBottom-(v-min)*plotH/(max-min);
  ctx.clearRect(0,0,w,h); ctx.fillStyle='#050505'; ctx.fillRect(0,0,w,h); ctx.strokeStyle='#333'; ctx.lineWidth=1; ctx.fillStyle='#b8b8b8'; ctx.font='16px monospace'; ctx.textAlign='right'; for (const tick of niceTicks(min,max,5)) { const y=yFor(tick); ctx.beginPath(); ctx.moveTo(plotLeft,y); ctx.lineTo(plotRight,y); ctx.stroke(); ctx.fillText(formatUnitTick(tick,'µV'), plotLeft-10, y+5); }
  ctx.textAlign='center'; ctx.fillStyle='#999'; ctx.font='15px monospace'; for (const seconds of [0,2.5,5,7.5,10]) { const x=plotLeft+seconds*plotW/10; ctx.fillText(`${seconds.toFixed(seconds % 1 ? 1 : 0)} s`, x, h-16); }
  ctx.strokeStyle='#555'; ctx.lineWidth=1; ctx.strokeRect(plotLeft, plotTop, plotW, plotH);
  drawRetainedPath(ctx, visible, 'device_ts_ns', 'microvolts', startNs, plotLeft, plotW, yFor, '#d2e88f', 2, ECG_GAP_BREAK_NS);
  drawPlayhead(ctx, latestTs, startNs, plotLeft, plotW, plotTop, plotBottom);
  const ecgAge = recvAgeNs(ecg); if (ecgAge > PMD_STALE_NS) drawStaleBadge(ctx, ecgAge, 'ECG', plotRight, plotTop);
}
function verticalBar({key, color, val, topText, bottomText}) { const wrap=document.createElement('div'); wrap.className='axisWidget'; const top=document.createElement('div'); top.className='axisTop'; top.textContent=topText; wrap.appendChild(top); const box=document.createElement('div'); box.className='barWrap'; const zero=document.createElement('div'); zero.className='zero'; box.appendChild(zero); const bar=document.createElement('div'); bar.className='bar'; bar.style.background=color; const pct=Math.max(-1, Math.min(1, val/1200)); if (pct>=0) { bar.style.top=`${50-pct*50}%`; bar.style.height=`${pct*50}%`; } else { bar.style.top='50%'; bar.style.height=`${-pct*50}%`; } box.appendChild(bar); wrap.appendChild(box); const bottom=document.createElement('div'); bottom.className='axisBottom'; bottom.textContent=bottomText; wrap.appendChild(bottom); const label=document.createElement('div'); label.className='label'; label.textContent=`${key.toUpperCase()} ${Math.round(val)}`; wrap.appendChild(label); return wrap; }
function horizontalBar({key, color, val, topText, leftText, rightText}) { const wrap=document.createElement('div'); wrap.className='axisWidget'; const top=document.createElement('div'); top.className='axisTop'; top.textContent=topText; wrap.appendChild(top); const row=document.createElement('div'); row.className='hAxisRow'; const left=document.createElement('div'); left.className='axisSide'; left.textContent=leftText; row.appendChild(left); const box=document.createElement('div'); box.className='hbarWrap'; const zero=document.createElement('div'); zero.className='hzero'; box.appendChild(zero); const bar=document.createElement('div'); bar.className='hbar'; bar.style.background=color; const pct=Math.max(-1, Math.min(1, val/1200)); if (pct>=0) { bar.style.left='50%'; bar.style.width=`${pct*50}%`; } else { bar.style.left=`${50+pct*50}%`; bar.style.width=`${-pct*50}%`; } box.appendChild(bar); row.appendChild(box); const right=document.createElement('div'); right.className='axisSide'; right.textContent=rightText; row.appendChild(right); wrap.appendChild(row); const spacer=document.createElement('div'); spacer.className='axisBottom'; spacer.textContent=''; wrap.appendChild(spacer); const label=document.createElement('div'); label.className='label'; label.textContent=`${key.toUpperCase()} ${Math.round(val)}`; wrap.appendChild(label); return wrap; }
function renderBars(latest) { const host=document.getElementById('accBars'); host.innerHTML=''; const x=latest ? asNumber(latest.x) : 0; const z=latest ? asNumber(latest.z) : 0; const y=latest ? asNumber(latest.y) : 0; host.appendChild(verticalBar({key:'x',color:'#58a6ff',val:x,topText:'superior',bottomText:'inferior'})); host.appendChild(verticalBar({key:'z',color:'#a371f7',val:z,topText:'supine',bottomText:'prone'})); host.appendChild(horizontalBar({key:'y',color:'#f2cc60',val:y,topText:'lateral',leftText:'L up',rightText:'R up'})); }
function drawAccTrace(canvas, acc) {
  const ctx = canvas.getContext('2d'); const w=canvas.width, h=canvas.height; const margin={left:105,right:20,top:18,bottom:46}; const plotW=w-margin.left-margin.right; const plotH=h-margin.top-margin.bottom; const plotLeft=margin.left, plotRight=margin.left+plotW, plotTop=margin.top, plotBottom=margin.top+plotH;
  ctx.clearRect(0,0,w,h); ctx.fillStyle='#050505'; ctx.fillRect(0,0,w,h);
  if (!currentSweep) { ctx.fillStyle='#aaa'; ctx.font='18px monospace'; ctx.fillText('Waiting for accelerometer sweep…', 18, 34); return; }
  const {startNs} = currentSweep; const latestTs = tailTs(acc, 'device_ts_ns'); const keys=['x','y','z']; const colors=['#58a6ff','#f2cc60','#a371f7'];
  const visible = sweepWindow(acc, 'device_ts_ns', latestTs);
  if (!visible.length) { ctx.fillStyle='#aaa'; ctx.font='18px monospace'; ctx.fillText('Waiting for accelerometer samples…', 18, 34); return; }
  let vals=[]; for (const row of visible) for (const k of keys) vals.push(asNumber(row[k]));
  let min=Math.min(...vals), max=Math.max(...vals); if (min===max) { min-=1; max+=1; } const pad=Math.max((max-min)*0.08,1); min-=pad; max+=pad; const yFor=v=>plotBottom-(v-min)*plotH/(max-min);
  ctx.strokeStyle='#333'; ctx.lineWidth=1; ctx.fillStyle='#b8b8b8'; ctx.font='16px monospace'; ctx.textAlign='right'; for (const tick of niceTicks(min,max,5)) { const y=yFor(tick); ctx.beginPath(); ctx.moveTo(plotLeft,y); ctx.lineTo(plotRight,y); ctx.stroke(); ctx.fillText(formatUnitTick(tick,'mG'), plotLeft-10, y+5); }
  ctx.textAlign='center'; ctx.fillStyle='#999'; ctx.font='15px monospace'; for (const seconds of [0,2.5,5,7.5,10]) { const x=plotLeft+seconds*plotW/10; ctx.fillText(`${seconds.toFixed(seconds % 1 ? 1 : 0)} s`, x, h-16); }
  ctx.strokeStyle='#555'; ctx.lineWidth=1; ctx.strokeRect(plotLeft, plotTop, plotW, plotH);
  keys.forEach((k,idx)=>drawRetainedPath(ctx, visible, 'device_ts_ns', k, startNs, plotLeft, plotW, yFor, colors[idx], 2, ECG_GAP_BREAK_NS));
  drawPlayhead(ctx, latestTs, startNs, plotLeft, plotW, plotTop, plotBottom);
  const accAge = recvAgeNs(acc); if (accAge > PMD_STALE_NS) drawStaleBadge(ctx, accAge, 'ACC', plotRight, plotTop);
}
function drawRssi(canvas, health) { const ctx=canvas.getContext('2d'); const w=canvas.width,h=canvas.height; const margin={left:105,right:20,top:10,bottom:22}; const plotW=w-margin.left-margin.right; const plotH=h-margin.top-margin.bottom; const plotLeft=margin.left, plotRight=margin.left+plotW, plotTop=margin.top, plotBottom=margin.top+plotH; ctx.clearRect(0,0,w,h); ctx.fillStyle='#050505'; ctx.fillRect(0,0,w,h); const yMin=-100,yMax=-30; const yFor=v=>plotBottom-(v-yMin)*plotH/(yMax-yMin); const xFor=i=>plotLeft+i*plotW/Math.max(1,(health||[]).length-1); const bands=[{lo:-50,hi:-30,color:'#123d22'},{lo:-65,hi:-50,color:'#123442'},{lo:-80,hi:-65,color:'#47350d'},{lo:-100,hi:-80,color:'#461717'}]; for (const b of bands) { const yTop=yFor(b.hi), yBot=yFor(b.lo); ctx.fillStyle=b.color; ctx.fillRect(plotLeft,yTop,plotW,yBot-yTop); } ctx.strokeStyle='#444'; ctx.lineWidth=1; ctx.fillStyle='#ddd'; ctx.font='16px monospace'; ctx.textAlign='right'; for (const v of [-30,-50,-65,-80,-100]) { const y=yFor(v); ctx.beginPath(); ctx.moveTo(plotLeft,y); ctx.lineTo(plotRight,y); ctx.stroke(); ctx.fillText(`${v} dBm`, plotLeft-10, y+5); } ctx.strokeStyle='#555'; ctx.strokeRect(plotLeft,plotTop,plotW,plotH); if (!health || !health.length) return; ctx.strokeStyle='#ffffff'; ctx.lineWidth=3; ctx.beginPath(); health.forEach((row,i)=>{ const x=xFor(i); const y=yFor(Math.max(yMin, Math.min(yMax, asNumber(row.rssi)))); if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke(); }
function drawPacketDelay(canvas, points) { const ctx=canvas.getContext('2d'); const w=canvas.width,h=canvas.height; ctx.clearRect(0,0,w,h); ctx.fillStyle='#050505'; ctx.fillRect(0,0,w,h); ctx.fillStyle='#eee'; ctx.font='22px monospace'; ctx.fillText('Packet delay = phone-local receive time since 2000 - H10 frame timestamp', 12, 30); if (!points || !points.length) { ctx.fillStyle='#aaa'; ctx.fillText('Waiting for PMD timing + connection PS-FTP timezone rows…', 12, 70); return; } const vals=[]; for (const p of points) { vals.push(p.delay_ms); vals.push(p.rolling_avg_ms); } let min=Math.min(...vals), max=Math.max(...vals); if (min===max) { min-=1; max+=1; } const pad=Math.max(1,(max-min)*0.08); min-=pad; max+=pad; const yFor=v=>h-(v-min)*(h-55)/(max-min)-10; ctx.strokeStyle='#333'; ctx.lineWidth=1; ctx.fillStyle='#aaa'; ctx.font='18px monospace'; for (let g=0; g<=4; g++) { const v=min+(max-min)*g/4; const y=yFor(v); ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(w,y); ctx.stroke(); ctx.fillText(`${v.toFixed(Math.abs(v)<100 ? 1 : 0)} ms`, 10, y-4); } ctx.strokeStyle='#ffffff'; ctx.lineWidth=1.5; ctx.beginPath(); points.forEach((p,i)=>{ const x=i*(w-1)/Math.max(1,points.length-1); const y=yFor(p.delay_ms); if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke(); ctx.strokeStyle='#ffb000'; ctx.lineWidth=3; ctx.beginPath(); points.forEach((p,i)=>{ const x=i*(w-1)/Math.max(1,points.length-1); const y=yFor(p.rolling_avg_ms); if (i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y); }); ctx.stroke(); const latest=points[points.length-1]; ctx.fillStyle='#fff'; ctx.font='20px monospace'; ctx.fillText(`latest ${latest.delay_ms.toFixed(2)} ms`, w-230, 28); ctx.fillStyle='#ffb000'; ctx.fillText(`avg1000 ${latest.rolling_avg_ms.toFixed(2)} ms`, w-260, 52); ctx.fillStyle='#fff'; ctx.fillRect(12,50,22,3); ctx.fillText('packet',42,56); ctx.fillStyle='#ffb000'; ctx.fillRect(140,50,22,3); ctx.fillText('rolling avg 1000',170,56); }
function fmtMaybe(value, digits=2, suffix='') { return (value === null || value === undefined || Number.isNaN(value)) ? '—' : `${Number(value).toFixed(digits)}${suffix}`; }
function driftSentence(drift) { if (!drift || !drift.points || drift.points < 10 || drift.h10_seconds_per_day === undefined) return `<p><b>${drift ? drift.label : 'drift'}:</b> waiting for enough data</p>`; const spd=Number(drift.h10_seconds_per_day); const direction=spd >= 0 ? 'faster' : 'slower'; const minutes=Number(drift.window_seconds)/60.0; return `<p><b>${drift.label}:</b> H10 clock appears <b>${direction}</b> than the phone collector clock by <b>${Math.abs(spd).toFixed(2)} s/day</b> <span class="small">(${drift.points} frames, ${minutes.toFixed(1)} min, ${Number(drift.h10_ppm_vs_collector).toFixed(2)} ppm)</span></p>`; }
function renderTiming(summary) { const host=document.getElementById('timingBox'); if (!summary || !summary.points || summary.points < 4) { host.innerHTML='<p>Waiting for timing rows from WAL.</p>'; return; } host.innerHTML=`${driftSentence(summary.short_drift)}${driftSentence(summary.long_drift)}<p class="small"><b>Interpretation:</b> positive/faster means the H10 timestamp advances more than the phone collector clock; negative/slower means it advances less. Short-term is noisy. Long-term becomes useful after tens of minutes.</p><p class="small">Packet delay still includes BLE/OS delay and H10 frame batching. The timezone offset is the one captured for each BLE connection. Gap vs expected latest ${fmtMaybe(summary.gap_latest_ms, 3, ' ms')}; max positive ${fmtMaybe(summary.gap_max_ms, 3, ' ms')}.</p>`; }
function htmlText(value) { return String(value ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;'); }
function fmtUnixMs(value) { const n=Number(value); return Number.isFinite(n)&&n>0 ? new Date(n).toLocaleString() : '—'; }
function walHealthHtml(status) {
  status = status || {};
  const archive = status.archive || {};
  const archiveState = archive.pending ? 'pending' : (archive.state || 'unknown');
  const sensorNs = status.latest_sensor_record_unix_ns || status.last_sensor_record_unix_ns;
  const ageMs = sensorNs ? (Date.now() - asNumber(sensorNs)/1_000_000) : null;
  const historyState = status.history_load_state || 'not_started';
  return `<p>Phone/source state: <b class="${status.offline?'bad':'ok'}">${htmlText(status.connection_state || 'offline')}</b>; connected <b>${!!status.phone_connected}</b>, collecting <b>${!!status.phone_collecting}</b>, uploading <b>${!!status.phone_uploading}</b>, server upload recent <b>${!!status.phone_upload_recent}</b>, replay <b>${!!status.replay}</b>; source <b>${htmlText(status.latest_source || '—')}</b></p>
<p>Cloud archive: <b class="${archiveState==='error'?'bad':archiveState==='ok'?'ok':''}">${htmlText(archiveState)}</b>; last success <b>${htmlText(fmtUnixMs(archive.last_success_unix_ms))}</b>, last failure <b>${htmlText(fmtUnixMs(archive.last_failure_unix_ms))}</b>; consecutive failures <b>${archive.consecutive_failures ?? 0}</b>; last archived segments/output files <b>${archive.last_archived_segments ?? 0}/${archive.last_output_files ?? 0}</b>; error <b class="${archive.error?'bad':'ok'}">${htmlText(archive.error || 'none')}</b></p>
<p>Phone health: battery <b>${fmtMaybe(status.phone_battery_percent,0,'%')}</b>, power save <b>${status.phone_power_save_mode ?? '—'}</b>, interactive <b>${status.phone_device_interactive ?? '—'}</b>, free <b>${fmtMaybe(status.phone_local_free_bytes,0,' B')}</b><br>backlog <b>${fmtMaybe(status.phone_backlog_records,0,' records')}</b> / <b>${fmtMaybe(status.phone_backlog_bytes,0,' B')}</b>; reported ages ECG/ACC/HR <b>${fmtMaybe(status.phone_reported_ecg_age_ms,0,'')}</b>/<b>${fmtMaybe(status.phone_reported_acc_age_ms,0,'')}</b>/<b>${fmtMaybe(status.phone_reported_hr_age_ms,0,'')} ms</b>; server-receive age <b>${fmtMaybe(status.upload_receive_freshness_age_ms,0,' ms')}</b></p>
<p>Last disconnect: <b>${htmlText(status.last_disconnect || 'none')}</b>; last upload ACK/error: <b>${htmlText(status.last_upload_ack || '—')}</b> / <b class="${status.last_upload_error?'bad':''}">${htmlText(status.last_upload_error || 'none')}</b>; last collector error: <b class="${status.last_collector_error?'bad':''}">${htmlText(status.last_collector_error || 'none')}</b></p>
<p>Live transport: <b>incremental SSE</b>; marker posting: <b>${htmlText(status.marker_status || '—')}</b></p>
<p>Windows: live <b>${fmtMaybe(status.live_window_seconds, 0, ' s')}</b>, history <b>${fmtMaybe(status.history_window_hours, 1, ' h')}</b></p>
<p>Critical load: <b>${htmlText(status.critical_load_index_state || '—')}</b>, <b>${status.critical_wal_records_processed ?? 0}</b> WAL records, <b>${status.critical_load_elapsed_ms ?? '—'} ms</b><br><span class="small">start ${status.critical_load_start_offset ?? '—'} ${htmlText(status.critical_load_start_path || '')}</span></p>
<p>History load: <b class="${historyState === 'error' ? 'bad' : historyState === 'complete' ? 'ok' : ''}">${htmlText(historyState)}</b>, scanned <b>${status.history_records_scanned ?? 0}</b>, matched <b>${status.history_records_matched ?? 0}</b>, elapsed <b>${status.history_load_elapsed_ms ?? '—'} ms</b><br><span class="small">events/markers load in background; old waveforms are intentionally not replayed into live buffers.</span></p>
<p>History metrics: disconnects <b>${status.history_disconnect_count ?? 0}</b>, watchdog/error/timeout <b>${status.history_watchdog_error_timeout_count ?? 0}</b>, RSSI latest/min/median <b>${fmtMaybe(status.history_rssi_latest_dbm,0,'')}</b>/<b>${fmtMaybe(status.history_rssi_min_dbm,0,'')}</b>/<b>${fmtMaybe(status.history_rssi_median_dbm,0,'')} dBm</b>, PMD gaps&gt;20ms <b>${status.history_pmd_gap_gt20ms_count ?? 0}</b> max <b>${fmtMaybe(status.history_pmd_max_positive_gap_ms,1,' ms')}</b></p>
<p>Last sensor record: <b>${sensorNs ? fmtNs(sensorNs) : '—'}</b> <span class="small">${ageMs === null ? '' : '(' + (ageMs/1000).toFixed(1) + ' s ago; live_lag ' + fmtMaybe(status.live_lag_ms,0,' ms') + ')'}</span></p>
<p>Freshness ages: ECG/ACC/HR/health <b>${fmtMaybe(status.ecg_freshness_age_ms,0,'')}</b>/<b>${fmtMaybe(status.acc_freshness_age_ms,0,'')}</b>/<b>${fmtMaybe(status.hr_freshness_age_ms,0,'')}</b>/<b>${fmtMaybe(status.health_freshness_age_ms,0,'')} ms</b>; PS-FTP offset <b>${fmtMaybe(status.current_tz_offset_minutes,0,' min')}</b></p>
<p>Catch-up records excluded from live: <b>${status.catch_up_records_skipped_live ?? 0}</b>; future-clock records excluded: <b>${status.future_records_skipped_live ?? 0}</b></p>
<p>WAL tail: last poll <b>${status.last_wal_poll_records_processed ?? 0}</b> rows in <b>${status.last_wal_poll_elapsed_ms ?? '—'} ms</b>; records processed <b>${status.wal_records_processed ?? '—'}</b>, segment reads <b>${status.wal_segments_read ?? '—'}</b>, latest seq <b>${status.latest_sequence ?? latestSeq}</b>, delta buffer <b>${status.delta_buffer_len ?? '—'}</b></p>
<p>Index: <b>${htmlText(status.index_build_state || '—')}</b>, points written <b>${status.index_points_written ?? 0}</b>, error <span class="${status.index_last_error ? 'bad' : 'ok'}">${htmlText(status.index_last_error || 'none')}</span></p>
<p>SSE: clients <b>${status.sse_connected_clients ?? 0}</b>, batches <b>${status.sse_batches_sent ?? 0}</b>, rows <b>${status.sse_rows_sent ?? 0}</b>, overflow/resync <b>${status.sse_overflow_resync_count ?? 0}</b>, last batch <b>${status.sse_last_batch_size ?? 0}</b></p>
<p>Buffers: ECG <b>${status.ecg_count ?? state.ecg.length}</b>, ACC <b>${status.acc_count ?? state.acc.length}</b>, HR <b>${status.hr_count ?? state.hr.length}</b>, RR <b>${status.rr_count ?? state.rr.length}</b>, events <b>${status.event_count ?? state.events.length}</b></p>
<p class="${status.last_tail_error ? 'bad' : 'ok'}">Tail error: ${htmlText(status.last_tail_error || 'none')}</p>
<p class="${status.history_last_error ? 'bad' : 'ok'}">History error: ${htmlText(status.history_last_error || 'none')}</p>
<p class="${status.sse_last_write_error ? 'bad' : 'ok'}">SSE write error: ${htmlText(status.sse_last_write_error || 'none')}</p>`;
}
function renderWalHealth(status) { document.getElementById('walHealth').innerHTML=walHealthHtml(status); }
function csrfToken(){const part=document.cookie.split(';').map(v=>v.trim()).find(v=>v.startsWith('sensors_csrf='));return part?decodeURIComponent(part.slice('sensors_csrf='.length)):'';}
async function markEvent(label) { const btn=document.getElementById('markLeave'); const status=document.getElementById('markerStatus'); btn.disabled=true; status.textContent='writing marker…'; try { const res=await fetch('/api/marker',{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':csrfToken()},body:JSON.stringify({label,client_id:'browser_dashboard'}),cache:'no-store'}); const payload=await res.json(); if (!res.ok || !payload.ok) throw new Error(payload.error || res.statusText); status.textContent=`marked ${fmtNs(payload.collector_unix_ns)} (${payload.label})`; } catch (err) { status.textContent=`marker failed: ${err}`; } finally { btn.disabled=false; } }
function renderActionBanner(data) {
  const banner=document.getElementById('actionBanner');
  for (const row of data.events || []) {
    const ns=asNumber(row.collector_unix_ns)||0;
    if (row.event==='pairing_breakdown_detected') lastPairingBreakdownNs=Math.max(lastPairingBreakdownNs,ns);
    else if (row.event==='pmd_stream_start_confirmed'||row.event==='wal_progress') lastStreamingProgressNs=Math.max(lastStreamingProgressNs,ns);
  }
  if (lastPairingBreakdownNs>0 && lastPairingBreakdownNs>lastStreamingProgressNs) {
    banner.style.display='block';
    banner.innerHTML='⚠️ ACTION NEEDED — Android reports a broken Bluetooth bond. On the Galaxy, open <b>Settings → Connections → Bluetooth</b>, choose <b>Polar H10</b>, then unpair and pair it again if Android still rejects the bond.<span class="sub">Keep the phone close to the worn strap. This banner clears when the foreground collector confirms that streaming resumed.</span>';
  } else banner.style.display='none';
}
function renderBatteryBanner(status) {
  const banner=document.getElementById('batteryBanner');
  if (!status.battery_low_active) { banner.style.display='none'; return; }
  const lowPct=status.battery_low_latest_percent ?? '—';
  const lowAt=status.battery_low_latest_unix_ns ? fmtNsDateTime(status.battery_low_latest_unix_ns) : '—';
  const latestPct=status.battery_latest_percent ?? '—';
  const latestAt=status.battery_latest_unix_ns ? fmtNsDateTime(status.battery_latest_unix_ns) : '—';
  const clearPct=status.battery_replaced_clear_percent ?? 90;
  banner.style.display='block';
  banner.innerHTML=`🔋 ACTION NEEDED — H10 battery is low. Replace the CR2025 coin cell.<span class="sub">Battery read ${lowPct}% at ${lowAt}; latest reading ${latestPct}% at ${latestAt}. Rest rebounds below ${clearPct}% are an aging-cell pattern and do not clear this banner. It clears automatically once a fresh battery reads ${clearPct}% or more.</span>`;
}
function renderLinkBanner(data) {
  const banner=document.getElementById('linkBanner');
  const status=data.status||{};
  let latestEcgNs=asNumber(status.latest_ecg_unix_ns)||0;
  for (const row of data.events || []) {
    const ns=asNumber(row.collector_unix_ns)||0;
    if (row.event==='connection_cycle_failed'||row.event==='ecg_watchdog_timeout'||row.event==='acc_watchdog_timeout'||row.event==='bluetooth_adapter_off') lastLinkFailureNs=Math.max(lastLinkFailureNs,ns);
    else if (row.event==='pmd_stream_start_confirmed') lastLinkRecoveryNs=Math.max(lastLinkRecoveryNs,ns);
  }
  if (status.replay) { banner.style.display='none'; return; }
  // Ages are null until the first row/upload ever arrives; asNumber would fold null to 0
  // ("perfectly fresh"), so keep null explicit here.
  const ecgAge=status.ecg_freshness_age_ms==null?null:asNumber(status.ecg_freshness_age_ms);
  const uploadAge=status.upload_receive_freshness_age_ms==null?null:asNumber(status.upload_receive_freshness_age_ms);
  const linkDown=lastLinkFailureNs>0 && lastLinkFailureNs>lastLinkRecoveryNs && lastLinkFailureNs>latestEcgNs;
  const ecgStale=!(ecgAge!=null && ecgAge<10000);
  const uploadsAlive=uploadAge!=null && uploadAge<30000;
  if (linkDown && ecgStale && uploadsAlive) {
    banner.className='';
    banner.style.display='block';
    banner.innerHTML='⚠️ DATA IS BEING LOST — the phone cannot reach the H10 strap. The strap does not store data itself: minutes without the Bluetooth link are gone for good. Get the phone within about 10 metres of the worn strap, check the strap is on with wet pads, and check Bluetooth is enabled on the phone.<span class="sub">The phone is reporting in and reconnect attempts are running. This banner clears when streaming resumes.</span>';
    return;
  }
  const everUploaded=status.latest_upload_received_unix_ns!=null;
  if (everUploaded && uploadAge!=null && uploadAge>60000) {
    const lastAt=fmtNsDateTime(status.latest_upload_received_unix_ns);
    const ageText=uploadAge>=120000?`${Math.round(uploadAge/60000)} minutes`:`${Math.round(uploadAge/1000)} seconds`;
    banner.className='buffering';
    banner.style.display='block';
    banner.innerHTML=`⏳ NO DATA ARRIVING — nothing has been received from the phone for ${ageText}. If the phone is with the wearer and recording, nothing is lost: data is buffered on the phone (days of capacity) and uploads when the connection recovers. If the phone is off, out of battery, or away from the worn strap, data is being lost right now.<span class="sub">Last upload received ${lastAt}. If recording was stopped on purpose, ignore this.</span>`;
    return;
  }
  banner.style.display='none';
}
function render() {
  const data=prepareData();
  document.getElementById('dataDir').textContent=data.status?.data_dir || data.data_dir || '';
  renderActionBanner(data);
  renderBatteryBanner(data.status || {});
  renderLinkBanner(data);
  const hr=data.latest_hr,health=data.latest_health,bat=data.latest_battery;
  const contact=hr ? (hr.contact_supported && hr.contact_detected) : false;
  document.getElementById('status').innerHTML=`HR <b>${hr ? hr.bpm : '—'}</b> bpm | contact <span class="${contact?'ok':'bad'}">${contact?'OK':'not OK/unknown'}</span> | RSSI ${health ? Number(health.rssi).toFixed(0) : '—'} | battery ${bat ? bat.battery_percent+'%' : '—'}`;
  document.getElementById('rssiSummary').textContent=`Latest RSSI: ${health ? Number(health.rssi).toFixed(0) + ' dBm' : '—'}`;
  document.getElementById('hrBox').innerHTML=`<p><b>${hr ? hr.bpm : '—'} bpm</b></p><p>RR latest: ${hr && hr.rr_ms.length ? hr.rr_ms.map(x=>Number(x).toFixed(1)).join(', ')+' ms' : '—'}</p><p>PMD frames: ${health ? health.pmd_frames : '—'}; HR frames: ${health ? health.hr_frames : '—'}</p>`;
  const sweepLatest=Math.max(tailTs(data.ecg,'device_ts_ns'),tailTs(data.acc,'device_ts_ns'));
  const heads=[genHeadTs(data.ecg,'device_ts_ns'),genHeadTs(data.acc,'device_ts_ns')].filter(v=>v>0);
  updateSweepFrame(sweepLatest,heads.length?Math.min(...heads):sweepLatest);
  drawEcg(document.getElementById('ecg'),data.ecg);
  drawSweepValueCanvas(document.getElementById('hrSweep'),data.hr,'bpm','bpm','#7ee787');
  drawSweepValueCanvas(document.getElementById('rrSweep'),data.rr,'rr_ms','ms','#ffa657');
  drawRssi(document.getElementById('rssi'),data.health);
  drawPacketDelay(document.getElementById('packetDelay'),data.packet_delay);
  renderTiming(data.timing_summary);
  renderWalHealth(data.status);
  drawAccTrace(document.getElementById('acc'),data.acc);
  renderBars(data.latest_acc);
  const eventsEl=document.getElementById('events');
  const eventsText=data.events.map(e=>{const session=e.session?` [${String(e.session).slice(0,8)}]`:'';return `${fmtNs(e.collector_unix_ns)}${session} ${e.event}: ${e.detail}`;}).join('\n');
  if (eventsText!==lastEventsText) {
    const previousScrollTop=eventsEl.scrollTop;
    const wasNearBottom=eventsEl.scrollTop+eventsEl.clientHeight>=eventsEl.scrollHeight-40;
    eventsEl.textContent=eventsText;
    eventsEl.scrollTop=wasNearBottom?eventsEl.scrollHeight:previousScrollTop;
    lastEventsText=eventsText;
  }
}
function renderLoop() { try { render(); } catch (err) { console.error(err); document.getElementById('status').textContent = err; } finally { setTimeout(renderLoop, RENDER_REFRESH_MS); } }
window.addEventListener('pagehide', event => {
  if (event.persisted) return;
  if (liveTransport?.kind === 'shared_worker') {
    liveTransport.port.postMessage({type:'disconnect'});
    liveTransport.port.close();
  } else if (eventSource) {
    eventSource.close();
  }
});
window.addEventListener('pageshow', event => {
  if (event.persisted && liveTransport?.kind === 'shared_worker') {
    liveTransport.port.postMessage({type:'subscribe', since:latestSeq, streamId:currentStreamId});
  }
});
setInterval(() => refreshStatus().catch(console.error), 10_000);
refreshSnapshot().then(() => refreshStatus()).then(() => { startSse(); renderLoop(); }).catch(err => { document.getElementById('status').textContent = `initial load failed: ${err}`; renderLoop(); });
</script>
</body>
</html>
"##;

fn unix_ns_i64_lossy() -> i64 {
    i64::try_from(crate::util::unix_ns()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ACC_MEASUREMENT_TYPE, ECG_MEASUREMENT_TYPE};
    use crate::replay::{
        ReplayConfig, ReplayOutput, StdReplaySleeper, replay_to_writer_with_sleeper,
    };
    use crate::wal::{
        CloudIngestBatchV1Body, CollectorEventBody, GattNotificationBody, GattReadResponseBody,
        PsftpSetLocalTimeAttemptBody, RssiSampleBody, WalRecordKind, WalWriter, WalWriterOptions,
        read_wal_records,
    };
    use chrono::{Datelike, Timelike};
    use std::fs::OpenOptions;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const START_NS: i64 = 1_770_000_000_000_000_000;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "training_sensors_live_server_{name}_{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).expect("uuid")
    }

    fn h10_context(ns: i64) -> WalRecordContext {
        WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            "training_sensors collect",
            uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        )
        .expect("context")
        .at_time(ns, u64::try_from(ns.saturating_sub(START_NS)).unwrap_or(0))
        .connection_id(Some(uuid("dddddddd-dddd-4ddd-8ddd-dddddddddddd")))
        .physical_device_id(Some("h10_a1b2c3d4"))
        .ble_peripheral_id(Some("72585434-07e3-d5e3-5ace-4e6c70118687"))
    }

    fn manual_context(ns: i64) -> WalRecordContext {
        WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            MANUAL_EVENTS_SOURCE,
            LIVE_WRITER_ID,
            uuid("ffffffff-ffff-4fff-8fff-ffffffffffff"),
            uuid("99999999-9999-4999-8999-999999999999"),
        )
        .expect("manual context")
        .at_time(ns, u64::try_from(ns.saturating_sub(START_NS)).unwrap_or(0))
    }

    fn pmd_frame(measurement_type: u8, frame_ts: u64, frame_type: u8, content: &[u8]) -> Vec<u8> {
        let mut data = Vec::with_capacity(content.len() + 10);
        data.push(measurement_type);
        data.extend_from_slice(&frame_ts.to_le_bytes());
        data.push(frame_type);
        data.extend_from_slice(content);
        data
    }

    fn hr_notification(bpm: u8) -> WalBody {
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                HR_MEASUREMENT_UUID,
                "heart_rate_measurement",
                &[0x16, bpm, 0x00, 0x04],
            )
            .expect("hr body"),
        )
    }

    fn ecg_notification() -> WalBody {
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                PMD_DATA_UUID,
                "pmd_data",
                &pmd_frame(
                    ECG_MEASUREMENT_TYPE,
                    1_000_000_000,
                    0,
                    &[1, 0, 0, 0xff, 0xff, 0xff],
                ),
            )
            .expect("ecg body"),
        )
    }

    fn acc_notification() -> WalBody {
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                PMD_DATA_UUID,
                "pmd_data",
                &pmd_frame(ACC_MEASUREMENT_TYPE, 2_000_000_000, 0, &[1, 2, 0xff]),
            )
            .expect("acc body"),
        )
    }

    fn malformed_pmd_notification() -> WalBody {
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                PMD_DATA_UUID,
                "pmd_data",
                &pmd_frame(ACC_MEASUREMENT_TYPE, 3_000_000_000, 0x80, &[0x01, 0x02]),
            )
            .expect("base64-valid malformed PMD body"),
        )
    }

    fn battery_read(level: u8) -> WalBody {
        WalBody::GattReadResponse(
            GattReadResponseBody::from_payload(
                uuid("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                BATTERY_LEVEL_UUID,
                "battery_level",
                &[level],
            )
            .expect("battery read"),
        )
    }

    fn collector_event(event: &str, detail: &str) -> WalBody {
        WalBody::CollectorEvent(CollectorEventBody {
            event: event.to_string(),
            severity: "info".to_string(),
            detail: detail.to_string(),
            fields: BTreeMap::new(),
        })
    }

    fn collector_event_fields(
        event: &str,
        detail: &str,
        fields: BTreeMap<String, Value>,
    ) -> WalBody {
        WalBody::CollectorEvent(CollectorEventBody {
            event: event.to_string(),
            severity: "info".to_string(),
            detail: detail.to_string(),
            fields,
        })
    }

    fn psftp_attempt(offset_minutes: i32) -> WalBody {
        WalBody::PsftpSetLocalTimeAttempt(PsftpSetLocalTimeAttemptBody {
            operation_id: Uuid::new_v4(),
            local_time_iso8601: "2026-08-04T08:00:00+00:00".to_string(),
            local_time_unix_ns: START_NS,
            tz_offset_minutes: offset_minutes,
            dst_offset_minutes: None,
            request_payload_b64: String::new(),
            control_characteristic_uuid: crate::config::PSFTP_MTU_UUID.to_string(),
            mtu_characteristic_uuid: crate::config::PSFTP_MTU_UUID.to_string(),
        })
    }

    fn rssi_sample(rssi_dbm: i16) -> WalBody {
        WalBody::RssiSample(RssiSampleBody {
            peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
            rssi_dbm,
            method: "test".to_string(),
            age_ms: 0,
        })
    }

    fn create_closed_h10_wal(base: &Path, offset_ns: i64, bodies: Vec<WalBody>) -> PathBuf {
        let records = bodies
            .into_iter()
            .enumerate()
            .map(|(index, body)| (offset_ns + i64::try_from(index).expect("index") + 1, body))
            .collect::<Vec<_>>();
        create_closed_h10_wal_at(base, offset_ns, records, offset_ns + 100)
    }

    fn create_closed_h10_wal_at(
        base: &Path,
        start_offset_ns: i64,
        records: Vec<(i64, WalBody)>,
        close_offset_ns: i64,
    ) -> PathBuf {
        let path_config =
            WalPathConfig::new(base, WAL_PARTICIPANT_ID, H10_WAL_SOURCE).expect("config");
        let start_ns = START_NS + start_offset_ns;
        let mut writer = WalWriter::create(
            &path_config,
            start_ns,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("writer");
        for (record_offset_ns, body) in records {
            let ns = START_NS + record_offset_ns;
            writer
                .append_record(&h10_context(ns).make_record(body))
                .expect("append");
        }
        let close_ns = START_NS + close_offset_ns;
        writer
            .close(&h10_context(close_ns), close_ns, "test", 0)
            .expect("close")
            .closed_path
    }

    fn create_open_h10_wal(base: &Path, bodies: Vec<WalBody>) -> PathBuf {
        let path_config =
            WalPathConfig::new(base, WAL_PARTICIPANT_ID, H10_WAL_SOURCE).expect("config");
        let mut writer = WalWriter::create(
            &path_config,
            START_NS,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("writer");
        for (index, body) in bodies.into_iter().enumerate() {
            let ns = START_NS + i64::try_from(index).expect("index") + 1;
            writer
                .append_record(&h10_context(ns).make_record(body))
                .expect("append");
        }
        let path = writer.open_path().to_path_buf();
        drop(writer);
        path
    }

    fn create_participant_hr_wal(base: &Path, participant_id: &str, bpm: u8) {
        let path_config = WalPathConfig::new(base, participant_id, H10_WAL_SOURCE).expect("config");
        let mut writer = WalWriter::create(
            &path_config,
            START_NS,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("writer");
        let context = WalRecordContext::new(
            participant_id,
            H10_WAL_SOURCE,
            "android collector",
            uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            Uuid::new_v4(),
        )
        .expect("context")
        .at_time(START_NS + 1, 1)
        .connection_id(Some(uuid("dddddddd-dddd-4ddd-8ddd-dddddddddddd")))
        .physical_device_id(Some("same_h10_sensor"));
        writer
            .append_record(&context.make_record(hr_notification(bpm)))
            .expect("append");
        writer
            .close(&context, START_NS + 2, "test", 0)
            .expect("close");
    }

    fn create_stale_manual_open(base: &Path) -> PathBuf {
        let path_config =
            WalPathConfig::new(base, WAL_PARTICIPANT_ID, MANUAL_EVENTS_SOURCE).expect("config");
        let mut writer = WalWriter::create(
            &path_config,
            START_NS,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("writer");
        let body = WalBody::ManualMarker(ManualMarkerBody {
            event_id: uuid("12345678-1234-4234-8234-123456789abc"),
            event_unix_ns: START_NS + 1,
            event_monotonic_ns: Some(1),
            event_time_source: "dashboard_server_receive".to_string(),
            created_by: "dashboard".to_string(),
            label: "old_marker".to_string(),
            note: None,
            client_id: Some("test".to_string()),
            client_event_id: None,
            dedupe_key: "12345678-1234-4234-8234-123456789abc".to_string(),
        });
        writer
            .append_record(&manual_context(START_NS + 1).make_record(body))
            .expect("append manual");
        let path = writer.open_path().to_path_buf();
        drop(writer);
        path
    }

    fn test_config(base: &Path, enable_markers: bool) -> LiveServerConfig {
        LiveServerConfig {
            data_dir: base.to_path_buf(),
            participant_id: WAL_PARTICIPANT_ID.to_string(),
            port: 0,
            live_window: Duration::from_secs(120),
            history_window: DEFAULT_HISTORY_WINDOW,
            poll_interval: Duration::from_millis(10),
            enable_markers,
        }
    }

    fn paths_ending_with(base: &Path, suffix: &str) -> Vec<PathBuf> {
        fn visit(path: &Path, suffix: &str, out: &mut Vec<PathBuf>) {
            if !path.exists() {
                return;
            }
            for entry in fs::read_dir(path).expect("read dir") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    visit(&path, suffix, out);
                } else if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.ends_with(suffix))
                    .unwrap_or(false)
                {
                    out.push(path);
                }
            }
        }
        let mut out = Vec::new();
        visit(base, suffix, &mut out);
        out.sort();
        out
    }

    #[test]
    fn fixture_wal_drives_dashboard_without_h10() {
        let dir = TestDir::new("fixture");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![
                ecg_notification(),
                acc_notification(),
                hr_notification(60),
                battery_read(87),
            ],
        );
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.ecg.len(), 2);
        assert_eq!(snapshot.ecg[0].microvolts, 1);
        assert_eq!(snapshot.ecg[1].microvolts, -1);
        assert_eq!(snapshot.acc.len(), 1);
        assert_eq!(snapshot.acc[0].raw_x, 1);
        assert_eq!(snapshot.hr.len(), 1);
        assert_eq!(snapshot.latest_hr.expect("hr").bpm, 60);
        assert_eq!(snapshot.rr.len(), 1);
        assert_eq!(
            snapshot.latest_battery.expect("battery").battery_percent,
            87
        );
        assert!(snapshot.timing.iter().any(|row| row.measurement == "ECG"));
    }

    #[test]
    fn structured_android_health_is_lossless_and_reduced_into_status() {
        let dir = TestDir::new("structured_android_health");
        let now = unix_ns_i64_lossy();
        let mut fields = BTreeMap::new();
        fields.insert("backlog_records".to_string(), Value::from(42));
        fields.insert("backlog_bytes".to_string(), Value::from(12_345));
        fields.insert("local_free_bytes".to_string(), Value::from(987_654_321_u64));
        fields.insert("phone_battery_percent".to_string(), Value::from(73));
        fields.insert("power_save_mode".to_string(), Value::from(true));
        fields.insert("device_interactive".to_string(), Value::from(false));
        fields.insert("ecg_age_ms".to_string(), Value::from(12));
        fields.insert("acc_age_ms".to_string(), Value::from(23));
        fields.insert("hr_age_ms".to_string(), Value::from(34));
        fields.insert("future_android_field".to_string(), Value::from("preserved"));
        create_closed_h10_wal_at(
            &dir.path,
            now - START_NS - 2_000_000,
            vec![(
                now - START_NS - 1_000_000,
                collector_event_fields("collector_health", "streaming", fields.clone()),
            )],
            now - START_NS,
        );
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), now).expect("runtime");
        let snapshot = runtime.snapshot();
        let status = snapshot.status;
        assert!(status.phone_collecting);
        assert_eq!(status.phone_backlog_records, Some(42));
        assert_eq!(status.phone_backlog_bytes, Some(12_345));
        assert_eq!(status.phone_local_free_bytes, Some(987_654_321));
        assert_eq!(status.phone_battery_percent, Some(73));
        assert_eq!(status.phone_power_save_mode, Some(true));
        assert_eq!(status.phone_device_interactive, Some(false));
        assert_eq!(status.phone_reported_ecg_age_ms, Some(12.0));
        assert_eq!(
            status.phone_health_fields.get("future_android_field"),
            Some(&Value::from("preserved"))
        );
        let event = snapshot
            .events
            .iter()
            .find(|event| event.event == "collector_health")
            .expect("collector health event");
        assert_eq!(event.fields, fields);
        assert_eq!(event.severity.as_deref(), Some("info"));
    }

    #[test]
    fn packet_delay_uses_each_connection_phone_timezone_and_survives_dst_change() {
        let mut parser = WalLiveParser::default();
        let mut state = LiveState::default();
        let first_connection = uuid("11111111-1111-4111-8111-111111111111");
        let second_connection = uuid("22222222-2222-4222-8222-222222222222");
        let first_attempt = h10_context(START_NS + 1)
            .connection_id(Some(first_connection))
            .make_record(psftp_attempt(60));
        parser
            .process_record(&first_attempt, &mut state)
            .expect("first timezone");
        let first_ecg = h10_context(START_NS + 2)
            .connection_id(Some(first_connection))
            .make_record(ecg_notification());
        parser
            .process_record(&first_ecg, &mut state)
            .expect("first ECG");
        let second_attempt = h10_context(START_NS + 3)
            .connection_id(Some(second_connection))
            .make_record(psftp_attempt(0));
        parser
            .process_record(&second_attempt, &mut state)
            .expect("DST timezone");
        let second_ecg = h10_context(START_NS + 4)
            .connection_id(Some(second_connection))
            .make_record(ecg_notification());
        parser
            .process_record(&second_ecg, &mut state)
            .expect("second ECG");

        let timing = state.timing.iter().collect::<Vec<_>>();
        assert_eq!(timing.len(), 2);
        assert_eq!(timing[0].tz_offset_minutes, Some(60));
        assert_eq!(timing[1].tz_offset_minutes, Some(0));
        assert_eq!(timing[0].connection_uuid, Some(first_connection));
        assert_eq!(timing[1].connection_uuid, Some(second_connection));
        let delay_difference = timing[0].packet_delay_ms.expect("first delay")
            - timing[1].packet_delay_ms.expect("second delay");
        assert!((delay_difference - 3_599_999.999_998).abs() < 0.01);
        assert!(!DASHBOARD_HTML.contains("getTimezoneOffset"));
        assert!(!DASHBOARD_HTML.contains("Mac local receive time"));
        assert!(!DASHBOARD_HTML.contains("macOS"));
        assert!(!DASHBOARD_HTML.contains("no re-pairing needed"));
        assert!(DASHBOARD_HTML.contains("Settings → Connections → Bluetooth"));
    }

    #[test]
    fn dashboard_html_carries_link_loss_banner() {
        assert!(DASHBOARD_HTML.contains("<div id=\"linkBanner\"></div>"));
        assert!(DASHBOARD_HTML.contains("#linkBanner.buffering"));
        assert!(DASHBOARD_HTML.contains("function renderLinkBanner"));
        assert!(DASHBOARD_HTML.contains("renderLinkBanner(data);"));
        assert!(DASHBOARD_HTML.contains("DATA IS BEING LOST"));
        assert!(DASHBOARD_HTML.contains("NO DATA ARRIVING"));
        // The red banner must key on device-clock ordering and a live upload path so
        // catch-up drains and upload stalls can never masquerade as live BLE loss.
        assert!(DASHBOARD_HTML.contains("lastLinkFailureNs>latestEcgNs"));
        assert!(DASHBOARD_HTML.contains("uploadAge!=null && uploadAge<30000"));
    }

    #[test]
    fn fresh_cloud_envelope_does_not_admit_ten_hour_old_children() {
        let dir = TestDir::new("cloud_envelope_child_cutoff");
        let now_offset = 12 * 60 * 60 * 1_000_000_000_i64;
        let stale = h10_context(START_NS + 1).make_record(hr_notification(50));
        let stale_event = h10_context(START_NS + 2)
            .make_record(collector_event("disconnect_result", "old outage event"));
        let fresh =
            h10_context(START_NS + now_offset - 1_000_000_000).make_record(hr_notification(70));
        let envelope = WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: Uuid::new_v4(),
            batch_id: Uuid::new_v4(),
            request_sha256: "a".repeat(64),
            encoded_bytes: 100,
            first_source_seq: 1,
            last_source_seq: 3,
            received_unix_ms: (START_NS + now_offset) / 1_000_000,
            record_sha256: vec!["b".repeat(64), "c".repeat(64), "d".repeat(64)],
            records: vec![stale, stale_event, fresh],
        });
        create_closed_h10_wal_at(
            &dir.path,
            now_offset - 2_000_000_000,
            vec![(now_offset - 500_000_000, envelope)],
            now_offset,
        );
        let now = START_NS + now_offset;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(10 * 60 * 60);
        let runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        assert_eq!(
            runtime
                .snapshot()
                .hr
                .iter()
                .map(|row| row.bpm)
                .collect::<Vec<_>>(),
            vec![70]
        );
        assert!(
            runtime
                .snapshot()
                .events
                .iter()
                .all(|event| !event.detail.contains("old outage"))
        );
        assert!(runtime
            .delta_batch_after(0, usize::MAX)
            .items
            .iter()
            .all(|item| !matches!(&item.change, LiveDeltaKind::Hr(row) if row.bpm == 50)
                && !matches!(&item.change, LiveDeltaKind::Event(row) if row.detail.contains("old outage"))));
        assert_eq!(runtime.status().catch_up_records_skipped_live, 2);
        let history = scan_history_at(config, now).expect("history");
        assert_eq!(history.metrics.records_scanned, 4); // three children plus segment close
        assert_eq!(history.metrics.records_matched, 2); // fresh child plus segment close
        assert!(
            history
                .events
                .iter()
                .all(|event| !event.detail.contains("old outage"))
        );
        assert_eq!(
            history.metrics.oldest_loaded_unix_ns,
            Some(START_NS + now_offset - 1_000_000_000)
        );
    }

    #[test]
    fn stale_cloud_backfill_is_excluded_then_fresh_rows_resume_incrementally() {
        let dir = TestDir::new("cloud_backfill_then_fresh");
        let now = unix_ns_i64_lossy();
        let stale_child =
            h10_context(now - 60 * 60 * 1_000_000_000).make_record(hr_notification(50));
        let stale_envelope = WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: Uuid::new_v4(),
            batch_id: Uuid::new_v4(),
            request_sha256: "1".repeat(64),
            encoded_bytes: 100,
            first_source_seq: 1,
            last_source_seq: 1,
            received_unix_ms: (now - 2_000_000_000) / 1_000_000,
            record_sha256: vec!["2".repeat(64)],
            records: vec![stale_child],
        });
        create_closed_h10_wal_at(
            &dir.path,
            now - START_NS - 3_000_000_000,
            vec![(now - START_NS - 2_000_000_000, stale_envelope)],
            now - START_NS - 1_500_000_000,
        );

        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        let mut runtime = LiveRuntime::new_at(config, now).expect("runtime");
        assert!(runtime.snapshot().hr.is_empty());
        assert_eq!(runtime.status().catch_up_records_skipped_live, 1);
        let before_fresh_seq = runtime.status().latest_sequence;

        let fresh_child = h10_context(now + 100_000_000).make_record(hr_notification(70));
        let fresh_envelope = WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: Uuid::new_v4(),
            batch_id: Uuid::new_v4(),
            request_sha256: "3".repeat(64),
            encoded_bytes: 100,
            first_source_seq: 2,
            last_source_seq: 2,
            received_unix_ms: (now + 200_000_000) / 1_000_000,
            record_sha256: vec!["4".repeat(64)],
            records: vec![fresh_child],
        });
        create_closed_h10_wal_at(
            &dir.path,
            now - START_NS + 50_000_000,
            vec![(now - START_NS + 200_000_000, fresh_envelope)],
            now - START_NS + 300_000_000,
        );
        runtime
            .tailer
            .poll_live_once(
                &dir.path,
                WAL_PARTICIPANT_ID,
                CaptureTimeWindow {
                    cutoff_unix_ns: now - 20_000_000_000,
                    now_unix_ns: now + 1_000_000_000,
                },
                &mut runtime.state,
                &mut runtime.index,
                &[H10_WAL_SOURCE],
            )
            .expect("poll fresh cloud batch");

        assert_eq!(
            runtime
                .snapshot()
                .hr
                .iter()
                .map(|row| row.bpm)
                .collect::<Vec<_>>(),
            vec![70]
        );
        let fresh_batch = runtime.delta_batch_after(before_fresh_seq, usize::MAX);
        assert!(
            fresh_batch
                .items
                .iter()
                .any(|item| matches!(&item.change, LiveDeltaKind::Hr(row) if row.bpm == 70))
        );
        assert!(
            fresh_batch
                .items
                .iter()
                .all(|item| !matches!(&item.change, LiveDeltaKind::Hr(row) if row.bpm == 50))
        );
        assert_eq!(runtime.status().catch_up_records_skipped_live, 1);
    }

    #[test]
    fn stale_catch_up_and_future_phone_clock_do_not_fake_live_connection() {
        let dir = TestDir::new("cloud_envelope_skew_freshness");
        let now = unix_ns_i64_lossy();
        let stale = h10_context(now - 60 * 60 * 1_000_000_000)
            .make_record(collector_event("collector_health", "stale catch-up"));
        // Ingest accepts a small bounded future skew so records remain durable,
        // but live freshness uses a much tighter tolerance and must not be
        // pinned for four minutes by this phone clock.
        let future = h10_context(now + 4 * 60 * 1_000_000_000)
            .make_record(collector_event("collector_health", "future phone clock"));
        let envelope = WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: Uuid::new_v4(),
            batch_id: Uuid::new_v4(),
            request_sha256: "e".repeat(64),
            encoded_bytes: 100,
            first_source_seq: 1,
            last_source_seq: 2,
            received_unix_ms: (now - 500_000_000) / 1_000_000,
            record_sha256: vec!["f".repeat(64), "0".repeat(64)],
            records: vec![stale, future],
        });
        create_closed_h10_wal_at(
            &dir.path,
            now - START_NS - 2_000_000_000,
            vec![(now - START_NS - 250_000_000, envelope)],
            now - START_NS,
        );
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2 * 60 * 60);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        let status = runtime.status();
        assert!(status.offline);
        assert!(!status.phone_connected);
        assert!(!status.phone_collecting);
        assert!(status.phone_upload_recent);
        assert!(status.upload_receive_freshness_age_ms.is_some());
        assert_eq!(status.catch_up_records_skipped_live, 1);
        assert_eq!(status.future_records_skipped_live, 1);
        assert!(runtime.snapshot().events.iter().all(|event| {
            !event.detail.contains("stale catch-up") && !event.detail.contains("future phone clock")
        }));

        runtime.apply_history_result(scan_history_at(config, now).expect("history"));
        assert!(runtime.status().offline);
        assert!(
            runtime
                .snapshot()
                .events
                .iter()
                .all(|event| !event.detail.contains("future phone clock"))
        );
    }

    #[test]
    fn malformed_pmd_child_is_quarantined_without_blocking_valid_siblings_or_retrying() {
        let dir = TestDir::new("malformed_pmd_quarantine");
        let now = unix_ns_i64_lossy();
        create_closed_h10_wal_at(
            &dir.path,
            now - START_NS - 2_000_000_000,
            vec![
                (now - START_NS - 1_000_000_000, malformed_pmd_notification()),
                (now - START_NS - 900_000_000, battery_read(20)),
                (now - START_NS - 800_000_000, hr_notification(77)),
            ],
            now - START_NS,
        );
        let config = test_config(&dir.path, false);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        assert_eq!(
            runtime.snapshot().latest_hr.expect("valid sibling HR").bpm,
            77
        );
        assert_eq!(
            runtime
                .snapshot()
                .latest_battery
                .expect("valid sibling battery")
                .battery_percent,
            20
        );
        let quarantine = runtime
            .snapshot()
            .events
            .into_iter()
            .find(|event| event.event == "wal_record_quarantined")
            .expect("loud live quarantine diagnostic");
        assert_eq!(quarantine.severity.as_deref(), Some("error"));
        assert_eq!(
            quarantine.fields.get("phase"),
            Some(&Value::String("live".to_string()))
        );
        assert!(quarantine.detail.contains("kind=gatt_notification"));
        let seq_after_first_read = runtime.status().latest_sequence;
        runtime
            .poll_once()
            .expect("second poll advances past quarantine");
        assert_eq!(runtime.status().latest_sequence, seq_after_first_read);

        let history = scan_history_at(config, now).expect("history continues after malformed PMD");
        assert!(history.metrics.battery_latch.low_active());
        assert!(history.events.iter().any(|event| {
            event.event == "wal_record_quarantined"
                && event.fields.get("phase") == Some(&Value::String("history".to_string()))
        }));
    }

    #[test]
    fn identical_sensor_ids_remain_isolated_between_tenant_runtimes() {
        let dir = TestDir::new("tenant_isolation");
        let first = Uuid::new_v4().to_string();
        let second = Uuid::new_v4().to_string();
        create_participant_hr_wal(&dir.path, &first, 61);
        create_participant_hr_wal(&dir.path, &second, 92);
        let mut first_config = test_config(&dir.path, false);
        first_config.participant_id = first;
        let mut second_config = test_config(&dir.path, false);
        second_config.participant_id = second;
        let first_runtime =
            LiveRuntime::new_at(first_config, START_NS + 10).expect("first runtime");
        let second_runtime =
            LiveRuntime::new_at(second_config, START_NS + 10).expect("second runtime");
        assert_eq!(
            first_runtime.snapshot().latest_hr.expect("first HR").bpm,
            61
        );
        assert_eq!(
            second_runtime.snapshot().latest_hr.expect("second HR").bpm,
            92
        );
        assert_eq!(
            first_runtime
                .snapshot()
                .hr
                .iter()
                .map(|row| row.bpm)
                .collect::<Vec<_>>(),
            vec![61]
        );
    }

    #[test]
    fn restart_history_restores_phone_backlog_disconnect_and_error_without_waveforms() {
        let dir = TestDir::new("restart_structured_history");
        let mut fields = BTreeMap::new();
        fields.insert("backlog_records".to_string(), Value::from(88));
        fields.insert("backlog_bytes".to_string(), Value::from(65_536));
        fields.insert("local_free_bytes".to_string(), Value::from(1_000_000));
        fields.insert("ecg_age_ms".to_string(), Value::from(10));
        fields.insert("acc_age_ms".to_string(), Value::from(20));
        fields.insert("hr_age_ms".to_string(), Value::from(30));
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![
                (1_000_000_000, ecg_notification()),
                (
                    2_000_000_000,
                    collector_event_fields("collector_health", "streaming", fields),
                ),
                (
                    3_000_000_000,
                    collector_event("disconnect_result", "link lost"),
                ),
                (
                    4_000_000_000,
                    collector_event("ecg_watchdog_timeout", "ECG stale"),
                ),
            ],
            5_000_000_000,
        );
        let now = START_NS + 60_000_000_000;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(120);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        assert!(runtime.snapshot().ecg.is_empty());
        runtime.apply_history_result(scan_history_at(config, now).expect("history"));
        let snapshot = runtime.snapshot();
        assert!(
            snapshot.ecg.is_empty(),
            "history must not replay old waveforms"
        );
        assert_eq!(snapshot.status.phone_backlog_records, Some(88));
        assert!(snapshot.status.last_disconnect.is_some());
        assert!(snapshot.status.last_collector_error.is_some());
        assert!(snapshot.status.history_disconnect_count >= 1);
        assert!(snapshot.status.history_watchdog_error_timeout_count >= 1);
    }

    #[test]
    fn operational_state_distinguishes_uploading_replay_and_offline() {
        let dir = TestDir::new("operational_states");
        let now = unix_ns_i64_lossy();
        let mut uploading =
            LiveRuntime::new_at(test_config(&dir.path, false), now).expect("runtime");
        let upload = h10_context(now).make_record(collector_event(
            "upload_attempt",
            "uploading immutable batch",
        ));
        uploading
            .tailer
            .parser
            .process_record(&upload, &mut uploading.state)
            .expect("upload event");
        assert!(uploading.status().phone_uploading);
        assert_eq!(uploading.status().connection_state, "uploading");

        let replay_context = WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            REPLAY_H10_SOURCE,
            "deterministic replay",
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .expect("replay context")
        .at_time(now, 0)
        .connection_id(Some(Uuid::new_v4()));
        let replay_hr = replay_context.make_record(hr_notification(64));
        uploading
            .tailer
            .parser
            .process_record(&replay_hr, &mut uploading.state)
            .expect("replay HR");
        let replay_status = uploading.status();
        assert!(replay_status.replay);
        assert_eq!(replay_status.connection_state, "replay");

        let manual_marker =
            manual_context(now + 1).make_record(WalBody::ManualMarker(ManualMarkerBody {
                event_id: Uuid::new_v4(),
                event_unix_ns: now + 1,
                event_monotonic_ns: None,
                event_time_source: "dashboard_server_receive".to_string(),
                created_by: "dashboard".to_string(),
                label: "admin note".to_string(),
                note: Some("replay remains replay".to_string()),
                client_id: None,
                client_event_id: None,
                dedupe_key: Uuid::new_v4().to_string(),
            }));
        uploading
            .tailer
            .parser
            .process_record(&manual_marker, &mut uploading.state)
            .expect("manual marker");
        assert!(uploading.status().replay);
        assert_eq!(
            uploading.status().latest_source.as_deref(),
            Some(REPLAY_H10_SOURCE),
            "server-side markers must not replace the active replay source"
        );

        let mut stale =
            LiveRuntime::new_at(test_config(&dir.path, false), now).expect("stale runtime");
        let stale_event = h10_context(now - 20_000_000_000)
            .make_record(collector_event("collector_health", "streaming"));
        stale
            .tailer
            .parser
            .process_record(&stale_event, &mut stale.state)
            .expect("stale event");
        let fresh_segment_close =
            h10_context(now).make_record(WalBody::SegmentClosed(SegmentClosedBody {
                segment_id: Uuid::new_v4(),
                segment_end_unix_ns: now,
                reason: "server recovery".to_string(),
                record_count_before_this: 1,
                size_bytes_before_this: 100,
                truncated_tail_bytes: 0,
            }));
        stale
            .tailer
            .parser
            .process_record(&fresh_segment_close, &mut stale.state)
            .expect("fresh server segment close");
        let fresh_manual =
            manual_context(now).make_record(WalBody::ManualMarker(ManualMarkerBody {
                event_id: Uuid::new_v4(),
                event_unix_ns: now,
                event_monotonic_ns: None,
                event_time_source: "dashboard_server_receive".to_string(),
                created_by: "dashboard".to_string(),
                label: "fresh admin marker".to_string(),
                note: None,
                client_id: None,
                client_event_id: None,
                dedupe_key: Uuid::new_v4().to_string(),
            }));
        stale
            .tailer
            .parser
            .process_record(&fresh_manual, &mut stale.state)
            .expect("fresh manual marker");
        assert!(stale.status().offline);
        assert_eq!(stale.status().connection_state, "offline");
    }

    #[test]
    fn restart_history_restores_connection_timezone_for_fresh_pmd_rows() {
        let dir = TestDir::new("restart_timezone_history");
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![(1_000_000_000, psftp_attempt(120))],
            2_000_000_000,
        );
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            vec![(1_000_000_000_001, ecg_notification())],
            1_000_000_000_100,
        );
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2_000);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        assert_eq!(runtime.status().current_tz_offset_minutes, None);
        runtime.apply_history_result(scan_history_at(config, now).expect("history"));
        let status = runtime.status();
        assert_eq!(status.current_tz_offset_minutes, Some(120));
        assert_eq!(
            status
                .connection_timezone_offsets
                .get("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
            Some(&120)
        );
    }

    #[test]
    fn battery_latch_trips_at_trigger_and_clears_only_at_replaced_level() {
        let mut latch = BatteryLatch::default();
        assert!(!latch.low_active());
        latch.note(1_000, 31);
        assert!(!latch.low_active(), "31% is above the 30% trigger");
        latch.note(2_000, 30);
        assert!(latch.low_active(), "30% reading must trip the latch");
        latch.note(3_000, 50);
        latch.note(4_000, 70);
        assert!(
            latch.low_active(),
            "rest rebounds below 90% must not clear the latch"
        );
        assert_eq!(latch.low_latest_percent, Some(30));
        assert_eq!(latch.latest_percent, Some(70));
        latch.note(5_000, 89);
        assert!(latch.low_active(), "89% is below the 90% clear threshold");
        latch.note(6_000, 90);
        assert!(!latch.low_active(), "90% reading must clear the latch");
        latch.note(7_000, 25);
        assert!(latch.low_active(), "a newer low reading must re-trip");
        assert_eq!(latch.low_latest_percent, Some(25));
    }

    #[test]
    fn battery_latch_is_order_insensitive() {
        // Low reading observed first, older replaced-level reading second: the
        // replaced reading is stale, so the latch stays tripped.
        let mut latch = BatteryLatch::default();
        latch.note(9_000, 10);
        latch.note(2_000, 100);
        assert!(latch.low_active());
        // Replaced-level reading is newer even though it arrives first.
        let mut latch = BatteryLatch::default();
        latch.note(9_000, 100);
        latch.note(2_000, 10);
        assert!(!latch.low_active());
        assert_eq!(latch.latest_percent, Some(100));
    }

    #[test]
    fn battery_latch_merged_combines_history_and_live_maxima() {
        let mut history = BatteryLatch::default();
        history.note(1_000, 10);
        let mut live = BatteryLatch::default();
        live.note(5_000, 100);
        assert!(!history.merged(&live).low_active());
        assert!(!live.merged(&history).low_active());
        assert_eq!(history.merged(&live).latest_percent, Some(100));

        let mut history = BatteryLatch::default();
        history.note(5_000, 20);
        let mut live = BatteryLatch::default();
        live.note(1_000, 100);
        let merged = history.merged(&live);
        assert!(merged.low_active());
        assert_eq!(merged.low_latest_percent, Some(20));
        assert_eq!(merged.latest_percent, Some(20));
        assert_eq!(merged.replaced_latest_unix_ns, Some(1_000));

        let empty = BatteryLatch::default();
        assert!(!empty.merged(&BatteryLatch::default()).low_active());
    }

    #[test]
    fn battery_low_banner_status_from_live_wal() {
        let dir = TestDir::new("battery_low_live");
        create_closed_h10_wal(&dir.path, 0, vec![battery_read(25)]);
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let status = runtime.status();
        assert!(status.battery_low_active);
        assert_eq!(status.battery_low_latest_percent, Some(25));
        assert_eq!(status.battery_latest_percent, Some(25));
        assert_eq!(status.battery_low_trigger_percent, 30);
        assert_eq!(status.battery_replaced_clear_percent, 90);
        assert!(status.battery_low_latest_unix_ns.is_some());
    }

    #[test]
    fn battery_low_banner_clears_after_replacement_reading() {
        let dir = TestDir::new("battery_replaced_live");
        create_closed_h10_wal(&dir.path, 0, vec![battery_read(25), battery_read(100)]);
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let status = runtime.status();
        assert!(!status.battery_low_active);
        assert_eq!(status.battery_latest_percent, Some(100));
        assert!(status.battery_replaced_latest_unix_ns.is_some());
    }

    #[test]
    fn battery_low_banner_survives_via_history_scan_when_outside_live_window() {
        let dir = TestDir::new("battery_low_history");
        // Battery readings far older than the live window but inside the history
        // window: 20% then a 60% rebound, mirroring an aging cell observed hours
        // before a dashboard restart.
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![
                (1_000_000_000, battery_read(20)),
                (2_000_000_000, battery_read(60)),
            ],
            3_000_000_000,
        );
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            vec![(1_000_000_000_001, hr_notification(70))],
            1_000_000_000_100,
        );
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2_000);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        assert!(
            !runtime.status().battery_low_active,
            "critical window holds no battery rows yet"
        );
        let history = scan_history_at(config, now).expect("history scan");
        runtime.apply_history_result(history);
        let status = runtime.status();
        assert!(
            status.battery_low_active,
            "history battery readings must trip the banner after a restart"
        );
        assert_eq!(status.battery_low_latest_percent, Some(20));
        assert_eq!(
            status.battery_latest_percent,
            Some(60),
            "latest reading is the 60% rebound, which must not clear the latch"
        );
    }

    #[test]
    fn battery_low_banner_cleared_by_newer_live_replacement_over_history_low() {
        let dir = TestDir::new("battery_replaced_after_history_low");
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![(1_000_000_000, battery_read(15))],
            2_000_000_000,
        );
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            vec![(1_000_000_000_001, battery_read(100))],
            1_000_000_000_100,
        );
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2_000);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        let history = scan_history_at(config, now).expect("history scan");
        runtime.apply_history_result(history);
        let status = runtime.status();
        assert!(
            !status.battery_low_active,
            "fresh-battery reading newer than the history low must clear the banner"
        );
        assert_eq!(status.battery_latest_percent, Some(100));
    }

    #[test]
    fn startup_loads_recent_window_only_not_full_catch_up() {
        let dir = TestDir::new("recent_only");
        create_closed_h10_wal(&dir.path, 0, vec![hr_notification(50)]);
        create_closed_h10_wal(&dir.path, 1_000_000_000_000, vec![hr_notification(61)]);
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), now).expect("runtime");
        let snapshot = runtime.snapshot();
        let bpms = snapshot.hr.iter().map(|row| row.bpm).collect::<Vec<_>>();
        assert_eq!(bpms, vec![61]);
        assert_eq!(
            snapshot.status.wal_segments_read, 1,
            "critical startup must not synchronously scan older history WAL"
        );
        assert_eq!(snapshot.status.history_load_state, "not_started");
    }

    #[test]
    fn critical_start_uses_sparse_live_index_offset() {
        let dir = TestDir::new("critical_index");
        let records = (0..61_i64)
            .map(|second| {
                (
                    second * 1_000_000_000,
                    hr_notification(u8::try_from(60 + second).expect("bpm")),
                )
            })
            .collect::<Vec<_>>();
        let path = create_closed_h10_wal_at(&dir.path, 0, records, 62_000_000_000);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("name");
        let file = parse_wal_file_name(&path, name)
            .expect("parse name")
            .expect("wal file");
        let read = read_wal_records_incremental(&path, WalReadMode::Strict, &HashMap::new())
            .expect("read full WAL");
        let mut index_metrics = IndexBuildMetrics::default();
        best_effort_update_live_index(
            &dir.path,
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            &file,
            &read,
            &mut index_metrics,
        );
        assert!(index_metrics.points_written >= 50, "{index_metrics:?}");

        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        let runtime = LiveRuntime::new_at(config, START_NS + 60_000_000_000).expect("runtime");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.status.critical_load_index_state, "used");
        assert!(
            snapshot.status.critical_load_start_offset.unwrap_or(0) > WAL_HEADER_LEN as u64,
            "critical load should seek into the WAL using the derived index"
        );
        assert!(
            snapshot.status.critical_wal_records_processed
                < u64::try_from(read.records.len()).unwrap(),
            "indexed critical load should not process the whole segment"
        );
        assert!(snapshot.hr.iter().all(|row| row.bpm >= 100));
    }

    #[test]
    fn stale_or_corrupt_live_index_falls_back_and_repairs_cache() {
        let dir = TestDir::new("stale_index_repair");
        let records = (0..30_i64)
            .map(|second| (second * 1_000_000_000, hr_notification(80)))
            .collect::<Vec<_>>();
        let path = create_closed_h10_wal_at(&dir.path, 0, records, 31_000_000_000);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("name");
        let file = parse_wal_file_name(&path, name)
            .expect("parse")
            .expect("wal file");
        let read = read_wal_records_incremental(&path, WalReadMode::Strict, &HashMap::new())
            .expect("read");
        let mut index_metrics = IndexBuildMetrics::default();
        best_effort_update_live_index(
            &dir.path,
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            &file,
            &read,
            &mut index_metrics,
        );
        let index_path =
            live_index_path(&dir.path, WAL_PARTICIPANT_ID, H10_WAL_SOURCE, &read.header)
                .expect("index path");
        let mut index: LiveWalIndexFile =
            serde_json::from_slice(&fs::read(&index_path).expect("read index"))
                .expect("decode index");
        for point in &mut index.points {
            point.frame_offset = read.records[0].frame_offset + WAL_FRAME_HEADER_LEN as u64;
        }
        fs::write(&index_path, serde_json::to_vec(&index).expect("json"))
            .expect("write in-range non-frame-boundary index");

        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        let runtime = LiveRuntime::new_at(config.clone(), START_NS + 29_000_000_000)
            .expect("runtime should retry fallback after an in-range bad index offset");
        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot.status.critical_load_index_state,
            "missing_or_stale_fallback"
        );
        assert!(!snapshot.hr.is_empty());
        let mut repaired: LiveWalIndexFile =
            serde_json::from_slice(&fs::read(&index_path).expect("read repaired index"))
                .expect("decode repaired index");
        assert!(repaired.points.iter().all(|point| point.frame_offset
            != read.records[0].frame_offset + WAL_FRAME_HEADER_LEN as u64));

        repaired.points[0].frame_offset = read.file_len + 4096;
        fs::write(&index_path, serde_json::to_vec(&repaired).expect("json"))
            .expect("write stale out-of-range index");
        let runtime = LiveRuntime::new_at(config.clone(), START_NS + 29_000_000_000)
            .expect("runtime should ignore stale out-of-range index");
        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot.status.critical_load_index_state,
            "missing_or_stale_fallback"
        );
        assert!(!snapshot.hr.is_empty());
        let repaired: LiveWalIndexFile =
            serde_json::from_slice(&fs::read(&index_path).expect("read repaired index"))
                .expect("decode repaired index");
        assert!(
            repaired
                .points
                .iter()
                .all(|point| point.frame_offset < read.file_len)
        );

        fs::write(&index_path, b"not valid json").expect("corrupt index");
        let runtime = LiveRuntime::new_at(config, START_NS + 29_000_000_000)
            .expect("runtime should ignore corrupt index and rebuild it");
        assert_eq!(
            runtime.snapshot().status.critical_load_index_state,
            "missing_or_stale_fallback"
        );
        let _: LiveWalIndexFile =
            serde_json::from_slice(&fs::read(&index_path).expect("read repaired corrupt index"))
                .expect("corrupt index should be repaired to valid JSON");
    }

    #[test]
    fn missing_index_fallback_scans_newest_segment_only() {
        let dir = TestDir::new("critical_missing_index");
        create_closed_h10_wal(&dir.path, 0, vec![hr_notification(50)]);
        let records = (0..61_i64)
            .map(|second| {
                (
                    1_000_000_000_000 + second * 1_000_000_000,
                    hr_notification(61),
                )
            })
            .collect::<Vec<_>>();
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            records,
            1_000_000_000_000 + 62_000_000_000,
        );
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        let now = START_NS + 1_000_000_000_000 + 60_000_000_000;
        let runtime = LiveRuntime::new_at(config, now).expect("runtime");
        let snapshot = runtime.snapshot();
        assert_eq!(
            snapshot.status.critical_load_index_state,
            "missing_or_stale_fallback"
        );
        assert_eq!(
            snapshot.status.wal_segments_read, 1,
            "missing-index fallback may scan the newest active segment, not the older history window"
        );
        assert!(
            snapshot.status.critical_wal_records_processed >= 60,
            "critical_wal_records_processed should count decoded WAL records during fallback, not only rows admitted after the cutoff"
        );
        assert!(snapshot.hr.iter().all(|row| row.bpm == 61));
    }

    #[test]
    fn background_history_scan_fills_events_and_metrics_not_waveforms() {
        let dir = TestDir::new("background_history");
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![
                (
                    1_000_000_000,
                    collector_event("central_device_disconnected", "timeout"),
                ),
                (2_000_000_000, rssi_sample(-80)),
                (3_000_000_000, hr_notification(50)),
            ],
            4_000_000_000,
        );
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            vec![(1_000_000_000_001, hr_notification(70))],
            1_000_000_000_100,
        );
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2_000);
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        let before = runtime.snapshot();
        assert_eq!(
            before.hr.iter().map(|row| row.bpm).collect::<Vec<_>>(),
            vec![70]
        );
        assert!(
            before
                .events
                .iter()
                .all(|event| !event.detail.contains("timeout")),
            "old history event should not be in the critical live window"
        );

        let history = scan_history_at(config, now).expect("history scan");
        runtime.apply_history_result(history);
        let after = runtime.snapshot();
        assert_eq!(
            after.hr.iter().map(|row| row.bpm).collect::<Vec<_>>(),
            vec![70]
        );
        assert!(
            after
                .events
                .iter()
                .any(|event| event.detail.contains("timeout")),
            "background history should merge old operational events"
        );
        assert_eq!(after.status.history_load_state, "complete");
        assert_eq!(after.status.history_rssi_min_dbm, Some(-80.0));
        assert!(after.status.history_disconnect_count >= 1);
    }

    #[test]
    fn history_merge_does_not_block_later_incremental_live_batches() {
        let dir = TestDir::new("history_then_incremental");
        create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![(
                1_000_000_000,
                collector_event("old_watchdog_timeout", "old event"),
            )],
            2_000_000_000,
        );
        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_000_000,
            vec![(1_000_000_000_001, hr_notification(70))],
            1_000_000_000_100,
        );
        let mut config = test_config(&dir.path, false);
        config.live_window = Duration::from_secs(20);
        config.history_window = Duration::from_secs(2_000);
        let now = START_NS + 1_000_000_000_000 + 10_000_000_000;
        let mut runtime = LiveRuntime::new_at(config.clone(), now).expect("runtime");
        let seq_after_critical = runtime.status().latest_sequence;
        let history = scan_history_at(config.clone(), now).expect("history");
        runtime.apply_history_result(history);
        let history_batch = runtime.delta_batch_after(seq_after_critical, 100);
        assert!(
            history_batch
                .items
                .iter()
                .any(|item| matches!(item.change, LiveDeltaKind::Event(_)))
        );
        assert!(
            history_batch
                .items
                .iter()
                .all(|item| !matches!(item.change, LiveDeltaKind::Hr(_)))
        );

        create_closed_h10_wal_at(
            &dir.path,
            1_000_000_020_000,
            vec![(1_000_000_020_001, hr_notification(71))],
            1_000_000_020_100,
        );
        runtime
            .tailer
            .poll_live_once(
                &dir.path,
                WAL_PARTICIPANT_ID,
                CaptureTimeWindow {
                    cutoff_unix_ns: START_NS + 1_000_000_000_000,
                    now_unix_ns: START_NS + 1_000_000_030_000,
                },
                &mut runtime.state,
                &mut runtime.index,
                &[H10_WAL_SOURCE],
            )
            .expect("poll new live WAL");
        let live_batch = runtime.delta_batch_after(history_batch.to_seq, 100);
        assert!(
            live_batch
                .items
                .iter()
                .any(|item| matches!(&item.change, LiveDeltaKind::Hr(row) if row.bpm == 71))
        );
    }

    #[test]
    fn live_startup_ignores_parquet_archive_tree() {
        let dir = TestDir::new("ignore_parquet");
        let parquet_dir = dir
            .path
            .join("archive/parquet/participant_id=local/stream=ecg/date=2026-01-01");
        fs::create_dir_all(&parquet_dir).expect("parquet dir");
        fs::write(parquet_dir.join("corrupt.parquet"), b"not parquet")
            .expect("write corrupt parquet");
        create_closed_h10_wal(&dir.path, 0, vec![hr_notification(65)]);
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 1_000_000_000)
            .expect("runtime should ignore Parquet entirely");
        assert_eq!(runtime.snapshot().latest_hr.expect("hr").bpm, 65);
    }

    #[test]
    fn ten_one_record_wal_boundaries_do_not_freeze_dashboard_load() {
        let dir = TestDir::new("one_record_boundaries");
        for index in 0..10_u8 {
            create_closed_h10_wal(
                &dir.path,
                i64::from(index) * 1_000_000_000,
                vec![hr_notification(50 + index)],
            );
        }
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 20_000_000_000)
            .expect("runtime");
        let snapshot = runtime.snapshot();
        let bpms = snapshot.hr.iter().map(|row| row.bpm).collect::<Vec<_>>();
        assert_eq!(bpms, (50_u16..60).collect::<Vec<_>>());
        assert_eq!(snapshot.status.last_tail_error, None);
    }

    #[test]
    fn wal_fixture_feeds_archiver_replay_live_and_marker_paths() {
        let dir = TestDir::new("final_architecture_fixture");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![
                ecg_notification(),
                acc_notification(),
                hr_notification(62),
                battery_read(86),
            ],
        );

        let archive_summary = crate::archive::run_wal_archiver(&dir.path).expect("archive fixture");
        assert_eq!(archive_summary.scanned_segments, 1);
        assert_eq!(archive_summary.archived_segments, 1);
        assert!(archive_summary.output_files >= 6, "{archive_summary:?}");

        let mut replay_config = ReplayConfig::new(dir.path.join("wal"));
        replay_config.output = ReplayOutput::Summary;
        let mut replay_output = Vec::new();
        let mut sleeper = StdReplaySleeper;
        let replay_summary =
            replay_to_writer_with_sleeper(&replay_config, &mut replay_output, &mut sleeper)
                .expect("replay fixture");
        assert_eq!(replay_summary.segments, 1);
        assert!(replay_summary.replay_events >= 6, "{replay_summary:?}");

        let mut runtime =
            LiveRuntime::new_at(test_config(&dir.path, true), START_NS + 10_000_000_000)
                .expect("runtime");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.latest_hr.expect("hr").bpm, 62);
        assert_eq!(snapshot.ecg.len(), 2);
        assert_eq!(snapshot.acc.len(), 1);
        assert_eq!(
            snapshot.latest_battery.expect("battery").battery_percent,
            86
        );

        let marker = runtime
            .post_marker(MarkerPostRequest {
                label: Some("fixture_marker".to_string()),
                note: None,
                client_id: Some("test".to_string()),
                client_event_id: None,
            })
            .expect("marker");
        assert_eq!(marker.label, "fixture_marker");
        let manual_open = paths_ending_with(
            &dir.path
                .join("wal/participant_id=local/source=manual_events"),
            ".open",
        );
        assert_eq!(manual_open.len(), 1);
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn invalid_timestamp_replays_raw_and_does_not_advance_live_sample_freshness() {
        let dir = TestDir::new("invalid_timestamp");
        let payload = pmd_frame(ECG_MEASUREMENT_TYPE, u64::MAX, 0, &[1, 2, 3]);
        let invalid = WalBody::GattNotification(
            GattNotificationBody::from_payload(PMD_DATA_UUID, "pmd_data", &payload).unwrap(),
        );
        let path = create_closed_h10_wal_at(
            &dir.path,
            0,
            vec![
                (1, invalid.clone()),
                (2, ecg_notification()),
                (3, acc_notification()),
                (4, invalid),
            ],
            5,
        );
        let mut output = Vec::new();
        replay_to_writer_with_sleeper(&ReplayConfig::new(path), &mut output, &mut StdReplaySleeper)
            .unwrap();
        let text = String::from_utf8(output).unwrap();
        assert_eq!(text.matches("pmd_invalid_timestamp").count(), 2);
        let encoded = base64::engine::general_purpose::STANDARD.encode(&payload);
        assert_eq!(text.matches(&encoded).count(), 2);
        let events: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.iter().filter(|e| e["type"] == "ecg").count(), 2);
        assert_eq!(
            events.iter().filter(|e| e["type"] == "pmd_timing").count(),
            2
        );

        let runtime =
            LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000).unwrap();
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.ecg.len(), 2);
        assert_eq!(snapshot.acc.len(), 1);
        assert_eq!(
            snapshot.status.latest_ecg_unix_ns,
            Some((START_NS + 2) as u128)
        );
        assert_eq!(
            snapshot.status.latest_acc_unix_ns,
            Some((START_NS + 3) as u128)
        );
        assert_eq!(
            snapshot.status.last_sensor_record_unix_ns,
            Some((START_NS + 3) as u128)
        );
        assert_eq!(
            snapshot
                .events
                .iter()
                .filter(|e| e.event == "pmd_invalid_timestamp")
                .count(),
            2
        );
    }

    #[test]
    fn legacy_python_dashboard_script_is_not_active() {
        assert!(
            !Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("scripts/dashboard.py")
                .exists(),
            "scripts/dashboard.py must not be active after the WAL live cut-over"
        );
    }

    #[test]
    fn snapshots_status_and_batches_share_one_runtime_stream_generation() {
        let dir = TestDir::new("stream_generation");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![ecg_notification(), acc_notification(), hr_notification(63)],
        );
        let first_id = uuid("11111111-1111-4111-8111-111111111111");
        let second_id = uuid("22222222-2222-4222-8222-222222222222");
        let first = LiveRuntime::new_at_with_stream_id(
            test_config(&dir.path, false),
            START_NS + 10_000_000_000,
            first_id,
        )
        .expect("first runtime");
        let snapshot = first.snapshot();
        let status = first.status();
        let batch = first.delta_batch_after(0, 10);
        assert_eq!(snapshot.stream_id, first_id);
        assert_eq!(snapshot.status.stream_id, first_id);
        assert_eq!(status.stream_id, first_id);
        assert_eq!(batch.stream_id, first_id);
        assert_eq!(batch.status.stream_id, first_id);

        let second = LiveRuntime::new_at_with_stream_id(
            test_config(&dir.path, false),
            START_NS + 10_000_000_000,
            second_id,
        )
        .expect("replacement runtime");
        assert_eq!(second.snapshot().stream_id, second_id);
        assert_ne!(second.snapshot().stream_id, snapshot.stream_id);
    }

    #[test]
    fn delta_batches_are_incremental_not_full_snapshots() {
        let dir = TestDir::new("delta_incremental");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![ecg_notification(), acc_notification(), hr_notification(63)],
        );
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let snapshot = runtime.snapshot();
        assert!(snapshot.ecg.len() >= 2);
        assert!(!snapshot.acc.is_empty());
        let batch = runtime.delta_batch_after(0, 10);
        assert!(!batch.items.is_empty());
        assert!(
            batch
                .items
                .iter()
                .any(|item| matches!(item.change, LiveDeltaKind::Ecg(_)))
        );
        let json = serde_json::to_string(&batch).expect("serialize batch");
        assert!(json.contains("\"kind\":\"ecg\""), "{json}");
        assert!(
            !json.contains("\"ecg\":"),
            "SSE batch must not embed the full snapshot ECG array: {json}"
        );
    }

    #[test]
    fn limited_delta_batches_do_not_instruct_client_to_skip_undelivered_rows() {
        let dir = TestDir::new("delta_limit");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![
                hr_notification(60),
                hr_notification(61),
                hr_notification(62),
                hr_notification(63),
            ],
        );
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let batch = runtime.delta_batch_after(0, 2);
        assert_eq!(batch.items.len(), 2);
        assert_eq!(batch.to_seq, batch.items.last().expect("last item").seq);
        assert!(
            batch.latest_seq > batch.to_seq,
            "fixture should leave undelivered rows for the next incremental batch"
        );
        assert!(
            !DASHBOARD_HTML.contains("batch.latest_seq) latestSeq"),
            "browser must not jump latestSeq to undelivered batch.latest_seq"
        );
        assert!(
            DASHBOARD_HTML.contains("function mergeEventRow"),
            "browser should timestamp-sort/deduplicate event history deltas before trimming"
        );
        assert!(
            !DASHBOARD_HTML.contains("case 'event': pushLimited(state.events"),
            "old history event deltas must not evict newer live events by arrival order"
        );
    }

    #[test]
    fn dashboard_prefers_one_shared_worker_sse_with_direct_fallback() {
        assert!(
            DASHBOARD_HTML.contains(
                "new SharedWorker('/live-worker.js?v=20260806', 'training-sensors-live-v4')"
            ),
            "dashboard tabs should attach to the same named shared worker"
        );
        assert!(
            DASHBOARD_HTML.contains("function startDirectSse"),
            "unsupported browsers should retain a direct SSE fallback"
        );
        assert_eq!(
            LIVE_WORKER_JS.matches("new EventSource(").count(),
            1,
            "the shared worker must own exactly one EventSource"
        );
        assert!(
            LIVE_WORKER_JS.contains("self.onconnect")
                && LIVE_WORKER_JS.contains("ports.add(port)")
                && LIVE_WORKER_JS.contains("broadcast(batch);")
                && LIVE_WORKER_JS.contains("sendCatchup(port, since)")
                && LIVE_WORKER_JS.contains("resetForStream(details.streamId")
                && LIVE_WORKER_JS.contains("recoverSameStream(port"),
            "the worker should fan one SSE batch out to every attached tab"
        );
        assert!(
            DASHBOARD_HTML.contains("observeAuthoritativeProgress(next)")
                && DASHBOARD_HTML.contains("type:'recover'")
                && LIVE_WORKER_JS.contains("reason:'same_generation_stall'"),
            "an independently observed same-generation cursor stall should resnapshot and reopen the shared upstream"
        );
        assert!(
            LIVE_WORKER_JS.contains("message.type === 'disconnect'")
                && LIVE_WORKER_JS.contains("stopEventSourceIfUnused()"),
            "the last departing tab should release the shared SSE connection"
        );
        assert!(
            LIVE_WORKER_JS.contains("dropping malformed live SSE batch")
                && LIVE_WORKER_JS.contains("if (batch) {")
                && LIVE_WORKER_JS.contains("broadcast(batch);"),
            "malformed SSE JSON must be dropped instead of poisoning catchup"
        );
    }

    #[test]
    fn events_do_not_mask_stale_sensor_data_freshness() {
        let dir = TestDir::new("event_not_sensor_fresh");
        create_closed_h10_wal(&dir.path, 0, vec![]);
        let runtime = LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
            .expect("runtime");
        let snapshot = runtime.snapshot();
        assert!(
            !snapshot.events.is_empty(),
            "fixture should contain WAL lifecycle events"
        );
        assert_eq!(snapshot.status.last_sensor_record_unix_ns, None);
    }

    #[test]
    fn incremental_wal_read_starts_after_processed_offset() {
        let dir = TestDir::new("incremental_read");
        let path = create_closed_h10_wal(
            &dir.path,
            0,
            vec![
                hr_notification(55),
                hr_notification(56),
                hr_notification(57),
            ],
        );
        let empty_offsets = HashMap::new();
        let first = read_wal_records_incremental(&path, WalReadMode::Strict, &empty_offsets)
            .expect("first read");
        assert!(first.records.len() >= 3);
        let mut offsets = HashMap::new();
        offsets.insert(first.header.segment_id, first.valid_end_offset);
        let second = read_wal_records_incremental(&path, WalReadMode::Strict, &offsets)
            .expect("second read");
        assert!(second.records.is_empty());
        assert_eq!(second.valid_end_offset, first.valid_end_offset);
    }

    #[tokio::test]
    async fn sse_endpoint_emits_incremental_batch_event_not_snapshot() {
        let dir = TestDir::new("sse_incremental");
        create_closed_h10_wal(
            &dir.path,
            0,
            vec![ecg_notification(), acc_notification(), hr_notification(66)],
        );
        let runtime = Arc::new(Mutex::new(
            LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10_000_000_000)
                .expect("runtime"),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let _ = handle_connection(stream, server_runtime).await;
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client
            .write_all(b"GET /events?since=0 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        let mut buf = [0_u8; 2048];
        for _ in 0..20 {
            let n = tokio::time::timeout(Duration::from_millis(200), client.read(&mut buf))
                .await
                .expect("read timeout")
                .expect("read response");
            if n == 0 {
                break;
            }
            response.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&response);
            if text.contains("event: batch") && text.contains("\"kind\":\"ecg\"") {
                break;
            }
        }
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        assert!(text.contains("event: batch"), "{text}");
        assert!(text.contains("\"kind\":\"ecg\""), "{text}");
        assert!(
            !text.contains("\"ecg\":"),
            "SSE must not stream full dashboard snapshots: {text}"
        );
        drop(client);
        server.abort();
    }

    #[tokio::test]
    async fn live_worker_endpoint_serves_javascript() {
        let dir = TestDir::new("live_worker_endpoint");
        let runtime = Arc::new(Mutex::new(
            LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 1).expect("runtime"),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_connection(stream, runtime)
                .await
                .expect("serve live worker");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client
            .write_all(b"GET /live-worker.js HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.await.expect("server join");

        let response = String::from_utf8(response).expect("UTF-8 response");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(
            response.contains("Content-Type: text/javascript; charset=utf-8"),
            "{response}"
        );
        assert!(response.contains("self.onconnect"), "{response}");
        assert!(response.contains("new EventSource"), "{response}");
    }

    #[test]
    fn marker_post_writes_valid_manual_events_wal_record() {
        let dir = TestDir::new("marker");
        let mut runtime =
            LiveRuntime::new_at(test_config(&dir.path, true), START_NS + 1).expect("runtime");
        let response = runtime
            .post_marker(MarkerPostRequest {
                label: Some(" leave_building_start ".to_string()),
                note: Some(" note ".to_string()),
                client_id: Some("browser".to_string()),
                client_event_id: None,
            })
            .expect("marker");
        assert_eq!(response.label, "leave_building_start");
        let open_paths = paths_ending_with(
            &dir.path
                .join("wal/participant_id=local/source=manual_events"),
            ".open",
        );
        assert_eq!(open_paths.len(), 1);
        let read =
            read_wal_records(&open_paths[0], WalReadMode::TailTolerant).expect("read manual");
        let marker = read
            .records
            .iter()
            .find(|frame| frame.record.kind == WalRecordKind::ManualMarker)
            .expect("manual marker");
        assert_eq!(marker.record.source, MANUAL_EVENTS_SOURCE);
        assert_eq!(marker.record.writer_id, LIVE_WRITER_ID);
        let WalBody::ManualMarker(body) = &marker.record.body else {
            panic!("expected manual marker");
        };
        assert_eq!(body.label, "leave_building_start");
        assert_eq!(body.note.as_deref(), Some("note"));
        assert_eq!(body.client_id.as_deref(), Some("browser"));
        runtime.shutdown().expect("shutdown");
    }

    #[tokio::test]
    async fn http_marker_post_endpoint_writes_manual_events_wal() {
        let dir = TestDir::new("http_marker");
        let runtime = Arc::new(Mutex::new(
            LiveRuntime::new_at(test_config(&dir.path, true), START_NS + 1).expect("runtime"),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let server_runtime = runtime.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_connection(stream, server_runtime)
                .await
                .expect("handle connection");
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let body = br#"{"label":"http_marker","client_id":"test_client"}"#;
        let request = format!(
            "POST /api/marker HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).expect("utf8 body")
        );
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.await.expect("server join");
        let response = String::from_utf8(response).expect("utf8 response");
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("http_marker"), "{response}");

        let open_paths = paths_ending_with(
            &dir.path
                .join("wal/participant_id=local/source=manual_events"),
            ".open",
        );
        assert_eq!(open_paths.len(), 1);
        let read =
            read_wal_records(&open_paths[0], WalReadMode::TailTolerant).expect("read manual");
        assert!(read.records.iter().any(|frame| matches!(
            &frame.record.body,
            WalBody::ManualMarker(body) if body.label == "http_marker"
        )));
        runtime
            .lock()
            .expect("runtime lock")
            .shutdown()
            .expect("shutdown");
    }

    #[test]
    fn manual_events_lock_contention_disables_markers_and_defers_recovery() {
        let dir = TestDir::new("manual_lock");
        let stale_open = create_stale_manual_open(&dir.path);
        let held =
            SourceWriterLock::try_acquire(&dir.path, WAL_PARTICIPANT_ID, MANUAL_EVENTS_SOURCE)
                .expect("held result")
                .expect("held lock");
        let mut runtime = LiveRuntime::new_at(test_config(&dir.path, true), START_NS + 2)
            .expect("runtime read-only");
        assert!(!runtime.status().marker_enabled);
        assert!(
            runtime
                .post_marker(MarkerPostRequest {
                    label: None,
                    note: None,
                    client_id: None,
                    client_event_id: None
                })
                .is_err()
        );
        assert!(
            stale_open.exists(),
            "contended live service must not recover manual .open"
        );
        drop(held);

        let mut runtime = LiveRuntime::new_at(test_config(&dir.path, true), START_NS + 3)
            .expect("runtime writable");
        assert!(runtime.status().marker_enabled);
        assert!(
            !stale_open.exists(),
            "writable live service recovers stale manual .open"
        );
        let manual_source_dir = dir
            .path
            .join("wal/participant_id=local/source=manual_events");
        let open = paths_ending_with(&manual_source_dir, ".open");
        assert_eq!(open.len(), 1);
        let new_open =
            read_wal_records(&open[0], WalReadMode::TailTolerant).expect("read new manual open");
        assert!(
            new_open
                .records
                .iter()
                .any(|frame| frame.record.kind == WalRecordKind::RecoveryEvent),
            "new manual-events WAL should record stale-open recovery"
        );
        let closed = paths_ending_with(&manual_source_dir, ".closed");
        assert!(!closed.is_empty());
        runtime.shutdown().expect("shutdown");
    }

    #[test]
    fn sensor_open_wal_is_tail_tolerantly_read_and_not_modified() {
        let dir = TestDir::new("sensor_open");
        let open_path = create_open_h10_wal(&dir.path, vec![hr_notification(64)]);
        OpenOptions::new()
            .append(true)
            .open(&open_path)
            .expect("append partial")
            .write_all(&[1, 2, 3])
            .expect("partial bytes");
        let len_before = fs::metadata(&open_path).expect("stat before").len();
        let runtime =
            LiveRuntime::new_at(test_config(&dir.path, false), START_NS + 10).expect("runtime");
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.latest_hr.expect("hr").bpm, 64);
        assert!(
            open_path.exists(),
            "live service must not rename sensor .open"
        );
        assert_eq!(
            fs::metadata(&open_path).expect("stat after").len(),
            len_before
        );
        assert!(
            paths_ending_with(
                &dir.path.join("wal/participant_id=local/source=chest_h10"),
                ".closed"
            )
            .is_empty(),
            "live service must not close sensor WAL"
        );
    }

    #[test]
    fn filename_timestamp_parser_round_trips_contract_format() {
        let text = "20260202T024000.123456789Z";
        let ns = parse_wal_filename_timestamp(text).expect("parse");
        let secs = ns.div_euclid(1_000_000_000);
        let nanos = ns.rem_euclid(1_000_000_000) as u32;
        let dt = DateTime::<Utc>::from_timestamp(secs, nanos).expect("dt");
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 2);
        assert_eq!(dt.day(), 2);
        assert_eq!(dt.hour(), 2);
        assert_eq!(dt.minute(), 40);
        assert_eq!(dt.second(), 0);
        assert_eq!(nanos, 123_456_789);
        assert!(parse_wal_filename_timestamp("bad").is_err());
    }
}
