use crate::config::{BATTERY_LEVEL_UUID, HR_MEASUREMENT_UUID, PMD_CONTROL_UUID, PMD_DATA_UUID};
use crate::hr::{HrData, handle_hr};
use crate::pmd::{PmdParser, measurement_name};
use crate::records::PmdFrameTiming;
use crate::sink::RecordSink;
use crate::wal::{
    WalBody, WalFrameRecord, WalReadMode, WalReadResult, WalRecord, read_wal_records,
};
use anyhow::{Context, Result, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum ReplayMode {
    Batch,
    Step,
    Realtime,
}

impl ReplayMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Step => "step",
            Self::Realtime => "realtime",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum ReplayOutput {
    Jsonl,
    Summary,
}

impl ReplayOutput {
    fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Summary => "summary",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplayConfig {
    pub from_wal: PathBuf,
    pub mode: ReplayMode,
    pub speed_factor: f64,
    pub allow_open: bool,
    pub output: ReplayOutput,
}

impl ReplayConfig {
    pub fn new(from_wal: PathBuf) -> Self {
        Self {
            from_wal,
            mode: ReplayMode::Batch,
            speed_factor: 1.0,
            allow_open: false,
            output: ReplayOutput::Jsonl,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplaySummary {
    pub input_path: String,
    pub mode: String,
    pub output: String,
    pub segments: usize,
    pub open_segments: usize,
    pub tail_tolerant_segments: usize,
    pub tail_issue_segments: usize,
    pub wal_records: usize,
    pub replay_events: usize,
    pub first_collector_unix_ns: Option<i64>,
    pub last_collector_unix_ns: Option<i64>,
    pub duplicate_record_ids: usize,
    pub out_of_order_records: usize,
    pub slept_ns: u64,
}

pub trait ReplaySleeper {
    fn sleep(&mut self, duration: Duration) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct StdReplaySleeper;

impl ReplaySleeper for StdReplaySleeper {
    fn sleep(&mut self, duration: Duration) -> Result<()> {
        std::thread::sleep(duration);
        Ok(())
    }
}

pub fn run(config: ReplayConfig) -> Result<ReplaySummary> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    let mut sleeper = StdReplaySleeper;
    replay_to_writer_with_sleeper(&config, &mut writer, &mut sleeper)
}

pub fn replay_to_writer_with_sleeper<W: Write, S: ReplaySleeper>(
    config: &ReplayConfig,
    writer: &mut W,
    sleeper: &mut S,
) -> Result<ReplaySummary> {
    validate_config(config)?;
    let segments = load_replay_segments(&config.from_wal, config.allow_open)?;
    if segments.is_empty() {
        bail!(
            "no WAL segments found under {}; pass a .closed WAL file or a directory containing .closed segments{}",
            config.from_wal.display(),
            if config.allow_open {
                ""
            } else {
                " (use --allow-open to include .open segments)"
            }
        );
    }

    let mut summary = ReplaySummary {
        input_path: config.from_wal.display().to_string(),
        mode: config.mode.as_str().to_string(),
        output: config.output.as_str().to_string(),
        segments: segments.len(),
        open_segments: segments.iter().filter(|segment| segment.is_open).count(),
        tail_tolerant_segments: segments
            .iter()
            .filter(|segment| segment.read_mode == WalReadMode::TailTolerant)
            .count(),
        tail_issue_segments: segments
            .iter()
            .filter(|segment| segment.read.tail_issue.is_some())
            .count(),
        ..ReplaySummary::default()
    };

    let mut processor = ReplayProcessor::default();
    let mut seen_record_ids = BTreeSet::new();
    let mut previous_replay_ts = None;
    let mut previous_append_ts = None;

    for segment in &segments {
        for frame in &segment.read.records {
            update_summary_for_frame(
                &mut summary,
                &mut seen_record_ids,
                previous_append_ts,
                frame,
            );
            if config.mode == ReplayMode::Realtime {
                if let Some(delay) = replay_delay(
                    previous_replay_ts,
                    frame.record.collector_unix_ns,
                    config.speed_factor,
                ) {
                    sleeper.sleep(delay)?;
                    summary.slept_ns = summary
                        .slept_ns
                        .saturating_add(u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX));
                }
                previous_replay_ts = Some(frame.record.collector_unix_ns);
            }
            previous_append_ts = Some(frame.record.collector_unix_ns);

            let events =
                processor.process_frame(&segment.path, segment.read.header.segment_id, frame)?;
            summary.replay_events = summary.replay_events.saturating_add(events.len());
            if config.output == ReplayOutput::Jsonl {
                for event in &events {
                    serde_json::to_writer(&mut *writer, event)
                        .context("serialize replay JSONL event")?;
                    writer
                        .write_all(b"\n")
                        .context("write replay JSONL newline")?;
                }
                if matches!(config.mode, ReplayMode::Step | ReplayMode::Realtime) {
                    writer.flush().context("flush replay output")?;
                }
            }
        }
    }

    if config.output == ReplayOutput::Summary {
        serde_json::to_writer(&mut *writer, &summary).context("serialize replay summary")?;
        writer
            .write_all(b"\n")
            .context("write replay summary newline")?;
    }
    writer.flush().context("flush replay output")?;
    Ok(summary)
}

fn validate_config(config: &ReplayConfig) -> Result<()> {
    if config.mode == ReplayMode::Realtime
        && (!config.speed_factor.is_finite() || config.speed_factor <= 0.0)
    {
        bail!("--speed-factor must be finite and positive for realtime replay");
    }
    Ok(())
}

fn replay_delay(previous_ts: Option<i64>, current_ts: i64, speed_factor: f64) -> Option<Duration> {
    let previous_ts = previous_ts?;
    if current_ts <= previous_ts {
        return None;
    }
    let delta_ns = current_ts.saturating_sub(previous_ts) as f64;
    let scaled_ns = (delta_ns / speed_factor).round();
    if !scaled_ns.is_finite() || scaled_ns <= 0.0 {
        return None;
    }
    Some(Duration::from_nanos(scaled_ns.min(u64::MAX as f64) as u64))
}

fn update_summary_for_frame(
    summary: &mut ReplaySummary,
    seen_record_ids: &mut BTreeSet<Uuid>,
    previous_append_ts: Option<i64>,
    frame: &WalFrameRecord,
) {
    summary.wal_records = summary.wal_records.saturating_add(1);
    let ts = frame.record.collector_unix_ns;
    summary.first_collector_unix_ns = Some(
        summary
            .first_collector_unix_ns
            .map(|current| current.min(ts))
            .unwrap_or(ts),
    );
    summary.last_collector_unix_ns = Some(
        summary
            .last_collector_unix_ns
            .map(|current| current.max(ts))
            .unwrap_or(ts),
    );
    if !seen_record_ids.insert(frame.record.record_id) {
        summary.duplicate_record_ids = summary.duplicate_record_ids.saturating_add(1);
    }
    if previous_append_ts
        .map(|previous| ts < previous)
        .unwrap_or(false)
    {
        summary.out_of_order_records = summary.out_of_order_records.saturating_add(1);
    }
}

#[derive(Debug)]
struct ReplaySegment {
    path: PathBuf,
    is_open: bool,
    read_mode: WalReadMode,
    read: WalReadResult,
}

fn load_replay_segments(path: &Path, allow_open: bool) -> Result<Vec<ReplaySegment>> {
    if path.is_file() {
        return Ok(vec![load_replay_file(path, true, allow_open)?]);
    }
    if !path.exists() {
        bail!("WAL path does not exist: {}", path.display());
    }
    if !path.is_dir() {
        bail!(
            "WAL path is neither a file nor directory: {}",
            path.display()
        );
    }

    let mut paths = Vec::new();
    collect_replay_paths(path, allow_open, &mut paths)?;
    let mut segments = paths
        .iter()
        .map(|path| load_replay_file(path, false, allow_open))
        .collect::<Result<Vec<_>>>()?;
    segments.sort_by(|left, right| {
        left.read
            .header
            .segment_start_unix_ns
            .cmp(&right.read.header.segment_start_unix_ns)
            .then(left.path.cmp(&right.path))
    });
    Ok(segments)
}

fn collect_replay_paths(dir: &Path, allow_open: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("scan replay WAL dir {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_replay_paths(&path, allow_open, out)?;
        } else if replay_file_mode(&path, false, allow_open)?.is_some() {
            out.push(path);
        }
    }
    out.sort();
    Ok(())
}

fn load_replay_file(path: &Path, direct: bool, allow_open: bool) -> Result<ReplaySegment> {
    let Some(read_mode) = replay_file_mode(path, direct, allow_open)? else {
        bail!("not a replayable WAL segment: {}", path.display());
    };
    let read = read_wal_records(path, read_mode)
        .with_context(|| format!("read replay WAL segment {}", path.display()))?;
    Ok(ReplaySegment {
        path: path.to_path_buf(),
        is_open: is_open_wal_path(path),
        read_mode,
        read,
    })
}

fn replay_file_mode(path: &Path, direct: bool, allow_open: bool) -> Result<Option<WalReadMode>> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(None);
    };
    if name.ends_with(".open.tmp") || name.ends_with(".archived.json") || name.ends_with(".tmp") {
        return Ok(None);
    }
    if name.ends_with(".closed") {
        return Ok(Some(WalReadMode::Strict));
    }
    if name.ends_with(".open") {
        if allow_open {
            return Ok(Some(WalReadMode::TailTolerant));
        }
        if direct {
            bail!(
                "refusing to replay .open WAL {} without --allow-open",
                path.display()
            );
        }
        return Ok(None);
    }
    if direct {
        return Ok(Some(WalReadMode::Strict));
    }
    Ok(None)
}

fn is_open_wal_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.ends_with(".open"))
        .unwrap_or(false)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayTrace {
    pub segment_path: String,
    pub segment_id: Uuid,
    pub frame_offset: u64,
    pub record_id: Uuid,
    pub record_kind: String,
    pub collector_unix_ns: i64,
    pub session_id: Uuid,
    pub connection_id: Option<Uuid>,
    pub physical_device_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplayJsonEvent {
    RawGatt {
        trace: ReplayTrace,
        characteristic_uuid: String,
        direction: String,
        operation: String,
        payload_b64: String,
        payload_len: u32,
        detail: String,
    },
    PmdTiming {
        trace: ReplayTrace,
        measurement: String,
        measurement_type: u8,
        frame_type: u8,
        compressed: bool,
        device_frame_ts_ns: u64,
        payload_len: u32,
        sample_count: u32,
        collector_delta_ms: Option<f64>,
        device_delta_ms: Option<f64>,
        expected_frame_delta_ms: f64,
        gap_ms: Option<f64>,
    },
    Ecg {
        trace: ReplayTrace,
        device_ts_ns: u64,
        sample_index_in_frame: u32,
        microvolts: i32,
    },
    Acc {
        trace: ReplayTrace,
        device_ts_ns: u64,
        sample_index_in_frame: u32,
        x_mg: i32,
        y_mg: i32,
        z_mg: i32,
    },
    Hr {
        trace: ReplayTrace,
        hr_bpm: u16,
        sensor_contact_supported: bool,
        sensor_contact_detected: bool,
        energy_expended: Option<u16>,
        rr_ms: Vec<f64>,
        raw_payload_b64: String,
    },
    Rr {
        trace: ReplayTrace,
        rr_index_in_frame: u32,
        rr_ms: f64,
        hr_frame_collector_unix_ns: i64,
    },
    Battery {
        trace: ReplayTrace,
        battery_percent: u8,
        reason: String,
    },
    Health {
        trace: ReplayTrace,
        rssi_dbm: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    },
    Event {
        trace: ReplayTrace,
        event: String,
        detail: String,
    },
}

#[derive(Default)]
struct ReplayProcessor {
    pmd: PmdParser,
}

impl ReplayProcessor {
    fn process_frame(
        &mut self,
        segment_path: &Path,
        segment_id: Uuid,
        frame: &WalFrameRecord,
    ) -> Result<Vec<ReplayJsonEvent>> {
        let mut sink = ReplaySink::new(segment_path, segment_id, frame);
        route_replay_record(&frame.record, &mut self.pmd, &mut sink)
            .with_context(|| format!("process replay WAL record {}", frame.record.record_id))?;
        Ok(sink.events)
    }
}

struct ReplaySink {
    base_trace: ReplayTrace,
    events: Vec<ReplayJsonEvent>,
}

impl ReplaySink {
    fn new(segment_path: &Path, segment_id: Uuid, frame: &WalFrameRecord) -> Self {
        let record = &frame.record;
        Self {
            base_trace: ReplayTrace {
                segment_path: segment_path.display().to_string(),
                segment_id,
                frame_offset: frame.frame_offset,
                record_id: record.record_id,
                record_kind: record.kind.as_str().to_string(),
                collector_unix_ns: record.collector_unix_ns,
                session_id: record.session_id,
                connection_id: record.connection_id,
                physical_device_id: record.physical_device_id.clone(),
            },
            events: Vec::new(),
        }
    }

    fn trace_with_collector_ns(&self, collector_ns: u128) -> ReplayTrace {
        let mut trace = self.base_trace.clone();
        trace.collector_unix_ns = u128_to_i64_saturating(collector_ns);
        trace
    }
}

impl RecordSink for ReplaySink {
    fn session_dir(&self) -> &Path {
        Path::new("")
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        self.events.push(ReplayJsonEvent::Event {
            trace: self.base_trace.clone(),
            event: event.to_string(),
            detail: detail.to_string(),
        });
        Ok(())
    }

    fn raw_gatt(
        &mut self,
        collector_ns: u128,
        characteristic_uuid: &str,
        direction: &str,
        operation: &str,
        payload: &[u8],
        detail: &str,
    ) -> Result<()> {
        self.events.push(ReplayJsonEvent::RawGatt {
            trace: self.trace_with_collector_ns(collector_ns),
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            direction: direction.to_string(),
            operation: operation.to_string(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            detail: detail.to_string(),
        });
        Ok(())
    }

    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        self.events.push(ReplayJsonEvent::PmdTiming {
            trace: self.trace_with_collector_ns(timing.collector_ns),
            measurement: measurement_name(timing.measurement_type).to_string(),
            measurement_type: timing.measurement_type,
            frame_type: timing.frame_type,
            compressed: timing.compressed,
            device_frame_ts_ns: timing.device_frame_ts,
            payload_len: u32::try_from(timing.payload_len).unwrap_or(u32::MAX),
            sample_count: u32::try_from(timing.sample_count).unwrap_or(u32::MAX),
            collector_delta_ms: timing.collector_delta_ms,
            device_delta_ms: timing.device_delta_ms,
            expected_frame_delta_ms: timing.expected_frame_delta_ms,
            gap_ms: timing.gap_ms,
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
        self.events.push(ReplayJsonEvent::Ecg {
            trace: self.trace_with_collector_ns(collector_ns),
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
        self.events.push(ReplayJsonEvent::Acc {
            trace: self.trace_with_collector_ns(collector_ns),
            device_ts_ns: device_ts,
            sample_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
            x_mg: x,
            y_mg: y,
            z_mg: z,
        });
        Ok(())
    }

    fn hr(&mut self, collector_ns: u128, data: &HrData, raw: &[u8]) -> Result<()> {
        let trace = self.trace_with_collector_ns(collector_ns);
        self.events.push(ReplayJsonEvent::Hr {
            trace: trace.clone(),
            hr_bpm: data.hr_bpm,
            sensor_contact_supported: data.sensor_contact_supported,
            sensor_contact_detected: data.sensor_contact_detected,
            energy_expended: data.energy_expended,
            rr_ms: data.rr_ms.clone(),
            raw_payload_b64: base64::engine::general_purpose::STANDARD.encode(raw),
        });
        let hr_frame_collector_unix_ns = u128_to_i64_saturating(collector_ns);
        for (index, rr_ms) in data.rr_ms.iter().enumerate() {
            self.events.push(ReplayJsonEvent::Rr {
                trace: trace.clone(),
                rr_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
                rr_ms: *rr_ms,
                hr_frame_collector_unix_ns,
            });
        }
        Ok(())
    }

    fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()> {
        self.events.push(ReplayJsonEvent::Battery {
            trace: self.trace_with_collector_ns(collector_ns),
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
        self.events.push(ReplayJsonEvent::Health {
            trace: self.trace_with_collector_ns(collector_ns),
            rssi_dbm: rssi,
            pmd_frames,
            hr_frames,
        });
        Ok(())
    }
}

fn route_replay_record(
    record: &WalRecord,
    parser: &mut PmdParser,
    sink: &mut ReplaySink,
) -> Result<()> {
    if let WalBody::CloudIngestBatchV1(body) = &record.body {
        for child in &body.records {
            route_replay_record(child, parser, sink)?;
        }
        return Ok(());
    }
    match &record.body {
        WalBody::CloudIngestBatchV1(_) => unreachable!("handled above"),
        WalBody::GattNotification(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_notification")?;
            sink.raw_gatt(
                collector_ns_u128(record)?,
                &body.characteristic_uuid,
                "inbound",
                "notification",
                &payload,
                &body.description,
            )?;
            route_replay_gatt_payload(
                collector_ns_u128(record)?,
                &body.characteristic_uuid,
                &body.description,
                &payload,
                parser,
                sink,
            )?;
        }
        WalBody::GattReadResponse(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_read_response")?;
            sink.raw_gatt(
                collector_ns_u128(record)?,
                &body.characteristic_uuid,
                "inbound",
                "read_response",
                &payload,
                &body.reason,
            )?;
            if is_battery_characteristic(&body.characteristic_uuid, &body.reason)
                && let Some(level) = payload.first()
            {
                sink.battery(collector_ns_u128(record)?, *level, &body.reason)?;
            }
        }
        WalBody::GattWriteRequest(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_write_request")?;
            sink.raw_gatt(
                collector_ns_u128(record)?,
                &body.characteristic_uuid,
                "outbound",
                "write_request",
                &payload,
                &body.reason,
            )?;
        }
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
        WalBody::RssiSample(body) => {
            sink.health(collector_ns_u128(record)?, Some(body.rssi_dbm), 0, 0)?;
        }
        WalBody::RssiReadError(body) => sink.event(
            "rssi_read_error",
            &format!(
                "peripheral_id={} method={} elapsed_ms={} error={}",
                body.peripheral_id, body.method, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::CentralEvent(body) => sink.event(
            "central_event",
            &format!(
                "event_name={} peripheral_id={} detail={} rssi_dbm={:?}",
                body.event_name, body.peripheral_id, body.detail, body.rssi_dbm
            ),
        )?,
        WalBody::ManualMarker(body) => sink.event(
            "manual_marker",
            &format!(
                "event_id={} event_unix_ns={} label={} note={:?}",
                body.event_id, body.event_unix_ns, body.label, body.note
            ),
        )?,
        WalBody::SegmentStarted(body) => sink.event("segment_started", &body.rotation_reason)?,
        WalBody::SegmentClosed(body) => sink.event("segment_closed", &body.reason)?,
        WalBody::SessionStarted(body) => sink.event("session_started", &body.started_reason)?,
        WalBody::SessionEnded(body) => sink.event(
            "session_ended",
            &format!("{} {}", body.reason, body.message),
        )?,
        WalBody::RecoveryEvent(body) => sink.event(
            "recovery_event",
            &format!(
                "action={} path={} truncated_tail_bytes={} message={}",
                body.action, body.recovered_path, body.truncated_tail_bytes, body.message
            ),
        )?,
        WalBody::ScanAttempt(body) => sink.event(
            "scan_attempt",
            &format!(
                "attempt_id={} target={} timeout_ms={}",
                body.attempt_id, body.target, body.scan_timeout_ms
            ),
        )?,
        WalBody::ScanResult(body) => sink.event(
            "scan_result",
            &format!(
                "attempt_id={} peripheral_id={} matched={} rssi_dbm={:?}",
                body.attempt_id, body.peripheral_id, body.matched, body.rssi_dbm
            ),
        )?,
        WalBody::ScanError(body) => sink.event(
            "scan_error",
            &format!(
                "attempt_id={} stage={} error={}",
                body.attempt_id, body.stage, body.error
            ),
        )?,
        WalBody::ConnectAttempt(body) => sink.event(
            "connect_attempt",
            &format!(
                "attempt_id={} peripheral_id={}",
                body.attempt_id, body.peripheral_id
            ),
        )?,
        WalBody::ConnectResult(body) => sink.event(
            "connect_result",
            &format!(
                "attempt_id={} connected={} elapsed_ms={}",
                body.attempt_id, body.connected, body.elapsed_ms
            ),
        )?,
        WalBody::ConnectError(body) => sink.event(
            "connect_error",
            &format!(
                "attempt_id={} stage={} elapsed_ms={} error={}",
                body.attempt_id, body.stage, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::DisconnectAttempt(body) => sink.event("disconnect_attempt", &body.reason)?,
        WalBody::DisconnectResult(body) => sink.event(
            "disconnect_result",
            &format!("reason={} elapsed_ms={}", body.reason, body.elapsed_ms),
        )?,
        WalBody::DisconnectError(body) => sink.event(
            "disconnect_error",
            &format!(
                "reason={} elapsed_ms={} error={}",
                body.reason, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::ServiceDiscoverySummary(body) => sink.event(
            "service_discovery_summary",
            &format!(
                "peripheral_id={} service_count={}",
                body.peripheral_id, body.service_count
            ),
        )?,
        WalBody::SubscriptionAttempt(body) => sink.event(
            "subscription_attempt",
            &format!(
                "operation_id={} characteristic_uuid={} reason={}",
                body.operation_id, body.characteristic_uuid, body.reason
            ),
        )?,
        WalBody::SubscriptionResult(body) => sink.event(
            "subscription_result",
            &format!(
                "operation_id={} characteristic_uuid={} elapsed_ms={}",
                body.operation_id, body.characteristic_uuid, body.elapsed_ms
            ),
        )?,
        WalBody::SubscriptionError(body) => sink.event(
            "subscription_error",
            &format!(
                "operation_id={} characteristic_uuid={} elapsed_ms={} error={}",
                body.operation_id, body.characteristic_uuid, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::GattReadRequest(body) => sink.event(
            "gatt_read_request",
            &format!(
                "operation_id={} characteristic_uuid={} reason={}",
                body.operation_id, body.characteristic_uuid, body.reason
            ),
        )?,
        WalBody::GattReadError(body) => sink.event(
            "gatt_read_error",
            &format!(
                "operation_id={} characteristic_uuid={} reason={} elapsed_ms={} error={}",
                body.operation_id,
                body.characteristic_uuid,
                body.reason,
                body.elapsed_ms,
                body.error
            ),
        )?,
        WalBody::GattWriteResult(body) => sink.event(
            "gatt_write_result",
            &format!(
                "operation_id={} characteristic_uuid={} elapsed_ms={}",
                body.operation_id, body.characteristic_uuid, body.elapsed_ms
            ),
        )?,
        WalBody::GattWriteError(body) => sink.event(
            "gatt_write_error",
            &format!(
                "operation_id={} characteristic_uuid={} elapsed_ms={} error={}",
                body.operation_id, body.characteristic_uuid, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::PsftpSetLocalTimeAttempt(body) => sink.event(
            "psftp_set_local_time_attempt",
            &format!(
                "operation_id={} local_time={} tz_offset_minutes={}",
                body.operation_id, body.local_time_iso8601, body.tz_offset_minutes
            ),
        )?,
        WalBody::PsftpSetLocalTimeResult(body) => sink.event(
            if body.success {
                "psftp_set_local_time_success"
            } else {
                "psftp_set_local_time_error"
            },
            &format!(
                "operation_id={} status_code={:?} elapsed_ms={} error={:?}",
                body.operation_id, body.status_code, body.elapsed_ms, body.error
            ),
        )?,
        WalBody::LocationFix(body) => sink.event(
            "location_fix",
            &format!(
                "provider={} sampling_mode={} lat={} lon={} altitude_m={:?} \
                 horizontal_accuracy_m={:?} speed_mps={:?} speed_accuracy_mps={:?} \
                 bearing_deg={:?} device_time_unix_ms={} is_mock={}",
                body.provider,
                body.sampling_mode,
                body.latitude_deg,
                body.longitude_deg,
                body.altitude_m,
                body.horizontal_accuracy_m,
                body.speed_mps,
                body.speed_accuracy_mps,
                body.bearing_deg,
                body.device_time_unix_ms,
                body.is_mock
            ),
        )?,
        WalBody::GnssStatus(body) => sink.event(
            "gnss_status",
            &format!(
                "satellites_used={} satellites_visible={} cn0_used_mean_db_hz={:?} \
                 cn0_used_max_db_hz={:?}",
                body.satellites_used,
                body.satellites_visible,
                body.cn0_used_mean_db_hz,
                body.cn0_used_max_db_hz
            ),
        )?,
        WalBody::PressureSample(body) => sink.event(
            "pressure_sample",
            &format!(
                "pressure_hpa={} sensor_accuracy={} sensor_timestamp_ns={} sampling_mode={}",
                body.pressure_hpa,
                body.sensor_accuracy,
                body.sensor_timestamp_ns,
                body.sampling_mode
            ),
        )?,
    }
    Ok(())
}

fn route_replay_gatt_payload(
    collector_ns: u128,
    characteristic_uuid: &str,
    description: &str,
    payload: &[u8],
    parser: &mut PmdParser,
    sink: &mut ReplaySink,
) -> Result<()> {
    let uuid = characteristic_uuid.to_ascii_lowercase();
    if uuid == PMD_DATA_UUID {
        parser.handle_pmd_data(collector_ns, payload, sink)?;
    } else if uuid == PMD_CONTROL_UUID {
        parser.handle_control(collector_ns, payload, sink)?;
    } else if uuid == HR_MEASUREMENT_UUID {
        handle_hr(collector_ns, payload, sink)?;
    } else if is_battery_characteristic(&uuid, description)
        && let Some(level) = payload.first()
    {
        sink.battery(collector_ns, *level, description)?;
    }
    Ok(())
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

fn collector_ns_u128(record: &WalRecord) -> Result<u128> {
    u128::try_from(record.collector_unix_ns)
        .with_context(|| format!("negative collector_unix_ns in record {}", record.record_id))
}

fn u128_to_i64_saturating(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ACC_MEASUREMENT_TYPE, ECG_MEASUREMENT_TYPE};
    use crate::wal::{
        GattNotificationBody, SegmentHeader, WalPathConfig, WalRecordContext, WalWriter,
        WalWriterOptions, encode_frame_payload, encode_record_payload,
    };

    const START_NS: i64 = 1_770_000_000_000_000_000;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "training_sensors_replay_{name}_{}_{}",
                std::process::id(),
                crate::util::unix_ns()
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Default)]
    struct NoopSleeper;

    impl ReplaySleeper for NoopSleeper {
        fn sleep(&mut self, _duration: Duration) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingSleeper {
        sleeps: Vec<Duration>,
    }

    impl ReplaySleeper for RecordingSleeper {
        fn sleep(&mut self, duration: Duration) -> Result<()> {
            self.sleeps.push(duration);
            Ok(())
        }
    }

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn wal_config(base: &Path) -> WalPathConfig {
        WalPathConfig::new(base, "local", "chest_h10").expect("wal config")
    }

    fn test_writer_options() -> WalWriterOptions {
        WalWriterOptions {
            fsync_interval: Duration::from_secs(60),
            max_create_attempts: 4,
        }
    }

    fn context_at(ns: i64, sequence: u128) -> WalRecordContext {
        WalRecordContext::new(
            "local",
            "chest_h10",
            "training_sensors collect",
            uuid(0x1000_0000),
            uuid(0x2000_0000),
        )
        .expect("context")
        .at_time(ns, u64::try_from(ns.saturating_sub(START_NS)).unwrap_or(0))
        .connection_id(Some(uuid(0x3000_0000 + sequence)))
        .physical_device_id(Some("h10_fixture"))
        .ble_peripheral_id(Some("72585434-07e3-d5e3-5ace-4e6c70118687"))
    }

    fn hr_record(sequence: u8, bpm: u8, ns: i64) -> WalRecord {
        context_at(ns, u128::from(sequence))
            .make_record(WalBody::GattNotification(
                GattNotificationBody::from_payload(
                    HR_MEASUREMENT_UUID,
                    "heart_rate_measurement",
                    &[0x16, bpm, 0x00, 0x04],
                )
                .expect("HR notification body"),
            ))
            .with_record_id(uuid(0x4000_0000 + u128::from(sequence)))
    }

    fn pmd_record(sequence: u8, payload: &[u8], ns: i64) -> WalRecord {
        context_at(ns, u128::from(sequence))
            .make_record(WalBody::GattNotification(
                GattNotificationBody::from_payload(PMD_DATA_UUID, "pmd_data", payload)
                    .expect("PMD notification body"),
            ))
            .with_record_id(uuid(0x5000_0000 + u128::from(sequence)))
    }

    fn record_with_body(sequence: u8, ns: i64, body: WalBody) -> WalRecord {
        context_at(ns, u128::from(sequence))
            .make_record(body)
            .with_record_id(uuid(0x6000_0000 + u128::from(sequence)))
    }

    fn write_closed_segment(base: &Path, start_ns: i64, records: Vec<WalRecord>) -> PathBuf {
        let mut writer =
            WalWriter::create(&wal_config(base), start_ns, test_writer_options()).expect("writer");
        for record in records {
            writer.append_record(&record).expect("append record");
        }
        let closed = writer
            .close(
                &context_at(start_ns + 9_000_000, 0),
                start_ns + 9_000_000,
                "test_close",
                0,
            )
            .expect("close segment");
        closed.closed_path
    }

    fn write_open_segment(base: &Path, start_ns: i64, records: Vec<WalRecord>) -> PathBuf {
        let mut writer =
            WalWriter::create(&wal_config(base), start_ns, test_writer_options()).expect("writer");
        for record in records {
            writer.append_record(&record).expect("append record");
        }
        let path = writer.open_path().to_path_buf();
        drop(writer);
        path
    }

    fn replay_events(base: &Path, allow_open: bool) -> (ReplaySummary, Vec<ReplayJsonEvent>) {
        let config = ReplayConfig {
            from_wal: base.to_path_buf(),
            mode: ReplayMode::Batch,
            speed_factor: 1.0,
            allow_open,
            output: ReplayOutput::Jsonl,
        };
        let mut out = Vec::new();
        let mut sleeper = NoopSleeper;
        let summary = replay_to_writer_with_sleeper(&config, &mut out, &mut sleeper)
            .expect("replay succeeds");
        let text = String::from_utf8(out).expect("utf8 output");
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<ReplayJsonEvent>(line).expect("parse event"))
            .collect::<Vec<_>>();
        (summary, events)
    }

    fn hr_bpms(events: &[ReplayJsonEvent]) -> Vec<u16> {
        events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::Hr { hr_bpm, .. } => Some(*hr_bpm),
                _ => None,
            })
            .collect()
    }

    fn pmd_frame(
        measurement_type: u8,
        frame_ts: u64,
        frame_type: u8,
        compressed: bool,
        content: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + content.len());
        out.push(measurement_type);
        out.extend_from_slice(&frame_ts.to_le_bytes());
        out.push(frame_type | if compressed { 0x80 } else { 0x00 });
        out.extend_from_slice(content);
        out
    }

    fn push_i24_le(out: &mut Vec<u8>, value: i32) {
        out.push(value as u8);
        out.push((value >> 8) as u8);
        out.push((value >> 16) as u8);
    }

    #[test]
    fn replays_multi_segment_directory_deterministically() {
        let dir = TestDir::new("multi_segment");
        write_closed_segment(
            dir.path(),
            START_NS,
            vec![
                hr_record(1, 60, START_NS + 1_000_000),
                hr_record(2, 61, START_NS + 2_000_000),
            ],
        );
        write_closed_segment(
            dir.path(),
            START_NS + 10_000_000_000,
            vec![hr_record(3, 62, START_NS + 10_001_000_000)],
        );

        let (summary, events) = replay_events(dir.path(), false);
        assert_eq!(summary.segments, 2);
        assert_eq!(summary.duplicate_record_ids, 0);
        assert_eq!(summary.out_of_order_records, 0);
        assert_eq!(hr_bpms(&events), vec![60, 61, 62]);
    }

    #[test]
    fn ten_one_record_segments_replay_without_gaps_or_duplicates() {
        let dir = TestDir::new("one_record_segments");
        for i in 0..10_u8 {
            write_closed_segment(
                dir.path(),
                START_NS + i64::from(i) * 1_000_000_000,
                vec![hr_record(
                    i,
                    70 + i,
                    START_NS + i64::from(i) * 1_000_000_000 + 1_000_000,
                )],
            );
        }

        let (summary, events) = replay_events(dir.path(), false);
        assert_eq!(summary.segments, 10);
        assert_eq!(summary.duplicate_record_ids, 0);
        assert_eq!(summary.out_of_order_records, 0);
        assert_eq!(hr_bpms(&events), (70_u16..80_u16).collect::<Vec<_>>());
    }

    #[test]
    fn parser_output_matches_synthetic_hr_ecg_and_acc_fixture() {
        let dir = TestDir::new("parser_output");
        let mut ecg_content = Vec::new();
        push_i24_le(&mut ecg_content, 1);
        push_i24_le(&mut ecg_content, -1);
        let ecg = pmd_frame(ECG_MEASUREMENT_TYPE, 1_000_000_000, 0, false, &ecg_content);
        let acc = pmd_frame(ACC_MEASUREMENT_TYPE, 2_000_000_000, 0, false, &[1, 2, 0xff]);
        write_closed_segment(
            dir.path(),
            START_NS,
            vec![
                hr_record(1, 60, START_NS + 1_000_000),
                pmd_record(2, &ecg, START_NS + 2_000_000),
                pmd_record(3, &acc, START_NS + 3_000_000),
            ],
        );

        let (_summary, events) = replay_events(dir.path(), false);
        assert_eq!(hr_bpms(&events), vec![60]);
        let rr = events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::Rr { rr_ms, .. } => Some(*rr_ms),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rr, vec![1000.0]);
        let ecg_values = events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::Ecg { microvolts, .. } => Some(*microvolts),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ecg_values, vec![1, -1]);
        let acc_values = events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::Acc {
                    x_mg, y_mg, z_mg, ..
                } => Some((*x_mg, *y_mg, *z_mg)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(acc_values, vec![(1, 2, -1)]);
        let timing_measurements = events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::PmdTiming { measurement, .. } => Some(measurement.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(timing_measurements, vec!["ECG", "ACC"]);
    }

    #[test]
    fn miscellaneous_wal_kinds_replay_as_events_raw_gatt_battery_and_health() {
        use crate::wal::{
            CentralEventBody, CollectorEventBody, ConnectAttemptBody, ConnectErrorBody,
            ConnectResultBody, DisconnectAttemptBody, DisconnectErrorBody, DisconnectResultBody,
            GattReadErrorBody, GattReadRequestBody, GattReadResponseBody, GattWriteErrorBody,
            GattWriteRequestBody, GattWriteResultBody, ManualMarkerBody,
            PsftpSetLocalTimeAttemptBody, PsftpSetLocalTimeResultBody, RecoveryEventBody,
            RssiReadErrorBody, RssiSampleBody, ScanAttemptBody, ScanErrorBody, ScanResultBody,
            SegmentStartedBody, ServiceDiscoveryDeviceInfo, ServiceDiscoverySummaryBody,
            SessionEndedBody, SessionStartedBody, SubscriptionAttemptBody, SubscriptionErrorBody,
            SubscriptionResultBody,
        };
        let dir = TestDir::new("misc_kinds");
        let operation_id = uuid(0x7000_0000);
        let attempt_id = uuid(0x7000_0001);
        let segment_id = uuid(0x7000_0002);
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("answer".to_string(), serde_json::json!(42));
        let records = vec![
            record_with_body(
                1,
                START_NS + 1,
                WalBody::GattReadResponse(
                    GattReadResponseBody::from_payload(
                        operation_id,
                        BATTERY_LEVEL_UUID,
                        "battery_level",
                        &[77],
                    )
                    .expect("read response body"),
                ),
            ),
            record_with_body(
                2,
                START_NS + 2,
                WalBody::GattWriteRequest(
                    GattWriteRequestBody::from_payload(
                        operation_id,
                        PMD_CONTROL_UUID,
                        "with_response",
                        "pmd_start_ecg",
                        &[2, 0, 0, 0],
                    )
                    .expect("write body"),
                ),
            ),
            record_with_body(
                3,
                START_NS + 3,
                WalBody::GattNotification(
                    GattNotificationBody::from_payload(
                        PMD_CONTROL_UUID,
                        "pmd_control",
                        &[0xf0, 0x02, ECG_MEASUREMENT_TYPE, 0x00, 0x00],
                    )
                    .expect("control notification"),
                ),
            ),
            record_with_body(
                4,
                START_NS + 4,
                WalBody::GattNotification(
                    GattNotificationBody::from_payload(BATTERY_LEVEL_UUID, "battery_level", &[76])
                        .expect("battery notification"),
                ),
            ),
            record_with_body(
                5,
                START_NS + 5,
                WalBody::CollectorEvent(CollectorEventBody {
                    event: "plain_event".to_string(),
                    severity: "info".to_string(),
                    detail: "plain".to_string(),
                    fields: std::collections::BTreeMap::new(),
                }),
            ),
            record_with_body(
                6,
                START_NS + 6,
                WalBody::CollectorEvent(CollectorEventBody {
                    event: "field_event".to_string(),
                    severity: "info".to_string(),
                    detail: "with_fields".to_string(),
                    fields,
                }),
            ),
            record_with_body(
                7,
                START_NS + 7,
                WalBody::RssiSample(RssiSampleBody {
                    peripheral_id: "p".to_string(),
                    rssi_dbm: -55,
                    method: "test".to_string(),
                    age_ms: 0,
                }),
            ),
            record_with_body(
                8,
                START_NS + 8,
                WalBody::RssiReadError(RssiReadErrorBody {
                    peripheral_id: "p".to_string(),
                    method: "test".to_string(),
                    error: "timeout".to_string(),
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                9,
                START_NS + 9,
                WalBody::CentralEvent(CentralEventBody {
                    event_name: "device_disconnected".to_string(),
                    peripheral_id: "p".to_string(),
                    detail: "detail".to_string(),
                    rssi_dbm: Some(-80),
                    service_uuids: vec![],
                    raw_debug: "debug".to_string(),
                }),
            ),
            record_with_body(
                10,
                START_NS + 10,
                WalBody::ManualMarker(ManualMarkerBody {
                    event_id: uuid(0x8000_0000),
                    event_unix_ns: START_NS + 10,
                    event_monotonic_ns: Some(10),
                    event_time_source: "dashboard_server_receive".to_string(),
                    created_by: "dashboard".to_string(),
                    label: "marker".to_string(),
                    note: Some("note".to_string()),
                    client_id: Some("client".to_string()),
                    client_event_id: None,
                    dedupe_key: "dedupe".to_string(),
                }),
            ),
            record_with_body(
                11,
                START_NS + 11,
                WalBody::SegmentStarted(SegmentStartedBody {
                    segment_id,
                    segment_start_unix_ns: START_NS,
                    segment_start_monotonic_ns: 0,
                    segment_path: "fixture.open".to_string(),
                    wal_container_version: "1.0".to_string(),
                    payload_codec: "canonical_json_v1".to_string(),
                    checksum_algorithm: "crc32c".to_string(),
                    rotation_reason: "test".to_string(),
                    previous_segment_id: None,
                }),
            ),
            record_with_body(
                12,
                START_NS + 12,
                WalBody::SessionStarted(SessionStartedBody {
                    software_name: "training_sensors".to_string(),
                    software_version: "0.1.0".to_string(),
                    git_commit: None,
                    hostname: "host".to_string(),
                    pid: 1,
                    target: "target".to_string(),
                    started_reason: "test".to_string(),
                    config_json: "{}".to_string(),
                }),
            ),
            record_with_body(
                13,
                START_NS + 13,
                WalBody::SessionEnded(SessionEndedBody {
                    reason: "done".to_string(),
                    message: "message".to_string(),
                }),
            ),
            record_with_body(
                14,
                START_NS + 14,
                WalBody::RecoveryEvent(RecoveryEventBody {
                    recovered_path: "old.open".to_string(),
                    action: "close_open".to_string(),
                    valid_records: 1,
                    truncated_tail_bytes: 0,
                    message: "recovered".to_string(),
                }),
            ),
            record_with_body(
                15,
                START_NS + 15,
                WalBody::ScanAttempt(ScanAttemptBody {
                    attempt_id,
                    target: "target".to_string(),
                    scan_timeout_ms: 100,
                    service_uuids: vec![],
                }),
            ),
            record_with_body(
                16,
                START_NS + 16,
                WalBody::ScanResult(ScanResultBody {
                    attempt_id,
                    peripheral_id: "p".to_string(),
                    local_name: Some("Polar".to_string()),
                    rssi_dbm: Some(-50),
                    manufacturer_data_b64: None,
                    service_uuids: vec![],
                    matched: true,
                }),
            ),
            record_with_body(
                17,
                START_NS + 17,
                WalBody::ScanError(ScanErrorBody {
                    attempt_id,
                    stage: "scan".to_string(),
                    error: "boom".to_string(),
                }),
            ),
            record_with_body(
                18,
                START_NS + 18,
                WalBody::ConnectAttempt(ConnectAttemptBody {
                    attempt_id,
                    peripheral_id: "p".to_string(),
                    local_name: Some("Polar".to_string()),
                    rssi_dbm: Some(-50),
                }),
            ),
            record_with_body(
                19,
                START_NS + 19,
                WalBody::ConnectResult(ConnectResultBody {
                    attempt_id,
                    connected: true,
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                20,
                START_NS + 20,
                WalBody::ConnectError(ConnectErrorBody {
                    attempt_id,
                    stage: "connect".to_string(),
                    error: "failed".to_string(),
                    elapsed_ms: 2,
                }),
            ),
            record_with_body(
                21,
                START_NS + 21,
                WalBody::DisconnectAttempt(DisconnectAttemptBody {
                    reason: "test".to_string(),
                    peripheral_id: "p".to_string(),
                }),
            ),
            record_with_body(
                22,
                START_NS + 22,
                WalBody::DisconnectResult(DisconnectResultBody {
                    reason: "test".to_string(),
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                23,
                START_NS + 23,
                WalBody::DisconnectError(DisconnectErrorBody {
                    reason: "test".to_string(),
                    error: "timeout".to_string(),
                    elapsed_ms: 2,
                }),
            ),
            record_with_body(
                24,
                START_NS + 24,
                WalBody::ServiceDiscoverySummary(ServiceDiscoverySummaryBody {
                    peripheral_id: "p".to_string(),
                    service_count: 0,
                    services: vec![],
                    device_info: Some(ServiceDiscoveryDeviceInfo {
                        manufacturer: None,
                        model: Some("H10".to_string()),
                        serial: None,
                        hardware_revision: None,
                        firmware_revision: None,
                        software_revision: None,
                    }),
                }),
            ),
            record_with_body(
                25,
                START_NS + 25,
                WalBody::SubscriptionAttempt(SubscriptionAttemptBody {
                    operation_id,
                    characteristic_uuid: PMD_DATA_UUID.to_string(),
                    notify_kind: "notify".to_string(),
                    reason: "pmd_data".to_string(),
                }),
            ),
            record_with_body(
                26,
                START_NS + 26,
                WalBody::SubscriptionResult(SubscriptionResultBody {
                    operation_id,
                    characteristic_uuid: PMD_DATA_UUID.to_string(),
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                27,
                START_NS + 27,
                WalBody::SubscriptionError(SubscriptionErrorBody {
                    operation_id,
                    characteristic_uuid: PMD_DATA_UUID.to_string(),
                    error: "failed".to_string(),
                    elapsed_ms: 2,
                }),
            ),
            record_with_body(
                28,
                START_NS + 28,
                WalBody::GattReadRequest(GattReadRequestBody {
                    operation_id,
                    characteristic_uuid: BATTERY_LEVEL_UUID.to_string(),
                    reason: "battery_level".to_string(),
                }),
            ),
            record_with_body(
                29,
                START_NS + 29,
                WalBody::GattReadError(GattReadErrorBody {
                    operation_id,
                    characteristic_uuid: BATTERY_LEVEL_UUID.to_string(),
                    reason: "battery_level".to_string(),
                    error: "timeout".to_string(),
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                30,
                START_NS + 30,
                WalBody::GattWriteResult(GattWriteResultBody {
                    operation_id,
                    characteristic_uuid: PMD_CONTROL_UUID.to_string(),
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                31,
                START_NS + 31,
                WalBody::GattWriteError(GattWriteErrorBody {
                    operation_id,
                    characteristic_uuid: PMD_CONTROL_UUID.to_string(),
                    error: "failed".to_string(),
                    elapsed_ms: 2,
                }),
            ),
            record_with_body(
                32,
                START_NS + 32,
                WalBody::PsftpSetLocalTimeAttempt(PsftpSetLocalTimeAttemptBody {
                    operation_id,
                    local_time_iso8601: "2026-05-24T12:00:00+00:00".to_string(),
                    local_time_unix_ns: START_NS,
                    tz_offset_minutes: 0,
                    dst_offset_minutes: None,
                    request_payload_b64: "AA==".to_string(),
                    control_characteristic_uuid: "fb005c51-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                    mtu_characteristic_uuid: "fb005c52-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                }),
            ),
            record_with_body(
                33,
                START_NS + 33,
                WalBody::PsftpSetLocalTimeResult(PsftpSetLocalTimeResultBody {
                    operation_id,
                    success: true,
                    status_code: Some(0),
                    response_payload_b64: "AA==".to_string(),
                    error: None,
                    elapsed_ms: 1,
                }),
            ),
            record_with_body(
                34,
                START_NS + 34,
                WalBody::PsftpSetLocalTimeResult(PsftpSetLocalTimeResultBody {
                    operation_id,
                    success: false,
                    status_code: Some(1),
                    response_payload_b64: "AA==".to_string(),
                    error: Some("bad".to_string()),
                    elapsed_ms: 2,
                }),
            ),
        ];
        write_closed_segment(dir.path(), START_NS, records);

        let (_summary, events) = replay_events(dir.path(), false);
        let event_names = events
            .iter()
            .filter_map(|event| match event {
                ReplayJsonEvent::Event { event, .. } => Some(event.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(event_names.contains(&"field_event"));
        assert!(event_names.contains(&"pmd_control_response"));
        assert!(event_names.contains(&"psftp_set_local_time_success"));
        assert!(event_names.contains(&"psftp_set_local_time_error"));
        assert!(events.iter().any(|event| matches!(event, ReplayJsonEvent::RawGatt { operation, .. } if operation == "write_request")));
        assert!(events.iter().any(|event| matches!(
            event,
            ReplayJsonEvent::Battery {
                battery_percent: 77,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ReplayJsonEvent::Battery {
                battery_percent: 76,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ReplayJsonEvent::Health {
                rssi_dbm: Some(-55),
                ..
            }
        )));
        assert!(decode_payload_b64("AQ==", 2, "bad_len").is_err());
    }

    #[test]
    fn step_summary_duplicate_and_error_paths_are_deterministic() {
        let dir = TestDir::new("step_and_errors");
        let path = dir.path().join("duplicates.wal");
        let first = hr_record(1, 60, START_NS + 2_000_000_000);
        let second = hr_record(2, 61, START_NS + 1_000_000_000).with_record_id(first.record_id);
        let mut bytes = SegmentHeader::new(uuid(0x9000), START_NS).encode().to_vec();
        for record in [&first, &second] {
            bytes.extend_from_slice(
                &encode_frame_payload(&encode_record_payload(record).expect("payload"))
                    .expect("frame"),
            );
        }
        fs::write(&path, bytes).expect("write fixture");

        let mut config = ReplayConfig::new(path);
        config.mode = ReplayMode::Step;
        let mut out = Vec::new();
        let mut sleeper = NoopSleeper;
        let summary =
            replay_to_writer_with_sleeper(&config, &mut out, &mut sleeper).expect("step replay");
        assert_eq!(summary.mode, "step");
        assert_eq!(summary.duplicate_record_ids, 1);
        assert_eq!(summary.out_of_order_records, 1);
        assert_eq!(
            hr_bpms(
                &String::from_utf8(out)
                    .expect("utf8")
                    .lines()
                    .map(|line| serde_json::from_str::<ReplayJsonEvent>(line).expect("event"))
                    .collect::<Vec<_>>()
            ),
            vec![60, 61]
        );

        let empty_dir = TestDir::new("empty");
        let mut empty_config = ReplayConfig::new(empty_dir.path().to_path_buf());
        let err = replay_to_writer_with_sleeper(&empty_config, &mut Vec::new(), &mut sleeper)
            .expect_err("empty dir fails loudly");
        assert!(format!("{err:#}").contains("no WAL segments"));
        empty_config.from_wal = empty_dir.path().join("missing");
        let err = replay_to_writer_with_sleeper(&empty_config, &mut Vec::new(), &mut sleeper)
            .expect_err("missing path fails loudly");
        assert!(format!("{err:#}").contains("does not exist"));
        empty_config.mode = ReplayMode::Realtime;
        empty_config.speed_factor = 0.0;
        let err = replay_to_writer_with_sleeper(&empty_config, &mut Vec::new(), &mut sleeper)
            .expect_err("invalid realtime speed fails");
        assert!(format!("{err:#}").contains("speed-factor"));
        assert!(replay_delay(Some(10), 10, 1.0).is_none());
        assert!(replay_delay(Some(10), 11, f64::INFINITY).is_none());
        sleeper.sleep(Duration::ZERO).expect("noop sleeper");
        let mut std_sleeper = StdReplaySleeper;
        std_sleeper.sleep(Duration::ZERO).expect("std sleeper zero");
    }

    #[test]
    fn open_file_requires_allow_open_and_tail_tolerates_valid_prefix() {
        let dir = TestDir::new("open_tail");
        let open_path = write_open_segment(
            dir.path(),
            START_NS,
            vec![hr_record(1, 88, START_NS + 1_000_000)],
        );
        fs::OpenOptions::new()
            .append(true)
            .open(&open_path)
            .expect("open append")
            .write_all(b"bad")
            .expect("write partial tail");

        let config = ReplayConfig {
            from_wal: open_path.clone(),
            mode: ReplayMode::Batch,
            speed_factor: 1.0,
            allow_open: false,
            output: ReplayOutput::Jsonl,
        };
        let mut out = Vec::new();
        let mut sleeper = NoopSleeper;
        let err = replay_to_writer_with_sleeper(&config, &mut out, &mut sleeper)
            .expect_err("open requires explicit allow");
        assert!(format!("{err:#}").contains("--allow-open"));

        let config = ReplayConfig {
            allow_open: true,
            ..config
        };
        let mut out = Vec::new();
        let summary = replay_to_writer_with_sleeper(&config, &mut out, &mut sleeper)
            .expect("open replay succeeds");
        assert_eq!(summary.open_segments, 1);
        assert_eq!(summary.tail_issue_segments, 1);
        let text = String::from_utf8(out).expect("utf8");
        let events = text
            .lines()
            .map(|line| serde_json::from_str::<ReplayJsonEvent>(line).expect("event"))
            .collect::<Vec<_>>();
        assert_eq!(hr_bpms(&events), vec![88]);
    }

    #[test]
    fn realtime_mode_uses_collector_timestamps_and_speed_factor() {
        let dir = TestDir::new("realtime");
        let path = dir.path().join("two_records.wal");
        let first = hr_record(1, 60, START_NS);
        let second = hr_record(2, 61, START_NS + 2_000_000_000);
        let mut bytes = SegmentHeader::new(uuid(0x9999), START_NS).encode().to_vec();
        for record in [&first, &second] {
            bytes.extend_from_slice(
                &encode_frame_payload(&encode_record_payload(record).expect("payload"))
                    .expect("frame"),
            );
        }
        fs::write(&path, bytes).expect("write fixture");

        let config = ReplayConfig {
            from_wal: path,
            mode: ReplayMode::Realtime,
            speed_factor: 4.0,
            allow_open: false,
            output: ReplayOutput::Summary,
        };
        let mut out = Vec::new();
        let mut sleeper = RecordingSleeper::default();
        let summary = replay_to_writer_with_sleeper(&config, &mut out, &mut sleeper)
            .expect("realtime replay");
        assert_eq!(sleeper.sleeps, vec![Duration::from_millis(500)]);
        assert_eq!(summary.slept_ns, 500_000_000);
        let decoded_summary: ReplaySummary =
            serde_json::from_slice(&out).expect("summary JSON output");
        assert_eq!(decoded_summary.wal_records, 2);
        assert_eq!(decoded_summary.mode, "realtime");
    }
}
