use super::db::{CloudDb, CommittedBatch, User};
use crate::config::{HR_MEASUREMENT_UUID, PMD_DATA_UUID};
use crate::hr::parse_hr;
use crate::sink::H10_WAL_SOURCE;
use crate::wal::{
    CloudIngestBatchV1Body, WalAppendPhase, WalBody, WalPathConfig, WalPhaseTiming, WalReadMode,
    WalRecord, WalRecordContext, WalRecordKind, WalRotationPolicy, WalSegmentManager,
    WalSegmentManagerOptions, WalWriterOptions, read_wal_records, recover_open_segment,
    recover_stale_open_segments,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::UNIX_EPOCH;
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const MAX_INGEST_RECORDS: usize = 256;
pub const MAX_INGEST_JSON_BYTES: usize = 512 * 1024;
pub const MAX_COLLECTOR_FUTURE_SKEW_MS: i64 = 5 * 60 * 1_000;
pub const MIN_COLLECTOR_UNIX_MS: i64 = 946_684_800_000; // 2000-01-01 UTC
pub(crate) const SLOW_INGEST_US: u64 = 250_000;
pub(crate) const NORMAL_INGEST_LOG_SAMPLE: u64 = 128;
const LATENCY_BUCKET_UPPER_US: [u64; 12] = [
    1_000,
    2_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    5_000_000,
    u64::MAX,
];

type BatchKey = (Uuid, Uuid, Uuid);
type ReceiptRecord = (i64, Uuid, String);

#[derive(Clone, Debug, Serialize)]
pub struct LatencyHistogram {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub checkpoint_overlap_count: u64,
    pub checkpoint_overlap_tracked: bool,
    pub bucket_upper_us: [u64; 12],
    pub buckets: [u64; 12],
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            count: 0,
            total_us: 0,
            max_us: 0,
            checkpoint_overlap_count: 0,
            checkpoint_overlap_tracked: false,
            bucket_upper_us: LATENCY_BUCKET_UPPER_US,
            buckets: [0; 12],
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, elapsed_ns: u128, checkpoint_overlap: Option<bool>) {
        let elapsed_us = u64::try_from(elapsed_ns / 1_000).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_us = self.total_us.saturating_add(elapsed_us);
        self.max_us = self.max_us.max(elapsed_us);
        if let Some(checkpoint_overlap) = checkpoint_overlap {
            self.checkpoint_overlap_tracked = true;
            self.checkpoint_overlap_count = self
                .checkpoint_overlap_count
                .saturating_add(u64::from(checkpoint_overlap));
        }
        if let Some(bucket) = LATENCY_BUCKET_UPPER_US
            .iter()
            .position(|upper| elapsed_us <= *upper)
        {
            self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        }
    }
}

pub(crate) trait TimingClock: Send + Sync + std::fmt::Debug {
    fn now_ns(&self) -> u128;
}

#[derive(Debug)]
struct SystemTimingClock {
    epoch: Instant,
}

impl SystemTimingClock {
    fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl TimingClock for SystemTimingClock {
    fn now_ns(&self) -> u128 {
        self.epoch.elapsed().as_nanos()
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct IngestTimingHealth {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub slow_events_logged: u64,
    pub normal_samples_logged: u64,
    pub http_auth: LatencyHistogram,
    pub http_decode_checksum: LatencyHistogram,
    pub blocking_queue: LatencyHistogram,
    pub gate_wait: LatencyHistogram,
    pub receipt_preflight: LatencyHistogram,
    pub request_and_envelope_build: LatencyHistogram,
    pub writer_lookup_or_create: LatencyHistogram,
    pub wal_append_total: LatencyHistogram,
    pub wal_rotation_check: LatencyHistogram,
    pub wal_rotation: LatencyHistogram,
    pub wal_append_write: LatencyHistogram,
    pub wal_envelope_encode: LatencyHistogram,
    pub wal_append_auto_sync: LatencyHistogram,
    pub wal_explicit_sync: LatencyHistogram,
    pub sqlite_writer_wait: LatencyHistogram,
    pub sqlite_transaction_begin: LatencyHistogram,
    pub sqlite_row_work: LatencyHistogram,
    pub sqlite_commit: LatencyHistogram,
    pub ack_queries: LatencyHistogram,
    pub ingest_total: LatencyHistogram,
    pub handler_total: LatencyHistogram,
}

#[derive(Clone, Copy)]
pub(crate) struct ActivitySnapshot {
    generation: u64,
    in_flight: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HttpPhaseTiming {
    pub(crate) elapsed_ns: u128,
    pub(crate) checkpoint_overlap: bool,
}

#[derive(Debug, Default)]
pub(crate) struct CheckpointActivity {
    generation: AtomicU64,
    in_flight: AtomicBool,
}

impl CheckpointActivity {
    pub(crate) fn begin(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.in_flight.store(true, Ordering::SeqCst);
    }

    pub(crate) fn finish(&self) {
        self.in_flight.store(false, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn snapshot(&self) -> ActivitySnapshot {
        ActivitySnapshot {
            generation: self.generation.load(Ordering::SeqCst),
            in_flight: self.in_flight.load(Ordering::SeqCst),
        }
    }

    pub(crate) fn guard(self: &Arc<Self>) -> CheckpointActivityGuard {
        self.begin();
        CheckpointActivityGuard {
            activity: self.clone(),
        }
    }
}

pub(crate) struct CheckpointActivityGuard {
    activity: Arc<CheckpointActivity>,
}

impl Drop for CheckpointActivityGuard {
    fn drop(&mut self) {
        self.activity.finish();
    }
}

#[derive(Clone, Copy, Default)]
struct PhaseTiming {
    elapsed_ns: u128,
    checkpoint_overlap: bool,
}

#[derive(Default)]
struct IngestAttemptPhases {
    gate_wait: Option<PhaseTiming>,
    receipt_preflight: Option<PhaseTiming>,
    request_and_envelope_build: Option<PhaseTiming>,
    writer_lookup_or_create: Option<PhaseTiming>,
    wal_append_total: Option<PhaseTiming>,
    wal_rotation_check: Option<PhaseTiming>,
    wal_rotation: Option<PhaseTiming>,
    wal_append_write: Option<PhaseTiming>,
    wal_envelope_encode: Option<PhaseTiming>,
    wal_append_auto_sync: Option<PhaseTiming>,
    wal_explicit_sync: Option<PhaseTiming>,
    sqlite_writer_wait: Option<PhaseTiming>,
    sqlite_transaction_begin: Option<PhaseTiming>,
    sqlite_row_work: Option<PhaseTiming>,
    sqlite_commit: Option<PhaseTiming>,
    ack_queries: Option<PhaseTiming>,
}

struct IngestTimingAttempt<'a> {
    health: &'a Mutex<IngestTimingHealth>,
    activity: &'a CheckpointActivity,
    clock: &'a dyn TimingClock,
    total_started_ns: u128,
    total_activity: ActivitySnapshot,
    phases: IngestAttemptPhases,
    success: bool,
    sequence: u64,
}

fn activity_overlapped(before: ActivitySnapshot, after: ActivitySnapshot) -> bool {
    before.in_flight || after.in_flight || before.generation != after.generation
}

fn measure_phase<T>(
    clock: &dyn TimingClock,
    activity: &CheckpointActivity,
    operation: impl FnOnce() -> T,
) -> (T, PhaseTiming) {
    let before = activity.snapshot();
    let started_ns = clock.now_ns();
    let result = operation();
    let elapsed_ns = clock.now_ns().saturating_sub(started_ns);
    let after = activity.snapshot();
    (
        result,
        PhaseTiming {
            elapsed_ns,
            checkpoint_overlap: activity_overlapped(before, after),
        },
    )
}

fn record_optional(histogram: &mut LatencyHistogram, timing: Option<PhaseTiming>) {
    if let Some(timing) = timing {
        histogram.record(timing.elapsed_ns, Some(timing.checkpoint_overlap));
    }
}

fn merge_phase(target: &mut Option<PhaseTiming>, value: PhaseTiming) {
    match target {
        Some(existing) => {
            existing.elapsed_ns = existing.elapsed_ns.saturating_add(value.elapsed_ns);
            existing.checkpoint_overlap |= value.checkpoint_overlap;
        }
        None => *target = Some(value),
    }
}

fn apply_commit_phase(
    phases: &mut IngestAttemptPhases,
    phase: super::db::CommitBatchPhase,
    timing: super::db::CommitPhaseTiming,
) {
    let timing = PhaseTiming {
        elapsed_ns: timing.elapsed_ns,
        checkpoint_overlap: timing.checkpoint_overlap,
    };
    match phase {
        super::db::CommitBatchPhase::WriterWait => phases.sqlite_writer_wait = Some(timing),
        super::db::CommitBatchPhase::TransactionBegin => {
            phases.sqlite_transaction_begin = Some(timing)
        }
        super::db::CommitBatchPhase::RowWork => phases.sqlite_row_work = Some(timing),
        super::db::CommitBatchPhase::Commit => phases.sqlite_commit = Some(timing),
    }
}

fn apply_wal_phase(
    phases: &mut IngestAttemptPhases,
    phase: WalAppendPhase,
    timing: WalPhaseTiming,
) {
    let timing = PhaseTiming {
        elapsed_ns: timing.elapsed_ns,
        checkpoint_overlap: timing.checkpoint_overlap,
    };
    match phase {
        WalAppendPhase::Encode => phases.wal_envelope_encode = Some(timing),
        WalAppendPhase::RotationCheck => phases.wal_rotation_check = Some(timing),
        WalAppendPhase::Rotation => phases.wal_rotation = Some(timing),
        WalAppendPhase::Write => phases.wal_append_write = Some(timing),
        WalAppendPhase::AutoSync => phases.wal_append_auto_sync = Some(timing),
    }
}

fn checkpoint_marker(activity: &CheckpointActivity) -> (u64, bool) {
    let snapshot = activity.snapshot();
    (snapshot.generation, snapshot.in_flight)
}

impl Drop for IngestTimingAttempt<'_> {
    fn drop(&mut self) {
        let total_after = self.activity.snapshot();
        let total = PhaseTiming {
            elapsed_ns: self.clock.now_ns().saturating_sub(self.total_started_ns),
            checkpoint_overlap: activity_overlapped(self.total_activity, total_after),
        };
        let Ok(mut health) = self.health.lock() else {
            eprintln!("cloud ingest timing health lock failed");
            return;
        };
        health.attempts = health.attempts.saturating_add(1);
        if self.success {
            health.successes = health.successes.saturating_add(1);
        } else {
            health.failures = health.failures.saturating_add(1);
        }
        record_optional(&mut health.gate_wait, self.phases.gate_wait);
        record_optional(&mut health.receipt_preflight, self.phases.receipt_preflight);
        record_optional(
            &mut health.request_and_envelope_build,
            self.phases.request_and_envelope_build,
        );
        record_optional(
            &mut health.writer_lookup_or_create,
            self.phases.writer_lookup_or_create,
        );
        record_optional(&mut health.wal_append_total, self.phases.wal_append_total);
        record_optional(
            &mut health.wal_rotation_check,
            self.phases.wal_rotation_check,
        );
        record_optional(&mut health.wal_rotation, self.phases.wal_rotation);
        record_optional(&mut health.wal_append_write, self.phases.wal_append_write);
        record_optional(
            &mut health.wal_envelope_encode,
            self.phases.wal_envelope_encode,
        );
        record_optional(
            &mut health.wal_append_auto_sync,
            self.phases.wal_append_auto_sync,
        );
        record_optional(&mut health.wal_explicit_sync, self.phases.wal_explicit_sync);
        record_optional(
            &mut health.sqlite_writer_wait,
            self.phases.sqlite_writer_wait,
        );
        record_optional(
            &mut health.sqlite_transaction_begin,
            self.phases.sqlite_transaction_begin,
        );
        record_optional(&mut health.sqlite_row_work, self.phases.sqlite_row_work);
        record_optional(&mut health.sqlite_commit, self.phases.sqlite_commit);
        record_optional(&mut health.ack_queries, self.phases.ack_queries);
        health
            .ingest_total
            .record(total.elapsed_ns, Some(total.checkpoint_overlap));

        let total_us = u64::try_from(total.elapsed_ns / 1_000).unwrap_or(u64::MAX);
        let slow = total_us > SLOW_INGEST_US;
        let sampled = !slow && self.sequence.is_multiple_of(NORMAL_INGEST_LOG_SAMPLE);
        if slow {
            health.slow_events_logged = health.slow_events_logged.saturating_add(1);
        } else if sampled {
            health.normal_samples_logged = health.normal_samples_logged.saturating_add(1);
        }
        let log_line = if slow || sampled {
            let phase_us = |phase: Option<PhaseTiming>| {
                phase
                    .map(|value| u64::try_from(value.elapsed_ns / 1_000).unwrap_or(u64::MAX))
                    .unwrap_or(0)
            };
            let phase_overlap = |phase: Option<PhaseTiming>| {
                phase.map(|value| value.checkpoint_overlap).unwrap_or(false)
            };
            Some(format!(
                "cloud_ingest_timing attempt_seq={} outcome={} total_us={} checkpoint_overlap={} gate_wait_us={} gate_wait_checkpoint_overlap={} receipt_preflight_us={} receipt_preflight_checkpoint_overlap={} request_and_envelope_build_us={} request_and_envelope_build_checkpoint_overlap={} writer_lookup_or_create_us={} writer_lookup_or_create_checkpoint_overlap={} wal_append_total_us={} wal_append_total_checkpoint_overlap={} wal_rotation_check_us={} wal_rotation_check_checkpoint_overlap={} wal_rotation_us={} wal_rotation_checkpoint_overlap={} wal_envelope_encode_us={} wal_envelope_encode_checkpoint_overlap={} wal_append_write_us={} wal_append_write_checkpoint_overlap={} wal_append_auto_sync_us={} wal_append_auto_sync_checkpoint_overlap={} wal_explicit_sync_us={} wal_explicit_sync_checkpoint_overlap={} sqlite_writer_wait_us={} sqlite_writer_wait_checkpoint_overlap={} sqlite_transaction_begin_us={} sqlite_transaction_begin_checkpoint_overlap={} sqlite_row_work_us={} sqlite_row_work_checkpoint_overlap={} sqlite_commit_us={} sqlite_commit_checkpoint_overlap={} ack_queries_us={} ack_queries_checkpoint_overlap={}",
                self.sequence,
                if self.success { "success" } else { "failure" },
                total_us,
                total.checkpoint_overlap,
                phase_us(self.phases.gate_wait),
                phase_overlap(self.phases.gate_wait),
                phase_us(self.phases.receipt_preflight),
                phase_overlap(self.phases.receipt_preflight),
                phase_us(self.phases.request_and_envelope_build),
                phase_overlap(self.phases.request_and_envelope_build),
                phase_us(self.phases.writer_lookup_or_create),
                phase_overlap(self.phases.writer_lookup_or_create),
                phase_us(self.phases.wal_append_total),
                phase_overlap(self.phases.wal_append_total),
                phase_us(self.phases.wal_rotation_check),
                phase_overlap(self.phases.wal_rotation_check),
                phase_us(self.phases.wal_rotation),
                phase_overlap(self.phases.wal_rotation),
                phase_us(self.phases.wal_envelope_encode),
                phase_overlap(self.phases.wal_envelope_encode),
                phase_us(self.phases.wal_append_write),
                phase_overlap(self.phases.wal_append_write),
                phase_us(self.phases.wal_append_auto_sync),
                phase_overlap(self.phases.wal_append_auto_sync),
                phase_us(self.phases.wal_explicit_sync),
                phase_overlap(self.phases.wal_explicit_sync),
                phase_us(self.phases.sqlite_writer_wait),
                phase_overlap(self.phases.sqlite_writer_wait),
                phase_us(self.phases.sqlite_transaction_begin),
                phase_overlap(self.phases.sqlite_transaction_begin),
                phase_us(self.phases.sqlite_row_work),
                phase_overlap(self.phases.sqlite_row_work),
                phase_us(self.phases.sqlite_commit),
                phase_overlap(self.phases.sqlite_commit),
                phase_us(self.phases.ack_queries),
                phase_overlap(self.phases.ack_queries),
            ))
        } else {
            None
        };
        drop(health);
        if let Some(log_line) = log_line {
            eprintln!("{log_line}");
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestBatchRequest {
    pub schema_version: u16,
    pub installation_id: Uuid,
    pub batch_id: Uuid,
    pub source: String,
    pub capture_session_id: Uuid,
    pub writer_epoch_id: Uuid,
    pub first_source_seq: i64,
    pub last_source_seq: i64,
    pub records: Vec<IngestRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestRecord {
    pub record_id: Uuid,
    pub source_seq: i64,
    pub connection_id: Option<Uuid>,
    pub collector_unix_ns: i64,
    pub collector_monotonic_ns: u64,
    pub physical_device_id: Option<String>,
    pub ble_peripheral_id: Option<String>,
    pub kind: WalRecordKind,
    pub body: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MissingRange {
    pub start_seq: i64,
    pub end_seq: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct IngestBatchResponse {
    pub schema_version: u16,
    pub installation_id: Uuid,
    pub batch_id: Uuid,
    pub accepted_records: usize,
    pub durably_committed_through_seq: i64,
    pub missing_ranges: Vec<MissingRange>,
    pub server_receive_unix_ms: i64,
    pub batch_checksum_sha256: String,
    pub encoded_bytes: usize,
    pub record_count: usize,
    #[serde(skip)]
    pub instrumentation_attempt_seq: u64,
}

#[derive(Debug)]
pub struct IngestStore {
    data_dir: PathBuf,
    db: Arc<CloudDb>,
    timing_health: Arc<Mutex<IngestTimingHealth>>,
    checkpoint_activity: Arc<CheckpointActivity>,
    attempt_sequence: AtomicU64,
    timing_clock: Arc<dyn TimingClock>,
    /// Serializing ingest also prevents a concurrent retry from crossing the
    /// receipt-check/WAL-fsync/SQLite-commit boundary.
    gate: Mutex<IngestState>,
}

#[derive(Debug, Default)]
struct IngestState {
    writers: HashMap<Uuid, WalSegmentManager>,
    durable_batches: HashMap<BatchKey, DurableBatch>,
    #[cfg(test)]
    fail_after_successful_sync: bool,
    #[cfg(test)]
    fail_next_wal_write: bool,
    #[cfg(test)]
    fail_next_wal_sync: bool,
}

#[derive(Clone, Debug)]
struct DurableBatch {
    checksum: String,
    first_source_seq: i64,
    last_source_seq: i64,
    received_unix_ms: i64,
    encoded_bytes: usize,
    records: Vec<ReceiptRecord>,
}

impl IngestStore {
    pub fn new(data_dir: PathBuf, db: Arc<CloudDb>) -> Result<Self> {
        Self::new_with_clock(data_dir, db, Arc::new(SystemTimingClock::new()))
    }

    fn new_with_clock(
        data_dir: PathBuf,
        db: Arc<CloudDb>,
        timing_clock: Arc<dyn TimingClock>,
    ) -> Result<Self> {
        fs::create_dir_all(&data_dir)
            .with_context(|| format!("create cloud data directory {}", data_dir.display()))?;
        cleanup_legacy_staging(&data_dir)?;
        recover_all_stale_open_segments(&data_dir)?;
        let store = Self {
            data_dir,
            db,
            timing_health: Arc::new(Mutex::new(IngestTimingHealth::default())),
            checkpoint_activity: Arc::new(CheckpointActivity::default()),
            attempt_sequence: AtomicU64::new(0),
            timing_clock,
            gate: Mutex::new(IngestState::default()),
        };
        store.recover_receipts_from_wal()?;
        Ok(store)
    }

    fn state(&self) -> Result<MutexGuard<'_, IngestState>> {
        self.gate
            .lock()
            .map_err(|_| anyhow!("cloud ingest lock poisoned"))
    }

    pub fn timing_snapshot(&self) -> IngestTimingHealth {
        self.timing_health
            .lock()
            .map(|health| health.clone())
            .unwrap_or_default()
    }

    pub(crate) fn checkpoint_activity(&self) -> Arc<CheckpointActivity> {
        self.checkpoint_activity.clone()
    }

    pub(crate) fn timing_now_ns(&self) -> u128 {
        self.timing_clock.now_ns()
    }

    pub(crate) fn timing_elapsed_ns(&self, started_ns: u128) -> u128 {
        self.timing_clock.now_ns().saturating_sub(started_ns)
    }

    pub(crate) fn record_blocking_queue(
        &self,
        elapsed_ns: u128,
        before: ActivitySnapshot,
        after: ActivitySnapshot,
    ) -> HttpPhaseTiming {
        let timing = HttpPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: activity_overlapped(before, after),
        };
        if let Ok(mut health) = self.timing_health.lock() {
            health
                .blocking_queue
                .record(elapsed_ns, Some(timing.checkpoint_overlap));
        }
        timing
    }

    pub(crate) fn record_http_auth(
        &self,
        elapsed_ns: u128,
        before: ActivitySnapshot,
        after: ActivitySnapshot,
    ) -> HttpPhaseTiming {
        let timing = HttpPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: activity_overlapped(before, after),
        };
        if let Ok(mut health) = self.timing_health.lock() {
            health
                .http_auth
                .record(elapsed_ns, Some(timing.checkpoint_overlap));
        }
        timing
    }

    pub(crate) fn record_http_decode_checksum(
        &self,
        elapsed_ns: u128,
        before: ActivitySnapshot,
        after: ActivitySnapshot,
    ) -> HttpPhaseTiming {
        let timing = HttpPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: activity_overlapped(before, after),
        };
        if let Ok(mut health) = self.timing_health.lock() {
            health
                .http_decode_checksum
                .record(elapsed_ns, Some(timing.checkpoint_overlap));
        }
        timing
    }

    pub(crate) fn record_handler_total(
        &self,
        elapsed_ns: u128,
        before: ActivitySnapshot,
        after: ActivitySnapshot,
    ) -> HttpPhaseTiming {
        let timing = HttpPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: activity_overlapped(before, after),
        };
        if let Ok(mut health) = self.timing_health.lock() {
            health
                .handler_total
                .record(elapsed_ns, Some(timing.checkpoint_overlap));
        }
        timing
    }

    pub(crate) fn reserve_ingest_attempt_sequence(&self) -> u64 {
        self.attempt_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn timing_attempt(&self, sequence: u64) -> IngestTimingAttempt<'_> {
        IngestTimingAttempt {
            health: &self.timing_health,
            activity: &self.checkpoint_activity,
            clock: &*self.timing_clock,
            total_started_ns: self.timing_clock.now_ns(),
            total_activity: self.checkpoint_activity.snapshot(),
            phases: IngestAttemptPhases::default(),
            success: false,
            sequence,
        }
    }

    fn commit_batch_observed(
        &self,
        batch: &CommittedBatch<'_>,
        phases: &mut IngestAttemptPhases,
    ) -> Result<()> {
        self.db
            .commit_batch_timed(
                batch,
                || checkpoint_marker(&self.checkpoint_activity),
                || self.timing_clock.now_ns(),
                |phase, phase_timing| apply_commit_phase(phases, phase, phase_timing),
            )
            .map(|_| ())
    }

    #[cfg(test)]
    pub fn ingest(
        &self,
        user: &User,
        authenticated_installation_id: Uuid,
        request: IngestBatchRequest,
        checksum: String,
        encoded_bytes: usize,
    ) -> Result<IngestBatchResponse> {
        let sequence = self.reserve_ingest_attempt_sequence();
        self.ingest_with_attempt_sequence(
            user,
            authenticated_installation_id,
            request,
            checksum,
            encoded_bytes,
            sequence,
        )
    }

    pub(crate) fn ingest_with_attempt_sequence(
        &self,
        user: &User,
        authenticated_installation_id: Uuid,
        request: IngestBatchRequest,
        checksum: String,
        encoded_bytes: usize,
        sequence: u64,
    ) -> Result<IngestBatchResponse> {
        let mut timing = self.timing_attempt(sequence);
        let received_ms = unix_ms();
        if authenticated_installation_id != request.installation_id {
            bail!("authenticated installation does not match batch installation_id");
        }
        validate_request(&request, received_ms)?;
        if encoded_bytes > MAX_INGEST_JSON_BYTES {
            bail!("ingest request exceeds {MAX_INGEST_JSON_BYTES} encoded bytes");
        }
        let (state, gate_wait) =
            measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                self.state()
            });
        timing.phases.gate_wait = Some(gate_wait);
        let mut state = state?;
        let (stored, receipt_lookup) =
            measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                self.db
                    .stored_batch(user.id, request.installation_id, request.batch_id)
            });
        merge_phase(&mut timing.phases.receipt_preflight, receipt_lookup);
        if let Some(stored) = stored? {
            if stored.checksum != checksum
                || stored.first_source_seq != request.first_source_seq
                || stored.last_source_seq != request.last_source_seq
            {
                bail!("batch_id was already committed with different content");
            }
            let (response, ack_queries) =
                measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                    self.response_for(
                        user.id,
                        &request,
                        0,
                        stored.received_unix_ms,
                        &checksum,
                        encoded_bytes,
                    )
                });
            timing.phases.ack_queries = Some(ack_queries);
            let mut response = response;
            if let Ok(response) = &mut response {
                response.instrumentation_attempt_seq = timing.sequence;
            }
            timing.success = response.is_ok();
            return response;
        }
        let key = (user.id, request.installation_id, request.batch_id);
        if let Some(durable) = state.durable_batches.get(&key).cloned() {
            if durable.checksum != checksum
                || durable.first_source_seq != request.first_source_seq
                || durable.last_source_seq != request.last_source_seq
            {
                bail!("batch_id reached durable WAL with different content");
            }
            self.commit_batch_observed(
                &CommittedBatch {
                    user_id: user.id,
                    installation_id: request.installation_id,
                    batch_id: request.batch_id,
                    checksum: &durable.checksum,
                    first_source_seq: durable.first_source_seq,
                    last_source_seq: durable.last_source_seq,
                    accepted_records: durable.records.len(),
                    received_unix_ms: durable.received_unix_ms,
                    records: &durable.records,
                },
                &mut timing.phases,
            )
            .context("reconcile fsynced ingest batch into SQLite")?;
            state.durable_batches.remove(&key);
            let (response, ack_queries) =
                measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                    self.response_for(
                        user.id,
                        &request,
                        0,
                        durable.received_unix_ms,
                        &checksum,
                        durable.encoded_bytes,
                    )
                });
            timing.phases.ack_queries = Some(ack_queries);
            let mut response = response;
            if let Ok(response) = &mut response {
                response.instrumentation_attempt_seq = timing.sequence;
            }
            timing.success = response.is_ok();
            return response;
        }

        let (built_records, build_timing) = measure_phase(
            &*self.timing_clock,
            &self.checkpoint_activity,
            || -> Result<(Vec<WalRecord>, Vec<ReceiptRecord>)> {
                let wal_records = request
                    .records
                    .iter()
                    .map(|record| build_wal_record(user.id, &request, record))
                    .collect::<Result<Vec<_>>>()?;
                for record in &wal_records {
                    validate_derived_record(record, received_ms)?;
                }
                let receipt_records = request
                    .records
                    .iter()
                    .zip(request_record_checksums(&request)?)
                    .map(|(record, checksum)| (record.source_seq, record.record_id, checksum))
                    .collect::<Vec<_>>();
                Ok((wal_records, receipt_records))
            },
        );
        merge_phase(&mut timing.phases.request_and_envelope_build, build_timing);
        let (wal_records, receipt_records) = built_records?;
        let (validation, validation_timing) =
            measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                self.db.validate_new_batch_records(
                    user.id,
                    request.installation_id,
                    &receipt_records,
                )
            });
        merge_phase(&mut timing.phases.receipt_preflight, validation_timing);
        validation?;
        let durable = DurableBatch {
            checksum: checksum.clone(),
            first_source_seq: request.first_source_seq,
            last_source_seq: request.last_source_seq,
            received_unix_ms: received_ms,
            encoded_bytes,
            records: receipt_records.clone(),
        };

        let (envelope, envelope_timing) =
            measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                ingest_envelope_record(
                    user.id,
                    &request,
                    &checksum,
                    encoded_bytes,
                    received_ms,
                    receipt_records
                        .iter()
                        .map(|record| record.2.clone())
                        .collect(),
                    wal_records,
                )
            });
        merge_phase(
            &mut timing.phases.request_and_envelope_build,
            envelope_timing,
        );
        let envelope = envelope?;
        let append_result = (|| -> Result<()> {
            #[cfg(test)]
            let fail_next_wal_write = std::mem::take(&mut state.fail_next_wal_write);
            #[cfg(test)]
            let fail_next_wal_sync = std::mem::take(&mut state.fail_next_wal_sync);
            let writer_activity_before = self.checkpoint_activity.snapshot();
            let writer_started_ns = self.timing_clock.now_ns();
            let writer = writer_for(&self.data_dir, &mut state, user.id, &request, received_ms);
            let writer_elapsed = self.timing_clock.now_ns().saturating_sub(writer_started_ns);
            let writer_activity_after = self.checkpoint_activity.snapshot();
            timing.phases.writer_lookup_or_create = Some(PhaseTiming {
                elapsed_ns: writer_elapsed,
                checkpoint_overlap: activity_overlapped(
                    writer_activity_before,
                    writer_activity_after,
                ),
            });
            let writer = writer?;
            #[cfg(test)]
            if fail_next_wal_write {
                writer.fail_next_append_write();
            }

            let append_activity_before = self.checkpoint_activity.snapshot();
            let append_started_ns = self.timing_clock.now_ns();
            let append = writer.append_record_timed(
                &envelope,
                || checkpoint_marker(&self.checkpoint_activity),
                || self.timing_clock.now_ns(),
                |phase, phase_timing| apply_wal_phase(&mut timing.phases, phase, phase_timing),
            );
            let append_elapsed = self.timing_clock.now_ns().saturating_sub(append_started_ns);
            let append_activity_after = self.checkpoint_activity.snapshot();
            let append_overlap = activity_overlapped(append_activity_before, append_activity_after);
            timing.phases.wal_append_total = Some(PhaseTiming {
                elapsed_ns: append_elapsed,
                checkpoint_overlap: append_overlap,
            });
            append.context("append atomic cloud ingest envelope")?;

            #[cfg(test)]
            if fail_next_wal_sync {
                writer.fail_next_sync();
            }
            let sync_activity_before = self.checkpoint_activity.snapshot();
            let sync_started_ns = self.timing_clock.now_ns();
            let sync_result = writer.sync_all();
            let measured_sync_ns = self.timing_clock.now_ns().saturating_sub(sync_started_ns);
            let sync_activity_after = self.checkpoint_activity.snapshot();
            timing.phases.wal_explicit_sync = Some(PhaseTiming {
                elapsed_ns: measured_sync_ns,
                checkpoint_overlap: activity_overlapped(sync_activity_before, sync_activity_after),
            });
            sync_result.context("fsync atomic cloud ingest envelope")?;
            #[cfg(test)]
            if std::mem::take(&mut state.fail_after_successful_sync) {
                bail!("injected ambiguous fsync result after durable write");
            }
            Ok(())
        })();
        if let Err(error) = append_result {
            // A short/torn final frame must be recovered before a retry can
            // append to this participant's canonical stream.
            state.writers.remove(&user.id);
            let recovery_ns = received_ms.saturating_mul(1_000_000);
            let path_config =
                WalPathConfig::new(&self.data_dir, user.id.to_string(), H10_WAL_SOURCE)?;
            let recovery_context = WalRecordContext::new(
                user.id.to_string(),
                H10_WAL_SOURCE,
                "training_sensors cloud ingest recovery",
                request.writer_epoch_id,
                request.capture_session_id,
            )?
            .at_time(recovery_ns, 0);
            let reports = recover_stale_open_segments(&path_config, &recovery_context, recovery_ns)
                .context("recover cloud WAL after ambiguous append/fsync failure")?;
            let envelope_is_complete = reports
                .iter()
                .filter_map(|report| report.closed_path.as_ref())
                .filter_map(|path| read_wal_records(path, WalReadMode::Strict).ok())
                .flat_map(|read| read.records)
                .any(|frame| {
                    matches!(
                        &frame.record.body,
                        WalBody::CloudIngestBatchV1(body)
                            if body.installation_id == request.installation_id
                                && body.batch_id == request.batch_id
                                && body.request_sha256 == checksum
                    )
                });
            if envelope_is_complete {
                state.durable_batches.insert(key, durable.clone());
                self.commit_batch_observed(
                    &CommittedBatch {
                        user_id: user.id,
                        installation_id: request.installation_id,
                        batch_id: request.batch_id,
                        checksum: &durable.checksum,
                        first_source_seq: durable.first_source_seq,
                        last_source_seq: durable.last_source_seq,
                        accepted_records: durable.records.len(),
                        received_unix_ms: durable.received_unix_ms,
                        records: &durable.records,
                    },
                    &mut timing.phases,
                )
                .context("reconcile exact cloud batch after ambiguous fsync result")?;
                state.durable_batches.remove(&key);
            }
            return Err(error);
        }

        state.durable_batches.insert(key, durable);

        let commit = CommittedBatch {
            user_id: user.id,
            installation_id: request.installation_id,
            batch_id: request.batch_id,
            checksum: &checksum,
            first_source_seq: request.first_source_seq,
            last_source_seq: request.last_source_seq,
            accepted_records: request.records.len(),
            received_unix_ms: received_ms,
            records: &receipt_records,
        };
        self.commit_batch_observed(&commit, &mut timing.phases)
            .context("record fsynced ingest batch in SQLite")?;
        state.durable_batches.remove(&key);
        let (response, ack_queries) =
            measure_phase(&*self.timing_clock, &self.checkpoint_activity, || {
                self.response_for(
                    user.id,
                    &request,
                    request.records.len(),
                    received_ms,
                    &checksum,
                    encoded_bytes,
                )
            });
        timing.phases.ack_queries = Some(ack_queries);
        let mut response = response;
        if let Ok(response) = &mut response {
            response.instrumentation_attempt_seq = timing.sequence;
        }
        timing.success = response.is_ok();
        response
    }

    fn response_for(
        &self,
        user_id: Uuid,
        request: &IngestBatchRequest,
        accepted_records: usize,
        received_ms: i64,
        checksum: &str,
        encoded_bytes: usize,
    ) -> Result<IngestBatchResponse> {
        let committed = self
            .db
            .contiguous_through(user_id, request.installation_id)?;
        let missing = self
            .db
            .missing_ranges(user_id, request.installation_id, request.last_source_seq)?
            .into_iter()
            .map(|(start_seq, end_seq)| MissingRange { start_seq, end_seq })
            .collect();
        Ok(IngestBatchResponse {
            schema_version: 1,
            installation_id: request.installation_id,
            batch_id: request.batch_id,
            accepted_records,
            durably_committed_through_seq: committed,
            missing_ranges: missing,
            server_receive_unix_ms: received_ms,
            batch_checksum_sha256: checksum.to_string(),
            encoded_bytes,
            record_count: request.records.len(),
            instrumentation_attempt_seq: 0,
        })
    }

    /// Rebuild receipt rows for a batch whose commit marker reached stable WAL
    /// storage immediately before a process crash and SQLite commit did not.
    pub fn recover_receipts_from_wal(&self) -> Result<usize> {
        let mut paths = Vec::new();
        collect_wal_paths(&self.data_dir.join("wal"), &mut paths)?;
        paths.sort();
        let mut scan_paths = Vec::new();
        for path in paths {
            let metadata = fs::metadata(&path)
                .with_context(|| format!("stat WAL reconciliation file {}", path.display()))?;
            let modified_unix_ns = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
                .unwrap_or(0);
            let path_text = path.to_string_lossy().to_string();
            if path.extension().and_then(|value| value.to_str()) == Some("closed")
                && self
                    .db
                    .wal_file_is_reconciled(&path_text, metadata.len(), modified_unix_ns)?
            {
                continue;
            }
            scan_paths.push((path, path_text, metadata.len(), modified_unix_ns));
        }
        let mut recovered = 0_usize;
        for (path, _, _, _) in &scan_paths {
            let read = read_wal_records(path, WalReadMode::TailTolerant)?;
            for frame in read.records {
                let record = frame.record;
                let WalBody::CloudIngestBatchV1(batch) = &record.body else {
                    continue;
                };
                let Ok(user_id) = Uuid::parse_str(&record.participant_id) else {
                    bail!("cloud ingest envelope participant_id is not a UUID");
                };
                if self
                    .db
                    .stored_batch(user_id, batch.installation_id, batch.batch_id)?
                    .is_some()
                {
                    continue;
                }
                let batch_records = receipt_records_from_envelope(batch)?;
                if batch_records.first().map(|record| record.0) != Some(batch.first_source_seq)
                    || batch_records.last().map(|record| record.0) != Some(batch.last_source_seq)
                {
                    bail!(
                        "WAL envelope {} sequence bounds do not match records",
                        batch.batch_id
                    );
                }
                self.db.reconcile_batch_from_wal(&CommittedBatch {
                    user_id,
                    installation_id: batch.installation_id,
                    batch_id: batch.batch_id,
                    checksum: &batch.request_sha256,
                    first_source_seq: batch.first_source_seq,
                    last_source_seq: batch.last_source_seq,
                    accepted_records: batch_records.len(),
                    received_unix_ms: batch.received_unix_ms,
                    records: &batch_records,
                })?;
                recovered = recovered.saturating_add(1);
            }
        }
        let now_ms = unix_ms();
        for (path, path_text, size_bytes, modified_unix_ns) in scan_paths {
            if path.extension().and_then(|value| value.to_str()) == Some("closed") {
                self.db.mark_wal_file_reconciled(
                    &path_text,
                    size_bytes,
                    modified_unix_ns,
                    now_ms,
                )?;
            }
        }
        Ok(recovered)
    }

    #[cfg(test)]
    fn current_wal_path(&self, user_id: Uuid) -> Option<PathBuf> {
        self.state()
            .ok()?
            .writers
            .get(&user_id)?
            .current_open_path()
            .map(Path::to_path_buf)
    }

    #[cfg(test)]
    fn fail_after_successful_sync(&self) {
        self.state().unwrap().fail_after_successful_sync = true;
    }

    #[cfg(test)]
    fn fail_next_wal_write(&self) {
        self.state().unwrap().fail_next_wal_write = true;
    }

    #[cfg(test)]
    fn fail_next_wal_sync(&self) {
        self.state().unwrap().fail_next_wal_sync = true;
    }
}

fn validate_request(request: &IngestBatchRequest, server_receive_unix_ms: i64) -> Result<()> {
    if request.schema_version != 1 {
        bail!(
            "unsupported ingest schema_version {}",
            request.schema_version
        );
    }
    if request.source != H10_WAL_SOURCE {
        bail!("source must be {H10_WAL_SOURCE}");
    }
    if request.records.is_empty() || request.records.len() > MAX_INGEST_RECORDS {
        bail!("records must contain 1..={MAX_INGEST_RECORDS} items");
    }
    if request.first_source_seq < 0
        || request.last_source_seq < request.first_source_seq
        || request.last_source_seq - request.first_source_seq + 1
            != i64::try_from(request.records.len()).unwrap_or(i64::MAX)
    {
        bail!("batch sequence bounds do not match record count");
    }
    let mut record_ids = HashSet::new();
    let minimum_collector_unix_ns = MIN_COLLECTOR_UNIX_MS.saturating_mul(1_000_000);
    let maximum_collector_unix_ns = server_receive_unix_ms
        .saturating_add(MAX_COLLECTOR_FUTURE_SKEW_MS)
        .saturating_mul(1_000_000);
    for (offset, record) in request.records.iter().enumerate() {
        let expected = request
            .first_source_seq
            .checked_add(i64::try_from(offset).context("record offset does not fit i64")?)
            .ok_or_else(|| anyhow!("source sequence overflow"))?;
        if record.source_seq != expected {
            bail!("records must have an exact contiguous source_seq ordering");
        }
        if !record_ids.insert(record.record_id) {
            bail!("record_id is duplicated within batch");
        }
        if record.collector_unix_ns < minimum_collector_unix_ns {
            bail!("collector_unix_ns predates supported epoch 2000-01-01");
        }
        if record.collector_unix_ns > maximum_collector_unix_ns {
            bail!(
                "collector_unix_ns exceeds maximum allowed future clock skew of {MAX_COLLECTOR_FUTURE_SKEW_MS} ms"
            );
        }
        if matches!(
            record.kind,
            WalRecordKind::SegmentStarted
                | WalRecordKind::SegmentClosed
                | WalRecordKind::RecoveryEvent
        ) {
            bail!(
                "server-owned WAL kind {} is not accepted",
                record.kind.as_str()
            );
        }
    }
    Ok(())
}

fn validate_derived_record(record: &WalRecord, server_receive_unix_ms: i64) -> Result<()> {
    let minimum_unix_ns = MIN_COLLECTOR_UNIX_MS.saturating_mul(1_000_000);
    let maximum_unix_ns = server_receive_unix_ms
        .saturating_add(MAX_COLLECTOR_FUTURE_SKEW_MS)
        .saturating_mul(1_000_000);
    match &record.body {
        WalBody::PsftpSetLocalTimeAttempt(body) => validate_embedded_unix_ns(
            "psftp local_time_unix_ns",
            body.local_time_unix_ns,
            minimum_unix_ns,
            maximum_unix_ns,
        ),
        WalBody::ManualMarker(body) => validate_embedded_unix_ns(
            "manual marker event_unix_ns",
            body.event_unix_ns,
            minimum_unix_ns,
            maximum_unix_ns,
        ),
        WalBody::GattNotification(body) => {
            let uuid = body.characteristic_uuid.to_ascii_lowercase();
            if uuid != PMD_DATA_UUID && uuid != HR_MEASUREMENT_UUID {
                return Ok(());
            }
            let payload = base64::engine::general_purpose::STANDARD
                .decode(&body.payload_b64)
                .context("decode notification payload for semantic validation")?;
            if uuid == HR_MEASUREMENT_UUID {
                parse_hr(&payload).context("invalid H10 HR notification")?;
                return Ok(());
            }
            if payload.len() < 10 {
                bail!("invalid H10 PMD notification: frame is shorter than 10-byte header");
            }
            // A device-clock anomaly is raw evidence, not a reason to block the
            // phone's immutable queue. The shared PMD parser reports and omits
            // invalid timestamps from derived data; WAL retains the exact bytes.
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_embedded_unix_ns(label: &str, value: i64, minimum: i64, maximum: i64) -> Result<()> {
    if value < minimum || value > maximum {
        bail!("{label} is outside supported timestamp range");
    }
    Ok(())
}

fn build_wal_record(
    user_id: Uuid,
    request: &IngestBatchRequest,
    record: &IngestRecord,
) -> Result<WalRecord> {
    let value = serde_json::json!({
        "schema_version": 1,
        "record_id": record.record_id,
        "kind": record.kind,
        "participant_id": user_id.to_string(),
        "source": request.source,
        "writer_id": format!("android:{}", request.installation_id),
        "writer_epoch_id": request.writer_epoch_id,
        "session_id": request.capture_session_id,
        "connection_id": record.connection_id,
        "collector_unix_ns": record.collector_unix_ns,
        "collector_monotonic_ns": record.collector_monotonic_ns,
        "physical_device_id": record.physical_device_id,
        "ble_peripheral_id": record.ble_peripheral_id,
        "device_installation_id": request.installation_id,
        "origin_batch_id": request.batch_id,
        "origin_source_seq": record.source_seq,
        "body": record.body,
    });
    serde_json::from_value(value).context("validate incoming WAL record body")
}

fn request_record_checksums(request: &IngestBatchRequest) -> Result<Vec<String>> {
    request
        .records
        .iter()
        .map(|record| {
            serde_json::to_vec(record)
                .map(|bytes| hex::encode(Sha256::digest(bytes)))
                .context("checksum ingest record")
        })
        .collect()
}

fn writer_for<'a>(
    data_dir: &Path,
    state: &'a mut IngestState,
    user_id: Uuid,
    request: &IngestBatchRequest,
    now_ms: i64,
) -> Result<&'a mut WalSegmentManager> {
    if let Entry::Vacant(entry) = state.writers.entry(user_id) {
        let now_ns = now_ms.saturating_mul(1_000_000);
        let path_config = WalPathConfig::new(data_dir, user_id.to_string(), H10_WAL_SOURCE)?;
        let recovery_context = WalRecordContext::new(
            user_id.to_string(),
            H10_WAL_SOURCE,
            "training_sensors cloud ingest recovery",
            request.writer_epoch_id,
            request.capture_session_id,
        )?
        .at_time(now_ns, 0);
        recover_stale_open_segments(&path_config, &recovery_context, now_ns)?;
        let context = WalRecordContext::new(
            user_id.to_string(),
            H10_WAL_SOURCE,
            "training_sensors cloud ingest",
            request.writer_epoch_id,
            request.capture_session_id,
        )?
        .at_time(now_ns, 0);
        let manager = WalSegmentManager::create(
            path_config,
            context,
            now_ns,
            0,
            WalSegmentManagerOptions {
                writer_options: WalWriterOptions {
                    // Every accepted envelope is explicitly fsynced below.
                    fsync_interval: Duration::from_secs(10),
                    ..WalWriterOptions::default()
                },
                rotation_policy: WalRotationPolicy::default(),
            },
        )?;
        entry.insert(manager);
    }
    state
        .writers
        .get_mut(&user_id)
        .ok_or_else(|| anyhow!("cloud WAL writer disappeared after creation"))
}

fn ingest_envelope_record(
    user_id: Uuid,
    request: &IngestBatchRequest,
    checksum: &str,
    encoded_bytes: usize,
    received_ms: i64,
    record_sha256: Vec<String>,
    records: Vec<WalRecord>,
) -> Result<WalRecord> {
    let now_ns = received_ms.saturating_mul(1_000_000);
    let context = WalRecordContext::new(
        user_id.to_string(),
        H10_WAL_SOURCE,
        "training_sensors cloud ingest",
        request.writer_epoch_id,
        request.capture_session_id,
    )?
    .at_time(now_ns, 0);
    Ok(
        context.make_record(WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: request.installation_id,
            batch_id: request.batch_id,
            request_sha256: checksum.to_string(),
            encoded_bytes: u64::try_from(encoded_bytes)
                .context("encoded request size does not fit u64")?,
            first_source_seq: request.first_source_seq,
            last_source_seq: request.last_source_seq,
            received_unix_ms: received_ms,
            record_sha256,
            records,
        })),
    )
}

fn receipt_records_from_envelope(batch: &CloudIngestBatchV1Body) -> Result<Vec<ReceiptRecord>> {
    if batch.records.is_empty() {
        bail!("cloud ingest envelope {} is empty", batch.batch_id);
    }
    let mut receipts = Vec::with_capacity(batch.records.len());
    let mut record_ids = HashSet::new();
    for (record, checksum) in batch.records.iter().zip(&batch.record_sha256) {
        if record.device_installation_id != Some(batch.installation_id)
            || record.origin_batch_id != Some(batch.batch_id)
        {
            bail!(
                "cloud ingest envelope {} has mismatched child identity",
                batch.batch_id
            );
        }
        let source_seq = record
            .origin_source_seq
            .ok_or_else(|| anyhow!("cloud ingest envelope child has no source_seq"))?;
        if !record_ids.insert(record.record_id) {
            bail!("cloud ingest envelope {} reuses record_id", batch.batch_id);
        }
        receipts.push((source_seq, record.record_id, checksum.clone()));
    }
    receipts.sort_by_key(|record| record.0);
    for pair in receipts.windows(2) {
        if pair[1].0 != pair[0].0.saturating_add(1) {
            bail!("cloud ingest envelope {} is not contiguous", batch.batch_id);
        }
    }
    Ok(receipts)
}

fn cleanup_legacy_staging(data_dir: &Path) -> Result<()> {
    let staging = data_dir.join(".ingest-staging");
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .with_context(|| format!("remove unpublished ingest staging {}", staging.display()))?;
    }
    Ok(())
}

fn recover_all_stale_open_segments(data_dir: &Path) -> Result<usize> {
    let mut paths = Vec::new();
    collect_wal_paths(&data_dir.join("wal"), &mut paths)?;
    let now_ns = unix_ms().saturating_mul(1_000_000);
    let mut recovered = 0_usize;
    for path in paths
        .into_iter()
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("open"))
    {
        let mut participant = None;
        let mut source = None;
        for component in path.components() {
            let value = component.as_os_str().to_string_lossy();
            if let Some(found) = value.strip_prefix("participant_id=") {
                participant = Some(found.to_string());
            }
            if let Some(found) = value.strip_prefix("source=") {
                source = Some(found.to_string());
            }
        }
        let participant = participant.ok_or_else(|| {
            anyhow!(
                "stale WAL path has no participant partition: {}",
                path.display()
            )
        })?;
        let source = source
            .ok_or_else(|| anyhow!("stale WAL path has no source partition: {}", path.display()))?;
        let context = WalRecordContext::new(
            participant,
            source,
            "training_sensors cloud startup recovery",
            Uuid::new_v4(),
            Uuid::new_v4(),
        )?
        .at_time(now_ns, 0);
        recover_open_segment(&path, &context, now_ns)
            .with_context(|| format!("recover stale cloud WAL {}", path.display()))?;
        recovered = recovered.saturating_add(1);
    }
    Ok(recovered)
}

fn collect_wal_paths(root: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root).with_context(|| format!("scan {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_wal_paths(&path, paths)?;
        } else if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension == "open" || extension == "closed")
        {
            paths.push(path);
        }
    }
    Ok(())
}

fn unix_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::db::{CommitBatchPhase, NewSession};
    use crate::wal::{GattNotificationBody, WalBody};

    struct TestDir(PathBuf);

    #[derive(Debug)]
    struct StepClock {
        next_ns: AtomicU64,
        step_ns: u64,
    }

    impl StepClock {
        fn new(step_ns: u64) -> Self {
            Self {
                next_ns: AtomicU64::new(0),
                step_ns,
            }
        }
    }

    impl TimingClock for StepClock {
        fn now_ns(&self) -> u128 {
            u128::from(self.next_ns.fetch_add(self.step_ns, Ordering::SeqCst))
        }
    }

    impl TestDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("training-sensors-cloud-{}", Uuid::new_v4()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn fixture_request(installation_id: Uuid, batch_id: Uuid, first: i64) -> IngestBatchRequest {
        let mut pmd_payload = vec![0];
        pmd_payload.extend_from_slice(&1_000_000_000_u64.to_le_bytes());
        pmd_payload.extend_from_slice(&[0, 1, 0, 0]);
        let body = WalBody::GattNotification(
            GattNotificationBody::from_payload(
                "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
                "pmd_data",
                &pmd_payload,
            )
            .unwrap(),
        );
        IngestBatchRequest {
            schema_version: 1,
            installation_id,
            batch_id,
            source: H10_WAL_SOURCE.to_string(),
            capture_session_id: Uuid::new_v4(),
            writer_epoch_id: Uuid::new_v4(),
            first_source_seq: first,
            last_source_seq: first,
            records: vec![IngestRecord {
                record_id: Uuid::new_v4(),
                source_seq: first,
                connection_id: Some(Uuid::new_v4()),
                collector_unix_ns: 1_700_000_000_000_000_000,
                collector_monotonic_ns: 42,
                physical_device_id: Some("h10".to_string()),
                ble_peripheral_id: Some("polar".to_string()),
                kind: WalRecordKind::GattNotification,
                body: serde_json::to_value(body).unwrap(),
            }],
        }
    }

    fn ingest_fixture(
        store: &IngestStore,
        user: &User,
        request: IngestBatchRequest,
    ) -> Result<IngestBatchResponse> {
        let bytes = serde_json::to_vec(&request)?;
        let checksum = hex::encode(Sha256::digest(&bytes));
        store.ingest(
            user,
            request.installation_id,
            request,
            checksum,
            bytes.len(),
        )
    }

    #[test]
    fn batch_is_fsynced_idempotent_and_preserves_phone_identity() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        let first = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(first.accepted_records, 1);
        assert_eq!(first.durably_committed_through_seq, 0);
        let duplicate = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(duplicate.accepted_records, 0);

        let path = store.current_wal_path(user.id).unwrap();
        let read = read_wal_records(&path, WalReadMode::Strict).unwrap();
        let captured = read
            .records
            .iter()
            .flat_map(|frame| crate::wal::logical_wal_records(&frame.record))
            .find(|record| record.record_id == request.records[0].record_id)
            .unwrap();
        assert_eq!(captured.origin_source_seq, Some(0));
        assert_eq!(
            captured.device_installation_id,
            Some(request.installation_id)
        );
        assert_eq!(captured.origin_batch_id, Some(request.batch_id));
        assert!(read.records.iter().any(|frame| matches!(&frame.record.body, WalBody::CloudIngestBatchV1(body) if body.batch_id == request.batch_id)));
    }

    #[test]
    fn invalid_device_timestamp_is_durable_idempotent_and_does_not_block_next_batch() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("clock@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        let installation = Uuid::new_v4();
        let mut request = fixture_request(installation, Uuid::new_v4(), 0);
        let mut payload = vec![0];
        payload.extend_from_slice(&u64::MAX.to_le_bytes());
        payload.extend_from_slice(&[0, 1, 0, 0]);
        request.records[0].body = serde_json::to_value(WalBody::GattNotification(
            GattNotificationBody::from_payload(PMD_DATA_UUID, "pmd_data", &payload).unwrap(),
        ))
        .unwrap();
        let bytes = serde_json::to_vec(&request).unwrap();
        let first = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(first.accepted_records, 1);
        assert_eq!(first.durably_committed_through_seq, 0);
        assert_eq!(
            first.batch_checksum_sha256,
            hex::encode(Sha256::digest(&bytes))
        );
        assert_eq!(first.encoded_bytes, bytes.len());
        let duplicate = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(duplicate.accepted_records, 0);
        assert_eq!(duplicate.durably_committed_through_seq, 0);
        let next = ingest_fixture(
            &store,
            &user,
            fixture_request(installation, Uuid::new_v4(), 1),
        )
        .unwrap();
        assert_eq!(next.durably_committed_through_seq, 1);
        let path = store.current_wal_path(user.id).unwrap();
        let read = read_wal_records(&path, WalReadMode::Strict).unwrap();
        let captured = read
            .records
            .iter()
            .flat_map(|frame| crate::wal::logical_wal_records(&frame.record))
            .filter(|record| record.record_id == request.records[0].record_id)
            .collect::<Vec<_>>();
        assert_eq!(captured.len(), 1);
        let WalBody::GattNotification(body) = &captured[0].body else {
            panic!("notification expected")
        };
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&body.payload_b64)
                .unwrap(),
            payload
        );
        assert_eq!(captured[0].origin_source_seq, Some(0));
        drop(store);
        let recovered = IngestStore::new(dir.0.clone(), db).unwrap();
        assert_eq!(
            ingest_fixture(&recovered, &user, request)
                .unwrap()
                .accepted_records,
            0
        );
    }

    #[test]
    fn ingest_timing_is_bounded_phase_complete_and_identifier_free() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("timing-secret@example.com", 1).unwrap();
        let store =
            IngestStore::new_with_clock(dir.0.clone(), db, Arc::new(StepClock::new(100_000)))
                .unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        let installation_text = request.installation_id.to_string();
        let response = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(response.instrumentation_attempt_seq, 1);

        let encoded = serde_json::to_string(&response).unwrap();
        assert!(!encoded.contains("instrumentation_attempt_seq"));
        let decoded: IngestBatchResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.instrumentation_attempt_seq, 0);

        let mut wrong_installation = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 1);
        let authenticated = wrong_installation.installation_id;
        wrong_installation.installation_id = Uuid::new_v4();
        let bytes = serde_json::to_vec(&wrong_installation).unwrap();
        assert!(
            store
                .ingest(
                    &user,
                    authenticated,
                    wrong_installation,
                    hex::encode(Sha256::digest(&bytes)),
                    bytes.len(),
                )
                .is_err()
        );

        let health = store.timing_snapshot();
        assert_eq!(health.attempts, 2);
        assert_eq!(health.successes, 1);
        assert_eq!(health.failures, 1);
        assert_eq!(health.ingest_total.count, 2);
        for phase in [
            &health.gate_wait,
            &health.receipt_preflight,
            &health.request_and_envelope_build,
            &health.writer_lookup_or_create,
            &health.wal_append_total,
            &health.wal_rotation_check,
            &health.wal_append_write,
            &health.wal_envelope_encode,
            &health.wal_explicit_sync,
            &health.sqlite_writer_wait,
            &health.sqlite_transaction_begin,
            &health.sqlite_row_work,
            &health.sqlite_commit,
            &health.ack_queries,
        ] {
            assert_eq!(phase.count, 1);
            assert_eq!(phase.buckets.iter().sum::<u64>(), phase.count);
        }
        for exact_phase in [
            &health.wal_rotation_check,
            &health.wal_append_write,
            &health.wal_envelope_encode,
            &health.wal_explicit_sync,
            &health.sqlite_writer_wait,
            &health.sqlite_transaction_begin,
            &health.sqlite_row_work,
            &health.sqlite_commit,
        ] {
            assert_eq!(exact_phase.total_us, 100);
        }
        assert_eq!(health.ingest_total.buckets.iter().sum::<u64>(), 2);
        let health_json = serde_json::to_string(&health).unwrap();
        assert!(!health_json.contains("timing-secret@example.com"));
        assert!(!health_json.contains(&user.id.to_string()));
        assert!(!health_json.contains(&installation_text));
        assert!(!health_json.contains(&request.batch_id.to_string()));
    }

    #[test]
    fn latency_histogram_has_fixed_pretraffic_schema_and_exact_boundaries() {
        let mut histogram = LatencyHistogram::default();
        assert_eq!(histogram.bucket_upper_us, LATENCY_BUCKET_UPPER_US);
        assert_eq!(histogram.buckets, [0; 12]);
        for (elapsed_us, expected_bucket) in [
            (0_u64, 0_usize),
            (1_000, 0),
            (1_001, 1),
            (5_000, 2),
            (5_000_001, 11),
        ] {
            histogram.record(
                u128::from(elapsed_us) * 1_000,
                Some(expected_bucket % 2 == 0),
            );
        }
        assert_eq!(histogram.count, 5);
        assert_eq!(histogram.buckets.iter().sum::<u64>(), histogram.count);
        assert_eq!(histogram.buckets[0], 2);
        assert_eq!(histogram.buckets[1], 1);
        assert_eq!(histogram.buckets[2], 1);
        assert_eq!(histogram.buckets[11], 1);
        assert_eq!(histogram.checkpoint_overlap_count, 3);
    }

    #[test]
    fn normal_sampling_uses_reserved_ordinal_after_preingest_gaps() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let store = IngestStore::new_with_clock(dir.0.clone(), db, Arc::new(StepClock::new(1_000)))
            .unwrap();

        // These reserved ordinals represent requests rejected before entering
        // core ingest. The first core attempt must still sample on the shared
        // ordinal used by the handler rather than on core-attempt cardinality.
        for expected in 1..128 {
            assert_eq!(store.reserve_ingest_attempt_sequence(), expected);
        }
        let sampled_sequence = store.reserve_ingest_attempt_sequence();
        assert_eq!(sampled_sequence, 128);
        let mut timing = store.timing_attempt(sampled_sequence);
        timing.success = true;
        drop(timing);

        let health = store.timing_snapshot();
        assert_eq!(health.attempts, 1);
        assert_eq!(health.successes, 1);
        assert_eq!(health.normal_samples_logged, 1);
    }

    #[test]
    fn failed_wal_and_sqlite_phases_remain_visible_with_injected_time() {
        for failing_phase in [
            CommitBatchPhase::WriterWait,
            CommitBatchPhase::TransactionBegin,
            CommitBatchPhase::RowWork,
            CommitBatchPhase::Commit,
        ] {
            let dir = TestDir::new();
            let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
            let user = db.ensure_admin("admin@example.com", 1).unwrap();
            let store = IngestStore::new_with_clock(
                dir.0.clone(),
                db.clone(),
                Arc::new(StepClock::new(100_000)),
            )
            .unwrap();
            db.fail_next_timed_commit_phase(failing_phase);
            assert!(
                ingest_fixture(
                    &store,
                    &user,
                    fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0),
                )
                .is_err()
            );
            let health = store.timing_snapshot();
            let failed = match failing_phase {
                CommitBatchPhase::WriterWait => &health.sqlite_writer_wait,
                CommitBatchPhase::TransactionBegin => &health.sqlite_transaction_begin,
                CommitBatchPhase::RowWork => &health.sqlite_row_work,
                CommitBatchPhase::Commit => &health.sqlite_commit,
            };
            assert_eq!(failed.count, 1);
            assert_eq!(failed.total_us, 100);
            assert_eq!(health.failures, 1);
        }

        let write_dir = TestDir::new();
        let write_db = Arc::new(CloudDb::open(&write_dir.0.join("cloud.sqlite3")).unwrap());
        let write_user = write_db.ensure_admin("write@example.com", 1).unwrap();
        let write_store = IngestStore::new_with_clock(
            write_dir.0.clone(),
            write_db,
            Arc::new(StepClock::new(100_000)),
        )
        .unwrap();
        write_store.fail_next_wal_write();
        assert!(
            ingest_fixture(
                &write_store,
                &write_user,
                fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0),
            )
            .is_err()
        );
        let write_health = write_store.timing_snapshot();
        assert_eq!(write_health.wal_append_write.count, 1);
        assert_eq!(write_health.wal_append_write.total_us, 100);
        assert_eq!(write_health.wal_explicit_sync.count, 0);

        let sync_dir = TestDir::new();
        let sync_db = Arc::new(CloudDb::open(&sync_dir.0.join("cloud.sqlite3")).unwrap());
        let sync_user = sync_db.ensure_admin("sync@example.com", 1).unwrap();
        let sync_store = IngestStore::new_with_clock(
            sync_dir.0.clone(),
            sync_db,
            Arc::new(StepClock::new(100_000)),
        )
        .unwrap();
        sync_store.fail_next_wal_sync();
        assert!(
            ingest_fixture(
                &sync_store,
                &sync_user,
                fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0),
            )
            .is_err()
        );
        let sync_health = sync_store.timing_snapshot();
        assert_eq!(sync_health.wal_explicit_sync.count, 1);
        assert_eq!(sync_health.wal_explicit_sync.total_us, 100);
    }

    #[test]
    fn every_exact_ingest_phase_records_checkpoint_overlap() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("overlap@example.com", 1).unwrap();
        let store =
            IngestStore::new_with_clock(dir.0.clone(), db, Arc::new(StepClock::new(100_000)))
                .unwrap();
        let activity = store.checkpoint_activity();
        let checkpoint = activity.guard();
        ingest_fixture(
            &store,
            &user,
            fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0),
        )
        .unwrap();
        drop(checkpoint);
        let health = store.timing_snapshot();
        for phase in [
            &health.gate_wait,
            &health.receipt_preflight,
            &health.request_and_envelope_build,
            &health.writer_lookup_or_create,
            &health.wal_append_total,
            &health.wal_rotation_check,
            &health.wal_envelope_encode,
            &health.wal_append_write,
            &health.wal_explicit_sync,
            &health.sqlite_writer_wait,
            &health.sqlite_transaction_begin,
            &health.sqlite_row_work,
            &health.sqlite_commit,
            &health.ack_queries,
        ] {
            assert!(phase.checkpoint_overlap_tracked);
            assert_eq!(phase.checkpoint_overlap_count, phase.count);
        }
    }

    #[test]
    fn checkpoint_activity_guard_clears_in_flight_during_unwind() {
        let activity = Arc::new(CheckpointActivity::default());
        let panicked = std::panic::catch_unwind({
            let activity = activity.clone();
            move || {
                let _guard = activity.guard();
                assert!(activity.snapshot().in_flight);
                panic!("injected checkpoint panic");
            }
        });
        assert!(panicked.is_err());
        let after = activity.snapshot();
        assert!(!after.in_flight);
        assert_eq!(after.generation, 2);
    }

    #[test]
    fn future_capture_timestamps_are_bounded_but_old_outage_catchup_is_allowed() {
        let server_receive_ms = 1_800_000_000_000_i64;
        let installation = Uuid::new_v4();
        let mut request = fixture_request(installation, Uuid::new_v4(), 0);
        request.records[0].collector_unix_ns = server_receive_ms
            .saturating_add(MAX_COLLECTOR_FUTURE_SKEW_MS)
            .saturating_mul(1_000_000);
        validate_request(&request, server_receive_ms).unwrap();

        request.records[0].collector_unix_ns = server_receive_ms
            .saturating_add(MAX_COLLECTOR_FUTURE_SKEW_MS + 1)
            .saturating_mul(1_000_000);
        let error = validate_request(&request, server_receive_ms).unwrap_err();
        assert!(format!("{error:#}").contains("future clock skew"));

        request.records[0].collector_unix_ns = 946_684_800_000_000_000;
        validate_request(&request, server_receive_ms).unwrap();

        request.records[0].collector_unix_ns = -1;
        let error = validate_request(&request, server_receive_ms).unwrap_err();
        assert!(format!("{error:#}").contains("predates supported epoch"));
    }

    #[test]
    fn invalid_timestamps_and_malformed_pmd_never_reach_wal_or_poison_another_tenant() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let attacker = db.ensure_admin("attacker@example.com", 1).unwrap();
        let valid_user = db.ensure_admin("valid@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();

        let mut negative = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        negative.records[0].collector_unix_ns = -1;
        assert!(ingest_fixture(&store, &attacker, negative).is_err());
        assert!(store.current_wal_path(attacker.id).is_none());

        let mut malformed = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        malformed.records[0].body = serde_json::to_value(WalBody::GattNotification(
            GattNotificationBody::from_payload(PMD_DATA_UUID, "pmd_data", &[0, 1, 2]).unwrap(),
        ))
        .unwrap();
        let error = ingest_fixture(&store, &attacker, malformed).unwrap_err();
        assert!(format!("{error:#}").contains("shorter than 10-byte header"));
        assert!(store.current_wal_path(attacker.id).is_none());

        let mut negative_local_time = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        negative_local_time.records[0].kind = WalRecordKind::PsftpSetLocalTimeAttempt;
        negative_local_time.records[0].body = serde_json::json!({
            "operation_id": Uuid::new_v4(),
            "local_time_iso8601": "invalid-negative-test",
            "local_time_unix_ns": -1,
            "tz_offset_minutes": 0,
            "dst_offset_minutes": null,
            "request_payload_b64": "",
            "control_characteristic_uuid": "fb005c51-02e7-f387-1cad-8acd2d8df0c8",
            "mtu_characteristic_uuid": "fb005c51-02e7-f387-1cad-8acd2d8df0c8"
        });
        let error = ingest_fixture(&store, &attacker, negative_local_time).unwrap_err();
        assert!(format!("{error:#}").contains("local_time_unix_ns"));
        assert!(store.current_wal_path(attacker.id).is_none());

        let accepted = ingest_fixture(
            &store,
            &valid_user,
            fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0),
        )
        .unwrap();
        assert_eq!(accepted.accepted_records, 1);
        let valid_path = store.current_wal_path(valid_user.id).unwrap();
        let read = read_wal_records(&valid_path, WalReadMode::Strict).unwrap();
        assert!(
            read.records
                .iter()
                .any(|frame| { matches!(frame.record.body, WalBody::CloudIngestBatchV1(_)) })
        );
    }

    #[test]
    fn out_of_order_batch_reports_gap_then_gap_fill_advances_contiguous_ack() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();
        let installation = Uuid::new_v4();
        let late = ingest_fixture(
            &store,
            &user,
            fixture_request(installation, Uuid::new_v4(), 1),
        )
        .unwrap();
        assert_eq!(late.durably_committed_through_seq, -1);
        assert_eq!(
            late.missing_ranges,
            vec![MissingRange {
                start_seq: 0,
                end_seq: 0
            }]
        );
        let filled = ingest_fixture(
            &store,
            &user,
            fixture_request(installation, Uuid::new_v4(), 0),
        )
        .unwrap();
        assert_eq!(filled.durably_committed_through_seq, 1);
        assert!(filled.missing_ranges.is_empty());
    }

    #[test]
    fn batch_id_content_mismatch_and_server_wal_kinds_are_rejected() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        ingest_fixture(&store, &user, request.clone()).unwrap();
        let mut changed = request;
        changed.records[0].collector_monotonic_ns += 1;
        assert!(ingest_fixture(&store, &user, changed).is_err());

        let mut forbidden = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        forbidden.records[0].kind = WalRecordKind::SegmentStarted;
        assert!(ingest_fixture(&store, &user, forbidden).is_err());
    }

    #[test]
    fn restart_recovers_fsynced_batch_when_sqlite_receipt_is_missing() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        {
            let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
            ingest_fixture(&store, &user, request.clone()).unwrap();
        }
        db.delete_batch_receipts(user.id, request.installation_id, request.batch_id)
            .unwrap();
        assert!(
            db.stored_batch(user.id, request.installation_id, request.batch_id)
                .unwrap()
                .is_none()
        );
        let restarted = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        assert!(
            db.stored_batch(user.id, request.installation_id, request.batch_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            ingest_fixture(&restarted, &user, request)
                .unwrap()
                .accepted_records,
            0
        );
    }

    #[test]
    fn sqlite_failure_after_atomic_wal_publish_retries_without_duplicate_records() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        db.fail_next_batch_commit();
        assert!(ingest_fixture(&store, &user, request.clone()).is_err());

        let retry = ingest_fixture(&store, &user, request.clone()).unwrap();
        assert_eq!(retry.accepted_records, 0);
        assert_eq!(retry.durably_committed_through_seq, 0);
        let mut paths = Vec::new();
        collect_wal_paths(&dir.0.join("wal"), &mut paths).unwrap();
        let occurrences = paths
            .iter()
            .flat_map(|path| read_wal_records(path, WalReadMode::Strict).unwrap().records)
            .flat_map(|frame| crate::wal::logical_wal_records(&frame.record).to_vec())
            .filter(|record| record.record_id == request.records[0].record_id)
            .count();
        assert_eq!(occurrences, 1);
    }

    #[test]
    fn ambiguous_fsync_result_recovers_before_retry_without_duplicate_envelope() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        let unrelated_corrupt = dir
            .0
            .join("wal/participant_id=corrupt/source=chest_h10/date=2026-08-04/unrelated.closed");
        fs::create_dir_all(unrelated_corrupt.parent().unwrap()).unwrap();
        fs::write(&unrelated_corrupt, b"unrelated corrupt WAL").unwrap();
        store.fail_after_successful_sync();
        assert!(ingest_fixture(&store, &user, request.clone()).is_err());
        assert_eq!(
            ingest_fixture(&store, &user, request.clone())
                .unwrap()
                .accepted_records,
            0
        );
        let mut paths = Vec::new();
        collect_wal_paths(&dir.0.join("wal"), &mut paths).unwrap();
        let envelopes = paths
            .iter()
            .filter(|path| {
                path.to_string_lossy()
                    .contains(&format!("participant_id={}", user.id))
            })
            .flat_map(|path| read_wal_records(path, WalReadMode::Strict).unwrap().records)
            .filter(|frame| {
                matches!(&frame.record.body, WalBody::CloudIngestBatchV1(body) if body.batch_id == request.batch_id)
            })
            .count();
        assert_eq!(envelopes, 1);
    }

    #[test]
    fn one_users_ambiguous_fsync_does_not_close_another_users_writer() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user_a = db.ensure_admin("a@example.com", 1).unwrap();
        let user_b = db.ensure_admin("b@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();
        let installation_b = Uuid::new_v4();
        ingest_fixture(
            &store,
            &user_b,
            fixture_request(installation_b, Uuid::new_v4(), 0),
        )
        .unwrap();
        let b_path = store.current_wal_path(user_b.id).unwrap();
        assert_eq!(
            b_path.extension().and_then(|value| value.to_str()),
            Some("open")
        );

        let request_a = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        store.fail_after_successful_sync();
        assert!(ingest_fixture(&store, &user_a, request_a).is_err());
        assert!(
            b_path.exists(),
            "other tenant open path must not be renamed"
        );
        assert_eq!(store.current_wal_path(user_b.id).as_ref(), Some(&b_path));

        ingest_fixture(
            &store,
            &user_b,
            fixture_request(installation_b, Uuid::new_v4(), 1),
        )
        .unwrap();
        let read = read_wal_records(&b_path, WalReadMode::Strict).unwrap();
        assert_eq!(
            read.records
                .iter()
                .filter(|frame| matches!(frame.record.body, WalBody::CloudIngestBatchV1(_)))
                .count(),
            2
        );
    }

    #[test]
    fn revocation_after_fsync_does_not_break_restart_reconciliation() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let installation = Uuid::new_v4();
        db.create_session(NewSession {
            user_id: user.id,
            installation_id: Some(installation),
            access_token: "installation-access",
            refresh_token: "installation-refresh",
            csrf_token: "installation-csrf",
            user_agent: None,
            now_ms: 1,
            access_expires_ms: i64::MAX,
            refresh_expires_ms: i64::MAX,
        })
        .unwrap();
        let request = fixture_request(installation, Uuid::new_v4(), 0);
        {
            let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
            db.fail_next_batch_commit();
            assert!(ingest_fixture(&store, &user, request.clone()).is_err());
            assert!(
                db.revoke_installation_for_user(installation, user.id, 2)
                    .unwrap()
            );
        }
        let restarted = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        assert!(
            db.stored_batch(user.id, installation, request.batch_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            ingest_fixture(&restarted, &user, request)
                .unwrap()
                .accepted_records,
            0
        );
    }

    #[test]
    fn unpublished_staging_is_invisible_and_removed_on_restart() {
        let dir = TestDir::new();
        let staging = dir
            .0
            .join(".ingest-staging")
            .join("attempt-crashed")
            .join("wal")
            .join("participant_id=test")
            .join("source=chest_h10");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("partial.open"), b"unpublished partial batch").unwrap();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        IngestStore::new(dir.0.clone(), db).unwrap();
        assert!(!dir.0.join(".ingest-staging/attempt-crashed").exists());
        let mut canonical = Vec::new();
        collect_wal_paths(&dir.0.join("wal"), &mut canonical).unwrap();
        assert!(canonical.is_empty());
    }

    #[test]
    fn torn_envelope_is_invisible_and_cannot_rebuild_receipts() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let request = fixture_request(Uuid::new_v4(), Uuid::new_v4(), 0);
        let path = {
            let store = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
            ingest_fixture(&store, &user, request.clone()).unwrap();
            store.current_wal_path(user.id).unwrap()
        };
        db.delete_batch_receipts(user.id, request.installation_id, request.batch_id)
            .unwrap();
        let length = fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 5)
            .unwrap();

        let restarted = IngestStore::new(dir.0.clone(), db.clone()).unwrap();
        assert_eq!(restarted.recover_receipts_from_wal().unwrap(), 0);
        assert!(
            db.stored_batch(user.id, request.installation_id, request.batch_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn many_batches_share_the_rotating_canonical_segment() {
        let dir = TestDir::new();
        let db = Arc::new(CloudDb::open(&dir.0.join("cloud.sqlite3")).unwrap());
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let store = IngestStore::new(dir.0.clone(), db).unwrap();
        let installation = Uuid::new_v4();
        for sequence in 0..100 {
            ingest_fixture(
                &store,
                &user,
                fixture_request(installation, Uuid::new_v4(), sequence),
            )
            .unwrap();
        }
        let mut paths = Vec::new();
        collect_wal_paths(&dir.0.join("wal"), &mut paths).unwrap();
        assert_eq!(paths.len(), 1, "batches must not create one file each");
        assert_eq!(
            paths[0].extension().and_then(|value| value.to_str()),
            Some("open")
        );
        let read = read_wal_records(&paths[0], WalReadMode::Strict).unwrap();
        assert_eq!(
            read.records
                .iter()
                .filter(|frame| matches!(frame.record.body, WalBody::CloudIngestBatchV1(_)))
                .count(),
            100
        );
    }
}
