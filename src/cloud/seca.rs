//! Immutable, fsync-before-ACK seca packet chunks. Independent from the H10 WAL.
use crate::seca;
use anyhow::{Context, Result, ensure};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};
use uuid::Uuid;

pub const MAX_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug)]
pub struct MissingRecording;
impl std::fmt::Display for MissingRecording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seca recording unavailable")
    }
}
impl std::error::Error for MissingRecording {}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WireRecord {
    pub unix_ns: i64,
    pub monotonic_ns: u64,
    pub direction: String,
    /// True only after rate/gain readback and a successful acquisition start.
    pub acquisition: bool,
    pub payload_b64: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub schema_version: u32,
    pub installation_id: Uuid,
    pub session_id: Uuid,
    pub batch_index: u64,
    pub device_id: String,
    pub sample_rate_hz: u32,
    pub gain: u8,
    pub transport: String,
    pub contact_detection: bool,
    pub end_reason: Option<String>,
    pub records: Vec<WireRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Summary {
    pub user_id: Uuid,
    pub installation_id: Uuid,
    pub session_id: Uuid,
    pub device_id: String,
    pub sample_rate_hz: u32,
    pub gain: u8,
    pub transport: String,
    pub contact_detection: bool,
    pub last_batch_index: u64,
    pub started_unix_ms: i64,
    pub received_unix_ms: i64,
    pub last_capture_unix_ns: i64,
    pub last_ecg_capture_unix_ns: Option<i64>,
    pub last_capture_monotonic_ns: Option<u64>,
    pub packet_count: u64,
    pub sample_count: u64,
    pub missing_blocks: u64,
    pub sequence_errors: u64,
    pub decode_errors: u64,
    pub last_error: Option<String>,
    pub last_counter: Option<u32>,
    pub battery_percent: Option<u8>,
    pub charging: Option<bool>,
    pub end_reason: Option<String>,
}

#[derive(Serialize)]
pub struct Ack {
    pub session_id: Uuid,
    pub batch_index: u64,
    pub sha256: String,
}

pub struct Store {
    root: PathBuf,
    lock: Mutex<()>,
}

fn mkdir_durable(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        mkdir_durable(parent)?;
    }
    if !path.is_dir() {
        fs::create_dir(path)
            .with_context(|| format!("create seca archive directory {}", path.display()))?;
    }
    // Repeat even when the directory exists: a previous failed fsync may have
    // left a visible but not crash-durable directory entry.
    if let Some(parent) = path.parent() {
        File::open(if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        })?
        .sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! { static FAIL_SYNC_AFTER: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn sync_committed(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_SYNC_AFTER.with(|counter| {
        let left = counter.get();
        if left > 0 {
            counter.set(left - 1);
        }
        left == 1
    }) {
        return Err(std::io::Error::other("injected archive fsync failure").into());
    }
    File::open(path)?.sync_all()?;
    File::open(path.parent().context("seca archive parent missing")?)?.sync_all()?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        sync_committed(path)?;
        Ok(())
    })();
    if result.is_err()
        && temp.exists()
        && let Err(error) = fs::remove_file(&temp)
    {
        eprintln!("seca temporary file cleanup failed: {error}");
    }
    result.context("durably write seca archive")
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl Store {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let root = data_dir.join("seca");
        mkdir_durable(&root)?;
        Ok(Self {
            root,
            lock: Mutex::new(()),
        })
    }

    fn directory(&self, user: Uuid, session: Uuid) -> PathBuf {
        self.root.join(user.to_string()).join(session.to_string())
    }

    fn summary(&self, user: Uuid, session: Uuid) -> Result<Option<Summary>> {
        read_optional(&self.directory(user, session).join("summary.json"))?
            .map(|b| serde_json::from_slice(&b).context("read seca session summary"))
            .transpose()
    }

    pub fn ingest(&self, user: Uuid, installation: Uuid, batch: Batch, now_ms: i64) -> Result<Ack> {
        ensure!(batch.schema_version == 1, "unsupported seca schema");
        ensure!(
            batch.installation_id == installation,
            "seca installation mismatch"
        );
        ensure!(
            batch.batch_index < 1_000_000_000,
            "seca batch index out of range"
        );
        ensure!(
            batch.device_id.len() <= 96 && !batch.device_id.is_empty(),
            "invalid seca identity"
        );
        ensure!(
            batch.end_reason.as_ref().is_none_or(|s| s.len() <= 240),
            "seca end reason too long"
        );
        ensure!(batch.gain == 6, "seca calibration requires gain 6");
        seca::nano_volts_per_count(batch.sample_rate_hz)?;
        ensure!(
            ["bluetooth", "usb"].contains(&batch.transport.as_str()),
            "invalid seca transport"
        );
        ensure!(
            batch.transport == "usb" || batch.sample_rate_hz <= 8000,
            "Bluetooth rate exceeds 8 kHz"
        );
        ensure!(
            batch.records.len() <= 2048
                && (!batch.records.is_empty() || batch.end_reason.is_some()),
            "invalid seca record count"
        );
        let encoded = serde_json::to_vec(&batch)?;
        ensure!(encoded.len() <= MAX_BYTES, "seca chunk exceeds byte limit");
        let checksum = hex::encode(Sha256::digest(&encoded));
        let ack = Ack {
            session_id: batch.session_id,
            batch_index: batch.batch_index,
            sha256: checksum,
        };
        let mut previous_clock = None;
        for record in &batch.records {
            ensure!(
                ["rx", "tx"].contains(&record.direction.as_str()),
                "invalid seca direction"
            );
            ensure!(
                record.unix_ns >= 946_684_800_000_000_000
                    && record.unix_ns / 1_000_000 <= now_ms + 300_000,
                "invalid seca receive time"
            );
            ensure!(
                previous_clock.is_none_or(|t| record.monotonic_ns >= t),
                "seca monotonic clock regressed"
            );
            previous_clock = Some(record.monotonic_ns);
            let raw = base64::engine::general_purpose::STANDARD
                .decode(&record.payload_b64)
                .context("invalid seca base64")?;
            ensure!(
                !raw.is_empty() && raw.len() <= 65535,
                "invalid seca raw packet size"
            );
        }
        let _guard = self
            .lock
            .lock()
            .map_err(|_| std::io::Error::other("seca archive lock poisoned"))?;
        let directory = self.directory(user, batch.session_id);
        let old = self.summary(user, batch.session_id)?;
        if let Some(old) = &old {
            ensure!(
                old.installation_id == installation
                    && old.device_id == batch.device_id
                    && old.sample_rate_hz == batch.sample_rate_hz
                    && old.gain == batch.gain
                    && old.transport == batch.transport
                    && old.contact_detection == batch.contact_detection,
                "seca session metadata changed"
            );
        }
        let chunk = directory.join(format!("{:010}.json", batch.batch_index));
        let existing = read_optional(&chunk)?;
        if let Some(existing) = &existing {
            ensure!(*existing == encoded, "conflicting seca chunk retry");
            if old
                .as_ref()
                .is_some_and(|s| s.last_batch_index >= batch.batch_index)
            {
                // A prior request may have renamed summary.json but failed its
                // directory fsync. Seeing matching bytes is not durability proof.
                mkdir_durable(&directory)?;
                sync_committed(&chunk)?;
                sync_committed(&directory.join("summary.json"))?;
                return Ok(ack);
            }
        }
        if let Some(last) = old.as_ref().and_then(|s| s.last_capture_monotonic_ns) {
            ensure!(
                batch.records.first().is_none_or(|r| r.monotonic_ns >= last),
                "seca monotonic clock regressed across chunks"
            );
        }
        let expected = old.as_ref().map_or(0, |s| s.last_batch_index + 1);
        ensure!(
            batch.batch_index == expected,
            "seca chunk sequence gap: expected {expected}"
        );
        ensure!(
            old.as_ref().is_none_or(|s| s.end_reason.is_none()),
            "seca session already ended"
        );
        let mut summary = old.unwrap_or(Summary {
            user_id: user,
            installation_id: installation,
            session_id: batch.session_id,
            device_id: batch.device_id.clone(),
            sample_rate_hz: batch.sample_rate_hz,
            gain: batch.gain,
            transport: batch.transport.clone(),
            contact_detection: batch.contact_detection,
            last_batch_index: 0,
            started_unix_ms: batch
                .records
                .first()
                .map_or(now_ms, |r| r.unix_ns / 1_000_000),
            received_unix_ms: now_ms,
            last_capture_unix_ns: 0,
            last_ecg_capture_unix_ns: None,
            last_capture_monotonic_ns: None,
            packet_count: 0,
            sample_count: 0,
            missing_blocks: 0,
            sequence_errors: 0,
            decode_errors: 0,
            last_error: None,
            last_counter: None,
            battery_percent: None,
            charging: None,
            end_reason: None,
        });
        for record in batch.records.iter().filter(|r| r.direction == "rx") {
            // Decode at most one packet at a time; raw archival is independent of decoding.
            let raw = base64::engine::general_purpose::STANDARD.decode(&record.payload_b64)?;
            match seca::decode(&raw) {
                Ok(packet) => {
                    if record.acquisition && !packet.blocks.is_empty() {
                        summary.last_ecg_capture_unix_ns = Some(record.unix_ns);
                    }
                    if packet.battery_percent.is_some() {
                        summary.battery_percent = packet.battery_percent;
                        summary.charging = packet.charging;
                    }
                    if !record.acquisition {
                        continue;
                    }
                    for block in packet.blocks {
                        if let Some(last) = summary.last_counter {
                            let advance = block.counter.wrapping_sub(last);
                            if advance == 0 || advance > 0x8000_0000 {
                                summary.sequence_errors += 1;
                                summary.last_error =
                                    Some("device block counter repeated or regressed".into());
                            } else {
                                summary.missing_blocks += u64::from(advance - 1);
                                summary.last_counter = Some(block.counter);
                            }
                        } else {
                            summary.last_counter = Some(block.counter);
                        }
                        summary.sample_count += 32;
                    }
                }
                Err(error) => {
                    summary.decode_errors += 1;
                    summary.last_error = Some(error.to_string());
                }
            }
        }
        summary.last_batch_index = batch.batch_index;
        summary.received_unix_ms = now_ms;
        if let Some(record) = batch.records.last() {
            summary.last_capture_unix_ns = record.unix_ns;
            summary.last_capture_monotonic_ns = Some(record.monotonic_ns);
        }
        summary.packet_count += batch.records.len() as u64;
        summary.end_reason = batch.end_reason;
        mkdir_durable(&directory)?;
        if existing.is_none() {
            atomic_write(&chunk, &encoded)?;
        }
        // If a crash occurs between the two renames, the unacknowledged chunk's
        // exact retry reconstructs this summary. Previously ACKed chunks remain valid.
        atomic_write(
            &directory.join("summary.json"),
            &serde_json::to_vec(&summary)?,
        )?;
        Ok(ack)
    }

    pub fn sessions(&self) -> Result<Vec<Summary>> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| std::io::Error::other("seca archive lock poisoned"))?;
        let mut result = Vec::new();
        for user in fs::read_dir(&self.root)? {
            let user = user?;
            let Ok(user_id) = Uuid::parse_str(&user.file_name().to_string_lossy()) else {
                continue;
            };
            for entry in fs::read_dir(user.path())? {
                let entry = entry?;
                let Ok(session) = Uuid::parse_str(&entry.file_name().to_string_lossy()) else {
                    continue;
                };
                if let Some(summary) = self.summary(user_id, session)? {
                    result.push(summary);
                }
            }
        }
        result.sort_by_key(|s| std::cmp::Reverse(s.started_unix_ms));
        result.truncate(500);
        Ok(result)
    }

    pub fn chunk(&self, user: Uuid, session: Uuid, index: u64) -> Result<Vec<u8>> {
        ensure!(index < 1_000_000_000, "seca batch index out of range");
        read_optional(
            &self
                .directory(user, session)
                .join(format!("{index:010}.json")),
        )?
        .ok_or_else(|| MissingRecording.into())
    }

    pub fn snapshot(
        &self,
        user: Uuid,
        session: Uuid,
        end: Option<u64>,
    ) -> Result<serde_json::Value> {
        let summary = self.summary(user, session)?.ok_or(MissingRecording)?;
        let end = end
            .unwrap_or(summary.last_batch_index)
            .min(summary.last_batch_index);
        let scale = f64::from(seca::nano_volts_per_count(summary.sample_rate_hz)?) / 1000.0;
        let stride = (summary.sample_rate_hz / 500).clamp(1, 32) as usize;
        let mut rows = std::collections::VecDeque::new();
        let mut truncated = false;
        let mut gaps = 0u64;
        let mut errors = Vec::new();
        'chunks: for index in (end.saturating_sub(2)..=end).rev() {
            let batch: Batch = serde_json::from_slice(&self.chunk(user, session, index)?)?;
            for record in batch
                .records
                .iter()
                .rev()
                .filter(|r| r.direction == "rx" && r.acquisition)
            {
                let raw = base64::engine::general_purpose::STANDARD.decode(&record.payload_b64)?;
                match seca::decode(&raw) {
                    Ok(packet) => {
                        for mut block in packet.blocks.into_iter().rev() {
                            if !summary.contact_detection {
                                block.lead_off = None;
                            }
                            for start in (0..32).step_by(stride).rev() {
                                if rows.len() == 6000 {
                                    truncated = true;
                                    break 'chunks;
                                }
                                let mut low: [Option<f64>; 12] = [None; 12];
                                let mut high: [Option<f64>; 12] = [None; 12];
                                for sample in start..(start + stride).min(32) {
                                    for (lead, value) in
                                        seca::lead_sample(&block, sample).iter().enumerate()
                                    {
                                        if let Some(v) = value {
                                            let v = v * scale;
                                            low[lead] = Some(low[lead].map_or(v, |old| old.min(v)));
                                            high[lead] =
                                                Some(high[lead].map_or(v, |old| old.max(v)));
                                        }
                                    }
                                }
                                rows.push_front(serde_json::json!({"sample":u64::from(block.counter)*32+start as u64,"low":low,"high":high,"gap":false}));
                            }
                        }
                    }
                    Err(error) => {
                        if errors.len() < 8 {
                            errors.push(error.to_string());
                        }
                    }
                }
            }
        }
        let mut previous_sample = None;
        for row in &mut rows {
            let sample = row["sample"].as_u64().context("invalid preview sample")?;
            if previous_sample.is_some_and(|last: u64| sample != last + stride as u64) {
                row["gap"] = serde_json::json!(true);
                gaps += 1;
            }
            previous_sample = Some(sample);
        }
        Ok(
            serde_json::json!({"summary":summary,"end_batch_index":end,"leads":seca::LEADS,
            "preview":"min/max envelope; raw archive retains every received byte", "stride":stride,"truncated":truncated,"gap_count":gaps,"rows":rows,"errors":errors}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (Store, PathBuf, Uuid, Uuid, Batch) {
        let root = std::env::temp_dir().join(format!("seca-test-{}", Uuid::new_v4()));
        let user = Uuid::new_v4();
        let installation = Uuid::new_v4();
        let packet = crate::seca::tests::frame(&crate::seca::tests::group(8), 1);
        let batch = Batch {
            schema_version: 1,
            installation_id: installation,
            session_id: Uuid::new_v4(),
            batch_index: 0,
            device_id: "test-device".into(),
            sample_rate_hz: 4000,
            gain: 6,
            transport: "bluetooth".into(),
            contact_detection: false,
            end_reason: None,
            records: vec![WireRecord {
                unix_ns: 1_700_000_000_000_000_000,
                monotonic_ns: 1,
                direction: "rx".into(),
                acquisition: true,
                payload_b64: base64::engine::general_purpose::STANDARD.encode(packet),
            }],
        };
        (Store::new(&root).unwrap(), root, user, installation, batch)
    }
    #[test]
    fn durable_retry_conflict_restart_and_orphan_recovery() {
        let (store, root, user, installation, batch) = setup();
        let now = 1_700_000_000_000;
        let ack = store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        assert_eq!(
            store
                .ingest(user, installation, batch.clone(), now + 1)
                .unwrap()
                .sha256,
            ack.sha256
        );
        let mut conflict = batch.clone();
        conflict.records[0].monotonic_ns = 2;
        assert!(store.ingest(user, installation, conflict, now).is_err());
        assert!(store.chunk(Uuid::new_v4(), batch.session_id, 0).is_err());
        let snapshot = store.snapshot(user, batch.session_id, None).unwrap();
        assert_eq!(snapshot["rows"].as_array().unwrap().len(), 4);
        assert_eq!(snapshot["summary"]["sample_count"], 32);
        assert_eq!(store.sessions().unwrap().len(), 1);
        fs::remove_file(store.directory(user, batch.session_id).join("summary.json")).unwrap();
        drop(store);
        let store = Store::new(&root).unwrap();
        store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        let mut next = batch.clone();
        next.batch_index = 1;
        next.end_reason = Some("stopped".into());
        next.records.clear();
        store.ingest(user, installation, next.clone(), now).unwrap();
        store.ingest(user, installation, next.clone(), now).unwrap();
        next.batch_index = 2;
        assert!(store.ingest(user, installation, next, now).is_err());
        assert_eq!(
            store.snapshot(user, batch.session_id, Some(0)).unwrap()["end_batch_index"],
            0
        );
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn unconfirmed_acquisition_is_retained_without_sample_or_counter_attribution() {
        let (store, root, user, installation, mut batch) = setup();
        batch.records[0].acquisition = false;
        let now = 1_700_000_000_000;
        let mut missing = serde_json::to_value(&batch).unwrap();
        missing["records"][0]
            .as_object_mut()
            .unwrap()
            .remove("acquisition");
        assert!(serde_json::from_value::<Batch>(missing).is_err());
        store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        let preview = store.snapshot(user, batch.session_id, None).unwrap();
        assert_eq!(preview["summary"]["sample_count"], 0);
        assert_eq!(
            preview["summary"]["last_ecg_capture_unix_ns"],
            serde_json::Value::Null
        );
        assert_eq!(preview["summary"]["last_counter"], serde_json::Value::Null);
        assert!(preview["rows"].as_array().unwrap().is_empty());
        let archived: Batch =
            serde_json::from_slice(&store.chunk(user, batch.session_id, 0).unwrap()).unwrap();
        assert_eq!(
            archived.records[0].payload_b64,
            batch.records[0].payload_b64
        );
        batch.batch_index = 1;
        batch.records[0].acquisition = true;
        store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        assert_eq!(
            store.snapshot(user, batch.session_id, None).unwrap()["summary"]["sample_count"],
            32
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn status_traffic_does_not_refresh_ecg_capture_time() {
        let (store, root, user, installation, batch) = setup();
        let now = 1_700_000_000_000;
        store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        let mut status = batch.clone();
        status.batch_index = 1;
        status.records[0].unix_ns += 1_000_000_000;
        status.records[0].payload_b64 =
            base64::engine::general_purpose::STANDARD.encode(crate::seca::tests::frame(&[], 1));
        store
            .ingest(user, installation, status, now + 1000)
            .unwrap();
        let summary = store.snapshot(user, batch.session_id, None).unwrap()["summary"].clone();
        assert_eq!(
            summary["last_ecg_capture_unix_ns"],
            batch.records[0].unix_ns
        );
        assert_eq!(
            summary["last_capture_unix_ns"],
            batch.records[0].unix_ns + 1_000_000_000
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retry_reestablishes_durability_after_summary_rename_sync_failure() {
        let (store, root, user, installation, batch) = setup();
        let now = 1_700_000_000_000;
        FAIL_SYNC_AFTER.with(|counter| counter.set(2));
        assert!(
            store
                .ingest(user, installation, batch.clone(), now)
                .is_err()
        );
        assert!(
            store
                .directory(user, batch.session_id)
                .join("summary.json")
                .exists()
        );
        // A visible matching summary is insufficient: duplicate ACK still needs
        // durable synchronization, and must fail when that operation fails.
        FAIL_SYNC_AFTER.with(|counter| counter.set(1));
        assert!(
            store
                .ingest(user, installation, batch.clone(), now)
                .is_err()
        );
        store
            .ingest(user, installation, batch.clone(), now)
            .unwrap();
        assert_eq!(
            store.snapshot(user, batch.session_id, None).unwrap()["summary"]["sample_count"],
            32
        );
        let mut next = batch;
        next.batch_index = 1;
        next.records.clear();
        next.end_reason = Some("stopped".into());
        store.ingest(user, installation, next, now).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn preview_bounds_latest_rows_and_exposes_counter_gaps_for_every_rate() {
        for rate in seca::RATES {
            let (store, root, user, installation, mut batch) = setup();
            batch.sample_rate_hz = rate;
            batch.transport = "usb".into();
            let group = crate::seca::tests::group(0);
            // Many maximally compressed blocks exceed the preview budget without
            // bloating raw storage. Tail-first decoding must preserve the latest block.
            batch.records.clear();
            for counter in 1..=1600 {
                let packet = crate::seca::tests::frame(&group, counter * 2);
                batch.records.push(WireRecord {
                    unix_ns: 1_700_000_000_000_000_000,
                    monotonic_ns: u64::from(counter),
                    direction: "rx".into(),
                    acquisition: true,
                    payload_b64: base64::engine::general_purpose::STANDARD.encode(packet),
                });
            }
            store
                .ingest(user, installation, batch.clone(), 1_700_000_000_000)
                .unwrap();
            let snapshot = store.snapshot(user, batch.session_id, None).unwrap();
            let rows = snapshot["rows"].as_array().unwrap();
            assert!(rows.len() <= 6000);
            assert!(rows.last().unwrap()["sample"].as_u64().unwrap() >= 3200 * 32);
            assert!(snapshot["gap_count"].as_u64().unwrap() > 0);
            assert_eq!(snapshot["summary"]["missing_blocks"], 1599);
            if rate <= 4000 {
                assert_eq!(snapshot["truncated"], true);
            }
            let mut wrong_gain = batch;
            wrong_gain.gain = 5;
            assert!(
                store
                    .ingest(user, installation, wrong_gain, 1_700_000_000_000)
                    .is_err()
            );
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn validates_metadata_limits_and_preserves_corrupt_raw_data() {
        let (store, root, user, installation, batch) = setup();
        let now = 1_700_000_000_000;
        assert!(
            store
                .ingest(user, Uuid::new_v4(), batch.clone(), now)
                .is_err()
        );
        for variant in 0..10 {
            let mut b = batch.clone();
            match variant {
                0 => b.schema_version = 2,
                1 => b.batch_index = 1,
                2 => b.sample_rate_hz = 32000,
                3 => b.device_id.clear(),
                4 => b.records.clear(),
                5 => b.records[0].direction = "?".into(),
                6 => b.records[0].unix_ns = 0,
                7 => b.records[0].payload_b64 = "!".into(),
                8 => b.end_reason = Some("x".repeat(241)),
                _ => b.records[0].payload_b64.clear(),
            }
            assert!(
                store.ingest(user, installation, b, now).is_err(),
                "variant {variant}"
            );
        }
        let mut b = batch.clone();
        b.records[0].payload_b64 = "AQ==".into();
        store.ingest(user, installation, b.clone(), now).unwrap();
        let s = store.snapshot(user, b.session_id, None).unwrap();
        assert_eq!(s["summary"]["decode_errors"], 1);
        assert_eq!(s["errors"].as_array().unwrap().len(), 1);
        let mut next = batch;
        next.batch_index = 1;
        next.sample_rate_hz = 2000;
        assert!(store.ingest(user, installation, next, now).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
