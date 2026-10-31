use crate::config::{BATTERY_LEVEL_UUID, HR_MEASUREMENT_UUID, PMD_CONTROL_UUID, PMD_DATA_UUID};
use crate::hr::{HrData, handle_hr};
use crate::pmd::{PmdParser, measurement_name};
use crate::process_lock::ArchiverProcessLock;
use crate::records::{
    AccSample, BatterySample, CollectorEvent, ConnectionHealthSample, EcgSample, HrFrame,
    PmdFrameTiming, PmdTimingRecord, RECORD_SCHEMA_VERSION, RawGattRecord, RecordMeta,
    RecordStream, RrInterval, SensorRecord,
};
use crate::sink::RecordSink;
use crate::wal::{
    WAL_CHECKSUM_CRC32C, WAL_CONTAINER_MAJOR, WAL_CONTAINER_MINOR,
    WAL_PAYLOAD_CODEC_CANONICAL_JSON_V1, WalBody, WalReadMode, WalReadResult, WalRecord,
    format_date_partition, format_wal_timestamp_ns, read_wal_records,
};
use anyhow::{Context, Result, anyhow, bail};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// Archive schema v2 adds non-null `severity` and `fields_json` columns to the events stream.
/// Raw WAL payload schema remains v1; this version describes the derived Parquet schema family.
const ARCHIVE_SCHEMA_VERSION: u16 = 2;

fn write_parquet_tmp_then_rename(
    tmp_path: &Path,
    final_path: &Path,
    batch: &RecordBatch,
) -> Result<()> {
    if final_path.exists() {
        bail!("final parquet already exists: {}", final_path.display());
    }
    if tmp_path.exists() {
        fs::remove_file(tmp_path)
            .with_context(|| format!("remove stale tmp {}", tmp_path.display()))?;
    }
    write_parquet_file(tmp_path, batch)?;
    fs::rename(tmp_path, final_path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), final_path.display()))?;
    if let Some(parent) = final_path.parent() {
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("fsync parquet dir {}", parent.display()))?;
    }
    Ok(())
}

fn write_parquet_file(path: &Path, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create parquet {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("fsync parquet {}", path.display()))?;
    Ok(())
}

fn sha256_file_hex(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read for checksum {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn record_batch_for_stream(stream: RecordStream, records: &[SensorRecord]) -> Result<RecordBatch> {
    match stream {
        RecordStream::Ecg => ecg_batch(records),
        RecordStream::Acc => acc_batch(records),
        RecordStream::Hr => hr_batch(records),
        RecordStream::Rr => rr_batch(records),
        RecordStream::Battery => battery_batch(records),
        RecordStream::Health => health_batch(records),
        RecordStream::Events => event_batch(records),
        RecordStream::PmdTiming => pmd_timing_batch(records),
        RecordStream::RawGatt => raw_gatt_batch(records),
    }
}

fn common_schema_fields() -> Vec<Field> {
    vec![
        Field::new("schema_version", DataType::UInt16, false),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("connection_id", DataType::UInt32, false),
        Field::new("collector_unix_ns", DataType::Int64, false),
        Field::new("collector_monotonic_ns", DataType::Int64, false),
    ]
}

fn common_arrays(records: &[SensorRecord]) -> Vec<ArrayRef> {
    vec![
        Arc::new(UInt16Array::from_iter_values(
            records.iter().map(|r| r.schema_version()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|r| r.sensor_id()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|r| r.session_id()),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            records.iter().map(|r| r.meta().connection_id),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            records.iter().map(|r| ns_to_i64(r.collector_unix_ns())),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            records
                .iter()
                .map(|r| ns_to_i64(r.meta().collector_monotonic_ns)),
        )) as ArrayRef,
    ]
}

fn ns_to_i64(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

fn ecg_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let ecg = records
        .iter()
        .map(|record| match record {
            SensorRecord::Ecg(value) => Ok(value),
            _ => Err(anyhow!("non-ECG record in ECG batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("device_time_ns", DataType::Int64, false),
        Field::new("device_time_epoch", DataType::Utf8, false),
        Field::new("sample_index_in_frame", DataType::UInt32, false),
        Field::new("ecg_uv", DataType::Int32, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(Int64Array::from_iter_values(
            ecg.iter().map(|r| r.device_time_ns as i64),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            ecg.iter().map(|r| r.device_time_epoch.as_str()),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            ecg.iter().map(|r| r.sample_index_in_frame),
        )) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(ecg.iter().map(|r| r.ecg_uv))) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn acc_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let acc = records
        .iter()
        .map(|record| match record {
            SensorRecord::Acc(value) => Ok(value),
            _ => Err(anyhow!("non-ACC record in ACC batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("device_time_ns", DataType::Int64, false),
        Field::new("device_time_epoch", DataType::Utf8, false),
        Field::new("sample_index_in_frame", DataType::UInt32, false),
        Field::new("x_mg", DataType::Int32, false),
        Field::new("y_mg", DataType::Int32, false),
        Field::new("z_mg", DataType::Int32, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(Int64Array::from_iter_values(
            acc.iter().map(|r| r.device_time_ns as i64),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            acc.iter().map(|r| r.device_time_epoch.as_str()),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            acc.iter().map(|r| r.sample_index_in_frame),
        )) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(acc.iter().map(|r| r.x_mg))) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(acc.iter().map(|r| r.y_mg))) as ArrayRef,
        Arc::new(Int32Array::from_iter_values(acc.iter().map(|r| r.z_mg))) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn hr_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let hr = records
        .iter()
        .map(|record| match record {
            SensorRecord::Hr(value) => Ok(value),
            _ => Err(anyhow!("non-HR record in HR batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("hr_bpm", DataType::UInt16, false),
        Field::new("sensor_contact_supported", DataType::Boolean, false),
        Field::new("sensor_contact_detected", DataType::Boolean, false),
        Field::new("energy_expended", DataType::UInt16, true),
        Field::new("rr_intervals_ms", DataType::Utf8, false),
        Field::new("raw_payload", DataType::Binary, false),
    ]);
    let rr_strings = hr
        .iter()
        .map(|r| {
            r.rr_intervals_ms
                .iter()
                .map(|v| format!("{v:.3}"))
                .collect::<Vec<_>>()
                .join(";")
        })
        .collect::<Vec<_>>();
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(UInt16Array::from_iter_values(hr.iter().map(|r| r.hr_bpm))) as ArrayRef,
        Arc::new(BooleanArray::from_iter(
            hr.iter().map(|r| Some(r.sensor_contact_supported)),
        )) as ArrayRef,
        Arc::new(BooleanArray::from_iter(
            hr.iter().map(|r| Some(r.sensor_contact_detected)),
        )) as ArrayRef,
        Arc::new(UInt16Array::from_iter(hr.iter().map(|r| r.energy_expended))) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            rr_strings.iter().map(String::as_str),
        )) as ArrayRef,
        Arc::new(BinaryArray::from_iter_values(
            hr.iter().map(|r| r.raw_payload.as_slice()),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn rr_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let rr = records
        .iter()
        .map(|record| match record {
            SensorRecord::Rr(value) => Ok(value),
            _ => Err(anyhow!("non-RR record in RR batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("rr_index_in_frame", DataType::UInt32, false),
        Field::new("rr_ms", DataType::Float64, false),
        Field::new("hr_frame_collector_unix_ns", DataType::Int64, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(UInt32Array::from_iter_values(
            rr.iter().map(|r| r.rr_index_in_frame),
        )) as ArrayRef,
        Arc::new(Float64Array::from_iter_values(rr.iter().map(|r| r.rr_ms))) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            rr.iter().map(|r| ns_to_i64(r.hr_frame_collector_unix_ns)),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn battery_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let battery = records
        .iter()
        .map(|record| match record {
            SensorRecord::Battery(value) => Ok(value),
            _ => Err(anyhow!("non-battery record in battery batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("battery_percent", DataType::UInt16, false),
        Field::new("reason", DataType::Utf8, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(UInt16Array::from_iter_values(
            battery.iter().map(|r| r.battery_percent as u16),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            battery.iter().map(|r| r.reason.as_str()),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn health_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let health = records
        .iter()
        .map(|record| match record {
            SensorRecord::Health(value) => Ok(value),
            _ => Err(anyhow!("non-health record in health batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("rssi_dbm", DataType::Int16, true),
        Field::new("pmd_frames", DataType::UInt64, false),
        Field::new("hr_frames", DataType::UInt64, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(Int16Array::from_iter(health.iter().map(|r| r.rssi_dbm))) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            health.iter().map(|r| r.pmd_frames),
        )) as ArrayRef,
        Arc::new(UInt64Array::from_iter_values(
            health.iter().map(|r| r.hr_frames),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn event_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let events = records
        .iter()
        .map(|record| match record {
            SensorRecord::Event(value) => Ok(value),
            _ => Err(anyhow!("non-event record in event batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("event", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("detail", DataType::Utf8, false),
        Field::new("fields_json", DataType::Utf8, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|r| r.event.as_str()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|r| r.severity.as_str()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|r| r.detail.as_str()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|r| r.fields_json.as_str()),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn pmd_timing_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let timing = records
        .iter()
        .map(|record| match record {
            SensorRecord::PmdTiming(value) => Ok(value),
            _ => Err(anyhow!("non-PMD-timing record in PMD timing batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("measurement", DataType::Utf8, false),
        Field::new("measurement_type", DataType::UInt16, false),
        Field::new("frame_type", DataType::UInt16, false),
        Field::new("compressed", DataType::Boolean, false),
        Field::new("device_frame_ts_ns", DataType::Int64, false),
        Field::new("payload_len", DataType::UInt32, false),
        Field::new("sample_count", DataType::UInt32, false),
        Field::new("collector_delta_ms", DataType::Float64, true),
        Field::new("device_delta_ms", DataType::Float64, true),
        Field::new("expected_frame_delta_ms", DataType::Float64, false),
        Field::new("gap_ms", DataType::Float64, true),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(StringArray::from_iter_values(
            timing.iter().map(|r| r.measurement.as_str()),
        )) as ArrayRef,
        Arc::new(UInt16Array::from_iter_values(
            timing.iter().map(|r| r.measurement_type as u16),
        )) as ArrayRef,
        Arc::new(UInt16Array::from_iter_values(
            timing.iter().map(|r| r.frame_type as u16),
        )) as ArrayRef,
        Arc::new(BooleanArray::from_iter(
            timing.iter().map(|r| Some(r.compressed)),
        )) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(
            timing.iter().map(|r| r.device_frame_ts_ns as i64),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            timing.iter().map(|r| r.payload_len),
        )) as ArrayRef,
        Arc::new(UInt32Array::from_iter_values(
            timing.iter().map(|r| r.sample_count),
        )) as ArrayRef,
        Arc::new(Float64Array::from_iter(
            timing.iter().map(|r| r.collector_delta_ms),
        )) as ArrayRef,
        Arc::new(Float64Array::from_iter(
            timing.iter().map(|r| r.device_delta_ms),
        )) as ArrayRef,
        Arc::new(Float64Array::from_iter_values(
            timing.iter().map(|r| r.expected_frame_delta_ms),
        )) as ArrayRef,
        Arc::new(Float64Array::from_iter(timing.iter().map(|r| r.gap_ms))) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

fn raw_gatt_batch(records: &[SensorRecord]) -> Result<RecordBatch> {
    let raw = records
        .iter()
        .map(|record| match record {
            SensorRecord::RawGatt(value) => Ok(value),
            _ => Err(anyhow!("non-raw-GATT record in raw GATT batch")),
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = common_schema_fields();
    fields.extend([
        Field::new("characteristic_uuid", DataType::Utf8, false),
        Field::new("direction", DataType::Utf8, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("detail", DataType::Utf8, false),
    ]);
    let mut arrays = common_arrays(records);
    arrays.extend([
        Arc::new(StringArray::from_iter_values(
            raw.iter().map(|r| r.characteristic_uuid.as_str()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            raw.iter().map(|r| r.direction.as_str()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            raw.iter().map(|r| r.operation.as_str()),
        )) as ArrayRef,
        Arc::new(BinaryArray::from_iter_values(
            raw.iter().map(|r| r.payload.as_slice()),
        )) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            raw.iter().map(|r| r.detail.as_str()),
        )) as ArrayRef,
    ]);
    Ok(RecordBatch::try_new(schema(fields), arrays)?)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalArchiverSummary {
    pub scanned_segments: usize,
    pub archived_segments: usize,
    pub skipped_segments: usize,
    pub output_files: usize,
    pub stale_tmp_files_removed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SegmentArchiveOutcome {
    Archived { output_files: usize },
    Skipped,
}

pub fn run_wal_archiver(data_dir: &Path) -> Result<WalArchiverSummary> {
    let lock = ArchiverProcessLock::acquire(data_dir)?;
    let _lock_path = lock.path();
    run_wal_archiver_locked(data_dir)
}

fn run_wal_archiver_locked(data_dir: &Path) -> Result<WalArchiverSummary> {
    let parquet_root = wal_archive_parquet_root(data_dir);
    let stale_tmp_files_removed = cleanup_stale_parquet_tmp_files(&parquet_root)?;
    let closed_segments = find_all_closed_wal_segments(data_dir)?;
    let mut summary = WalArchiverSummary {
        scanned_segments: closed_segments.len(),
        stale_tmp_files_removed,
        ..WalArchiverSummary::default()
    };

    let mut errors = Vec::new();
    for wal_path in closed_segments {
        match archive_closed_wal_segment(data_dir, &wal_path)
            .with_context(|| format!("archive closed WAL {}", wal_path.display()))
        {
            Ok(SegmentArchiveOutcome::Archived { output_files }) => {
                summary.archived_segments += 1;
                summary.output_files += output_files;
            }
            Ok(SegmentArchiveOutcome::Skipped) => {
                summary.skipped_segments += 1;
            }
            Err(error) => errors.push(format!("{error:#}")),
        }
    }
    if !errors.is_empty() {
        bail!(
            "{} WAL segment(s) failed while other participants continued: {}",
            errors.len(),
            errors.join(" | ")
        );
    }
    Ok(summary)
}

fn wal_archive_parquet_root(data_dir: &Path) -> PathBuf {
    data_dir.join("archive").join("parquet")
}

fn find_all_closed_wal_segments(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let wal_root = data_dir.join("wal");
    if !wal_root.exists() {
        return Ok(Vec::new());
    }

    let mut closed = Vec::new();
    for participant_entry in
        fs::read_dir(&wal_root).with_context(|| format!("scan WAL root {}", wal_root.display()))?
    {
        let participant_entry = participant_entry?;
        let participant_path = participant_entry.path();
        if !participant_path.is_dir() || !dir_name_starts_with(&participant_path, "participant_id=")
        {
            continue;
        }
        for source_entry in fs::read_dir(&participant_path)
            .with_context(|| format!("scan WAL participant dir {}", participant_path.display()))?
        {
            let source_entry = source_entry?;
            let source_path = source_entry.path();
            if !source_path.is_dir() || !dir_name_starts_with(&source_path, "source=") {
                continue;
            }
            for date_entry in fs::read_dir(&source_path)
                .with_context(|| format!("scan WAL source dir {}", source_path.display()))?
            {
                let date_entry = date_entry?;
                let date_path = date_entry.path();
                if !date_path.is_dir() || !dir_name_starts_with(&date_path, "date=") {
                    continue;
                }
                for file_entry in fs::read_dir(&date_path)
                    .with_context(|| format!("scan WAL date dir {}", date_path.display()))?
                {
                    let file_entry = file_entry?;
                    let path = file_entry.path();
                    if file_name_ends_with(&path, ".closed") {
                        closed.push(path);
                    }
                }
            }
        }
    }
    closed.sort();
    Ok(closed)
}

fn dir_name_starts_with(path: &Path, prefix: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with(prefix))
        .unwrap_or(false)
}

fn file_name_ends_with(path: &Path, suffix: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.ends_with(suffix))
        .unwrap_or(false)
}

fn archive_closed_wal_segment(data_dir: &Path, wal_path: &Path) -> Result<SegmentArchiveOutcome> {
    let sidecar_path = sidecar_path_for_closed_wal(wal_path)?;

    // Fast path: a `.closed` WAL segment is immutable, so a structurally-valid "archived" sidecar
    // proves it was already archived in a prior pass — skip it WITHOUT re-hashing and re-parsing
    // the whole segment. The previous unconditional `sha256_file_hex` + full `read_wal_records`
    // below ran for EVERY segment on EVERY pass, making each archiver pass cost O(total WAL bytes)
    // and pinning a CPU core for minutes once hundreds of already-archived segments existed
    // (the 2026-06-03 runaway archive process). See docs/archive_idempotent_skip_20260603.md.
    if sidecar_path.exists() {
        let sidecar = read_archive_sidecar(&sidecar_path)?;
        if sidecar_is_valid_archived(&sidecar) {
            return Ok(SegmentArchiveOutcome::Skipped);
        }
    }

    // Slow path: first-ever archive of this segment, or a sidecar that is missing/malformed and
    // must be re-derived. Only here do we pay the hash + full parse.
    let wal_sha256 = sha256_file_hex(wal_path)?;
    let read = read_wal_records(wal_path, WalReadMode::Strict)?;
    let wal_record_count = u64::try_from(read.records.len())
        .context("WAL record count does not fit u64 for sidecar")?;

    if sidecar_path.exists() {
        let sidecar = read_archive_sidecar(&sidecar_path)?;
        if sidecar_matches_wal(&sidecar, &wal_sha256, wal_record_count) {
            return Ok(SegmentArchiveOutcome::Skipped);
        }
        bail!(
            "existing sidecar {} does not match WAL checksum/count; refusing to overwrite",
            sidecar_path.display()
        );
    }

    let partition = WalPartition::from_path(wal_path)?;
    let segment_end_unix_ns = segment_end_unix_ns(&read);
    let typed_records = parse_wal_to_sensor_records(&read)?;
    let outputs = write_wal_archive_outputs(
        data_dir,
        &partition.participant_id,
        read.header.segment_start_unix_ns,
        segment_end_unix_ns,
        read.header.segment_id,
        &typed_records,
    )?;
    let provenance = SourceProvenance::from_wal(&read);
    let sidecar = WalArchiveSidecar::from_archive_result(
        wal_path,
        &partition,
        &read,
        segment_end_unix_ns,
        wal_sha256,
        provenance,
        outputs.clone(),
    )?;
    write_archive_sidecar_tmp_then_rename(&sidecar_path, &sidecar)?;
    Ok(SegmentArchiveOutcome::Archived {
        output_files: outputs.len(),
    })
}

fn sidecar_path_for_closed_wal(wal_path: &Path) -> Result<PathBuf> {
    let name = wal_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("WAL path {} has no UTF-8 filename", wal_path.display()))?;
    let stem = name
        .strip_suffix(".closed")
        .ok_or_else(|| anyhow!("WAL path {} is not a .closed segment", wal_path.display()))?;
    Ok(wal_path.with_file_name(format!("{stem}.archived.json")))
}

fn sidecar_tmp_path(sidecar_path: &Path) -> Result<PathBuf> {
    let name = sidecar_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "sidecar path {} has no UTF-8 filename",
                sidecar_path.display()
            )
        })?;
    Ok(sidecar_path.with_file_name(format!("{name}.tmp")))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WalPartition {
    participant_id: String,
    source: String,
    date: String,
}

impl WalPartition {
    fn from_path(path: &Path) -> Result<Self> {
        let mut participant_id = None;
        let mut source = None;
        let mut date = None;
        for component in path.components() {
            let text = component.as_os_str().to_string_lossy();
            if let Some(value) = text.strip_prefix("participant_id=") {
                participant_id = Some(value.to_string());
            } else if let Some(value) = text.strip_prefix("source=") {
                source = Some(value.to_string());
            } else if let Some(value) = text.strip_prefix("date=") {
                date = Some(value.to_string());
            }
        }
        Ok(Self {
            participant_id: participant_id.ok_or_else(|| {
                anyhow!("WAL path {} lacks participant_id partition", path.display())
            })?,
            source: source
                .ok_or_else(|| anyhow!("WAL path {} lacks source partition", path.display()))?,
            date: date
                .ok_or_else(|| anyhow!("WAL path {} lacks date partition", path.display()))?,
        })
    }
}

fn read_archive_sidecar(path: &Path) -> Result<WalArchiveSidecar> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read archive sidecar {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse archive sidecar {}", path.display()))
}

fn sidecar_matches_wal(sidecar: &WalArchiveSidecar, wal_sha256: &str, record_count: u64) -> bool {
    sidecar.sidecar_schema_version == 1
        && sidecar.status == "archived"
        && sidecar.wal.sha256 == wal_sha256
        && sidecar.wal.record_count == record_count
        && sidecar.wal.wal_container_major == WAL_CONTAINER_MAJOR
        && sidecar.wal.wal_container_minor == WAL_CONTAINER_MINOR
}

/// Structural validity of an "archived" sidecar that does NOT require reading the WAL. A `.closed`
/// WAL segment is immutable once renamed, so a structurally-valid sidecar is sufficient proof the
/// segment was already archived in a prior pass — letting the archiver skip it without re-hashing
/// or re-parsing the whole file.
fn sidecar_is_valid_archived(sidecar: &WalArchiveSidecar) -> bool {
    sidecar.sidecar_schema_version == 1
        && sidecar.status == "archived"
        && sidecar.wal.wal_container_major == WAL_CONTAINER_MAJOR
        && sidecar.wal.wal_container_minor == WAL_CONTAINER_MINOR
        && !sidecar.wal.sha256.is_empty()
}

fn segment_end_unix_ns(read: &WalReadResult) -> i64 {
    read.records
        .iter()
        .rev()
        .find_map(|frame| match &frame.record.body {
            WalBody::SegmentClosed(body) => Some(body.segment_end_unix_ns),
            _ => None,
        })
        .or_else(|| {
            read.records
                .last()
                .map(|frame| frame.record.collector_unix_ns)
        })
        .unwrap_or(read.header.segment_start_unix_ns)
}

#[derive(Default)]
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

fn parse_wal_to_sensor_records(read: &WalReadResult) -> Result<Vec<SensorRecord>> {
    let mut parser = PmdParser::new();
    let mut collector = WalTypedRecordCollector::default();
    let mut connection_ids = ConnectionIdMapper::default();

    for frame in &read.records {
        for record in crate::wal::logical_wal_records(&frame.record) {
            collector.set_meta(record_meta_from_wal(record, &mut connection_ids)?);
            if let Err(error) = route_wal_record(record, &mut parser, &mut collector) {
                let detail = format!(
                    "quarantined {} record {}: {error:#}",
                    record.kind.as_str(),
                    record.record_id
                );
                let fields_json = serde_json::json!({
                    "record_id": record.record_id,
                    "record_kind": record.kind.as_str(),
                    "error": format!("{error:#}"),
                })
                .to_string();
                collector.push_structured_event(
                    collector.current_meta.collector_unix_ns,
                    "archive_record_parse_error",
                    "error",
                    &detail,
                    &fields_json,
                );
                // A malformed PMD control/data record must not leave parser state that changes
                // the interpretation of later valid records in this or another tenant's WAL.
                parser = PmdParser::new();
            }
        }
    }

    Ok(collector.records)
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
            .unwrap_or_else(|| "unknown".to_string()),
        session_id: record.session_id.to_string(),
        connection_id: connection_ids.map(record.connection_id),
        collector_unix_ns,
        collector_monotonic_ns: u128::from(record.collector_monotonic_ns),
    })
}

fn route_wal_record(
    record: &WalRecord,
    parser: &mut PmdParser,
    collector: &mut WalTypedRecordCollector,
) -> Result<()> {
    match &record.body {
        WalBody::CloudIngestBatchV1(_) => {
            bail!("nested cloud ingest envelope reached archive logical router")
        }
        WalBody::GattNotification(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_notification")?;
            collector.raw_gatt(
                collector.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                "inbound",
                "notification",
                &payload,
                &body.description,
            )?;
            route_gatt_payload(
                collector.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                &body.description,
                &payload,
                parser,
                collector,
            )?;
        }
        WalBody::GattReadResponse(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_read_response")?;
            collector.raw_gatt(
                collector.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                "inbound",
                "read_response",
                &payload,
                &body.reason,
            )?;
            if is_battery_characteristic(&body.characteristic_uuid, &body.reason)
                && let Some(level) = payload.first()
            {
                collector.battery(
                    collector.current_meta.collector_unix_ns,
                    *level,
                    &body.reason,
                )?;
            }
        }
        WalBody::GattWriteRequest(body) => {
            let payload =
                decode_payload_b64(&body.payload_b64, body.payload_len, "gatt_write_request")?;
            collector.raw_gatt(
                collector.current_meta.collector_unix_ns,
                &body.characteristic_uuid,
                "outbound",
                "write_request",
                &payload,
                &format!("write_type={} reason={}", body.write_type, body.reason),
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
            collector.push_structured_event(
                collector.current_meta.collector_unix_ns,
                &body.event,
                &body.severity,
                &detail,
                &serde_json::to_string(&body.fields)?,
            );
        }
        WalBody::RssiSample(body) => {
            let pmd_frames = collector.pmd_frames;
            let hr_frames = collector.hr_frames;
            collector.health(
                collector.current_meta.collector_unix_ns,
                Some(body.rssi_dbm),
                pmd_frames,
                hr_frames,
            )?;
        }
        WalBody::ManualMarker(body) => {
            collector.event(
                "manual_marker",
                &format!(
                    "event_id={} event_unix_ns={} label={} note={:?}",
                    body.event_id, body.event_unix_ns, body.label, body.note
                ),
            )?;
        }
        _ => {
            let detail = serde_json::to_string(&record.body)?;
            collector.event(record.kind.as_str(), &detail)?;
        }
    }
    Ok(())
}

fn route_gatt_payload(
    collector_ns: u128,
    characteristic_uuid: &str,
    description: &str,
    payload: &[u8],
    parser: &mut PmdParser,
    collector: &mut WalTypedRecordCollector,
) -> Result<()> {
    let uuid = characteristic_uuid.to_ascii_lowercase();
    if uuid == PMD_DATA_UUID {
        parser.handle_pmd_data(collector_ns, payload, collector)?;
        collector.pmd_frames = collector.pmd_frames.saturating_add(1);
    } else if uuid == PMD_CONTROL_UUID {
        parser.handle_control(collector_ns, payload, collector)?;
    } else if uuid == HR_MEASUREMENT_UUID {
        handle_hr(collector_ns, payload, collector)?;
        collector.hr_frames = collector.hr_frames.saturating_add(1);
    } else if is_battery_characteristic(&uuid, description)
        && let Some(level) = payload.first()
    {
        collector.battery(collector_ns, *level, description)?;
    }
    Ok(())
}

fn is_battery_characteristic(characteristic_uuid: &str, reason_or_description: &str) -> bool {
    characteristic_uuid.eq_ignore_ascii_case(BATTERY_LEVEL_UUID)
        || reason_or_description == "battery_level"
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

#[derive(Debug)]
struct WalTypedRecordCollector {
    records: Vec<SensorRecord>,
    current_meta: RecordMeta,
    dummy_dir: PathBuf,
    pmd_frames: u64,
    hr_frames: u64,
}

impl Default for WalTypedRecordCollector {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            current_meta: RecordMeta {
                schema_version: RECORD_SCHEMA_VERSION,
                sensor_id: "unknown".to_string(),
                session_id: "unknown".to_string(),
                connection_id: 0,
                collector_unix_ns: 0,
                collector_monotonic_ns: 0,
            },
            dummy_dir: PathBuf::new(),
            pmd_frames: 0,
            hr_frames: 0,
        }
    }
}

impl WalTypedRecordCollector {
    fn set_meta(&mut self, meta: RecordMeta) {
        self.current_meta = meta;
    }

    fn meta_at(&self, collector_ns: u128) -> RecordMeta {
        let mut meta = self.current_meta.clone();
        meta.collector_unix_ns = collector_ns;
        meta
    }

    fn push_event(&mut self, collector_ns: u128, event: &str, detail: &str) {
        self.push_structured_event(collector_ns, event, "info", detail, "{}");
    }

    fn push_structured_event(
        &mut self,
        collector_ns: u128,
        event: &str,
        severity: &str,
        detail: &str,
        fields_json: &str,
    ) {
        self.records.push(SensorRecord::Event(CollectorEvent {
            meta: self.meta_at(collector_ns),
            event: event.to_string(),
            severity: severity.to_string(),
            detail: detail.to_string(),
            fields_json: fields_json.to_string(),
        }));
    }
}

impl RecordSink for WalTypedRecordCollector {
    fn session_dir(&self) -> &Path {
        &self.dummy_dir
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        self.push_event(self.current_meta.collector_unix_ns, event, detail);
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
        self.records.push(SensorRecord::RawGatt(RawGattRecord {
            meta: self.meta_at(collector_ns),
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            direction: direction.to_string(),
            operation: operation.to_string(),
            payload: payload.to_vec(),
            detail: detail.to_string(),
        }));
        Ok(())
    }

    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        self.records.push(SensorRecord::PmdTiming(PmdTimingRecord {
            meta: self.meta_at(timing.collector_ns),
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
        }));
        Ok(())
    }

    fn ecg(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        microvolts: i32,
    ) -> Result<()> {
        self.records.push(SensorRecord::Ecg(EcgSample {
            meta: self.meta_at(collector_ns),
            device_time_ns: device_ts,
            device_time_epoch: "h10_local_since_2000".to_string(),
            sample_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
            ecg_uv: microvolts,
        }));
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
        self.records.push(SensorRecord::Acc(AccSample {
            meta: self.meta_at(collector_ns),
            device_time_ns: device_ts,
            device_time_epoch: "h10_local_since_2000".to_string(),
            sample_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
            x_mg: x,
            y_mg: y,
            z_mg: z,
        }));
        Ok(())
    }

    fn hr(&mut self, collector_ns: u128, data: &HrData, raw: &[u8]) -> Result<()> {
        self.records.push(SensorRecord::Hr(HrFrame {
            meta: self.meta_at(collector_ns),
            hr_bpm: data.hr_bpm,
            sensor_contact_supported: data.sensor_contact_supported,
            sensor_contact_detected: data.sensor_contact_detected,
            energy_expended: data.energy_expended,
            rr_intervals_ms: data.rr_ms.clone(),
            raw_payload: raw.to_vec(),
        }));
        for (index, rr_ms) in data.rr_ms.iter().enumerate() {
            self.records.push(SensorRecord::Rr(RrInterval {
                meta: self.meta_at(collector_ns),
                rr_index_in_frame: u32::try_from(index).unwrap_or(u32::MAX),
                rr_ms: *rr_ms,
                hr_frame_collector_unix_ns: collector_ns,
            }));
        }
        Ok(())
    }

    fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()> {
        self.records.push(SensorRecord::Battery(BatterySample {
            meta: self.meta_at(collector_ns),
            battery_percent: level,
            reason: reason.to_string(),
        }));
        Ok(())
    }

    fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        self.records
            .push(SensorRecord::Health(ConnectionHealthSample {
                meta: self.meta_at(collector_ns),
                rssi_dbm: rssi,
                pmd_frames,
                hr_frames,
            }));
        Ok(())
    }
}

fn write_wal_archive_outputs(
    data_dir: &Path,
    participant_id: &str,
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    segment_id: Uuid,
    records: &[SensorRecord],
) -> Result<Vec<ArchiveOutputSidecar>> {
    let mut by_stream: BTreeMap<(RecordStream, String), Vec<SensorRecord>> = BTreeMap::new();
    for record in records {
        let capture_ns = i64::try_from(record.collector_unix_ns())
            .context("archive collector timestamp does not fit i64")?;
        let capture_date = format_date_partition(capture_ns)?;
        by_stream
            .entry((record.stream(), capture_date))
            .or_default()
            .push(record.clone());
    }

    let parquet_root = wal_archive_parquet_root(data_dir);
    let mut outputs = Vec::new();
    for ((stream, _capture_date), records) in by_stream {
        if records.is_empty() {
            continue;
        }
        let output_start_unix_ns = records
            .iter()
            .map(|record| i64::try_from(record.collector_unix_ns()).unwrap_or(i64::MAX))
            .min()
            .unwrap_or(segment_start_unix_ns);
        let output_end_unix_ns = records
            .iter()
            .map(|record| i64::try_from(record.collector_unix_ns()).unwrap_or(i64::MAX))
            .max()
            .unwrap_or(segment_end_unix_ns);
        let final_path = wal_archive_parquet_path(
            &parquet_root,
            participant_id,
            stream,
            output_start_unix_ns,
            output_end_unix_ns,
            segment_id,
        )?;
        let parent = final_path
            .parent()
            .ok_or_else(|| anyhow!("Parquet output {} has no parent", final_path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create WAL archive output dir {}", parent.display()))?;
        let file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                anyhow!(
                    "Parquet output {} has no UTF-8 filename",
                    final_path.display()
                )
            })?;
        let tmp_path = final_path.with_file_name(format!("{file_name}.tmp"));
        let batch = record_batch_for_stream(stream, &records)?;
        if final_path.exists() {
            verify_existing_parquet_matches_records(&final_path, &batch, records.len())?;
        } else {
            write_parquet_tmp_then_rename(&tmp_path, &final_path, &batch)
                .with_context(|| format!("write WAL archive Parquet {}", final_path.display()))?;
        }
        let row_count = u64::try_from(records.len()).context("row count does not fit u64")?;
        let size_bytes = fs::metadata(&final_path)
            .with_context(|| format!("stat Parquet {}", final_path.display()))?
            .len();
        let sha256 = sha256_file_hex(&final_path)?;
        outputs.push(ArchiveOutputSidecar::from_records(
            stream,
            &final_path,
            &records,
            row_count,
            size_bytes,
            sha256,
        ));
    }
    Ok(outputs)
}

fn wal_archive_parquet_path(
    parquet_root: &Path,
    participant_id: &str,
    stream: RecordStream,
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    segment_id: Uuid,
) -> Result<PathBuf> {
    let start = format_wal_timestamp_ns(segment_start_unix_ns)?;
    let end = format_wal_timestamp_ns(segment_end_unix_ns)?;
    let date = format_date_partition(segment_start_unix_ns)?;
    Ok(parquet_root
        .join(format!("participant_id={participant_id}"))
        .join(format!("stream={}", stream.as_str()))
        .join(format!("date={date}"))
        .join(format!(
            "part-{start}-{end}-{}-{}.parquet",
            segment_id.hyphenated(),
            stream.as_str()
        )))
}

fn verify_existing_parquet_matches_records(
    final_path: &Path,
    expected_batch: &RecordBatch,
    expected_rows: usize,
) -> Result<()> {
    let existing_rows = parquet_row_count(final_path)?;
    let expected_rows_u64 = u64::try_from(expected_rows).context("row count does not fit u64")?;
    if existing_rows != expected_rows_u64 {
        bail!(
            "existing Parquet {} has {} rows, expected {}; refusing to checkpoint",
            final_path.display(),
            existing_rows,
            expected_rows_u64
        );
    }

    let file_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "Parquet output {} has no UTF-8 filename",
                final_path.display()
            )
        })?;
    let verify_tmp = final_path.with_file_name(format!(
        "{file_name}.verify-{}.parquet.tmp",
        Uuid::new_v4().hyphenated()
    ));
    write_parquet_file(&verify_tmp, expected_batch)
        .with_context(|| format!("write verification Parquet {}", verify_tmp.display()))?;
    let verify_result = (|| -> Result<()> {
        let existing_sha = sha256_file_hex(final_path)?;
        let expected_sha = sha256_file_hex(&verify_tmp)?;
        if existing_sha != expected_sha {
            bail!(
                "existing Parquet {} does not match regenerated expected Parquet; refusing to checkpoint",
                final_path.display()
            );
        }
        Ok(())
    })();
    let cleanup_result = fs::remove_file(&verify_tmp)
        .with_context(|| format!("remove verification Parquet tmp {}", verify_tmp.display()));
    verify_result.and(cleanup_result)
}

fn parquet_row_count(path: &Path) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("open Parquet {}", path.display()))?;
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("read Parquet metadata {}", path.display()))?;
    let rows = reader.metadata().file_metadata().num_rows();
    u64::try_from(rows).with_context(|| format!("negative Parquet row count in {}", path.display()))
}

pub fn cleanup_stale_parquet_tmp_files(parquet_root: &Path) -> Result<usize> {
    fn visit(path: &Path, removed: &mut usize) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(path).with_context(|| format!("read dir {}", path.display()))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit(&path, removed)?;
            } else if file_name_ends_with(&path, ".parquet.tmp") {
                fs::remove_file(&path)
                    .with_context(|| format!("remove stale Parquet tmp {}", path.display()))?;
                *removed += 1;
            }
        }
        Ok(())
    }

    let mut removed = 0;
    visit(parquet_root, &mut removed)?;
    Ok(removed)
}

fn write_archive_sidecar_tmp_then_rename(path: &Path, sidecar: &WalArchiveSidecar) -> Result<()> {
    if path.exists() {
        bail!("archive sidecar already exists: {}", path.display());
    }
    let tmp_path = sidecar_tmp_path(path)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create sidecar dir {}", parent.display()))?;
    }
    if tmp_path.exists() {
        fs::remove_file(&tmp_path)
            .with_context(|| format!("remove stale sidecar tmp {}", tmp_path.display()))?;
    }
    let mut file = File::create(&tmp_path)
        .with_context(|| format!("create sidecar tmp {}", tmp_path.display()))?;
    let bytes = serde_json::to_vec_pretty(sidecar).context("serialize archive sidecar")?;
    file.write_all(&bytes)
        .with_context(|| format!("write sidecar tmp {}", tmp_path.display()))?;
    file.flush()
        .with_context(|| format!("flush sidecar tmp {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync sidecar tmp {}", tmp_path.display()))?;
    drop(file);
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "rename sidecar tmp {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("fsync sidecar dir {}", parent.display()))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct WalArchiveSidecar {
    sidecar_schema_version: u16,
    status: String,
    wal: WalSidecarInfo,
    source_provenance: SourceProvenance,
    archiver: ArchiverSidecarInfo,
    outputs: Vec<ArchiveOutputSidecar>,
    recovery: RecoverySidecarInfo,
    warnings: Vec<String>,
    errors: Vec<String>,
}

impl WalArchiveSidecar {
    fn from_archive_result(
        wal_path: &Path,
        partition: &WalPartition,
        read: &WalReadResult,
        segment_end_unix_ns: i64,
        wal_sha256: String,
        source_provenance: SourceProvenance,
        outputs: Vec<ArchiveOutputSidecar>,
    ) -> Result<Self> {
        let first_record_unix_ns = read
            .records
            .first()
            .map(|frame| frame.record.collector_unix_ns)
            .unwrap_or(read.header.segment_start_unix_ns);
        let last_record_unix_ns = read
            .records
            .last()
            .map(|frame| frame.record.collector_unix_ns)
            .unwrap_or(read.header.segment_start_unix_ns);
        let record_count = u64::try_from(read.records.len())
            .context("WAL record count does not fit u64 for sidecar")?;
        Ok(Self {
            sidecar_schema_version: 1,
            status: "archived".to_string(),
            wal: WalSidecarInfo {
                path: wal_path.display().to_string(),
                filename: wal_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_string(),
                participant_id: partition.participant_id.clone(),
                source: partition.source.clone(),
                segment_id: read.header.segment_id,
                wal_container_major: WAL_CONTAINER_MAJOR,
                wal_container_minor: WAL_CONTAINER_MINOR,
                payload_codec: match WAL_PAYLOAD_CODEC_CANONICAL_JSON_V1 {
                    1 => "canonical_json_v1".to_string(),
                    other => format!("unknown_{other}"),
                },
                checksum_algorithm: match WAL_CHECKSUM_CRC32C {
                    1 => "crc32c".to_string(),
                    other => format!("unknown_{other}"),
                },
                segment_start_unix_ns: read.header.segment_start_unix_ns,
                segment_end_unix_ns,
                first_record_unix_ns,
                last_record_unix_ns,
                record_count,
                size_bytes: read.file_len,
                sha256: wal_sha256,
            },
            source_provenance,
            archiver: ArchiverSidecarInfo {
                software_name: "training_sensors".to_string(),
                software_version: env!("CARGO_PKG_VERSION").to_string(),
                git_commit: option_env!("GIT_COMMIT").map(str::to_string),
                run_id: Uuid::new_v4(),
                archived_at_unix_ns: i64::try_from(crate::util::unix_ns()).unwrap_or(i64::MAX),
                archive_schema_version: ARCHIVE_SCHEMA_VERSION,
            },
            outputs,
            recovery: RecoverySidecarInfo {
                was_recovered_open: false,
                truncated_tail_bytes: 0,
                recovery_events: Vec::new(),
            },
            warnings: Vec::new(),
            errors: Vec::new(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct WalSidecarInfo {
    path: String,
    filename: String,
    participant_id: String,
    source: String,
    segment_id: Uuid,
    wal_container_major: u16,
    wal_container_minor: u16,
    payload_codec: String,
    checksum_algorithm: String,
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    first_record_unix_ns: i64,
    last_record_unix_ns: i64,
    record_count: u64,
    size_bytes: u64,
    sha256: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct SourceProvenance {
    physical_device_ids: Vec<String>,
    ble_peripheral_ids: Vec<String>,
    device_serials: Vec<String>,
    device_models: Vec<String>,
    firmware_versions: Vec<String>,
}

impl SourceProvenance {
    fn from_wal(read: &WalReadResult) -> Self {
        let mut physical_device_ids = BTreeSet::new();
        let mut ble_peripheral_ids = BTreeSet::new();
        let mut device_serials = BTreeSet::new();
        let mut device_models = BTreeSet::new();
        let mut firmware_versions = BTreeSet::new();

        for frame in &read.records {
            for record in crate::wal::logical_wal_records(&frame.record) {
                if let Some(value) = &record.physical_device_id {
                    physical_device_ids.insert(value.clone());
                }
                if let Some(value) = &record.ble_peripheral_id {
                    ble_peripheral_ids.insert(value.clone());
                }
                if let WalBody::ServiceDiscoverySummary(body) = &record.body
                    && let Some(info) = &body.device_info
                {
                    if let Some(value) = &info.serial {
                        device_serials.insert(value.clone());
                    }
                    if let Some(value) = &info.model {
                        device_models.insert(value.clone());
                    }
                    if let Some(value) = &info.firmware_revision {
                        firmware_versions.insert(value.clone());
                    }
                }
            }
        }

        Self {
            physical_device_ids: physical_device_ids.into_iter().collect(),
            ble_peripheral_ids: ble_peripheral_ids.into_iter().collect(),
            device_serials: device_serials.into_iter().collect(),
            device_models: device_models.into_iter().collect(),
            firmware_versions: firmware_versions.into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ArchiverSidecarInfo {
    software_name: String,
    software_version: String,
    git_commit: Option<String>,
    run_id: Uuid,
    archived_at_unix_ns: i64,
    archive_schema_version: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ArchiveOutputSidecar {
    stream: String,
    path: String,
    schema_version: u16,
    row_count: u64,
    size_bytes: u64,
    sha256: String,
    first_record_unix_ns: i64,
    last_record_unix_ns: i64,
}

impl ArchiveOutputSidecar {
    fn from_records(
        stream: RecordStream,
        path: &Path,
        records: &[SensorRecord],
        row_count: u64,
        size_bytes: u64,
        sha256: String,
    ) -> Self {
        let first_record_unix_ns = records
            .iter()
            .map(SensorRecord::collector_unix_ns)
            .min()
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or(0);
        let last_record_unix_ns = records
            .iter()
            .map(SensorRecord::collector_unix_ns)
            .max()
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or(0);
        Self {
            stream: stream.as_str().to_string(),
            path: path.display().to_string(),
            schema_version: match stream {
                RecordStream::Events => 2,
                _ => 1,
            },
            row_count,
            size_bytes,
            sha256,
            first_record_unix_ns,
            last_record_unix_ns,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
struct RecoverySidecarInfo {
    was_recovered_open: bool,
    truncated_tail_bytes: u64,
    recovery_events: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::{
        AccSample, BatterySample, CollectorEvent, ConnectionHealthSample, EcgSample, HrFrame,
        PmdTimingRecord, RECORD_SCHEMA_VERSION, RecordMeta, RrInterval,
    };
    use crate::wal::{
        CloudIngestBatchV1Body, CollectorEventBody, GattNotificationBody, GattReadResponseBody,
        WalPathConfig, WalRecordContext, WalWriter, WalWriterOptions,
    };
    use arrow_array::{Array, BinaryArray, Int32Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::time::Duration;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_archive_{name}_{}_{}",
            std::process::id(),
            crate::util::unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn meta(sensor_id: &str, session_id: &str, ns: u128) -> RecordMeta {
        RecordMeta {
            schema_version: RECORD_SCHEMA_VERSION,
            sensor_id: sensor_id.to_string(),
            session_id: session_id.to_string(),
            connection_id: 1,
            collector_unix_ns: ns,
            collector_monotonic_ns: ns / 10,
        }
    }

    fn ecg_record(ns: u128, uv: i32) -> SensorRecord {
        SensorRecord::Ecg(EcgSample {
            meta: meta("h10_TEST", "session", ns),
            device_time_ns: 42,
            device_time_epoch: "h10_local_since_2000".to_string(),
            sample_index_in_frame: 0,
            ecg_uv: uv,
        })
    }

    const WAL_TEST_START_NS: i64 = 1_770_000_000_000_000_000;

    fn test_uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).expect("test uuid")
    }

    fn wal_context_at(ns: i64) -> WalRecordContext {
        WalRecordContext::new(
            "local",
            "chest_h10",
            "training_sensors collect",
            test_uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            test_uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        )
        .expect("wal context")
        .at_time(
            ns,
            u64::try_from(ns.saturating_sub(WAL_TEST_START_NS)).unwrap_or(0),
        )
        .connection_id(Some(test_uuid("dddddddd-dddd-4ddd-8ddd-dddddddddddd")))
        .physical_device_id(Some("h10_a1b2c3d4"))
        .ble_peripheral_id(Some("72585434-07e3-d5e3-5ace-4e6c70118687"))
    }

    fn pmd_frame(
        measurement_type: u8,
        frame_ts: u64,
        frame_type: u8,
        compressed: bool,
        content: &[u8],
    ) -> Vec<u8> {
        let mut data = Vec::with_capacity(content.len() + 10);
        data.push(measurement_type);
        data.extend_from_slice(&frame_ts.to_le_bytes());
        data.push(frame_type | if compressed { 0x80 } else { 0 });
        data.extend_from_slice(content);
        data
    }

    fn hr_notification(bpm: u8, with_rr: bool) -> WalBody {
        let payload = if with_rr {
            vec![0x16, bpm, 0x00, 0x04]
        } else {
            vec![0x06, bpm]
        };
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                HR_MEASUREMENT_UUID,
                "heart_rate_measurement",
                &payload,
            )
            .expect("hr body"),
        )
    }

    fn ecg_pmd_notification() -> WalBody {
        WalBody::GattNotification(
            GattNotificationBody::from_payload(
                PMD_DATA_UUID,
                "pmd_data",
                &pmd_frame(0, 1_000_000_000, 0, false, &[1, 0, 0, 0xff, 0xff, 0xff]),
            )
            .expect("pmd body"),
        )
    }

    fn battery_read_response(level: u8) -> WalBody {
        WalBody::GattReadResponse(
            GattReadResponseBody::from_payload(
                test_uuid("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                BATTERY_LEVEL_UUID,
                "battery_level",
                &[level],
            )
            .expect("battery read"),
        )
    }

    fn create_closed_wal(base: &Path, offset_ns: i64, bodies: Vec<WalBody>) -> PathBuf {
        let path_config = WalPathConfig::new(base, "local", "chest_h10").expect("wal config");
        let start_ns = WAL_TEST_START_NS + offset_ns;
        let mut writer = WalWriter::create(
            &path_config,
            start_ns,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("wal writer");
        for (index, body) in bodies.into_iter().enumerate() {
            let ns = start_ns + i64::try_from(index).expect("index fits") + 1;
            writer
                .append_record(&wal_context_at(ns).make_record(body))
                .expect("append wal record");
        }
        writer
            .close(&wal_context_at(start_ns + 100), start_ns + 100, "test", 0)
            .expect("close wal")
            .closed_path
    }

    fn create_open_wal(base: &Path) -> PathBuf {
        let path_config = WalPathConfig::new(base, "local", "chest_h10").expect("wal config");
        let mut writer = WalWriter::create(
            &path_config,
            WAL_TEST_START_NS,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("wal writer");
        writer
            .append_record(
                &wal_context_at(WAL_TEST_START_NS + 1).make_record(hr_notification(60, false)),
            )
            .expect("append open wal");
        let path = writer.open_path().to_path_buf();
        drop(writer);
        path
    }

    fn output_path_for(sidecar: &WalArchiveSidecar, stream: &str) -> PathBuf {
        sidecar
            .outputs
            .iter()
            .find(|output| output.stream == stream)
            .map(|output| PathBuf::from(&output.path))
            .unwrap_or_else(|| panic!("missing output stream {stream}"))
    }

    fn read_first_batch(path: &Path) -> RecordBatch {
        let file = File::open(path).expect("open parquet");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("reader")
            .build()
            .expect("build reader");
        reader.next().expect("one batch").expect("batch ok")
    }

    fn sidecar_for_wal(wal_path: &Path) -> WalArchiveSidecar {
        read_archive_sidecar(&sidecar_path_for_closed_wal(wal_path).expect("sidecar path"))
            .expect("read sidecar")
    }

    fn find_files_ending_with(root: &Path, suffix: &str) -> Vec<PathBuf> {
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
        visit(root, suffix, &mut out);
        out.sort();
        out
    }

    #[test]
    fn wal_archiver_ignores_open_wal_segments() {
        let base = temp_dir("wal_ignore_open");
        let open_path = create_open_wal(&base);
        let summary = run_wal_archiver(&base).expect("archiver run");
        assert_eq!(summary.scanned_segments, 0);
        assert_eq!(summary.archived_segments, 0);
        assert_eq!(summary.skipped_segments, 0);
        assert!(open_path.exists(), "archiver must not touch .open WAL");
        assert!(
            find_all_closed_wal_segments(&base)
                .expect("scan")
                .is_empty()
        );
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn one_closed_wal_archives_expected_parquet_and_sidecar() {
        let base = temp_dir("wal_one_closed");
        let wal_path = create_closed_wal(
            &base,
            0,
            vec![
                ecg_pmd_notification(),
                hr_notification(60, true),
                battery_read_response(88),
            ],
        );

        let summary = run_wal_archiver(&base).expect("archiver run");
        assert_eq!(summary.scanned_segments, 1);
        assert_eq!(summary.archived_segments, 1);
        assert_eq!(summary.skipped_segments, 0);
        assert!(summary.output_files >= 6, "summary: {summary:?}");

        let sidecar_path = sidecar_path_for_closed_wal(&wal_path).expect("sidecar path");
        assert!(sidecar_path.exists());
        let sidecar = read_archive_sidecar(&sidecar_path).expect("sidecar");
        assert_eq!(sidecar.status, "archived");
        assert_eq!(sidecar.wal.participant_id, "local");
        assert_eq!(sidecar.wal.source, "chest_h10");
        assert_eq!(sidecar.wal.record_count, 4);
        assert_eq!(sidecar.wal.sha256.len(), 64);
        assert_eq!(
            sidecar.source_provenance.physical_device_ids,
            vec!["h10_a1b2c3d4"]
        );
        assert!(sidecar.errors.is_empty());
        let streams = sidecar
            .outputs
            .iter()
            .map(|output| output.stream.as_str())
            .collect::<BTreeSet<_>>();
        for stream in [
            "battery",
            "ecg",
            "events",
            "hr",
            "pmd_timing",
            "raw_gatt",
            "rr",
        ] {
            assert!(
                streams.contains(stream),
                "missing stream {stream}; got {streams:?}"
            );
        }

        let ecg_path = output_path_for(&sidecar, "ecg");
        let ecg_path_text = ecg_path.to_string_lossy();
        assert!(ecg_path_text.contains("archive/parquet/participant_id=local/stream=ecg/date="));
        assert!(!ecg_path_text.contains("hour="));
        let batch = read_first_batch(&ecg_path);
        assert_eq!(batch.num_rows(), 2);
        let ecg_uv = batch
            .column_by_name("ecg_uv")
            .expect("ecg_uv")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        assert_eq!(ecg_uv.value(0), 1);
        assert_eq!(ecg_uv.value(1), -1);
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn cloud_envelope_preserves_capture_date_provenance_and_structured_fields() {
        let base = temp_dir("cloud_envelope_capture_date");
        let capture_ns = chrono::DateTime::parse_from_rfc3339("2026-08-02T23:59:59Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("backlog_records".to_string(), serde_json::json!(4321));
        fields.insert("phone_battery_percent".to_string(), serde_json::json!(77));
        let child = wal_context_at(capture_ns)
            .physical_device_id(Some("android_h10_serial"))
            .ble_peripheral_id(Some("android-ble-id"))
            .make_record(WalBody::CollectorEvent(CollectorEventBody {
                event: "collector_health".to_string(),
                severity: "critical".to_string(),
                detail: "delayed phone health".to_string(),
                fields,
            }));
        let envelope = WalBody::CloudIngestBatchV1(CloudIngestBatchV1Body {
            installation_id: Uuid::new_v4(),
            batch_id: Uuid::new_v4(),
            request_sha256: "a".repeat(64),
            encoded_bytes: 100,
            first_source_seq: 0,
            last_source_seq: 0,
            received_unix_ms: WAL_TEST_START_NS / 1_000_000,
            record_sha256: vec!["b".repeat(64)],
            records: vec![child],
        });
        let wal_path = create_closed_wal(&base, 0, vec![envelope]);
        run_wal_archiver(&base).unwrap();
        let sidecar = sidecar_for_wal(&wal_path);
        assert!(
            sidecar
                .source_provenance
                .physical_device_ids
                .contains(&"android_h10_serial".to_string())
        );
        assert!(
            sidecar
                .source_provenance
                .ble_peripheral_ids
                .contains(&"android-ble-id".to_string())
        );
        let capture_date = format_date_partition(capture_ns).unwrap();
        let output = sidecar
            .outputs
            .iter()
            .find(|output| {
                output.stream == "events"
                    && output.first_record_unix_ns == capture_ns
                    && output.path.contains(&format!("date={capture_date}"))
            })
            .expect("capture-date structured event output");
        assert_eq!(sidecar.archiver.archive_schema_version, 2);
        assert_eq!(output.schema_version, 2);
        let batch = read_first_batch(Path::new(&output.path));
        let fields = batch
            .column_by_name("fields_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let severity = batch
            .column_by_name("severity")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(severity.value(0), "critical");
        assert!(fields.value(0).contains("\"backlog_records\":4321"));
        assert!(fields.value(0).contains("\"phone_battery_percent\":77"));
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn corrupt_participant_does_not_prevent_other_segment_archival() {
        let base = temp_dir("archive_tenant_isolation");
        let corrupt = create_closed_wal(&base, 0, vec![hr_notification(60, false)]);
        let valid = create_closed_wal(&base, 1_000_000_000, vec![hr_notification(61, false)]);
        fs::write(&corrupt, b"corrupt WAL").unwrap();
        let error = run_wal_archiver(&base).unwrap_err();
        assert!(format!("{error:#}").contains("failed while other participants continued"));
        assert!(sidecar_path_for_closed_wal(&valid).unwrap().exists());
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn malformed_pmd_record_is_quarantined_without_blocking_valid_sibling_or_health() {
        let base = temp_dir("archive_malformed_pmd_quarantine");
        let malformed = WalBody::GattNotification(
            GattNotificationBody::from_payload(
                PMD_DATA_UUID,
                "pmd_data",
                &pmd_frame(2, 1_000_000_000, 0, true, &[1, 2]),
            )
            .expect("malformed-but-well-framed GATT body"),
        );
        let wal_path = create_closed_wal(&base, 0, vec![malformed, hr_notification(71, false)]);
        let summary = run_wal_archiver(&base).expect("semantic parse errors are quarantined");
        assert_eq!(summary.archived_segments, 1);
        let sidecar = sidecar_for_wal(&wal_path);

        let hr = read_first_batch(&output_path_for(&sidecar, "hr"));
        let bpm = hr
            .column_by_name("hr_bpm")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(bpm.value(0), 71);

        let events = read_first_batch(&output_path_for(&sidecar, "events"));
        let names = events
            .column_by_name("event")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let severities = events
            .column_by_name("severity")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            (0..events.num_rows()).any(|index| {
                names.value(index) == "archive_record_parse_error"
                    && severities.value(index) == "error"
            }),
            "quarantine must remain visible in the typed event archive"
        );
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn invalid_timestamp_raw_bytes_survive_archival_with_valid_following_samples() {
        let base = temp_dir("archive_invalid_timestamp");
        let payload = pmd_frame(0, u64::MAX, 0, false, &[1, 2, 3]);
        let invalid = WalBody::GattNotification(
            GattNotificationBody::from_payload(PMD_DATA_UUID, "pmd_data", &payload).unwrap(),
        );
        let path = create_closed_wal(
            &base,
            0,
            vec![invalid, ecg_pmd_notification(), hr_notification(71, false)],
        );
        assert_eq!(run_wal_archiver(&base).unwrap().archived_segments, 1);
        let sidecar = sidecar_for_wal(&path);
        let raw = read_first_batch(&output_path_for(&sidecar, "raw_gatt"));
        let payloads = raw
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(payloads.value(0), payload);
        assert_eq!(raw.num_rows(), 3);
        assert_eq!(
            read_first_batch(&output_path_for(&sidecar, "ecg")).num_rows(),
            2
        );
        assert_eq!(
            read_first_batch(&output_path_for(&sidecar, "pmd_timing")).num_rows(),
            1
        );
        assert_eq!(
            read_first_batch(&output_path_for(&sidecar, "hr")).num_rows(),
            1
        );
        let events = read_first_batch(&output_path_for(&sidecar, "events"));
        let names = events
            .column_by_name("event")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!((0..events.num_rows()).any(|i| names.value(i) == "pmd_invalid_timestamp"));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn wal_archiver_is_idempotent_with_matching_sidecar() {
        let base = temp_dir("wal_idempotent");
        let wal_path = create_closed_wal(&base, 0, vec![hr_notification(61, true)]);
        let first = run_wal_archiver(&base).expect("first run");
        assert_eq!(first.archived_segments, 1);
        let sidecar_path = sidecar_path_for_closed_wal(&wal_path).expect("sidecar path");
        let before = fs::read_to_string(&sidecar_path).expect("sidecar before");

        let second = run_wal_archiver(&base).expect("second run");
        assert_eq!(second.scanned_segments, 1);
        assert_eq!(second.archived_segments, 0);
        assert_eq!(second.skipped_segments, 1);
        assert_eq!(second.output_files, 0);
        let after = fs::read_to_string(&sidecar_path).expect("sidecar after");
        assert_eq!(before, after, "idempotent skip must not rewrite sidecar");
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn already_archived_segment_is_skipped_via_sidecar_without_re_reading_wal() {
        let base = temp_dir("wal_skip_no_reread");
        let wal_path = create_closed_wal(&base, 0, vec![hr_notification(61, true)]);
        let first = run_wal_archiver(&base).expect("first run");
        assert_eq!(first.archived_segments, 1);

        // Corrupt the (immutable) closed WAL after archiving. The fast path must skip on the valid
        // sidecar WITHOUT re-hashing/re-parsing the WAL, so a second run must neither touch the WAL
        // contents nor error. The old code re-hashed + Strict-parsed every segment on every pass
        // and would fail here — that per-pass O(total WAL bytes) work is what pinned a CPU core
        // (2026-06-03 runaway archiver).
        fs::write(&wal_path, b"not a valid WAL container anymore").expect("corrupt closed wal");

        let second =
            run_wal_archiver(&base).expect("second run must skip via sidecar, not re-read WAL");
        assert_eq!(second.scanned_segments, 1);
        assert_eq!(second.archived_segments, 0);
        assert_eq!(second.skipped_segments, 1);
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn ten_one_record_wal_segments_archive_without_gaps_or_duplicates() {
        let base = temp_dir("wal_ten_segments");
        for index in 0..10_u8 {
            create_closed_wal(
                &base,
                i64::from(index) * 1_000_000_000,
                vec![hr_notification(50 + index, false)],
            );
        }

        let summary = run_wal_archiver(&base).expect("archiver run");
        assert_eq!(summary.scanned_segments, 10);
        assert_eq!(summary.archived_segments, 10);
        assert_eq!(summary.skipped_segments, 0);

        let mut bpms = BTreeSet::new();
        for wal_path in find_all_closed_wal_segments(&base).expect("scan closed") {
            let sidecar = sidecar_for_wal(&wal_path);
            let raw_path = output_path_for(&sidecar, "raw_gatt");
            let batch = read_first_batch(&raw_path);
            assert_eq!(batch.num_rows(), 1);
            let payloads = batch
                .column_by_name("payload")
                .expect("payload")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary payload");
            let payload = payloads.value(0);
            assert_eq!(payload[0], 0x06);
            bpms.insert(payload[1]);
        }
        assert_eq!(bpms, (50_u8..60).collect::<BTreeSet<_>>());
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn stale_parquet_tmp_cleanup_and_existing_final_reconcile_write_sidecar() {
        let base = temp_dir("wal_reconcile");
        let wal_path = create_closed_wal(&base, 0, vec![hr_notification(70, false)]);
        let read = read_wal_records(&wal_path, WalReadMode::Strict).expect("read wal");
        let records = parse_wal_to_sensor_records(&read).expect("parse wal");
        let end_ns = segment_end_unix_ns(&read);
        let outputs = write_wal_archive_outputs(
            &base,
            "local",
            read.header.segment_start_unix_ns,
            end_ns,
            read.header.segment_id,
            &records,
        )
        .expect("prewrite parquet outputs");
        assert!(
            !sidecar_path_for_closed_wal(&wal_path)
                .expect("sidecar path")
                .exists()
        );
        assert!(!outputs.is_empty());
        let output_parent = PathBuf::from(&outputs[0].path)
            .parent()
            .expect("output parent")
            .to_path_buf();
        let stale_parquet_tmp = output_parent.join("stale.parquet.tmp");
        let unrelated_tmp = output_parent.join("keep.tmp");
        fs::write(&stale_parquet_tmp, b"partial parquet").expect("write stale tmp");
        fs::write(&unrelated_tmp, b"not a parquet tmp").expect("write unrelated tmp");

        let summary = run_wal_archiver(&base).expect("archiver reconcile run");
        assert_eq!(summary.scanned_segments, 1);
        assert_eq!(summary.archived_segments, 1);
        assert_eq!(summary.stale_tmp_files_removed, 1);
        assert!(!stale_parquet_tmp.exists());
        assert!(
            unrelated_tmp.exists(),
            "cleanup should only remove *.parquet.tmp"
        );
        let sidecar = sidecar_for_wal(&wal_path);
        assert_eq!(sidecar.outputs.len(), outputs.len());
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn existing_same_row_count_wrong_parquet_is_not_blessed_by_sidecar() {
        let base = temp_dir("wal_wrong_existing");
        let wal_path = create_closed_wal(&base, 0, vec![hr_notification(70, false)]);
        let read = read_wal_records(&wal_path, WalReadMode::Strict).expect("read wal");
        let mut wrong_records = parse_wal_to_sensor_records(&read).expect("parse wal");
        for record in &mut wrong_records {
            match record {
                SensorRecord::Hr(hr) => hr.hr_bpm = 71,
                SensorRecord::RawGatt(raw) if raw.payload.len() >= 2 => raw.payload[1] = 71,
                _ => {}
            }
        }
        write_wal_archive_outputs(
            &base,
            "local",
            read.header.segment_start_unix_ns,
            segment_end_unix_ns(&read),
            read.header.segment_id,
            &wrong_records,
        )
        .expect("write wrong parquet finals");

        let err = run_wal_archiver(&base).expect_err("wrong final parquet must fail");
        assert!(
            format!("{err:#}").contains("does not match regenerated expected Parquet"),
            "unexpected error: {err:#}"
        );
        assert!(
            !sidecar_path_for_closed_wal(&wal_path)
                .expect("sidecar path")
                .exists()
        );
        let leftovers = find_files_ending_with(&wal_archive_parquet_root(&base), ".parquet.tmp");
        assert!(
            leftovers.is_empty(),
            "verification tmp files should be cleaned on failure: {leftovers:?}"
        );
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn record_batch_rejects_mixed_streams() {
        let records = vec![
            ecg_record(1_774_444_800_000_000_000, 1),
            SensorRecord::Event(CollectorEvent {
                meta: meta("h10_TEST", "session", 1_774_444_800_000_000_001),
                event: "event".to_string(),
                severity: "info".to_string(),
                detail: "detail".to_string(),
                fields_json: "{}".to_string(),
            }),
        ];
        let err = record_batch_for_stream(RecordStream::Ecg, &records).expect_err("mixed stream");
        assert!(format!("{err:#}").contains("non-ECG"));
    }

    #[test]
    fn acc_batch_schema_contains_units_in_column_names() {
        let records = vec![SensorRecord::Acc(AccSample {
            meta: meta("h10_TEST", "session", 1_774_444_800_000_000_000),
            device_time_ns: 123,
            device_time_epoch: "h10_local_since_2000".to_string(),
            sample_index_in_frame: 0,
            x_mg: 1,
            y_mg: 2,
            z_mg: 3,
        })];
        let batch = record_batch_for_stream(RecordStream::Acc, &records).expect("acc batch");
        assert!(batch.schema().field_with_name("x_mg").is_ok());
        assert!(batch.schema().field_with_name("device_time_ns").is_ok());
    }

    #[test]
    fn record_batches_cover_wal_archiver_stream_schemas() {
        let ns = 1_774_444_800_000_000_000;
        let cases: Vec<(RecordStream, Vec<SensorRecord>, &str)> = vec![
            (
                RecordStream::Hr,
                vec![SensorRecord::Hr(HrFrame {
                    meta: meta("h10_TEST", "session", ns),
                    hr_bpm: 61,
                    sensor_contact_supported: true,
                    sensor_contact_detected: false,
                    energy_expended: Some(9),
                    rr_intervals_ms: vec![1000.0, 750.0],
                    raw_payload: vec![0x16, 61],
                })],
                "rr_intervals_ms",
            ),
            (
                RecordStream::Rr,
                vec![SensorRecord::Rr(RrInterval {
                    meta: meta("h10_TEST", "session", ns + 1),
                    rr_index_in_frame: 0,
                    rr_ms: 1000.0,
                    hr_frame_collector_unix_ns: ns,
                })],
                "rr_ms",
            ),
            (
                RecordStream::Battery,
                vec![SensorRecord::Battery(BatterySample {
                    meta: meta("h10_TEST", "session", ns + 2),
                    battery_percent: 88,
                    reason: "initial".to_string(),
                })],
                "battery_percent",
            ),
            (
                RecordStream::Health,
                vec![SensorRecord::Health(ConnectionHealthSample {
                    meta: meta("h10_TEST", "session", ns + 3),
                    rssi_dbm: Some(-42),
                    pmd_frames: 10,
                    hr_frames: 3,
                })],
                "rssi_dbm",
            ),
            (
                RecordStream::Events,
                vec![SensorRecord::Event(CollectorEvent {
                    meta: meta("h10_TEST", "session", ns + 4),
                    event: "archiver_status".to_string(),
                    severity: "info".to_string(),
                    detail: "ok".to_string(),
                    fields_json: "{}".to_string(),
                })],
                "detail",
            ),
            (
                RecordStream::PmdTiming,
                vec![SensorRecord::PmdTiming(PmdTimingRecord {
                    meta: meta("h10_TEST", "session", ns + 5),
                    measurement: "ECG".to_string(),
                    measurement_type: 0,
                    frame_type: 0,
                    compressed: false,
                    device_frame_ts_ns: 123,
                    payload_len: 3,
                    sample_count: 1,
                    collector_delta_ms: Some(1.0),
                    device_delta_ms: Some(7.69),
                    expected_frame_delta_ms: 7.69,
                    gap_ms: Some(0.0),
                })],
                "gap_ms",
            ),
        ];

        for (stream, records, expected_field) in cases {
            let batch = record_batch_for_stream(stream, &records).expect("batch");
            assert_eq!(batch.num_rows(), 1);
            assert!(batch.schema().field_with_name(expected_field).is_ok());
        }
    }
}
