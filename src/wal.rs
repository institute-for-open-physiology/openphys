use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const WAL_MAGIC: &[u8; 8] = b"TSWALV1\n";
pub const WAL_HEADER_LEN: usize = 64;
pub const WAL_HEADER_LEN_U16: u16 = 64;
pub const WAL_CONTAINER_MAJOR: u16 = 1;
pub const WAL_CONTAINER_MINOR: u16 = 0;
pub const WAL_FRAME_HEADER_LEN: usize = 8;
pub const WAL_FRAME_HEADER_LEN_U16: u16 = 8;
pub const WAL_PAYLOAD_CODEC_CANONICAL_JSON_V1: u16 = 1;
pub const WAL_CHECKSUM_CRC32C: u16 = 1;
pub const WAL_MAX_PAYLOAD_LEN: usize = 16_777_216;
pub const DEFAULT_WAL_FSYNC_INTERVAL: Duration = Duration::from_secs(10);
const WAL_HEADER_RESERVED_LEN: usize = 16;
const WAL_OPEN_SUFFIX: &str = ".open";
const WAL_OPEN_TMP_SUFFIX: &str = ".open.tmp";
const WAL_CLOSED_SUFFIX: &str = ".closed";
#[cfg(test)]
const WAL_ARCHIVED_JSON_SUFFIX: &str = ".archived.json";
const DEFAULT_WAL_SEGMENT_DURATION: Duration = Duration::from_secs(30 * 60);
// The archiver reads a whole closed segment into memory, so segments must stay small.
const DEFAULT_WAL_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub segment_id: Uuid,
    pub segment_start_unix_ns: i64,
}

impl SegmentHeader {
    pub fn new(segment_id: Uuid, segment_start_unix_ns: i64) -> Self {
        Self {
            segment_id,
            segment_start_unix_ns,
        }
    }

    pub fn encode(&self) -> [u8; WAL_HEADER_LEN] {
        let mut out = [0_u8; WAL_HEADER_LEN];
        out[0..8].copy_from_slice(WAL_MAGIC);
        out[8..10].copy_from_slice(&WAL_HEADER_LEN_U16.to_le_bytes());
        out[10..12].copy_from_slice(&WAL_CONTAINER_MAJOR.to_le_bytes());
        out[12..14].copy_from_slice(&WAL_CONTAINER_MINOR.to_le_bytes());
        out[14..16].copy_from_slice(&WAL_FRAME_HEADER_LEN_U16.to_le_bytes());
        out[16..18].copy_from_slice(&WAL_PAYLOAD_CODEC_CANONICAL_JSON_V1.to_le_bytes());
        out[18..20].copy_from_slice(&WAL_CHECKSUM_CRC32C.to_le_bytes());
        out[20..24].copy_from_slice(&0_u32.to_le_bytes());
        out[24..40].copy_from_slice(self.segment_id.as_bytes());
        out[40..48].copy_from_slice(&self.segment_start_unix_ns.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < WAL_HEADER_LEN {
            bail!(
                "WAL header is incomplete: got {} bytes, expected {WAL_HEADER_LEN}",
                bytes.len()
            );
        }
        if bytes.get(0..8) != Some(&WAL_MAGIC[..]) {
            bail!("WAL header magic mismatch");
        }
        let header_len = read_u16_at(bytes, 8)?;
        let major = read_u16_at(bytes, 10)?;
        let minor = read_u16_at(bytes, 12)?;
        let frame_header_len = read_u16_at(bytes, 14)?;
        let payload_codec = read_u16_at(bytes, 16)?;
        let checksum_algorithm = read_u16_at(bytes, 18)?;
        let header_flags = read_u32_at(bytes, 20)?;
        let segment_id =
            Uuid::from_slice(read_slice_at(bytes, 24, 16)?).context("decode WAL segment UUID")?;
        let segment_start_unix_ns = read_i64_at(bytes, 40)?;
        let reserved = read_slice_at(bytes, 48, WAL_HEADER_RESERVED_LEN)?;

        if header_len != WAL_HEADER_LEN_U16 {
            bail!("unsupported WAL header_len {header_len}");
        }
        if major != WAL_CONTAINER_MAJOR {
            bail!("unsupported WAL container_major {major}");
        }
        if minor != WAL_CONTAINER_MINOR {
            bail!("unsupported WAL container_minor {minor}");
        }
        if frame_header_len != WAL_FRAME_HEADER_LEN_U16 {
            bail!("unsupported WAL frame_header_len {frame_header_len}");
        }
        if payload_codec != WAL_PAYLOAD_CODEC_CANONICAL_JSON_V1 {
            bail!("unsupported WAL payload_codec {payload_codec}");
        }
        if checksum_algorithm != WAL_CHECKSUM_CRC32C {
            bail!("unsupported WAL checksum_algorithm {checksum_algorithm}");
        }
        if header_flags != 0 {
            bail!("unsupported non-zero WAL header_flags {header_flags}");
        }
        if reserved.iter().any(|byte| *byte != 0) {
            bail!("unsupported non-zero WAL reserved header bytes");
        }

        Ok(Self {
            segment_id,
            segment_start_unix_ns,
        })
    }
}

fn read_slice_at(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8]> {
    bytes
        .get(offset..offset.saturating_add(len))
        .ok_or_else(|| anyhow!("buffer too short at offset {offset} for {len} bytes"))
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let slice = read_slice_at(bytes, offset, 2)?;
    let array: [u8; 2] = slice
        .try_into()
        .map_err(|_| anyhow!("u16 slice length mismatch at offset {offset}"))?;
    Ok(u16::from_le_bytes(array))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let slice = read_slice_at(bytes, offset, 4)?;
    let array: [u8; 4] = slice
        .try_into()
        .map_err(|_| anyhow!("u32 slice length mismatch at offset {offset}"))?;
    Ok(u32::from_le_bytes(array))
}

fn read_i64_at(bytes: &[u8], offset: usize) -> Result<i64> {
    let slice = read_slice_at(bytes, offset, 8)?;
    let array: [u8; 8] = slice
        .try_into()
        .map_err(|_| anyhow!("i64 slice length mismatch at offset {offset}"))?;
    Ok(i64::from_le_bytes(array))
}

pub fn crc32c(bytes: &[u8]) -> u32 {
    let poly = 0x82F6_3B78_u32;
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ poly;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF_u32
}

pub fn encode_frame_payload(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.is_empty() {
        bail!("WAL frame payload must not be empty");
    }
    if payload.len() > WAL_MAX_PAYLOAD_LEN {
        bail!(
            "WAL frame payload length {} exceeds maximum {WAL_MAX_PAYLOAD_LEN}",
            payload.len()
        );
    }
    let len_u32 = u32::try_from(payload.len()).context("payload length does not fit u32")?;
    let mut out = Vec::with_capacity(WAL_FRAME_HEADER_LEN + payload.len());
    out.extend_from_slice(&len_u32.to_le_bytes());
    out.extend_from_slice(&crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}

fn write_frame_to<W: Write>(writer: &mut W, payload: &[u8]) -> Result<usize> {
    let frame = encode_frame_payload(payload)?;
    writer.write_all(&frame).context("write WAL frame")?;
    Ok(frame.len())
}

pub fn encode_record_payload(record: &WalRecord) -> Result<Vec<u8>> {
    validate_record(record)?;
    serde_json::to_vec(record).context("serialize canonical WAL JSON record")
}

pub fn decode_record_payload(payload: &[u8]) -> Result<WalRecord> {
    if payload.is_empty() {
        bail!("WAL frame payload must not be empty");
    }
    let record: WalRecord = serde_json::from_slice(payload).context("decode WAL JSON record")?;
    validate_record(&record)?;
    Ok(record)
}

fn validate_record(record: &WalRecord) -> Result<()> {
    if record.schema_version != 1 {
        bail!(
            "unsupported WAL record schema_version {}",
            record.schema_version
        );
    }
    if record.kind != record.body.kind() {
        bail!(
            "WAL record kind {:?} does not match body kind {:?}",
            record.kind,
            record.body.kind()
        );
    }
    validate_partition_value(&record.participant_id, "participant_id")?;
    validate_partition_value(&record.source, "source")?;
    validate_body_payload_lengths(&record.body)
}

fn validate_body_payload_lengths(body: &WalBody) -> Result<()> {
    match body {
        WalBody::GattNotification(body) => validate_base64_len(
            &body.payload_b64,
            body.payload_len,
            "gatt_notification.payload",
        ),
        WalBody::GattReadResponse(body) => validate_base64_len(
            &body.payload_b64,
            body.payload_len,
            "gatt_read_response.payload",
        ),
        WalBody::GattWriteRequest(body) => validate_base64_len(
            &body.payload_b64,
            body.payload_len,
            "gatt_write_request.payload",
        ),
        WalBody::CloudIngestBatchV1(body) => {
            if body.records.is_empty() {
                bail!("cloud ingest batch envelope must contain records");
            }
            if body.record_sha256.len() != body.records.len()
                || body.record_sha256.iter().any(|value| value.len() != 64)
            {
                bail!("cloud ingest batch envelope record checksums are invalid");
            }
            for record in &body.records {
                if matches!(record.body, WalBody::CloudIngestBatchV1(_)) {
                    bail!("nested cloud ingest batch envelopes are forbidden");
                }
                validate_record(record)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_base64_len(encoded: &str, expected_len: u32, field_name: &str) -> Result<()> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .with_context(|| format!("decode base64 field {field_name}"))?;
    let expected_len = usize::try_from(expected_len).context("payload_len does not fit usize")?;
    if decoded.len() != expected_len {
        bail!(
            "base64 field {field_name} decoded length {} does not match payload_len {expected_len}",
            decoded.len()
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalRecordKind {
    SegmentStarted,
    SegmentClosed,
    SessionStarted,
    SessionEnded,
    CollectorEvent,
    RecoveryEvent,
    ScanAttempt,
    ScanResult,
    ScanError,
    ConnectAttempt,
    ConnectResult,
    ConnectError,
    DisconnectAttempt,
    DisconnectResult,
    DisconnectError,
    CentralEvent,
    RssiSample,
    RssiReadError,
    ServiceDiscoverySummary,
    SubscriptionAttempt,
    SubscriptionResult,
    SubscriptionError,
    GattNotification,
    GattReadRequest,
    GattReadResponse,
    GattReadError,
    GattWriteRequest,
    GattWriteResult,
    GattWriteError,
    PsftpSetLocalTimeAttempt,
    PsftpSetLocalTimeResult,
    ManualMarker,
    LocationFix,
    GnssStatus,
    PressureSample,
    CloudIngestBatchV1,
}

impl WalRecordKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SegmentStarted => "segment_started",
            Self::SegmentClosed => "segment_closed",
            Self::SessionStarted => "session_started",
            Self::SessionEnded => "session_ended",
            Self::CollectorEvent => "collector_event",
            Self::RecoveryEvent => "recovery_event",
            Self::ScanAttempt => "scan_attempt",
            Self::ScanResult => "scan_result",
            Self::ScanError => "scan_error",
            Self::ConnectAttempt => "connect_attempt",
            Self::ConnectResult => "connect_result",
            Self::ConnectError => "connect_error",
            Self::DisconnectAttempt => "disconnect_attempt",
            Self::DisconnectResult => "disconnect_result",
            Self::DisconnectError => "disconnect_error",
            Self::CentralEvent => "central_event",
            Self::RssiSample => "rssi_sample",
            Self::RssiReadError => "rssi_read_error",
            Self::ServiceDiscoverySummary => "service_discovery_summary",
            Self::SubscriptionAttempt => "subscription_attempt",
            Self::SubscriptionResult => "subscription_result",
            Self::SubscriptionError => "subscription_error",
            Self::GattNotification => "gatt_notification",
            Self::GattReadRequest => "gatt_read_request",
            Self::GattReadResponse => "gatt_read_response",
            Self::GattReadError => "gatt_read_error",
            Self::GattWriteRequest => "gatt_write_request",
            Self::GattWriteResult => "gatt_write_result",
            Self::GattWriteError => "gatt_write_error",
            Self::PsftpSetLocalTimeAttempt => "psftp_set_local_time_attempt",
            Self::PsftpSetLocalTimeResult => "psftp_set_local_time_result",
            Self::ManualMarker => "manual_marker",
            Self::LocationFix => "location_fix",
            Self::GnssStatus => "gnss_status",
            Self::PressureSample => "pressure_sample",
            Self::CloudIngestBatchV1 => "cloud_ingest_batch_v1",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WalRecord {
    pub schema_version: u16,
    pub record_id: Uuid,
    pub kind: WalRecordKind,
    pub participant_id: String,
    pub source: String,
    pub writer_id: String,
    pub writer_epoch_id: Uuid,
    pub session_id: Uuid,
    pub connection_id: Option<Uuid>,
    pub collector_unix_ns: i64,
    pub collector_monotonic_ns: u64,
    pub physical_device_id: Option<String>,
    pub ble_peripheral_id: Option<String>,
    /// Original Android installation for cloud-ingested records. Absent on
    /// records written directly by the desktop collector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_installation_id: Option<Uuid>,
    /// Phone batch that first carried this record to the server. This makes a
    /// WAL-fsynced/SQLite-not-yet-committed crash deterministically recoverable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_batch_id: Option<Uuid>,
    /// Stable per-installation source sequence. Absent on legacy/local WAL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_source_seq: Option<i64>,
    pub body: WalBody,
}

impl WalRecord {
    pub fn new(meta: &WalRecordContext, body: WalBody) -> Self {
        Self {
            schema_version: 1,
            record_id: Uuid::new_v4(),
            kind: body.kind(),
            participant_id: meta.participant_id.clone(),
            source: meta.source.clone(),
            writer_id: meta.writer_id.clone(),
            writer_epoch_id: meta.writer_epoch_id,
            session_id: meta.session_id,
            connection_id: meta.connection_id,
            collector_unix_ns: meta.collector_unix_ns,
            collector_monotonic_ns: meta.collector_monotonic_ns,
            physical_device_id: meta.physical_device_id.clone(),
            ble_peripheral_id: meta.ble_peripheral_id.clone(),
            device_installation_id: None,
            origin_batch_id: None,
            origin_source_seq: None,
            body,
        }
    }

    #[cfg(test)]
    pub fn with_record_id(mut self, record_id: Uuid) -> Self {
        self.record_id = record_id;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecordContext {
    pub participant_id: String,
    pub source: String,
    pub writer_id: String,
    pub writer_epoch_id: Uuid,
    pub session_id: Uuid,
    pub connection_id: Option<Uuid>,
    pub collector_unix_ns: i64,
    pub collector_monotonic_ns: u64,
    pub physical_device_id: Option<String>,
    pub ble_peripheral_id: Option<String>,
}

impl WalRecordContext {
    pub fn new(
        participant_id: impl Into<String>,
        source: impl Into<String>,
        writer_id: impl Into<String>,
        writer_epoch_id: Uuid,
        session_id: Uuid,
    ) -> Result<Self> {
        let participant_id = participant_id.into();
        let source = source.into();
        validate_partition_value(&participant_id, "participant_id")?;
        validate_partition_value(&source, "source")?;
        Ok(Self {
            participant_id,
            source,
            writer_id: writer_id.into(),
            writer_epoch_id,
            session_id,
            connection_id: None,
            collector_unix_ns: 0,
            collector_monotonic_ns: 0,
            physical_device_id: None,
            ble_peripheral_id: None,
        })
    }

    pub fn at_time(mut self, collector_unix_ns: i64, collector_monotonic_ns: u64) -> Self {
        self.collector_unix_ns = collector_unix_ns;
        self.collector_monotonic_ns = collector_monotonic_ns;
        self
    }

    pub fn connection_id(mut self, connection_id: Option<Uuid>) -> Self {
        self.connection_id = connection_id;
        self
    }

    pub fn physical_device_id(mut self, physical_device_id: Option<impl Into<String>>) -> Self {
        self.physical_device_id = physical_device_id.map(Into::into);
        self
    }

    pub fn ble_peripheral_id(mut self, ble_peripheral_id: Option<impl Into<String>>) -> Self {
        self.ble_peripheral_id = ble_peripheral_id.map(Into::into);
        self
    }

    pub fn make_record(&self, body: WalBody) -> WalRecord {
        WalRecord::new(self, body)
    }
}

#[derive(Deserialize)]
struct RawWalRecordForDecode {
    schema_version: u16,
    record_id: Uuid,
    kind: WalRecordKind,
    participant_id: String,
    source: String,
    writer_id: String,
    writer_epoch_id: Uuid,
    session_id: Uuid,
    connection_id: Option<Uuid>,
    collector_unix_ns: i64,
    collector_monotonic_ns: u64,
    physical_device_id: Option<String>,
    ble_peripheral_id: Option<String>,
    #[serde(default)]
    device_installation_id: Option<Uuid>,
    #[serde(default)]
    origin_batch_id: Option<Uuid>,
    #[serde(default)]
    origin_source_seq: Option<i64>,
    body: Value,
}

impl<'de> Deserialize<'de> for WalRecord {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWalRecordForDecode::deserialize(deserializer)?;
        let body = WalBody::from_kind_and_value(raw.kind, raw.body).map_err(D::Error::custom)?;
        Ok(Self {
            schema_version: raw.schema_version,
            record_id: raw.record_id,
            kind: raw.kind,
            participant_id: raw.participant_id,
            source: raw.source,
            writer_id: raw.writer_id,
            writer_epoch_id: raw.writer_epoch_id,
            session_id: raw.session_id,
            connection_id: raw.connection_id,
            collector_unix_ns: raw.collector_unix_ns,
            collector_monotonic_ns: raw.collector_monotonic_ns,
            physical_device_id: raw.physical_device_id,
            ble_peripheral_id: raw.ble_peripheral_id,
            device_installation_id: raw.device_installation_id,
            origin_batch_id: raw.origin_batch_id,
            origin_source_seq: raw.origin_source_seq,
            body,
        })
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum WalBody {
    SegmentStarted(SegmentStartedBody),
    SegmentClosed(SegmentClosedBody),
    SessionStarted(SessionStartedBody),
    SessionEnded(SessionEndedBody),
    CollectorEvent(CollectorEventBody),
    RecoveryEvent(RecoveryEventBody),
    ScanAttempt(ScanAttemptBody),
    ScanResult(ScanResultBody),
    ScanError(ScanErrorBody),
    ConnectAttempt(ConnectAttemptBody),
    ConnectResult(ConnectResultBody),
    ConnectError(ConnectErrorBody),
    DisconnectAttempt(DisconnectAttemptBody),
    DisconnectResult(DisconnectResultBody),
    DisconnectError(DisconnectErrorBody),
    CentralEvent(CentralEventBody),
    RssiSample(RssiSampleBody),
    RssiReadError(RssiReadErrorBody),
    ServiceDiscoverySummary(ServiceDiscoverySummaryBody),
    SubscriptionAttempt(SubscriptionAttemptBody),
    SubscriptionResult(SubscriptionResultBody),
    SubscriptionError(SubscriptionErrorBody),
    GattNotification(GattNotificationBody),
    GattReadRequest(GattReadRequestBody),
    GattReadResponse(GattReadResponseBody),
    GattReadError(GattReadErrorBody),
    GattWriteRequest(GattWriteRequestBody),
    GattWriteResult(GattWriteResultBody),
    GattWriteError(GattWriteErrorBody),
    PsftpSetLocalTimeAttempt(PsftpSetLocalTimeAttemptBody),
    PsftpSetLocalTimeResult(PsftpSetLocalTimeResultBody),
    ManualMarker(ManualMarkerBody),
    LocationFix(LocationFixBody),
    GnssStatus(GnssStatusBody),
    PressureSample(PressureSampleBody),
    CloudIngestBatchV1(CloudIngestBatchV1Body),
}

impl WalBody {
    pub fn kind(&self) -> WalRecordKind {
        match self {
            Self::SegmentStarted(_) => WalRecordKind::SegmentStarted,
            Self::SegmentClosed(_) => WalRecordKind::SegmentClosed,
            Self::SessionStarted(_) => WalRecordKind::SessionStarted,
            Self::SessionEnded(_) => WalRecordKind::SessionEnded,
            Self::CollectorEvent(_) => WalRecordKind::CollectorEvent,
            Self::RecoveryEvent(_) => WalRecordKind::RecoveryEvent,
            Self::ScanAttempt(_) => WalRecordKind::ScanAttempt,
            Self::ScanResult(_) => WalRecordKind::ScanResult,
            Self::ScanError(_) => WalRecordKind::ScanError,
            Self::ConnectAttempt(_) => WalRecordKind::ConnectAttempt,
            Self::ConnectResult(_) => WalRecordKind::ConnectResult,
            Self::ConnectError(_) => WalRecordKind::ConnectError,
            Self::DisconnectAttempt(_) => WalRecordKind::DisconnectAttempt,
            Self::DisconnectResult(_) => WalRecordKind::DisconnectResult,
            Self::DisconnectError(_) => WalRecordKind::DisconnectError,
            Self::CentralEvent(_) => WalRecordKind::CentralEvent,
            Self::RssiSample(_) => WalRecordKind::RssiSample,
            Self::RssiReadError(_) => WalRecordKind::RssiReadError,
            Self::ServiceDiscoverySummary(_) => WalRecordKind::ServiceDiscoverySummary,
            Self::SubscriptionAttempt(_) => WalRecordKind::SubscriptionAttempt,
            Self::SubscriptionResult(_) => WalRecordKind::SubscriptionResult,
            Self::SubscriptionError(_) => WalRecordKind::SubscriptionError,
            Self::GattNotification(_) => WalRecordKind::GattNotification,
            Self::GattReadRequest(_) => WalRecordKind::GattReadRequest,
            Self::GattReadResponse(_) => WalRecordKind::GattReadResponse,
            Self::GattReadError(_) => WalRecordKind::GattReadError,
            Self::GattWriteRequest(_) => WalRecordKind::GattWriteRequest,
            Self::GattWriteResult(_) => WalRecordKind::GattWriteResult,
            Self::GattWriteError(_) => WalRecordKind::GattWriteError,
            Self::PsftpSetLocalTimeAttempt(_) => WalRecordKind::PsftpSetLocalTimeAttempt,
            Self::PsftpSetLocalTimeResult(_) => WalRecordKind::PsftpSetLocalTimeResult,
            Self::ManualMarker(_) => WalRecordKind::ManualMarker,
            Self::LocationFix(_) => WalRecordKind::LocationFix,
            Self::GnssStatus(_) => WalRecordKind::GnssStatus,
            Self::PressureSample(_) => WalRecordKind::PressureSample,
            Self::CloudIngestBatchV1(_) => WalRecordKind::CloudIngestBatchV1,
        }
    }

    fn from_kind_and_value(kind: WalRecordKind, value: Value) -> Result<Self> {
        macro_rules! body {
            ($variant:ident, $ty:ty) => {
                Ok(Self::$variant(
                    serde_json::from_value::<$ty>(value)
                        .with_context(|| format!("decode {} WAL body", kind.as_str()))?,
                ))
            };
        }
        match kind {
            WalRecordKind::SegmentStarted => body!(SegmentStarted, SegmentStartedBody),
            WalRecordKind::SegmentClosed => body!(SegmentClosed, SegmentClosedBody),
            WalRecordKind::SessionStarted => body!(SessionStarted, SessionStartedBody),
            WalRecordKind::SessionEnded => body!(SessionEnded, SessionEndedBody),
            WalRecordKind::CollectorEvent => body!(CollectorEvent, CollectorEventBody),
            WalRecordKind::RecoveryEvent => body!(RecoveryEvent, RecoveryEventBody),
            WalRecordKind::ScanAttempt => body!(ScanAttempt, ScanAttemptBody),
            WalRecordKind::ScanResult => body!(ScanResult, ScanResultBody),
            WalRecordKind::ScanError => body!(ScanError, ScanErrorBody),
            WalRecordKind::ConnectAttempt => body!(ConnectAttempt, ConnectAttemptBody),
            WalRecordKind::ConnectResult => body!(ConnectResult, ConnectResultBody),
            WalRecordKind::ConnectError => body!(ConnectError, ConnectErrorBody),
            WalRecordKind::DisconnectAttempt => body!(DisconnectAttempt, DisconnectAttemptBody),
            WalRecordKind::DisconnectResult => body!(DisconnectResult, DisconnectResultBody),
            WalRecordKind::DisconnectError => body!(DisconnectError, DisconnectErrorBody),
            WalRecordKind::CentralEvent => body!(CentralEvent, CentralEventBody),
            WalRecordKind::RssiSample => body!(RssiSample, RssiSampleBody),
            WalRecordKind::RssiReadError => body!(RssiReadError, RssiReadErrorBody),
            WalRecordKind::ServiceDiscoverySummary => {
                body!(ServiceDiscoverySummary, ServiceDiscoverySummaryBody)
            }
            WalRecordKind::SubscriptionAttempt => {
                body!(SubscriptionAttempt, SubscriptionAttemptBody)
            }
            WalRecordKind::SubscriptionResult => body!(SubscriptionResult, SubscriptionResultBody),
            WalRecordKind::SubscriptionError => body!(SubscriptionError, SubscriptionErrorBody),
            WalRecordKind::GattNotification => body!(GattNotification, GattNotificationBody),
            WalRecordKind::GattReadRequest => body!(GattReadRequest, GattReadRequestBody),
            WalRecordKind::GattReadResponse => body!(GattReadResponse, GattReadResponseBody),
            WalRecordKind::GattReadError => body!(GattReadError, GattReadErrorBody),
            WalRecordKind::GattWriteRequest => body!(GattWriteRequest, GattWriteRequestBody),
            WalRecordKind::GattWriteResult => body!(GattWriteResult, GattWriteResultBody),
            WalRecordKind::GattWriteError => body!(GattWriteError, GattWriteErrorBody),
            WalRecordKind::PsftpSetLocalTimeAttempt => {
                body!(PsftpSetLocalTimeAttempt, PsftpSetLocalTimeAttemptBody)
            }
            WalRecordKind::PsftpSetLocalTimeResult => {
                body!(PsftpSetLocalTimeResult, PsftpSetLocalTimeResultBody)
            }
            WalRecordKind::ManualMarker => body!(ManualMarker, ManualMarkerBody),
            WalRecordKind::LocationFix => body!(LocationFix, LocationFixBody),
            WalRecordKind::GnssStatus => body!(GnssStatus, GnssStatusBody),
            WalRecordKind::PressureSample => body!(PressureSample, PressureSampleBody),
            WalRecordKind::CloudIngestBatchV1 => {
                body!(CloudIngestBatchV1, CloudIngestBatchV1Body)
            }
        }
    }
}

impl Serialize for WalBody {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::SegmentStarted(body) => body.serialize(serializer),
            Self::SegmentClosed(body) => body.serialize(serializer),
            Self::SessionStarted(body) => body.serialize(serializer),
            Self::SessionEnded(body) => body.serialize(serializer),
            Self::CollectorEvent(body) => body.serialize(serializer),
            Self::RecoveryEvent(body) => body.serialize(serializer),
            Self::ScanAttempt(body) => body.serialize(serializer),
            Self::ScanResult(body) => body.serialize(serializer),
            Self::ScanError(body) => body.serialize(serializer),
            Self::ConnectAttempt(body) => body.serialize(serializer),
            Self::ConnectResult(body) => body.serialize(serializer),
            Self::ConnectError(body) => body.serialize(serializer),
            Self::DisconnectAttempt(body) => body.serialize(serializer),
            Self::DisconnectResult(body) => body.serialize(serializer),
            Self::DisconnectError(body) => body.serialize(serializer),
            Self::CentralEvent(body) => body.serialize(serializer),
            Self::RssiSample(body) => body.serialize(serializer),
            Self::RssiReadError(body) => body.serialize(serializer),
            Self::ServiceDiscoverySummary(body) => body.serialize(serializer),
            Self::SubscriptionAttempt(body) => body.serialize(serializer),
            Self::SubscriptionResult(body) => body.serialize(serializer),
            Self::SubscriptionError(body) => body.serialize(serializer),
            Self::GattNotification(body) => body.serialize(serializer),
            Self::GattReadRequest(body) => body.serialize(serializer),
            Self::GattReadResponse(body) => body.serialize(serializer),
            Self::GattReadError(body) => body.serialize(serializer),
            Self::GattWriteRequest(body) => body.serialize(serializer),
            Self::GattWriteResult(body) => body.serialize(serializer),
            Self::GattWriteError(body) => body.serialize(serializer),
            Self::PsftpSetLocalTimeAttempt(body) => body.serialize(serializer),
            Self::PsftpSetLocalTimeResult(body) => body.serialize(serializer),
            Self::ManualMarker(body) => body.serialize(serializer),
            Self::LocationFix(body) => body.serialize(serializer),
            Self::GnssStatus(body) => body.serialize(serializer),
            Self::PressureSample(body) => body.serialize(serializer),
            Self::CloudIngestBatchV1(body) => body.serialize(serializer),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentStartedBody {
    pub segment_id: Uuid,
    pub segment_start_unix_ns: i64,
    pub segment_start_monotonic_ns: u64,
    pub segment_path: String,
    pub wal_container_version: String,
    pub payload_codec: String,
    pub checksum_algorithm: String,
    pub rotation_reason: String,
    pub previous_segment_id: Option<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SegmentClosedBody {
    pub segment_id: Uuid,
    pub segment_end_unix_ns: i64,
    pub reason: String,
    pub record_count_before_this: u64,
    pub size_bytes_before_this: u64,
    pub truncated_tail_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionStartedBody {
    pub software_name: String,
    pub software_version: String,
    pub git_commit: Option<String>,
    pub hostname: String,
    pub pid: u32,
    pub target: String,
    pub started_reason: String,
    pub config_json: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEndedBody {
    pub reason: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CollectorEventBody {
    pub event: String,
    pub severity: String,
    pub detail: String,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecoveryEventBody {
    pub recovered_path: String,
    pub action: String,
    pub valid_records: u64,
    pub truncated_tail_bytes: u64,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanAttemptBody {
    pub attempt_id: Uuid,
    pub target: String,
    pub scan_timeout_ms: u64,
    pub service_uuids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanResultBody {
    pub attempt_id: Uuid,
    pub peripheral_id: String,
    pub local_name: Option<String>,
    pub rssi_dbm: Option<i16>,
    pub manufacturer_data_b64: Option<String>,
    pub service_uuids: Vec<String>,
    pub matched: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanErrorBody {
    pub attempt_id: Uuid,
    pub stage: String,
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectAttemptBody {
    pub attempt_id: Uuid,
    pub peripheral_id: String,
    pub local_name: Option<String>,
    pub rssi_dbm: Option<i16>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectResultBody {
    pub attempt_id: Uuid,
    pub connected: bool,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConnectErrorBody {
    pub attempt_id: Uuid,
    pub stage: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DisconnectAttemptBody {
    pub reason: String,
    pub peripheral_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DisconnectResultBody {
    pub reason: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DisconnectErrorBody {
    pub reason: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CentralEventBody {
    pub event_name: String,
    pub peripheral_id: String,
    pub detail: String,
    pub rssi_dbm: Option<i16>,
    pub service_uuids: Vec<String>,
    pub raw_debug: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RssiSampleBody {
    pub peripheral_id: String,
    pub rssi_dbm: i16,
    pub method: String,
    pub age_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RssiReadErrorBody {
    pub peripheral_id: String,
    pub method: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceDiscoverySummaryBody {
    pub peripheral_id: String,
    pub service_count: u32,
    pub services: Vec<ServiceDiscoveryService>,
    pub device_info: Option<ServiceDiscoveryDeviceInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceDiscoveryService {
    pub service_uuid: String,
    pub characteristics: Vec<ServiceDiscoveryCharacteristic>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceDiscoveryCharacteristic {
    pub characteristic_uuid: String,
    pub properties: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceDiscoveryDeviceInfo {
    pub manufacturer: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    pub hardware_revision: Option<String>,
    pub firmware_revision: Option<String>,
    pub software_revision: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionAttemptBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub notify_kind: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionResultBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionErrorBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattNotificationBody {
    pub characteristic_uuid: String,
    pub description: String,
    pub payload_b64: String,
    pub payload_len: u32,
}

impl GattNotificationBody {
    pub fn from_payload(
        characteristic_uuid: impl Into<String>,
        description: impl Into<String>,
        payload: &[u8],
    ) -> Result<Self> {
        Ok(Self {
            characteristic_uuid: characteristic_uuid.into(),
            description: description.into(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_len: u32::try_from(payload.len())
                .context("GATT payload length does not fit u32")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattReadRequestBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattReadResponseBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub reason: String,
    pub payload_b64: String,
    pub payload_len: u32,
}

impl GattReadResponseBody {
    pub fn from_payload(
        operation_id: Uuid,
        characteristic_uuid: impl Into<String>,
        reason: impl Into<String>,
        payload: &[u8],
    ) -> Result<Self> {
        Ok(Self {
            operation_id,
            characteristic_uuid: characteristic_uuid.into(),
            reason: reason.into(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_len: u32::try_from(payload.len())
                .context("GATT payload length does not fit u32")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattReadErrorBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub reason: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattWriteRequestBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub write_type: String,
    pub reason: String,
    pub payload_b64: String,
    pub payload_len: u32,
}

impl GattWriteRequestBody {
    pub fn from_payload(
        operation_id: Uuid,
        characteristic_uuid: impl Into<String>,
        write_type: impl Into<String>,
        reason: impl Into<String>,
        payload: &[u8],
    ) -> Result<Self> {
        Ok(Self {
            operation_id,
            characteristic_uuid: characteristic_uuid.into(),
            write_type: write_type.into(),
            reason: reason.into(),
            payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
            payload_len: u32::try_from(payload.len())
                .context("GATT payload length does not fit u32")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattWriteResultBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GattWriteErrorBody {
    pub operation_id: Uuid,
    pub characteristic_uuid: String,
    pub error: String,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PsftpSetLocalTimeAttemptBody {
    pub operation_id: Uuid,
    pub local_time_iso8601: String,
    pub local_time_unix_ns: i64,
    pub tz_offset_minutes: i32,
    pub dst_offset_minutes: Option<i32>,
    pub request_payload_b64: String,
    pub control_characteristic_uuid: String,
    pub mtu_characteristic_uuid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PsftpSetLocalTimeResultBody {
    pub operation_id: Uuid,
    pub success: bool,
    pub status_code: Option<u32>,
    pub response_payload_b64: String,
    pub error: Option<String>,
    pub elapsed_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CloudIngestBatchV1Body {
    pub installation_id: Uuid,
    pub batch_id: Uuid,
    pub request_sha256: String,
    pub encoded_bytes: u64,
    pub first_source_seq: i64,
    pub last_source_seq: i64,
    pub received_unix_ms: i64,
    pub record_sha256: Vec<String>,
    pub records: Vec<WalRecord>,
}

/// Return the application records represented by one physical WAL frame.
/// Cloud ingestion writes a whole phone batch as one CRC-protected envelope so
/// readers either observe every child or none of them after a torn write.
pub fn logical_wal_records(record: &WalRecord) -> &[WalRecord] {
    match &record.body {
        WalBody::CloudIngestBatchV1(body) => &body.records,
        _ => std::slice::from_ref(record),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManualMarkerBody {
    pub event_id: Uuid,
    pub event_unix_ns: i64,
    pub event_monotonic_ns: Option<u64>,
    pub event_time_source: String,
    pub created_by: String,
    pub label: String,
    pub note: Option<String>,
    pub client_id: Option<String>,
    pub client_event_id: Option<String>,
    pub dedupe_key: String,
}

/// One raw Android `Location` fix, unmodified platform fields
/// (docs/gps_tracking_proposal_20260808.md). `altitude_m` is WGS84 ellipsoid
/// height, not height above sea level. `speed_mps` is Doppler-derived and is
/// the primary running-speed signal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LocationFixBody {
    pub provider: String,
    pub sampling_mode: String,
    pub latitude_deg: f64,
    pub longitude_deg: f64,
    pub altitude_m: Option<f64>,
    pub horizontal_accuracy_m: Option<f64>,
    pub vertical_accuracy_m: Option<f64>,
    pub speed_mps: Option<f64>,
    pub speed_accuracy_mps: Option<f64>,
    pub bearing_deg: Option<f64>,
    pub bearing_accuracy_deg: Option<f64>,
    pub device_time_unix_ms: i64,
    pub device_elapsed_realtime_ns: i64,
    pub is_mock: bool,
}

/// Low-rate GNSS quality context so bad fixes stay explainable.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GnssStatusBody {
    pub satellites_visible: u32,
    pub satellites_used: u32,
    pub cn0_used_mean_db_hz: Option<f64>,
    pub cn0_used_max_db_hz: Option<f64>,
}

/// Barometric pressure sample; relative elevation reference during runs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PressureSampleBody {
    pub pressure_hpa: f64,
    pub sensor_accuracy: i32,
    pub sensor_timestamp_ns: i64,
    pub sampling_mode: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalPathConfig {
    pub data_dir: PathBuf,
    pub participant_id: String,
    pub source: String,
}

impl WalPathConfig {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        participant_id: impl Into<String>,
        source: impl Into<String>,
    ) -> Result<Self> {
        let participant_id = participant_id.into();
        let source = source.into();
        validate_partition_value(&participant_id, "participant_id")?;
        validate_partition_value(&source, "source")?;
        Ok(Self {
            data_dir: data_dir.into(),
            participant_id,
            source,
        })
    }

    pub fn source_dir(&self) -> PathBuf {
        self.data_dir
            .join("wal")
            .join(format!("participant_id={}", self.participant_id))
            .join(format!("source={}", self.source))
    }

    pub fn date_dir(&self, segment_start_unix_ns: i64) -> Result<PathBuf> {
        Ok(self.source_dir().join(format!(
            "date={}",
            format_date_partition(segment_start_unix_ns)?
        )))
    }

    pub fn open_path(&self, segment_start_unix_ns: i64, segment_id: Uuid) -> Result<PathBuf> {
        Ok(self
            .date_dir(segment_start_unix_ns)?
            .join(open_filename(segment_start_unix_ns, segment_id)?))
    }

    pub fn open_tmp_path(&self, segment_start_unix_ns: i64, segment_id: Uuid) -> Result<PathBuf> {
        Ok(self
            .date_dir(segment_start_unix_ns)?
            .join(open_tmp_filename(segment_start_unix_ns, segment_id)?))
    }

    #[cfg(test)]
    pub fn closed_path(
        &self,
        segment_start_unix_ns: i64,
        segment_end_unix_ns: i64,
        segment_id: Uuid,
    ) -> Result<PathBuf> {
        Ok(self.date_dir(segment_start_unix_ns)?.join(closed_filename(
            segment_start_unix_ns,
            segment_end_unix_ns,
            segment_id,
        )?))
    }

    #[cfg(test)]
    pub fn sidecar_path(
        &self,
        segment_start_unix_ns: i64,
        segment_end_unix_ns: i64,
        segment_id: Uuid,
    ) -> Result<PathBuf> {
        Ok(self.date_dir(segment_start_unix_ns)?.join(sidecar_filename(
            segment_start_unix_ns,
            segment_end_unix_ns,
            segment_id,
        )?))
    }
}

pub fn validate_partition_value(value: &str, field_name: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("{field_name} partition value must not be empty");
    };
    if value.len() > 64 {
        bail!("{field_name} partition value must be at most 64 bytes");
    }
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        bail!("{field_name} partition value must start with [a-z0-9]");
    }
    if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-') {
        bail!("{field_name} partition value must match [a-z0-9][a-z0-9_-]{{0,63}}");
    }
    Ok(())
}

pub fn format_wal_timestamp_ns(unix_ns: i64) -> Result<String> {
    let secs = unix_ns.div_euclid(1_000_000_000);
    let nanos_i64 = unix_ns.rem_euclid(1_000_000_000);
    let nanos = u32::try_from(nanos_i64).context("timestamp nanosecond remainder out of range")?;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("timestamp {unix_ns} ns is outside chrono range"))?;
    Ok(format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}.{:09}Z",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        nanos
    ))
}

pub fn format_date_partition(unix_ns: i64) -> Result<String> {
    let secs = unix_ns.div_euclid(1_000_000_000);
    let nanos_i64 = unix_ns.rem_euclid(1_000_000_000);
    let nanos = u32::try_from(nanos_i64).context("timestamp nanosecond remainder out of range")?;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| anyhow!("timestamp {unix_ns} ns is outside chrono range"))?;
    Ok(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        dt.month(),
        dt.day()
    ))
}

pub fn open_filename(segment_start_unix_ns: i64, segment_id: Uuid) -> Result<String> {
    Ok(format!(
        "wal-{}-{}{WAL_OPEN_SUFFIX}",
        format_wal_timestamp_ns(segment_start_unix_ns)?,
        segment_id.hyphenated()
    ))
}

pub fn open_tmp_filename(segment_start_unix_ns: i64, segment_id: Uuid) -> Result<String> {
    Ok(format!(
        "wal-{}-{}{WAL_OPEN_TMP_SUFFIX}",
        format_wal_timestamp_ns(segment_start_unix_ns)?,
        segment_id.hyphenated()
    ))
}

pub fn closed_filename(
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    segment_id: Uuid,
) -> Result<String> {
    Ok(format!(
        "wal-{}-{}-{}{WAL_CLOSED_SUFFIX}",
        format_wal_timestamp_ns(segment_start_unix_ns)?,
        format_wal_timestamp_ns(segment_end_unix_ns)?,
        segment_id.hyphenated()
    ))
}

#[cfg(test)]
pub fn sidecar_filename(
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    segment_id: Uuid,
) -> Result<String> {
    Ok(format!(
        "wal-{}-{}-{}{WAL_ARCHIVED_JSON_SUFFIX}",
        format_wal_timestamp_ns(segment_start_unix_ns)?,
        format_wal_timestamp_ns(segment_end_unix_ns)?,
        segment_id.hyphenated()
    ))
}

pub trait SegmentIdSource {
    fn next_segment_id(&mut self) -> Result<Uuid>;
}

#[derive(Debug, Default)]
pub struct RandomSegmentIdSource;

impl SegmentIdSource for RandomSegmentIdSource {
    fn next_segment_id(&mut self) -> Result<Uuid> {
        Ok(Uuid::new_v4())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalWriterOptions {
    pub fsync_interval: Duration,
    pub max_create_attempts: usize,
}

impl Default for WalWriterOptions {
    fn default() -> Self {
        Self {
            fsync_interval: DEFAULT_WAL_FSYNC_INTERVAL,
            max_create_attempts: 16,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalWriterMetrics {
    pub records_written: u64,
    pub bytes_written: u64,
    pub fsync_count: u64,
    pub last_append_elapsed_ns: Option<u128>,
    pub last_fsync_elapsed_ns: Option<u128>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalAppendReport {
    pub payload_len: usize,
    pub frame_len: usize,
    pub encode_elapsed_ns: u128,
    pub encode_checkpoint_overlap: bool,
    pub rotation_check_elapsed_ns: u128,
    pub rotation_check_checkpoint_overlap: bool,
    pub write_elapsed_ns: u128,
    pub write_checkpoint_overlap: bool,
    pub did_fsync: bool,
    pub fsync_elapsed_ns: Option<u128>,
    pub fsync_checkpoint_overlap: Option<bool>,
    pub rotation_elapsed_ns: Option<u128>,
    pub rotation_checkpoint_overlap: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalAppendPhase {
    Encode,
    RotationCheck,
    Rotation,
    Write,
    AutoSync,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalPhaseTiming {
    pub elapsed_ns: u128,
    pub checkpoint_overlap: bool,
}

fn timed_wal_phase<T>(
    activity: &mut impl FnMut() -> (u64, bool),
    clock_ns: &mut impl FnMut() -> u128,
    operation: impl FnOnce() -> T,
) -> (T, WalPhaseTiming) {
    let before = activity();
    let started_ns = clock_ns();
    let result = operation();
    let elapsed_ns = clock_ns().saturating_sub(started_ns);
    let after = activity();
    (
        result,
        WalPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: before.1 || after.1 || before.0 != after.0,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalClosedSegment {
    pub open_path: PathBuf,
    pub closed_path: PathBuf,
    pub header: SegmentHeader,
    pub metrics: WalWriterMetrics,
}

#[derive(Debug)]
pub struct WalWriter {
    file: File,
    open_path: PathBuf,
    header: SegmentHeader,
    options: WalWriterOptions,
    last_fsync: Instant,
    metrics: WalWriterMetrics,
    #[cfg(test)]
    fail_next_write: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

impl WalWriter {
    pub fn create(
        path_config: &WalPathConfig,
        segment_start_unix_ns: i64,
        options: WalWriterOptions,
    ) -> Result<Self> {
        let mut source = RandomSegmentIdSource;
        Self::create_with_id_source(path_config, segment_start_unix_ns, options, &mut source)
    }

    pub fn create_with_id_source(
        path_config: &WalPathConfig,
        segment_start_unix_ns: i64,
        options: WalWriterOptions,
        id_source: &mut dyn SegmentIdSource,
    ) -> Result<Self> {
        let date_dir = path_config.date_dir(segment_start_unix_ns)?;
        let _hierarchy = ensure_wal_hierarchy_durable(path_config, &date_dir)?;

        let attempts = options.max_create_attempts.max(1);
        for _ in 0..attempts {
            let segment_id = id_source.next_segment_id()?;
            let tmp_path = path_config.open_tmp_path(segment_start_unix_ns, segment_id)?;
            let open_path = path_config.open_path(segment_start_unix_ns, segment_id)?;
            if open_path.exists() {
                continue;
            }

            let create_result = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path);
            let mut tmp_file = match create_result {
                Ok(file) => file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("exclusive-create WAL temp {}", tmp_path.display())
                    });
                }
            };

            let header = SegmentHeader::new(segment_id, segment_start_unix_ns);
            tmp_file
                .write_all(&header.encode())
                .with_context(|| format!("write WAL header to {}", tmp_path.display()))?;
            tmp_file
                .flush()
                .with_context(|| format!("flush WAL temp {}", tmp_path.display()))?;
            tmp_file
                .sync_all()
                .with_context(|| format!("fsync WAL temp {}", tmp_path.display()))?;
            drop(tmp_file);

            if open_path.exists() {
                let _ = fs::remove_file(&tmp_path);
                continue;
            }
            fs::rename(&tmp_path, &open_path).with_context(|| {
                format!(
                    "atomically rename WAL temp {} to open {}",
                    tmp_path.display(),
                    open_path.display()
                )
            })?;
            fsync_dir(&date_dir)?;

            let file = OpenOptions::new()
                .append(true)
                .read(true)
                .open(&open_path)
                .with_context(|| format!("open WAL segment {} for append", open_path.display()))?;
            return Ok(Self {
                file,
                open_path,
                header,
                options,
                last_fsync: Instant::now(),
                metrics: WalWriterMetrics {
                    bytes_written: WAL_HEADER_LEN as u64,
                    ..WalWriterMetrics::default()
                },
                #[cfg(test)]
                fail_next_write: false,
                #[cfg(test)]
                fail_next_sync: false,
            });
        }

        bail!(
            "failed to create collision-free WAL segment after {attempts} exclusive-create attempts"
        );
    }

    pub fn open_path(&self) -> &Path {
        &self.open_path
    }

    #[cfg(test)]
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn segment_id(&self) -> Uuid {
        self.header.segment_id
    }

    #[cfg(test)]
    pub fn metrics(&self) -> &WalWriterMetrics {
        &self.metrics
    }

    pub fn current_size_bytes(&self) -> u64 {
        self.metrics.bytes_written
    }

    pub fn append_record(&mut self, record: &WalRecord) -> Result<WalAppendReport> {
        let epoch = Instant::now();
        self.append_record_timed(
            record,
            || (0, false),
            || epoch.elapsed().as_nanos(),
            |_, _| {},
        )
    }

    fn append_record_timed(
        &mut self,
        record: &WalRecord,
        mut activity: impl FnMut() -> (u64, bool),
        mut clock_ns: impl FnMut() -> u128,
        mut observe: impl FnMut(WalAppendPhase, WalPhaseTiming),
    ) -> Result<WalAppendReport> {
        let (payload, encode) = timed_wal_phase(&mut activity, &mut clock_ns, || {
            encode_record_payload(record)
        });
        observe(WalAppendPhase::Encode, encode);
        let payload = payload?;
        let mut report =
            self.append_payload_timed(&payload, &mut activity, &mut clock_ns, &mut observe)?;
        report.encode_elapsed_ns = encode.elapsed_ns;
        report.encode_checkpoint_overlap = encode.checkpoint_overlap;
        Ok(report)
    }

    fn append_payload_timed(
        &mut self,
        payload: &[u8],
        activity: &mut impl FnMut() -> (u64, bool),
        clock_ns: &mut impl FnMut() -> u128,
        observe: &mut impl FnMut(WalAppendPhase, WalPhaseTiming),
    ) -> Result<WalAppendReport> {
        let (frame_len, write_timing) = timed_wal_phase(activity, clock_ns, || {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_write) {
                bail!("injected WAL append-write phase failure");
            }
            write_frame_to(&mut self.file, payload)
                .with_context(|| format!("append WAL frame to {}", self.open_path.display()))
        });
        observe(WalAppendPhase::Write, write_timing);
        let frame_len = frame_len
            .with_context(|| format!("append WAL frame to {}", self.open_path.display()))?;
        let write_elapsed_ns = write_timing.elapsed_ns;
        let frame_len_u64 =
            u64::try_from(frame_len).context("WAL frame length does not fit u64")?;
        self.metrics.records_written = self
            .metrics
            .records_written
            .checked_add(1)
            .ok_or_else(|| anyhow!("WAL record count overflow"))?;
        self.metrics.bytes_written = self
            .metrics
            .bytes_written
            .checked_add(frame_len_u64)
            .ok_or_else(|| anyhow!("WAL byte count overflow"))?;
        self.metrics.last_append_elapsed_ns = Some(write_elapsed_ns);

        let mut did_fsync = false;
        let mut fsync_elapsed_ns = None;
        let mut fsync_checkpoint_overlap = None;
        if self.options.fsync_interval == Duration::ZERO
            || self.last_fsync.elapsed() >= self.options.fsync_interval
        {
            let (sync, sync_timing) =
                timed_wal_phase(activity, clock_ns, || self.sync_all().map(|_| ()));
            observe(WalAppendPhase::AutoSync, sync_timing);
            sync?;
            did_fsync = true;
            fsync_elapsed_ns = Some(sync_timing.elapsed_ns);
            fsync_checkpoint_overlap = Some(sync_timing.checkpoint_overlap);
        }

        Ok(WalAppendReport {
            payload_len: payload.len(),
            frame_len,
            encode_elapsed_ns: 0,
            encode_checkpoint_overlap: false,
            rotation_check_elapsed_ns: 0,
            rotation_check_checkpoint_overlap: false,
            write_elapsed_ns,
            write_checkpoint_overlap: write_timing.checkpoint_overlap,
            did_fsync,
            fsync_elapsed_ns,
            fsync_checkpoint_overlap,
            rotation_elapsed_ns: None,
            rotation_checkpoint_overlap: None,
        })
    }

    pub fn sync_all(&mut self) -> Result<u128> {
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_sync) {
            bail!("injected WAL sync phase failure");
        }
        let start = Instant::now();
        self.file
            .flush()
            .with_context(|| format!("flush WAL segment {}", self.open_path.display()))?;
        self.file
            .sync_all()
            .with_context(|| format!("fsync WAL segment {}", self.open_path.display()))?;
        let elapsed = start.elapsed().as_nanos();
        self.last_fsync = Instant::now();
        self.metrics.fsync_count = self
            .metrics
            .fsync_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("WAL fsync count overflow"))?;
        self.metrics.last_fsync_elapsed_ns = Some(elapsed);
        Ok(elapsed)
    }

    pub fn close(
        self,
        close_context: &WalRecordContext,
        segment_end_unix_ns: i64,
        reason: impl Into<String>,
        truncated_tail_bytes: u64,
    ) -> Result<WalClosedSegment> {
        let record_count_before_this = self.metrics.records_written;
        let size_bytes_before_this = self.metrics.bytes_written;
        let body = SegmentClosedBody {
            segment_id: self.header.segment_id,
            segment_end_unix_ns,
            reason: reason.into(),
            record_count_before_this,
            size_bytes_before_this,
            truncated_tail_bytes,
        };
        let record = close_context
            .clone()
            .at_time(segment_end_unix_ns, close_context.collector_monotonic_ns)
            .make_record(WalBody::SegmentClosed(body));
        self.close_with_record(&record)
    }

    pub fn close_with_record(mut self, close_record: &WalRecord) -> Result<WalClosedSegment> {
        let WalBody::SegmentClosed(close_body) = &close_record.body else {
            bail!("WAL close record must have segment_closed body");
        };
        if close_body.segment_id != self.header.segment_id {
            bail!(
                "WAL close record segment_id {} does not match writer segment_id {}",
                close_body.segment_id,
                self.header.segment_id
            );
        }
        let closed_path = closed_path_next_to_open(
            &self.open_path,
            self.header.segment_start_unix_ns,
            close_body.segment_end_unix_ns,
            self.header.segment_id,
        )?;
        if closed_path.exists() {
            bail!(
                "WAL closed target already exists: {}",
                closed_path.display()
            );
        }

        self.append_record(close_record)?;
        self.sync_all()?;

        let open_path = self.open_path.clone();
        let header = self.header.clone();
        let metrics = self.metrics.clone();
        let parent = parent_dir(&open_path)?;
        drop(self.file);

        fs::rename(&open_path, &closed_path).with_context(|| {
            format!(
                "atomically rename WAL open {} to closed {}",
                open_path.display(),
                closed_path.display()
            )
        })?;
        fsync_dir(&parent)?;

        Ok(WalClosedSegment {
            open_path,
            closed_path,
            header,
            metrics,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WalHierarchyDurabilityReport {
    created: Vec<PathBuf>,
    synced: Vec<PathBuf>,
}

fn ensure_wal_hierarchy_durable(
    path_config: &WalPathConfig,
    date_dir: &Path,
) -> Result<WalHierarchyDurabilityReport> {
    ensure_wal_hierarchy_durable_with(path_config, date_dir, fsync_dir)
}

fn ensure_wal_hierarchy_durable_with<F>(
    path_config: &WalPathConfig,
    date_dir: &Path,
    mut sync_dir: F,
) -> Result<WalHierarchyDurabilityReport>
where
    F: FnMut(&Path) -> Result<()>,
{
    let source_dir = path_config.source_dir();
    if !date_dir.starts_with(&source_dir) {
        bail!("WAL hierarchy does not match validated path configuration");
    }
    let mut durability_chain = Vec::new();
    let mut cursor = date_dir.to_path_buf();
    loop {
        durability_chain.push(cursor.clone());
        if cursor == Path::new(".") || cursor.parent().is_none() {
            break;
        }
        let Some(parent) = cursor.parent() else {
            break;
        };
        cursor = if parent.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            parent.to_path_buf()
        };
    }

    let mut missing = durability_chain
        .iter()
        .take_while(|path| !path.exists())
        .cloned()
        .collect::<Vec<_>>();
    let existing_ancestor = durability_chain
        .get(missing.len())
        .ok_or_else(|| anyhow!("canonical WAL hierarchy has no existing durable ancestor"))?;
    if !existing_ancestor.is_dir() {
        bail!(
            "canonical WAL durable ancestor is not a directory: {}",
            existing_ancestor.display()
        );
    }
    missing.reverse();
    let mut report = WalHierarchyDurabilityReport::default();
    for directory in missing {
        match fs::create_dir(&directory) {
            Ok(()) => report.created.push(directory),
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists && directory.is_dir() =>
            {
                // Another source writer may share and create a parent such as
                // `wal` concurrently. It is still included in the mandatory
                // complete sync chain below.
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("create canonical WAL directory {}", directory.display())
                });
            }
        }
    }

    // Always repeat the entire path through the real pre-existing ancestor
    // (`.` for a relative data path, filesystem root for an absolute path).
    // A retry after an ambiguous sync failure cannot infer which entry reached
    // stable storage merely from the directories now existing.
    for directory in durability_chain {
        sync_dir(&directory)
            .with_context(|| format!("fsync canonical WAL hierarchy {}", directory.display()))?;
        report.synced.push(directory);
    }
    Ok(report)
}

fn parent_dir(path: &Path) -> Result<PathBuf> {
    path.parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("path {} has no parent directory", path.display()))
}

fn closed_path_next_to_open(
    open_path: &Path,
    segment_start_unix_ns: i64,
    segment_end_unix_ns: i64,
    segment_id: Uuid,
) -> Result<PathBuf> {
    Ok(parent_dir(open_path)?.join(closed_filename(
        segment_start_unix_ns,
        segment_end_unix_ns,
        segment_id,
    )?))
}

fn fsync_dir(path: &Path) -> Result<()> {
    let dir =
        File::open(path).with_context(|| format!("open directory {} for fsync", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("fsync directory {}", path.display()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalReadMode {
    Strict,
    TailTolerant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalTailIssue {
    IncompleteFrameHeader {
        offset: u64,
        bytes_present: usize,
    },
    InvalidPayloadLen {
        offset: u64,
        payload_len: u32,
    },
    ExcessivePayloadLen {
        offset: u64,
        payload_len: u32,
        max_payload_len: usize,
    },
    IncompletePayload {
        offset: u64,
        payload_len: u32,
        bytes_present: usize,
    },
    ChecksumMismatch {
        offset: u64,
        payload_len: u32,
        expected_crc32c: u32,
        actual_crc32c: u32,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct WalFrameRecord {
    pub frame_offset: u64,
    pub payload_len: u32,
    pub payload_crc32c: u32,
    pub payload: Vec<u8>,
    pub record: WalRecord,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WalReadResult {
    pub header: SegmentHeader,
    pub records: Vec<WalFrameRecord>,
    pub valid_end_offset: u64,
    pub file_len: u64,
    pub tail_issue: Option<WalTailIssue>,
}

impl WalReadResult {
    #[cfg(test)]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

pub fn read_wal_records(path: &Path, mode: WalReadMode) -> Result<WalReadResult> {
    let mut file = File::open(path).with_context(|| format!("open WAL {}", path.display()))?;
    let file_len = file
        .metadata()
        .with_context(|| format!("stat WAL {}", path.display()))?
        .len();
    read_wal_records_from_reader(&mut file, file_len, mode)
        .with_context(|| format!("read WAL records from {}", path.display()))
}

fn read_wal_records_from_reader<R: Read>(
    reader: &mut R,
    file_len: u64,
    mode: WalReadMode,
) -> Result<WalReadResult> {
    let mut header_bytes = [0_u8; WAL_HEADER_LEN];
    let header_read =
        read_exact_or_partial(reader, &mut header_bytes).context("read WAL header")?;
    if header_read != WAL_HEADER_LEN {
        bail!("WAL header is incomplete: got {header_read} bytes, expected {WAL_HEADER_LEN}");
    }
    let header = SegmentHeader::decode(&header_bytes)?;
    let mut records = Vec::new();
    let mut valid_end_offset = WAL_HEADER_LEN as u64;
    let mut offset = WAL_HEADER_LEN as u64;
    let mut tail_issue = None;

    loop {
        let mut frame_header = [0_u8; WAL_FRAME_HEADER_LEN];
        let frame_header_read =
            read_exact_or_partial(reader, &mut frame_header).context("read WAL frame header")?;
        if frame_header_read == 0 {
            break;
        }
        if frame_header_read != WAL_FRAME_HEADER_LEN {
            let issue = WalTailIssue::IncompleteFrameHeader {
                offset,
                bytes_present: frame_header_read,
            };
            if handle_tail_issue(mode, &issue)? {
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
            if handle_tail_issue(mode, &issue)? {
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
            if handle_tail_issue(mode, &issue)? {
                tail_issue = Some(issue);
                break;
            }
        }

        let payload_len_usize =
            usize::try_from(payload_len).context("payload_len does not fit usize")?;
        let mut payload = vec![0_u8; payload_len_usize];
        let payload_read =
            read_exact_or_partial(reader, &mut payload).context("read WAL payload")?;
        if payload_read != payload_len_usize {
            let issue = WalTailIssue::IncompletePayload {
                offset,
                payload_len,
                bytes_present: payload_read,
            };
            if handle_tail_issue(mode, &issue)? {
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
            if handle_tail_issue(mode, &issue)? {
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

    Ok(WalReadResult {
        header,
        records,
        valid_end_offset,
        file_len,
        tail_issue,
    })
}

fn read_exact_or_partial<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("read WAL bytes"),
        }
    }
    Ok(total)
}

fn handle_tail_issue(mode: WalReadMode, issue: &WalTailIssue) -> Result<bool> {
    match mode {
        WalReadMode::Strict => bail!("invalid WAL frame in strict mode: {issue:?}"),
        WalReadMode::TailTolerant => Ok(true),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalRecoveryAction {
    CloseOpen,
    DeleteTmp,
}

impl WalRecoveryAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CloseOpen => "close_open",
            Self::DeleteTmp => "delete_tmp",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecoveryReport {
    pub path: PathBuf,
    pub action: WalRecoveryAction,
    pub closed_path: Option<PathBuf>,
    pub segment_id: Option<Uuid>,
    pub segment_start_unix_ns: Option<i64>,
    pub valid_records: u64,
    pub truncated_tail_bytes: u64,
    pub message: String,
}

pub fn recover_open_segment(
    open_path: &Path,
    close_context: &WalRecordContext,
    segment_end_unix_ns: i64,
) -> Result<WalRecoveryReport> {
    let read_result = read_wal_records(open_path, WalReadMode::TailTolerant)?;
    let truncated_tail_bytes = read_result
        .file_len
        .checked_sub(read_result.valid_end_offset)
        .ok_or_else(|| anyhow!("WAL valid end offset exceeds file length"))?;
    let valid_records = u64::try_from(read_result.records.len())
        .context("valid WAL record count does not fit u64")?;
    let closed_path = closed_path_next_to_open(
        open_path,
        read_result.header.segment_start_unix_ns,
        segment_end_unix_ns,
        read_result.header.segment_id,
    )?;
    if closed_path.exists() {
        bail!(
            "recovery WAL closed target already exists: {}",
            closed_path.display()
        );
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(open_path)
        .with_context(|| format!("open stale WAL {} for recovery", open_path.display()))?;
    if truncated_tail_bytes > 0 {
        file.set_len(read_result.valid_end_offset)
            .with_context(|| format!("truncate stale WAL {}", open_path.display()))?;
    }
    file.seek(SeekFrom::End(0))
        .with_context(|| format!("seek stale WAL {} to end", open_path.display()))?;

    let close_body = SegmentClosedBody {
        segment_id: read_result.header.segment_id,
        segment_end_unix_ns,
        reason: "recovery".to_string(),
        record_count_before_this: valid_records,
        size_bytes_before_this: read_result.valid_end_offset,
        truncated_tail_bytes,
    };
    let close_record = close_context
        .clone()
        .at_time(segment_end_unix_ns, close_context.collector_monotonic_ns)
        .make_record(WalBody::SegmentClosed(close_body));
    let close_payload = encode_record_payload(&close_record)?;
    write_frame_to(&mut file, &close_payload)
        .with_context(|| format!("append recovery close record to {}", open_path.display()))?;
    file.flush()
        .with_context(|| format!("flush recovered WAL {}", open_path.display()))?;
    file.sync_all()
        .with_context(|| format!("fsync recovered WAL {}", open_path.display()))?;
    drop(file);

    if closed_path.exists() {
        bail!(
            "recovery WAL closed target already exists after fsync: {}",
            closed_path.display()
        );
    }
    fs::rename(open_path, &closed_path).with_context(|| {
        format!(
            "rename recovered WAL open {} to closed {}",
            open_path.display(),
            closed_path.display()
        )
    })?;
    fsync_dir(&parent_dir(&closed_path)?)?;

    Ok(WalRecoveryReport {
        path: open_path.to_path_buf(),
        action: WalRecoveryAction::CloseOpen,
        closed_path: Some(closed_path),
        segment_id: Some(read_result.header.segment_id),
        segment_start_unix_ns: Some(read_result.header.segment_start_unix_ns),
        valid_records,
        truncated_tail_bytes,
        message: if truncated_tail_bytes > 0 {
            "truncated invalid or partial WAL tail and closed stale open segment".to_string()
        } else {
            "closed stale open segment without truncation".to_string()
        },
    })
}

pub fn recover_stale_open_segments(
    path_config: &WalPathConfig,
    close_context: &WalRecordContext,
    segment_end_unix_ns: i64,
) -> Result<Vec<WalRecoveryReport>> {
    let source_dir = path_config.source_dir();
    if !source_dir.exists() {
        return Ok(Vec::new());
    }

    let mut tmp_paths = Vec::new();
    let mut open_paths = Vec::new();
    for date_entry in fs::read_dir(&source_dir)
        .with_context(|| format!("scan WAL source dir {}", source_dir.display()))?
    {
        let date_entry =
            date_entry.with_context(|| format!("read entry in {}", source_dir.display()))?;
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
            .with_context(|| format!("scan WAL date dir {}", date_path.display()))?
        {
            let file_entry =
                file_entry.with_context(|| format!("read entry in {}", date_path.display()))?;
            let path = file_entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.ends_with(WAL_OPEN_TMP_SUFFIX) {
                tmp_paths.push(path);
            } else if name.ends_with(WAL_OPEN_SUFFIX) {
                open_paths.push(path);
            }
        }
    }

    tmp_paths.sort();
    open_paths.sort();
    let mut reports = Vec::new();
    for tmp_path in tmp_paths {
        let parent = parent_dir(&tmp_path)?;
        fs::remove_file(&tmp_path)
            .with_context(|| format!("delete stale WAL temp {}", tmp_path.display()))?;
        fsync_dir(&parent)?;
        reports.push(WalRecoveryReport {
            path: tmp_path,
            action: WalRecoveryAction::DeleteTmp,
            closed_path: None,
            segment_id: None,
            segment_start_unix_ns: None,
            valid_records: 0,
            truncated_tail_bytes: 0,
            message: "deleted abandoned WAL .open.tmp file".to_string(),
        });
    }
    for open_path in open_paths {
        reports.push(recover_open_segment(
            &open_path,
            close_context,
            segment_end_unix_ns,
        )?);
    }
    Ok(reports)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRotationPolicy {
    pub max_duration: Option<Duration>,
    pub max_bytes: Option<u64>,
    pub max_records: Option<u64>,
}

impl Default for WalRotationPolicy {
    fn default() -> Self {
        Self {
            max_duration: Some(DEFAULT_WAL_SEGMENT_DURATION),
            max_bytes: Some(DEFAULT_WAL_SEGMENT_MAX_BYTES),
            max_records: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalSegmentManagerOptions {
    pub writer_options: WalWriterOptions,
    pub rotation_policy: WalRotationPolicy,
}

#[derive(Debug)]
struct ActiveSegment {
    writer: WalWriter,
    normal_records: u64,
}

#[derive(Debug)]
pub struct WalSegmentManager {
    path_config: WalPathConfig,
    base_context: WalRecordContext,
    options: WalSegmentManagerOptions,
    current: Option<ActiveSegment>,
    closed_segments: Vec<WalClosedSegment>,
    #[cfg(test)]
    fail_next_rotation_check: bool,
    #[cfg(test)]
    fail_next_rotation: bool,
}

impl WalSegmentManager {
    pub fn create(
        path_config: WalPathConfig,
        base_context: WalRecordContext,
        segment_start_unix_ns: i64,
        segment_start_monotonic_ns: u64,
        options: WalSegmentManagerOptions,
    ) -> Result<Self> {
        let mut manager = Self {
            path_config,
            base_context,
            options,
            current: None,
            closed_segments: Vec::new(),
            #[cfg(test)]
            fail_next_rotation_check: false,
            #[cfg(test)]
            fail_next_rotation: false,
        };
        manager.open_new_segment(
            segment_start_unix_ns,
            segment_start_monotonic_ns,
            "new_session",
            None,
        )?;
        Ok(manager)
    }

    pub fn append_record(&mut self, record: &WalRecord) -> Result<WalAppendReport> {
        let epoch = Instant::now();
        self.append_record_timed(
            record,
            || (0, false),
            || epoch.elapsed().as_nanos(),
            |_, _| {},
        )
    }

    pub fn append_record_timed(
        &mut self,
        record: &WalRecord,
        mut activity: impl FnMut() -> (u64, bool),
        mut clock_ns: impl FnMut() -> u128,
        mut observe: impl FnMut(WalAppendPhase, WalPhaseTiming),
    ) -> Result<WalAppendReport> {
        let (payload, encode) = timed_wal_phase(&mut activity, &mut clock_ns, || {
            encode_record_payload(record)
        });
        observe(WalAppendPhase::Encode, encode);
        let payload = payload?;
        let next_frame_len = u64::try_from(
            WAL_FRAME_HEADER_LEN
                .checked_add(payload.len())
                .ok_or_else(|| anyhow!("encoded WAL frame length overflow"))?,
        )
        .context("encoded WAL frame length does not fit u64")?;
        let (should_rotate, rotation_check) =
            timed_wal_phase(&mut activity, &mut clock_ns, || -> Result<bool> {
                #[cfg(test)]
                if std::mem::take(&mut self.fail_next_rotation_check) {
                    bail!("injected WAL rotation-check phase failure");
                }
                self.should_rotate_before_append(record.collector_unix_ns, next_frame_len)
            });
        observe(WalAppendPhase::RotationCheck, rotation_check);
        let should_rotate = should_rotate?;
        let mut rotation = None;
        if should_rotate {
            let (rotate, rotation_timing) =
                timed_wal_phase(&mut activity, &mut clock_ns, || -> Result<()> {
                    #[cfg(test)]
                    if std::mem::take(&mut self.fail_next_rotation) {
                        bail!("injected WAL rotation phase failure");
                    }
                    self.rotate(
                        record.collector_unix_ns,
                        record.collector_monotonic_ns,
                        "rotation",
                    )
                });
            observe(WalAppendPhase::Rotation, rotation_timing);
            rotate?;
            rotation = Some(rotation_timing);
        }
        let active = self
            .current
            .as_mut()
            .ok_or_else(|| anyhow!("WAL segment manager has no active segment"))?;
        let mut report = active.writer.append_payload_timed(
            &payload,
            &mut activity,
            &mut clock_ns,
            &mut observe,
        )?;
        report.encode_elapsed_ns = encode.elapsed_ns;
        report.encode_checkpoint_overlap = encode.checkpoint_overlap;
        report.rotation_check_elapsed_ns = rotation_check.elapsed_ns;
        report.rotation_check_checkpoint_overlap = rotation_check.checkpoint_overlap;
        report.rotation_elapsed_ns = rotation.map(|timing| timing.elapsed_ns);
        report.rotation_checkpoint_overlap = rotation.map(|timing| timing.checkpoint_overlap);
        active.normal_records = active
            .normal_records
            .checked_add(1)
            .ok_or_else(|| anyhow!("WAL segment normal record count overflow"))?;
        Ok(report)
    }

    /// Force the active segment to stable storage before acknowledging a
    /// user-visible durable operation.
    pub fn sync_all(&mut self) -> Result<u128> {
        self.current
            .as_mut()
            .ok_or_else(|| anyhow!("WAL segment manager has no active segment"))?
            .writer
            .sync_all()
    }

    pub fn current_open_path(&self) -> Option<&Path> {
        self.current
            .as_ref()
            .map(|active| active.writer.open_path())
    }

    #[cfg(test)]
    pub fn fail_next_append_write(&mut self) {
        if let Some(active) = &mut self.current {
            active.writer.fail_next_write = true;
        }
    }

    #[cfg(test)]
    pub fn fail_next_sync(&mut self) {
        if let Some(active) = &mut self.current {
            active.writer.fail_next_sync = true;
        }
    }

    #[cfg(test)]
    pub fn fail_next_rotation_check(&mut self) {
        self.fail_next_rotation_check = true;
    }

    #[cfg(test)]
    pub fn fail_next_rotation(&mut self) {
        self.fail_next_rotation = true;
    }

    #[cfg(test)]
    pub fn current_segment_id(&self) -> Option<Uuid> {
        self.current
            .as_ref()
            .map(|active| active.writer.segment_id())
    }

    #[cfg(test)]
    pub fn closed_segments(&self) -> &[WalClosedSegment] {
        &self.closed_segments
    }

    pub fn close(
        mut self,
        segment_end_unix_ns: i64,
        segment_end_monotonic_ns: u64,
        reason: impl Into<String>,
    ) -> Result<Vec<WalClosedSegment>> {
        let reason = reason.into();
        if let Some(closed) =
            self.close_current(segment_end_unix_ns, segment_end_monotonic_ns, reason)?
        {
            self.closed_segments.push(closed);
        }
        Ok(self.closed_segments)
    }

    fn should_rotate_before_append(&self, now_unix_ns: i64, next_frame_len: u64) -> Result<bool> {
        let Some(active) = &self.current else {
            return Ok(false);
        };
        if active.normal_records == 0 {
            return Ok(false);
        }
        if let Some(max_records) = self.options.rotation_policy.max_records
            && active.normal_records >= max_records.max(1)
        {
            return Ok(true);
        }
        if let Some(max_duration) = self.options.rotation_policy.max_duration {
            let elapsed_ns = now_unix_ns.saturating_sub(active.writer.header.segment_start_unix_ns);
            let max_duration_ns = i64::try_from(max_duration.as_nanos()).unwrap_or(i64::MAX);
            if elapsed_ns >= max_duration_ns {
                return Ok(true);
            }
        }
        if let Some(max_bytes) = self.options.rotation_policy.max_bytes {
            let prospective_size = active
                .writer
                .current_size_bytes()
                .checked_add(next_frame_len)
                .ok_or_else(|| anyhow!("WAL prospective size overflow"))?;
            if prospective_size > max_bytes.max(WAL_HEADER_LEN as u64) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn rotate(&mut self, now_unix_ns: i64, now_monotonic_ns: u64, reason: &str) -> Result<()> {
        let previous_segment_id = self
            .current
            .as_ref()
            .map(|active| active.writer.segment_id());
        if let Some(closed) =
            self.close_current(now_unix_ns, now_monotonic_ns, reason.to_string())?
        {
            self.closed_segments.push(closed);
        }
        self.open_new_segment(now_unix_ns, now_monotonic_ns, reason, previous_segment_id)
    }

    fn open_new_segment(
        &mut self,
        segment_start_unix_ns: i64,
        segment_start_monotonic_ns: u64,
        rotation_reason: &str,
        previous_segment_id: Option<Uuid>,
    ) -> Result<()> {
        let mut writer = WalWriter::create(
            &self.path_config,
            segment_start_unix_ns,
            self.options.writer_options.clone(),
        )?;
        let segment_id = writer.segment_id();
        let segment_path = writer.open_path().display().to_string();
        let body = SegmentStartedBody {
            segment_id,
            segment_start_unix_ns,
            segment_start_monotonic_ns,
            segment_path,
            wal_container_version: "1.0".to_string(),
            payload_codec: "canonical_json_v1".to_string(),
            checksum_algorithm: "crc32c".to_string(),
            rotation_reason: rotation_reason.to_string(),
            previous_segment_id,
        };
        let record = self
            .base_context
            .clone()
            .at_time(segment_start_unix_ns, segment_start_monotonic_ns)
            .make_record(WalBody::SegmentStarted(body));
        writer.append_record(&record)?;
        self.current = Some(ActiveSegment {
            writer,
            normal_records: 0,
        });
        Ok(())
    }

    fn close_current(
        &mut self,
        segment_end_unix_ns: i64,
        segment_end_monotonic_ns: u64,
        reason: String,
    ) -> Result<Option<WalClosedSegment>> {
        let Some(active) = self.current.take() else {
            return Ok(None);
        };
        let context = self
            .base_context
            .clone()
            .at_time(segment_end_unix_ns, segment_end_monotonic_ns);
        active
            .writer
            .close(&context, segment_end_unix_ns, reason, 0)
            .map(Some)
    }
}

#[cfg(test)]
pub fn find_closed_wal_segments(path_config: &WalPathConfig) -> Result<Vec<PathBuf>> {
    let source_dir = path_config.source_dir();
    if !source_dir.exists() {
        return Ok(Vec::new());
    }
    let mut closed_paths = Vec::new();
    for date_entry in fs::read_dir(&source_dir)
        .with_context(|| format!("scan WAL source dir {}", source_dir.display()))?
    {
        let date_entry =
            date_entry.with_context(|| format!("read entry in {}", source_dir.display()))?;
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
            .with_context(|| format!("scan WAL date dir {}", date_path.display()))?
        {
            let file_entry =
                file_entry.with_context(|| format!("read entry in {}", date_path.display()))?;
            let path = file_entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.ends_with(WAL_CLOSED_SUFFIX) {
                closed_paths.push(path);
            }
        }
    }
    closed_paths.sort();
    Ok(closed_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::io;
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::thread;

    const START_NS: i64 = 1_770_000_000_000_000_000;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "training_sensors_wal_test_{name}_{}",
                Uuid::new_v4()
            ));
            fs::create_dir_all(&path).unwrap();
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

    struct ChildGuard {
        child: Child,
    }

    impl ChildGuard {
        fn new(child: Child) -> Self {
            Self { child }
        }

        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            self.child.try_wait()
        }

        fn kill_and_wait(&mut self) -> io::Result<ExitStatus> {
            self.child.kill()?;
            self.child.wait()
        }
    }

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            // Tests must never leave the deliberately parked helper running,
            // including when an assertion or I/O unwrap unwinds the parent.
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[derive(Debug)]
    struct FixedSegmentIdSource {
        ids: Vec<Uuid>,
        index: usize,
    }

    impl FixedSegmentIdSource {
        fn new(ids: Vec<Uuid>) -> Self {
            Self { ids, index: 0 }
        }
    }

    impl SegmentIdSource for FixedSegmentIdSource {
        fn next_segment_id(&mut self) -> Result<Uuid> {
            let id = self
                .ids
                .get(self.index)
                .copied()
                .ok_or_else(|| anyhow!("no more fixed UUIDs"))?;
            self.index += 1;
            Ok(id)
        }
    }

    fn uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).unwrap()
    }

    fn h10_context() -> WalRecordContext {
        WalRecordContext::new(
            "local",
            "chest_h10",
            "training_sensors collect",
            uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        )
        .unwrap()
        .at_time(1_770_000_000_123_456_789, 123_456_789)
        .connection_id(Some(uuid("dddddddd-dddd-4ddd-8ddd-dddddddddddd")))
        .physical_device_id(Some("h10_a1b2c3d4"))
        .ble_peripheral_id(Some("72585434-07e3-d5e3-5ace-4e6c70118687"))
    }

    fn manual_context() -> WalRecordContext {
        WalRecordContext::new(
            "local",
            "manual_events",
            "training_sensors live",
            uuid("ffffffff-ffff-4fff-8fff-ffffffffffff"),
            uuid("99999999-9999-4999-8999-999999999999"),
        )
        .unwrap()
        .at_time(1_770_000_001_123_456_789, 987_654_321)
    }

    fn sample_gatt_record(sequence: u8) -> WalRecord {
        h10_context()
            .at_time(
                START_NS + i64::from(sequence) * 1_000_000_000,
                u64::from(sequence),
            )
            .make_record(WalBody::GattNotification(
                GattNotificationBody::from_payload(
                    "00002a37-0000-1000-8000-00805f9b34fb",
                    "heart_rate_measurement",
                    &[0x10, sequence],
                )
                .unwrap(),
            ))
            .with_record_id(Uuid::from_u128(u128::from(sequence) + 1))
    }

    fn test_options() -> WalWriterOptions {
        WalWriterOptions {
            fsync_interval: Duration::ZERO,
            max_create_attempts: 8,
        }
    }

    fn wal_config(dir: &TestDir) -> WalPathConfig {
        WalPathConfig::new(dir.path(), "local", "chest_h10").unwrap()
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    fn durability_chain(path: &Path) -> Vec<PathBuf> {
        let mut chain = Vec::new();
        let mut cursor = path.to_path_buf();
        loop {
            chain.push(cursor.clone());
            if cursor == Path::new(".") || cursor.parent().is_none() {
                break;
            }
            let Some(parent) = cursor.parent() else {
                break;
            };
            cursor = if parent.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                parent.to_path_buf()
            };
        }
        chain
    }

    #[test]
    fn wal_hierarchy_sync_is_bottom_up_and_repeated_after_ambiguous_failure() {
        let dir = TestDir::new("durable_hierarchy_retry");
        let data_dir = dir.path().join("new-data");
        let config = WalPathConfig::new(&data_dir, "participant", "chest_h10").unwrap();
        let date_dir = config.date_dir(START_NS).unwrap();
        let expected = durability_chain(&date_dir);

        let mut first_attempt = Vec::new();
        let error = ensure_wal_hierarchy_durable_with(&config, &date_dir, |path| {
            first_attempt.push(path.to_path_buf());
            if first_attempt.len() == 3 {
                bail!("injected directory sync failure");
            }
            Ok(())
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("injected directory sync failure"));
        assert_eq!(first_attempt, expected[..3]);
        assert!(
            date_dir.is_dir(),
            "ambiguous failure leaves created hierarchy"
        );

        let mut retry = Vec::new();
        let retry_report = ensure_wal_hierarchy_durable_with(&config, &date_dir, |path| {
            retry.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(retry, expected, "retry must resync all existing ancestors");
        assert!(retry_report.created.is_empty());
        assert_eq!(retry_report.synced, expected);

        let mut all_existing = Vec::new();
        let all_existing_report = ensure_wal_hierarchy_durable_with(&config, &date_dir, |path| {
            all_existing.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            all_existing, expected,
            "existing hierarchy is never a fast path"
        );
        assert!(all_existing_report.created.is_empty());
        assert_eq!(all_existing_report.synced, expected);
    }

    #[test]
    fn wal_hierarchy_reports_fully_new_and_partially_existing_components() {
        let dir = TestDir::new("durable_hierarchy_created");
        let new_data = dir.path().join("new/nested/data");
        let new_config = WalPathConfig::new(&new_data, "local", "chest_h10").unwrap();
        let new_date = new_config.date_dir(START_NS).unwrap();
        let new_report =
            ensure_wal_hierarchy_durable_with(&new_config, &new_date, |_| Ok(())).unwrap();
        assert_eq!(new_report.created.first(), Some(&dir.path().join("new")));
        assert_eq!(new_report.created.last(), Some(&new_date));
        assert_eq!(new_report.synced, durability_chain(&new_date));

        let partial_data = dir.path().join("partial/data");
        let partial_wal = partial_data.join("wal");
        fs::create_dir_all(&partial_wal).unwrap();
        let partial_config = WalPathConfig::new(&partial_data, "local", "chest_h10").unwrap();
        let partial_date = partial_config.date_dir(START_NS).unwrap();
        let partial_report =
            ensure_wal_hierarchy_durable_with(&partial_config, &partial_date, |_| Ok(())).unwrap();
        assert_eq!(
            partial_report.created,
            vec![
                partial_wal.join("participant_id=local"),
                partial_wal
                    .join("participant_id=local")
                    .join("source=chest_h10"),
                partial_date.clone(),
            ]
        );
        assert_eq!(partial_report.synced, durability_chain(&partial_date));
    }

    fn timed_manager(dir: &TestDir, fsync_interval: Duration) -> WalSegmentManager {
        WalSegmentManager::create(
            wal_config(dir),
            h10_context(),
            START_NS,
            0,
            WalSegmentManagerOptions {
                writer_options: WalWriterOptions {
                    fsync_interval,
                    max_create_attempts: 8,
                },
                rotation_policy: WalRotationPolicy::default(),
            },
        )
        .unwrap()
    }

    #[test]
    fn wal_subphases_use_injected_time_preserve_failure_and_track_overlap() {
        let dir = TestDir::new("timed_wal_success");
        let mut manager = timed_manager(&dir, Duration::ZERO);
        let mut now_ns = 0_u128;
        let mut generation = 0_u64;
        let mut observed = Vec::new();
        manager
            .append_record_timed(
                &sample_gatt_record(1),
                || {
                    generation = generation.saturating_add(1);
                    (generation, false)
                },
                || {
                    let value = now_ns;
                    now_ns = now_ns.saturating_add(100);
                    value
                },
                |phase, timing| observed.push((phase, timing)),
            )
            .unwrap();
        assert_eq!(
            observed.iter().map(|item| item.0).collect::<Vec<_>>(),
            vec![
                WalAppendPhase::Encode,
                WalAppendPhase::RotationCheck,
                WalAppendPhase::Write,
                WalAppendPhase::AutoSync,
            ]
        );
        assert!(
            observed
                .iter()
                .all(|item| item.1.elapsed_ns == 100 && item.1.checkpoint_overlap)
        );

        let encode_dir = TestDir::new("timed_wal_encode_failure");
        let mut encode_manager = timed_manager(&encode_dir, Duration::from_secs(60));
        let mut invalid = sample_gatt_record(1);
        invalid.schema_version = 2;
        let mut encode_observed = Vec::new();
        assert!(
            encode_manager
                .append_record_timed(
                    &invalid,
                    || (0, false),
                    || 0,
                    |phase, timing| encode_observed.push((phase, timing)),
                )
                .is_err()
        );
        assert_eq!(encode_observed.len(), 1);
        assert_eq!(encode_observed[0].0, WalAppendPhase::Encode);

        let write_dir = TestDir::new("timed_wal_write_failure");
        let mut write_manager = timed_manager(&write_dir, Duration::from_secs(60));
        write_manager.fail_next_append_write();
        let mut write_observed = Vec::new();
        assert!(
            write_manager
                .append_record_timed(
                    &sample_gatt_record(1),
                    || (0, false),
                    || 0,
                    |phase, timing| write_observed.push((phase, timing)),
                )
                .is_err()
        );
        assert_eq!(write_observed.last().unwrap().0, WalAppendPhase::Write);

        let sync_dir = TestDir::new("timed_wal_sync_failure");
        let mut sync_manager = timed_manager(&sync_dir, Duration::ZERO);
        sync_manager.fail_next_sync();
        let mut sync_observed = Vec::new();
        assert!(
            sync_manager
                .append_record_timed(
                    &sample_gatt_record(1),
                    || (0, false),
                    || 0,
                    |phase, timing| sync_observed.push((phase, timing)),
                )
                .is_err()
        );
        assert_eq!(sync_observed.last().unwrap().0, WalAppendPhase::AutoSync);

        let check_dir = TestDir::new("timed_wal_rotation_check_failure");
        let mut check_manager = timed_manager(&check_dir, Duration::from_secs(60));
        check_manager.fail_next_rotation_check();
        let mut check_observed = Vec::new();
        assert!(
            check_manager
                .append_record_timed(
                    &sample_gatt_record(1),
                    || (0, false),
                    || 0,
                    |phase, timing| check_observed.push((phase, timing)),
                )
                .is_err()
        );
        assert_eq!(
            check_observed.last().unwrap().0,
            WalAppendPhase::RotationCheck
        );

        let rotation_dir = TestDir::new("timed_wal_rotation_failure");
        let mut rotation_manager = WalSegmentManager::create(
            wal_config(&rotation_dir),
            h10_context(),
            START_NS,
            0,
            WalSegmentManagerOptions {
                writer_options: test_options(),
                rotation_policy: WalRotationPolicy {
                    max_duration: None,
                    max_bytes: None,
                    max_records: Some(0),
                },
            },
        )
        .unwrap();
        rotation_manager
            .append_record(&sample_gatt_record(1))
            .unwrap();
        rotation_manager.fail_next_rotation();
        let mut rotation_observed = Vec::new();
        assert!(
            rotation_manager
                .append_record_timed(
                    &sample_gatt_record(2),
                    || (0, false),
                    || 0,
                    |phase, timing| rotation_observed.push((phase, timing)),
                )
                .is_err()
        );
        assert_eq!(
            rotation_observed.last().unwrap().0,
            WalAppendPhase::Rotation
        );
    }

    #[test]
    fn wal_rotation_phase_has_its_own_timing_and_overlap() {
        let dir = TestDir::new("timed_wal_rotation");
        let mut manager = WalSegmentManager::create(
            wal_config(&dir),
            h10_context(),
            START_NS,
            0,
            WalSegmentManagerOptions {
                writer_options: test_options(),
                rotation_policy: WalRotationPolicy {
                    max_duration: None,
                    max_bytes: None,
                    max_records: Some(0),
                },
            },
        )
        .unwrap();
        manager.append_record(&sample_gatt_record(1)).unwrap();
        let mut now_ns = 0_u128;
        let mut generation = 0_u64;
        let mut observed = Vec::new();
        manager
            .append_record_timed(
                &sample_gatt_record(2),
                || {
                    generation = generation.saturating_add(1);
                    (generation, false)
                },
                || {
                    let value = now_ns;
                    now_ns = now_ns.saturating_add(50);
                    value
                },
                |phase, timing| observed.push((phase, timing)),
            )
            .unwrap();
        let rotation = observed
            .iter()
            .find(|item| item.0 == WalAppendPhase::Rotation)
            .unwrap();
        assert_eq!(rotation.1.elapsed_ns, 50);
        assert!(rotation.1.checkpoint_overlap);
    }

    #[test]
    fn canonical_hierarchy_survives_child_process_kill_and_reopen() {
        const CHILD_DATA_ENV: &str = "TRAINING_SENSORS_WAL_KILL_TEST_DATA";
        const CHILD_READY_ENV: &str = "TRAINING_SENSORS_WAL_KILL_TEST_READY";
        if let (Ok(data_dir), Ok(ready_path)) = (
            std::env::var(CHILD_DATA_ENV),
            std::env::var(CHILD_READY_ENV),
        ) {
            let config = WalPathConfig::new(data_dir, "local", "chest_h10").unwrap();
            let mut writer = WalWriter::create(&config, START_NS, test_options()).unwrap();
            writer.append_record(&sample_gatt_record(1)).unwrap();
            writer.sync_all().unwrap();
            fs::write(
                ready_path,
                writer.open_path().as_os_str().as_encoded_bytes(),
            )
            .unwrap();
            loop {
                thread::park();
            }
        }

        let dir = TestDir::new("durable_hierarchy_child_kill");
        let data_dir = dir.path().join("nested/data");
        let ready_path = dir.path().join("child-ready");
        let current_exe = std::env::current_exe().unwrap();
        let child = Command::new(current_exe)
            .args([
                "--exact",
                "wal::tests::canonical_hierarchy_survives_child_process_kill_and_reopen",
                "--nocapture",
            ])
            .env(CHILD_DATA_ENV, &data_dir)
            .env(CHILD_READY_ENV, &ready_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut child = ChildGuard::new(child);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready_path.exists() && Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before sync"
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready_path.exists(), "child did not publish sync sentinel");
        let status = child.kill_and_wait().unwrap();
        assert!(!status.success(), "child must be killed, not exit normally");

        let open_path = PathBuf::from(fs::read_to_string(&ready_path).unwrap());
        let config = WalPathConfig::new(&data_dir, "local", "chest_h10").unwrap();

        let reports =
            recover_stale_open_segments(&config, &h10_context(), START_NS + 2_000_000_000).unwrap();
        assert_eq!(reports.len(), 1);
        let closed_path = reports[0].closed_path.as_ref().unwrap();
        assert!(!open_path.exists());
        let read = read_wal_records(closed_path, WalReadMode::Strict).unwrap();
        assert_eq!(read.records[0].record.kind, WalRecordKind::GattNotification);
        assert_eq!(read.records[1].record.kind, WalRecordKind::SegmentClosed);
    }

    fn h10_golden_record() -> WalRecord {
        WalRecord {
            schema_version: 1,
            record_id: uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            kind: WalRecordKind::GattNotification,
            participant_id: "local".to_string(),
            source: "chest_h10".to_string(),
            writer_id: "training_sensors collect".to_string(),
            writer_epoch_id: uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            session_id: uuid("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
            connection_id: Some(uuid("dddddddd-dddd-4ddd-8ddd-dddddddddddd")),
            collector_unix_ns: 1_770_000_000_123_456_789,
            collector_monotonic_ns: 123_456_789,
            physical_device_id: Some("h10_a1b2c3d4".to_string()),
            ble_peripheral_id: Some("72585434-07e3-d5e3-5ace-4e6c70118687".to_string()),
            device_installation_id: None,
            origin_batch_id: None,
            origin_source_seq: None,
            body: WalBody::GattNotification(
                GattNotificationBody::from_payload(
                    "00002a37-0000-1000-8000-00805f9b34fb",
                    "heart_rate_measurement",
                    &[0x10, 0x48],
                )
                .unwrap(),
            ),
        }
    }

    fn manual_golden_record() -> WalRecord {
        WalRecord {
            schema_version: 1,
            record_id: uuid("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
            kind: WalRecordKind::ManualMarker,
            participant_id: "local".to_string(),
            source: "manual_events".to_string(),
            writer_id: "training_sensors live".to_string(),
            writer_epoch_id: uuid("ffffffff-ffff-4fff-8fff-ffffffffffff"),
            session_id: uuid("99999999-9999-4999-8999-999999999999"),
            connection_id: None,
            collector_unix_ns: 1_770_000_001_123_456_789,
            collector_monotonic_ns: 987_654_321,
            physical_device_id: None,
            ble_peripheral_id: None,
            device_installation_id: None,
            origin_batch_id: None,
            origin_source_seq: None,
            body: WalBody::ManualMarker(ManualMarkerBody {
                event_id: uuid("12345678-1234-4234-8234-123456789abc"),
                event_unix_ns: 1_770_000_001_123_000_000,
                event_monotonic_ns: Some(987_200_000),
                event_time_source: "dashboard_server_receive".to_string(),
                created_by: "dashboard".to_string(),
                label: "leave_building_start".to_string(),
                note: None,
                client_id: Some("macbook_dashboard".to_string()),
                client_event_id: None,
                dedupe_key: "12345678-1234-4234-8234-123456789abc".to_string(),
            }),
        }
    }

    fn one_record_wal(segment_id: Uuid, start_ns: i64, record: &WalRecord) -> Vec<u8> {
        let mut data = SegmentHeader::new(segment_id, start_ns).encode().to_vec();
        data.extend_from_slice(
            &encode_frame_payload(&encode_record_payload(record).unwrap()).unwrap(),
        );
        data
    }

    #[test]
    fn crc32c_matches_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn segment_header_encodes_decodes_and_rejects_invalid_fields() {
        let segment_id = uuid("11111111-1111-4111-8111-111111111111");
        let header = SegmentHeader::new(segment_id, START_NS);
        let encoded = header.encode();
        assert_eq!(&encoded[0..8], WAL_MAGIC);
        assert_eq!(u16::from_le_bytes(encoded[8..10].try_into().unwrap()), 64);
        assert_eq!(SegmentHeader::decode(&encoded).unwrap(), header);

        let mut bad = encoded;
        bad[0] = b'X';
        assert!(SegmentHeader::decode(&bad).is_err());
        let mut bad = encoded;
        bad[12..14].copy_from_slice(&1_u16.to_le_bytes());
        assert!(SegmentHeader::decode(&bad).is_err());
        let mut bad = encoded;
        bad[48] = 1;
        assert!(SegmentHeader::decode(&bad).is_err());
        assert!(SegmentHeader::decode(&encoded[..10]).is_err());
    }

    #[test]
    fn golden_fixtures_match_contract() {
        let h10_data = one_record_wal(
            uuid("11111111-1111-4111-8111-111111111111"),
            1_770_000_000_000_000_000,
            &h10_golden_record(),
        );
        assert_eq!(h10_data.len(), 735);
        assert_eq!(
            sha256_hex(&h10_data),
            "ed692a999ee7351c0b083861be8ad9b86dcce7a4026d21fe8593413f39912b35"
        );
        assert_eq!(
            u32::from_le_bytes(h10_data[64..68].try_into().unwrap()),
            663
        );
        assert_eq!(
            u32::from_le_bytes(h10_data[68..72].try_into().unwrap()),
            0xcba2_f767
        );
        let h10_payload = &h10_data[72..];
        let decoded = decode_record_payload(h10_payload).unwrap();
        assert_eq!(decoded.kind, WalRecordKind::GattNotification);
        assert_eq!(decoded.participant_id, "local");
        assert_eq!(decoded.source, "chest_h10");
        let WalBody::GattNotification(body) = decoded.body else {
            panic!("expected gatt_notification");
        };
        assert_eq!(body.payload_b64, "EEg=");
        assert_eq!(body.payload_len, 2);

        let manual_data = one_record_wal(
            uuid("22222222-2222-4222-8222-222222222222"),
            1_770_000_001_000_000_000,
            &manual_golden_record(),
        );
        assert_eq!(manual_data.len(), 856);
        assert_eq!(
            sha256_hex(&manual_data),
            "5772347a8c90a455ceab282ffeb107e1e7a149dd10772a5e1b3bf1f5ccaf1ef6"
        );
        assert_eq!(
            u32::from_le_bytes(manual_data[64..68].try_into().unwrap()),
            784
        );
        assert_eq!(
            u32::from_le_bytes(manual_data[68..72].try_into().unwrap()),
            0xd20c_184f
        );
        let decoded = decode_record_payload(&manual_data[72..]).unwrap();
        assert_eq!(decoded.kind, WalRecordKind::ManualMarker);
        let WalBody::ManualMarker(body) = decoded.body else {
            panic!("expected manual_marker");
        };
        assert_eq!(body.label, "leave_building_start");
    }

    #[test]
    fn all_v1_body_variants_round_trip_through_record_envelope() {
        let mut fields = BTreeMap::new();
        fields.insert("measurement".to_string(), json!("acc"));
        fields.insert("sample_rate".to_string(), json!(200));
        let operation_id = uuid("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee");
        let bodies = vec![
            WalBody::SegmentStarted(SegmentStartedBody {
                segment_id: uuid("11111111-1111-4111-8111-111111111111"),
                segment_start_unix_ns: START_NS,
                segment_start_monotonic_ns: 0,
                segment_path:
                    "data/wal/participant_id=local/source=chest_h10/date=2026-02-02/wal-x.open"
                        .to_string(),
                wal_container_version: "1.0".to_string(),
                payload_codec: "canonical_json_v1".to_string(),
                checksum_algorithm: "crc32c".to_string(),
                rotation_reason: "new_session".to_string(),
                previous_segment_id: None,
            }),
            WalBody::SegmentClosed(SegmentClosedBody {
                segment_id: uuid("11111111-1111-4111-8111-111111111111"),
                segment_end_unix_ns: START_NS + 1,
                reason: "rotation".to_string(),
                record_count_before_this: 10,
                size_bytes_before_this: 1024,
                truncated_tail_bytes: 0,
            }),
            WalBody::SessionStarted(SessionStartedBody {
                software_name: "training_sensors".to_string(),
                software_version: "0.1.0".to_string(),
                git_commit: None,
                hostname: "macbook".to_string(),
                pid: 123,
                target: "A1B2C3D4".to_string(),
                started_reason: "process_start".to_string(),
                config_json: "{}".to_string(),
            }),
            WalBody::SessionEnded(SessionEndedBody {
                reason: "ctrl_c".to_string(),
                message: "stopping".to_string(),
            }),
            WalBody::CollectorEvent(CollectorEventBody {
                event: "pmd_stream_start_confirmed".to_string(),
                severity: "info".to_string(),
                detail: "acc".to_string(),
                fields,
            }),
            WalBody::RecoveryEvent(RecoveryEventBody {
                recovered_path: "data/wal/x.open".to_string(),
                action: "close_open".to_string(),
                valid_records: 3,
                truncated_tail_bytes: 17,
                message: "truncated".to_string(),
            }),
            WalBody::ScanAttempt(ScanAttemptBody {
                attempt_id: uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                target: "A1B2C3D4".to_string(),
                scan_timeout_ms: 10_000,
                service_uuids: Vec::new(),
            }),
            WalBody::ScanResult(ScanResultBody {
                attempt_id: uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                local_name: Some("Polar H10 A1B2C3D4".to_string()),
                rssi_dbm: Some(-43),
                manufacturer_data_b64: Some("AQI=".to_string()),
                service_uuids: vec!["0000180d-0000-1000-8000-00805f9b34fb".to_string()],
                matched: true,
            }),
            WalBody::ScanError(ScanErrorBody {
                attempt_id: uuid("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                stage: "adapter_peripherals".to_string(),
                error: "timeout".to_string(),
            }),
            WalBody::ConnectAttempt(ConnectAttemptBody {
                attempt_id: uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                local_name: Some("Polar H10 A1B2C3D4".to_string()),
                rssi_dbm: Some(-43),
            }),
            WalBody::ConnectResult(ConnectResultBody {
                attempt_id: uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                connected: true,
                elapsed_ms: 42,
            }),
            WalBody::ConnectError(ConnectErrorBody {
                attempt_id: uuid("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                stage: "connect".to_string(),
                error: "failed".to_string(),
                elapsed_ms: 5_000,
            }),
            WalBody::DisconnectAttempt(DisconnectAttemptBody {
                reason: "ctrl_c".to_string(),
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
            }),
            WalBody::DisconnectResult(DisconnectResultBody {
                reason: "ctrl_c".to_string(),
                elapsed_ms: 95,
            }),
            WalBody::DisconnectError(DisconnectErrorBody {
                reason: "cleanup".to_string(),
                error: "timeout".to_string(),
                elapsed_ms: 2_000,
            }),
            WalBody::CentralEvent(CentralEventBody {
                event_name: "device_disconnected".to_string(),
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                detail: "CentralEvent::DeviceDisconnected".to_string(),
                rssi_dbm: None,
                service_uuids: Vec::new(),
                raw_debug: "DeviceDisconnected(...)".to_string(),
            }),
            WalBody::RssiSample(RssiSampleBody {
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                rssi_dbm: -57,
                method: "corebluetooth_read_rssi".to_string(),
                age_ms: 0,
            }),
            WalBody::RssiReadError(RssiReadErrorBody {
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                method: "corebluetooth_read_rssi".to_string(),
                error: "timeout".to_string(),
                elapsed_ms: 500,
            }),
            WalBody::ServiceDiscoverySummary(ServiceDiscoverySummaryBody {
                peripheral_id: "72585434-07e3-d5e3-5ace-4e6c70118687".to_string(),
                service_count: 1,
                services: vec![ServiceDiscoveryService {
                    service_uuid: "0000180d-0000-1000-8000-00805f9b34fb".to_string(),
                    characteristics: vec![ServiceDiscoveryCharacteristic {
                        characteristic_uuid: "00002a37-0000-1000-8000-00805f9b34fb".to_string(),
                        properties: vec!["notify".to_string()],
                    }],
                }],
                device_info: Some(ServiceDiscoveryDeviceInfo {
                    manufacturer: Some("Polar Electro Oy".to_string()),
                    model: Some("H10".to_string()),
                    serial: Some("A1B2C3D4".to_string()),
                    hardware_revision: Some("00760690.05".to_string()),
                    firmware_revision: Some("5.0.0".to_string()),
                    software_revision: Some("4.1.10".to_string()),
                }),
            }),
            WalBody::SubscriptionAttempt(SubscriptionAttemptBody {
                operation_id,
                characteristic_uuid: "fb005c82-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                notify_kind: "notify".to_string(),
                reason: "pmd_data".to_string(),
            }),
            WalBody::SubscriptionResult(SubscriptionResultBody {
                operation_id,
                characteristic_uuid: "fb005c82-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                elapsed_ms: 42,
            }),
            WalBody::SubscriptionError(SubscriptionErrorBody {
                operation_id,
                characteristic_uuid: "fb005c82-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                error: "Device disconnected".to_string(),
                elapsed_ms: 3_000,
            }),
            WalBody::GattNotification(
                GattNotificationBody::from_payload(
                    "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
                    "pmd_data",
                    &[1, 2, 3, 4],
                )
                .unwrap(),
            ),
            WalBody::GattReadRequest(GattReadRequestBody {
                operation_id,
                characteristic_uuid: "00002a19-0000-1000-8000-00805f9b34fb".to_string(),
                reason: "battery_level".to_string(),
            }),
            WalBody::GattReadResponse(
                GattReadResponseBody::from_payload(
                    operation_id,
                    "00002a19-0000-1000-8000-00805f9b34fb",
                    "battery_level",
                    &[100],
                )
                .unwrap(),
            ),
            WalBody::GattReadError(GattReadErrorBody {
                operation_id,
                characteristic_uuid: "00002a19-0000-1000-8000-00805f9b34fb".to_string(),
                reason: "battery_level".to_string(),
                error: "timeout".to_string(),
                elapsed_ms: 2_000,
            }),
            WalBody::GattWriteRequest(
                GattWriteRequestBody::from_payload(
                    operation_id,
                    "fb005c81-02e7-f387-1cad-8acd2d8df0c8",
                    "with_response",
                    "pmd_start_ecg",
                    &[2, 0, 0, 0],
                )
                .unwrap(),
            ),
            WalBody::GattWriteResult(GattWriteResultBody {
                operation_id,
                characteristic_uuid: "fb005c81-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                elapsed_ms: 35,
            }),
            WalBody::GattWriteError(GattWriteErrorBody {
                operation_id,
                characteristic_uuid: "fb005c81-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                error: "Device disconnected".to_string(),
                elapsed_ms: 3_000,
            }),
            WalBody::PsftpSetLocalTimeAttempt(PsftpSetLocalTimeAttemptBody {
                operation_id,
                local_time_iso8601: "2026-05-24T12:13:14.123456789+01:00".to_string(),
                local_time_unix_ns: 1_779_621_194_123_456_789,
                tz_offset_minutes: 60,
                dst_offset_minutes: None,
                request_payload_b64: "AAAA".to_string(),
                control_characteristic_uuid: "fb005c51-02e7-f387-1cad-8acd2d8df0c8".to_string(),
                mtu_characteristic_uuid: "fb005c52-02e7-f387-1cad-8acd2d8df0c8".to_string(),
            }),
            WalBody::PsftpSetLocalTimeResult(PsftpSetLocalTimeResultBody {
                operation_id,
                success: true,
                status_code: Some(0),
                response_payload_b64: "AAAA".to_string(),
                error: None,
                elapsed_ms: 121,
            }),
            WalBody::ManualMarker(ManualMarkerBody {
                event_id: uuid("12345678-1234-4234-8234-123456789abc"),
                event_unix_ns: START_NS + 1,
                event_monotonic_ns: Some(987_200_000),
                event_time_source: "dashboard_server_receive".to_string(),
                created_by: "dashboard".to_string(),
                label: "leave_building_start".to_string(),
                note: None,
                client_id: Some("macbook_dashboard".to_string()),
                client_event_id: None,
                dedupe_key: "12345678-1234-4234-8234-123456789abc".to_string(),
            }),
            WalBody::LocationFix(LocationFixBody {
                provider: "gps".to_string(),
                sampling_mode: "moving".to_string(),
                latitude_deg: 55.9445,
                longitude_deg: -3.1892,
                altitude_m: Some(102.4),
                horizontal_accuracy_m: Some(3.9),
                vertical_accuracy_m: Some(5.1),
                speed_mps: Some(3.42),
                speed_accuracy_mps: Some(0.4),
                bearing_deg: Some(271.5),
                bearing_accuracy_deg: None,
                device_time_unix_ms: 1_770_000_000_123,
                device_elapsed_realtime_ns: 987_654_321_000,
                is_mock: false,
            }),
            WalBody::LocationFix(LocationFixBody {
                provider: "fused".to_string(),
                sampling_mode: "stationary".to_string(),
                latitude_deg: 55.9445,
                longitude_deg: -3.1892,
                altitude_m: None,
                horizontal_accuracy_m: None,
                vertical_accuracy_m: None,
                speed_mps: None,
                speed_accuracy_mps: None,
                bearing_deg: None,
                bearing_accuracy_deg: None,
                device_time_unix_ms: 1_770_000_000_123,
                device_elapsed_realtime_ns: 987_654_321_000,
                is_mock: true,
            }),
            WalBody::GnssStatus(GnssStatusBody {
                satellites_visible: 21,
                satellites_used: 9,
                cn0_used_mean_db_hz: Some(34.5),
                cn0_used_max_db_hz: Some(44.0),
            }),
            WalBody::GnssStatus(GnssStatusBody {
                satellites_visible: 4,
                satellites_used: 0,
                cn0_used_mean_db_hz: None,
                cn0_used_max_db_hz: None,
            }),
            WalBody::PressureSample(PressureSampleBody {
                pressure_hpa: 1_006.32,
                sensor_accuracy: 3,
                sensor_timestamp_ns: 987_654_400_000,
                sampling_mode: "moving".to_string(),
            }),
        ];

        for (index, body) in bodies.into_iter().enumerate() {
            let context = if matches!(body, WalBody::ManualMarker(_)) {
                manual_context()
            } else {
                h10_context()
            };
            let record = context
                .make_record(body)
                .with_record_id(Uuid::from_u128(0x1000 + index as u128));
            let payload = encode_record_payload(&record).unwrap();
            let decoded = decode_record_payload(&payload).unwrap();
            assert_eq!(decoded, record);
        }
    }

    #[test]
    fn android_location_bodies_decode_from_collector_json() {
        // Field names and explicit nulls exactly as the Android encoder
        // (LocationRecords.kt, gson serializeNulls) produces them.
        let fix: LocationFixBody = serde_json::from_value(json!({
            "provider": "fused",
            "sampling_mode": "stationary",
            "latitude_deg": 55.9445,
            "longitude_deg": -3.1892,
            "altitude_m": null,
            "horizontal_accuracy_m": 12.0,
            "vertical_accuracy_m": null,
            "speed_mps": null,
            "speed_accuracy_mps": null,
            "bearing_deg": null,
            "bearing_accuracy_deg": null,
            "device_time_unix_ms": 1_770_000_000_123i64,
            "device_elapsed_realtime_ns": 987_654_321_000i64,
            "is_mock": false
        }))
        .unwrap();
        assert_eq!(fix.provider, "fused");
        assert_eq!(fix.speed_mps, None);
        let gnss: GnssStatusBody = serde_json::from_value(json!({
            "satellites_visible": 17,
            "satellites_used": 8,
            "cn0_used_mean_db_hz": 33.25,
            "cn0_used_max_db_hz": 41.0
        }))
        .unwrap();
        assert_eq!(gnss.satellites_used, 8);
        let pressure: PressureSampleBody = serde_json::from_value(json!({
            "pressure_hpa": 1001.87,
            "sensor_accuracy": 3,
            "sensor_timestamp_ns": 555_000_000i64,
            "sampling_mode": "moving"
        }))
        .unwrap();
        assert_eq!(pressure.sensor_accuracy, 3);
    }

    #[test]
    fn writer_reader_and_atomic_close_round_trip() {
        let dir = TestDir::new("writer_close");
        let config = wal_config(&dir);
        let mut writer = WalWriter::create(&config, START_NS, test_options()).unwrap();
        let open_path = writer.open_path().to_path_buf();
        let segment_id = writer.segment_id();
        assert_eq!(writer.header().segment_id, segment_id);
        let report = writer.append_record(&sample_gatt_record(1)).unwrap();
        assert!(report.did_fsync);
        assert_eq!(writer.metrics().records_written, 1);

        let closed = writer
            .close(&h10_context(), START_NS + 2_000_000_000, "ctrl_c", 0)
            .unwrap();
        assert!(!open_path.exists());
        assert!(closed.closed_path.exists());
        assert_eq!(closed.header.segment_id, segment_id);
        assert!(closed.metrics.fsync_count >= 2);

        let read = read_wal_records(&closed.closed_path, WalReadMode::Strict).unwrap();
        assert_eq!(read.header.segment_id, segment_id);
        assert_eq!(read.record_count(), 2);
        assert_eq!(read.records[0].record.kind, WalRecordKind::GattNotification);
        assert_eq!(read.records[1].record.kind, WalRecordKind::SegmentClosed);
    }

    #[test]
    fn checksum_failure_is_fatal_in_strict_mode_and_tail_tolerated_for_open_files() {
        let dir = TestDir::new("checksum_failure");
        let path = dir.path().join("bad.wal");
        let mut data = SegmentHeader::new(uuid("11111111-1111-4111-8111-111111111111"), START_NS)
            .encode()
            .to_vec();
        data.extend_from_slice(
            &encode_frame_payload(&encode_record_payload(&sample_gatt_record(1)).unwrap()).unwrap(),
        );
        let second_start = data.len();
        data.extend_from_slice(
            &encode_frame_payload(&encode_record_payload(&sample_gatt_record(2)).unwrap()).unwrap(),
        );
        let payload_start = second_start + WAL_FRAME_HEADER_LEN;
        data[payload_start] ^= 0x01;
        fs::write(&path, data).unwrap();

        assert!(read_wal_records(&path, WalReadMode::Strict).is_err());
        let tolerant = read_wal_records(&path, WalReadMode::TailTolerant).unwrap();
        assert_eq!(tolerant.record_count(), 1);
        assert!(matches!(
            tolerant.tail_issue,
            Some(WalTailIssue::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn partial_open_recovery_truncates_tail_appends_close_and_renames_closed() {
        let dir = TestDir::new("partial_recovery");
        let config = wal_config(&dir);
        let mut writer = WalWriter::create(&config, START_NS, test_options()).unwrap();
        writer.append_record(&sample_gatt_record(1)).unwrap();
        let open_path = writer.open_path().to_path_buf();
        drop(writer);
        let partial =
            encode_frame_payload(&encode_record_payload(&sample_gatt_record(2)).unwrap()).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&open_path)
            .unwrap()
            .write_all(&partial[..13])
            .unwrap();
        let size_before = fs::metadata(&open_path).unwrap().len();

        let report =
            recover_open_segment(&open_path, &h10_context(), START_NS + 3_000_000_000).unwrap();
        assert_eq!(report.action, WalRecoveryAction::CloseOpen);
        assert_eq!(report.valid_records, 1);
        assert_eq!(report.truncated_tail_bytes, 13);
        assert!(!open_path.exists());
        let closed_path = report.closed_path.unwrap();
        assert!(closed_path.exists());
        assert!(fs::metadata(&closed_path).unwrap().len() > size_before - 13);

        let read = read_wal_records(&closed_path, WalReadMode::Strict).unwrap();
        assert_eq!(read.record_count(), 2);
        let WalBody::SegmentClosed(body) = &read.records[1].record.body else {
            panic!("expected segment_closed");
        };
        assert_eq!(body.reason, "recovery");
        assert_eq!(body.truncated_tail_bytes, 13);
        assert_eq!(body.record_count_before_this, 1);
    }

    #[test]
    fn recovery_scan_deletes_tmp_and_closes_open_segments() {
        let dir = TestDir::new("recovery_scan");
        let config = wal_config(&dir);
        let mut writer = WalWriter::create(&config, START_NS, test_options()).unwrap();
        writer.append_record(&sample_gatt_record(1)).unwrap();
        let tmp_path = config
            .open_tmp_path(START_NS, uuid("99999999-9999-4999-8999-999999999999"))
            .unwrap();
        fs::write(&tmp_path, b"stale temp").unwrap();
        drop(writer);

        let reports = recover_stale_open_segments(&config, &h10_context(), START_NS + 9).unwrap();
        assert_eq!(reports.len(), 2);
        assert!(
            reports
                .iter()
                .any(|report| report.action == WalRecoveryAction::DeleteTmp)
        );
        assert!(
            reports
                .iter()
                .any(|report| report.action == WalRecoveryAction::CloseOpen)
        );
        assert!(!tmp_path.exists());
        assert_eq!(find_closed_wal_segments(&config).unwrap().len(), 1);
    }

    #[test]
    fn closed_target_collision_fails_without_overwriting_open_file() {
        let dir = TestDir::new("close_collision");
        let config = wal_config(&dir);
        let mut writer = WalWriter::create(&config, START_NS, test_options()).unwrap();
        writer.append_record(&sample_gatt_record(1)).unwrap();
        let open_path = writer.open_path().to_path_buf();
        let closed_path =
            closed_path_next_to_open(&open_path, START_NS, START_NS + 1, writer.segment_id())
                .unwrap();
        fs::write(&closed_path, b"existing closed").unwrap();

        let err = writer
            .close(&h10_context(), START_NS + 1, "writer_shutdown", 0)
            .unwrap_err();
        assert!(err.to_string().contains("closed target already exists"));
        assert!(open_path.exists());
        assert_eq!(fs::read(&closed_path).unwrap(), b"existing closed");
    }

    #[test]
    fn path_rules_date_partition_and_creation_collision_retry_work() {
        let dir = TestDir::new("paths");
        let config = wal_config(&dir);
        let id1 = uuid("11111111-1111-4111-8111-111111111111");
        let id2 = uuid("22222222-2222-4222-8222-222222222222");
        let date_dir = config.date_dir(START_NS).unwrap();
        fs::create_dir_all(&date_dir).unwrap();
        let colliding_open = config.open_path(START_NS, id1).unwrap();
        fs::write(&colliding_open, b"already here").unwrap();
        let mut ids = FixedSegmentIdSource::new(vec![id1, id2]);
        let writer =
            WalWriter::create_with_id_source(&config, START_NS, test_options(), &mut ids).unwrap();
        assert_eq!(writer.segment_id(), id2);
        assert!(writer.open_path().exists());

        assert_eq!(
            format_wal_timestamp_ns(START_NS).unwrap(),
            "20260202T024000.000000000Z"
        );
        assert_eq!(format_date_partition(START_NS).unwrap(), "2026-02-02");
        let before_midnight = 1_767_225_599_999_999_999;
        assert_eq!(
            format_date_partition(before_midnight).unwrap(),
            "2025-12-31"
        );
        let crossed_end = before_midnight + 2;
        let path = config
            .closed_path(before_midnight, crossed_end, id1)
            .unwrap();
        assert!(path.to_string_lossy().contains("date=2025-12-31"));
        assert!(validate_partition_value("local_1-ok", "participant_id").is_ok());
        assert!(validate_partition_value("Local", "participant_id").is_err());
        assert!(validate_partition_value("a.b", "participant_id").is_err());
    }

    #[test]
    fn public_string_default_and_sidecar_helpers_match_contract() {
        let kinds = [
            (WalRecordKind::SegmentStarted, "segment_started"),
            (WalRecordKind::SegmentClosed, "segment_closed"),
            (WalRecordKind::SessionStarted, "session_started"),
            (WalRecordKind::SessionEnded, "session_ended"),
            (WalRecordKind::CollectorEvent, "collector_event"),
            (WalRecordKind::RecoveryEvent, "recovery_event"),
            (WalRecordKind::ScanAttempt, "scan_attempt"),
            (WalRecordKind::ScanResult, "scan_result"),
            (WalRecordKind::ScanError, "scan_error"),
            (WalRecordKind::ConnectAttempt, "connect_attempt"),
            (WalRecordKind::ConnectResult, "connect_result"),
            (WalRecordKind::ConnectError, "connect_error"),
            (WalRecordKind::DisconnectAttempt, "disconnect_attempt"),
            (WalRecordKind::DisconnectResult, "disconnect_result"),
            (WalRecordKind::DisconnectError, "disconnect_error"),
            (WalRecordKind::CentralEvent, "central_event"),
            (WalRecordKind::RssiSample, "rssi_sample"),
            (WalRecordKind::RssiReadError, "rssi_read_error"),
            (
                WalRecordKind::ServiceDiscoverySummary,
                "service_discovery_summary",
            ),
            (WalRecordKind::SubscriptionAttempt, "subscription_attempt"),
            (WalRecordKind::SubscriptionResult, "subscription_result"),
            (WalRecordKind::SubscriptionError, "subscription_error"),
            (WalRecordKind::GattNotification, "gatt_notification"),
            (WalRecordKind::GattReadRequest, "gatt_read_request"),
            (WalRecordKind::GattReadResponse, "gatt_read_response"),
            (WalRecordKind::GattReadError, "gatt_read_error"),
            (WalRecordKind::GattWriteRequest, "gatt_write_request"),
            (WalRecordKind::GattWriteResult, "gatt_write_result"),
            (WalRecordKind::GattWriteError, "gatt_write_error"),
            (
                WalRecordKind::PsftpSetLocalTimeAttempt,
                "psftp_set_local_time_attempt",
            ),
            (
                WalRecordKind::PsftpSetLocalTimeResult,
                "psftp_set_local_time_result",
            ),
            (WalRecordKind::ManualMarker, "manual_marker"),
        ];
        for (kind, expected) in kinds {
            assert_eq!(kind.as_str(), expected);
        }
        assert_eq!(WalRecoveryAction::CloseOpen.as_str(), "close_open");
        assert_eq!(WalRecoveryAction::DeleteTmp.as_str(), "delete_tmp");

        let writer_options = WalWriterOptions::default();
        assert_eq!(writer_options.fsync_interval, Duration::from_secs(10));
        assert_eq!(writer_options.max_create_attempts, 16);
        let rotation = WalRotationPolicy::default();
        assert_eq!(rotation.max_duration, Some(Duration::from_secs(30 * 60)));
        assert_eq!(rotation.max_bytes, Some(64 * 1024 * 1024));
        assert_eq!(rotation.max_records, None);

        let dir = TestDir::new("sidecar_helpers");
        let config = wal_config(&dir);
        let segment_id = uuid("11111111-1111-4111-8111-111111111111");
        let sidecar = config
            .sidecar_path(START_NS, START_NS + 1_000_000_000, segment_id)
            .unwrap();
        assert!(sidecar.to_string_lossy().contains(".archived.json"));
        assert_eq!(
            sidecar.file_name().and_then(|name| name.to_str()).unwrap(),
            sidecar_filename(START_NS, START_NS + 1_000_000_000, segment_id).unwrap()
        );
    }

    #[test]
    fn segment_manager_rotates_ten_one_normal_record_segments() {
        let dir = TestDir::new("ten_segments");
        let config = wal_config(&dir);
        let manager_options = WalSegmentManagerOptions {
            writer_options: test_options(),
            rotation_policy: WalRotationPolicy {
                max_duration: None,
                max_bytes: None,
                max_records: Some(1),
            },
        };
        let mut manager =
            WalSegmentManager::create(config.clone(), h10_context(), START_NS, 0, manager_options)
                .unwrap();
        assert!(manager.current_open_path().unwrap().exists());
        assert!(manager.current_segment_id().is_some());
        assert!(manager.closed_segments().is_empty());
        for index in 0..10_u8 {
            manager.append_record(&sample_gatt_record(index)).unwrap();
        }
        let closed = manager
            .close(START_NS + 20_000_000_000, 20, "writer_shutdown")
            .unwrap();
        assert_eq!(closed.len(), 10);
        let found = find_closed_wal_segments(&config).unwrap();
        assert_eq!(found.len(), 10);
        for path in found {
            let read = read_wal_records(&path, WalReadMode::Strict).unwrap();
            let normal_records = read
                .records
                .iter()
                .filter(|frame| frame.record.kind == WalRecordKind::GattNotification)
                .count();
            assert_eq!(normal_records, 1);
            assert_eq!(
                read.records.first().unwrap().record.kind,
                WalRecordKind::SegmentStarted
            );
            assert_eq!(
                read.records.last().unwrap().record.kind,
                WalRecordKind::SegmentClosed
            );
        }
    }

    #[test]
    fn segment_manager_rotates_by_duration_and_bytes() {
        let dir = TestDir::new("duration_rotation");
        let config = wal_config(&dir);
        let manager_options = WalSegmentManagerOptions {
            writer_options: test_options(),
            rotation_policy: WalRotationPolicy {
                max_duration: Some(Duration::from_secs(3)),
                max_bytes: None,
                max_records: None,
            },
        };
        let mut manager =
            WalSegmentManager::create(config.clone(), h10_context(), START_NS, 0, manager_options)
                .unwrap();
        manager.append_record(&sample_gatt_record(1)).unwrap();
        manager.append_record(&sample_gatt_record(5)).unwrap();
        let closed = manager
            .close(START_NS + 6_000_000_000, 6, "writer_shutdown")
            .unwrap();
        assert_eq!(closed.len(), 2);

        let byte_dir = TestDir::new("byte_rotation");
        let byte_config = wal_config(&byte_dir);
        let byte_options = WalSegmentManagerOptions {
            writer_options: test_options(),
            rotation_policy: WalRotationPolicy {
                max_duration: None,
                max_bytes: Some(900),
                max_records: None,
            },
        };
        let mut manager =
            WalSegmentManager::create(byte_config, h10_context(), START_NS, 0, byte_options)
                .unwrap();
        manager.append_record(&sample_gatt_record(1)).unwrap();
        manager.append_record(&sample_gatt_record(2)).unwrap();
        let closed = manager
            .close(START_NS + 3_000_000_000, 3, "writer_shutdown")
            .unwrap();
        assert_eq!(closed.len(), 2);
    }

    #[test]
    fn reader_reports_invalid_lengths_and_incomplete_headers_without_truncating() {
        let dir = TestDir::new("bad_lengths");
        let path = dir.path().join("bad_len.wal");
        let mut data = SegmentHeader::new(uuid("11111111-1111-4111-8111-111111111111"), START_NS)
            .encode()
            .to_vec();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
        fs::write(&path, &data).unwrap();
        let read = read_wal_records(&path, WalReadMode::TailTolerant).unwrap();
        assert!(matches!(
            read.tail_issue,
            Some(WalTailIssue::InvalidPayloadLen { .. })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), data.len() as u64);

        let path = dir.path().join("too_large.wal");
        let mut data = SegmentHeader::new(uuid("33333333-3333-4333-8333-333333333333"), START_NS)
            .encode()
            .to_vec();
        data.extend_from_slice(&((WAL_MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
        fs::write(&path, &data).unwrap();
        let read = read_wal_records(&path, WalReadMode::TailTolerant).unwrap();
        assert!(matches!(
            read.tail_issue,
            Some(WalTailIssue::ExcessivePayloadLen { .. })
        ));

        let path = dir.path().join("partial_header.wal");
        let mut data = SegmentHeader::new(uuid("22222222-2222-4222-8222-222222222222"), START_NS)
            .encode()
            .to_vec();
        data.extend_from_slice(&[1, 2, 3]);
        fs::write(&path, &data).unwrap();
        let read = read_wal_records(&path, WalReadMode::TailTolerant).unwrap();
        assert!(matches!(
            read.tail_issue,
            Some(WalTailIssue::IncompleteFrameHeader {
                bytes_present: 3,
                ..
            })
        ));
    }

    #[test]
    fn payload_validation_rejects_empty_oversized_and_bad_base64_lengths() {
        assert!(encode_frame_payload(&[]).is_err());
        let huge = vec![0_u8; WAL_MAX_PAYLOAD_LEN + 1];
        assert!(encode_frame_payload(&huge).is_err());

        let mut record = h10_golden_record();
        let WalBody::GattNotification(body) = &mut record.body else {
            panic!("expected gatt");
        };
        body.payload_len = 999;
        assert!(encode_record_payload(&record).is_err());

        let mut record = h10_golden_record();
        record.schema_version = 2;
        assert!(encode_record_payload(&record).is_err());
        record.schema_version = 1;
        record.kind = WalRecordKind::ManualMarker;
        assert!(encode_record_payload(&record).is_err());
    }

    #[test]
    fn generic_frame_writer_supports_slow_and_failing_writers() {
        struct SlowWriter {
            bytes: Vec<u8>,
            delay: Duration,
        }

        impl Write for SlowWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                thread::sleep(self.delay);
                self.bytes.extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        struct FailingWriter {
            bytes_until_error: usize,
        }

        impl Write for FailingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                if self.bytes_until_error == 0 {
                    return Err(io::Error::other("injected failure"));
                }
                let written = self.bytes_until_error.min(buf.len());
                self.bytes_until_error -= written;
                Ok(written)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let payload = encode_record_payload(&sample_gatt_record(1)).unwrap();
        let mut slow = SlowWriter {
            bytes: Vec::new(),
            delay: Duration::from_millis(2),
        };
        let started = Instant::now();
        let frame_len = write_frame_to(&mut slow, &payload).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(2));
        assert_eq!(slow.bytes.len(), frame_len);

        let mut failing = FailingWriter {
            bytes_until_error: 4,
        };
        assert!(write_frame_to(&mut failing, &payload).is_err());
    }
}
