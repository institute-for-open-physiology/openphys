use serde::{Deserialize, Serialize};

pub const RECORD_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordStream {
    Ecg,
    Acc,
    Hr,
    Rr,
    Battery,
    Health,
    Events,
    PmdTiming,
    RawGatt,
}

impl RecordStream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ecg => "ecg",
            Self::Acc => "acc",
            Self::Hr => "hr",
            Self::Rr => "rr",
            Self::Battery => "battery",
            Self::Health => "health",
            Self::Events => "events",
            Self::PmdTiming => "pmd_timing",
            Self::RawGatt => "raw_gatt",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMeta {
    pub schema_version: u16,
    pub sensor_id: String,
    pub session_id: String,
    pub connection_id: u32,
    pub collector_unix_ns: u128,
    pub collector_monotonic_ns: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EcgSample {
    pub meta: RecordMeta,
    pub device_time_ns: u64,
    pub device_time_epoch: String,
    pub sample_index_in_frame: u32,
    pub ecg_uv: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccSample {
    pub meta: RecordMeta,
    pub device_time_ns: u64,
    pub device_time_epoch: String,
    pub sample_index_in_frame: u32,
    pub x_mg: i32,
    pub y_mg: i32,
    pub z_mg: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HrFrame {
    pub meta: RecordMeta,
    pub hr_bpm: u16,
    pub sensor_contact_supported: bool,
    pub sensor_contact_detected: bool,
    pub energy_expended: Option<u16>,
    pub rr_intervals_ms: Vec<f64>,
    pub raw_payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RrInterval {
    pub meta: RecordMeta,
    pub rr_index_in_frame: u32,
    pub rr_ms: f64,
    pub hr_frame_collector_unix_ns: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatterySample {
    pub meta: RecordMeta,
    pub battery_percent: u8,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionHealthSample {
    pub meta: RecordMeta,
    pub rssi_dbm: Option<i16>,
    pub pmd_frames: u64,
    pub hr_frames: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorEvent {
    pub meta: RecordMeta,
    pub event: String,
    pub severity: String,
    pub detail: String,
    pub fields_json: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PmdFrameTiming {
    pub collector_ns: u128,
    pub measurement_type: u8,
    pub frame_type: u8,
    pub compressed: bool,
    pub device_frame_ts: u64,
    pub payload_len: usize,
    pub sample_count: usize,
    pub collector_delta_ms: Option<f64>,
    pub device_delta_ms: Option<f64>,
    pub expected_frame_delta_ms: f64,
    pub gap_ms: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PmdTimingRecord {
    pub meta: RecordMeta,
    pub measurement: String,
    pub measurement_type: u8,
    pub frame_type: u8,
    pub compressed: bool,
    pub device_frame_ts_ns: u64,
    pub payload_len: u32,
    pub sample_count: u32,
    pub collector_delta_ms: Option<f64>,
    pub device_delta_ms: Option<f64>,
    pub expected_frame_delta_ms: f64,
    pub gap_ms: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawGattRecord {
    pub meta: RecordMeta,
    pub characteristic_uuid: String,
    pub direction: String,
    pub operation: String,
    pub payload: Vec<u8>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "stream", content = "record", rename_all = "snake_case")]
pub enum SensorRecord {
    Ecg(EcgSample),
    Acc(AccSample),
    Hr(HrFrame),
    Rr(RrInterval),
    Battery(BatterySample),
    Health(ConnectionHealthSample),
    Event(CollectorEvent),
    PmdTiming(PmdTimingRecord),
    RawGatt(RawGattRecord),
}

impl SensorRecord {
    pub fn stream(&self) -> RecordStream {
        match self {
            Self::Ecg(_) => RecordStream::Ecg,
            Self::Acc(_) => RecordStream::Acc,
            Self::Hr(_) => RecordStream::Hr,
            Self::Rr(_) => RecordStream::Rr,
            Self::Battery(_) => RecordStream::Battery,
            Self::Health(_) => RecordStream::Health,
            Self::Event(_) => RecordStream::Events,
            Self::PmdTiming(_) => RecordStream::PmdTiming,
            Self::RawGatt(_) => RecordStream::RawGatt,
        }
    }

    pub fn schema_version(&self) -> u16 {
        self.meta().schema_version
    }

    pub fn sensor_id(&self) -> &str {
        &self.meta().sensor_id
    }

    pub fn session_id(&self) -> &str {
        &self.meta().session_id
    }

    pub fn collector_unix_ns(&self) -> u128 {
        self.meta().collector_unix_ns
    }

    #[cfg(test)]
    pub fn device_time_ns(&self) -> Option<u64> {
        match self {
            Self::Ecg(record) => Some(record.device_time_ns),
            Self::Acc(record) => Some(record.device_time_ns),
            Self::PmdTiming(record) => Some(record.device_frame_ts_ns),
            _ => None,
        }
    }

    pub fn meta(&self) -> &RecordMeta {
        match self {
            Self::Ecg(record) => &record.meta,
            Self::Acc(record) => &record.meta,
            Self::Hr(record) => &record.meta,
            Self::Rr(record) => &record.meta,
            Self::Battery(record) => &record.meta,
            Self::Health(record) => &record.meta,
            Self::Event(record) => &record.meta,
            Self::PmdTiming(record) => &record.meta,
            Self::RawGatt(record) => &record.meta,
        }
    }
}

#[cfg(test)]
pub fn sensor_id_from_target(target: &str) -> String {
    let mut cleaned = target
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_uppercase();
    if cleaned.is_empty() {
        cleaned = "UNKNOWN".to_string();
    }
    format!("h10_{cleaned}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> RecordMeta {
        RecordMeta {
            schema_version: RECORD_SCHEMA_VERSION,
            sensor_id: "h10_TEST".to_string(),
            session_id: "session".to_string(),
            connection_id: 7,
            collector_unix_ns: 123,
            collector_monotonic_ns: 45,
        }
    }

    #[test]
    fn stream_names_match_wal_archive_layout() {
        assert_eq!(RecordStream::Ecg.as_str(), "ecg");
        assert_eq!(RecordStream::Acc.as_str(), "acc");
        assert_eq!(RecordStream::RawGatt.as_str(), "raw_gatt");
    }

    #[test]
    fn sensor_record_reports_common_metadata() {
        let record = SensorRecord::Ecg(EcgSample {
            meta: meta(),
            device_time_ns: 999,
            device_time_epoch: "h10_local_since_2000".to_string(),
            sample_index_in_frame: 1,
            ecg_uv: -42,
        });
        assert_eq!(record.stream(), RecordStream::Ecg);
        assert_eq!(record.schema_version(), RECORD_SCHEMA_VERSION);
        assert_eq!(record.sensor_id(), "h10_TEST");
        assert_eq!(record.session_id(), "session");
        assert_eq!(record.collector_unix_ns(), 123);
        assert_eq!(record.device_time_ns(), Some(999));
    }

    #[test]
    fn target_sensor_id_is_stable_and_sanitized() {
        assert_eq!(sensor_id_from_target("A1B2C3D4"), "h10_A1B2C3D4");
        assert_eq!(sensor_id_from_target(" a1:b2-c3 d4 "), "h10_A1B2C3D4");
        assert_eq!(sensor_id_from_target("   "), "h10_UNKNOWN");
    }

    #[test]
    fn records_are_json_serializable_for_live_snapshot_debugging() {
        let record = SensorRecord::RawGatt(RawGattRecord {
            meta: meta(),
            characteristic_uuid: "uuid".to_string(),
            direction: "inbound".to_string(),
            operation: "notification".to_string(),
            payload: vec![1, 2, 3],
            detail: "pmd".to_string(),
        });
        let json = serde_json::to_string(&record).expect("serialize record");
        assert!(json.contains("raw_gatt"));
        assert!(json.contains("notification"));
    }
}
