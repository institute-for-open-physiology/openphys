use crate::gatt::{BleCharacteristic, BleWriteType};
use crate::hr::HrData;
use crate::records::PmdFrameTiming;
use crate::util::unix_ns;
use crate::wal::{
    CentralEventBody, CollectorEventBody, ConnectAttemptBody, ConnectErrorBody, ConnectResultBody,
    DisconnectAttemptBody, DisconnectErrorBody, DisconnectResultBody, GattNotificationBody,
    GattReadErrorBody, GattReadRequestBody, GattReadResponseBody, GattWriteErrorBody,
    GattWriteRequestBody, GattWriteResultBody, PsftpSetLocalTimeAttemptBody,
    PsftpSetLocalTimeResultBody, RecoveryEventBody, RssiReadErrorBody, RssiSampleBody,
    ScanAttemptBody, ScanErrorBody, ScanResultBody, ServiceDiscoveryCharacteristic,
    ServiceDiscoveryDeviceInfo, ServiceDiscoveryService, ServiceDiscoverySummaryBody,
    SessionEndedBody, SessionStartedBody, SubscriptionAttemptBody, SubscriptionErrorBody,
    SubscriptionResultBody, WalBody, WalPathConfig, WalRecord, WalRecordContext, WalRecoveryReport,
    WalSegmentManager, WalSegmentManagerOptions, WalWriterOptions, recover_stale_open_segments,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;

pub const WAL_PARTICIPANT_ID: &str = "local";
pub const H10_WAL_SOURCE: &str = "chest_h10";
const COLLECTOR_WRITER_ID: &str = "training_sensors collect";

pub trait RecordSink {
    fn begin_connection(&mut self) -> u32 {
        0
    }

    fn session_dir(&self) -> &Path;
    fn event(&mut self, event: &str, detail: &str) -> Result<()>;
    fn raw_gatt(
        &mut self,
        collector_ns: u128,
        characteristic_uuid: &str,
        direction: &str,
        operation: &str,
        payload: &[u8],
        detail: &str,
    ) -> Result<()>;
    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()>;
    fn ecg(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        microvolts: i32,
    ) -> Result<()>;
    fn acc(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        x: i32,
        y: i32,
        z: i32,
    ) -> Result<()>;
    fn hr(&mut self, collector_ns: u128, data: &HrData, raw: &[u8]) -> Result<()>;
    fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()>;
    fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()>;

    fn set_ble_peripheral_id(&mut self, _peripheral_id: Option<&str>) -> Result<()> {
        Ok(())
    }

    fn scan_attempt(
        &mut self,
        attempt_id: Uuid,
        target: &str,
        scan_timeout_ms: u64,
        service_uuids: &[String],
    ) -> Result<()> {
        self.event(
            "scan_attempt",
            &format!(
                "attempt_id={attempt_id} target={target} timeout_ms={scan_timeout_ms} service_uuids={service_uuids:?}"
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_result(
        &mut self,
        attempt_id: Uuid,
        peripheral_id: &str,
        local_name: Option<&str>,
        rssi_dbm: Option<i16>,
        manufacturer_data: Option<&[u8]>,
        service_uuids: &[String],
        matched: bool,
    ) -> Result<()> {
        let manufacturer = manufacturer_data.map(hex::encode);
        self.event(
            "scan_result",
            &format!(
                "attempt_id={attempt_id} peripheral_id={peripheral_id} local_name={local_name:?} rssi_dbm={rssi_dbm:?} manufacturer_data={manufacturer:?} service_uuids={service_uuids:?} matched={matched}"
            ),
        )
    }

    fn scan_error(&mut self, attempt_id: Uuid, stage: &str, error: &str) -> Result<()> {
        self.event(
            "scan_error",
            &format!("attempt_id={attempt_id} stage={stage} error={error}"),
        )
    }

    fn connect_attempt(
        &mut self,
        attempt_id: Uuid,
        peripheral_id: &str,
        local_name: Option<&str>,
        rssi_dbm: Option<i16>,
    ) -> Result<()> {
        self.event(
            "connect_attempt",
            &format!(
                "attempt_id={attempt_id} peripheral_id={peripheral_id} local_name={local_name:?} rssi_dbm={rssi_dbm:?}"
            ),
        )
    }

    fn connect_result(&mut self, attempt_id: Uuid, connected: bool, elapsed_ms: u64) -> Result<()> {
        self.event(
            "connect_result",
            &format!("attempt_id={attempt_id} connected={connected} elapsed_ms={elapsed_ms}"),
        )
    }

    fn connect_error(
        &mut self,
        attempt_id: Uuid,
        stage: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "connect_error",
            &format!("attempt_id={attempt_id} stage={stage} elapsed_ms={elapsed_ms} error={error}"),
        )
    }

    fn disconnect_attempt(&mut self, reason: &str, peripheral_id: &str) -> Result<()> {
        self.event(
            "disconnect_attempt",
            &format!("reason={reason} peripheral_id={peripheral_id}"),
        )
    }

    fn disconnect_result(&mut self, reason: &str, elapsed_ms: u64) -> Result<()> {
        self.event(
            "disconnect_result",
            &format!("reason={reason} elapsed_ms={elapsed_ms}"),
        )
    }

    fn disconnect_error(&mut self, reason: &str, error: &str, elapsed_ms: u64) -> Result<()> {
        self.event(
            "disconnect_error",
            &format!("reason={reason} elapsed_ms={elapsed_ms} error={error}"),
        )
    }

    fn central_event(
        &mut self,
        event_name: &str,
        peripheral_id: &str,
        detail: &str,
        rssi_dbm: Option<i16>,
        service_uuids: &[String],
        raw_debug: &str,
    ) -> Result<()> {
        self.event(
            "central_event",
            &format!(
                "event_name={event_name} peripheral_id={peripheral_id} detail={detail} rssi_dbm={rssi_dbm:?} service_uuids={service_uuids:?} raw_debug={raw_debug}"
            ),
        )
    }

    fn rssi_sample(
        &mut self,
        peripheral_id: &str,
        rssi_dbm: i16,
        method: &str,
        age_ms: u64,
    ) -> Result<()> {
        self.event(
            "rssi_sample",
            &format!(
                "peripheral_id={peripheral_id} rssi_dbm={rssi_dbm} method={method} age_ms={age_ms}"
            ),
        )
    }

    fn rssi_read_error(
        &mut self,
        peripheral_id: &str,
        method: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "rssi_read_error",
            &format!(
                "peripheral_id={peripheral_id} method={method} elapsed_ms={elapsed_ms} error={error}"
            ),
        )
    }

    fn service_discovery_summary(
        &mut self,
        peripheral_id: &str,
        characteristics: &[BleCharacteristic],
        device_info: Option<ServiceDiscoveryDeviceInfo>,
    ) -> Result<()> {
        self.event(
            "service_discovery_summary",
            &format!(
                "peripheral_id={peripheral_id} characteristics={} device_info={device_info:?}",
                characteristics.len()
            ),
        )
    }

    fn subscription_attempt(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        notify_kind: &str,
        reason: &str,
    ) -> Result<()> {
        self.event(
            "subscription_attempt",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} notify_kind={notify_kind} reason={reason}"
            ),
        )
    }

    fn subscription_result(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "subscription_result",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} elapsed_ms={elapsed_ms}"
            ),
        )
    }

    fn subscription_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "subscription_error",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} elapsed_ms={elapsed_ms} error={error}"
            ),
        )
    }

    fn gatt_notification(
        &mut self,
        collector_ns: u128,
        characteristic_uuid: &str,
        description: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.raw_gatt(
            collector_ns,
            characteristic_uuid,
            "inbound",
            "notification",
            payload,
            description,
        )
    }

    fn gatt_read_request(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
    ) -> Result<()> {
        self.event(
            "gatt_read_request",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} reason={reason}"
            ),
        )
    }

    fn gatt_read_response(
        &mut self,
        collector_ns: u128,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
        payload: &[u8],
    ) -> Result<()> {
        let _ = operation_id;
        self.raw_gatt(
            collector_ns,
            characteristic_uuid,
            "inbound",
            "read_response",
            payload,
            reason,
        )
    }

    fn gatt_read_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "gatt_read_error",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} reason={reason} elapsed_ms={elapsed_ms} error={error}"
            ),
        )
    }

    fn gatt_write_request(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        write_type: BleWriteType,
        reason: &str,
        payload: &[u8],
    ) -> Result<()> {
        let write_type = write_type_name(write_type);
        self.raw_gatt(
            unix_ns(),
            characteristic_uuid,
            "outbound",
            "write_request",
            payload,
            &format!("operation_id={operation_id} write_type={write_type} reason={reason}"),
        )
    }

    fn gatt_write_result(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "gatt_write_result",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} elapsed_ms={elapsed_ms}"
            ),
        )
    }

    fn gatt_write_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.event(
            "gatt_write_error",
            &format!(
                "operation_id={operation_id} characteristic_uuid={characteristic_uuid} elapsed_ms={elapsed_ms} error={error}"
            ),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn psftp_set_local_time_attempt(
        &mut self,
        operation_id: Uuid,
        local_time_iso8601: &str,
        local_time_unix_ns: i64,
        tz_offset_minutes: i32,
        dst_offset_minutes: Option<i32>,
        request_payload: &[u8],
        control_characteristic_uuid: &str,
        mtu_characteristic_uuid: &str,
    ) -> Result<()> {
        let _ = (local_time_unix_ns, dst_offset_minutes, request_payload);
        self.event(
            "psftp_set_local_time_attempt",
            &format!(
                "operation_id={operation_id} local_time={local_time_iso8601} tz_offset_minutes={tz_offset_minutes} control={control_characteristic_uuid} mtu={mtu_characteristic_uuid}"
            ),
        )
    }

    fn psftp_set_local_time_result(
        &mut self,
        operation_id: Uuid,
        success: bool,
        status_code: Option<u32>,
        response_payload: &[u8],
        error: Option<&str>,
        elapsed_ms: u64,
    ) -> Result<()> {
        let _ = response_payload;
        self.event(
            "psftp_set_local_time_result",
            &format!(
                "operation_id={operation_id} success={success} status_code={status_code:?} elapsed_ms={elapsed_ms} error={error:?}"
            ),
        )
    }

    #[cfg(test)]
    #[allow(dead_code)]
    fn raw_pmd(&mut self, collector_ns: u128, bytes: &[u8]) -> Result<()> {
        self.raw_gatt(
            collector_ns,
            crate::config::PMD_DATA_UUID,
            "inbound",
            "notification",
            bytes,
            "pmd_data",
        )
    }

    fn flush_streams(&mut self) -> Result<()> {
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct CollectorSinks {
    dir: PathBuf,
    base_context: WalRecordContext,
    manager: Option<WalSegmentManager>,
    started_at: Instant,
    connection_index: u32,
    connection_id: Option<Uuid>,
    physical_device_id: String,
    ble_peripheral_id: Option<String>,
    shutdown_reason: Option<String>,
    last_health_event_ns: u128,
}

impl CollectorSinks {
    pub fn new(data_dir: &Path, target: &str) -> Result<Self> {
        fs::create_dir_all(data_dir)
            .with_context(|| format!("create collector data dir {}", data_dir.display()))?;
        let path_config = WalPathConfig::new(data_dir, WAL_PARTICIPANT_ID, H10_WAL_SOURCE)?;
        let dir = path_config.source_dir();
        let started_at = Instant::now();
        let writer_epoch_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let physical_device_id = physical_device_id_from_target(target);
        let base_context = WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            COLLECTOR_WRITER_ID,
            writer_epoch_id,
            session_id,
        )?
        .physical_device_id(Some(physical_device_id.clone()));

        let recovery_end_ns = unix_ns_i64()?;
        let recovery_context = base_context.clone().at_time(recovery_end_ns, 0);
        let recovery_reports =
            recover_stale_open_segments(&path_config, &recovery_context, recovery_end_ns)
                .context("recover stale H10 WAL segments")?;

        let start_ns = unix_ns_i64()?;
        let manager = WalSegmentManager::create(
            path_config.clone(),
            base_context.clone(),
            start_ns,
            0,
            WalSegmentManagerOptions {
                writer_options: WalWriterOptions::default(),
                ..WalSegmentManagerOptions::default()
            },
        )
        .context("open H10 WAL segment")?;

        let mut sinks = Self {
            dir,
            base_context,
            manager: Some(manager),
            started_at,
            connection_index: 0,
            connection_id: None,
            physical_device_id,
            ble_peripheral_id: None,
            shutdown_reason: None,
            last_health_event_ns: 0,
        };

        for report in &recovery_reports {
            sinks.record_recovery(report)?;
        }
        sinks.session_started(target)?;
        Ok(sinks)
    }

    pub fn current_open_wal_path(&self) -> Option<&Path> {
        self.manager
            .as_ref()
            .and_then(WalSegmentManager::current_open_path)
    }

    fn session_started(&mut self, target: &str) -> Result<()> {
        let body = SessionStartedBody {
            software_name: "training_sensors".to_string(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: option_env!("GIT_COMMIT").map(str::to_string),
            hostname: hostname_fallback(),
            pid: std::process::id(),
            target: target.to_string(),
            started_reason: "process_start".to_string(),
            config_json: "{}".to_string(),
        };
        self.append_now(WalBody::SessionStarted(body))
    }

    fn record_recovery(&mut self, report: &WalRecoveryReport) -> Result<()> {
        let body = RecoveryEventBody {
            recovered_path: report.path.display().to_string(),
            action: report.action.as_str().to_string(),
            valid_records: report.valid_records,
            truncated_tail_bytes: report.truncated_tail_bytes,
            message: report.message.clone(),
        };
        self.append_now(WalBody::RecoveryEvent(body))
    }

    fn append_now(&mut self, body: WalBody) -> Result<()> {
        self.append_at(unix_ns(), body)
    }

    fn append_at(&mut self, collector_ns: u128, body: WalBody) -> Result<()> {
        let context = self.context_at(collector_ns)?;
        let record = context.make_record(body);
        self.append_record(&record)
    }

    fn append_record(&mut self, record: &WalRecord) -> Result<()> {
        let manager = self
            .manager
            .as_mut()
            .ok_or_else(|| anyhow!("collector WAL is already closed"))?;
        manager
            .append_record(record)
            .with_context(|| format!("append {} record to collector WAL", record.kind.as_str()))?;
        Ok(())
    }

    fn context_at(&self, collector_ns: u128) -> Result<WalRecordContext> {
        let unix_ns = u128_to_i64(collector_ns)?;
        let monotonic_ns = u128_to_u64_saturating(self.started_at.elapsed().as_nanos());
        Ok(self
            .base_context
            .clone()
            .at_time(unix_ns, monotonic_ns)
            .connection_id(self.connection_id)
            .physical_device_id(Some(self.physical_device_id.clone()))
            .ble_peripheral_id(self.ble_peripheral_id.clone()))
    }

    fn collector_event_at(
        &mut self,
        collector_ns: u128,
        event: &str,
        severity: &str,
        detail: &str,
        fields: BTreeMap<String, Value>,
    ) -> Result<()> {
        let body = CollectorEventBody {
            event: event.to_string(),
            severity: severity.to_string(),
            detail: detail.to_string(),
            fields,
        };
        self.append_at(collector_ns, WalBody::CollectorEvent(body))
    }

    fn close_wal(&mut self, reason: &str, message: &str) -> Result<()> {
        if self.manager.is_none() {
            return Ok(());
        }
        let end_ns_u128 = unix_ns();
        let end_ns = u128_to_i64(end_ns_u128)?;
        let end_monotonic = u128_to_u64_saturating(self.started_at.elapsed().as_nanos());
        let body = SessionEndedBody {
            reason: reason.to_string(),
            message: message.to_string(),
        };
        self.append_at(end_ns_u128, WalBody::SessionEnded(body))?;
        let manager = self
            .manager
            .take()
            .ok_or_else(|| anyhow!("collector WAL closed during shutdown"))?;
        manager
            .close(end_ns, end_monotonic, reason)
            .context("close collector WAL segment")?;
        Ok(())
    }

    fn maybe_log_raw_progress(
        &mut self,
        collector_ns: u128,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        if collector_ns.saturating_sub(self.last_health_event_ns) < 30_000_000_000 {
            return Ok(());
        }
        self.last_health_event_ns = collector_ns;
        let mut fields = BTreeMap::new();
        fields.insert("pmd_frames".to_string(), Value::from(pmd_frames));
        fields.insert("hr_frames".to_string(), Value::from(hr_frames));
        if let Some(path) = self.current_open_wal_path() {
            fields.insert(
                "wal_path".to_string(),
                Value::from(path.display().to_string()),
            );
        }
        self.collector_event_at(collector_ns, "wal_progress", "debug", "", fields)
    }
}

impl RecordSink for CollectorSinks {
    fn begin_connection(&mut self) -> u32 {
        self.connection_index = self.connection_index.saturating_add(1);
        self.connection_id = Some(Uuid::new_v4());
        self.connection_index
    }

    fn session_dir(&self) -> &Path {
        &self.dir
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        if event == "ctrl_c" {
            self.shutdown_reason = Some("ctrl_c".to_string());
        }
        self.collector_event_at(
            unix_ns(),
            event,
            severity_for_event(event),
            detail,
            BTreeMap::new(),
        )
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
        match (direction, operation) {
            ("inbound", "notification") => self.gatt_notification(
                collector_ns,
                characteristic_uuid,
                &notification_description(characteristic_uuid, detail),
                payload,
            ),
            ("inbound", "read_response") => self.gatt_read_response(
                collector_ns,
                Uuid::new_v4(),
                characteristic_uuid,
                detail,
                payload,
            ),
            ("outbound", "write_request") => self.gatt_write_request(
                Uuid::new_v4(),
                characteristic_uuid,
                BleWriteType::WithResponse,
                detail,
                payload,
            ),
            _ => {
                let mut fields = BTreeMap::new();
                fields.insert(
                    "characteristic_uuid".to_string(),
                    Value::from(characteristic_uuid),
                );
                fields.insert("direction".to_string(), Value::from(direction));
                fields.insert("operation".to_string(), Value::from(operation));
                fields.insert("payload_hex".to_string(), Value::from(hex::encode(payload)));
                self.collector_event_at(collector_ns, "raw_gatt", "info", detail, fields)
            }
        }
    }

    fn pmd_timing(&mut self, _timing: &PmdFrameTiming) -> Result<()> {
        // WAL-era collection stores raw PMD notification payloads only. Parsed timing rows
        // are derived downstream by replay/archiver/live services.
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

    fn battery(&mut self, _collector_ns: u128, _level: u8, _reason: &str) -> Result<()> {
        Ok(())
    }

    fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        let _ = rssi;
        self.maybe_log_raw_progress(collector_ns, pmd_frames, hr_frames)
    }

    fn set_ble_peripheral_id(&mut self, peripheral_id: Option<&str>) -> Result<()> {
        self.ble_peripheral_id = peripheral_id.map(|id| id.to_ascii_lowercase());
        if peripheral_id.is_none() {
            self.connection_id = None;
        }
        Ok(())
    }

    fn scan_attempt(
        &mut self,
        attempt_id: Uuid,
        target: &str,
        scan_timeout_ms: u64,
        service_uuids: &[String],
    ) -> Result<()> {
        self.append_now(WalBody::ScanAttempt(ScanAttemptBody {
            attempt_id,
            target: target.to_string(),
            scan_timeout_ms,
            service_uuids: service_uuids.to_vec(),
        }))
    }

    fn scan_result(
        &mut self,
        attempt_id: Uuid,
        peripheral_id: &str,
        local_name: Option<&str>,
        rssi_dbm: Option<i16>,
        manufacturer_data: Option<&[u8]>,
        service_uuids: &[String],
        matched: bool,
    ) -> Result<()> {
        self.append_now(WalBody::ScanResult(ScanResultBody {
            attempt_id,
            peripheral_id: peripheral_id.to_ascii_lowercase(),
            local_name: local_name.map(str::to_string),
            rssi_dbm,
            manufacturer_data_b64: manufacturer_data
                .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes)),
            service_uuids: service_uuids.to_vec(),
            matched,
        }))
    }

    fn scan_error(&mut self, attempt_id: Uuid, stage: &str, error: &str) -> Result<()> {
        self.append_now(WalBody::ScanError(ScanErrorBody {
            attempt_id,
            stage: stage.to_string(),
            error: error.to_string(),
        }))
    }

    fn connect_attempt(
        &mut self,
        attempt_id: Uuid,
        peripheral_id: &str,
        local_name: Option<&str>,
        rssi_dbm: Option<i16>,
    ) -> Result<()> {
        self.set_ble_peripheral_id(Some(peripheral_id))?;
        self.append_now(WalBody::ConnectAttempt(ConnectAttemptBody {
            attempt_id,
            peripheral_id: peripheral_id.to_ascii_lowercase(),
            local_name: local_name.map(str::to_string),
            rssi_dbm,
        }))
    }

    fn connect_result(&mut self, attempt_id: Uuid, connected: bool, elapsed_ms: u64) -> Result<()> {
        self.append_now(WalBody::ConnectResult(ConnectResultBody {
            attempt_id,
            connected,
            elapsed_ms,
        }))
    }

    fn connect_error(
        &mut self,
        attempt_id: Uuid,
        stage: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::ConnectError(ConnectErrorBody {
            attempt_id,
            stage: stage.to_string(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn disconnect_attempt(&mut self, reason: &str, peripheral_id: &str) -> Result<()> {
        self.append_now(WalBody::DisconnectAttempt(DisconnectAttemptBody {
            reason: reason.to_string(),
            peripheral_id: peripheral_id.to_ascii_lowercase(),
        }))
    }

    fn disconnect_result(&mut self, reason: &str, elapsed_ms: u64) -> Result<()> {
        self.append_now(WalBody::DisconnectResult(DisconnectResultBody {
            reason: reason.to_string(),
            elapsed_ms,
        }))
    }

    fn disconnect_error(&mut self, reason: &str, error: &str, elapsed_ms: u64) -> Result<()> {
        self.append_now(WalBody::DisconnectError(DisconnectErrorBody {
            reason: reason.to_string(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn central_event(
        &mut self,
        event_name: &str,
        peripheral_id: &str,
        detail: &str,
        rssi_dbm: Option<i16>,
        service_uuids: &[String],
        raw_debug: &str,
    ) -> Result<()> {
        self.append_now(WalBody::CentralEvent(CentralEventBody {
            event_name: event_name.to_string(),
            peripheral_id: peripheral_id.to_ascii_lowercase(),
            detail: detail.to_string(),
            rssi_dbm,
            service_uuids: service_uuids.to_vec(),
            raw_debug: raw_debug.to_string(),
        }))
    }

    fn rssi_sample(
        &mut self,
        peripheral_id: &str,
        rssi_dbm: i16,
        method: &str,
        age_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::RssiSample(RssiSampleBody {
            peripheral_id: peripheral_id.to_ascii_lowercase(),
            rssi_dbm,
            method: method.to_string(),
            age_ms,
        }))
    }

    fn rssi_read_error(
        &mut self,
        peripheral_id: &str,
        method: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::RssiReadError(RssiReadErrorBody {
            peripheral_id: peripheral_id.to_ascii_lowercase(),
            method: method.to_string(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn service_discovery_summary(
        &mut self,
        peripheral_id: &str,
        characteristics: &[BleCharacteristic],
        device_info: Option<ServiceDiscoveryDeviceInfo>,
    ) -> Result<()> {
        self.append_now(WalBody::ServiceDiscoverySummary(
            ServiceDiscoverySummaryBody {
                peripheral_id: peripheral_id.to_ascii_lowercase(),
                service_count: service_count(characteristics),
                services: service_discovery_services(characteristics),
                device_info,
            },
        ))
    }

    fn subscription_attempt(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        notify_kind: &str,
        reason: &str,
    ) -> Result<()> {
        self.append_now(WalBody::SubscriptionAttempt(SubscriptionAttemptBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            notify_kind: notify_kind.to_string(),
            reason: reason.to_string(),
        }))
    }

    fn subscription_result(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::SubscriptionResult(SubscriptionResultBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            elapsed_ms,
        }))
    }

    fn subscription_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::SubscriptionError(SubscriptionErrorBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn gatt_notification(
        &mut self,
        collector_ns: u128,
        characteristic_uuid: &str,
        description: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.append_at(
            collector_ns,
            WalBody::GattNotification(GattNotificationBody::from_payload(
                characteristic_uuid.to_ascii_lowercase(),
                description,
                payload,
            )?),
        )
    }

    fn gatt_read_request(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
    ) -> Result<()> {
        self.append_now(WalBody::GattReadRequest(GattReadRequestBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            reason: reason.to_string(),
        }))
    }

    fn gatt_read_response(
        &mut self,
        collector_ns: u128,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.append_at(
            collector_ns,
            WalBody::GattReadResponse(GattReadResponseBody::from_payload(
                operation_id,
                characteristic_uuid.to_ascii_lowercase(),
                reason,
                payload,
            )?),
        )
    }

    fn gatt_read_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        reason: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::GattReadError(GattReadErrorBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            reason: reason.to_string(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn gatt_write_request(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        write_type: BleWriteType,
        reason: &str,
        payload: &[u8],
    ) -> Result<()> {
        self.append_now(WalBody::GattWriteRequest(
            GattWriteRequestBody::from_payload(
                operation_id,
                characteristic_uuid.to_ascii_lowercase(),
                write_type_name(write_type),
                reason,
                payload,
            )?,
        ))
    }

    fn gatt_write_result(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::GattWriteResult(GattWriteResultBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            elapsed_ms,
        }))
    }

    fn gatt_write_error(
        &mut self,
        operation_id: Uuid,
        characteristic_uuid: &str,
        error: &str,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::GattWriteError(GattWriteErrorBody {
            operation_id,
            characteristic_uuid: characteristic_uuid.to_ascii_lowercase(),
            error: error.to_string(),
            elapsed_ms,
        }))
    }

    fn psftp_set_local_time_attempt(
        &mut self,
        operation_id: Uuid,
        local_time_iso8601: &str,
        local_time_unix_ns: i64,
        tz_offset_minutes: i32,
        dst_offset_minutes: Option<i32>,
        request_payload: &[u8],
        control_characteristic_uuid: &str,
        mtu_characteristic_uuid: &str,
    ) -> Result<()> {
        self.append_now(WalBody::PsftpSetLocalTimeAttempt(
            PsftpSetLocalTimeAttemptBody {
                operation_id,
                local_time_iso8601: local_time_iso8601.to_string(),
                local_time_unix_ns,
                tz_offset_minutes,
                dst_offset_minutes,
                request_payload_b64: base64::engine::general_purpose::STANDARD
                    .encode(request_payload),
                control_characteristic_uuid: control_characteristic_uuid.to_ascii_lowercase(),
                mtu_characteristic_uuid: mtu_characteristic_uuid.to_ascii_lowercase(),
            },
        ))
    }

    fn psftp_set_local_time_result(
        &mut self,
        operation_id: Uuid,
        success: bool,
        status_code: Option<u32>,
        response_payload: &[u8],
        error: Option<&str>,
        elapsed_ms: u64,
    ) -> Result<()> {
        self.append_now(WalBody::PsftpSetLocalTimeResult(
            PsftpSetLocalTimeResultBody {
                operation_id,
                success,
                status_code,
                response_payload_b64: base64::engine::general_purpose::STANDARD
                    .encode(response_payload),
                error: error.map(str::to_string),
                elapsed_ms,
            },
        ))
    }

    fn flush_streams(&mut self) -> Result<()> {
        // WAL appends are synchronous. Periodic fsync is handled by the WAL writer.
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        let reason = self
            .shutdown_reason
            .clone()
            .unwrap_or_else(|| "writer_shutdown".to_string());
        self.close_wal(&reason, "collector shutdown")
    }
}

fn severity_for_event(event: &str) -> &'static str {
    if event.contains("critical") || event.contains("archive-critical") {
        "critical"
    } else if event.contains("error")
        || event.contains("failed")
        || event.contains("timeout")
        // Action-needed conditions: a broken Mac<->H10 bond and a prolonged total outage are
        // operationally serious and must stand out (and be counted) as errors, not info.
        || event.contains("pairing_breakdown")
        || event.contains("prolonged_reconnect_outage")
    {
        "error"
    } else if event.contains("missing") || event.contains("watchdog") || event.contains("skipped") {
        "warn"
    } else {
        "info"
    }
}

fn notification_description(characteristic_uuid: &str, detail: &str) -> String {
    if !detail.is_empty()
        && detail != "gatt_notification"
        && detail != "pmd_start_wait_notification"
    {
        return detail.to_string();
    }
    match characteristic_uuid.to_ascii_lowercase().as_str() {
        crate::config::PMD_DATA_UUID => "pmd_data",
        crate::config::PMD_CONTROL_UUID => "pmd_control",
        crate::config::HR_MEASUREMENT_UUID => "heart_rate_measurement",
        crate::config::BATTERY_LEVEL_UUID => "battery_level",
        crate::config::PSFTP_MTU_UUID => "psftp_mtu",
        _ => "unknown",
    }
    .to_string()
}

pub fn write_type_name(write_type: BleWriteType) -> &'static str {
    match write_type {
        BleWriteType::WithResponse => "with_response",
        BleWriteType::WithoutResponse => "without_response",
    }
}

fn u128_to_i64(value: u128) -> Result<i64> {
    i64::try_from(value).context("nanosecond timestamp does not fit i64")
}

fn unix_ns_i64() -> Result<i64> {
    u128_to_i64(unix_ns())
}

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn physical_device_id_from_target(target: &str) -> String {
    let cleaned = target
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if cleaned.is_empty() {
        "h10_unknown".to_string()
    } else {
        format!("h10_{cleaned}")
    }
}

fn hostname_fallback() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

fn service_count(characteristics: &[BleCharacteristic]) -> u32 {
    let count = characteristics
        .iter()
        .map(|ch| ch.service_uuid)
        .collect::<BTreeSet<_>>()
        .len();
    u32::try_from(count).unwrap_or(u32::MAX)
}

fn service_discovery_services(
    characteristics: &[BleCharacteristic],
) -> Vec<ServiceDiscoveryService> {
    let mut grouped: BTreeMap<String, Vec<ServiceDiscoveryCharacteristic>> = BTreeMap::new();
    for ch in characteristics {
        grouped
            .entry(ch.service_uuid.to_string().to_ascii_lowercase())
            .or_default()
            .push(ServiceDiscoveryCharacteristic {
                characteristic_uuid: ch.uuid.to_string().to_ascii_lowercase(),
                properties: characteristic_properties(ch),
            });
    }
    grouped
        .into_iter()
        .map(|(service_uuid, mut characteristics)| {
            characteristics
                .sort_by(|left, right| left.characteristic_uuid.cmp(&right.characteristic_uuid));
            ServiceDiscoveryService {
                service_uuid,
                characteristics,
            }
        })
        .collect()
}

fn characteristic_properties(ch: &BleCharacteristic) -> Vec<String> {
    let mut properties = Vec::new();
    if ch.properties.read {
        properties.push("read".to_string());
    }
    if ch.properties.write {
        properties.push("write".to_string());
    }
    if ch.properties.write_without_response {
        properties.push("write_without_response".to_string());
    }
    if ch.properties.notify {
        properties.push("notify".to_string());
    }
    properties
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HR_MEASUREMENT_UUID, PMD_DATA_UUID};
    use crate::gatt::BleCharacteristicProperties;
    use crate::wal::{
        GattNotificationBody, WalReadMode, WalRecordKind, WalWriter, WalWriterOptions,
        find_closed_wal_segments, read_wal_records,
    };
    use std::time::Duration;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_sink_wal_{name}_{}_{}",
            std::process::id(),
            crate::util::unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn wal_config(base: &Path) -> WalPathConfig {
        WalPathConfig::new(base, WAL_PARTICIPANT_ID, H10_WAL_SOURCE).expect("wal config")
    }

    fn closed_paths(base: &Path) -> Vec<PathBuf> {
        find_closed_wal_segments(&wal_config(base)).expect("find closed WAL")
    }

    fn open_paths(base: &Path) -> Vec<PathBuf> {
        let source = wal_config(base).source_dir();
        let mut out = Vec::new();
        if !source.exists() {
            return out;
        }
        for date_entry in fs::read_dir(source).expect("scan source") {
            let date_path = date_entry.expect("date entry").path();
            if !date_path.is_dir() {
                continue;
            }
            for file_entry in fs::read_dir(date_path).expect("scan date") {
                let path = file_entry.expect("file entry").path();
                if path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.ends_with(".open"))
                    .unwrap_or(false)
                {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
    }

    fn kinds(path: &Path) -> Vec<WalRecordKind> {
        read_wal_records(path, WalReadMode::TailTolerant)
            .expect("read wal")
            .records
            .into_iter()
            .map(|frame| frame.record.kind)
            .collect()
    }

    #[test]
    fn collector_sinks_write_raw_events_to_wal_only() {
        let base = temp_dir("raw_events");
        let mut sinks = CollectorSinks::new(&base, "A1B2C3D4").expect("sinks");
        assert!(sinks.current_open_wal_path().is_some());
        sinks.begin_connection();
        sinks
            .set_ble_peripheral_id(Some("72585434-07e3-d5e3-5ace-4e6c70118687"))
            .expect("set peripheral");
        sinks
            .raw_gatt(
                1_770_000_000_000_000_000,
                PMD_DATA_UUID,
                "inbound",
                "notification",
                &[0, 1, 2],
                "pmd_data",
            )
            .expect("raw notification");
        sinks.event("collector_test", "ok").expect("event");
        sinks.flush_all().expect("close wal");

        let paths = closed_paths(&base);
        assert_eq!(paths.len(), 1);
        let read = read_wal_records(&paths[0], WalReadMode::Strict).expect("read closed");
        let kinds: Vec<_> = read.records.iter().map(|frame| frame.record.kind).collect();
        assert!(kinds.contains(&WalRecordKind::SegmentStarted));
        assert!(kinds.contains(&WalRecordKind::SessionStarted));
        assert!(kinds.contains(&WalRecordKind::GattNotification));
        assert!(kinds.contains(&WalRecordKind::CollectorEvent));
        assert!(kinds.contains(&WalRecordKind::SessionEnded));
        assert!(kinds.contains(&WalRecordKind::SegmentClosed));
        let notification = read
            .records
            .iter()
            .find(|frame| frame.record.kind == WalRecordKind::GattNotification)
            .expect("notification");
        match &notification.record.body {
            WalBody::GattNotification(GattNotificationBody { payload_len, .. }) => {
                assert_eq!(*payload_len, 3);
            }
            other => panic!("unexpected notification body: {other:?}"),
        }
        assert!(!base.join("latest").exists());
        assert!(!base.join("live").exists());
        assert!(!base.join("archive").exists());
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn startup_recovers_stale_open_wal_and_logs_recovery_in_new_segment() {
        let base = temp_dir("recovery");
        let path_config = wal_config(&base);
        let context = WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            COLLECTOR_WRITER_ID,
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .expect("context")
        .physical_device_id(Some("h10_a1b2c3d4"));
        let mut writer = WalWriter::create(
            &path_config,
            1_770_000_000_000_000_000,
            WalWriterOptions {
                fsync_interval: Duration::ZERO,
                max_create_attempts: 4,
            },
        )
        .expect("writer");
        writer
            .append_record(
                &context.make_record(WalBody::GattNotification(
                    GattNotificationBody::from_payload(
                        HR_MEASUREMENT_UUID,
                        "heart_rate_measurement",
                        &[0x10, 60],
                    )
                    .expect("body"),
                )),
            )
            .expect("append");
        drop(writer);
        assert_eq!(open_paths(&base).len(), 1);

        let mut sinks = CollectorSinks::new(&base, "A1B2C3D4").expect("sinks");
        assert_eq!(open_paths(&base).len(), 1, "new active WAL remains open");
        let recovered_closed = closed_paths(&base);
        assert_eq!(recovered_closed.len(), 1);
        assert!(kinds(&recovered_closed[0]).contains(&WalRecordKind::SegmentClosed));

        let open = open_paths(&base).pop().expect("new open");
        let open_kinds = kinds(&open);
        assert!(open_kinds.contains(&WalRecordKind::RecoveryEvent));
        sinks.flush_all().expect("close new wal");
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn flush_all_closes_current_wal_cleanly_and_is_idempotent() {
        let base = temp_dir("clean_close");
        let mut sinks = CollectorSinks::new(&base, "A1B2C3D4").expect("sinks");
        sinks.event("ctrl_c", "test shutdown").expect("event");
        sinks.flush_all().expect("first close");
        sinks.flush_all().expect("second close no-op");
        assert!(open_paths(&base).is_empty());
        let paths = closed_paths(&base);
        assert_eq!(paths.len(), 1);
        let read = read_wal_records(&paths[0], WalReadMode::Strict).expect("read");
        let close = read
            .records
            .iter()
            .rev()
            .find(|frame| frame.record.kind == WalRecordKind::SegmentClosed)
            .expect("close record");
        match &close.record.body {
            WalBody::SegmentClosed(body) => assert_eq!(body.reason, "ctrl_c"),
            other => panic!("unexpected close body: {other:?}"),
        }
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn collector_sinks_do_not_create_phase1_live_or_archive_paths() {
        let base = temp_dir("no_phase1");
        let mut sinks = CollectorSinks::new(&base, "A1B2C3D4").expect("sinks");
        sinks.event("collector_started", "test").expect("event");
        assert!(
            base.join("wal/participant_id=local/source=chest_h10")
                .exists()
        );
        assert!(!base.join("latest").exists());
        assert!(!base.join("live").exists());
        assert!(!base.join("archive").exists());
        assert!(
            fs::read_dir(&base)
                .expect("read base")
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("h10_session_"))
        );
        sinks.flush_all().expect("close");
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn service_summary_groups_characteristics_and_properties() {
        let ch = BleCharacteristic {
            uuid: crate::util::uuid(HR_MEASUREMENT_UUID),
            service_uuid: crate::util::uuid("0000180d-0000-1000-8000-00805f9b34fb"),
            properties: BleCharacteristicProperties {
                read: true,
                write: false,
                write_without_response: false,
                notify: true,
            },
        };
        let services = service_discovery_services(&[ch]);
        assert_eq!(services.len(), 1);
        assert_eq!(
            services[0].characteristics[0].properties,
            vec!["read", "notify"]
        );
    }
}
