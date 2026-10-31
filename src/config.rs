use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_TARGET: &str = "A1B2C3D4";

// Standard GATT UUIDs.
pub const HR_MEASUREMENT_UUID: &str = "00002a37-0000-1000-8000-00805f9b34fb";
pub const BATTERY_LEVEL_UUID: &str = "00002a19-0000-1000-8000-00805f9b34fb";
pub const CURRENT_TIME_UUID: &str = "00002a2b-0000-1000-8000-00805f9b34fb";
pub const MANUFACTURER_NAME_UUID: &str = "00002a29-0000-1000-8000-00805f9b34fb";
pub const MODEL_NUMBER_UUID: &str = "00002a24-0000-1000-8000-00805f9b34fb";
pub const SERIAL_NUMBER_UUID: &str = "00002a25-0000-1000-8000-00805f9b34fb";
pub const HARDWARE_REV_UUID: &str = "00002a27-0000-1000-8000-00805f9b34fb";
pub const FIRMWARE_REV_UUID: &str = "00002a26-0000-1000-8000-00805f9b34fb";
pub const SOFTWARE_REV_UUID: &str = "00002a28-0000-1000-8000-00805f9b34fb";

// Polar Measurement Data (PMD) service.
pub const PMD_CONTROL_UUID: &str = "fb005c81-02e7-f387-1cad-8acd2d8df0c8";
pub const PMD_DATA_UUID: &str = "fb005c82-02e7-f387-1cad-8acd2d8df0c8";

// Polar PS-FTP/PFTP service used by the official SDK for H10 local time setting.
pub const PSFTP_MTU_UUID: &str = "fb005c51-02e7-f387-1cad-8acd2d8df0c8";
pub const PSFTP_SET_LOCAL_TIME_QUERY_ID: u16 = 3;
pub const PSFTP_ATT_MTU_PAYLOAD: usize = 20;

pub const ECG_MEASUREMENT_TYPE: u8 = 0;
pub const ACC_MEASUREMENT_TYPE: u8 = 2;

// Known H10 online stream start commands from Polar PMD docs / SDK examples.
// ECG: 130 Hz, 14-bit resolution. Data frame samples are 24-bit signed µV.
pub const ECG_START_CMD: &[u8] = &[0x02, 0x00, 0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0e, 0x00];
// ACC: 200 Hz, 16-bit resolution, +/-8G range. Data is usually compressed delta frames.
pub const ACC_START_CMD: &[u8] = &[
    0x02, 0x02, 0x00, 0x01, 0xc8, 0x00, 0x01, 0x01, 0x10, 0x00, 0x02, 0x01, 0x08, 0x00,
];
pub const ECG_STOP_CMD: &[u8] = &[0x03, 0x00];
pub const ACC_STOP_CMD: &[u8] = &[0x03, 0x02];

pub const BLE_MANAGER_TIMEOUT: Duration = Duration::from_secs(5);
pub const BLE_SCAN_OP_TIMEOUT: Duration = Duration::from_secs(3);
pub const SCAN_COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(750);

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub target: String,
    pub data_dir: PathBuf,
    pub scan_timeout: Duration,
    pub scan_poll_interval: Duration,
    pub reconnect_sleep: Duration,
    pub pre_connect_disconnect_timeout: Duration,
    pub pre_connect_settle: Duration,
    pub connect_timeout: Duration,
    pub pairing_failure_threshold: u32,
    pub pairing_retry_interval: Duration,
    pub outage_alert_after: Duration,
    pub outage_alert_repeat: Duration,
    pub discover_services_timeout: Duration,
    pub notification_stream_timeout: Duration,
    pub central_events_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub subscribe_timeout: Duration,
    pub pmd_control_response_timeout: Duration,
    pub startup_stop_settle: Duration,
    pub startup_between_streams: Duration,
    pub notification_watchdog: Duration,
    pub pmd_stream_watchdog: Duration,
    pub health_interval: Duration,
    pub battery_interval: Duration,
    pub rssi_timeout: Duration,
    pub properties_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub psftp_response_timeout: Duration,
    pub progress_every_pmd_frames: u64,
}

impl CollectorConfig {
    pub fn new(target: String, data_dir: PathBuf, scan_timeout_seconds: u64) -> Self {
        Self {
            target,
            data_dir,
            scan_timeout: Duration::from_secs(scan_timeout_seconds),
            scan_poll_interval: Duration::from_millis(200),
            reconnect_sleep: Duration::from_millis(200),
            pre_connect_disconnect_timeout: Duration::from_millis(500),
            pre_connect_settle: Duration::from_millis(100),
            // 8 s is intentionally conservative for CoreBluetooth: the attempted 3 s
            // fast-fail policy could preempt macOS' own connect/encryption retry window
            // and trigger a persistent LE connection deny-list storm. See
            // docs/connect_timeout_regression_20260603.md.
            connect_timeout: Duration::from_secs(8),
            // After this many consecutive pairing/encryption rejections ("Peer removed pairing
            // information" / "Failed to encrypt", STATUS 634) the Mac<->H10 bond has almost
            // certainly desynced. The collector cannot clear a macOS bond, so we (a) raise a loud
            // ACTION-NEEDED banner on the dashboard telling the user to forget the device, and
            // (b) keep retrying at the calm `pairing_retry_interval` below so the moment the user
            // fixes it, we reconnect. See docs/connect_storm_rootcause_20260603.md.
            pairing_failure_threshold: 2,
            // Normal reconnect stays at the fast `reconnect_sleep` (200 ms) cadence so
            // out-of-range -> walk-back and weak-RF blips recover instantly. Only once the bond
            // is recognised as broken do we slow to this calm fixed interval (no escalation, no
            // long cooldown) — just "keep trying every ~8 s" while the dashboard banner asks the
            // user to forget the device.
            pairing_retry_interval: Duration::from_secs(8),
            // Prefer loud failure over silent data loss: if no successful stream has happened
            // for this long, the collector emits a loud, rate-limited outage alert instead of
            // churning silently (the 2026-06-03 storm lost ~11h of data with no alert).
            outage_alert_after: Duration::from_secs(300),
            outage_alert_repeat: Duration::from_secs(60),
            discover_services_timeout: Duration::from_secs(8),
            notification_stream_timeout: Duration::from_secs(3),
            central_events_timeout: Duration::from_secs(3),
            read_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(3),
            subscribe_timeout: Duration::from_secs(3),
            pmd_control_response_timeout: Duration::from_secs(3),
            startup_stop_settle: Duration::from_millis(100),
            startup_between_streams: Duration::from_millis(100),
            notification_watchdog: Duration::from_secs(2),
            pmd_stream_watchdog: Duration::from_secs(2),
            health_interval: Duration::from_secs(1),
            battery_interval: Duration::from_secs(60),
            rssi_timeout: Duration::from_millis(500),
            properties_timeout: Duration::from_millis(500),
            cleanup_timeout: Duration::from_secs(2),
            psftp_response_timeout: Duration::from_secs(8),
            progress_every_pmd_frames: 25,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_config_new_sets_all_expected_defaults() {
        let config = CollectorConfig::new("TARGET".to_string(), PathBuf::from("out"), 42);
        assert_eq!(config.target, "TARGET");
        assert_eq!(config.data_dir, PathBuf::from("out"));
        assert_eq!(config.scan_timeout, Duration::from_secs(42));
        assert_eq!(config.scan_poll_interval, Duration::from_millis(200));
        assert_eq!(config.reconnect_sleep, Duration::from_millis(200));
        assert_eq!(
            config.pre_connect_disconnect_timeout,
            Duration::from_millis(500)
        );
        assert_eq!(config.pre_connect_settle, Duration::from_millis(100));
        assert_eq!(config.connect_timeout, Duration::from_secs(8));
        assert_eq!(config.pairing_failure_threshold, 2);
        assert_eq!(config.pairing_retry_interval, Duration::from_secs(8));
        assert_eq!(config.outage_alert_after, Duration::from_secs(300));
        assert_eq!(config.outage_alert_repeat, Duration::from_secs(60));
        assert_eq!(config.discover_services_timeout, Duration::from_secs(8));
        assert_eq!(config.notification_stream_timeout, Duration::from_secs(3));
        assert_eq!(config.central_events_timeout, Duration::from_secs(3));
        assert_eq!(config.read_timeout, Duration::from_secs(3));
        assert_eq!(config.write_timeout, Duration::from_secs(3));
        assert_eq!(config.subscribe_timeout, Duration::from_secs(3));
        assert_eq!(config.pmd_control_response_timeout, Duration::from_secs(3));
        assert_eq!(config.startup_stop_settle, Duration::from_millis(100));
        assert_eq!(config.startup_between_streams, Duration::from_millis(100));
        assert_eq!(config.notification_watchdog, Duration::from_secs(2));
        assert_eq!(config.pmd_stream_watchdog, Duration::from_secs(2));
        assert_eq!(config.health_interval, Duration::from_secs(1));
        assert_eq!(config.battery_interval, Duration::from_secs(60));
        assert_eq!(config.rssi_timeout, Duration::from_millis(500));
        assert_eq!(config.properties_timeout, Duration::from_millis(500));
        assert_eq!(config.cleanup_timeout, Duration::from_secs(2));
        assert_eq!(config.psftp_response_timeout, Duration::from_secs(8));
        assert_eq!(config.progress_every_pmd_frames, 25);
    }

    #[test]
    fn protocol_constants_match_h10_mvp_settings() {
        assert_eq!(DEFAULT_TARGET, "A1B2C3D4");
        assert_eq!(ECG_MEASUREMENT_TYPE, 0);
        assert_eq!(ACC_MEASUREMENT_TYPE, 2);
        assert_eq!(
            ECG_START_CMD,
            &[0x02, 0x00, 0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0e, 0x00]
        );
        assert_eq!(ACC_STOP_CMD, &[0x03, 0x02]);
        assert_eq!(PSFTP_ATT_MTU_PAYLOAD, 20);
    }
}
