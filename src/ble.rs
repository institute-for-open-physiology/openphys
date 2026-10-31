use crate::config::{
    ACC_MEASUREMENT_TYPE, ACC_START_CMD, ACC_STOP_CMD, BATTERY_LEVEL_UUID, BLE_MANAGER_TIMEOUT,
    BLE_SCAN_OP_TIMEOUT, CURRENT_TIME_UUID, CollectorConfig, ECG_MEASUREMENT_TYPE, ECG_START_CMD,
    ECG_STOP_CMD, FIRMWARE_REV_UUID, HARDWARE_REV_UUID, HR_MEASUREMENT_UUID,
    MANUFACTURER_NAME_UUID, MODEL_NUMBER_UUID, PMD_CONTROL_UUID, PMD_DATA_UUID,
    SCAN_COMMAND_POLL_INTERVAL, SERIAL_NUMBER_UUID, SOFTWARE_REV_UUID,
};
use crate::gatt::{
    BleCentralEvent, BleCharacteristic, BleNotification, BleWriteType, GattPeripheral,
    RSSI_READ_METHOD, StreamingCentral, find_ble_char,
};
use crate::hr::handle_hr;
use crate::pmd::{PmdParser, parse_pmd_control_response, pmd_measurement_type};
use crate::process_lock::{CollectorProcessLock, collector_lock_path};
use crate::psftp::try_set_h10_local_time_psftp;
use crate::sink::{CollectorSinks, RecordSink};
use crate::util::{ble_timeout, unix_ns, uuid};
use anyhow::{Context, Result, anyhow};
use btleplug::api::{Central, Manager as _, Peripheral as _, ScanFilter};
use btleplug::platform::{Adapter, Manager, Peripheral};
use chrono::{Datelike, Local, Timelike, Utc};
use futures::{
    Stream, StreamExt,
    future::{BoxFuture, FutureExt},
};
use std::collections::HashSet;
use std::io::Write;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, sleep_until, timeout};
use uuid::Uuid;

struct PmdCommand<'a> {
    name: &'a str,
    command: &'a [u8],
    expected_op: u8,
    measurement_type: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamLoopCleanup {
    StopAndDisconnect,
    AlreadyDisconnected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BleScanProperties {
    address: String,
    local_name: Option<String>,
    rssi: Option<i16>,
    services: Vec<uuid::Uuid>,
    manufacturer_data_len: usize,
    service_data_len: usize,
}

#[derive(Clone, Debug)]
struct FoundTarget<P> {
    peripheral: P,
    id: String,
    props: BleScanProperties,
}

trait ScanPeripheral: Clone + Send + Sync {
    fn id_string(&self) -> String;
    fn properties_for_scan(
        &self,
        timeout_duration: Duration,
    ) -> BoxFuture<'_, Result<Option<BleScanProperties>>>;
}

trait ScanAdapter: Send + Sync {
    type Peripheral: ScanPeripheral;

    fn start_scan_for_target(&self) -> BoxFuture<'_, Result<()>>;
    fn stop_scan_for_target(&self) -> BoxFuture<'_, Result<()>>;
    fn peripherals_for_scan(&self) -> BoxFuture<'_, Result<Vec<Self::Peripheral>>>;
}

impl ScanPeripheral for Peripheral {
    fn id_string(&self) -> String {
        self.id().to_string()
    }

    fn properties_for_scan(
        &self,
        timeout_duration: Duration,
    ) -> BoxFuture<'_, Result<Option<BleScanProperties>>> {
        async move {
            let props = ble_timeout(
                "read scan peripheral properties",
                timeout_duration,
                self.properties(),
            )
            .await?;
            Ok(props.map(|props| BleScanProperties {
                address: props.address.to_string(),
                local_name: props.local_name,
                rssi: props.rssi,
                services: props.services,
                manufacturer_data_len: props.manufacturer_data.len(),
                service_data_len: props.service_data.len(),
            }))
        }
        .boxed()
    }
}

impl ScanAdapter for Adapter {
    type Peripheral = Peripheral;

    fn start_scan_for_target(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            ble_timeout(
                "start BLE scan",
                BLE_SCAN_OP_TIMEOUT,
                self.start_scan(ScanFilter::default()),
            )
            .await
        }
        .boxed()
    }

    fn stop_scan_for_target(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            timeout(BLE_SCAN_OP_TIMEOUT, self.stop_scan())
                .await
                .map_err(|_| {
                    anyhow!(
                        "stop BLE scan timed out after {} ms",
                        BLE_SCAN_OP_TIMEOUT.as_millis()
                    )
                })?
                .map_err(|err| anyhow!("stop BLE scan: {err}"))
        }
        .boxed()
    }

    fn peripherals_for_scan(&self) -> BoxFuture<'_, Result<Vec<Self::Peripheral>>> {
        async move {
            ble_timeout(
                "list scan peripherals",
                BLE_SCAN_OP_TIMEOUT,
                self.peripherals(),
            )
            .await
        }
        .boxed()
    }
}

pub async fn default_adapter() -> Result<Adapter> {
    let manager = ble_timeout("create BLE manager", BLE_MANAGER_TIMEOUT, Manager::new()).await?;
    let adapters =
        ble_timeout("list BLE adapters", BLE_MANAGER_TIMEOUT, manager.adapters()).await?;
    adapters
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no Bluetooth adapters found"))
}

pub async fn scan(seconds: u64) -> Result<()> {
    let adapter = default_adapter().await?;
    ble_timeout(
        "start BLE scan",
        BLE_SCAN_OP_TIMEOUT,
        adapter.start_scan(ScanFilter::default()),
    )
    .await?;
    println!("Scanning for {seconds}s...");

    let deadline = Instant::now() + std::time::Duration::from_secs(seconds);
    let mut seen = HashSet::new();
    while Instant::now() < deadline {
        let peripherals = match ble_timeout(
            "list scan peripherals",
            BLE_SCAN_OP_TIMEOUT,
            adapter.peripherals(),
        )
        .await
        {
            Ok(peripherals) => peripherals,
            Err(err) => {
                eprintln!("scan peripheral list failed: {err:#}");
                sleep(SCAN_COMMAND_POLL_INTERVAL).await;
                continue;
            }
        };
        for peripheral in peripherals {
            let props = match ble_timeout(
                "read scan peripheral properties",
                BLE_SCAN_OP_TIMEOUT,
                peripheral.properties(),
            )
            .await
            {
                Ok(Some(props)) => props,
                Ok(None) => continue,
                Err(err) => {
                    eprintln!("scan properties failed for {}: {err:#}", peripheral.id());
                    continue;
                }
            };
            let id = peripheral.id().to_string();
            let key = format!("{}:{:?}", id, props.local_name);
            if seen.insert(key) {
                println!(
                    "id={} address={} name={:?} rssi={:?} services={:?} manufacturer_data={} service_data={}",
                    id,
                    props.address,
                    props.local_name,
                    props.rssi,
                    props.services,
                    props.manufacturer_data.len(),
                    props.service_data.len()
                );
            }
        }
        sleep(SCAN_COMMAND_POLL_INTERVAL).await;
    }
    stop_scan_quietly(&adapter).await;
    Ok(())
}

pub async fn collect(config: CollectorConfig) -> Result<()> {
    let lock_path = collector_lock_path(&config.data_dir);
    let collector_lock = match CollectorProcessLock::acquire(&config.data_dir) {
        Ok(lock) => {
            eprintln!(
                "collector_process_lock_acquired path={}",
                lock.path().display()
            );
            lock
        }
        Err(err) => {
            eprintln!(
                "collector_process_lock_failed path={} error={err:#}",
                lock_path.display()
            );
            return Err(err);
        }
    };

    let collector_lock_path = collector_lock.path().display().to_string();
    // Keep this guard alive for the full collect loop; dropping it releases the OS lock.
    let _collector_lock_guard = collector_lock;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    let mut writers = CollectorSinks::new(&config.data_dir, &config.target)?;
    let mut parser = PmdParser::new();
    writers.event(
        "collector_process_lock_acquired",
        &format!("path={collector_lock_path}"),
    )?;
    writers.event("collector_started", &format!("target={}", config.target))?;
    println!("Writing H10 WAL under {}", writers.session_dir().display());

    let adapter_result = tokio::select! {
        result = default_adapter() => result,
        signal = &mut ctrl_c => {
            signal.context("wait for ctrl-c during BLE adapter setup")?;
            writers.event("ctrl_c", "stopping during adapter setup")?;
            writers.flush_all()?;
            return Ok(());
        }
    };
    let adapter = match adapter_result {
        Ok(adapter) => adapter,
        Err(err) => {
            writers.event("ble_adapter_error", &format!("{err:#}"))?;
            writers.flush_all()?;
            return Err(err);
        }
    };

    // Reconnect-health state, persistent across loop iterations.
    // `last_stream_ok` advances only when a streaming session actually ran (run_connected
    // returns Ok on a normal disconnect/watchdog), so a prolonged failure to stream grows the
    // measured outage. `consecutive_pairing_failures` drives both the calm pairing retry interval
    // and the (no-wait) ACTION-NEEDED re-pair alert/banner.
    let mut last_stream_ok = Instant::now();
    let mut consecutive_pairing_failures: u32 = 0;
    let mut last_outage_alert: Option<Instant> = None;
    let mut last_pairing_alert: Option<Instant> = None;

    loop {
        let mut reconnect_delay = config.reconnect_sleep;
        writers.set_ble_peripheral_id(None)?;
        writers.event(
            "scan_started",
            &format!("timeout_seconds={}", config.scan_timeout.as_secs()),
        )?;
        let scan_attempt_id = Uuid::new_v4();
        writers.scan_attempt(
            scan_attempt_id,
            &config.target,
            duration_ms(config.scan_timeout),
            &[],
        )?;
        let target_result = tokio::select! {
            result = find_target(&adapter, &config) => result,
            signal = &mut ctrl_c => {
                signal.context("wait for ctrl-c during scan")?;
                handle_collect_ctrl_c(&adapter, &mut writers, "scan").await?;
                return Ok(());
            }
        };
        match target_result {
            Ok(found) => {
                let service_uuids = found
                    .props
                    .services
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                writers.scan_result(
                    scan_attempt_id,
                    &found.id,
                    found.props.local_name.as_deref(),
                    found.props.rssi,
                    None,
                    &service_uuids,
                    true,
                )?;
                match run_connected(
                    &adapter,
                    &found.peripheral,
                    &mut writers,
                    &mut parser,
                    &config,
                )
                .await
                {
                    Ok(()) => {
                        // A streaming session ran and ended via a normal disconnect/watchdog.
                        // Reset all reconnect-health counters: we are healthy, not stuck.
                        last_stream_ok = Instant::now();
                        consecutive_pairing_failures = 0;
                        last_pairing_alert = None;
                    }
                    Err(err) => {
                        let msg = format!("{err:#}");
                        eprintln!("connection loop ended: {msg}");
                        writers.event("connection_loop_error", &msg)?;
                        let archive_critical = msg.contains("collector WAL")
                            || msg.contains("append")
                            || msg.contains("WAL")
                            || msg.contains("archive sink")
                            || msg.contains("parquet sink")
                            || msg.contains("catalog");
                        // A connect-stage failure that already cleanly cancelled the pending
                        // connection marks itself so we skip a redundant disconnect here, which
                        // could otherwise block up to cleanup_timeout on an already-gone
                        // peripheral and erode the fast-fail reconnect win.
                        let already_cancelled = err
                            .downcast_ref::<PendingConnectAlreadyCancelled>()
                            .is_some();
                        let failure_kind = classify_connect_failure(&msg);
                        // Accumulate pairing failures across interleaved non-pairing failures: a
                        // real bond breakdown alternates "Peer removed pairing information" with
                        // deny-list "connect timed out", so a non-pairing failure must NOT reset
                        // the counter (that would keep it below threshold and suppress the
                        // alert/8s cadence). It is reset only on a successful stream or when the
                        // device drops out of range (handled in the Ok and not-found branches).
                        consecutive_pairing_failures =
                            next_pairing_failure_count(consecutive_pairing_failures, failure_kind);
                        // Normal failures (weak RF, a single connect timeout) retry at the fast
                        // 200 ms cadence so a walk-back into range reconnects instantly. Only a
                        // recognised bond breakdown slows to the calm pairing_retry_interval
                        // (~8 s) while the dashboard shows the ACTION-NEEDED banner. No escalation,
                        // no long cooldown.
                        reconnect_delay =
                            reconnect_delay_for(&config, consecutive_pairing_failures);
                        // A repeated pairing/encryption rejection means the Mac<->H10 bond has
                        // desynced and cannot recover without a re-pair. Tell the user the actual
                        // remedy (loudly, rate-limited). This is informational only — it adds NO
                        // wait beyond the normal backoff above.
                        if pairing_alert_due(
                            failure_kind,
                            consecutive_pairing_failures,
                            last_pairing_alert.map(|at| at.elapsed()),
                            &config,
                        ) {
                            last_pairing_alert = Some(Instant::now());
                            let detail = format!(
                                "consecutive_pairing_failures={} hint=re-pair the H10 (System Settings > Bluetooth > forget \"Polar H10 {}\", no re-pair needed) error={msg}",
                                consecutive_pairing_failures, config.target,
                            );
                            eprintln!("pairing_breakdown_detected: {detail}");
                            writers.event("pairing_breakdown_detected", &detail)?;
                        }
                        if !already_cancelled {
                            let disconnect_start = Instant::now();
                            writers.disconnect_attempt("cleanup_after_error", &found.id)?;
                            match timeout(
                                config.cleanup_timeout,
                                found.peripheral.disconnect_gatt(),
                            )
                            .await
                            {
                                Ok(Ok(())) => {
                                    writers.disconnect_result(
                                        "cleanup_after_error",
                                        elapsed_ms(disconnect_start),
                                    )?;
                                    writers.event("disconnect_after_error", "ok")?;
                                }
                                Ok(Err(err)) => {
                                    let detail = format!("{err:#}");
                                    writers.disconnect_error(
                                        "cleanup_after_error",
                                        &detail,
                                        elapsed_ms(disconnect_start),
                                    )?;
                                    writers.event("disconnect_after_error_failed", &detail)?;
                                }
                                Err(_) => {
                                    let detail =
                                        format!("{}ms", config.cleanup_timeout.as_millis());
                                    writers.disconnect_error(
                                        "cleanup_after_error",
                                        "timeout",
                                        elapsed_ms(disconnect_start),
                                    )?;
                                    writers.event("disconnect_after_error_timeout", &detail)?;
                                }
                            };
                        }
                        if archive_critical {
                            writers.flush_all()?;
                            return Err(anyhow!("archive-critical collector failure: {msg}"));
                        }
                    }
                }
            }
            Err(err) => {
                let msg = format!("{err:#}");
                eprintln!("target not found yet: {msg}");
                writers.scan_error(scan_attempt_id, "find_target", &msg)?;
                writers.event("target_not_found", &msg)?;
                // The device is out of range / asleep, not refusing to connect. Reset the
                // pairing-failure state so the next time it reappears (walk-back into range)
                // reconnects at the full-speed cadence instead of the calm pairing interval.
                consecutive_pairing_failures = 0;
                last_pairing_alert = None;
            }
        }
        // Prefer loud failure over silent data loss: once we have gone too long without a
        // successful stream, alert loudly (rate-limited) regardless of the failure cause
        // (pairing breakdown, deny-list, or genuinely-out-of-range H10).
        let outage = last_stream_ok.elapsed();
        if outage_alert_due(outage, last_outage_alert.map(|at| at.elapsed()), &config) {
            last_outage_alert = Some(Instant::now());
            let detail = format!(
                "no_stream_for_s={} consecutive_pairing_failures={} reconnect_delay_ms={}",
                outage.as_secs(),
                consecutive_pairing_failures,
                duration_ms(reconnect_delay),
            );
            eprintln!("prolonged_reconnect_outage: {detail}");
            writers.event("prolonged_reconnect_outage", &detail)?;
        }
        writers.event(
            "reconnect_sleep",
            &format!("{}ms", reconnect_delay.as_millis()),
        )?;
        tokio::select! {
            _ = sleep(reconnect_delay) => {}
            signal = &mut ctrl_c => {
                signal.context("wait for ctrl-c during reconnect sleep")?;
                handle_collect_ctrl_c(&adapter, &mut writers, "reconnect_sleep").await?;
                return Ok(());
            }
        }
    }
}

async fn handle_collect_ctrl_c(
    adapter: &Adapter,
    writers: &mut impl RecordSink,
    phase: &str,
) -> Result<()> {
    writers.event("ctrl_c", &format!("stopping during {phase}"))?;
    stop_scan_quietly(adapter).await;
    writers.flush_all()?;
    Ok(())
}

async fn find_target(
    adapter: &Adapter,
    config: &CollectorConfig,
) -> Result<FoundTarget<Peripheral>> {
    find_target_details_with_adapter(adapter, config).await
}

#[cfg(test)]
async fn find_target_with_adapter<A>(adapter: &A, config: &CollectorConfig) -> Result<A::Peripheral>
where
    A: ScanAdapter,
{
    Ok(find_target_details_with_adapter(adapter, config)
        .await?
        .peripheral)
}

async fn find_target_details_with_adapter<A>(
    adapter: &A,
    config: &CollectorConfig,
) -> Result<FoundTarget<A::Peripheral>>
where
    A: ScanAdapter,
{
    adapter.start_scan_for_target().await?;
    let target_lower = config.target.trim().to_ascii_lowercase();
    let deadline = Instant::now() + config.scan_timeout;

    while Instant::now() < deadline {
        let peripherals = match adapter.peripherals_for_scan().await {
            Ok(peripherals) => peripherals,
            Err(err) => {
                stop_scan_for_target(adapter).await;
                return Err(err);
            }
        };
        for peripheral in peripherals {
            let props = match peripheral
                .properties_for_scan(config.properties_timeout)
                .await
            {
                Ok(props) => props,
                Err(err) => {
                    stop_scan_for_target(adapter).await;
                    return Err(err);
                }
            };
            let Some(props) = props else { continue };
            let id = peripheral.id_string();
            let is_target = target_matches(&id, &props, &target_lower);

            // Intentionally do not log every advertisement here. The collector rescans
            // continuously, so per-advertisement stdout logging produced a ~1.5 GB log during
            // the 2026-06-03 multi-hour reconnect storm (99.9% advertisement spam). Only the
            // matched target is logged below; non-target sightings are not collector-relevant.
            if is_target {
                stop_scan_for_target(adapter).await;
                println!(
                    "matched target: id={} address={} name={:?}",
                    id, props.address, props.local_name
                );
                return Ok(FoundTarget {
                    peripheral,
                    id,
                    props,
                });
            }
        }
        sleep(config.scan_poll_interval).await;
    }
    stop_scan_for_target(adapter).await;
    Err(anyhow!(
        "no BLE peripheral matched target {:?}",
        config.target
    ))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn elapsed_ms(start: Instant) -> u64 {
    duration_ms(start.elapsed())
}

fn target_matches(id: &str, props: &BleScanProperties, target_lower: &str) -> bool {
    let name = props.local_name.clone().unwrap_or_default();
    let haystack = format!("{} {} {}", id, props.address, name).to_ascii_lowercase();
    let is_polar_h10 = name.to_ascii_lowercase().contains("polar h10");
    if target_lower.is_empty() {
        is_polar_h10
    } else {
        haystack.contains(target_lower)
    }
}

/// Reconnect delay for the outer collect loop. Normal failures (out-of-range, weak RF, a single
/// transient connect timeout) retry at the fast `reconnect_sleep` (200 ms) cadence so a walk-back
/// into range reconnects instantly. Only once the bond is recognised as broken (>=
/// `pairing_failure_threshold` consecutive pairing/encryption rejections) do we slow to the calm
/// fixed `pairing_retry_interval` — no escalation, no long cooldown — while the dashboard shows the
/// ACTION-NEEDED banner. Resets to fast automatically because the caller clears the counter on any
/// successful stream or when the device drops out of range.
fn reconnect_delay_for(config: &CollectorConfig, consecutive_pairing_failures: u32) -> Duration {
    if consecutive_pairing_failures >= config.pairing_failure_threshold {
        config.pairing_retry_interval
    } else {
        config.reconnect_sleep
    }
}

/// Classification of a connect-stage failure, used to choose backoff/alerting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectFailureKind {
    /// CoreBluetooth pairing/encryption rejection: the Mac<->H10 bond has desynced
    /// ("Peer removed pairing information", "Failed to encrypt", STATUS 634). Not recoverable
    /// by retrying or by a longer timeout — the H10 must be re-paired on the Mac.
    Pairing,
    /// App-level connect timeout (e.g. CoreBluetooth ignoring a deny-listed device, or a
    /// genuinely slow/weak-RF connect).
    Timeout,
    /// Anything else (service discovery, watchdog, PMD start rejection, ...).
    Other,
}

/// Classify a connect/`run_connected` error message. Order matters: the encryption failure
/// surfaces as "Failed to encrypt the connection, the connection has timed out unexpectedly",
/// which contains "timed out", so the pairing/encryption case is checked first.
fn classify_connect_failure(error_message: &str) -> ConnectFailureKind {
    let lower = error_message.to_ascii_lowercase();
    if lower.contains("peer removed pairing information")
        || lower.contains("encrypt")
        || lower.contains("pairing")
        || lower.contains("insufficient authentication")
    {
        ConnectFailureKind::Pairing
    } else if lower.contains("timed out") {
        ConnectFailureKind::Timeout
    } else {
        ConnectFailureKind::Other
    }
}

/// Update the consecutive-pairing-failure counter for one connect failure. Increments on a pairing
/// rejection; leaves the count UNCHANGED on a non-pairing failure (so a bond breakdown that
/// alternates pairing rejections with deny-list timeouts still accumulates past the threshold). The
/// caller resets it to 0 only on a successful stream or when the device drops out of range. Pure.
fn next_pairing_failure_count(prev: u32, kind: ConnectFailureKind) -> u32 {
    if kind == ConnectFailureKind::Pairing {
        prev.saturating_add(1)
    } else {
        prev
    }
}

/// Whether the loud, actionable "re-pair the H10" alert is due. Fires at/after
/// `pairing_failure_threshold` consecutive pairing failures, then no more often than
/// `outage_alert_repeat`. This only governs the *alert*; it imposes no extra reconnect wait (the
/// reconnect cadence is owned solely by [`reconnect_delay_for`]). Pure for tests.
fn pairing_alert_due(
    kind: ConnectFailureKind,
    consecutive_pairing_failures: u32,
    since_last_alert: Option<Duration>,
    config: &CollectorConfig,
) -> bool {
    if kind != ConnectFailureKind::Pairing
        || consecutive_pairing_failures < config.pairing_failure_threshold
    {
        return false;
    }
    match since_last_alert {
        None => true,
        Some(elapsed) => elapsed >= config.outage_alert_repeat,
    }
}

/// Whether a prolonged-outage alert is due. Fires once the outage exceeds `outage_alert_after`,
/// then no more often than `outage_alert_repeat`. Pure (takes elapsed durations) so it is unit
/// testable without a clock.
fn outage_alert_due(
    outage: Duration,
    since_last_alert: Option<Duration>,
    config: &CollectorConfig,
) -> bool {
    if outage < config.outage_alert_after {
        return false;
    }
    match since_last_alert {
        None => true,
        Some(elapsed) => elapsed >= config.outage_alert_repeat,
    }
}

async fn stop_scan_for_target<A>(adapter: &A)
where
    A: ScanAdapter,
{
    adapter.stop_scan_for_target().await.ok();
}

async fn is_connected_quick<P>(peripheral: &P, config: &CollectorConfig) -> bool
where
    P: GattPeripheral,
{
    timeout(
        config.pre_connect_disconnect_timeout,
        peripheral.is_connected_gatt(),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .unwrap_or(false)
}

/// Marker error returned when a connect-stage failure has already cleanly torn
/// down the pending/half-open connection via [`cancel_pending_connect`]. The outer
/// collect loop downcasts this to skip its own redundant `cleanup_after_error`
/// disconnect, which would otherwise block up to `cleanup_timeout` waiting on a
/// peripheral CoreBluetooth has already dropped — diluting the fast-fail win. The
/// marker carries the original connect-error message for logging.
#[derive(Debug)]
struct PendingConnectAlreadyCancelled {
    message: String,
}

impl std::fmt::Display for PendingConnectAlreadyCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PendingConnectAlreadyCancelled {}

/// Best-effort, bounded cancellation of a pending/half-open connection after a
/// failed or timed-out connect. CoreBluetooth keeps trying to establish the link
/// with no native timeout, so dropping the connect future is not enough; we must
/// explicitly disconnect or the zombie connection lingers into the next attempt.
/// Disconnect failures are logged, not propagated (the connect already failed);
/// only record-sink/WAL append failures bubble up. Returns `true` only when the
/// disconnect completed cleanly within the timeout, so the caller can decide
/// whether the outer-loop cleanup disconnect is still needed.
async fn cancel_pending_connect<P>(
    peripheral: &P,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<bool>
where
    P: GattPeripheral,
{
    let id = peripheral.id_string();
    let start = Instant::now();
    writers.disconnect_attempt("cancel_pending_connect", &id)?;
    match timeout(
        config.pre_connect_disconnect_timeout,
        peripheral.disconnect_gatt(),
    )
    .await
    {
        Ok(Ok(())) => {
            writers.disconnect_result("cancel_pending_connect", elapsed_ms(start))?;
            writers.event("cancel_pending_connect", "ok")?;
            Ok(true)
        }
        Ok(Err(err)) => {
            let detail = format!("{err:#}");
            writers.disconnect_error("cancel_pending_connect", &detail, elapsed_ms(start))?;
            writers.event("cancel_pending_connect_failed", &detail)?;
            Ok(false)
        }
        Err(_) => {
            writers.disconnect_error("cancel_pending_connect", "timeout", elapsed_ms(start))?;
            writers.event(
                "cancel_pending_connect_timeout",
                &format!("{}ms", config.pre_connect_disconnect_timeout.as_millis()),
            )?;
            Ok(false)
        }
    }
}

async fn run_connected(
    adapter: &Adapter,
    peripheral: &Peripheral,
    writers: &mut impl RecordSink,
    parser: &mut PmdParser,
    config: &CollectorConfig,
) -> Result<()> {
    run_connected_with_gatt(adapter, peripheral, writers, parser, config).await
}

async fn run_connected_with_gatt<C, P>(
    central: &C,
    peripheral: &P,
    writers: &mut impl RecordSink,
    parser: &mut PmdParser,
    config: &CollectorConfig,
) -> Result<()>
where
    C: StreamingCentral,
    P: GattPeripheral,
{
    let id = peripheral.id_string();
    writers.set_ble_peripheral_id(Some(&id))?;
    let connection_id = writers.begin_connection();
    let connect_attempt_id = Uuid::new_v4();
    writers.connect_attempt(connect_attempt_id, &id, None, None)?;
    writers.event(
        "connecting",
        &format!("peripheral_id={id} connection_id={connection_id}"),
    )?;
    if is_connected_quick(peripheral, config).await {
        let disconnect_start = Instant::now();
        writers.disconnect_attempt("pre_connect", &id)?;
        match timeout(
            config.pre_connect_disconnect_timeout,
            peripheral.disconnect_gatt(),
        )
        .await
        {
            Ok(Ok(())) => {
                writers.disconnect_result("pre_connect", elapsed_ms(disconnect_start))?;
                writers.event("pre_connect_disconnect", "ok")?;
            }
            Ok(Err(err)) => {
                let detail = format!("{err:#}");
                writers.disconnect_error("pre_connect", &detail, elapsed_ms(disconnect_start))?;
                writers.event("pre_connect_disconnect_error", &detail)?;
            }
            Err(_) => {
                writers.disconnect_error("pre_connect", "timeout", elapsed_ms(disconnect_start))?;
                writers.event(
                    "pre_connect_disconnect_timeout",
                    &format!("{}ms", config.pre_connect_disconnect_timeout.as_millis()),
                )?;
            }
        }
        sleep(config.pre_connect_settle).await;
    }
    let connect_start = Instant::now();
    match ble_timeout("connect", config.connect_timeout, peripheral.connect_gatt()).await {
        Ok(()) => writers.connect_result(connect_attempt_id, true, elapsed_ms(connect_start))?,
        Err(err) => {
            let detail = format!("{err:#}");
            writers.connect_error(
                connect_attempt_id,
                "connect",
                &detail,
                elapsed_ms(connect_start),
            )?;
            // CoreBluetooth's connectPeripheral has no native timeout, so a connect that
            // timed out (or errored mid-handshake) can leave it still trying to establish
            // the link. Dropping the connect future does not cancel that, so explicitly
            // tear down the pending/half-open connection before returning, so it cannot
            // linger into the next scan/connect attempt. Best-effort and bounded.
            let cancelled = cancel_pending_connect(peripheral, writers, config).await?;
            // If we already disconnected cleanly, mark the error so the outer collect loop
            // skips its redundant cleanup_after_error disconnect (which can block for
            // cleanup_timeout on an already-gone peripheral). If the cancel did not
            // complete, fall through with the plain error so the outer loop retries the
            // teardown with its longer timeout.
            if cancelled {
                return Err(anyhow::Error::new(PendingConnectAlreadyCancelled {
                    message: detail,
                }));
            }
            return Err(err);
        }
    }
    writers.event("connected", &format!("peripheral_id={id}"))?;
    match ble_timeout(
        "discover GATT services",
        config.discover_services_timeout,
        peripheral.discover_services_gatt(),
    )
    .await
    {
        Ok(()) => {}
        Err(err) => {
            let detail = format!("{err:#}");
            writers.connect_error(
                connect_attempt_id,
                "discover_services",
                &detail,
                elapsed_ms(connect_start),
            )?;
            return Err(err);
        }
    };
    let characteristics = peripheral.characteristics_gatt();
    writers.event(
        "services_discovered",
        &format!("characteristics={}", characteristics.len()),
    )?;
    writers.service_discovery_summary(&id, &characteristics, None)?;

    log_characteristics(&characteristics, writers)?;
    read_device_info(peripheral, &characteristics, writers, config).await?;
    if let Err(err) = read_battery(peripheral, &characteristics, writers, "initial", config).await {
        writers.event("battery_read_error", &format!("initial: {err:#}"))?;
    }
    try_set_current_time(peripheral, &characteristics, writers, config).await?;

    let hr_char = find_ble_char(&characteristics, HR_MEASUREMENT_UUID);
    let battery_char = find_ble_char(&characteristics, BATTERY_LEVEL_UUID);
    let pmd_cp_char = find_ble_char(&characteristics, PMD_CONTROL_UUID)
        .ok_or_else(|| anyhow!("PMD control characteristic not found"))?;
    let pmd_data_char = find_ble_char(&characteristics, PMD_DATA_UUID)
        .ok_or_else(|| anyhow!("PMD data characteristic not found"))?;

    let mut notifications = ble_timeout(
        "notification stream",
        config.notification_stream_timeout,
        peripheral.notifications_gatt(),
    )
    .await?;

    match try_set_h10_local_time_psftp(peripheral, &mut notifications, writers, config).await {
        Ok(()) => {}
        Err(err) => {
            let msg = format!("{err:#}");
            eprintln!("PS-FTP H10 time set failed: {msg}");
            writers.event("psftp_set_local_time_error", &msg)?;
            if !is_connected_quick(peripheral, config).await {
                return Err(anyhow!(
                    "PS-FTP H10 time set failed and peripheral is disconnected: {msg}"
                ));
            }
        }
    }

    if let Some(ch) = &hr_char {
        subscribe_gatt_logged(peripheral, ch, "heart_rate_measurement", writers, config).await?;
        writers.event("subscribed", "heart_rate_measurement")?;
    } else {
        writers.event("missing_characteristic", HR_MEASUREMENT_UUID)?;
    }
    subscribe_gatt_logged(peripheral, &pmd_cp_char, "pmd_control", writers, config).await?;
    writers.event("subscribed", "pmd_control")?;
    subscribe_gatt_logged(peripheral, &pmd_data_char, "pmd_data", writers, config).await?;
    writers.event("subscribed", "pmd_data")?;

    match read_gatt_logged(
        peripheral,
        &pmd_cp_char,
        "pmd_features_control",
        writers,
        config,
    )
    .await
    {
        Ok(features) => {
            writers.event("pmd_features_read", &hex::encode(&features))?;
            println!("PMD features/control read: {}", hex::encode(&features));
        }
        Err(err) => writers.event("pmd_features_read_error", &format!("{err:#}"))?,
    }

    // Clear any old stream state. Stop responses are consumed and logged by the
    // following start-response waits, but stop write failures are logged here.
    best_effort_write(
        peripheral,
        &pmd_cp_char,
        ECG_STOP_CMD,
        "pre_start_stop_ecg",
        writers,
        config,
    )
    .await?;
    best_effort_write(
        peripheral,
        &pmd_cp_char,
        ACC_STOP_CMD,
        "pre_start_stop_acc",
        writers,
        config,
    )
    .await?;
    sleep(config.startup_stop_settle).await;

    writers.event(
        "start_stream",
        &format!("ecg_cmd={}", hex::encode(ECG_START_CMD)),
    )?;
    write_pmd_command_and_wait(
        peripheral,
        &pmd_cp_char,
        &mut notifications,
        parser,
        writers,
        config,
        PmdCommand {
            name: "ECG",
            command: ECG_START_CMD,
            expected_op: 0x02,
            measurement_type: ECG_MEASUREMENT_TYPE,
        },
    )
    .await?;
    sleep(config.startup_between_streams).await;
    writers.event(
        "start_stream",
        &format!("acc_cmd={}", hex::encode(ACC_START_CMD)),
    )?;
    write_pmd_command_and_wait(
        peripheral,
        &pmd_cp_char,
        &mut notifications,
        parser,
        writers,
        config,
        PmdCommand {
            name: "ACC",
            command: ACC_START_CMD,
            expected_op: 0x02,
            measurement_type: ACC_MEASUREMENT_TYPE,
        },
    )
    .await?;

    let mut last_battery = Instant::now();
    let mut last_health = Instant::now();
    let stream_started_at = Instant::now();
    let mut last_notification = stream_started_at;
    let mut last_pmd_notification = stream_started_at;
    let mut last_ecg_pmd = stream_started_at;
    let mut last_acc_pmd = stream_started_at;
    let mut pmd_frames: u64 = 0;
    let mut hr_frames: u64 = 0;
    let mut central_events = ble_timeout(
        "central event stream",
        config.central_events_timeout,
        central.central_events_gatt(),
    )
    .await?;
    let mut central_events_active = true;
    let mut cleanup_after_loop = StreamLoopCleanup::StopAndDisconnect;
    let mut tick = interval(config.health_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    println!("Streaming. Press Ctrl-C to stop this process.");

    loop {
        let notification_deadline = last_notification + config.notification_watchdog;
        let pmd_deadline = last_pmd_notification + config.pmd_stream_watchdog;
        let ecg_deadline = last_ecg_pmd + config.pmd_stream_watchdog;
        let acc_deadline = last_acc_pmd + config.pmd_stream_watchdog;
        tokio::select! {
            // Like notifications.next(), central_events.next() is bounded by the same
            // select-level stream watchdog deadlines rather than a nested timeout.
            central_event = central_events.next(), if central_events_active => {
                match central_event {
                    Some(BleCentralEvent::DeviceDisconnected(event_id)) => {
                        writers.central_event(
                            "device_disconnected",
                            &event_id,
                            "CentralEvent::DeviceDisconnected",
                            None,
                            &[],
                            &format!("DeviceDisconnected({event_id})"),
                        )?;
                        if event_id == id {
                            writers.event(
                                "central_device_disconnected",
                                &format!(
                                    "peripheral_id={id} no_notifications_for_ms={}",
                                    last_notification.elapsed().as_millis()
                                ),
                            )?;
                            cleanup_after_loop = StreamLoopCleanup::AlreadyDisconnected;
                            break;
                        }
                    }
                    Some(BleCentralEvent::StateUpdate(state)) => {
                        writers.central_event(
                            "state_update",
                            &id,
                            &state,
                            None,
                            &[],
                            &format!("StateUpdate({state})"),
                        )?;
                        writers.event("central_state_update", &state)?;
                    }
                    Some(BleCentralEvent::Other) => {
                        writers.central_event("other", &id, "CentralEvent::Other", None, &[], "Other")?;
                    }
                    None => {
                        central_events_active = false;
                        writers.event("central_event_stream_ended", "")?;
                    }
                }
            }
            maybe_notification = notifications.next() => {
                // The notification stream is not wrapped in timeout here because the select! has
                // explicit per-stream watchdog deadlines. This keeps the loop responsive while still
                // preventing a stuck CoreBluetooth notification wait from blocking recovery.
                let Some(notification) = maybe_notification else {
                    writers.event("notification_stream_ended", "")?;
                    break;
                };
                let now_instant = Instant::now();
                let now_ns = unix_ns();
                last_notification = now_instant;
                let notification_uuid = notification.uuid.to_string();
                writers.raw_gatt(
                    now_ns,
                    &notification_uuid,
                    "inbound",
                    "notification",
                    &notification.value,
                    "gatt_notification",
                )?;
                if notification.uuid == uuid(PMD_DATA_UUID) {
                    last_pmd_notification = now_instant;
                    match pmd_measurement_type(&notification.value) {
                        Some(ECG_MEASUREMENT_TYPE) => last_ecg_pmd = now_instant,
                        Some(ACC_MEASUREMENT_TYPE) => last_acc_pmd = now_instant,
                        _ => {}
                    }
                    pmd_frames += 1;
                    parser.handle_pmd_data(now_ns, &notification.value, writers)?;
                    // Keep the live dashboard latency low: PMD frames are only ~7/s total
                    // on H10 ECG+ACC, so flushing every frame is cheap and avoids an
                    // extra multi-frame delay from BufWriter.
                    writers.flush_streams()?;
                    if config.progress_every_pmd_frames > 0 && pmd_frames.is_multiple_of(config.progress_every_pmd_frames) {
                        print!("\rPMD frames={pmd_frames} HR frames={hr_frames} latest={}", Local::now().format("%H:%M:%S"));
                        std::io::stdout().flush().ok();
                    }
                } else if notification.uuid == uuid(PMD_CONTROL_UUID) {
                    parser.handle_control(now_ns, &notification.value, writers)?;
                } else if notification.uuid == uuid(HR_MEASUREMENT_UUID) {
                    hr_frames += 1;
                    handle_hr(now_ns, &notification.value, writers)?;
                    writers.flush_streams()?;
                } else {
                    writers.event("notification_unknown", &format!("uuid={} len={} hex={}", notification.uuid, notification.value.len(), hex::encode(&notification.value)))?;
                }
            }
            _ = sleep_until(notification_deadline) => {
                writers.event(
                    "notification_watchdog_timeout",
                    &format!("no_notifications_for_ms={}", last_notification.elapsed().as_millis()),
                )?;
                break;
            }
            _ = sleep_until(pmd_deadline) => {
                writers.event(
                    "pmd_watchdog_timeout",
                    &format!("no_pmd_for_ms={}", last_pmd_notification.elapsed().as_millis()),
                )?;
                break;
            }
            _ = sleep_until(ecg_deadline) => {
                writers.event(
                    "ecg_watchdog_timeout",
                    &format!("no_ecg_pmd_for_ms={}", last_ecg_pmd.elapsed().as_millis()),
                )?;
                break;
            }
            _ = sleep_until(acc_deadline) => {
                writers.event(
                    "acc_watchdog_timeout",
                    &format!("no_acc_pmd_for_ms={}", last_acc_pmd.elapsed().as_millis()),
                )?;
                break;
            }
            _ = tick.tick() => {
                if last_health.elapsed() >= config.health_interval {
                    last_health = Instant::now();
                    match timeout(config.rssi_timeout, peripheral.read_rssi_gatt()).await {
                        Ok(Ok(rssi)) => {
                            writers.rssi_sample(&id, rssi, RSSI_READ_METHOD, 0)?;
                            writers.health(unix_ns(), Some(rssi), pmd_frames, hr_frames)?;
                        }
                        Ok(Err(err)) => {
                            let detail = format!("{err:#}");
                            writers.rssi_read_error(&id, RSSI_READ_METHOD, &detail, duration_ms(config.rssi_timeout))?;
                            writers.event("read_rssi_error", &detail)?;
                            // Fallback to btleplug's cached advertisement RSSI if available.
                            match timeout(config.properties_timeout, peripheral.properties_gatt()).await {
                                Ok(Ok(Some(props))) => {
                                    if let Some(rssi) = props.rssi {
                                        writers.rssi_sample(&id, rssi, "platform_property", 0)?;
                                    }
                                    writers.health(unix_ns(), props.rssi, pmd_frames, hr_frames)?;
                                }
                                Ok(Ok(None)) => writers.health(unix_ns(), None, pmd_frames, hr_frames)?,
                                Ok(Err(err)) => writers.event("properties_error", &format!("{err:#}"))?,
                                Err(_) => writers.event("properties_timeout", &format!("{}ms", config.properties_timeout.as_millis()))?,
                            }
                        }
                        Err(_) => {
                            writers.rssi_read_error(&id, RSSI_READ_METHOD, "timeout", duration_ms(config.rssi_timeout))?;
                            writers.event("read_rssi_timeout", &format!("{}ms", config.rssi_timeout.as_millis()))?;
                        }
                    }
                }
                if last_battery.elapsed() >= config.battery_interval {
                    last_battery = Instant::now();
                    if battery_char.is_some() {
                        match read_battery(peripheral, &characteristics, writers, "periodic", config).await {
                            Ok(()) => {}
                            Err(err) => {
                                writers.event("battery_read_error", &format!("{err:#}"))?;
                                if !is_connected_quick(peripheral, config).await {
                                    writers.event("battery_read_disconnected", "breaking connection loop")?;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            signal = &mut ctrl_c => {
                signal.context("wait for ctrl-c")?;
                writers.event("ctrl_c", "stopping streams and disconnecting")?;
                best_effort_stop_streams_and_disconnect(
                    peripheral,
                    Some(&pmd_cp_char),
                    writers,
                    config,
                    "ctrl_c",
                )
                .await?;
                writers.flush_all()?;
                std::process::exit(0);
            }
        }
    }

    match cleanup_after_loop {
        StreamLoopCleanup::StopAndDisconnect => {
            writers.event("stream_loop_ending", "best_effort_stop_and_disconnect")?;
            best_effort_stop_streams_and_disconnect(
                peripheral,
                Some(&pmd_cp_char),
                writers,
                config,
                "stream_loop",
            )
            .await?;
        }
        StreamLoopCleanup::AlreadyDisconnected => {
            writers.event(
                "stream_loop_ending",
                "central_device_disconnected_cleanup_skipped",
            )?;
            writers.event(
                "cleanup_skipped_stream_loop",
                "central_device_disconnected_already_disconnected",
            )?;
        }
    }
    // Do not close the WAL on a recoverable BLE dropout; the same collector
    // session continues across reconnects. Ctrl-C/clean shutdown closes it.
    writers.flush_streams()?;
    Ok(())
}

fn log_characteristics(
    characteristics: &[BleCharacteristic],
    writers: &mut impl RecordSink,
) -> Result<()> {
    for ch in characteristics {
        writers.event(
            "characteristic",
            &format!(
                "uuid={} service={} properties={:?}",
                ch.uuid, ch.service_uuid, ch.properties
            ),
        )?;
    }
    Ok(())
}

async fn read_gatt_logged<P>(
    peripheral: &P,
    characteristic: &BleCharacteristic,
    reason: &str,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<Vec<u8>>
where
    P: GattPeripheral,
{
    let operation_id = Uuid::new_v4();
    let characteristic_uuid = characteristic.uuid.to_string();
    writers.gatt_read_request(operation_id, &characteristic_uuid, reason)?;
    let start = Instant::now();
    match ble_timeout(
        &format!("read GATT {reason}"),
        config.read_timeout,
        peripheral.read_gatt(characteristic),
    )
    .await
    {
        Ok(bytes) => {
            writers.gatt_read_response(
                unix_ns(),
                operation_id,
                &characteristic_uuid,
                reason,
                &bytes,
            )?;
            Ok(bytes)
        }
        Err(err) => {
            let detail = format!("{err:#}");
            writers.gatt_read_error(
                operation_id,
                &characteristic_uuid,
                reason,
                &detail,
                elapsed_ms(start),
            )?;
            Err(anyhow!(detail))
        }
    }
}

async fn write_gatt_logged<P>(
    peripheral: &P,
    characteristic: &BleCharacteristic,
    payload: &[u8],
    write_type: BleWriteType,
    reason: &str,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    let operation_id = Uuid::new_v4();
    let characteristic_uuid = characteristic.uuid.to_string();
    writers.gatt_write_request(
        operation_id,
        &characteristic_uuid,
        write_type,
        reason,
        payload,
    )?;
    let start = Instant::now();
    match ble_timeout(
        &format!("write GATT {reason}"),
        config.write_timeout,
        peripheral.write_gatt(characteristic, payload, write_type),
    )
    .await
    {
        Ok(()) => writers.gatt_write_result(operation_id, &characteristic_uuid, elapsed_ms(start)),
        Err(err) => {
            let detail = format!("{err:#}");
            writers.gatt_write_error(
                operation_id,
                &characteristic_uuid,
                &detail,
                elapsed_ms(start),
            )?;
            Err(anyhow!(detail))
        }
    }
}

async fn subscribe_gatt_logged<P>(
    peripheral: &P,
    characteristic: &BleCharacteristic,
    reason: &str,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    let operation_id = Uuid::new_v4();
    let characteristic_uuid = characteristic.uuid.to_string();
    writers.subscription_attempt(operation_id, &characteristic_uuid, "notify", reason)?;
    let start = Instant::now();
    match ble_timeout(
        &format!("subscribe {reason}"),
        config.subscribe_timeout,
        peripheral.subscribe_gatt(characteristic),
    )
    .await
    {
        Ok(()) => {
            writers.subscription_result(operation_id, &characteristic_uuid, elapsed_ms(start))
        }
        Err(err) => {
            let detail = format!("{err:#}");
            writers.subscription_error(
                operation_id,
                &characteristic_uuid,
                &detail,
                elapsed_ms(start),
            )?;
            Err(anyhow!(detail))
        }
    }
}

async fn read_device_info<P>(
    peripheral: &P,
    characteristics: &[BleCharacteristic],
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    let infos = [
        ("manufacturer", MANUFACTURER_NAME_UUID),
        ("model", MODEL_NUMBER_UUID),
        ("serial", SERIAL_NUMBER_UUID),
        ("hardware_rev", HARDWARE_REV_UUID),
        ("firmware_rev", FIRMWARE_REV_UUID),
        ("software_rev", SOFTWARE_REV_UUID),
    ];
    for (name, uuid_str) in infos {
        if let Some(ch) = find_ble_char(characteristics, uuid_str) {
            match read_gatt_logged(
                peripheral,
                &ch,
                &format!("device_info_{name}"),
                writers,
                config,
            )
            .await
            {
                Ok(bytes) => {
                    let text = String::from_utf8_lossy(&bytes)
                        .trim_end_matches('\0')
                        .to_string();
                    writers.event("device_info", &format!("{name}={text}"))?;
                    println!("{name}: {text}");
                }
                Err(err) => writers.event("device_info_read_error", &format!("{name}: {err:#}"))?,
            }
        }
    }
    Ok(())
}

async fn read_battery<P>(
    peripheral: &P,
    characteristics: &[BleCharacteristic],
    writers: &mut impl RecordSink,
    reason: &str,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    let Some(ch) = find_ble_char(characteristics, BATTERY_LEVEL_UUID) else {
        writers.event("missing_characteristic", BATTERY_LEVEL_UUID)?;
        return Ok(());
    };
    let collector_ns = unix_ns();
    let bytes = read_gatt_logged(
        peripheral,
        &ch,
        &format!("battery_{reason}"),
        writers,
        config,
    )
    .await?;
    if let Some(level) = bytes.first() {
        writers.battery(collector_ns, *level, reason)?;
        println!("battery: {level}% ({reason})");
    }
    Ok(())
}

async fn try_set_current_time<P>(
    peripheral: &P,
    characteristics: &[BleCharacteristic],
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    let Some(ch) = find_ble_char(characteristics, CURRENT_TIME_UUID) else {
        writers.event(
            "current_time_not_available",
            "Current Time characteristic absent",
        )?;
        return Ok(());
    };
    if !(ch.properties.write || ch.properties.write_without_response) {
        writers.event(
            "current_time_not_writable",
            &format!("properties={:?}", ch.properties),
        )?;
        return Ok(());
    }

    let now = Utc::now();
    let year = now.year() as u16;
    let weekday = now.weekday().number_from_monday() as u8;
    let fractions_256 = ((now.nanosecond() as f64 / 1_000_000_000.0) * 256.0).round() as u8;
    let payload = [
        (year & 0xff) as u8,
        (year >> 8) as u8,
        now.month() as u8,
        now.day() as u8,
        now.hour() as u8,
        now.minute() as u8,
        now.second() as u8,
        weekday,
        fractions_256,
        0x02, // Adjust Reason: external reference time update.
    ];
    let write_type = if ch.properties.write {
        BleWriteType::WithResponse
    } else {
        BleWriteType::WithoutResponse
    };
    match write_gatt_logged(
        peripheral,
        &ch,
        &payload,
        write_type,
        "current_time_set_utc",
        writers,
        config,
    )
    .await
    {
        Ok(()) => writers.event(
            "current_time_set_utc",
            &format!("{} payload={}", now.to_rfc3339(), hex::encode(payload)),
        )?,
        Err(err) => writers.event("current_time_set_error", &format!("{err:#}"))?,
    }
    Ok(())
}

async fn write_pmd_command_and_wait<P, S>(
    peripheral: &P,
    pmd_cp_char: &BleCharacteristic,
    notifications: &mut S,
    parser: &mut PmdParser,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
    command: PmdCommand<'_>,
) -> Result<()>
where
    P: GattPeripheral,
    S: Stream<Item = BleNotification> + Unpin,
{
    write_gatt_logged(
        peripheral,
        pmd_cp_char,
        command.command,
        BleWriteType::WithResponse,
        &format!("pmd_{}_command", command.name.to_ascii_lowercase()),
        writers,
        config,
    )
    .await?;

    let deadline = Instant::now() + config.pmd_control_response_timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!(
                "PMD {} command did not receive matching control response within {} ms",
                command.name,
                config.pmd_control_response_timeout.as_millis()
            ));
        }
        let remaining = deadline - now;
        let notification = timeout(remaining, notifications.next())
            .await
            .context("timeout waiting for PMD control response")?
            .ok_or_else(|| {
                anyhow!("notification stream ended while waiting for PMD control response")
            })?;
        let collector_ns = unix_ns();
        let notification_uuid = notification.uuid.to_string();
        writers.raw_gatt(
            collector_ns,
            &notification_uuid,
            "inbound",
            "notification",
            &notification.value,
            "pmd_start_wait_notification",
        )?;

        if notification.uuid == uuid(PMD_CONTROL_UUID) {
            parser.handle_control(collector_ns, &notification.value, writers)?;
            if let Some(response) = parse_pmd_control_response(&notification.value)
                && response.op == command.expected_op
                && response.measurement_type == command.measurement_type
            {
                if response.error_code == 0 {
                    writers.event(
                        "pmd_stream_start_confirmed",
                        &format!(
                            "stream={} response={}",
                            command.name,
                            hex::encode(&notification.value)
                        ),
                    )?;
                    return Ok(());
                }
                return Err(anyhow!(
                    "PMD {} command rejected: error_code={} response={}",
                    command.name,
                    response.error_code,
                    hex::encode(&notification.value)
                ));
            }
        } else if notification.uuid == uuid(PMD_DATA_UUID) {
            parser.handle_pmd_data(collector_ns, &notification.value, writers)?;
            writers.flush_streams()?;
        } else if notification.uuid == uuid(HR_MEASUREMENT_UUID) {
            handle_hr(collector_ns, &notification.value, writers)?;
            writers.flush_streams()?;
        } else {
            writers.event(
                "pmd_start_wait_ignored_notification",
                &format!(
                    "uuid={} len={} hex={}",
                    notification.uuid,
                    notification.value.len(),
                    hex::encode(&notification.value)
                ),
            )?;
        }
    }
}

async fn best_effort_write<P>(
    peripheral: &P,
    characteristic: &BleCharacteristic,
    data: &[u8],
    label: &str,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
{
    match write_gatt_logged(
        peripheral,
        characteristic,
        data,
        BleWriteType::WithResponse,
        label,
        writers,
        config,
    )
    .await
    {
        Ok(()) => writers.event(label, "ok")?,
        Err(err) => {
            let detail = format!("{err:#}");
            if detail.contains("timed out") {
                writers.event(&format!("{label}_timeout"), &detail)?;
            } else {
                writers.event(&format!("{label}_error"), &detail)?;
            }
        }
    }
    Ok(())
}

async fn best_effort_stop_streams_and_disconnect<P>(
    peripheral: &P,
    pmd_cp_char: Option<&BleCharacteristic>,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
    reason: &str,
) -> Result<()>
where
    P: GattPeripheral,
{
    if let Some(ch) = pmd_cp_char {
        best_effort_write(
            peripheral,
            ch,
            ECG_STOP_CMD,
            &format!("cleanup_stop_ecg_{reason}"),
            writers,
            config,
        )
        .await?;
        best_effort_write(
            peripheral,
            ch,
            ACC_STOP_CMD,
            &format!("cleanup_stop_acc_{reason}"),
            writers,
            config,
        )
        .await?;
    }

    let peripheral_id = peripheral.id_string();
    let disconnect_start = Instant::now();
    writers.disconnect_attempt(reason, &peripheral_id)?;
    match timeout(config.cleanup_timeout, peripheral.disconnect_gatt()).await {
        Ok(Ok(())) => {
            writers.disconnect_result(reason, elapsed_ms(disconnect_start))?;
            writers.event(&format!("cleanup_disconnect_{reason}"), "ok")?;
        }
        Ok(Err(err)) => {
            let detail = format!("{err:#}");
            writers.disconnect_error(reason, &detail, elapsed_ms(disconnect_start))?;
            writers.event(&format!("cleanup_disconnect_{reason}_error"), &detail)?;
        }
        Err(_) => {
            writers.disconnect_error(reason, "timeout", elapsed_ms(disconnect_start))?;
            writers.event(
                &format!("cleanup_disconnect_{reason}_timeout"),
                &format!("{}ms", config.cleanup_timeout.as_millis()),
            )?;
        }
    }
    Ok(())
}

async fn stop_scan_quietly(adapter: &Adapter) {
    timeout(BLE_SCAN_OP_TIMEOUT, adapter.stop_scan())
        .await
        .ok()
        .and_then(Result::ok);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_lock::CollectorProcessLock;
    use crate::util::uuid;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    struct FakePeripheral {
        id: String,
        props: FakePropertiesResult,
    }

    #[derive(Clone, Debug)]
    enum FakePropertiesResult {
        Ok(Option<BleScanProperties>),
        Err(&'static str),
    }

    #[derive(Clone, Debug, Default)]
    struct FakeAdapter {
        state: Arc<Mutex<FakeAdapterState>>,
    }

    #[derive(Debug, Default)]
    struct FakeAdapterState {
        start_calls: usize,
        stop_calls: usize,
        peripherals_calls: usize,
        start_error: Option<&'static str>,
        peripherals_error: Option<&'static str>,
        peripherals: Vec<FakePeripheral>,
    }

    impl FakeAdapter {
        fn with_peripherals(peripherals: Vec<FakePeripheral>) -> Self {
            let adapter = Self::default();
            adapter.state.lock().expect("lock").peripherals = peripherals;
            adapter
        }

        fn with_start_error(error: &'static str) -> Self {
            let adapter = Self::default();
            adapter.state.lock().expect("lock").start_error = Some(error);
            adapter
        }

        fn with_peripherals_error(error: &'static str) -> Self {
            let adapter = Self::default();
            adapter.state.lock().expect("lock").peripherals_error = Some(error);
            adapter
        }

        fn counts(&self) -> (usize, usize, usize) {
            let state = self.state.lock().expect("lock");
            (state.start_calls, state.stop_calls, state.peripherals_calls)
        }
    }

    impl ScanPeripheral for FakePeripheral {
        fn id_string(&self) -> String {
            self.id.clone()
        }

        fn properties_for_scan(
            &self,
            _timeout_duration: Duration,
        ) -> BoxFuture<'_, Result<Option<BleScanProperties>>> {
            async move {
                match &self.props {
                    FakePropertiesResult::Ok(props) => Ok(props.clone()),
                    FakePropertiesResult::Err(err) => Err(anyhow!(*err)),
                }
            }
            .boxed()
        }
    }

    impl ScanAdapter for FakeAdapter {
        type Peripheral = FakePeripheral;

        fn start_scan_for_target(&self) -> BoxFuture<'_, Result<()>> {
            async move {
                let mut state = self.state.lock().expect("lock");
                state.start_calls += 1;
                if let Some(err) = state.start_error {
                    Err(anyhow!(err))
                } else {
                    Ok(())
                }
            }
            .boxed()
        }

        fn stop_scan_for_target(&self) -> BoxFuture<'_, Result<()>> {
            async move {
                let mut state = self.state.lock().expect("lock");
                state.stop_calls += 1;
                Ok(())
            }
            .boxed()
        }

        fn peripherals_for_scan(&self) -> BoxFuture<'_, Result<Vec<Self::Peripheral>>> {
            async move {
                let mut state = self.state.lock().expect("lock");
                state.peripherals_calls += 1;
                if let Some(err) = state.peripherals_error {
                    Err(anyhow!(err))
                } else {
                    Ok(state.peripherals.clone())
                }
            }
            .boxed()
        }
    }

    fn props(name: Option<&str>, address: &str, rssi: Option<i16>) -> BleScanProperties {
        BleScanProperties {
            address: address.to_string(),
            local_name: name.map(str::to_string),
            rssi,
            services: vec![uuid("0000180d-0000-1000-8000-00805f9b34fb")],
            manufacturer_data_len: 1,
            service_data_len: 2,
        }
    }

    fn peripheral(id: &str, properties: Option<BleScanProperties>) -> FakePeripheral {
        FakePeripheral {
            id: id.to_string(),
            props: FakePropertiesResult::Ok(properties),
        }
    }

    fn config(target: &str, timeout_ms: u64) -> CollectorConfig {
        let mut config =
            CollectorConfig::new(target.to_string(), std::path::PathBuf::from("data"), 1);
        config.scan_timeout = Duration::from_millis(timeout_ms);
        config.scan_poll_interval = Duration::from_millis(1);
        config
    }

    fn temp_data_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_collect_lock_{name}_{}_{}",
            std::process::id(),
            crate::util::unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp data dir");
        dir
    }

    fn count_open_wal_files(data_dir: &Path) -> usize {
        let source = data_dir.join("wal/participant_id=local/source=chest_h10");
        if !source.exists() {
            return 0;
        }
        let mut count = 0;
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
                    count += 1;
                }
            }
        }
        count
    }

    #[tokio::test]
    async fn collect_exits_before_ble_work_when_process_lock_is_held() {
        let data_dir = temp_data_dir("duplicate_collect");
        let held_lock = CollectorProcessLock::acquire(&data_dir).expect("hold collector lock");
        let err = collect(CollectorConfig::new(
            "NO_SUCH_H10".to_string(),
            data_dir.clone(),
            1,
        ))
        .await
        .expect_err("duplicate collect must fail before BLE setup");
        assert!(
            format!("{err:#}").contains("will not start BLE work"),
            "unexpected error: {err:#}"
        );
        assert!(
            !data_dir.join("latest").exists(),
            "collector should fail before creating a session"
        );
        drop(held_lock);
        fs::remove_dir_all(data_dir).ok();
    }

    #[tokio::test]
    async fn duplicate_collect_does_not_recover_stale_open_wal_before_lock_failure() {
        use crate::sink::{H10_WAL_SOURCE, WAL_PARTICIPANT_ID};
        use crate::wal::{
            GattNotificationBody, WalBody, WalPathConfig, WalRecordContext, WalWriter,
            WalWriterOptions, find_closed_wal_segments,
        };

        let data_dir = temp_data_dir("duplicate_before_recovery");
        let path_config =
            WalPathConfig::new(&data_dir, WAL_PARTICIPANT_ID, H10_WAL_SOURCE).expect("wal config");
        let context = WalRecordContext::new(
            WAL_PARTICIPANT_ID,
            H10_WAL_SOURCE,
            "training_sensors collect",
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .expect("context");
        let mut writer = WalWriter::create(
            &path_config,
            1_770_000_000_000_000_000,
            WalWriterOptions::default(),
        )
        .expect("stale writer");
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
            .expect("append stale record");
        drop(writer);
        assert_eq!(count_open_wal_files(&data_dir), 1);

        let held_lock = CollectorProcessLock::acquire(&data_dir).expect("hold collector lock");
        let err = collect(CollectorConfig::new(
            "NO_SUCH_H10".to_string(),
            data_dir.clone(),
            1,
        ))
        .await
        .expect_err("duplicate collect must fail before recovery");
        assert!(
            format!("{err:#}").contains("will not start BLE work"),
            "unexpected error: {err:#}"
        );
        assert_eq!(count_open_wal_files(&data_dir), 1);
        assert!(
            find_closed_wal_segments(&path_config)
                .expect("closed scan")
                .is_empty()
        );
        drop(held_lock);
        fs::remove_dir_all(data_dir).ok();
    }

    #[test]
    fn target_matching_covers_id_address_name_and_empty_polar_h10() {
        let p = props(Some("Polar H10 A1B2C3D4"), "AA:BB", Some(-42));
        assert!(target_matches("peripheral-id", &p, "a1b2c3d4"));
        assert!(target_matches("peripheral-id", &p, "aa:bb"));
        assert!(target_matches("peripheral-id", &p, "peripheral-id"));
        assert!(target_matches("peripheral-id", &p, ""));
        assert!(target_matches("peripheral-id", &p, "a1b2c3d4"));
        assert!(!target_matches(
            "peripheral-id",
            &props(Some("Other"), "CC", None),
            ""
        ));
        assert!(!target_matches("peripheral-id", &p, "not-present"));
    }

    #[test]
    fn pairing_failure_count_accumulates_across_interleaved_non_pairing_failures() {
        // A real bond-breakdown storm alternates "Peer removed pairing information" with deny-list
        // "connect timed out". The count must accumulate across the interleaved timeouts (only a
        // successful stream / out-of-range reset it, done by the caller) so the >=2 threshold is
        // reached and the alert + 8 s pairing cadence engage — not reset back to 0 each timeout.
        let seq = [
            ConnectFailureKind::Pairing,
            ConnectFailureKind::Timeout,
            ConnectFailureKind::Pairing,
            ConnectFailureKind::Timeout,
            ConnectFailureKind::Other,
            ConnectFailureKind::Pairing,
        ];
        let mut count = 0u32;
        let progression: Vec<u32> = seq
            .iter()
            .map(|kind| {
                count = next_pairing_failure_count(count, *kind);
                count
            })
            .collect();
        assert_eq!(progression, vec![1, 1, 2, 2, 2, 3]);
    }

    #[test]
    fn reconnect_delay_for_is_fast_until_bond_breakdown() {
        let config = config("A1B2C3D4", 50);
        // Defaults: threshold=2, reconnect_sleep=200ms (fast), pairing_retry_interval=8s.
        assert_eq!(config.pairing_failure_threshold, 2);
        // No / sub-threshold pairing failures => fast cadence (walk-back, weak-RF blip).
        assert_eq!(reconnect_delay_for(&config, 0), Duration::from_millis(200));
        assert_eq!(reconnect_delay_for(&config, 1), Duration::from_millis(200));
        // Recognised bond breakdown => calm fixed 8 s retry (no escalation, no long cooldown).
        assert_eq!(reconnect_delay_for(&config, 2), Duration::from_secs(8));
        assert_eq!(reconnect_delay_for(&config, 50), Duration::from_secs(8));
        assert_eq!(
            reconnect_delay_for(&config, u32::MAX),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn classify_connect_failure_detects_pairing_encryption_and_timeout() {
        // The real H10 bond-desync errors from the 2026-06-03 storm classify as Pairing.
        assert_eq!(
            classify_connect_failure("connect: Runtime Error: Peer removed pairing information"),
            ConnectFailureKind::Pairing
        );
        // The encryption error contains "timed out", so order matters: it must stay Pairing.
        assert_eq!(
            classify_connect_failure(
                "connect: Runtime Error: Failed to encrypt the connection, the connection has timed out unexpectedly."
            ),
            ConnectFailureKind::Pairing
        );
        // A plain app-level connect timeout (deny-list / weak RF) is Timeout, not Pairing.
        assert_eq!(
            classify_connect_failure("connect: timed out after 8000 ms"),
            ConnectFailureKind::Timeout
        );
        assert_eq!(
            classify_connect_failure("discover GATT services: timed out after 8000 ms"),
            ConnectFailureKind::Timeout
        );
        assert_eq!(
            classify_connect_failure("PMD control characteristic not found"),
            ConnectFailureKind::Other
        );
    }

    #[test]
    fn pairing_alert_due_fires_at_threshold_and_rate_limits() {
        let config = config("A1B2C3D4", 50);
        assert_eq!(config.pairing_failure_threshold, 2);
        // Below threshold: not due (one pairing failure could be a transient glitch).
        assert!(!pairing_alert_due(
            ConnectFailureKind::Pairing,
            1,
            None,
            &config
        ));
        // At/over threshold with no prior alert: due.
        assert!(pairing_alert_due(
            ConnectFailureKind::Pairing,
            2,
            None,
            &config
        ));
        assert!(pairing_alert_due(
            ConnectFailureKind::Pairing,
            9,
            None,
            &config
        ));
        // Non-pairing failures never raise the re-pair alert, even past the threshold.
        assert!(!pairing_alert_due(
            ConnectFailureKind::Timeout,
            9,
            None,
            &config
        ));
        assert!(!pairing_alert_due(
            ConnectFailureKind::Other,
            9,
            None,
            &config
        ));
        // Rate limited by outage_alert_repeat (60 s): a recent alert suppresses, an old one re-fires.
        assert!(!pairing_alert_due(
            ConnectFailureKind::Pairing,
            5,
            Some(Duration::from_secs(59)),
            &config
        ));
        assert!(pairing_alert_due(
            ConnectFailureKind::Pairing,
            5,
            Some(Duration::from_secs(60)),
            &config
        ));
    }

    #[test]
    fn outage_alert_due_respects_threshold_and_repeat() {
        let config = config("A1B2C3D4", 50);
        // Defaults: outage_alert_after = 300 s, outage_alert_repeat = 60 s.
        // Below the alert threshold: never due.
        assert!(!outage_alert_due(Duration::from_secs(299), None, &config));
        // At/over threshold with no prior alert: due.
        assert!(outage_alert_due(Duration::from_secs(300), None, &config));
        assert!(outage_alert_due(Duration::from_secs(3600), None, &config));
        // Over threshold but a recent alert (< repeat interval) suppresses it.
        assert!(!outage_alert_due(
            Duration::from_secs(3600),
            Some(Duration::from_secs(59)),
            &config
        ));
        // Over threshold and the repeat interval elapsed: due again.
        assert!(outage_alert_due(
            Duration::from_secs(3600),
            Some(Duration::from_secs(60)),
            &config
        ));
    }

    #[tokio::test]
    async fn find_target_matches_by_name_and_stops_scan() {
        let adapter = FakeAdapter::with_peripherals(vec![peripheral(
            "id-1",
            Some(props(Some("Polar H10 A1B2C3D4"), "AA", Some(-50))),
        )]);
        let found = find_target_with_adapter(&adapter, &config("A1B2C3D4", 50))
            .await
            .expect("target found");
        assert_eq!(found.id, "id-1");
        assert_eq!(adapter.counts(), (1, 1, 1));
    }

    #[tokio::test]
    async fn find_target_matches_empty_target_only_for_polar_h10() {
        let adapter = FakeAdapter::with_peripherals(vec![
            peripheral(
                "other",
                Some(props(Some("Polar Sense 196A5131"), "AA", None)),
            ),
            peripheral(
                "h10",
                Some(props(Some("Polar H10 A1B2C3D4"), "BB", Some(-41))),
            ),
        ]);
        let found = find_target_with_adapter(&adapter, &config("   ", 50))
            .await
            .expect("H10 found");
        assert_eq!(found.id, "h10");
        assert_eq!(adapter.counts(), (1, 1, 1));
    }

    #[tokio::test]
    async fn find_target_skips_absent_properties() {
        let adapter = FakeAdapter::with_peripherals(vec![
            peripheral("none", None),
            peripheral("match", Some(props(Some("Polar H10 A1B2C3D4"), "CC", None))),
        ]);
        let found = find_target_with_adapter(&adapter, &config("match", 50))
            .await
            .expect("target found by id");
        assert_eq!(found.id, "match");
        assert_eq!(adapter.counts(), (1, 1, 1));
    }

    #[tokio::test]
    async fn find_target_timeout_stops_scan() {
        let adapter = FakeAdapter::with_peripherals(vec![peripheral(
            "other",
            Some(props(Some("Other Device"), "AA", None)),
        )]);
        let err = find_target_with_adapter(&adapter, &config("missing", 2))
            .await
            .expect_err("target missing");
        assert!(format!("{err:#}").contains("no BLE peripheral matched target"));
        let (start, stop, peripherals) = adapter.counts();
        assert_eq!(start, 1);
        assert_eq!(stop, 1);
        assert!(peripherals >= 1);
    }

    #[tokio::test]
    async fn find_target_start_error_does_not_stop_scan_that_never_started() {
        let adapter = FakeAdapter::with_start_error("start failed");
        let err = find_target_with_adapter(&adapter, &config("target", 50))
            .await
            .expect_err("start error");
        assert!(format!("{err:#}").contains("start failed"));
        assert_eq!(adapter.counts(), (1, 0, 0));
    }

    #[tokio::test]
    async fn find_target_peripherals_error_stops_scan() {
        let adapter = FakeAdapter::with_peripherals_error("peripherals failed");
        let err = find_target_with_adapter(&adapter, &config("target", 50))
            .await
            .expect_err("peripherals error");
        assert!(format!("{err:#}").contains("peripherals failed"));
        assert_eq!(adapter.counts(), (1, 1, 1));
    }

    #[tokio::test]
    async fn find_target_properties_error_stops_scan() {
        let adapter = FakeAdapter::with_peripherals(vec![FakePeripheral {
            id: "bad-props".to_string(),
            props: FakePropertiesResult::Err("properties failed"),
        }]);
        let err = find_target_with_adapter(&adapter, &config("target", 50))
            .await
            .expect_err("properties error");
        assert!(format!("{err:#}").contains("properties failed"));
        assert_eq!(adapter.counts(), (1, 1, 1));
    }

    use crate::csv::CsvWriters;
    use crate::gatt::{
        BleCentralEventStream, BleCharacteristicProperties, BleNotificationStream,
        BlePeripheralProperties,
    };
    use crate::hr::HrData;
    use crate::records::PmdFrameTiming;
    use crate::util::unix_ns;
    use futures::{future, stream};
    use std::collections::VecDeque;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[derive(Clone, Debug)]
    enum FakeOutcome<T> {
        Ok(T),
        Err(&'static str),
        Pending,
    }

    fn outcome_future<T: Send + 'static>(outcome: FakeOutcome<T>) -> BoxFuture<'static, Result<T>> {
        async move {
            match outcome {
                FakeOutcome::Ok(value) => Ok(value),
                FakeOutcome::Err(err) => Err(anyhow!(err)),
                FakeOutcome::Pending => future::pending().await,
            }
        }
        .boxed()
    }

    struct FakeGattState {
        id: String,
        connected: bool,
        characteristics: Vec<BleCharacteristic>,
        is_connected_results: VecDeque<FakeOutcome<bool>>,
        connect_results: VecDeque<FakeOutcome<()>>,
        discover_results: VecDeque<FakeOutcome<()>>,
        disconnect_results: VecDeque<FakeOutcome<()>>,
        read_results: VecDeque<FakeOutcome<Vec<u8>>>,
        write_results: VecDeque<FakeOutcome<()>>,
        subscribe_results: VecDeque<FakeOutcome<()>>,
        rssi_results: VecDeque<FakeOutcome<i16>>,
        properties_results: VecDeque<FakeOutcome<Option<BlePeripheralProperties>>>,
        notifications: Option<BleNotificationStream>,
        writes: Vec<(uuid::Uuid, Vec<u8>, BleWriteType)>,
        subscribes: Vec<uuid::Uuid>,
        reads: Vec<uuid::Uuid>,
        disconnect_calls: usize,
    }

    impl FakeGattState {
        fn new(id: &str, characteristics: Vec<BleCharacteristic>) -> Self {
            Self {
                id: id.to_string(),
                connected: false,
                characteristics,
                is_connected_results: VecDeque::new(),
                connect_results: VecDeque::new(),
                discover_results: VecDeque::new(),
                disconnect_results: VecDeque::new(),
                read_results: VecDeque::new(),
                write_results: VecDeque::new(),
                subscribe_results: VecDeque::new(),
                rssi_results: VecDeque::new(),
                properties_results: VecDeque::new(),
                notifications: None,
                writes: Vec::new(),
                subscribes: Vec::new(),
                reads: Vec::new(),
                disconnect_calls: 0,
            }
        }
    }

    #[derive(Clone)]
    struct FakeGattPeripheral {
        state: Arc<Mutex<FakeGattState>>,
    }

    impl FakeGattPeripheral {
        fn new(
            characteristics: Vec<BleCharacteristic>,
            notifications: BleNotificationStream,
        ) -> Self {
            let mut state = FakeGattState::new("fake-peripheral", characteristics);
            state.notifications = Some(notifications);
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn with_connect_error(error: &'static str) -> Self {
            let peripheral = Self::new(base_characteristics(), pending_notifications());
            peripheral
                .state
                .lock()
                .expect("lock")
                .connect_results
                .push_back(FakeOutcome::Err(error));
            peripheral
        }

        fn push_is_connected(&self, outcome: FakeOutcome<bool>) {
            self.state
                .lock()
                .expect("lock")
                .is_connected_results
                .push_back(outcome);
        }

        fn push_discover(&self, outcome: FakeOutcome<()>) {
            self.state
                .lock()
                .expect("lock")
                .discover_results
                .push_back(outcome);
        }

        fn push_read(&self, outcome: FakeOutcome<Vec<u8>>) {
            self.state
                .lock()
                .expect("lock")
                .read_results
                .push_back(outcome);
        }

        fn push_write(&self, outcome: FakeOutcome<()>) {
            self.state
                .lock()
                .expect("lock")
                .write_results
                .push_back(outcome);
        }

        fn push_subscribe(&self, outcome: FakeOutcome<()>) {
            self.state
                .lock()
                .expect("lock")
                .subscribe_results
                .push_back(outcome);
        }

        fn push_rssi(&self, outcome: FakeOutcome<i16>) {
            self.state
                .lock()
                .expect("lock")
                .rssi_results
                .push_back(outcome);
        }

        fn push_properties(&self, outcome: FakeOutcome<Option<BlePeripheralProperties>>) {
            self.state
                .lock()
                .expect("lock")
                .properties_results
                .push_back(outcome);
        }

        fn written_commands(&self) -> Vec<Vec<u8>> {
            self.state
                .lock()
                .expect("lock")
                .writes
                .iter()
                .map(|(_, data, _)| data.clone())
                .collect()
        }
    }

    impl GattPeripheral for FakeGattPeripheral {
        fn id_string(&self) -> String {
            self.state.lock().expect("lock").id.clone()
        }

        fn is_connected_gatt(&self) -> BoxFuture<'_, Result<bool>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                state
                    .is_connected_results
                    .pop_front()
                    .unwrap_or(FakeOutcome::Ok(state.connected))
            };
            outcome_future(outcome)
        }

        fn connect_gatt(&self) -> BoxFuture<'_, Result<()>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                let outcome = state
                    .connect_results
                    .pop_front()
                    .unwrap_or(FakeOutcome::Ok(()));
                if matches!(outcome, FakeOutcome::Ok(())) {
                    state.connected = true;
                }
                outcome
            };
            outcome_future(outcome)
        }

        fn disconnect_gatt(&self) -> BoxFuture<'_, Result<()>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                state.disconnect_calls += 1;
                let outcome = state
                    .disconnect_results
                    .pop_front()
                    .unwrap_or(FakeOutcome::Ok(()));
                if matches!(outcome, FakeOutcome::Ok(())) {
                    state.connected = false;
                }
                outcome
            };
            outcome_future(outcome)
        }

        fn discover_services_gatt(&self) -> BoxFuture<'_, Result<()>> {
            let outcome = self
                .state
                .lock()
                .expect("lock")
                .discover_results
                .pop_front()
                .unwrap_or(FakeOutcome::Ok(()));
            outcome_future(outcome)
        }

        fn characteristics_gatt(&self) -> Vec<BleCharacteristic> {
            self.state.lock().expect("lock").characteristics.clone()
        }

        fn read_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<Vec<u8>>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                state.reads.push(characteristic.uuid);
                state.read_results.pop_front().unwrap_or_else(|| {
                    if characteristic.uuid == uuid(PMD_CONTROL_UUID) {
                        FakeOutcome::Ok(vec![0x0f, 0x05])
                    } else if characteristic.uuid == uuid(BATTERY_LEVEL_UUID) {
                        FakeOutcome::Ok(vec![88])
                    } else {
                        FakeOutcome::Ok(b"fake".to_vec())
                    }
                })
            };
            outcome_future(outcome)
        }

        fn write_gatt(
            &self,
            characteristic: &BleCharacteristic,
            data: &[u8],
            write_type: BleWriteType,
        ) -> BoxFuture<'_, Result<()>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                state
                    .writes
                    .push((characteristic.uuid, data.to_vec(), write_type));
                state
                    .write_results
                    .pop_front()
                    .unwrap_or(FakeOutcome::Ok(()))
            };
            outcome_future(outcome)
        }

        fn subscribe_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<()>> {
            let outcome = {
                let mut state = self.state.lock().expect("lock");
                state.subscribes.push(characteristic.uuid);
                state
                    .subscribe_results
                    .pop_front()
                    .unwrap_or(FakeOutcome::Ok(()))
            };
            outcome_future(outcome)
        }

        fn notifications_gatt(&self) -> BoxFuture<'_, Result<BleNotificationStream>> {
            let stream = self
                .state
                .lock()
                .expect("lock")
                .notifications
                .take()
                .unwrap_or_else(pending_notifications);
            outcome_future(FakeOutcome::Ok(stream))
        }

        fn read_rssi_gatt(&self) -> BoxFuture<'_, Result<i16>> {
            let outcome = self
                .state
                .lock()
                .expect("lock")
                .rssi_results
                .pop_front()
                .unwrap_or(FakeOutcome::Ok(-42));
            outcome_future(outcome)
        }

        fn properties_gatt(&self) -> BoxFuture<'_, Result<Option<BlePeripheralProperties>>> {
            let outcome = self
                .state
                .lock()
                .expect("lock")
                .properties_results
                .pop_front()
                .unwrap_or(FakeOutcome::Ok(Some(BlePeripheralProperties {
                    rssi: Some(-43),
                })));
            outcome_future(outcome)
        }
    }

    #[derive(Clone)]
    struct FakeCentral {
        events: Arc<Mutex<Option<BleCentralEventStream>>>,
    }

    impl FakeCentral {
        fn new(events: BleCentralEventStream) -> Self {
            Self {
                events: Arc::new(Mutex::new(Some(events))),
            }
        }
    }

    impl Default for FakeCentral {
        fn default() -> Self {
            Self::new(Box::pin(stream::pending()))
        }
    }

    impl StreamingCentral for FakeCentral {
        fn central_events_gatt(&self) -> BoxFuture<'_, Result<BleCentralEventStream>> {
            let events = self
                .events
                .lock()
                .expect("lock")
                .take()
                .unwrap_or_else(|| Box::pin(stream::pending()));
            outcome_future(FakeOutcome::Ok(events))
        }
    }

    fn pending_notifications() -> BleNotificationStream {
        Box::pin(stream::pending())
    }

    fn notification_stream(values: Vec<BleNotification>) -> BleNotificationStream {
        Box::pin(stream::iter(values))
    }

    fn notifications_then_pending(values: Vec<BleNotification>) -> BleNotificationStream {
        Box::pin(stream::iter(values).chain(stream::pending()))
    }

    fn repeating_notification(
        delay: Duration,
        notification: BleNotification,
    ) -> BleNotificationStream {
        Box::pin(stream::unfold(0u64, move |counter| {
            let notification = notification.clone();
            async move {
                sleep(delay).await;
                Some((notification, counter + 1))
            }
        }))
    }

    fn delayed_disconnect_event(id: &str, delay: Duration) -> BleCentralEventStream {
        let id = id.to_string();
        Box::pin(stream::once(async move {
            sleep(delay).await;
            BleCentralEvent::DeviceDisconnected(id)
        }))
    }

    fn ch(uuid_str: &str) -> BleCharacteristic {
        BleCharacteristic {
            uuid: uuid(uuid_str),
            service_uuid: uuid("0000180d-0000-1000-8000-00805f9b34fb"),
            properties: BleCharacteristicProperties {
                read: true,
                write: true,
                write_without_response: true,
                notify: true,
            },
        }
    }

    fn base_characteristics() -> Vec<BleCharacteristic> {
        vec![
            ch(HR_MEASUREMENT_UUID),
            ch(PMD_CONTROL_UUID),
            ch(PMD_DATA_UUID),
        ]
    }

    fn base_characteristics_with(extra: &[&str]) -> Vec<BleCharacteristic> {
        let mut chars = base_characteristics();
        chars.extend(extra.iter().map(|uuid_str| ch(uuid_str)));
        chars
    }

    fn pmd_control_notification(measurement_type: u8, error_code: u8) -> BleNotification {
        notification(
            PMD_CONTROL_UUID,
            &[0xf0, 0x02, measurement_type, error_code, 0x00],
        )
    }

    fn startup_success_notifications() -> Vec<BleNotification> {
        vec![
            pmd_control_notification(ECG_MEASUREMENT_TYPE, 0),
            pmd_control_notification(ACC_MEASUREMENT_TYPE, 0),
        ]
    }

    fn notification(uuid_str: &str, value: &[u8]) -> BleNotification {
        BleNotification {
            uuid: uuid(uuid_str),
            value: value.to_vec(),
        }
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
        data.push(frame_type | if compressed { 0x80 } else { 0x00 });
        data.extend_from_slice(content);
        data
    }

    fn ecg_notification() -> BleNotification {
        notification(
            PMD_DATA_UUID,
            &pmd_frame(ECG_MEASUREMENT_TYPE, 1_000_000_000, 0, false, &[1, 0, 0]),
        )
    }

    fn acc_notification() -> BleNotification {
        notification(
            PMD_DATA_UUID,
            &pmd_frame(ACC_MEASUREMENT_TYPE, 2_000_000_000, 0, false, &[1, 2, 3]),
        )
    }

    fn hr_notification() -> BleNotification {
        notification(HR_MEASUREMENT_UUID, &[0x06, 60])
    }

    fn test_config_gatt() -> CollectorConfig {
        let mut config = CollectorConfig::new("fake".to_string(), PathBuf::from("data"), 1);
        config.pre_connect_disconnect_timeout = Duration::from_millis(1);
        config.pre_connect_settle = Duration::ZERO;
        config.connect_timeout = Duration::from_millis(5);
        config.discover_services_timeout = Duration::from_millis(5);
        config.notification_stream_timeout = Duration::from_millis(5);
        config.central_events_timeout = Duration::from_millis(5);
        config.read_timeout = Duration::from_millis(5);
        config.write_timeout = Duration::from_millis(5);
        config.subscribe_timeout = Duration::from_millis(5);
        config.pmd_control_response_timeout = Duration::from_millis(5);
        config.startup_stop_settle = Duration::ZERO;
        config.startup_between_streams = Duration::ZERO;
        config.notification_watchdog = Duration::from_millis(20);
        config.pmd_stream_watchdog = Duration::from_millis(20);
        config.health_interval = Duration::from_millis(1);
        config.battery_interval = Duration::from_millis(1);
        config.rssi_timeout = Duration::from_millis(1);
        config.properties_timeout = Duration::from_millis(1);
        config.cleanup_timeout = Duration::from_millis(1);
        config.psftp_response_timeout = Duration::from_millis(5);
        config.progress_every_pmd_frames = 0;
        config
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_ble_{name}_{}_{}",
            std::process::id(),
            unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn test_writers(name: &str) -> (PathBuf, CsvWriters) {
        let base = temp_dir(name);
        let writers = CsvWriters::new(&base).expect("writers");
        (base, writers)
    }

    fn read_events(writers: &mut impl RecordSink) -> String {
        writers.flush_all().expect("flush");
        fs::read_to_string(writers.session_dir().join("events.csv")).expect("read events")
    }

    fn remove_dir(path: &Path) {
        fs::remove_dir_all(path).ok();
    }

    async fn run_fake_connection(
        name: &str,
        central: &FakeCentral,
        peripheral: &FakeGattPeripheral,
        config: &CollectorConfig,
    ) -> (Result<()>, String, PathBuf) {
        let (base, mut writers) = test_writers(name);
        let mut parser = PmdParser::new();
        let result =
            run_connected_with_gatt(central, peripheral, &mut writers, &mut parser, config).await;
        let events = read_events(&mut writers);
        (result, events, base)
    }

    struct FailingRawSink {
        dir: PathBuf,
        connection_id: u32,
    }

    impl FailingRawSink {
        fn new(name: &str) -> Self {
            Self {
                dir: temp_dir(name),
                connection_id: 0,
            }
        }
    }

    impl RecordSink for FailingRawSink {
        fn begin_connection(&mut self) -> u32 {
            self.connection_id = self.connection_id.saturating_add(1);
            self.connection_id
        }

        fn session_dir(&self) -> &Path {
            &self.dir
        }

        fn event(&mut self, _event: &str, _detail: &str) -> Result<()> {
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
            Err(anyhow!("collector WAL append failed"))
        }

        fn pmd_timing(&mut self, _timing: &PmdFrameTiming) -> Result<()> {
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
            _collector_ns: u128,
            _rssi: Option<i16>,
            _pmd_frames: u64,
            _hr_frames: u64,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn raw_wal_write_failure_propagates_loudly_from_collect_path() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notification_stream(startup_success_notifications()),
        );
        let mut sink = FailingRawSink::new("failing_raw");
        let mut parser = PmdParser::new();
        let err = run_connected_with_gatt(&central, &peripheral, &mut sink, &mut parser, &config)
            .await
            .expect_err("raw WAL write failure must stop collection");
        assert!(
            format!("{err:#}").contains("collector WAL append failed"),
            "unexpected error: {err:#}"
        );
        fs::remove_dir_all(sink.dir).ok();
    }

    #[tokio::test]
    async fn connect_failure_returns_error_after_logging_connecting_event() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::with_connect_error("connect failed");
        let (result, events, base) =
            run_fake_connection("connect_failure", &central, &peripheral, &config).await;
        let err = result.expect_err("connect failure returned");
        assert!(format!("{err:#}").contains("connect failed"));
        assert!(events.contains("connecting"));
        assert!(!events.contains("connected"));
        // A failed connect must still cancel any pending/half-open connection.
        assert!(events.contains("cancel_pending_connect"));
        assert_eq!(
            peripheral.state.lock().expect("lock").disconnect_calls,
            1,
            "failed connect must cancel the pending connection exactly once"
        );
        // A clean cancel marks the error so the outer loop skips its redundant disconnect.
        assert!(
            err.downcast_ref::<PendingConnectAlreadyCancelled>()
                .is_some(),
            "cleanly cancelled connect failure should carry the skip-cleanup marker"
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn connect_timeout_cancels_pending_connection() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(base_characteristics(), pending_notifications());
        // A connect that never resolves forces the connect_timeout (ble_timeout) to fire.
        peripheral
            .state
            .lock()
            .expect("lock")
            .connect_results
            .push_back(FakeOutcome::Pending);
        let (result, events, base) =
            run_fake_connection("connect_timeout", &central, &peripheral, &config).await;
        let err = result.expect_err("connect timeout returned");
        assert!(
            format!("{err:#}").contains("timed out"),
            "unexpected error: {err:#}"
        );
        assert!(events.contains("connecting"));
        assert!(!events.contains("connected"));
        // CoreBluetooth has no native connect timeout, so the timed-out connect must be
        // explicitly cancelled to avoid a zombie pending connection.
        assert!(
            events.contains("cancel_pending_connect"),
            "expected cancel_pending_connect event, got: {events}"
        );
        assert_eq!(
            peripheral.state.lock().expect("lock").disconnect_calls,
            1,
            "timed-out connect must trigger exactly one cancel disconnect"
        );
        assert!(
            err.downcast_ref::<PendingConnectAlreadyCancelled>()
                .is_some(),
            "cleanly cancelled connect timeout should carry the skip-cleanup marker"
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn connect_failure_cancel_tolerates_disconnect_error() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::with_connect_error("connect failed");
        // The cancel's disconnect also fails; it must be tolerated (logged, not fatal),
        // and the original connect error must be preserved, not replaced by it.
        peripheral
            .state
            .lock()
            .expect("lock")
            .disconnect_results
            .push_back(FakeOutcome::Err("disconnect failed"));
        let (result, events, base) =
            run_fake_connection("connect_cancel_disc_err", &central, &peripheral, &config).await;
        let err = result.expect_err("connect failure returned");
        assert!(format!("{err:#}").contains("connect failed"));
        assert!(events.contains("cancel_pending_connect_failed"));
        // The cancel did not complete, so the error is NOT marked: the outer loop must
        // still run its own cleanup disconnect with the longer timeout.
        assert!(
            err.downcast_ref::<PendingConnectAlreadyCancelled>()
                .is_none(),
            "an incomplete cancel must not mark the error as already cleaned up"
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn connect_failure_cancel_timeout_falls_through_to_outer_cleanup() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::with_connect_error("connect failed");
        // The cancel's disconnect hangs and hits pre_connect_disconnect_timeout; the error
        // must fall through unmarked so the outer loop retries teardown.
        peripheral
            .state
            .lock()
            .expect("lock")
            .disconnect_results
            .push_back(FakeOutcome::Pending);
        let (result, events, base) = run_fake_connection(
            "connect_cancel_disc_timeout",
            &central,
            &peripheral,
            &config,
        )
        .await;
        let err = result.expect_err("connect failure returned");
        assert!(format!("{err:#}").contains("connect failed"));
        assert!(events.contains("cancel_pending_connect_timeout"));
        assert!(
            err.downcast_ref::<PendingConnectAlreadyCancelled>()
                .is_none(),
            "a timed-out cancel must not mark the error as already cleaned up"
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn service_discovery_failure_is_returned_after_connect() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(base_characteristics(), pending_notifications());
        peripheral.push_discover(FakeOutcome::Err("discover failed"));
        let (result, events, base) =
            run_fake_connection("discover_failure", &central, &peripheral, &config).await;
        let err = result.expect_err("discovery failure returned");
        assert!(format!("{err:#}").contains("discover failed"));
        assert!(events.contains("connected"));
        assert!(!events.contains("services_discovered"));
        remove_dir(&base);
    }

    #[tokio::test]
    async fn missing_pmd_control_or_data_characteristic_fails_startup() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let missing_control = FakeGattPeripheral::new(
            vec![ch(HR_MEASUREMENT_UUID), ch(PMD_DATA_UUID)],
            pending_notifications(),
        );
        let (result, _events, base) =
            run_fake_connection("missing_pmd_control", &central, &missing_control, &config).await;
        assert!(
            format!("{:#}", result.expect_err("missing control fails"))
                .contains("PMD control characteristic not found")
        );
        remove_dir(&base);

        let missing_data = FakeGattPeripheral::new(
            vec![ch(HR_MEASUREMENT_UUID), ch(PMD_CONTROL_UUID)],
            pending_notifications(),
        );
        let (result, _events, base) =
            run_fake_connection("missing_pmd_data", &central, &missing_data, &config).await;
        assert!(
            format!("{:#}", result.expect_err("missing data fails"))
                .contains("PMD data characteristic not found")
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn psftp_failure_while_disconnected_aborts_startup() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics_with(&[crate::config::PSFTP_MTU_UUID]),
            pending_notifications(),
        );
        peripheral.push_is_connected(FakeOutcome::Ok(false));
        peripheral.push_is_connected(FakeOutcome::Ok(false));
        peripheral.push_subscribe(FakeOutcome::Err("psftp subscribe failed"));

        let (result, events, base) =
            run_fake_connection("psftp_disconnected", &central, &peripheral, &config).await;
        let err = result.expect_err("disconnected PS-FTP failure aborts");
        assert!(format!("{err:#}").contains("PS-FTP H10 time set failed"));
        assert!(events.contains("psftp_set_local_time_error"));
        assert!(!events.contains("subscribed,\"heart_rate_measurement\""));
        remove_dir(&base);
    }

    #[tokio::test]
    async fn pmd_ecg_start_rejection_causes_startup_failure() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notification_stream(vec![pmd_control_notification(ECG_MEASUREMENT_TYPE, 5)]),
        );
        let (result, events, base) =
            run_fake_connection("ecg_rejected", &central, &peripheral, &config).await;
        let err = result.expect_err("ECG rejection fails startup");
        assert!(format!("{err:#}").contains("PMD ECG command rejected"));
        assert!(events.contains("start_stream"));
        assert!(!events.contains("stream=ECG"));
        remove_dir(&base);
    }

    #[tokio::test]
    async fn pmd_acc_start_rejection_causes_startup_failure() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notification_stream(vec![
                pmd_control_notification(ECG_MEASUREMENT_TYPE, 0),
                pmd_control_notification(ACC_MEASUREMENT_TYPE, 6),
            ]),
        );
        let (result, events, base) =
            run_fake_connection("acc_rejected", &central, &peripheral, &config).await;
        let err = result.expect_err("ACC rejection fails startup");
        assert!(format!("{err:#}").contains("PMD ACC command rejected"));
        assert!(events.contains("stream=ECG"));
        assert!(!events.contains("stream=ACC"));
        remove_dir(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn ecg_watchdog_fires_while_acc_and_hr_notifications_continue() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let startup = startup_success_notifications();
        let continuing = stream::iter(vec![acc_notification(), hr_notification()])
            .cycle()
            .then(|notification| async move {
                sleep(Duration::from_millis(1)).await;
                notification
            });
        let notifications = Box::pin(stream::iter(startup).chain(continuing));
        let peripheral = FakeGattPeripheral::new(base_characteristics(), notifications);

        let (result, events, base) =
            run_fake_connection("ecg_watchdog", &central, &peripheral, &config).await;
        result.expect("watchdog exits cleanly");
        assert!(events.contains("ecg_watchdog_timeout"));
        assert!(!events.contains("notification_watchdog_timeout"));
        remove_dir(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn acc_watchdog_fires_while_ecg_and_hr_notifications_continue() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let startup = startup_success_notifications();
        let continuing = stream::iter(vec![ecg_notification(), hr_notification()])
            .cycle()
            .then(|notification| async move {
                sleep(Duration::from_millis(1)).await;
                notification
            });
        let notifications = Box::pin(stream::iter(startup).chain(continuing));
        let peripheral = FakeGattPeripheral::new(base_characteristics(), notifications);

        let (result, events, base) =
            run_fake_connection("acc_watchdog", &central, &peripheral, &config).await;
        result.expect("watchdog exits cleanly");
        assert!(events.contains("acc_watchdog_timeout"));
        assert!(!events.contains("notification_watchdog_timeout"));
        remove_dir(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn central_device_disconnected_exits_before_notification_watchdog() {
        let config = test_config_gatt();
        let central = FakeCentral::new(delayed_disconnect_event(
            "fake-peripheral",
            Duration::from_millis(1),
        ));
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notifications_then_pending(startup_success_notifications()),
        );

        let (result, events, base) =
            run_fake_connection("central_disconnect", &central, &peripheral, &config).await;
        result.expect("central disconnect exits cleanly");
        assert!(events.contains("central_device_disconnected"));
        assert!(events.contains("cleanup_skipped_stream_loop"));
        assert!(!events.contains("notification_watchdog_timeout"));
        assert!(!events.contains("cleanup_stop_ecg_stream_loop"));
        assert_eq!(
            peripheral.written_commands(),
            vec![
                ECG_STOP_CMD.to_vec(),
                ACC_STOP_CMD.to_vec(),
                ECG_START_CMD.to_vec(),
                ACC_START_CMD.to_vec(),
            ]
        );
        remove_dir(&base);
    }

    #[tokio::test]
    async fn notification_stream_end_exits_stream_loop() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notification_stream(startup_success_notifications()),
        );

        let (result, events, base) =
            run_fake_connection("notification_end", &central, &peripheral, &config).await;
        result.expect("stream end exits cleanly");
        assert!(events.contains("notification_stream_ended"));
        assert!(events.contains("stream_loop_ending"));
        remove_dir(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_logs_stop_errors_and_timeouts() {
        let config = test_config_gatt();
        let central = FakeCentral::default();
        let peripheral = FakeGattPeripheral::new(
            base_characteristics(),
            notification_stream(startup_success_notifications()),
        );
        for _ in 0..4 {
            peripheral.push_write(FakeOutcome::Ok(()));
        }
        peripheral.push_write(FakeOutcome::Err("cleanup ECG stop failed"));
        peripheral.push_write(FakeOutcome::Pending);
        peripheral
            .state
            .lock()
            .expect("lock")
            .disconnect_results
            .push_back(FakeOutcome::Pending);

        let (result, events, base) =
            run_fake_connection("cleanup_errors", &central, &peripheral, &config).await;
        result.expect("cleanup errors are best effort");
        assert!(events.contains("cleanup_stop_ecg_stream_loop_error"));
        assert!(events.contains("cleanup_stop_acc_stream_loop_timeout"));
        assert!(events.contains("cleanup_disconnect_stream_loop_timeout"));
        assert_eq!(
            peripheral.written_commands(),
            vec![
                ECG_STOP_CMD.to_vec(),
                ACC_STOP_CMD.to_vec(),
                ECG_START_CMD.to_vec(),
                ACC_START_CMD.to_vec(),
                ECG_STOP_CMD.to_vec(),
                ACC_STOP_CMD.to_vec(),
            ]
        );
        remove_dir(&base);
    }

    #[tokio::test(start_paused = true)]
    async fn battery_rssi_and_properties_errors_are_logged_without_hanging() {
        let config = test_config_gatt();
        let central = FakeCentral::new(delayed_disconnect_event(
            "fake-peripheral",
            Duration::from_millis(5),
        ));
        let startup = startup_success_notifications();
        let notifications = Box::pin(stream::iter(startup).chain(repeating_notification(
            Duration::from_millis(1),
            hr_notification(),
        )));
        let peripheral = FakeGattPeripheral::new(
            base_characteristics_with(&[BATTERY_LEVEL_UUID]),
            notifications,
        );
        peripheral.push_read(FakeOutcome::Err("initial battery failed"));
        peripheral.push_read(FakeOutcome::Ok(vec![0x0f, 0x05]));
        peripheral.push_read(FakeOutcome::Err("periodic battery failed"));
        peripheral.push_rssi(FakeOutcome::Err("RSSI failed"));
        peripheral.push_properties(FakeOutcome::Err("properties failed"));

        let (result, events, base) =
            run_fake_connection("health_errors", &central, &peripheral, &config).await;
        result.expect("health errors do not hang");
        assert!(events.contains("battery_read_error"));
        assert!(events.contains("read_rssi_error"));
        assert!(events.contains("properties_error"));
        assert!(events.contains("central_device_disconnected"));
        remove_dir(&base);
    }
}
