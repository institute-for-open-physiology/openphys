use crate::config::{
    CollectorConfig, PSFTP_ATT_MTU_PAYLOAD, PSFTP_MTU_UUID, PSFTP_SET_LOCAL_TIME_QUERY_ID,
};
use crate::gatt::{BleNotification, BleWriteType, GattPeripheral, find_ble_char};
use crate::sink::RecordSink;
use crate::util::{ble_timeout, unix_ns, uuid};
use anyhow::{Context, Result, anyhow};
use chrono::{Datelike, Local, Timelike};
use futures::{Stream, StreamExt};
use tokio::time::{Instant, timeout};
use uuid::Uuid;

pub async fn try_set_h10_local_time_psftp<P, S>(
    peripheral: &P,
    notifications: &mut S,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<()>
where
    P: GattPeripheral,
    S: Stream<Item = BleNotification> + Unpin,
{
    let Some(mtu_char) = find_ble_char(&peripheral.characteristics_gatt(), PSFTP_MTU_UUID) else {
        writers.event(
            "psftp_set_local_time_unavailable",
            "PS-FTP MTU characteristic absent",
        )?;
        return Ok(());
    };

    let operation_id = Uuid::new_v4();
    let operation_start = Instant::now();
    let subscription_id = Uuid::new_v4();
    writers.subscription_attempt(
        subscription_id,
        &mtu_char.uuid.to_string(),
        "notify",
        "psftp_mtu",
    )?;
    let subscribe_start = Instant::now();
    match ble_timeout(
        "subscribe PS-FTP MTU",
        config.subscribe_timeout,
        peripheral.subscribe_gatt(&mtu_char),
    )
    .await
    {
        Ok(()) => {
            writers.subscription_result(
                subscription_id,
                &mtu_char.uuid.to_string(),
                elapsed_ms(subscribe_start),
            )?;
            writers.event("subscribed", "psftp_mtu")?;
        }
        Err(err) => {
            let detail = format!("{err:#}");
            writers.subscription_error(
                subscription_id,
                &mtu_char.uuid.to_string(),
                &detail,
                elapsed_ms(subscribe_start),
            )?;
            writers.psftp_set_local_time_result(
                operation_id,
                false,
                None,
                &[],
                Some(&detail),
                elapsed_ms(operation_start),
            )?;
            return Err(anyhow!(detail));
        }
    }

    let now = Local::now();
    let tz_offset_minutes = now.offset().local_minus_utc() / 60;
    let params = build_pftp_set_local_time_params(now, tz_offset_minutes);
    let packets = build_psftp_query_packets(PSFTP_SET_LOCAL_TIME_QUERY_ID, &params);

    let local_time_iso8601 = now.to_rfc3339();
    let local_time_unix_ns = now
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("local time is outside nanosecond timestamp range"))?;
    writers.psftp_set_local_time_attempt(
        operation_id,
        &local_time_iso8601,
        local_time_unix_ns,
        tz_offset_minutes,
        None,
        &params,
        PSFTP_MTU_UUID,
        PSFTP_MTU_UUID,
    )?;
    writers.event(
        "psftp_set_local_time_attempt",
        &format!(
            "local_time={} tz_offset_minutes={} params={} packets={}",
            local_time_iso8601,
            tz_offset_minutes,
            hex::encode(&params),
            packets.len()
        ),
    )?;

    for packet in &packets {
        let write_id = Uuid::new_v4();
        let write_start = Instant::now();
        writers.gatt_write_request(
            write_id,
            &mtu_char.uuid.to_string(),
            BleWriteType::WithResponse,
            "psftp_set_local_time_packet",
            packet,
        )?;
        match ble_timeout(
            &format!("write PS-FTP MTU packet {}", hex::encode(packet)),
            config.write_timeout,
            peripheral.write_gatt(&mtu_char, packet, BleWriteType::WithResponse),
        )
        .await
        {
            Ok(()) => writers.gatt_write_result(
                write_id,
                &mtu_char.uuid.to_string(),
                elapsed_ms(write_start),
            )?,
            Err(err) => {
                let detail = format!("{err:#}");
                writers.gatt_write_error(
                    write_id,
                    &mtu_char.uuid.to_string(),
                    &detail,
                    elapsed_ms(write_start),
                )?;
                writers.psftp_set_local_time_result(
                    operation_id,
                    false,
                    None,
                    &[],
                    Some(&detail),
                    elapsed_ms(operation_start),
                )?;
                return Err(anyhow!(detail));
            }
        }
    }

    let response = match read_psftp_query_response(notifications, writers, config).await {
        Ok(response) => response,
        Err(err) => {
            let detail = format!("{err:#}");
            writers.psftp_set_local_time_result(
                operation_id,
                false,
                None,
                &[],
                Some(&detail),
                elapsed_ms(operation_start),
            )?;
            return Err(err);
        }
    };
    writers.psftp_set_local_time_result(
        operation_id,
        true,
        Some(0),
        &response,
        None,
        elapsed_ms(operation_start),
    )?;
    writers.event(
        "psftp_set_local_time_success",
        &format!(
            "local_time={} tz_offset_minutes={} response={}",
            local_time_iso8601,
            tz_offset_minutes,
            hex::encode(response)
        ),
    )?;
    Ok(())
}

async fn read_psftp_query_response<S>(
    notifications: &mut S,
    writers: &mut impl RecordSink,
    config: &CollectorConfig,
) -> Result<Vec<u8>>
where
    S: Stream<Item = BleNotification> + Unpin,
{
    let mut expected_seq: u8 = 0;
    let mut expected_next: u8 = 0;
    let mut output = Vec::new();
    let deadline = Instant::now() + config.psftp_response_timeout;

    loop {
        // This is the one intentional notification-stream await outside the main
        // streaming loop; it is bounded by one absolute psftp_response_timeout
        // deadline because the H10 should answer setLocalTime immediately or be
        // treated as a failed connection. Ignored unrelated notifications must
        // not reset this startup timeout indefinitely.
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!(
                "timeout waiting for PS-FTP response after {} ms",
                config.psftp_response_timeout.as_millis()
            ));
        }
        let notification = timeout(deadline - now, notifications.next())
            .await
            .context("timeout waiting for PS-FTP response")?
            .ok_or_else(|| {
                anyhow!("notification stream ended while waiting for PS-FTP response")
            })?;
        let collector_ns = unix_ns();
        let notification_uuid = notification.uuid.to_string();
        writers.raw_gatt(
            collector_ns,
            &notification_uuid,
            "inbound",
            "notification",
            &notification.value,
            "psftp_response_wait_notification",
        )?;

        if notification.uuid != uuid(PSFTP_MTU_UUID) {
            writers.event(
                "psftp_ignored_notification",
                &format!(
                    "uuid={} len={}",
                    notification.uuid,
                    notification.value.len()
                ),
            )?;
            continue;
        }

        let data = notification.value;
        writers.event(
            "psftp_mtu_notification",
            &format!("hex={}", hex::encode(&data)),
        )?;
        if data.is_empty() {
            return Err(anyhow!("empty PS-FTP response frame"));
        }

        let header = data[0];
        let next = header & 0x01;
        let status = (header & 0x06) >> 1;
        let seq = header >> 4;
        if seq != expected_seq {
            return Err(anyhow!(
                "PS-FTP sequence mismatch: expected {expected_seq}, got {seq}"
            ));
        }
        expected_seq = (expected_seq + 1) & 0x0f;
        if next != expected_next {
            return Err(anyhow!(
                "PS-FTP next-frame mismatch: expected {expected_next}, got {next}"
            ));
        }
        expected_next = 1;

        match status {
            3 => output.extend_from_slice(&data[1..]),
            1 => {
                output.extend_from_slice(&data[1..]);
                return Ok(output);
            }
            0 => {
                if data.len() == 3 {
                    let error = u16::from_le_bytes([data[1], data[2]]);
                    if error == 0 {
                        return Ok(output);
                    }
                    return Err(anyhow!("PS-FTP response error code {error}"));
                }
                return Ok(output);
            }
            other => return Err(anyhow!("unexpected PS-FTP frame status {other}")),
        }
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn build_pftp_set_local_time_params(
    now: chrono::DateTime<Local>,
    tz_offset_minutes: i32,
) -> Vec<u8> {
    let mut date = Vec::new();
    pb_uint32_field(&mut date, 1, now.year() as u32);
    pb_uint32_field(&mut date, 2, now.month());
    pb_uint32_field(&mut date, 3, now.day());

    let mut time = Vec::new();
    pb_uint32_field(&mut time, 1, now.hour());
    pb_uint32_field(&mut time, 2, now.minute());
    pb_uint32_field(&mut time, 3, now.second());
    pb_uint32_field(&mut time, 4, now.nanosecond() / 1_000_000);

    let mut params = Vec::new();
    pb_message_field(&mut params, 1, &date);
    pb_message_field(&mut params, 2, &time);
    pb_int32_field(&mut params, 3, tz_offset_minutes);
    params
}

fn build_psftp_query_packets(query_id: u16, params: &[u8]) -> Vec<Vec<u8>> {
    let mut stream = Vec::with_capacity(params.len() + 2);
    stream.push((query_id & 0xff) as u8);
    stream.push(((query_id >> 8) as u8) | 0x80);
    stream.extend_from_slice(params);

    let mut packets = Vec::new();
    let mut offset = 0usize;
    let mut next = 0u8;
    let mut seq = 0u8;
    loop {
        let remaining = stream.len().saturating_sub(offset);
        let take = remaining.min(PSFTP_ATT_MTU_PAYLOAD - 1);
        let more = remaining > (PSFTP_ATT_MTU_PAYLOAD - 1);
        let mut packet = Vec::with_capacity(take + 1);
        let status_bits = if more { 0x06 } else { 0x02 };
        packet.push(status_bits | next | (seq << 4));
        packet.extend_from_slice(&stream[offset..offset + take]);
        packets.push(packet);
        offset += take;
        if !more {
            break;
        }
        next = 1;
        seq = (seq + 1) & 0x0f;
    }
    packets
}

fn pb_uint32_field(out: &mut Vec<u8>, field_number: u32, value: u32) {
    out.push((field_number << 3) as u8);
    encode_varint(out, value as u64);
}

fn pb_int32_field(out: &mut Vec<u8>, field_number: u32, value: i32) {
    out.push((field_number << 3) as u8);
    if value < 0 {
        encode_varint(out, value as i64 as u64);
    } else {
        encode_varint(out, value as u64);
    }
}

fn pb_message_field(out: &mut Vec<u8>, field_number: u32, bytes: &[u8]) {
    out.push(((field_number << 3) | 2) as u8);
    encode_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value as u8) & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::CsvWriters;
    use crate::gatt::{
        BleCharacteristic, BleCharacteristicProperties, BleNotificationStream,
        BlePeripheralProperties,
    };
    use crate::util::{unix_ns, uuid};
    use chrono::{TimeZone, Timelike};
    use futures::{FutureExt, stream};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_psftp_{name}_{}_{}",
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

    fn test_config(timeout_ms: u64) -> CollectorConfig {
        let mut config = CollectorConfig::new("target".to_string(), PathBuf::from("data"), 1);
        config.psftp_response_timeout = Duration::from_millis(timeout_ms);
        config
    }

    fn notification(uuid_str: &str, value: &[u8]) -> BleNotification {
        BleNotification {
            uuid: uuid(uuid_str),
            value: value.to_vec(),
        }
    }

    #[derive(Clone)]
    struct FakePsftpPeripheral {
        characteristics: Vec<BleCharacteristic>,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl FakePsftpPeripheral {
        fn with_psftp_mtu() -> Self {
            Self {
                characteristics: vec![BleCharacteristic {
                    uuid: uuid(PSFTP_MTU_UUID),
                    service_uuid: uuid("0000180d-0000-1000-8000-00805f9b34fb"),
                    properties: BleCharacteristicProperties {
                        read: false,
                        write: true,
                        write_without_response: false,
                        notify: true,
                    },
                }],
                writes: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn writes(&self) -> Vec<Vec<u8>> {
            self.writes.lock().expect("lock").clone()
        }
    }

    impl GattPeripheral for FakePsftpPeripheral {
        fn id_string(&self) -> String {
            "fake-psftp".to_string()
        }

        fn is_connected_gatt(&self) -> futures::future::BoxFuture<'_, Result<bool>> {
            async { Ok(true) }.boxed()
        }

        fn connect_gatt(&self) -> futures::future::BoxFuture<'_, Result<()>> {
            async { Ok(()) }.boxed()
        }

        fn disconnect_gatt(&self) -> futures::future::BoxFuture<'_, Result<()>> {
            async { Ok(()) }.boxed()
        }

        fn discover_services_gatt(&self) -> futures::future::BoxFuture<'_, Result<()>> {
            async { Ok(()) }.boxed()
        }

        fn characteristics_gatt(&self) -> Vec<BleCharacteristic> {
            self.characteristics.clone()
        }

        fn read_gatt(
            &self,
            _characteristic: &BleCharacteristic,
        ) -> futures::future::BoxFuture<'_, Result<Vec<u8>>> {
            async { Ok(Vec::new()) }.boxed()
        }

        fn write_gatt(
            &self,
            _characteristic: &BleCharacteristic,
            data: &[u8],
            _write_type: BleWriteType,
        ) -> futures::future::BoxFuture<'_, Result<()>> {
            self.writes.lock().expect("lock").push(data.to_vec());
            async { Ok(()) }.boxed()
        }

        fn subscribe_gatt(
            &self,
            _characteristic: &BleCharacteristic,
        ) -> futures::future::BoxFuture<'_, Result<()>> {
            async { Ok(()) }.boxed()
        }

        fn notifications_gatt(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<BleNotificationStream>> {
            async { Ok(Box::pin(stream::pending()) as BleNotificationStream) }.boxed()
        }

        fn read_rssi_gatt(&self) -> futures::future::BoxFuture<'_, Result<i16>> {
            async { Ok(-42) }.boxed()
        }

        fn properties_gatt(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<Option<BlePeripheralProperties>>> {
            async { Ok(Some(BlePeripheralProperties { rssi: Some(-42) })) }.boxed()
        }
    }

    async fn read_response(values: Vec<BleNotification>) -> Result<Vec<u8>> {
        let (base, mut writers) = test_writers("response");
        let config = test_config(50);
        let mut notifications = stream::iter(values);
        let result = read_psftp_query_response(&mut notifications, &mut writers, &config).await;
        fs::remove_dir_all(base).ok();
        result
    }

    #[test]
    fn builds_single_psftp_query_packet_fixture() {
        assert_eq!(
            build_psftp_query_packets(3, &[]),
            vec![vec![0x02, 0x03, 0x80]]
        );
    }

    #[tokio::test]
    async fn psftp_gatt_wrapper_subscribes_writes_and_logs_success() {
        let (base, mut writers) = test_writers("wrapper_success");
        let config = test_config(50);
        let peripheral = FakePsftpPeripheral::with_psftp_mtu();
        let mut notifications =
            stream::iter(vec![notification(PSFTP_MTU_UUID, &[0x00, 0x00, 0x00])]);

        try_set_h10_local_time_psftp(&peripheral, &mut notifications, &mut writers, &config)
            .await
            .expect("PS-FTP wrapper success");
        writers.flush_all().expect("flush");

        let events = fs::read_to_string(writers.dir.join("events.csv")).expect("events");
        assert!(events.contains("\"subscribed\",\"psftp_mtu\""));
        assert!(events.contains("psftp_set_local_time_attempt"));
        assert!(events.contains("psftp_set_local_time_success"));
        assert!(!peripheral.writes().is_empty());
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn builds_multi_packet_psftp_query_fixture() {
        let params: Vec<u8> = (0..25).collect();
        let packets = build_psftp_query_packets(3, &params);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0][0], 0x06);
        assert_eq!(packets[0].len(), PSFTP_ATT_MTU_PAYLOAD);
        assert_eq!(packets[1][0], 0x13);
        assert_eq!(&packets[1][1..], &params[17..]);
    }

    #[test]
    fn builds_local_time_params_fixture() {
        let now = Local
            .with_ymd_and_hms(2026, 5, 17, 8, 9, 10)
            .single()
            .expect("valid local time")
            .with_nanosecond(123_000_000)
            .expect("valid nanos");
        assert_eq!(
            hex::encode(build_pftp_set_local_time_params(now, 60)),
            "0a0708ea0f10051811120808081009180a207b183c"
        );
    }

    #[test]
    fn protobuf_field_helpers_encode_expected_wire_bytes() {
        let mut out = Vec::new();
        pb_uint32_field(&mut out, 1, 300);
        pb_message_field(&mut out, 2, &[0xaa, 0xbb]);
        pb_int32_field(&mut out, 3, 60);
        assert_eq!(hex::encode(out), "08ac021202aabb183c");
    }

    #[test]
    fn protobuf_int32_negative_is_ten_byte_varint() {
        let mut out = Vec::new();
        pb_int32_field(&mut out, 3, -1);
        assert_eq!(out.len(), 11);
        assert_eq!(out[0], 0x18);
        assert_eq!(
            &out[1..],
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
        );
    }

    #[tokio::test]
    async fn psftp_response_status_zero_success() {
        let response = read_response(vec![notification(PSFTP_MTU_UUID, &[0x00, 0x00, 0x00])])
            .await
            .expect("status zero success");
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn psftp_response_status_one_payload_success() {
        let response = read_response(vec![notification(PSFTP_MTU_UUID, &[0x02, 0xaa, 0xbb])])
            .await
            .expect("status one success");
        assert_eq!(response, vec![0xaa, 0xbb]);
    }

    #[tokio::test]
    async fn psftp_response_multiframe_payload_success() {
        let response = read_response(vec![
            notification(PSFTP_MTU_UUID, &[0x06, 0xaa]),
            notification(PSFTP_MTU_UUID, &[0x13, 0xbb]),
        ])
        .await
        .expect("multi-frame success");
        assert_eq!(response, vec![0xaa, 0xbb]);
    }

    #[tokio::test]
    async fn psftp_response_ignores_unrelated_notifications() {
        let response = read_response(vec![
            notification("00002a37-0000-1000-8000-00805f9b34fb", &[0x16, 60]),
            notification(PSFTP_MTU_UUID, &[0x02, 0xcc]),
        ])
        .await
        .expect("ignored then success");
        assert_eq!(response, vec![0xcc]);
    }

    #[tokio::test]
    async fn psftp_response_rejects_empty_frame() {
        let err = read_response(vec![notification(PSFTP_MTU_UUID, &[])])
            .await
            .expect_err("empty frame rejected");
        assert!(format!("{err:#}").contains("empty PS-FTP response frame"));
    }

    #[tokio::test]
    async fn psftp_response_rejects_sequence_mismatch() {
        let err = read_response(vec![notification(PSFTP_MTU_UUID, &[0x12])])
            .await
            .expect_err("sequence mismatch rejected");
        assert!(format!("{err:#}").contains("sequence mismatch"));
    }

    #[tokio::test]
    async fn psftp_response_rejects_next_frame_mismatch() {
        let err = read_response(vec![
            notification(PSFTP_MTU_UUID, &[0x06, 0xaa]),
            notification(PSFTP_MTU_UUID, &[0x12, 0xbb]),
        ])
        .await
        .expect_err("next mismatch rejected");
        assert!(format!("{err:#}").contains("next-frame mismatch"));
    }

    #[tokio::test]
    async fn psftp_response_rejects_device_error_code() {
        let err = read_response(vec![notification(PSFTP_MTU_UUID, &[0x00, 0x01, 0x00])])
            .await
            .expect_err("error code rejected");
        assert!(format!("{err:#}").contains("response error code 1"));
    }

    #[tokio::test]
    async fn psftp_response_rejects_unexpected_status() {
        let err = read_response(vec![notification(PSFTP_MTU_UUID, &[0x04])])
            .await
            .expect_err("unexpected status rejected");
        assert!(format!("{err:#}").contains("unexpected PS-FTP frame status 2"));
    }

    #[tokio::test]
    async fn psftp_response_errors_when_stream_ends() {
        let err = read_response(Vec::new())
            .await
            .expect_err("ended stream rejected");
        assert!(format!("{err:#}").contains("notification stream ended"));
    }

    #[tokio::test]
    async fn psftp_response_times_out_on_pending_stream() {
        let (base, mut writers) = test_writers("timeout");
        let config = test_config(1);
        let mut notifications = stream::pending::<BleNotification>();
        let err = read_psftp_query_response(&mut notifications, &mut writers, &config)
            .await
            .expect_err("pending stream times out");
        assert!(format!("{err:#}").contains("timeout waiting for PS-FTP response"));
        fs::remove_dir_all(base).ok();
    }
}
