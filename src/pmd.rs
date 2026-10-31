use crate::config::{ACC_MEASUREMENT_TYPE, ECG_MEASUREMENT_TYPE};
use crate::records::PmdFrameTiming;
use crate::sink::RecordSink;
use anyhow::{Result, anyhow};
use std::collections::HashMap;

const TIMESTAMP_SMEAR_GAP_THRESHOLD_NS: f64 = 250_000_000.0;
const MAX_H10_LOCAL_EPOCH_NS: u64 = 150 * 366 * 24 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmdControlResponse {
    pub op: u8,
    pub measurement_type: u8,
    pub error_code: u8,
    pub more: bool,
    pub params: Vec<u8>,
}

#[derive(Debug)]
struct PmdDataFrame<'a> {
    measurement_type: u8,
    frame_ts: u64,
    frame_type: u8,
    compressed: bool,
    content: &'a [u8],
}

#[derive(Debug)]
struct ObservedPmdFrame {
    collector_ns: u128,
    measurement_type: u8,
    frame_type: u8,
    compressed: bool,
    device_frame_ts: u64,
    payload_len: usize,
    sample_count: usize,
}

#[derive(Debug)]
pub struct PmdParser {
    previous_ts: HashMap<(u8, u8), u64>,
    previous_collector_ns: HashMap<(u8, u8), u128>,
    sample_rates: HashMap<u8, u32>,
    factors: HashMap<u8, f32>,
}

impl PmdParser {
    pub fn new() -> Self {
        let mut sample_rates = HashMap::new();
        sample_rates.insert(ECG_MEASUREMENT_TYPE, 130); // ECG
        sample_rates.insert(ACC_MEASUREMENT_TYPE, 200); // ACC
        let mut factors = HashMap::new();
        factors.insert(ECG_MEASUREMENT_TYPE, 1.0);
        factors.insert(ACC_MEASUREMENT_TYPE, 1.0);
        Self {
            previous_ts: HashMap::new(),
            previous_collector_ns: HashMap::new(),
            sample_rates,
            factors,
        }
    }

    pub fn handle_control(
        &mut self,
        collector_ns: u128,
        data: &[u8],
        writers: &mut impl RecordSink,
    ) -> Result<()> {
        writers.event("pmd_control", &format!("hex={}", hex::encode(data)))?;
        if let Some(response) = parse_pmd_control_response(data) {
            writers.event(
                "pmd_control_response",
                &format!(
                    "collector_ns={collector_ns} op={} type={}({}) error={} more={} params={}",
                    response.op,
                    measurement_name(response.measurement_type),
                    response.measurement_type,
                    response.error_code,
                    response.more,
                    hex::encode(&response.params)
                ),
            )?;
            if response.error_code == 0
                && let Ok(settings) = parse_pmd_settings(&response.params)
            {
                for (setting_type, values) in &settings {
                    writers.event(
                        "pmd_settings",
                        &format!(
                            "type={} setting={} values={values:?}",
                            measurement_name(response.measurement_type),
                            setting_type
                        ),
                    )?;
                    if *setting_type == 0
                        && let Some(max) = values.iter().max()
                    {
                        self.sample_rates.insert(response.measurement_type, *max);
                    }
                    if *setting_type == 5
                        && let Some(raw) = values.first()
                    {
                        let factor = f32::from_bits(*raw);
                        self.factors.insert(response.measurement_type, factor);
                        writers.event(
                            "pmd_factor",
                            &format!(
                                "type={} factor={factor}",
                                measurement_name(response.measurement_type)
                            ),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn handle_pmd_data(
        &mut self,
        collector_ns: u128,
        data: &[u8],
        writers: &mut impl RecordSink,
    ) -> Result<()> {
        let frame = match parse_pmd_data_frame(data) {
            Ok(frame) => frame,
            Err(err) => {
                writers.event(
                    "pmd_data_short",
                    &format!("error={err:#} len={} hex={}", data.len(), hex::encode(data)),
                )?;
                return Ok(());
            }
        };

        // Preserve anomalous packets in raw storage, but never let an invalid
        // device clock enter derived samples, timing, or the parser's state.
        // Enforcing this here also protects local collection and raw replay.
        if frame.frame_ts > MAX_H10_LOCAL_EPOCH_NS {
            writers.event(
                "pmd_invalid_timestamp",
                &format!(
                    "type={} device_timestamp_ns={} exceeds supported 150-year local epoch; raw packet retained, derived samples omitted",
                    frame.measurement_type, frame.frame_ts
                ),
            )?;
            return Ok(());
        }

        match frame.measurement_type {
            ECG_MEASUREMENT_TYPE => self.parse_ecg(
                collector_ns,
                frame.frame_ts,
                frame.frame_type,
                frame.compressed,
                frame.content,
                writers,
            ),
            ACC_MEASUREMENT_TYPE => self.parse_acc(
                collector_ns,
                frame.frame_ts,
                frame.frame_type,
                frame.compressed,
                frame.content,
                writers,
            ),
            other => {
                writers.event(
                    "pmd_data_unhandled_type",
                    &format!(
                        "type={other} frame_type={} compressed={} len={}",
                        frame.frame_type,
                        frame.compressed,
                        frame.content.len()
                    ),
                )?;
                Ok(())
            }
        }
    }

    fn parse_ecg(
        &mut self,
        collector_ns: u128,
        frame_ts: u64,
        frame_type: u8,
        compressed: bool,
        content: &[u8],
        writers: &mut impl RecordSink,
    ) -> Result<()> {
        if compressed || frame_type != 0 {
            writers.event(
                "ecg_unsupported_frame",
                &format!(
                    "frame_type={frame_type} compressed={compressed} len={}",
                    content.len()
                ),
            )?;
            return Ok(());
        }
        let sample_count = content.len() / 3;
        if sample_count == 0 {
            return Ok(());
        }
        self.log_frame_timing(
            ObservedPmdFrame {
                collector_ns,
                measurement_type: ECG_MEASUREMENT_TYPE,
                frame_type,
                compressed,
                device_frame_ts: frame_ts,
                payload_len: content.len(),
                sample_count,
            },
            writers,
        )?;
        let timestamps =
            self.sample_timestamps(ECG_MEASUREMENT_TYPE, frame_type, frame_ts, sample_count);
        for (i, chunk) in content.chunks_exact(3).enumerate() {
            let microvolts = sign_extend_le(chunk);
            writers.ecg(collector_ns, timestamps[i], i, microvolts)?;
        }
        self.previous_ts
            .insert((ECG_MEASUREMENT_TYPE, frame_type), frame_ts);
        Ok(())
    }

    fn parse_acc(
        &mut self,
        collector_ns: u128,
        frame_ts: u64,
        frame_type: u8,
        compressed: bool,
        content: &[u8],
        writers: &mut impl RecordSink,
    ) -> Result<()> {
        let samples: Vec<[i32; 3]> = if compressed {
            match frame_type {
                0 | 1 => parse_delta_frames_to_samples(content, 3, 16)
                    .map_err(|err| anyhow!("ACC delta parse failed: {err:#}"))?,
                _ => {
                    writers.event(
                        "acc_unsupported_compressed_frame",
                        &format!("frame_type={frame_type} len={}", content.len()),
                    )?;
                    return Ok(());
                }
            }
        } else {
            match frame_type {
                0 => parse_raw_acc(content, 1),
                1 => parse_raw_acc(content, 2),
                2 => parse_raw_acc(content, 3),
                _ => {
                    writers.event(
                        "acc_unsupported_raw_frame",
                        &format!("frame_type={frame_type} len={}", content.len()),
                    )?;
                    return Ok(());
                }
            }
        };

        if samples.is_empty() {
            return Ok(());
        }
        let sample_count = samples.len();
        self.log_frame_timing(
            ObservedPmdFrame {
                collector_ns,
                measurement_type: ACC_MEASUREMENT_TYPE,
                frame_type,
                compressed,
                device_frame_ts: frame_ts,
                payload_len: content.len(),
                sample_count,
            },
            writers,
        )?;
        let factor = *self.factors.get(&ACC_MEASUREMENT_TYPE).unwrap_or(&1.0);
        let timestamps =
            self.sample_timestamps(ACC_MEASUREMENT_TYPE, frame_type, frame_ts, sample_count);
        for (i, sample) in samples.iter().enumerate() {
            let (mut x, mut y, mut z) = (sample[0], sample[1], sample[2]);
            if compressed && frame_type == 0 {
                // SDK special case: type 0 compressed ACC is in G; convert to mG.
                x = ((x as f32) * factor * 1000.0) as i32;
                y = ((y as f32) * factor * 1000.0) as i32;
                z = ((z as f32) * factor * 1000.0) as i32;
            } else if (factor - 1.0).abs() > f32::EPSILON {
                x = ((x as f32) * factor) as i32;
                y = ((y as f32) * factor) as i32;
                z = ((z as f32) * factor) as i32;
            }
            writers.acc(collector_ns, timestamps[i], i, x, y, z)?;
        }
        self.previous_ts
            .insert((ACC_MEASUREMENT_TYPE, frame_type), frame_ts);
        Ok(())
    }

    fn log_frame_timing(
        &mut self,
        frame: ObservedPmdFrame,
        writers: &mut impl RecordSink,
    ) -> Result<()> {
        let key = (frame.measurement_type, frame.frame_type);
        let prev_device_ts = self.previous_ts.get(&key).copied();
        let prev_collector_ns = self.previous_collector_ns.get(&key).copied();
        let collector_delta_ms = prev_collector_ns
            .map(|prev| frame.collector_ns.saturating_sub(prev) as f64 / 1_000_000.0);
        let device_delta_ms = prev_device_ts.and_then(|prev| {
            if prev > 0 && frame.device_frame_ts >= prev {
                Some((frame.device_frame_ts - prev) as f64 / 1_000_000.0)
            } else {
                None
            }
        });
        let sample_rate = *self.sample_rates.get(&frame.measurement_type).unwrap_or(&1) as f64;
        let expected_frame_delta_ms = frame.sample_count as f64 * 1000.0 / sample_rate;
        let gap_ms = device_delta_ms.map(|actual| actual - expected_frame_delta_ms);
        writers.pmd_timing(&PmdFrameTiming {
            collector_ns: frame.collector_ns,
            measurement_type: frame.measurement_type,
            frame_type: frame.frame_type,
            compressed: frame.compressed,
            device_frame_ts: frame.device_frame_ts,
            payload_len: frame.payload_len,
            sample_count: frame.sample_count,
            collector_delta_ms,
            device_delta_ms,
            expected_frame_delta_ms,
            gap_ms,
        })?;
        self.previous_collector_ns.insert(key, frame.collector_ns);
        Ok(())
    }

    fn sample_timestamps(
        &self,
        measurement_type: u8,
        frame_type: u8,
        frame_ts: u64,
        sample_count: usize,
    ) -> Vec<u64> {
        let prev = *self
            .previous_ts
            .get(&(measurement_type, frame_type))
            .unwrap_or(&0);
        let sample_rate = *self.sample_rates.get(&measurement_type).unwrap_or(&1) as f64;
        let nominal_delta = 1_000_000_000.0 / sample_rate;
        let expected_frame_delta = nominal_delta * sample_count as f64;
        let continuous_delta = if prev > 0 && prev < frame_ts {
            let actual_delta = (frame_ts - prev) as f64;
            if actual_delta <= expected_frame_delta + TIMESTAMP_SMEAR_GAP_THRESHOLD_NS {
                Some(actual_delta / sample_count as f64)
            } else {
                None
            }
        } else {
            None
        };
        let delta = continuous_delta.unwrap_or(nominal_delta);
        let start = if let Some(delta) = continuous_delta {
            prev as f64 + delta
        } else {
            (frame_ts as f64 - delta * (sample_count.saturating_sub(1)) as f64).max(0.0)
        };
        let mut timestamps = Vec::with_capacity(sample_count);
        for i in 0..sample_count {
            if i + 1 == sample_count {
                timestamps.push(frame_ts);
            } else {
                timestamps.push((start + delta * i as f64).round() as u64);
            }
        }
        timestamps
    }
}

impl Default for PmdParser {
    fn default() -> Self {
        Self::new()
    }
}

pub fn pmd_measurement_type(data: &[u8]) -> Option<u8> {
    data.first().map(|value| value & 0x3f)
}

pub fn parse_pmd_control_response(data: &[u8]) -> Option<PmdControlResponse> {
    if data.len() < 5 || data[0] != 0xf0 {
        return None;
    }
    Some(PmdControlResponse {
        op: data[1],
        measurement_type: data[2],
        error_code: data[3],
        more: data[4] != 0,
        params: data[5..].to_vec(),
    })
}

fn parse_pmd_data_frame(data: &[u8]) -> Result<PmdDataFrame<'_>> {
    if data.len() < 10 {
        return Err(anyhow!("PMD data frame too short: {}", data.len()));
    }
    let mut ts_bytes = [0u8; 8];
    ts_bytes.copy_from_slice(&data[1..9]);
    let frame_type_byte = data[9];
    Ok(PmdDataFrame {
        measurement_type: data[0] & 0x3f,
        frame_ts: u64::from_le_bytes(ts_bytes),
        frame_type: frame_type_byte & 0x7f,
        compressed: frame_type_byte & 0x80 != 0,
        content: &data[10..],
    })
}

pub fn parse_pmd_settings(data: &[u8]) -> Result<Vec<(u8, Vec<u32>)>> {
    let mut offset = 0usize;
    let mut out = Vec::new();
    while offset + 2 <= data.len() {
        let setting_type = data[offset];
        offset += 1;
        let count = data[offset] as usize;
        offset += 1;
        let size = match setting_type {
            0 => 2, // sample rate
            1 => 2, // resolution
            2 => 2, // range
            3 => 4, // range milli-unit / legacy not-used
            4 => 1, // channels
            5 => 4, // conversion factor f32 bits
            6 => 1, // security in newer SDKs
            _ => break,
        };
        if offset + count * size > data.len() {
            return Err(anyhow!("truncated PMD settings"));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let mut value = 0u32;
            for i in 0..size {
                value |= (data[offset + i] as u32) << (8 * i);
            }
            offset += size;
            values.push(value);
        }
        out.push((setting_type, values));
    }
    Ok(out)
}

fn parse_raw_acc(content: &[u8], field_size: usize) -> Vec<[i32; 3]> {
    let sample_size = field_size * 3;
    let mut samples = Vec::with_capacity(content.len() / sample_size);
    let mut offset = 0usize;
    while offset + sample_size <= content.len() {
        let x = sign_extend_le(&content[offset..offset + field_size]);
        offset += field_size;
        let y = sign_extend_le(&content[offset..offset + field_size]);
        offset += field_size;
        let z = sign_extend_le(&content[offset..offset + field_size]);
        offset += field_size;
        samples.push([x, y, z]);
    }
    samples
}

fn parse_delta_frames_to_samples(
    data: &[u8],
    channels: usize,
    resolution_bits: usize,
) -> Result<Vec<[i32; 3]>> {
    if channels != 3 {
        return Err(anyhow!("only 3-channel delta frames are implemented"));
    }
    if resolution_bits == 0 {
        return Err(anyhow!("delta frame resolution must be non-zero"));
    }
    let resolution_bytes = resolution_bits.div_ceil(8);
    let ref_len = channels * resolution_bytes;
    if data.len() < ref_len {
        return Err(anyhow!("delta frame too short for reference samples"));
    }

    let mut current = [0i32; 3];
    for (ch, slot) in current.iter_mut().enumerate() {
        let start = ch * resolution_bytes;
        *slot = sign_extend_le(&data[start..start + resolution_bytes]);
    }
    let mut samples = vec![current];
    let mut offset = ref_len;

    while offset < data.len() {
        if offset + 2 > data.len() {
            break;
        }
        let bit_width = data[offset] as usize;
        offset += 1;
        let sample_count = data[offset] as usize;
        offset += 1;
        if bit_width == 0 || sample_count == 0 {
            continue;
        }
        let bit_len = bit_width * sample_count * channels;
        let byte_len = bit_len.div_ceil(8);
        if offset + byte_len > data.len() {
            return Err(anyhow!(
                "delta payload truncated: offset={offset} byte_len={byte_len} data_len={}",
                data.len()
            ));
        }
        let payload = &data[offset..offset + byte_len];
        offset += byte_len;

        for sample_idx in 0..sample_count {
            let mut next = current;
            for (ch, next_value) in next.iter_mut().enumerate().take(channels) {
                let bit_offset = sample_idx * channels * bit_width + ch * bit_width;
                let delta = read_signed_bits_lsb_first(payload, bit_offset, bit_width);
                *next_value = current[ch].wrapping_add(delta);
            }
            current = next;
            samples.push(current);
        }
    }
    Ok(samples)
}

fn read_signed_bits_lsb_first(payload: &[u8], bit_offset: usize, bit_width: usize) -> i32 {
    let mut value = 0i32;
    for i in 0..bit_width {
        let absolute_bit = bit_offset + i;
        let byte = payload[absolute_bit / 8];
        let bit = (byte >> (absolute_bit % 8)) & 0x01;
        value |= (bit as i32) << i;
    }
    if bit_width < 32 && bit_width > 0 && (value & (1 << (bit_width - 1))) != 0 {
        value |= !0i32 << bit_width;
    }
    value
}

fn sign_extend_le(bytes: &[u8]) -> i32 {
    let mut value = 0i32;
    for (i, byte) in bytes.iter().enumerate() {
        value |= (*byte as i32) << (8 * i);
    }
    let bits = bytes.len() * 8;
    if bits > 0 && bits < 32 && (value & (1 << (bits - 1))) != 0 {
        value |= !0i32 << bits;
    }
    value
}

pub fn measurement_name(measurement_type: u8) -> &'static str {
    match measurement_type & 0x3f {
        0 => "ECG",
        1 => "PPG",
        2 => "ACC",
        3 => "PPI",
        5 => "GYRO",
        6 => "MAGNETOMETER",
        9 => "SDK_MODE",
        10 => "LOCATION",
        11 => "PRESSURE",
        12 => "TEMPERATURE",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv::CsvWriters;
    use crate::util::unix_ns;
    use std::fs;
    use std::path::{Path, PathBuf};

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_pmd_{name}_{}_{}",
            std::process::id(),
            unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn writers(name: &str) -> (PathBuf, CsvWriters) {
        let base = temp_dir(name);
        let writers = CsvWriters::new(&base).expect("writers");
        (base, writers)
    }

    fn read(path: &Path) -> String {
        fs::read_to_string(path).expect("read file")
    }

    fn frame(
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

    #[test]
    fn invalid_device_timestamps_leave_samples_timing_and_parser_state_unchanged() {
        let (base, mut writers) = writers("invalid_timestamps");
        let mut parser = PmdParser::new();
        for kind in [ECG_MEASUREMENT_TYPE, ACC_MEASUREMENT_TYPE] {
            let valid = frame(kind, 1_000_000_000, 0, false, &[1, 2, 3]);
            parser.handle_pmd_data(100, &valid, &mut writers).unwrap();
            let previous_ts = parser.previous_ts.clone();
            let previous_collector_ns = parser.previous_collector_ns.clone();
            for timestamp in [MAX_H10_LOCAL_EPOCH_NS + 1, u64::MAX] {
                let invalid = frame(kind, timestamp, 0, false, &[1, 2, 3]);
                parser.handle_pmd_data(200, &invalid, &mut writers).unwrap();
                assert_eq!(parser.previous_ts, previous_ts);
                assert_eq!(parser.previous_collector_ns, previous_collector_ns);
            }
            let next = frame(kind, 1_010_000_000, 0, false, &[1, 2, 3]);
            parser.handle_pmd_data(300, &next, &mut writers).unwrap();
            assert_eq!(parser.previous_ts[&(kind, 0)], 1_010_000_000);
            assert_eq!(parser.previous_collector_ns[&(kind, 0)], 300);
        }
        writers.flush_all().unwrap();
        assert_eq!(read(&writers.dir.join("ecg.csv")).lines().count(), 3);
        assert_eq!(read(&writers.dir.join("acc.csv")).lines().count(), 3);
        assert_eq!(read(&writers.dir.join("pmd_timing.csv")).lines().count(), 5);
        assert_eq!(
            read(&writers.dir.join("events.csv"))
                .matches("pmd_invalid_timestamp")
                .count(),
            4
        );

        // The existing inclusive bound remains supported.
        parser
            .handle_pmd_data(
                400,
                &frame(0, MAX_H10_LOCAL_EPOCH_NS, 0, false, &[1, 2, 3]),
                &mut writers,
            )
            .unwrap();
        assert_eq!(parser.previous_ts[&(0, 0)], MAX_H10_LOCAL_EPOCH_NS);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn invalid_timestamp_event_write_failure_is_propagated() {
        struct FailingSink;
        impl RecordSink for FailingSink {
            fn session_dir(&self) -> &Path {
                unreachable!()
            }
            fn event(&mut self, event: &str, _: &str) -> Result<()> {
                assert_eq!(event, "pmd_invalid_timestamp");
                Err(anyhow!("event storage unavailable"))
            }
            fn raw_gatt(
                &mut self,
                _: u128,
                _: &str,
                _: &str,
                _: &str,
                _: &[u8],
                _: &str,
            ) -> Result<()> {
                unreachable!()
            }
            fn pmd_timing(&mut self, _: &PmdFrameTiming) -> Result<()> {
                unreachable!()
            }
            fn ecg(&mut self, _: u128, _: u64, _: usize, _: i32) -> Result<()> {
                unreachable!()
            }
            fn acc(&mut self, _: u128, _: u64, _: usize, _: i32, _: i32, _: i32) -> Result<()> {
                unreachable!()
            }
            fn hr(&mut self, _: u128, _: &crate::hr::HrData, _: &[u8]) -> Result<()> {
                unreachable!()
            }
            fn battery(&mut self, _: u128, _: u8, _: &str) -> Result<()> {
                unreachable!()
            }
            fn health(&mut self, _: u128, _: Option<i16>, _: u64, _: u64) -> Result<()> {
                unreachable!()
            }
        }
        let mut parser = PmdParser::new();
        let error = parser
            .handle_pmd_data(
                100,
                &frame(0, u64::MAX, 0, false, &[1, 2, 3]),
                &mut FailingSink,
            )
            .unwrap_err();
        assert!(error.to_string().contains("event storage unavailable"));
        assert!(parser.previous_ts.is_empty());
        assert!(parser.previous_collector_ns.is_empty());
    }

    #[test]
    fn sign_extends_24_bit_fixture() {
        assert_eq!(sign_extend_le(&[0xff, 0xff, 0x7f]), 8_388_607);
        assert_eq!(sign_extend_le(&[0x00, 0x00, 0x80]), -8_388_608);
        assert_eq!(sign_extend_le(&[0xff, 0xff, 0xff]), -1);
    }

    #[test]
    fn parses_pmd_control_response_fixture() {
        let response = parse_pmd_control_response(&[0xf0, 0x02, 0x00, 0x00, 0x00])
            .expect("control response parses");
        assert_eq!(response.op, 2);
        assert_eq!(response.measurement_type, ECG_MEASUREMENT_TYPE);
        assert_eq!(response.error_code, 0);
        assert!(!response.more);
    }

    #[test]
    fn parses_pmd_settings_fixture() {
        let settings = parse_pmd_settings(&[0x00, 0x01, 0x82, 0x00, 0x01, 0x01, 0x0e, 0x00])
            .expect("settings parse");
        assert_eq!(settings, vec![(0, vec![130]), (1, vec![14])]);
    }

    #[test]
    fn parses_live_ecg_frame_header_and_first_sample_fixture() {
        let frame = hex::decode("005e2d107f63ff8c0b003401002b01003b01004201004001004e01005a0100630100740100870100a30100bd0100cb0100de0100f30100fa0100f10100d90100b601009001006601002b0100e60000a700006c00003300000b0000f0ffffd8ffffc8ffffc3ffffc6ffffcaffffd1ffffd8ffffe4fffff5ffff0400001500002000002000002800003300003600003100003300003300002c00002500001c00001900002800003f0000650000960000ae0000b00000a000007100004600003a0000250000090000040000070000f9fffff7ffff0000000700000b0000020000e4ffffcfffff")
            .expect("hex fixture");
        let parsed = parse_pmd_data_frame(&frame).expect("PMD frame parses");
        assert_eq!(parsed.measurement_type, ECG_MEASUREMENT_TYPE);
        assert_eq!(parsed.frame_type, 0);
        assert!(!parsed.compressed);
        assert_eq!(parsed.content.len() / 3, 73);
        assert_eq!(sign_extend_le(&parsed.content[0..3]), 308);
        assert_eq!(sign_extend_le(&parsed.content[216..219]), -49);
    }

    #[test]
    fn parses_live_raw_acc_fixture() {
        let frame = hex::decode("02d4ff7e7763ff8c0b0141fc5b00f30037fc6000ec0037fc5f00f10035fc6400f20035fc6500f60031fc6700f7002ffc6b00f9002dfc6e00f6002cfc7200f3002dfc7500f2002ffc7500f4002dfc7700f1002dfc7400f1002dfc7300f1002cfc7200ec002dfc7200ed002cfc7000f0002ffc6f00f3002efc6500f3002ffc6800f5002cfc6500f4002efc6200f7002efc6000f6002dfc5d00f4002bfc5d00f6002cfc6000f3002cfc6000f3002dfc6000f30030fc6000f30031fc5f00f50031fc6000f40031fc6000f30032fc6400f50031fc6300f90033fc6200fa0031fc6100fa00")
            .expect("hex fixture");
        let parsed = parse_pmd_data_frame(&frame).expect("PMD frame parses");
        assert_eq!(parsed.measurement_type, ACC_MEASUREMENT_TYPE);
        assert_eq!(parsed.frame_type, 1);
        assert!(!parsed.compressed);
        let samples = parse_raw_acc(parsed.content, 2);
        assert_eq!(samples.len(), 36);
        assert_eq!(samples[0], [-959, 91, 243]);
        assert_eq!(samples[35], [-975, 97, 250]);
    }

    #[test]
    fn parses_synthetic_acc_delta_fixture() {
        // Reference sample [100, -50, 25], then two 4-bit signed delta samples:
        // [+1, -1, 0] and [-2, +2, +3].
        let data = [100, 0, 206, 255, 25, 0, 4, 2, 0xf1, 0xe0, 0x32];
        let samples = parse_delta_frames_to_samples(&data, 3, 16).expect("delta parses");
        assert_eq!(samples, vec![[100, -50, 25], [101, -51, 25], [99, -49, 28]]);
    }

    #[test]
    fn timestamps_do_not_smear_large_cross_reconnect_gaps() {
        let mut parser = PmdParser::new();
        parser
            .previous_ts
            .insert((ECG_MEASUREMENT_TYPE, 0), 1_000_000_000);
        let timestamps = parser.sample_timestamps(ECG_MEASUREMENT_TYPE, 0, 12_000_000_000, 73);
        assert_eq!(timestamps.len(), 73);
        assert_eq!(
            *timestamps.last().expect("timestamp exists"),
            12_000_000_000
        );
        assert!(timestamps[0] > 11_400_000_000);
        assert!(timestamps[0] < 12_000_000_000);
    }

    #[test]
    fn timestamps_use_previous_frame_for_continuous_data() {
        let mut parser = PmdParser::new();
        parser
            .previous_ts
            .insert((ECG_MEASUREMENT_TYPE, 0), 1_000_000_000);
        let timestamps = parser.sample_timestamps(ECG_MEASUREMENT_TYPE, 0, 1_562_000_000, 73);
        assert_eq!(timestamps[0], 1_007_698_630);
        assert_eq!(*timestamps.last().expect("timestamp exists"), 1_562_000_000);
    }

    #[test]
    fn handle_pmd_data_writes_ecg_acc_and_timing_rows() {
        let (base, mut writers) = writers("handle_data");
        let mut parser = PmdParser::new();
        let ecg_content = [0x01, 0x00, 0x00, 0xff, 0xff, 0xff];
        parser
            .handle_pmd_data(
                100,
                &frame(ECG_MEASUREMENT_TYPE, 1_000_000_000, 0, false, &ecg_content),
                &mut writers,
            )
            .expect("ecg frame");
        parser
            .handle_pmd_data(
                200,
                &frame(ECG_MEASUREMENT_TYPE, 1_015_384_615, 0, false, &ecg_content),
                &mut writers,
            )
            .expect("second ecg frame");
        parser
            .handle_pmd_data(
                300,
                &frame(ACC_MEASUREMENT_TYPE, 2_000_000_000, 0, false, &[1, 2, 0xff]),
                &mut writers,
            )
            .expect("acc frame type 0");
        parser
            .handle_pmd_data(
                400,
                &frame(
                    ACC_MEASUREMENT_TYPE,
                    2_010_000_000,
                    2,
                    false,
                    &[1, 0, 0, 0xff, 0xff, 0xff, 0x00, 0x00, 0x80],
                ),
                &mut writers,
            )
            .expect("acc frame type 2");
        writers.flush_all().expect("flush");

        assert!(read(&writers.dir.join("ecg.csv")).contains("100,1000000000,1,-1"));
        assert!(read(&writers.dir.join("acc.csv")).contains("300,2000000000,0,1,2,-1"));
        assert!(read(&writers.dir.join("acc.csv")).contains("400,2010000000,0,1,-1,-8388608"));
        let timing = read(&writers.dir.join("pmd_timing.csv"));
        assert!(timing.contains("100,\"ECG\",0,0,false,1000000000,6,2"));
        assert!(timing.contains("200,\"ECG\",0,0,false,1015384615,6,2,0.000,15.385,15.385,"));
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn handle_control_updates_settings_and_compressed_acc_scaling() {
        let (base, mut writers) = writers("control_factor");
        let mut parser = PmdParser::new();
        let mut params = vec![0x00, 0x01, 100, 0, 0x05, 0x01];
        params.extend_from_slice(&0.001f32.to_bits().to_le_bytes());
        let mut control = vec![0xf0, 0x01, ACC_MEASUREMENT_TYPE, 0x00, 0x00];
        control.extend_from_slice(&params);
        parser
            .handle_control(1, &control, &mut writers)
            .expect("control");
        parser
            .handle_pmd_data(
                2,
                &frame(
                    ACC_MEASUREMENT_TYPE,
                    3_000_000_000,
                    0,
                    true,
                    &[1, 0, 2, 0, 3, 0],
                ),
                &mut writers,
            )
            .expect("compressed acc type 0");
        writers.flush_all().expect("flush");
        assert!(
            read(&writers.dir.join("events.csv"))
                .contains("\"pmd_factor\",\"type=ACC factor=0.001\"")
        );
        assert!(read(&writers.dir.join("acc.csv")).contains("2,3000000000,0,1,2,3"));
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn handle_pmd_data_covers_compressed_type_one_and_error_events() {
        let (base, mut writers) = writers("errors");
        let mut parser = PmdParser::new();
        parser
            .handle_pmd_data(1, &[0x00], &mut writers)
            .expect("short frame logged");
        parser
            .handle_pmd_data(2, &frame(9, 1, 0, false, &[1, 2, 3]), &mut writers)
            .expect("unknown type logged");
        parser
            .handle_pmd_data(
                3,
                &frame(ECG_MEASUREMENT_TYPE, 2, 1, false, &[1, 2, 3]),
                &mut writers,
            )
            .expect("unsupported ecg logged");
        parser
            .handle_pmd_data(
                4,
                &frame(ECG_MEASUREMENT_TYPE, 3, 0, true, &[1, 2, 3]),
                &mut writers,
            )
            .expect("compressed ecg logged");
        parser
            .handle_pmd_data(
                5,
                &frame(ACC_MEASUREMENT_TYPE, 4, 3, false, &[1, 2, 3]),
                &mut writers,
            )
            .expect("unsupported raw acc logged");
        parser
            .handle_pmd_data(
                6,
                &frame(ACC_MEASUREMENT_TYPE, 5, 2, true, &[1, 2, 3]),
                &mut writers,
            )
            .expect("unsupported compressed acc logged");
        parser
            .handle_pmd_data(
                7,
                &frame(ACC_MEASUREMENT_TYPE, 6, 1, true, &[10, 0, 20, 0, 30, 0]),
                &mut writers,
            )
            .expect("compressed acc type 1");
        parser
            .handle_pmd_data(
                8,
                &frame(ECG_MEASUREMENT_TYPE, 7, 0, false, &[]),
                &mut writers,
            )
            .expect("empty ecg ignored");
        parser
            .handle_pmd_data(
                9,
                &frame(ACC_MEASUREMENT_TYPE, 8, 1, false, &[]),
                &mut writers,
            )
            .expect("empty acc ignored");
        writers.flush_all().expect("flush");
        let events = read(&writers.dir.join("events.csv"));
        assert!(events.contains("pmd_data_short"));
        assert!(events.contains("pmd_data_unhandled_type"));
        assert!(events.contains("ecg_unsupported_frame"));
        assert!(events.contains("acc_unsupported_raw_frame"));
        assert!(events.contains("acc_unsupported_compressed_frame"));
        assert!(read(&writers.dir.join("acc.csv")).contains("7,6,0,10,20,30"));
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn parser_helpers_cover_edge_cases() {
        assert_eq!(pmd_measurement_type(&[]), None);
        assert_eq!(pmd_measurement_type(&[0x82]), Some(2));
        assert!(parse_pmd_control_response(&[0xf0, 1, 2, 3]).is_none());
        assert!(parse_pmd_control_response(&[0x00, 1, 2, 3, 4]).is_none());
        assert_eq!(
            parse_pmd_settings(&[0x04, 0x02, 1, 2]).expect("channels"),
            vec![(4, vec![1, 2])]
        );
        assert_eq!(
            parse_pmd_settings(&[0x03, 0x01, 1, 2, 3, 4]).expect("u32"),
            vec![(3, vec![0x04030201])]
        );
        assert_eq!(
            parse_pmd_settings(&[0x06, 0x01, 7]).expect("security"),
            vec![(6, vec![7])]
        );
        assert_eq!(
            parse_pmd_settings(&[0xff, 0x01]).expect("unknown stops"),
            Vec::<(u8, Vec<u32>)>::new()
        );
        assert!(parse_pmd_settings(&[0x00, 0x02, 1, 0]).is_err());
        assert_eq!(parse_raw_acc(&[1, 2], 1), Vec::<[i32; 3]>::new());
        assert_eq!(sign_extend_le(&[]), 0);
        assert_eq!(sign_extend_le(&[0xff, 0xff, 0xff, 0x7f]), i32::MAX);
        assert_eq!(
            read_signed_bits_lsb_first(&[0xff, 0xff, 0xff, 0xff], 0, 32),
            -1
        );
    }

    #[test]
    fn delta_parser_error_and_skip_branches() {
        assert!(parse_delta_frames_to_samples(&[0; 6], 2, 16).is_err());
        assert!(parse_delta_frames_to_samples(&[0; 6], 3, 0).is_err());
        assert!(parse_delta_frames_to_samples(&[0; 5], 3, 16).is_err());
        assert!(parse_delta_frames_to_samples(&[0, 0, 0, 0, 0, 0, 8, 2, 0xff], 3, 16).is_err());
        assert_eq!(
            parse_delta_frames_to_samples(&[1, 0, 2, 0, 3, 0, 0, 9, 4, 0], 3, 16)
                .expect("zero width skipped"),
            vec![[1, 2, 3]]
        );
        assert_eq!(
            parse_delta_frames_to_samples(&[1, 0, 2, 0, 3, 0, 4, 0, 4, 1, 0x21, 0x00], 3, 16)
                .expect("zero count skipped"),
            vec![[1, 2, 3], [2, 4, 3]]
        );
    }

    #[test]
    fn timestamp_edge_cases_and_timing_without_device_delta() {
        let (base, mut writers) = writers("timing_edges");
        let mut parser = PmdParser::new();
        let first = parser.sample_timestamps(ECG_MEASUREMENT_TYPE, 0, 1_000_000_000, 3);
        assert_eq!(first.len(), 3);
        assert_eq!(first[2], 1_000_000_000);
        parser
            .previous_ts
            .insert((ECG_MEASUREMENT_TYPE, 0), 2_000_000_000);
        let backwards = parser.sample_timestamps(ECG_MEASUREMENT_TYPE, 0, 1_000_000_000, 0);
        assert!(backwards.is_empty());
        parser
            .log_frame_timing(
                ObservedPmdFrame {
                    collector_ns: 10,
                    measurement_type: ECG_MEASUREMENT_TYPE,
                    frame_type: 0,
                    compressed: false,
                    device_frame_ts: 1_000_000_000,
                    payload_len: 0,
                    sample_count: 1,
                },
                &mut writers,
            )
            .expect("timing");
        writers.flush_all().expect("flush");
        assert!(
            read(&writers.dir.join("pmd_timing.csv"))
                .contains("10,\"ECG\",0,0,false,1000000000,0,1,,")
        );
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn measurement_name_covers_known_and_unknown_types() {
        assert_eq!(measurement_name(0), "ECG");
        assert_eq!(measurement_name(1), "PPG");
        assert_eq!(measurement_name(2), "ACC");
        assert_eq!(measurement_name(3), "PPI");
        assert_eq!(measurement_name(5), "GYRO");
        assert_eq!(measurement_name(6), "MAGNETOMETER");
        assert_eq!(measurement_name(9), "SDK_MODE");
        assert_eq!(measurement_name(10), "LOCATION");
        assert_eq!(measurement_name(11), "PRESSURE");
        assert_eq!(measurement_name(12), "TEMPERATURE");
        assert_eq!(measurement_name(63), "UNKNOWN");
        assert_eq!(measurement_name(0x82), "ACC");
    }
}
