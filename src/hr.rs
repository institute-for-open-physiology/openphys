use crate::sink::RecordSink;
use anyhow::{Result, anyhow};

#[derive(Debug, PartialEq)]
pub struct HrData {
    pub hr_bpm: u16,
    pub sensor_contact_supported: bool,
    pub sensor_contact_detected: bool,
    pub energy_expended: Option<u16>,
    pub rr_ms: Vec<f64>,
}

pub fn handle_hr(collector_ns: u128, data: &[u8], writers: &mut impl RecordSink) -> Result<()> {
    let parsed = parse_hr(data)?;
    writers.hr(collector_ns, &parsed, data)?;
    Ok(())
}

pub fn parse_hr(data: &[u8]) -> Result<HrData> {
    if data.len() < 2 {
        return Err(anyhow!("HR notification too short: {}", data.len()));
    }
    let flags = data[0];
    let mut offset = 1;
    let hr_bpm = if flags & 0x01 != 0 {
        if data.len() < offset + 2 {
            return Err(anyhow!("HR u16 missing"));
        }
        let value = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;
        value
    } else {
        let value = data[offset] as u16;
        offset += 1;
        value
    };
    let sensor_contact_detected = flags & 0x02 != 0;
    let sensor_contact_supported = flags & 0x04 != 0;
    let energy_expended = if flags & 0x08 != 0 {
        if data.len() < offset + 2 {
            return Err(anyhow!("energy expended missing"));
        }
        let value = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;
        Some(value)
    } else {
        None
    };
    let mut rr_ms = Vec::new();
    if flags & 0x10 != 0 {
        while data.len() >= offset + 2 {
            let rr_1024 = u16::from_le_bytes([data[offset], data[offset + 1]]);
            offset += 2;
            rr_ms.push(rr_1024 as f64 * 1000.0 / 1024.0);
        }
    }
    Ok(HrData {
        hr_bpm,
        sensor_contact_supported,
        sensor_contact_detected,
        energy_expended,
        rr_ms,
    })
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
            "training_sensors_hr_{name}_{}_{}",
            std::process::id(),
            unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn read(path: &Path) -> String {
        fs::read_to_string(path).expect("read file")
    }

    #[test]
    fn parses_hr_rr_fixture() {
        let hr = parse_hr(&[0x16, 60, 0x00, 0x04]).expect("HR fixture parses");
        assert_eq!(hr.hr_bpm, 60);
        assert!(hr.sensor_contact_supported);
        assert!(hr.sensor_contact_detected);
        assert_eq!(hr.energy_expended, None);
        assert_eq!(hr.rr_ms, vec![1000.0]);
    }

    #[test]
    fn parses_u16_hr_energy_and_multiple_rrs() {
        let hr = parse_hr(&[0x1f, 0x2c, 0x01, 0x34, 0x12, 0x00, 0x04, 0x00, 0x03])
            .expect("HR fixture parses");
        assert_eq!(hr.hr_bpm, 300);
        assert!(hr.sensor_contact_supported);
        assert!(hr.sensor_contact_detected);
        assert_eq!(hr.energy_expended, Some(0x1234));
        assert_eq!(hr.rr_ms, vec![1000.0, 750.0]);
    }

    #[test]
    fn parses_hr_without_optional_fields_and_ignores_trailing_rr_byte() {
        let hr = parse_hr(&[0x10, 72, 0x00]).expect("HR fixture parses");
        assert_eq!(hr.hr_bpm, 72);
        assert!(!hr.sensor_contact_supported);
        assert!(!hr.sensor_contact_detected);
        assert_eq!(hr.energy_expended, None);
        assert!(hr.rr_ms.is_empty());
    }

    #[test]
    fn handle_hr_writes_csv_row() {
        let base = temp_dir("handle");
        let mut writers = CsvWriters::new(&base).expect("writers");
        handle_hr(123, &[0x16, 60, 0x00, 0x04], &mut writers).expect("handle hr");
        writers.flush_all().expect("flush");
        let text = read(&writers.dir.join("hr.csv"));
        assert!(text.contains("123,60,true,true,,\"1000.000\",163c0004"));
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn rejects_truncated_hr() {
        assert!(parse_hr(&[]).is_err());
        assert!(parse_hr(&[0x01, 0x34]).is_err());
        assert!(parse_hr(&[0x08, 60, 0x00]).is_err());
    }
}
