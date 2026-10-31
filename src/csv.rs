//! Test-only legacy CSV `RecordSink` implementation.
//!
//! Production WAL-era collection does not compile or instantiate this module; `src/main.rs`
//! includes it only under `#[cfg(test)]` so parser and BLE state-machine tests can keep using
//! simple file assertions while runtime collection writes raw WAL only.

use crate::hr::HrData;
use crate::pmd::measurement_name;
use crate::records::PmdFrameTiming;
use crate::sink::RecordSink;
use crate::util::{csv, fmt_opt_f64, unix_ns};
use anyhow::{Context, Result, anyhow};
use chrono::Local;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct CsvWriters {
    pub dir: PathBuf,
    events: BufWriter<File>,
    raw_pmd: BufWriter<File>,
    pmd_timing: BufWriter<File>,
    ecg: BufWriter<File>,
    acc: BufWriter<File>,
    hr: BufWriter<File>,
    battery: BufWriter<File>,
    health: BufWriter<File>,
}

impl CsvWriters {
    pub fn new(base: &Path) -> Result<Self> {
        fs::create_dir_all(base).with_context(|| format!("create data dir {}", base.display()))?;
        let session = format!("h10_session_{}", Local::now().format("%Y%m%d_%H%M%S"));
        let dir = base.join(session);
        fs::create_dir_all(&dir)
            .with_context(|| format!("create session dir {}", dir.display()))?;
        update_latest_symlink(base, &dir).ok();
        Ok(Self {
            events: open_csv(&dir, "events.csv", "collector_unix_ns,event,detail\n")?,
            raw_pmd: open_csv(&dir, "raw_pmd.csv", "collector_unix_ns,hex\n")?,
            pmd_timing: open_csv(
                &dir,
                "pmd_timing.csv",
                "collector_unix_ns,measurement,measurement_type,frame_type,compressed,device_frame_ts_ns,payload_len,sample_count,collector_delta_ms,device_delta_ms,expected_frame_delta_ms,gap_ms\n",
            )?,
            ecg: open_csv(
                &dir,
                "ecg.csv",
                "collector_unix_ns,device_ts_ns,sample_index_in_frame,microvolts\n",
            )?,
            acc: open_csv(
                &dir,
                "acc.csv",
                "collector_unix_ns,device_ts_ns,sample_index_in_frame,x_mg,y_mg,z_mg\n",
            )?,
            hr: open_csv(
                &dir,
                "hr.csv",
                "collector_unix_ns,hr_bpm,sensor_contact_supported,sensor_contact_detected,energy_expended,rr_intervals_ms,raw_hex\n",
            )?,
            battery: open_csv(
                &dir,
                "battery.csv",
                "collector_unix_ns,battery_percent,reason\n",
            )?,
            health: open_csv(
                &dir,
                "health.csv",
                "collector_unix_ns,rssi,pmd_frames,hr_frames\n",
            )?,
            dir,
        })
    }

    pub fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        writeln!(self.events, "{},{},{}", unix_ns(), csv(event), csv(detail))?;
        self.events.flush()?;
        Ok(())
    }

    pub fn raw_pmd(&mut self, collector_ns: u128, bytes: &[u8]) -> Result<()> {
        writeln!(self.raw_pmd, "{},{}", collector_ns, hex::encode(bytes))?;
        Ok(())
    }

    pub fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        writeln!(
            self.pmd_timing,
            "{},{},{},{},{},{},{},{},{},{},{:.3},{}",
            timing.collector_ns,
            csv(measurement_name(timing.measurement_type)),
            timing.measurement_type,
            timing.frame_type,
            timing.compressed,
            timing.device_frame_ts,
            timing.payload_len,
            timing.sample_count,
            fmt_opt_f64(timing.collector_delta_ms),
            fmt_opt_f64(timing.device_delta_ms),
            timing.expected_frame_delta_ms,
            fmt_opt_f64(timing.gap_ms),
        )?;
        Ok(())
    }

    pub fn ecg(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        microvolts: i32,
    ) -> Result<()> {
        writeln!(self.ecg, "{collector_ns},{device_ts},{index},{microvolts}")?;
        Ok(())
    }

    pub fn acc(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        x: i32,
        y: i32,
        z: i32,
    ) -> Result<()> {
        writeln!(self.acc, "{collector_ns},{device_ts},{index},{x},{y},{z}")?;
        Ok(())
    }

    pub fn hr(&mut self, collector_ns: u128, data: &HrData, raw: &[u8]) -> Result<()> {
        let rr = data
            .rr_ms
            .iter()
            .map(|v| format!("{v:.3}"))
            .collect::<Vec<_>>()
            .join(";");
        writeln!(
            self.hr,
            "{},{},{},{},{},{},{}",
            collector_ns,
            data.hr_bpm,
            data.sensor_contact_supported,
            data.sensor_contact_detected,
            data.energy_expended
                .map(|v| v.to_string())
                .unwrap_or_default(),
            csv(&rr),
            hex::encode(raw),
        )?;
        Ok(())
    }

    pub fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()> {
        writeln!(self.battery, "{collector_ns},{level},{}", csv(reason))?;
        self.battery.flush()?;
        Ok(())
    }

    pub fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        writeln!(
            self.health,
            "{collector_ns},{},{pmd_frames},{hr_frames}",
            rssi.map(|v| v.to_string()).unwrap_or_default()
        )?;
        self.health.flush()?;
        Ok(())
    }

    pub fn flush_streams(&mut self) -> Result<()> {
        self.raw_pmd.flush()?;
        self.pmd_timing.flush()?;
        self.ecg.flush()?;
        self.acc.flush()?;
        self.hr.flush()?;
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        self.events.flush()?;
        self.raw_pmd.flush()?;
        self.pmd_timing.flush()?;
        self.ecg.flush()?;
        self.acc.flush()?;
        self.hr.flush()?;
        self.battery.flush()?;
        self.health.flush()?;
        Ok(())
    }
}

fn open_csv(dir: &Path, name: &str, header: &str) -> Result<BufWriter<File>> {
    let path = dir.join(name);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    if file.metadata()?.len() == 0 {
        file.write_all(header.as_bytes())?;
    }
    Ok(BufWriter::new(file))
}

impl RecordSink for CsvWriters {
    fn session_dir(&self) -> &Path {
        &self.dir
    }

    fn event(&mut self, event: &str, detail: &str) -> Result<()> {
        CsvWriters::event(self, event, detail)
    }

    fn raw_gatt(
        &mut self,
        collector_ns: u128,
        characteristic_uuid: &str,
        _direction: &str,
        operation: &str,
        payload: &[u8],
        detail: &str,
    ) -> Result<()> {
        if characteristic_uuid == crate::config::PMD_DATA_UUID && operation == "notification" {
            CsvWriters::raw_pmd(self, collector_ns, payload)
        } else {
            CsvWriters::event(
                self,
                "raw_gatt",
                &format!(
                    "uuid={characteristic_uuid} operation={operation} detail={detail} hex={}",
                    hex::encode(payload)
                ),
            )
        }
    }

    fn pmd_timing(&mut self, timing: &PmdFrameTiming) -> Result<()> {
        CsvWriters::pmd_timing(self, timing)
    }

    fn ecg(
        &mut self,
        collector_ns: u128,
        device_ts: u64,
        index: usize,
        microvolts: i32,
    ) -> Result<()> {
        CsvWriters::ecg(self, collector_ns, device_ts, index, microvolts)
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
        CsvWriters::acc(self, collector_ns, device_ts, index, x, y, z)
    }

    fn hr(&mut self, collector_ns: u128, data: &HrData, raw: &[u8]) -> Result<()> {
        CsvWriters::hr(self, collector_ns, data, raw)
    }

    fn battery(&mut self, collector_ns: u128, level: u8, reason: &str) -> Result<()> {
        CsvWriters::battery(self, collector_ns, level, reason)
    }

    fn health(
        &mut self,
        collector_ns: u128,
        rssi: Option<i16>,
        pmd_frames: u64,
        hr_frames: u64,
    ) -> Result<()> {
        CsvWriters::health(self, collector_ns, rssi, pmd_frames, hr_frames)
    }

    fn flush_streams(&mut self) -> Result<()> {
        CsvWriters::flush_streams(self)
    }

    fn flush_all(&mut self) -> Result<()> {
        CsvWriters::flush_all(self)
    }
}

fn update_latest_symlink(base: &Path, dir: &Path) -> Result<()> {
    let latest = base.join("latest");
    if latest.exists() || latest.is_symlink() {
        fs::remove_file(&latest)
            .or_else(|_| fs::remove_dir(&latest))
            .ok();
    }

    #[cfg(unix)]
    {
        let session_name = dir
            .file_name()
            .ok_or_else(|| anyhow!("session dir has no file name: {}", dir.display()))?;
        std::os::unix::fs::symlink(session_name, latest)?;
    }

    #[cfg(not(unix))]
    {
        let _ = (base, dir);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ACC_MEASUREMENT_TYPE, ECG_MEASUREMENT_TYPE};
    use std::io::Read;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_{name}_{}_{}",
            std::process::id(),
            unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn read_to_string(path: &Path) -> String {
        let mut text = String::new();
        File::open(path)
            .expect("open file")
            .read_to_string(&mut text)
            .expect("read file");
        text
    }

    #[test]
    fn csv_writers_create_all_files_and_write_rows() {
        let base = temp_dir("csv_all_files");
        let mut writers = CsvWriters::new(&base).expect("writers create");
        writers
            .event("quoted,event", "detail with \"quotes\"")
            .expect("event");
        writers.raw_pmd(10, &[0x01, 0xab]).expect("raw");
        writers
            .pmd_timing(&PmdFrameTiming {
                collector_ns: 11,
                measurement_type: ECG_MEASUREMENT_TYPE,
                frame_type: 0,
                compressed: false,
                device_frame_ts: 12,
                payload_len: 3,
                sample_count: 1,
                collector_delta_ms: Some(1.23456),
                device_delta_ms: None,
                expected_frame_delta_ms: 7.6923,
                gap_ms: Some(-0.25),
            })
            .expect("timing");
        writers.ecg(13, 14, 0, -123).expect("ecg");
        writers.acc(15, 16, 1, -1, 2, 3).expect("acc");
        writers
            .hr(
                17,
                &HrData {
                    hr_bpm: 61,
                    sensor_contact_supported: true,
                    sensor_contact_detected: false,
                    energy_expended: Some(99),
                    rr_ms: vec![1000.0, 750.125],
                },
                &[0x16, 61],
            )
            .expect("hr");
        writers.battery(18, 80, "periodic").expect("battery");
        writers.health(19, Some(-42), 7, 8).expect("health");
        writers.health(20, None, 9, 10).expect("health none");
        writers.flush_all().expect("flush");

        let dir = writers.dir.clone();
        assert!(dir.ends_with(dir.file_name().expect("session name")));
        assert!(dir.join("events.csv").exists());
        assert!(dir.join("raw_pmd.csv").exists());
        assert!(dir.join("pmd_timing.csv").exists());
        assert!(dir.join("ecg.csv").exists());
        assert!(dir.join("acc.csv").exists());
        assert!(dir.join("hr.csv").exists());
        assert!(dir.join("battery.csv").exists());
        assert!(dir.join("health.csv").exists());

        let events = read_to_string(&dir.join("events.csv"));
        assert!(events.contains("\"quoted,event\",\"detail with \"\"quotes\"\"\""));
        assert_eq!(
            read_to_string(&dir.join("raw_pmd.csv")),
            "collector_unix_ns,hex\n10,01ab\n"
        );
        assert!(
            read_to_string(&dir.join("pmd_timing.csv"))
                .contains("11,\"ECG\",0,0,false,12,3,1,1.235,,7.692,-0.250")
        );
        assert_eq!(
            read_to_string(&dir.join("ecg.csv")),
            "collector_unix_ns,device_ts_ns,sample_index_in_frame,microvolts\n13,14,0,-123\n"
        );
        assert_eq!(
            read_to_string(&dir.join("acc.csv")),
            "collector_unix_ns,device_ts_ns,sample_index_in_frame,x_mg,y_mg,z_mg\n15,16,1,-1,2,3\n"
        );
        assert!(
            read_to_string(&dir.join("hr.csv"))
                .contains("17,61,true,false,99,\"1000.000;750.125\",163d")
        );
        assert_eq!(
            read_to_string(&dir.join("battery.csv")),
            "collector_unix_ns,battery_percent,reason\n18,80,\"periodic\"\n"
        );
        assert_eq!(
            read_to_string(&dir.join("health.csv")),
            "collector_unix_ns,rssi,pmd_frames,hr_frames\n19,-42,7,8\n20,,9,10\n"
        );

        #[cfg(unix)]
        assert_eq!(
            fs::read_link(base.join("latest")).expect("latest symlink"),
            dir.file_name().expect("session name")
        );

        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn open_csv_appends_without_rewriting_header() {
        let dir = temp_dir("csv_append");
        {
            let mut writer = open_csv(&dir, "append.csv", "header\n").expect("open first");
            writeln!(writer, "row1").expect("row1");
            writer.flush().expect("flush first");
        }
        {
            let mut writer = open_csv(&dir, "append.csv", "header\n").expect("open second");
            writeln!(writer, "row2").expect("row2");
            writer.flush().expect("flush second");
        }
        assert_eq!(
            read_to_string(&dir.join("append.csv")),
            "header\nrow1\nrow2\n"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn update_latest_symlink_replaces_existing_file_or_directory() {
        let base = temp_dir("latest_replace");
        let first = base.join("first");
        let second = base.join("second");
        fs::create_dir_all(&first).expect("first");
        fs::create_dir_all(&second).expect("second");
        File::create(base.join("latest")).expect("stale latest file");
        update_latest_symlink(&base, &first).expect("first latest");
        #[cfg(unix)]
        assert_eq!(
            fs::read_link(base.join("latest")).expect("latest first"),
            PathBuf::from("first")
        );

        update_latest_symlink(&base, &second).expect("second latest");
        #[cfg(unix)]
        assert_eq!(
            fs::read_link(base.join("latest")).expect("latest second"),
            PathBuf::from("second")
        );
        fs::remove_dir_all(base).ok();
    }

    #[test]
    fn pmd_timing_uses_measurement_names_for_unknown_and_acc() {
        let base = temp_dir("csv_measurements");
        let mut writers = CsvWriters::new(&base).expect("writers");
        for (measurement_type, expected) in [(ACC_MEASUREMENT_TYPE, "ACC"), (63, "UNKNOWN")] {
            writers
                .pmd_timing(&PmdFrameTiming {
                    collector_ns: measurement_type as u128,
                    measurement_type,
                    frame_type: 1,
                    compressed: true,
                    device_frame_ts: 2,
                    payload_len: 3,
                    sample_count: 4,
                    collector_delta_ms: None,
                    device_delta_ms: Some(5.0),
                    expected_frame_delta_ms: 6.0,
                    gap_ms: None,
                })
                .expect("timing");
            writers.flush_all().expect("flush");
            assert!(
                read_to_string(&writers.dir.join("pmd_timing.csv"))
                    .contains(&format!("\"{expected}\""))
            );
        }
        fs::remove_dir_all(base).ok();
    }
}
