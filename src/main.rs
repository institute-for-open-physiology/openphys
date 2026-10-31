mod archive;
mod ble;
mod cloud;
mod config;
#[cfg(test)]
mod csv;
mod gatt;
mod hr;
mod live_server;
mod pmd;
mod process_lock;
mod psftp;
mod records;
mod replay;
mod seca;
mod sink;
mod util;
mod wal;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use config::{CollectorConfig, DEFAULT_TARGET};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "training_sensors")]
#[command(about = "Polar H10 collector MVP")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Scan nearby BLE devices and print what CoreBluetooth exposes.
    Scan {
        #[arg(long, default_value_t = 12)]
        seconds: u64,
    },
    /// Aggressively connect/reconnect to the fixed H10 and write raw WAL capture.
    Collect {
        #[arg(long, default_value = DEFAULT_TARGET)]
        target: String,
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
        #[arg(long, default_value_t = 30)]
        scan_timeout_seconds: u64,
    },
    /// Scan closed WAL segments and write derived Parquet archive files.
    Archive {
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
    },
    /// Serve a WAL-backed live dashboard and marker API.
    Live {
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
        #[arg(long, default_value_t = 8000)]
        port: u16,
        #[arg(long, default_value_t = 20)]
        live_window_seconds: u64,
        #[arg(long, default_value_t = 10.0)]
        history_window_hours: f64,
    },
    /// Run the authenticated multi-user cloud ingestion and dashboard service.
    Cloud {
        #[arg(long, default_value = "data")]
        data_dir: PathBuf,
        #[arg(long, default_value = "data/db/cloud.sqlite3")]
        db_path: PathBuf,
        #[arg(long, default_value_t = 8000)]
        port: u16,
    },
    /// Replay WAL records deterministically as JSONL or a summary.
    Replay {
        #[arg(long = "from-wal")]
        from_wal: PathBuf,
        #[arg(long, value_enum, default_value_t = replay::ReplayMode::Batch)]
        mode: replay::ReplayMode,
        #[arg(long = "speed-factor", default_value_t = 1.0)]
        speed_factor: f64,
        #[arg(long)]
        allow_open: bool,
        #[arg(long, value_enum, default_value_t = replay::ReplayOutput::Jsonl)]
        output: replay::ReplayOutput,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Scan { seconds } => ble::scan(seconds).await,
        Commands::Collect {
            target,
            data_dir,
            scan_timeout_seconds,
        } => ble::collect(CollectorConfig::new(target, data_dir, scan_timeout_seconds)).await,
        Commands::Archive { data_dir } => {
            let summary = archive::run_wal_archiver(&data_dir)?;
            println!(
                "archive complete: scanned={} archived={} skipped={} outputs={} stale_tmp_removed={}",
                summary.scanned_segments,
                summary.archived_segments,
                summary.skipped_segments,
                summary.output_files,
                summary.stale_tmp_files_removed
            );
            Ok(())
        }
        Commands::Live {
            data_dir,
            port,
            live_window_seconds,
            history_window_hours,
        } => {
            if !history_window_hours.is_finite() || history_window_hours <= 0.0 {
                bail!("--history-window-hours must be a positive finite number");
            }
            let mut config = live_server::LiveServerConfig::new(data_dir, port);
            config.live_window = std::time::Duration::from_secs(live_window_seconds);
            config.history_window =
                std::time::Duration::from_secs_f64(history_window_hours * 3600.0);
            live_server::run(config).await
        }
        Commands::Cloud {
            data_dir,
            db_path,
            port,
        } => {
            let config = cloud::CloudConfig::from_env(data_dir, db_path, port)?;
            cloud::run(config).await
        }
        Commands::Replay {
            from_wal,
            mode,
            speed_factor,
            allow_open,
            output,
        } => {
            let mut config = replay::ReplayConfig::new(from_wal);
            config.mode = mode;
            config.speed_factor = speed_factor;
            config.allow_open = allow_open;
            config.output = output;
            replay::run(config)?;
            Ok(())
        }
    }
}
