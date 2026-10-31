use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const COLLECTOR_LOCK_FILENAME: &str = "collector.lock";
const ARCHIVER_LOCK_FILENAME: &str = "archiver.lock";
const SOURCE_WRITER_LOCK_FILENAME: &str = "writer.lock";

pub fn collector_lock_path(data_dir: &Path) -> PathBuf {
    data_dir.join(COLLECTOR_LOCK_FILENAME)
}

pub fn archiver_lock_path(data_dir: &Path) -> PathBuf {
    data_dir.join("archive").join(ARCHIVER_LOCK_FILENAME)
}

pub fn source_writer_lock_path(data_dir: &Path, participant_id: &str, source: &str) -> PathBuf {
    data_dir
        .join("wal")
        .join(format!("participant_id={participant_id}"))
        .join(format!("source={source}"))
        .join(SOURCE_WRITER_LOCK_FILENAME)
}

#[derive(Debug)]
pub struct CollectorProcessLock {
    file: File,
    path: PathBuf,
}

#[derive(Debug)]
pub struct ArchiverProcessLock {
    file: File,
    path: PathBuf,
}

#[derive(Debug)]
pub struct SourceWriterLock {
    file: File,
    _path: PathBuf,
}

impl CollectorProcessLock {
    pub fn acquire(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)
            .with_context(|| format!("create collector data dir {}", data_dir.display()))?;
        let path = collector_lock_path(data_dir);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open collector process lock {}", path.display()))?;

        if let Err(err) = file.try_lock_exclusive() {
            if err.kind() == fs2::lock_contended_error().kind() {
                return Err(anyhow!(
                    "collector process lock is already held at {}; another `training_sensors collect` process is already running, so this process will not start BLE work",
                    path.display()
                ));
            }
            return Err(err)
                .with_context(|| format!("acquire collector process lock {}", path.display()));
        }

        write_lock_metadata(&mut file, &path)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CollectorProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl ArchiverProcessLock {
    pub fn acquire(data_dir: &Path) -> Result<Self> {
        let archive_dir = data_dir.join("archive");
        fs::create_dir_all(&archive_dir)
            .with_context(|| format!("create archive dir {}", archive_dir.display()))?;
        let path = archiver_lock_path(data_dir);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open archiver process lock {}", path.display()))?;

        if let Err(err) = file.try_lock_exclusive() {
            if err.kind() == fs2::lock_contended_error().kind() {
                return Err(anyhow!(
                    "archiver process lock is already held at {}; another `training_sensors archive` process is already running",
                    path.display()
                ));
            }
            return Err(err)
                .with_context(|| format!("acquire archiver process lock {}", path.display()));
        }

        write_archiver_lock_metadata(&mut file, &path)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ArchiverProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl SourceWriterLock {
    pub fn try_acquire(
        data_dir: &Path,
        participant_id: &str,
        source: &str,
    ) -> Result<Option<Self>> {
        let path = source_writer_lock_path(data_dir, participant_id, source);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("source writer lock path {} has no parent", path.display()))?;
        fs::create_dir_all(parent)
            .with_context(|| format!("create source writer lock dir {}", parent.display()))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("open source writer lock {}", path.display()))?;

        if let Err(err) = file.try_lock_exclusive() {
            if err.kind() == fs2::lock_contended_error().kind() {
                return Ok(None);
            }
            return Err(err)
                .with_context(|| format!("acquire source writer lock {}", path.display()));
        }

        write_source_lock_metadata(&mut file, &path, participant_id, source)?;
        Ok(Some(Self { file, _path: path }))
    }
}

impl Drop for SourceWriterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn write_lock_metadata(file: &mut File, path: &Path) -> Result<()> {
    file.set_len(0)
        .with_context(|| format!("truncate collector process lock {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind collector process lock {}", path.display()))?;
    writeln!(file, "pid={}", std::process::id())
        .with_context(|| format!("write collector process lock {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync collector process lock {}", path.display()))?;
    Ok(())
}

fn write_archiver_lock_metadata(file: &mut File, path: &Path) -> Result<()> {
    file.set_len(0)
        .with_context(|| format!("truncate archiver process lock {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind archiver process lock {}", path.display()))?;
    writeln!(file, "pid={}", std::process::id())
        .with_context(|| format!("write archiver process lock {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync archiver process lock {}", path.display()))?;
    Ok(())
}

fn write_source_lock_metadata(
    file: &mut File,
    path: &Path,
    participant_id: &str,
    source: &str,
) -> Result<()> {
    file.set_len(0)
        .with_context(|| format!("truncate source writer lock {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind source writer lock {}", path.display()))?;
    writeln!(file, "pid={}", std::process::id())
        .with_context(|| format!("write source writer lock {}", path.display()))?;
    writeln!(file, "participant_id={participant_id}")
        .with_context(|| format!("write source writer lock {}", path.display()))?;
    writeln!(file, "source={source}")
        .with_context(|| format!("write source writer lock {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("sync source writer lock {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "training_sensors_process_lock_{name}_{}_{}",
            std::process::id(),
            crate::util::unix_ns()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn collector_lock_path_is_under_data_dir() {
        assert_eq!(
            collector_lock_path(Path::new("data")),
            PathBuf::from("data").join("collector.lock")
        );
        assert_eq!(
            archiver_lock_path(Path::new("data")),
            PathBuf::from("data").join("archive").join("archiver.lock")
        );
        assert_eq!(
            source_writer_lock_path(Path::new("data"), "local", "manual_events"),
            PathBuf::from("data")
                .join("wal")
                .join("participant_id=local")
                .join("source=manual_events")
                .join("writer.lock")
        );
    }

    #[test]
    fn second_acquire_fails_while_first_lock_is_held() {
        let dir = temp_dir("held");
        let first = CollectorProcessLock::acquire(&dir).expect("first lock");
        let err = CollectorProcessLock::acquire(&dir).expect_err("second lock should fail");
        assert!(
            format!("{err:#}")
                .contains("another `training_sensors collect` process is already running"),
            "unexpected error: {err:#}"
        );
        assert_eq!(first.path(), dir.join("collector.lock"));
        drop(first);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn acquire_rewrites_lock_metadata_with_current_pid() {
        let dir = temp_dir("metadata");
        let path = collector_lock_path(&dir);
        fs::write(&path, "pid=1\nold stale metadata\n").expect("write stale lock file");

        let lock = CollectorProcessLock::acquire(&dir).expect("lock stale file");
        let text = fs::read_to_string(&path).expect("lock metadata");
        assert_eq!(text, format!("pid={}\n", std::process::id()));
        drop(lock);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn stale_lock_file_alone_does_not_block_after_os_lock_release() {
        let dir = temp_dir("stale_file");
        let path = collector_lock_path(&dir);
        fs::write(&path, "pid=1\nold stale metadata\n").expect("write stale lock file");

        {
            let lock = CollectorProcessLock::acquire(&dir).expect("lock stale file");
            assert_eq!(lock.path(), path.as_path());
            assert!(path.exists());
        }

        assert!(path.exists(), "lock file may remain on disk after release");
        let lock = CollectorProcessLock::acquire(&dir).expect("relock after release");
        drop(lock);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn acquire_fails_when_data_dir_path_is_a_file() {
        let dir = temp_dir("bad_data_dir");
        let file_path = dir.join("not_a_directory");
        fs::write(&file_path, "not a directory").expect("write file path");

        let err = CollectorProcessLock::acquire(&file_path).expect_err("file path must fail");
        assert!(
            format!("{err:#}").contains("create collector data dir"),
            "unexpected error: {err:#}"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn archiver_lock_blocks_duplicate_archivers_but_not_stale_files() {
        let dir = temp_dir("archiver_held");
        let first = ArchiverProcessLock::acquire(&dir).expect("first archiver lock");
        let err = ArchiverProcessLock::acquire(&dir).expect_err("second archiver lock fails");
        assert!(
            format!("{err:#}")
                .contains("another `training_sensors archive` process is already running"),
            "unexpected error: {err:#}"
        );
        assert_eq!(first.path(), archiver_lock_path(&dir));
        drop(first);

        fs::write(archiver_lock_path(&dir), "pid=1\nstale\n").expect("stale lock file");
        let second = ArchiverProcessLock::acquire(&dir).expect("stale lock file relock");
        let text = fs::read_to_string(archiver_lock_path(&dir)).expect("metadata");
        assert_eq!(text, format!("pid={}\n", std::process::id()));
        drop(second);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn archiver_lock_reports_bad_archive_dir_and_lock_path_errors() {
        let dir = temp_dir("archiver_bad_paths");
        fs::write(dir.join("archive"), "not a directory").expect("archive file");
        let err = ArchiverProcessLock::acquire(&dir).expect_err("archive file blocks dir");
        assert!(
            format!("{err:#}").contains("create archive dir"),
            "unexpected error: {err:#}"
        );
        fs::remove_file(dir.join("archive")).expect("remove archive file");

        fs::create_dir_all(dir.join("archive/archiver.lock")).expect("lock path dir");
        let err = ArchiverProcessLock::acquire(&dir).expect_err("lock path dir blocks file");
        assert!(
            format!("{err:#}").contains("open archiver process lock"),
            "unexpected error: {err:#}"
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn source_writer_lock_is_optional_on_contention_and_rewrites_metadata() {
        let dir = temp_dir("source_writer");
        let first = SourceWriterLock::try_acquire(&dir, "local", "manual_events")
            .expect("first result")
            .expect("first lock");
        let second =
            SourceWriterLock::try_acquire(&dir, "local", "manual_events").expect("second result");
        assert!(second.is_none(), "contended source lock should be optional");
        assert!(source_writer_lock_path(&dir, "local", "manual_events").exists());
        drop(first);

        fs::write(
            source_writer_lock_path(&dir, "local", "manual_events"),
            "pid=1\nstale\n",
        )
        .expect("stale metadata");
        let relock = SourceWriterLock::try_acquire(&dir, "local", "manual_events")
            .expect("relock result")
            .expect("relock");
        let text = fs::read_to_string(source_writer_lock_path(&dir, "local", "manual_events"))
            .expect("metadata");
        assert!(text.contains(&format!("pid={}\n", std::process::id())));
        assert!(text.contains("participant_id=local\n"));
        assert!(text.contains("source=manual_events\n"));
        drop(relock);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn source_writer_lock_reports_bad_paths() {
        let dir = temp_dir("source_writer_bad_paths");
        fs::write(dir.join("wal"), "not a directory").expect("wal file");
        let err = SourceWriterLock::try_acquire(&dir, "local", "manual_events")
            .expect_err("wal file blocks source lock dir");
        assert!(
            format!("{err:#}").contains("create source writer lock dir"),
            "unexpected error: {err:#}"
        );
        fs::remove_file(dir.join("wal")).expect("remove wal file");

        let lock_path = source_writer_lock_path(&dir, "local", "manual_events");
        fs::create_dir_all(&lock_path).expect("lock path dir");
        let err = SourceWriterLock::try_acquire(&dir, "local", "manual_events")
            .expect_err("lock path directory blocks source lock file");
        assert!(
            format!("{err:#}").contains("open source writer lock"),
            "unexpected error: {err:#}"
        );
        fs::remove_dir_all(dir).ok();
    }
}
