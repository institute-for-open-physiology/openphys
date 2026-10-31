use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use uuid::Uuid;

const MAX_MISSING_RANGES: usize = 64;
pub const CURRENT_SCHEMA_VERSION: i64 = 2;
// Retained with the passkey tables and helpers so the prior schema remains
// executable during a rollback window.
#[allow(dead_code)]
const CEREMONY_RATE_WINDOW_MS: i64 = 60_000;
#[allow(dead_code)]
const MAX_ACTIVE_CEREMONIES_GLOBAL: i64 = 256;
#[allow(dead_code)]
const MAX_ACTIVE_CEREMONIES_PER_EMAIL: i64 = 8;
#[allow(dead_code)]
const MAX_CEREMONY_STARTS_GLOBAL_PER_WINDOW: i64 = 256;
#[allow(dead_code)]
const MAX_CEREMONY_STARTS_PER_EMAIL_PER_WINDOW: i64 = 24;
const READ_BUSY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
pub struct AuthRateLimits {
    pub window_ms: i64,
    pub global: i64,
    pub account: i64,
    pub client: i64,
}

#[derive(Debug)]
pub struct CloudDb {
    // Readers are declared before the writer so they close first. This leaves
    // the read/write connection as SQLite's final WAL owner during clean drop.
    request_reader: Option<Mutex<Connection>>,
    maintenance_reader: Option<Mutex<Connection>>,
    checkpoint: Option<Mutex<Connection>>,
    writer: Mutex<Connection>,
    wal_path: Option<PathBuf>,
    #[cfg(test)]
    fail_next_batch_commit: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_timed_commit_phase: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    fail_next_enrollment_exchange_phase: std::sync::atomic::AtomicU8,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub is_admin: bool,
    pub disabled: bool,
    pub retired_unix_ms: Option<i64>,
}

impl User {
    pub fn is_active(&self) -> bool {
        !self.disabled && self.retired_unix_ms.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionUser {
    pub user: User,
    pub session_id: Uuid,
    pub installation_id: Option<Uuid>,
    pub csrf_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBatch {
    pub checksum: String,
    pub first_source_seq: i64,
    pub last_source_seq: i64,
    pub accepted_records: usize,
    pub received_unix_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invitation {
    pub id: Uuid,
    pub email: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct PasswordLogin {
    pub user: User,
    pub password_hash: Option<String>,
}

impl std::fmt::Debug for PasswordLogin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PasswordLogin")
            .field("user_id", &self.user.id)
            .field(
                "password_hash",
                &self.password_hash.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DevicePrincipal {
    pub user: User,
    pub credential_id: Uuid,
    pub installation_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DeviceCredentialSummary {
    pub id: Uuid,
    pub created_unix_ms: i64,
    pub last_used_unix_ms: Option<i64>,
    pub revoked_unix_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InstallationSummary {
    pub id: Uuid,
    pub user_id: Uuid,
    pub display_label: Option<String>,
    pub android_manufacturer: Option<String>,
    pub android_model: Option<String>,
    pub android_sdk: Option<i64>,
    pub app_package: Option<String>,
    pub app_version_name: Option<String>,
    pub app_version_code: Option<i64>,
    pub created_unix_ms: i64,
    pub enrolled_unix_ms: Option<i64>,
    pub last_seen_unix_ms: i64,
    pub revoked_unix_ms: Option<i64>,
    pub credentials: Vec<DeviceCredentialSummary>,
}

/// One signed-in browser "device": a session rotation family, keyed by its
/// root session id. Deliberately excludes every token hash column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BrowserSessionDevice {
    pub family_id: Uuid,
    pub created_unix_ms: i64,
    pub last_active_unix_ms: i64,
    pub user_agent: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct InvitationSummary {
    pub id: Uuid,
    pub email: String,
    pub created_unix_ms: i64,
    pub expires_unix_ms: i64,
    pub consumed_unix_ms: Option<i64>,
    pub revoked_unix_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct CommittedBatch<'a> {
    pub user_id: Uuid,
    pub installation_id: Uuid,
    pub batch_id: Uuid,
    pub checksum: &'a str,
    pub first_source_seq: i64,
    pub last_source_seq: i64,
    pub accepted_records: usize,
    pub received_unix_ms: i64,
    pub records: &'a [(i64, Uuid, String)],
}

#[allow(dead_code)]
pub struct NewRegistration<'a> {
    pub invitation_id: Uuid,
    pub requested_user_id: Uuid,
    pub email: &'a str,
    pub display_name: &'a str,
    pub credential_id: &'a str,
    pub passkey_json: &'a str,
    pub now_ms: i64,
}

pub struct NewPasswordRegistration<'a> {
    pub invitation_token: &'a str,
    pub password_hash: &'a str,
    pub requested_user_id: Uuid,
    pub session: NewSession<'a>,
}

pub struct VerifiedPasswordLogin<'a> {
    pub user_id: Uuid,
    pub verified_password_hash: &'a str,
    pub replacement_password_hash: Option<&'a str>,
    pub session: NewSession<'a>,
}

pub struct PasswordResetFinish<'a> {
    pub reset_token: &'a str,
    pub password_hash: &'a str,
    pub session: NewSession<'a>,
}

pub struct NewEnrollment<'a> {
    pub user_id: Uuid,
    pub browser_session_id: Uuid,
    pub token: &'a str,
    pub now_ms: i64,
    pub expires_ms: i64,
}

pub const MAC_APP_PACKAGE: &str = "com.vineyardfinance.sensors.seca-usb";

pub struct EnrollmentExchange<'a> {
    pub enrollment_token: &'a str,
    pub exchange_id: Uuid,
    pub installation_id: Uuid,
    pub credential_id: Uuid,
    pub credential_hash: &'a str,
    pub display_label: Option<&'a str>,
    pub android_manufacturer: Option<&'a str>,
    pub android_model: Option<&'a str>,
    pub android_sdk: Option<i64>,
    pub app_package: &'a str,
    pub app_version_name: &'a str,
    pub app_version_code: i64,
    pub now_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnrollmentExchangeResult {
    pub user_id: Uuid,
    pub credential_id: Uuid,
    pub installation_id: Uuid,
    pub expires_unix_ms: i64,
    pub exact_retry: bool,
}

#[derive(Debug)]
pub enum EnrollmentExchangeError {
    Invalid,
    Internal(anyhow::Error),
}

#[derive(Debug)]
struct TerminalAuthToken(&'static str);

impl std::fmt::Display for TerminalAuthToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for TerminalAuthToken {}

fn terminal_auth_token(message: &'static str) -> anyhow::Error {
    anyhow::Error::new(TerminalAuthToken(message))
}

pub fn is_terminal_auth_token_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<TerminalAuthToken>().is_some()
}

#[derive(Debug)]
struct InvalidEnrollmentExchange;

impl std::fmt::Display for InvalidEnrollmentExchange {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("enrollment is invalid or unavailable")
    }
}

impl std::error::Error for InvalidEnrollmentExchange {}

fn invalid_enrollment_exchange() -> anyhow::Error {
    anyhow::Error::new(InvalidEnrollmentExchange)
}

#[allow(dead_code)]
pub struct NewCeremony<'a> {
    pub challenge_id: Uuid,
    pub kind: &'a str,
    pub email: &'a str,
    pub invitation_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub installation_id: Option<Uuid>,
    pub state_json: &'a str,
    pub now_ms: i64,
    pub expires_ms: i64,
}

pub struct NewSession<'a> {
    pub user_id: Uuid,
    pub installation_id: Option<Uuid>,
    pub access_token: &'a str,
    pub refresh_token: &'a str,
    pub csrf_token: &'a str,
    pub user_agent: Option<&'a str>,
    pub now_ms: i64,
    pub access_expires_ms: i64,
    pub refresh_expires_ms: i64,
}

pub struct RefreshRotation<'a> {
    pub old_refresh: &'a str,
    pub rotation_id: Uuid,
    pub access_token: &'a str,
    pub refresh_token: &'a str,
    pub csrf_token: &'a str,
    pub user_agent: Option<&'a str>,
    pub now_ms: i64,
    pub access_expires_ms: i64,
    pub refresh_expires_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshResult {
    pub user: User,
    pub access_expires_ms: i64,
    pub refresh_expires_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PassiveCheckpointResult {
    pub busy: bool,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
    pub wal_bytes: u64,
    pub checkpoint_operation_ns: u128,
    pub wal_stat_ns: u128,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitPhaseTiming {
    pub elapsed_ns: u128,
    pub checkpoint_overlap: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommitBatchTiming {
    pub writer_wait: CommitPhaseTiming,
    pub transaction_begin: CommitPhaseTiming,
    pub row_work: CommitPhaseTiming,
    pub commit: CommitPhaseTiming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitBatchPhase {
    WriterWait,
    TransactionBegin,
    RowWork,
    Commit,
}

#[derive(Clone, Copy)]
enum CommitBatchParentPolicy {
    AuthenticatedLive,
    ExistingWalParents,
}

fn timed_commit_phase<T>(
    activity: &mut impl FnMut() -> (u64, bool),
    clock_ns: &mut impl FnMut() -> u128,
    operation: impl FnOnce() -> T,
) -> (T, CommitPhaseTiming) {
    let before = activity();
    let started_ns = clock_ns();
    let result = operation();
    let elapsed_ns = clock_ns().saturating_sub(started_ns);
    let after = activity();
    (
        result,
        CommitPhaseTiming {
            elapsed_ns,
            checkpoint_overlap: before.1 || after.1 || before.0 != after.0,
        },
    )
}

enum CloudConnectionGuard<'a> {
    Dedicated(MutexGuard<'a, Connection>),
    MemoryFallback(MutexGuard<'a, Connection>),
}

impl Deref for CloudConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Dedicated(connection) | Self::MemoryFallback(connection) => connection,
        }
    }
}

fn open_read_only_connection(path: &Path, role: &str) -> Result<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open cloud DB {role} reader {}", path.display()))?;
    connection
        .busy_timeout(READ_BUSY_TIMEOUT)
        .with_context(|| format!("configure cloud DB {role} reader busy timeout"))?;
    connection
        .pragma_update(None, "query_only", "ON")
        .with_context(|| format!("enforce cloud DB {role} reader query-only mode"))?;
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .with_context(|| format!("query cloud DB {role} reader journal mode"))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        bail!("cloud DB {role} reader did not open in WAL mode");
    }
    Ok(connection)
}

fn open_checkpoint_connection(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
        .with_context(|| format!("open cloud DB checkpoint connection {}", path.display()))?;
    connection
        .busy_timeout(Duration::ZERO)
        .context("disable checkpoint connection busy waiting")?;
    let journal_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .context("query checkpoint connection journal mode")?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        bail!("cloud DB checkpoint connection did not open in WAL mode");
    }
    connection
        .pragma_update(None, "synchronous", "FULL")
        .context("enable checkpoint connection FULL synchronous mode")?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .context("enable checkpoint connection foreign keys")?;
    connection
        .pragma_update(None, "wal_autocheckpoint", 0)
        .context("disable checkpoint connection automatic WAL checkpoints")?;
    Ok(connection)
}

fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push("-wal");
    PathBuf::from(value)
}

impl CloudDb {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create cloud DB directory {}", parent.display()))?;
        }
        let mut writer =
            Connection::open(path).with_context(|| format!("open cloud DB {}", path.display()))?;
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .context("enable SQLite WAL mode")?;
        writer
            .pragma_update(None, "synchronous", "FULL")
            .context("enable SQLite FULL synchronous mode")?;
        writer
            .pragma_update(None, "foreign_keys", "ON")
            .context("enable SQLite foreign keys")?;
        writer
            .pragma_update(None, "wal_autocheckpoint", 0)
            .context("disable writer automatic WAL checkpoints")?;
        apply_schema_migrations(&mut writer)?;
        let (request_reader, maintenance_reader, checkpoint, wal_path) =
            if path == Path::new(":memory:") {
                // A second ordinary :memory: connection names a distinct database.
                // Unit tests retain the legacy one-connection topology; the
                // production concurrency regression uses a real temporary WAL DB.
                (None, None, None, None)
            } else {
                (
                    Some(Mutex::new(open_read_only_connection(path, "request")?)),
                    Some(Mutex::new(open_read_only_connection(path, "maintenance")?)),
                    Some(Mutex::new(open_checkpoint_connection(path)?)),
                    Some(sqlite_wal_path(path)),
                )
            };
        Ok(Self {
            request_reader,
            maintenance_reader,
            checkpoint,
            writer: Mutex::new(writer),
            wal_path,
            #[cfg(test)]
            fail_next_batch_commit: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            fail_next_timed_commit_phase: std::sync::atomic::AtomicU8::new(0),
            #[cfg(test)]
            fail_next_enrollment_exchange_phase: std::sync::atomic::AtomicU8::new(0),
        })
    }

    fn writer(&self) -> Result<MutexGuard<'_, Connection>> {
        self.writer
            .lock()
            .map_err(|_| anyhow!("cloud DB writer lock poisoned"))
    }

    fn request_reader(&self) -> Result<CloudConnectionGuard<'_>> {
        self.read_connection(&self.request_reader, "request reader")
    }

    fn maintenance_reader(&self) -> Result<CloudConnectionGuard<'_>> {
        self.read_connection(&self.maintenance_reader, "maintenance reader")
    }

    fn checkpoint(&self) -> Result<MutexGuard<'_, Connection>> {
        self.checkpoint
            .as_ref()
            .context("checkpoint connection unavailable for in-memory cloud DB")?
            .lock()
            .map_err(|_| anyhow!("cloud DB checkpoint mutex was poisoned"))
    }

    fn read_connection<'a>(
        &'a self,
        reader: &'a Option<Mutex<Connection>>,
        name: &str,
    ) -> Result<CloudConnectionGuard<'a>> {
        match reader {
            Some(reader) => reader
                .lock()
                .map(CloudConnectionGuard::Dedicated)
                .map_err(|_| anyhow!("cloud DB {name} lock poisoned")),
            None => self.writer().map(CloudConnectionGuard::MemoryFallback),
        }
    }

    pub fn ensure_admin(&self, email: &str, now_ms: i64) -> Result<User> {
        let email = normalize_email(email)?;
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) \
             VALUES (?1,?2,?3,1,0,?4) ON CONFLICT(email) DO UPDATE SET is_admin=1",
            params![Uuid::new_v4().to_string(), email, email, now_ms],
        )?;
        let user = query_user_by_email(&transaction, &email)?
            .ok_or_else(|| anyhow!("admin disappeared after upsert"))?;
        transaction.commit()?;
        Ok(user)
    }

    pub fn user_by_id(&self, user_id: Uuid) -> Result<Option<User>> {
        query_user_by_id(&*self.request_reader()?, user_id)
    }

    #[allow(dead_code)]
    pub fn user_by_email(&self, email: &str) -> Result<Option<User>> {
        query_user_by_email(&*self.request_reader()?, &normalize_email(email)?)
    }

    pub fn list_users(&self) -> Result<Vec<User>> {
        let connection = self.request_reader()?;
        let mut statement = connection.prepare(
            "SELECT id,email,display_name,is_admin,disabled,retired_unix_ms \
             FROM users WHERE retired_unix_ms IS NULL ORDER BY email",
        )?;
        let rows = statement.query_map([], row_to_user)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("list users")
    }

    pub fn password_login(&self, email: &str) -> Result<Option<PasswordLogin>> {
        let email = normalize_email(email)?;
        self.request_reader()?
            .query_row(
                "SELECT u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms,p.password_hash \
                 FROM users u LEFT JOIN password_credentials p ON p.user_id=u.id WHERE u.email=?1",
                [email],
                |row| {
                    Ok(PasswordLogin {
                        user: row_to_user(row)?,
                        password_hash: row.get(6)?,
                    })
                },
            )
            .optional()
            .context("load password login")
    }

    pub fn finish_verified_password_login(&self, login: VerifiedPasswordLogin<'_>) -> Result<bool> {
        if login.session.installation_id.is_some() || login.session.user_id != login.user_id {
            bail!("password login requires a browser-only session for the intended user");
        }
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let current_hash = transaction
            .query_row(
                "SELECT p.password_hash FROM password_credentials p \
                 JOIN users u ON u.id=p.user_id WHERE p.user_id=?1 \
                   AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                [login.user_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if current_hash.as_deref() != Some(login.verified_password_hash) {
            transaction.commit()?;
            return Ok(false);
        }
        if let Some(replacement) = login.replacement_password_hash {
            let changed = transaction.execute(
                "UPDATE password_credentials SET password_hash=?3,changed_unix_ms=?4 \
                 WHERE user_id=?1 AND password_hash=?2",
                params![
                    login.user_id.to_string(),
                    login.verified_password_hash,
                    replacement,
                    login.session.now_ms
                ],
            )?;
            if changed != 1 {
                transaction.rollback()?;
                return Ok(false);
            }
        }
        insert_session(&transaction, &login.session)?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn register_password(&self, registration: NewPasswordRegistration<'_>) -> Result<User> {
        if registration.session.installation_id.is_some()
            || registration.session.user_id != registration.requested_user_id
        {
            bail!("password registration requires a browser-only session for the intended user");
        }
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let (invitation_id, email): (String, String) = transaction
            .query_row(
                "SELECT id,email FROM invitations WHERE token_hash=?1 \
                 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?2",
                params![
                    hash_secret(registration.invitation_token),
                    registration.session.now_ms
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| {
                terminal_auth_token("invitation is invalid, expired, revoked, or already used")
            })?;
        let existing = query_user_by_email(&transaction, &email)?;
        let user_id = existing
            .as_ref()
            .map(|user| user.id)
            .unwrap_or(registration.requested_user_id);
        if user_id != registration.session.user_id {
            return Err(terminal_auth_token("invitation became unavailable"));
        }
        if existing.as_ref().is_some_and(|user| !user.is_active()) {
            return Err(terminal_auth_token("invitation became unavailable"));
        }
        let has_password = transaction
            .query_row(
                "SELECT 1 FROM password_credentials WHERE user_id=?1",
                [user_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if has_password {
            return Err(terminal_auth_token("invitation became unavailable"));
        }
        if existing.is_none() {
            transaction.execute(
                "INSERT INTO users(id,email,display_name,is_admin,disabled,retired_unix_ms,created_unix_ms) \
                 VALUES (?1,?2,?2,0,0,NULL,?3)",
                params![user_id.to_string(), email, registration.session.now_ms],
            )?;
        }
        transaction.execute(
            "INSERT INTO password_credentials(user_id,password_hash,created_unix_ms,changed_unix_ms) \
             VALUES (?1,?2,?3,?3)",
            params![user_id.to_string(), registration.password_hash, registration.session.now_ms],
        )?;
        let consumed = transaction.execute(
            "UPDATE invitations SET consumed_unix_ms=?2,consumed_by=?3 WHERE id=?1 \
             AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?2",
            params![
                invitation_id,
                registration.session.now_ms,
                user_id.to_string()
            ],
        )?;
        if consumed != 1 {
            return Err(terminal_auth_token("invitation became unavailable"));
        }
        insert_session(&transaction, &registration.session)?;
        let user = query_user_by_id(&transaction, user_id)?
            .ok_or_else(|| anyhow!("registered user disappeared"))?;
        transaction.commit()?;
        Ok(user)
    }

    pub fn issue_password_reset_for_email(
        &self,
        email: &str,
        token: &str,
        now_ms: i64,
        expires_ms: i64,
    ) -> Result<bool> {
        let email = normalize_email(email)?;
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let user = query_user_by_email(&transaction, &email)?;
        let Some(user) = user.filter(User::is_active) else {
            transaction.commit()?;
            return Ok(false);
        };
        transaction.execute(
            "UPDATE password_reset_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE user_id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL",
            params![user.id.to_string(), now_ms],
        )?;
        transaction.execute(
            "INSERT INTO password_reset_tokens(id,user_id,token_hash,created_unix_ms,expires_unix_ms) \
             VALUES (?1,?2,?3,?4,?5)",
            params![
                Uuid::new_v4().to_string(),
                user.id.to_string(),
                hash_secret(token),
                now_ms,
                expires_ms
            ],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn password_reset_user(&self, token: &str, now_ms: i64) -> Result<Option<User>> {
        self.request_reader()?
            .query_row(
                "SELECT u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms \
                 FROM password_reset_tokens r JOIN users u ON u.id=r.user_id \
                 WHERE r.token_hash=?1 AND r.consumed_unix_ms IS NULL AND r.revoked_unix_ms IS NULL \
                   AND r.expires_unix_ms>?2 AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                params![hash_secret(token), now_ms],
                row_to_user,
            )
            .optional()
            .context("load password reset user")
    }

    pub fn finish_password_reset(&self, reset: PasswordResetFinish<'_>) -> Result<User> {
        if reset.session.installation_id.is_some() {
            bail!("password reset requires a browser-only session");
        }
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let user = transaction
            .query_row(
                "SELECT u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms \
                 FROM password_reset_tokens r JOIN users u ON u.id=r.user_id \
                 WHERE r.token_hash=?1 AND r.consumed_unix_ms IS NULL AND r.revoked_unix_ms IS NULL \
                   AND r.expires_unix_ms>?2 AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                params![hash_secret(reset.reset_token), reset.session.now_ms],
                row_to_user,
            )
            .optional()?
            .ok_or_else(|| {
                terminal_auth_token(
                    "password reset is invalid, expired, revoked, or already used",
                )
            })?;
        if reset.session.user_id != user.id {
            return Err(terminal_auth_token("password reset became unavailable"));
        }
        transaction.execute(
            "INSERT INTO password_credentials(user_id,password_hash,created_unix_ms,changed_unix_ms) \
             VALUES (?1,?2,?3,?3) ON CONFLICT(user_id) DO UPDATE SET \
             password_hash=excluded.password_hash,changed_unix_ms=excluded.changed_unix_ms",
            params![user.id.to_string(), reset.password_hash, reset.session.now_ms],
        )?;
        let consumed = transaction.execute(
            "UPDATE password_reset_tokens SET consumed_unix_ms=?2 WHERE token_hash=?1 \
             AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?2",
            params![hash_secret(reset.reset_token), reset.session.now_ms],
        )?;
        if consumed != 1 {
            return Err(terminal_auth_token("password reset became unavailable"));
        }
        transaction.execute(
            "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE user_id=?1",
            params![user.id.to_string(), reset.session.now_ms],
        )?;
        transaction.execute(
            "UPDATE enrollment_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE user_id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL",
            params![user.id.to_string(), reset.session.now_ms],
        )?;
        insert_session(&transaction, &reset.session)?;
        transaction.commit()?;
        Ok(user)
    }

    pub fn admit_auth_attempt(
        &self,
        kind: &str,
        account_bucket: &str,
        client_bucket: &str,
        now_ms: i64,
        limits: AuthRateLimits,
    ) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let cutoff = now_ms.saturating_sub(limits.window_ms);
        transaction.execute(
            "DELETE FROM auth_rate_events WHERE kind=?1 AND created_unix_ms<=?2",
            params![kind, cutoff],
        )?;
        let count = |scope: &str, bucket: &str| -> rusqlite::Result<i64> {
            transaction.query_row(
                "SELECT COUNT(*) FROM auth_rate_events WHERE kind=?1 AND scope=?2 \
                 AND bucket_hash=?3 AND created_unix_ms>?4",
                params![kind, scope, bucket, cutoff],
                |row| row.get(0),
            )
        };
        if count("global", "global")? >= limits.global
            || count("account", account_bucket)? >= limits.account
            || count("client", client_bucket)? >= limits.client
        {
            transaction.commit()?;
            return Ok(false);
        }
        for (scope, bucket) in [
            ("global", "global"),
            ("account", account_bucket),
            ("client", client_bucket),
        ] {
            transaction.execute(
                "INSERT INTO auth_rate_events(id,kind,scope,bucket_hash,created_unix_ms) \
                 VALUES (?1,?2,?3,?4,?5)",
                params![Uuid::new_v4().to_string(), kind, scope, bucket, now_ms],
            )?;
        }
        transaction.commit()?;
        Ok(true)
    }

    pub fn create_enrollment(&self, enrollment: NewEnrollment<'_>) -> Result<Uuid> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let active_session = transaction
            .query_row(
                "SELECT 1 FROM sessions s JOIN users u ON u.id=s.user_id \
                 WHERE s.id=?1 AND s.user_id=?2 AND s.installation_id IS NULL \
                   AND s.revoked_unix_ms IS NULL AND s.access_expires_unix_ms>?3 \
                   AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                params![
                    enrollment.browser_session_id.to_string(),
                    enrollment.user_id.to_string(),
                    enrollment.now_ms
                ],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !active_session {
            bail!("browser session is not active for enrollment");
        }
        transaction.execute(
            "UPDATE enrollment_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE user_id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL \
               AND browser_session_id IN (\
                 SELECT family_member.id FROM sessions family_member \
                 WHERE COALESCE(family_member.family_id,family_member.id)=(\
                   SELECT COALESCE(current.family_id,current.id) FROM sessions current WHERE current.id=?3\
                 )\
               )",
            params![
                enrollment.user_id.to_string(),
                enrollment.now_ms,
                enrollment.browser_session_id.to_string()
            ],
        )?;
        let id = Uuid::new_v4();
        transaction.execute(
            "INSERT INTO enrollment_tokens(id,user_id,browser_session_id,token_hash,created_unix_ms,expires_unix_ms) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                id.to_string(),
                enrollment.user_id.to_string(),
                enrollment.browser_session_id.to_string(),
                hash_secret(enrollment.token),
                enrollment.now_ms,
                enrollment.expires_ms
            ],
        )?;
        transaction.commit()?;
        Ok(id)
    }

    pub fn exchange_enrollment(
        &self,
        exchange: EnrollmentExchange<'_>,
    ) -> std::result::Result<EnrollmentExchangeResult, EnrollmentExchangeError> {
        self.exchange_enrollment_inner(exchange).map_err(|error| {
            if error.downcast_ref::<InvalidEnrollmentExchange>().is_some() {
                EnrollmentExchangeError::Invalid
            } else {
                EnrollmentExchangeError::Internal(error)
            }
        })
    }

    fn exchange_enrollment_inner(
        &self,
        exchange: EnrollmentExchange<'_>,
    ) -> Result<EnrollmentExchangeResult> {
        let token_hash = hash_secret(exchange.enrollment_token);
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let stored = transaction
            .query_row(
                "SELECT e.user_id,e.expires_unix_ms,e.consumed_unix_ms,e.revoked_unix_ms,\
                        e.consumed_installation_id,e.consumed_exchange_id,e.credential_id,\
                        u.disabled,u.retired_unix_ms \
                 FROM enrollment_tokens e JOIN users u ON u.id=e.user_id WHERE e.token_hash=?1",
                [&token_hash],
                |row| {
                    Ok((
                        parse_uuid(row.get::<_, String>(0)?)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        optional_uuid(row.get(4)?)?,
                        optional_uuid(row.get(5)?)?,
                        optional_uuid(row.get(6)?)?,
                        row.get::<_, bool>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(invalid_enrollment_exchange)?;
        let (
            user_id,
            expires_unix_ms,
            consumed_unix_ms,
            revoked_unix_ms,
            consumed_installation_id,
            consumed_exchange_id,
            stored_credential_id,
            user_disabled,
            user_retired,
        ) = stored;
        if user_disabled || user_retired.is_some() || revoked_unix_ms.is_some() {
            return Err(invalid_enrollment_exchange());
        }
        if expires_unix_ms <= exchange.now_ms {
            return Err(invalid_enrollment_exchange());
        }
        let existing: Option<(Uuid, Option<String>, Option<i64>)> = transaction
            .query_row(
                "SELECT user_id,app_package,revoked_unix_ms FROM installations WHERE id=?1",
                [exchange.installation_id.to_string()],
                |row| {
                    Ok((
                        parse_uuid(row.get::<_, String>(0)?)?,
                        row.get(1)?,
                        row.get(2)?,
                    ))
                },
            )
            .optional()?;
        if existing.as_ref().is_some_and(|(owner, package, _)| {
            *owner != user_id
                || (package.as_deref() == Some(MAC_APP_PACKAGE))
                    != (exchange.app_package == MAC_APP_PACKAGE)
        }) {
            return Err(invalid_enrollment_exchange());
        }
        if consumed_unix_ms.is_some() {
            if existing
                .as_ref()
                .is_none_or(|(_, _, revoked)| revoked.is_some())
            {
                return Err(invalid_enrollment_exchange());
            }
            if consumed_installation_id != Some(exchange.installation_id)
                || consumed_exchange_id != Some(exchange.exchange_id)
            {
                return Err(invalid_enrollment_exchange());
            }
            let credential_id = stored_credential_id
                .ok_or_else(|| anyhow!("consumed enrollment has no credential"))?;
            let active_hash: Option<String> = transaction
                .query_row(
                    "SELECT credential_hash FROM device_credentials \
                     WHERE id=?1 AND installation_id=?2 AND revoked_unix_ms IS NULL",
                    params![
                        credential_id.to_string(),
                        exchange.installation_id.to_string()
                    ],
                    |row| row.get(0),
                )
                .optional()?;
            if active_hash.as_deref() != Some(exchange.credential_hash) {
                return Err(invalid_enrollment_exchange());
            }
            transaction.commit()?;
            return Ok(EnrollmentExchangeResult {
                user_id,
                credential_id,
                installation_id: exchange.installation_id,
                expires_unix_ms,
                exact_retry: true,
            });
        }

        transaction.execute(
            "INSERT INTO installations(\
                 id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms,display_label,\
                 android_manufacturer,android_model,android_sdk,app_package,app_version_name,\
                 app_version_code,enrolled_unix_ms\
             ) VALUES (?1,?2,?3,?3,NULL,?4,?5,?6,?7,?8,?9,?10,?3) \
             ON CONFLICT(id) DO UPDATE SET revoked_unix_ms=NULL,display_label=excluded.display_label,\
                 android_manufacturer=excluded.android_manufacturer,android_model=excluded.android_model,\
                 android_sdk=excluded.android_sdk,app_package=excluded.app_package,\
                 app_version_name=excluded.app_version_name,app_version_code=excluded.app_version_code,\
                 enrolled_unix_ms=excluded.enrolled_unix_ms \
             WHERE installations.user_id=excluded.user_id",
            params![
                exchange.installation_id.to_string(),
                user_id.to_string(),
                exchange.now_ms,
                exchange.display_label,
                exchange.android_manufacturer,
                exchange.android_model,
                exchange.android_sdk,
                exchange.app_package,
                exchange.app_version_name,
                exchange.app_version_code,
            ],
        )?;
        transaction.execute(
            "UPDATE device_credentials SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE installation_id=?1 AND revoked_unix_ms IS NULL",
            params![exchange.installation_id.to_string(), exchange.now_ms],
        )?;
        transaction.execute(
            "INSERT INTO device_credentials(\
                 id,installation_id,credential_hash,created_unix_ms,last_used_unix_ms\
             ) VALUES (?1,?2,?3,?4,NULL)",
            params![
                exchange.credential_id.to_string(),
                exchange.installation_id.to_string(),
                exchange.credential_hash,
                exchange.now_ms
            ],
        )?;
        let consumed = transaction.execute(
            "UPDATE enrollment_tokens SET consumed_unix_ms=?2,consumed_installation_id=?3,\
                 consumed_exchange_id=?4,credential_id=?5 \
             WHERE token_hash=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL \
               AND expires_unix_ms>?2",
            params![
                token_hash,
                exchange.now_ms,
                exchange.installation_id.to_string(),
                exchange.exchange_id.to_string(),
                exchange.credential_id.to_string()
            ],
        )?;
        if consumed != 1 {
            bail!("enrollment was consumed concurrently");
        }
        #[cfg(test)]
        if self.take_enrollment_exchange_failure(1) {
            bail!("injected enrollment exchange pre-commit failure");
        }
        transaction.commit()?;
        #[cfg(test)]
        if self.take_enrollment_exchange_failure(2) {
            bail!("injected enrollment exchange post-commit failure");
        }
        Ok(EnrollmentExchangeResult {
            user_id,
            credential_id: exchange.credential_id,
            installation_id: exchange.installation_id,
            expires_unix_ms,
            exact_retry: false,
        })
    }

    pub fn authenticate_device(
        &self,
        credential: &str,
        _now_ms: i64,
    ) -> Result<Option<DevicePrincipal>> {
        self.request_reader()?
            .query_row(
                "SELECT u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms,\
                        c.id,i.id \
                 FROM device_credentials c \
                 JOIN installations i ON i.id=c.installation_id \
                 JOIN users u ON u.id=i.user_id \
                 WHERE c.credential_hash=?1 AND c.revoked_unix_ms IS NULL \
                   AND i.revoked_unix_ms IS NULL AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                [hash_secret(credential)],
                |row| {
                    Ok(DevicePrincipal {
                        user: row_to_user(row)?,
                        credential_id: parse_uuid(row.get::<_, String>(6)?)?,
                        installation_id: parse_uuid(row.get::<_, String>(7)?)?,
                    })
                },
            )
            .optional()
            .context("authenticate device credential")
    }

    #[cfg(test)]
    pub fn insert_test_device_credential(
        &self,
        user_id: Uuid,
        installation_id: Uuid,
        credential_id: Uuid,
        credential: &str,
        now_ms: i64,
    ) -> Result<()> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO installations(id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms) \
             VALUES (?1,?2,?3,?3,NULL) ON CONFLICT(id) DO UPDATE SET revoked_unix_ms=NULL \
             WHERE installations.user_id=excluded.user_id",
            params![installation_id.to_string(), user_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "UPDATE device_credentials SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE installation_id=?1 AND revoked_unix_ms IS NULL",
            params![installation_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "INSERT INTO device_credentials(id,installation_id,credential_hash,created_unix_ms) \
             VALUES (?1,?2,?3,?4)",
            params![
                credential_id.to_string(),
                installation_id.to_string(),
                hash_secret(credential),
                now_ms
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn replace_password_hash_for_test(&self, user_id: Uuid, password_hash: &str) -> Result<()> {
        let updated = self.writer()?.execute(
            "UPDATE password_credentials SET password_hash=?2 WHERE user_id=?1",
            params![user_id.to_string(), password_hash],
        )?;
        if updated != 1 {
            bail!("test password credential is missing");
        }
        Ok(())
    }

    pub fn list_installations(&self) -> Result<Vec<InstallationSummary>> {
        let connection = self.request_reader()?;
        query_installation_summaries(
            &connection,
            &format!(
                "SELECT {INSTALLATION_SUMMARY_COLUMNS} \
                 FROM installations i JOIN users u ON u.id=i.user_id \
                 WHERE u.retired_unix_ms IS NULL ORDER BY u.email,i.created_unix_ms,i.id"
            ),
            params![],
        )
    }

    /// User-scoped variant of [`Self::list_installations`] for the
    /// "Your devices" view.
    pub fn list_installations_for_user(&self, user_id: Uuid) -> Result<Vec<InstallationSummary>> {
        let connection = self.request_reader()?;
        query_installation_summaries(
            &connection,
            &format!(
                "SELECT {INSTALLATION_SUMMARY_COLUMNS} \
                 FROM installations i WHERE i.user_id=?1 ORDER BY i.created_unix_ms,i.id"
            ),
            params![user_id.to_string()],
        )
    }

    /// Active signed-in browser "devices" for one user, one row per session
    /// rotation family, newest activity first. Families whose every row is
    /// revoked or refresh-expired are omitted; token hashes never leave the
    /// database.
    pub fn list_browser_session_devices(
        &self,
        user_id: Uuid,
        now_ms: i64,
    ) -> Result<Vec<BrowserSessionDevice>> {
        let connection = self.request_reader()?;
        let mut statement = connection.prepare(
            "SELECT grouped.family_root,grouped.created_unix_ms,grouped.last_active_unix_ms,\
                    (SELECT newest.user_agent FROM sessions newest \
                      WHERE COALESCE(newest.family_id,newest.id)=grouped.family_root \
                      ORDER BY newest.created_unix_ms DESC,newest.id DESC LIMIT 1) \
             FROM (SELECT COALESCE(family_id,id) AS family_root,\
                          MIN(created_unix_ms) AS created_unix_ms,\
                          MAX(created_unix_ms) AS last_active_unix_ms \
                   FROM sessions WHERE user_id=?1 AND installation_id IS NULL \
                   GROUP BY COALESCE(family_id,id) \
                   HAVING SUM(CASE WHEN revoked_unix_ms IS NULL \
                                    AND refresh_expires_unix_ms>?2 THEN 1 ELSE 0 END)>0) grouped \
             ORDER BY grouped.last_active_unix_ms DESC,grouped.family_root",
        )?;
        let devices = statement
            .query_map(params![user_id.to_string(), now_ms], |row| {
                Ok(BrowserSessionDevice {
                    family_id: parse_uuid(row.get::<_, String>(0)?)?,
                    created_unix_ms: row.get(1)?,
                    last_active_unix_ms: row.get(2)?,
                    user_agent: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("list browser session devices")?;
        Ok(devices)
    }

    pub fn revoke_device_credential(&self, credential_id: Uuid, now_ms: i64) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let credential: Option<(String, Option<i64>)> = transaction
            .query_row(
                "SELECT installation_id,revoked_unix_ms FROM device_credentials WHERE id=?1",
                [credential_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((installation_id, already_revoked)) = credential else {
            transaction.commit()?;
            return Ok(false);
        };
        if already_revoked.is_none() {
            transaction.execute(
                "UPDATE device_credentials SET revoked_unix_ms=?2 WHERE id=?1 AND revoked_unix_ms IS NULL",
                params![credential_id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE installations SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
                 WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM device_credentials \
                     WHERE installation_id=?1 AND revoked_unix_ms IS NULL)",
                params![installation_id, now_ms],
            )?;
        }
        transaction.commit()?;
        Ok(true)
    }

    #[allow(dead_code)]
    pub fn set_users_retired_exact(
        &self,
        targets: &[(Uuid, &str)],
        protected_user_id: Uuid,
        protected_email: &str,
        reason: &str,
        retired: bool,
        now_ms: i64,
    ) -> Result<usize> {
        let protected_email = normalize_email(protected_email)?;
        if targets.is_empty() {
            bail!("exact retirement target set must not be empty");
        }
        let mut exact_targets = Vec::with_capacity(targets.len());
        for (user_id, email) in targets {
            let email = normalize_email(email)?;
            if *user_id == protected_user_id || email == protected_email {
                bail!("protected user must not be an exact retirement target");
            }
            if exact_targets.iter().any(|(existing_id, existing_email)| {
                existing_id == user_id || existing_email == &email
            }) {
                bail!("exact retirement targets contain a duplicate identity");
            }
            exact_targets.push((*user_id, email));
        }

        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let protected_exists = transaction
            .query_row(
                "SELECT 1 FROM users WHERE id=?1 AND email=?2",
                params![protected_user_id.to_string(), protected_email],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !protected_exists {
            bail!("protected user identity does not match exactly");
        }
        for (user_id, email) in &exact_targets {
            let matches = transaction
                .query_row(
                    "SELECT 1 FROM users WHERE id=?1 AND email=?2",
                    params![user_id.to_string(), email],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !matches {
                bail!("exact retirement target identity does not match");
            }
        }
        for (user_id, email) in &exact_targets {
            apply_exact_retirement(&transaction, *user_id, email, reason, retired, now_ms)?;
        }
        transaction.commit()?;
        Ok(exact_targets.len())
    }

    pub fn quick_check(&self) -> Result<()> {
        let connection = self.maintenance_reader()?;
        let result: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
        if result != "ok" {
            bail!("SQLite quick_check failed");
        }
        let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
        if statement.query([])?.next()?.is_some() {
            bail!("SQLite foreign_key_check failed");
        }
        Ok(())
    }

    pub fn schema_version(&self) -> Result<i64> {
        self.maintenance_reader()?
            .query_row(
                "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
                [],
                |row| row.get(0),
            )
            .context("query authoritative cloud schema version")
    }

    pub fn passive_checkpoint(&self) -> Result<PassiveCheckpointResult> {
        let checkpoint_started = Instant::now();
        let (busy, log_frames, checkpointed_frames): (i64, i64, i64) = self
            .checkpoint()?
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .context("run passive cloud DB checkpoint")?;
        let checkpoint_operation_ns = checkpoint_started.elapsed().as_nanos();
        let wal_stat_started = Instant::now();
        let wal_bytes = match &self.wal_path {
            Some(path) => match std::fs::metadata(path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("stat SQLite WAL {}", path.display()));
                }
            },
            None => 0,
        };
        let wal_stat_ns = wal_stat_started.elapsed().as_nanos();
        Ok(PassiveCheckpointResult {
            busy: busy != 0,
            log_frames,
            checkpointed_frames,
            wal_bytes,
            checkpoint_operation_ns,
            wal_stat_ns,
        })
    }

    pub fn set_user_disabled(&self, user_id: Uuid, disabled: bool, now_ms: i64) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let updated = transaction.execute(
            "UPDATE users SET disabled=?2 WHERE id=?1",
            params![user_id.to_string(), disabled],
        )? == 1;
        if updated && disabled {
            transaction.execute(
                "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE user_id=?1",
                params![user_id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE enrollment_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
                 WHERE user_id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL",
                params![user_id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE password_reset_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
                 WHERE user_id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL",
                params![user_id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE device_credentials SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
                 WHERE installation_id IN (SELECT id FROM installations WHERE user_id=?1) \
                   AND revoked_unix_ms IS NULL",
                params![user_id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE installations SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
                 WHERE user_id=?1",
                params![user_id.to_string(), now_ms],
            )?;
        }
        transaction.commit()?;
        Ok(updated)
    }

    pub fn stored_batch(
        &self,
        user_id: Uuid,
        installation_id: Uuid,
        batch_id: Uuid,
    ) -> Result<Option<StoredBatch>> {
        self.writer()?
            .query_row(
                "SELECT checksum,first_source_seq,last_source_seq,accepted_records,received_unix_ms \
                 FROM ingest_batches WHERE user_id=?1 AND installation_id=?2 AND batch_id=?3",
                params![user_id.to_string(), installation_id.to_string(), batch_id.to_string()],
                |row| {
                    Ok(StoredBatch {
                        checksum: row.get(0)?,
                        first_source_seq: row.get(1)?,
                        last_source_seq: row.get(2)?,
                        accepted_records: row.get::<_, u64>(3)? as usize,
                        received_unix_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .context("query stored ingest batch")
    }

    pub fn validate_new_batch_records(
        &self,
        user_id: Uuid,
        installation_id: Uuid,
        records: &[(i64, Uuid, String)],
    ) -> Result<()> {
        let connection = self.writer()?;
        let owner = connection
            .query_row(
                "SELECT user_id,revoked_unix_ms FROM installations WHERE id=?1",
                [installation_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .optional()?;
        if let Some((owner, revoked)) = owner {
            if owner != user_id.to_string() {
                bail!("installation belongs to another user");
            }
            if revoked.is_some() {
                bail!("installation is revoked");
            }
        }
        for (source_seq, record_id, _) in records {
            let conflict = connection
                .query_row(
                    "SELECT source_seq,record_id FROM ingest_records \
                     WHERE user_id=?1 AND (installation_id=?2 AND source_seq=?3 OR record_id=?4) LIMIT 1",
                    params![
                        user_id.to_string(),
                        installation_id.to_string(),
                        source_seq,
                        record_id.to_string()
                    ],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            if conflict.is_some() {
                bail!("source_seq or record_id is already committed by another batch");
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn commit_batch(&self, batch: &CommittedBatch<'_>) -> Result<()> {
        let epoch = Instant::now();
        self.commit_batch_timed_with_parent_policy(
            batch,
            CommitBatchParentPolicy::AuthenticatedLive,
            || (0, false),
            || epoch.elapsed().as_nanos(),
            |_, _| {},
        )
        .map(|_| ())
    }

    pub fn commit_batch_timed(
        &self,
        batch: &CommittedBatch<'_>,
        activity: impl FnMut() -> (u64, bool),
        clock_ns: impl FnMut() -> u128,
        observe: impl FnMut(CommitBatchPhase, CommitPhaseTiming),
    ) -> Result<CommitBatchTiming> {
        self.commit_batch_timed_with_parent_policy(
            batch,
            CommitBatchParentPolicy::AuthenticatedLive,
            activity,
            clock_ns,
            observe,
        )
    }

    pub fn reconcile_batch_from_wal(&self, batch: &CommittedBatch<'_>) -> Result<()> {
        let epoch = Instant::now();
        self.commit_batch_timed_with_parent_policy(
            batch,
            CommitBatchParentPolicy::ExistingWalParents,
            || (0, false),
            || epoch.elapsed().as_nanos(),
            |_, _| {},
        )
        .map(|_| ())
    }

    fn commit_batch_timed_with_parent_policy(
        &self,
        batch: &CommittedBatch<'_>,
        parent_policy: CommitBatchParentPolicy,
        mut activity: impl FnMut() -> (u64, bool),
        mut clock_ns: impl FnMut() -> u128,
        mut observe: impl FnMut(CommitBatchPhase, CommitPhaseTiming),
    ) -> Result<CommitBatchTiming> {
        let expected_records = batch
            .last_source_seq
            .checked_sub(batch.first_source_seq)
            .and_then(|span| span.checked_add(1))
            .and_then(|count| usize::try_from(count).ok());
        if expected_records != Some(batch.accepted_records)
            || batch.records.len() != batch.accepted_records
            || batch.records.iter().enumerate().any(|(offset, record)| {
                record.0
                    != batch
                        .first_source_seq
                        .saturating_add(i64::try_from(offset).unwrap_or(i64::MAX))
            })
        {
            bail!("committed ingest batch receipt bounds are inconsistent");
        }
        #[cfg(test)]
        if self
            .fail_next_batch_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            bail!("injected SQLite commit failure after WAL fsync");
        }
        let (connection, writer_wait) = timed_commit_phase(
            &mut activity,
            &mut clock_ns,
            || -> Result<MutexGuard<'_, Connection>> {
                #[cfg(test)]
                if self.take_timed_commit_failure(CommitBatchPhase::WriterWait) {
                    bail!("injected SQLite writer-wait phase failure");
                }
                self.writer()
            },
        );
        observe(CommitBatchPhase::WriterWait, writer_wait);
        let mut connection = connection?;
        let (transaction, transaction_begin) =
            timed_commit_phase(&mut activity, &mut clock_ns, || -> Result<_> {
                #[cfg(test)]
                if self.take_timed_commit_failure(CommitBatchPhase::TransactionBegin) {
                    bail!("injected SQLite transaction-begin phase failure");
                }
                connection.transaction().context("begin ingest transaction")
            });
        observe(CommitBatchPhase::TransactionBegin, transaction_begin);
        let transaction = transaction?;
        let (row_work, row_work_timing) = timed_commit_phase(
            &mut activity,
            &mut clock_ns,
            || -> Result<()> {
                #[cfg(test)]
                if self.take_timed_commit_failure(CommitBatchPhase::RowWork) {
                    bail!("injected SQLite row-work phase failure");
                }
                if matches!(parent_policy, CommitBatchParentPolicy::AuthenticatedLive) {
                    transaction.execute(
                        "INSERT INTO installations(id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms) \
                         VALUES (?1,?2,?3,?3,NULL) ON CONFLICT(id) DO UPDATE SET last_seen_unix_ms=excluded.last_seen_unix_ms \
                         WHERE installations.revoked_unix_ms IS NULL",
                        params![
                            batch.installation_id.to_string(),
                            batch.user_id.to_string(),
                            batch.received_unix_ms
                        ],
                    )?;
                } else {
                    let user_exists = transaction
                        .query_row(
                            "SELECT 1 FROM users WHERE id=?1",
                            [batch.user_id.to_string()],
                            |_| Ok(()),
                        )
                        .optional()?
                        .is_some();
                    if !user_exists {
                        bail!(
                            "WAL reconciliation parent user {} is missing",
                            batch.user_id
                        );
                    }
                }
                // A revocation or retirement racing after WAL fsync blocks future
                // authentication but must not make this already-durable batch
                // unrecoverable on restart. Reconciliation validates retained
                // ownership without changing any installation security metadata.
                let owner = transaction
                    .query_row(
                        "SELECT user_id FROM installations WHERE id=?1",
                        [batch.installation_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                let Some(owner) = owner else {
                    bail!(
                        "WAL reconciliation parent installation {} is missing",
                        batch.installation_id
                    );
                };
                if owner != batch.user_id.to_string() {
                    bail!("installation belongs to another user");
                }
                if matches!(parent_policy, CommitBatchParentPolicy::AuthenticatedLive) {
                    transaction.execute(
                        "UPDATE device_credentials SET last_used_unix_ms=?2 \
                         WHERE installation_id=?1 AND revoked_unix_ms IS NULL",
                        params![batch.installation_id.to_string(), batch.received_unix_ms],
                    )?;
                }
                transaction.execute(
            "INSERT INTO ingest_batches(user_id,installation_id,batch_id,checksum,first_source_seq,last_source_seq,accepted_records,received_unix_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                batch.user_id.to_string(),
                batch.installation_id.to_string(),
                batch.batch_id.to_string(),
                batch.checksum,
                batch.first_source_seq,
                batch.last_source_seq,
                u64::try_from(batch.accepted_records).unwrap_or(u64::MAX),
                batch.received_unix_ms,
            ],
            )?;
                for (source_seq, record_id, checksum) in batch.records {
                    transaction.execute(
                "INSERT INTO ingest_records(user_id,installation_id,source_seq,record_id,checksum,batch_id) \
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    batch.user_id.to_string(),
                    batch.installation_id.to_string(),
                    source_seq,
                    record_id.to_string(),
                    checksum,
                    batch.batch_id.to_string(),
                ],
                )?;
                }
                update_ingest_ack_state(
                    &transaction,
                    batch.user_id,
                    batch.installation_id,
                    batch.first_source_seq,
                    batch.last_source_seq,
                )?;
                Ok(())
            },
        );
        observe(CommitBatchPhase::RowWork, row_work_timing);
        row_work?;
        let (commit, commit_timing) =
            timed_commit_phase(&mut activity, &mut clock_ns, || -> Result<()> {
                #[cfg(test)]
                if self.take_timed_commit_failure(CommitBatchPhase::Commit) {
                    bail!("injected SQLite commit phase failure");
                }
                transaction
                    .commit()
                    .context("commit ingest receipt transaction")
            });
        observe(CommitBatchPhase::Commit, commit_timing);
        commit?;
        Ok(CommitBatchTiming {
            writer_wait,
            transaction_begin,
            row_work: row_work_timing,
            commit: commit_timing,
        })
    }

    pub fn contiguous_through(&self, user_id: Uuid, installation_id: Uuid) -> Result<i64> {
        Ok(self
            .writer()?
            .query_row(
                "SELECT contiguous_through FROM ingest_ack_state WHERE user_id=?1 AND installation_id=?2",
                params![user_id.to_string(), installation_id.to_string()],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(-1))
    }

    pub fn missing_ranges(
        &self,
        user_id: Uuid,
        installation_id: Uuid,
        through: i64,
    ) -> Result<Vec<(i64, i64)>> {
        if through < 0 {
            return Ok(Vec::new());
        }
        let connection = self.writer()?;
        let mut statement = connection.prepare(
            "SELECT start_seq,MIN(end_seq,?3) FROM ingest_missing_ranges \
             WHERE user_id=?1 AND installation_id=?2 AND start_seq<=?3 \
             ORDER BY start_seq LIMIT ?4",
        )?;
        statement
            .query_map(
                params![
                    user_id.to_string(),
                    installation_id.to_string(),
                    through,
                    i64::try_from(MAX_MISSING_RANGES).unwrap_or(i64::MAX)
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("query bounded ingest missing ranges")
    }

    pub fn issue_invitation(
        &self,
        email: &str,
        token: &str,
        admin_id: Uuid,
        now_ms: i64,
        expires_ms: i64,
    ) -> Result<Uuid> {
        let email = normalize_email(email)?;
        let id = Uuid::new_v4();
        self.writer()?.execute(
            "INSERT INTO invitations(id,email,token_hash,created_by,created_unix_ms,expires_unix_ms) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![id.to_string(), email, hash_secret(token), admin_id.to_string(), now_ms, expires_ms],
        )?;
        Ok(id)
    }

    pub fn list_invitations(&self) -> Result<Vec<InvitationSummary>> {
        let connection = self.request_reader()?;
        let mut statement = connection.prepare(
            "SELECT id,email,created_unix_ms,expires_unix_ms,consumed_unix_ms,revoked_unix_ms \
             FROM invitations ORDER BY created_unix_ms DESC",
        )?;
        statement
            .query_map([], |row| {
                Ok(InvitationSummary {
                    id: parse_uuid(row.get::<_, String>(0)?)?,
                    email: row.get(1)?,
                    created_unix_ms: row.get(2)?,
                    expires_unix_ms: row.get(3)?,
                    consumed_unix_ms: row.get(4)?,
                    revoked_unix_ms: row.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()
            .context("list invitations")
    }

    pub fn invitation_by_id(&self, invitation_id: Uuid) -> Result<Option<InvitationSummary>> {
        self.request_reader()?
            .query_row(
                "SELECT id,email,created_unix_ms,expires_unix_ms,consumed_unix_ms,revoked_unix_ms \
                 FROM invitations WHERE id=?1",
                [invitation_id.to_string()],
                |row| {
                    Ok(InvitationSummary {
                        id: parse_uuid(row.get::<_, String>(0)?)?,
                        email: row.get(1)?,
                        created_unix_ms: row.get(2)?,
                        expires_unix_ms: row.get(3)?,
                        consumed_unix_ms: row.get(4)?,
                        revoked_unix_ms: row.get(5)?,
                    })
                },
            )
            .optional()
            .context("query invitation by id")
    }

    pub fn revoke_invitation(&self, invitation_id: Uuid, now_ms: i64) -> Result<bool> {
        Ok(self.writer()?.execute(
            "UPDATE invitations SET revoked_unix_ms=?2 \
             WHERE id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL",
            params![invitation_id.to_string(), now_ms],
        )? == 1)
    }

    #[cfg(test)]
    pub fn valid_invitation(&self, token: &str, email: &str, now_ms: i64) -> Result<Option<Uuid>> {
        let email = normalize_email(email)?;
        self.request_reader()?
            .query_row(
                "SELECT id FROM invitations WHERE token_hash=?1 AND email=?2 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?3",
                params![hash_secret(token), email, now_ms],
                |row| parse_uuid(row.get::<_, String>(0)?),
            )
            .optional()
            .context("query valid invitation")
    }

    pub fn invitation_by_token(&self, token: &str, now_ms: i64) -> Result<Option<Invitation>> {
        self.request_reader()?
            .query_row(
                "SELECT id,email FROM invitations WHERE token_hash=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?2",
                params![hash_secret(token), now_ms],
                |row| {
                    Ok(Invitation {
                        id: parse_uuid(row.get::<_, String>(0)?)?,
                        email: row.get(1)?,
                    })
                },
            )
            .optional()
            .context("query invitation by token")
    }

    #[allow(dead_code)]
    pub fn consume_invitation_and_create_user(
        &self,
        registration: NewRegistration<'_>,
    ) -> Result<User> {
        let email = normalize_email(registration.email)?;
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let invitation_email: String = transaction
            .query_row(
                "SELECT email FROM invitations WHERE id=?1 AND consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL AND expires_unix_ms>?2",
                params![registration.invitation_id.to_string(), registration.now_ms],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow!("invitation is invalid, expired, or already used"))?;
        if invitation_email != email {
            bail!("invitation email mismatch");
        }
        let user_id = transaction
            .query_row("SELECT id FROM users WHERE email=?1", [&email], |row| {
                row.get::<_, String>(0)
            })
            .optional()?
            .map(|id| Uuid::parse_str(&id).context("parse user UUID"))
            .transpose()?
            .unwrap_or(registration.requested_user_id);
        transaction.execute(
            "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) VALUES (?1,?2,?3,0,0,?4) \
             ON CONFLICT(email) DO UPDATE SET display_name=excluded.display_name",
            params![user_id.to_string(), email, registration.display_name, registration.now_ms],
        )?;
        transaction.execute(
            "INSERT INTO passkeys(id,user_id,credential_id,credential_json,created_unix_ms) VALUES (?1,?2,?3,?4,?5)",
            params![
                Uuid::new_v4().to_string(),
                user_id.to_string(),
                registration.credential_id,
                registration.passkey_json,
                registration.now_ms
            ],
        )?;
        let updated = transaction.execute(
            "UPDATE invitations SET consumed_unix_ms=?2,consumed_by=?3 WHERE id=?1 AND consumed_unix_ms IS NULL",
            params![registration.invitation_id.to_string(), registration.now_ms, user_id.to_string()],
        )?;
        if updated != 1 {
            bail!("invitation was consumed concurrently");
        }
        let user = query_user_by_id(&transaction, user_id)?
            .ok_or_else(|| anyhow!("created user missing"))?;
        transaction.commit()?;
        Ok(user)
    }

    #[allow(dead_code)]
    pub fn passkeys_for_user(&self, user_id: Uuid) -> Result<Vec<String>> {
        let connection = self.request_reader()?;
        let mut statement = connection.prepare(
            "SELECT credential_json FROM passkeys WHERE user_id=?1 ORDER BY created_unix_ms",
        )?;
        let rows = statement.query_map([user_id.to_string()], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("load passkeys")
    }

    #[allow(dead_code)]
    pub fn replace_passkey(
        &self,
        user_id: Uuid,
        old_credential_json: &str,
        new_credential_json: &str,
    ) -> Result<bool> {
        Ok(self.writer()?.execute(
            "UPDATE passkeys SET credential_json=?3 WHERE user_id=?1 AND credential_json=?2",
            params![
                user_id.to_string(),
                old_credential_json,
                new_credential_json
            ],
        )? == 1)
    }

    #[allow(dead_code)]
    pub fn store_ceremony(&self, ceremony: NewCeremony<'_>) -> Result<()> {
        let email = normalize_email(ceremony.email)?;
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM auth_ceremonies WHERE expires_unix_ms<=?1",
            [ceremony.now_ms],
        )?;
        transaction.execute(
            "DELETE FROM auth_ceremony_rate_events WHERE created_unix_ms<=?1",
            [ceremony.now_ms.saturating_sub(CEREMONY_RATE_WINDOW_MS)],
        )?;
        let active_global: i64 =
            transaction.query_row("SELECT COUNT(*) FROM auth_ceremonies", [], |row| row.get(0))?;
        let active_email: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM auth_ceremonies WHERE email=?1",
            [&email],
            |row| row.get(0),
        )?;
        let starts_global: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM auth_ceremony_rate_events",
            [],
            |row| row.get(0),
        )?;
        let starts_email: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM auth_ceremony_rate_events WHERE email=?1",
            [&email],
            |row| row.get(0),
        )?;
        if active_global >= MAX_ACTIVE_CEREMONIES_GLOBAL
            || active_email >= MAX_ACTIVE_CEREMONIES_PER_EMAIL
            || starts_global >= MAX_CEREMONY_STARTS_GLOBAL_PER_WINDOW
            || starts_email >= MAX_CEREMONY_STARTS_PER_EMAIL_PER_WINDOW
        {
            bail!("authentication ceremony capacity or rate limit reached");
        }
        transaction.execute(
            "INSERT INTO auth_ceremony_rate_events(id,email,created_unix_ms) VALUES (?1,?2,?3)",
            params![Uuid::new_v4().to_string(), email, ceremony.now_ms],
        )?;
        transaction.execute(
            "INSERT INTO auth_ceremonies(challenge_id,kind,email,invitation_id,user_id,installation_id,state_json,created_unix_ms,expires_unix_ms) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![ceremony.challenge_id.to_string(), ceremony.kind, email, ceremony.invitation_id.map(|v|v.to_string()), ceremony.user_id.map(|v|v.to_string()), ceremony.installation_id.map(|v|v.to_string()), ceremony.state_json, ceremony.now_ms, ceremony.expires_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn ceremony_row_counts(&self) -> Result<(i64, i64)> {
        let connection = self.writer()?;
        Ok((
            connection.query_row("SELECT COUNT(*) FROM auth_ceremonies", [], |row| row.get(0))?,
            connection.query_row(
                "SELECT COUNT(*) FROM auth_ceremony_rate_events",
                [],
                |row| row.get(0),
            )?,
        ))
    }

    #[allow(dead_code)]
    pub fn take_ceremony(
        &self,
        challenge_id: Uuid,
        kind: &str,
        now_ms: i64,
    ) -> Result<Option<Ceremony>> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let ceremony = transaction
            .query_row(
                "SELECT email,invitation_id,user_id,installation_id,state_json FROM auth_ceremonies \
                 WHERE challenge_id=?1 AND kind=?2 AND expires_unix_ms>?3",
                params![challenge_id.to_string(), kind, now_ms],
                |row| {
                    Ok(Ceremony {
                        email: row.get(0)?,
                        invitation_id: optional_uuid(row.get(1)?)?,
                        user_id: optional_uuid(row.get(2)?)?,
                        installation_id: optional_uuid(row.get(3)?)?,
                        state_json: row.get(4)?,
                    })
                },
            )
            .optional()?;
        if ceremony.is_some() {
            transaction.execute(
                "DELETE FROM auth_ceremonies WHERE challenge_id=?1",
                [challenge_id.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(ceremony)
    }

    #[allow(dead_code)]
    pub fn create_session(&self, session: NewSession<'_>) -> Result<Uuid> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let id = insert_session(&transaction, &session)?;
        transaction.commit()?;
        Ok(id)
    }

    /// Read-only bootstrap check for the enrollment browser fallback page:
    /// reports whether the token is currently exchangeable, without touching
    /// its one-use state. Returns the token's expiry when valid.
    pub fn preview_enrollment(&self, token: &str, now_ms: i64) -> Result<Option<i64>> {
        let connection = self.request_reader()?;
        Ok(connection
            .query_row(
                "SELECT e.expires_unix_ms FROM enrollment_tokens e JOIN users u ON u.id=e.user_id \
                 WHERE e.token_hash=?1 AND e.consumed_unix_ms IS NULL AND e.revoked_unix_ms IS NULL \
                   AND e.expires_unix_ms>?2 AND u.disabled=0 AND u.retired_unix_ms IS NULL",
                params![hash_secret(token), now_ms],
                |row| row.get::<_, i64>(0),
            )
            .optional()?)
    }

    pub fn authenticate_access(&self, token: &str, now_ms: i64) -> Result<Option<SessionUser>> {
        let connection = self.request_reader()?;
        connection
            .query_row(
                "SELECT u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms,s.id,s.installation_id,s.csrf_hash \
                 FROM sessions s JOIN users u ON u.id=s.user_id \
                 LEFT JOIN installations i ON i.id=s.installation_id \
                 WHERE s.access_hash=?1 AND s.revoked_unix_ms IS NULL AND s.access_expires_unix_ms>?2 \
                   AND u.disabled=0 AND u.retired_unix_ms IS NULL \
                   AND (s.installation_id IS NULL OR (i.user_id=s.user_id AND i.revoked_unix_ms IS NULL))",
                params![hash_secret(token), now_ms],
                |row| {
                    Ok(SessionUser {
                        user: row_to_user(row)?,
                        session_id: parse_uuid(row.get::<_, String>(6)?)?,
                        installation_id: optional_uuid(row.get(7)?)?,
                        csrf_hash: row.get(8)?,
                    })
                },
            )
            .optional()
            .context("authenticate access token")
    }

    pub fn refresh_csrf_matches(
        &self,
        refresh_token: &str,
        csrf_token: &str,
        rotation_id: Uuid,
        now_ms: i64,
    ) -> Result<bool> {
        Ok(self
            .request_reader()?
            .query_row(
                "SELECT 1 FROM sessions s JOIN users u ON u.id=s.user_id \
             LEFT JOIN installations i ON i.id=s.installation_id \
             WHERE s.refresh_hash=?1 AND s.csrf_hash=?2 \
               AND s.refresh_expires_unix_ms>?3 AND u.disabled=0 AND u.retired_unix_ms IS NULL \
               AND (s.installation_id IS NULL OR i.revoked_unix_ms IS NULL) \
               AND (\
                 (s.revoked_unix_ms IS NULL AND s.refresh_used_unix_ms IS NULL) OR \
                 (s.refresh_used_unix_ms IS NOT NULL AND s.refresh_rotation_id=?4 AND EXISTS(\
                    SELECT 1 FROM sessions child WHERE child.parent_id=s.id \
                    AND child.revoked_unix_ms IS NULL AND child.refresh_expires_unix_ms>?3\
                 ))\
               )",
                params![
                    hash_secret(refresh_token),
                    hash_secret(csrf_token),
                    now_ms,
                    rotation_id.to_string()
                ],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn rotate_refresh(&self, rotation: RefreshRotation<'_>) -> Result<Option<RefreshResult>> {
        let rotation_id_text = rotation.rotation_id.to_string();
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let old = transaction
            .query_row(
                "SELECT s.id,u.id,u.email,u.display_name,u.is_admin,u.disabled,u.retired_unix_ms,s.installation_id,\
                        COALESCE(s.family_id,s.id),s.revoked_unix_ms,s.refresh_used_unix_ms,s.refresh_expires_unix_ms,\
                        s.refresh_rotation_id,s.rotation_access_expires_unix_ms,s.rotation_refresh_expires_unix_ms \
                 FROM sessions s JOIN users u ON u.id=s.user_id \
                 WHERE s.refresh_hash=?1",
                [hash_secret(rotation.old_refresh)],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        User {
                            id: parse_uuid(row.get::<_, String>(1)?)?,
                            email: row.get(2)?,
                            display_name: row.get(3)?,
                            is_admin: row.get(4)?,
                            disabled: row.get(5)?,
                            retired_unix_ms: row.get(6)?,
                        },
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, i64>(11)?,
                        row.get::<_, Option<String>>(12)?,
                        row.get::<_, Option<i64>>(13)?,
                        row.get::<_, Option<i64>>(14)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            old_id,
            user,
            installation,
            family_id,
            revoked,
            used,
            expires,
            stored_rotation_id,
            stored_access_expires,
            stored_refresh_expires,
        )) = old
        else {
            return Ok(None);
        };
        if used.is_some() && stored_rotation_id.as_deref() == Some(rotation_id_text.as_str()) {
            let child_active = transaction
                .query_row(
                    "SELECT 1 FROM sessions WHERE parent_id=?1 AND revoked_unix_ms IS NULL AND refresh_expires_unix_ms>?2",
                    params![old_id, rotation.now_ms],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if child_active
                && let (Some(access_expires_ms), Some(refresh_expires_ms)) =
                    (stored_access_expires, stored_refresh_expires)
            {
                transaction.commit()?;
                return Ok(Some(RefreshResult {
                    user,
                    access_expires_ms,
                    refresh_expires_ms,
                }));
            }
            transaction.commit()?;
            return Ok(None);
        }
        if revoked.is_some() || used.is_some() || expires <= rotation.now_ms || !user.is_active() {
            revoke_session_family_rows(&transaction, &family_id, rotation.now_ms)?;
            transaction.commit()?;
            return Ok(None);
        }
        transaction.execute(
            "UPDATE sessions SET revoked_unix_ms=?2,refresh_used_unix_ms=?2,refresh_rotation_id=?3,\
             rotation_access_expires_unix_ms=?4,rotation_refresh_expires_unix_ms=?5 WHERE id=?1",
            params![
                old_id,
                rotation.now_ms,
                rotation_id_text,
                rotation.access_expires_ms,
                rotation.refresh_expires_ms
            ],
        )?;
        let new_id = Uuid::new_v4().to_string();
        transaction.execute(
            "INSERT INTO sessions(id,user_id,installation_id,access_hash,refresh_hash,csrf_hash,family_id,parent_id,created_unix_ms,access_expires_unix_ms,refresh_expires_unix_ms,user_agent) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![new_id, user.id.to_string(), installation, hash_secret(rotation.access_token), hash_secret(rotation.refresh_token), hash_secret(rotation.csrf_token), family_id, old_id, rotation.now_ms, rotation.access_expires_ms, rotation.refresh_expires_ms, rotation.user_agent],
        )?;
        transaction.commit()?;
        Ok(Some(RefreshResult {
            user,
            access_expires_ms: rotation.access_expires_ms,
            refresh_expires_ms: rotation.refresh_expires_ms,
        }))
    }

    #[allow(dead_code)]
    pub fn revoke_session(&self, session_id: Uuid, now_ms: i64) -> Result<()> {
        self.writer()?.execute(
            "UPDATE sessions SET revoked_unix_ms=?2 WHERE id=?1",
            params![session_id.to_string(), now_ms],
        )?;
        Ok(())
    }

    pub fn revoke_session_family(&self, session_id: Uuid, now_ms: i64) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let family: Option<String> = transaction
            .query_row(
                "SELECT COALESCE(family_id,id) FROM sessions WHERE id=?1",
                [session_id.to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(family) = family else {
            transaction.commit()?;
            return Ok(false);
        };
        revoke_session_family_rows(&transaction, &family, now_ms)?;
        transaction.commit()?;
        Ok(true)
    }

    /// Root session id of the rotation family the given session belongs to:
    /// the stable "browser device" identity used by the devices view.
    pub fn session_family_root(&self, session_id: Uuid) -> Result<Option<Uuid>> {
        let family: Option<String> = self
            .request_reader()?
            .query_row(
                "SELECT COALESCE(family_id,id) FROM sessions WHERE id=?1",
                [session_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .context("look up session family root")?;
        Ok(family.map(parse_uuid).transpose()?)
    }

    /// User-scoped family revocation for the "Your devices" view: refuses
    /// (returns false, surfaced as 404) unless the family root identifies a
    /// browser session family belonging to `user_id`, then revokes the whole
    /// family including its unconsumed enrollment tokens.
    pub fn revoke_session_family_for_user(
        &self,
        family_root: Uuid,
        user_id: Uuid,
        now_ms: i64,
    ) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let owned = transaction
            .query_row(
                "SELECT 1 FROM sessions \
                 WHERE COALESCE(family_id,id)=?1 AND user_id=?2 AND installation_id IS NULL",
                params![family_root.to_string(), user_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !owned {
            transaction.commit()?;
            return Ok(false);
        }
        revoke_session_family_rows(&transaction, &family_root.to_string(), now_ms)?;
        transaction.commit()?;
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn hold_uncommitted_session_revocation(
        &self,
        session_id: Uuid,
        now_ms: i64,
        started: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let updated = transaction.execute(
            "UPDATE sessions SET revoked_unix_ms=?2 WHERE id=?1",
            params![session_id.to_string(), now_ms],
        )?;
        if updated != 1 {
            bail!("test session disappeared before uncommitted revocation");
        }
        started
            .send(())
            .map_err(|_| anyhow!("test revocation start receiver disappeared"))?;
        release
            .recv()
            .map_err(|_| anyhow!("test revocation release sender disappeared"))?;
        transaction
            .commit()
            .context("commit test session revocation")
    }

    #[cfg(test)]
    pub(crate) fn hold_checkpoint_connection(
        &self,
        started: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        let _checkpoint = self.checkpoint()?;
        started
            .send(())
            .map_err(|_| anyhow!("test checkpoint start receiver disappeared"))?;
        release
            .recv()
            .map_err(|_| anyhow!("test checkpoint release sender disappeared"))
    }

    pub fn revoke_refresh_family(&self, refresh_token: &str, now_ms: i64) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let family = transaction
            .query_row(
                "SELECT COALESCE(family_id,id) FROM sessions WHERE refresh_hash=?1",
                [hash_secret(refresh_token)],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(family) = family else {
            transaction.commit()?;
            return Ok(false);
        };
        revoke_session_family_rows(&transaction, &family, now_ms)?;
        transaction.commit()?;
        Ok(true)
    }

    /// User-scoped installation revocation for the "Your devices" view:
    /// refuses (returns false, surfaced as 404) unless the installation
    /// belongs to `user_id`, then revokes the installation and its bound
    /// sessions. `authenticate_device` requires an unrevoked installation, so
    /// the phone's next upload gets the established `device_reconnect_required`
    /// 401 while local collection continues.
    pub fn revoke_installation_for_user(
        &self,
        installation_id: Uuid,
        user_id: Uuid,
        now_ms: i64,
    ) -> Result<bool> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        let owned = transaction
            .query_row(
                "SELECT 1 FROM installations WHERE id=?1 AND user_id=?2",
                params![installation_id.to_string(), user_id.to_string()],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !owned {
            transaction.commit()?;
            return Ok(false);
        }
        let revoked = revoke_installation_rows(&transaction, installation_id, now_ms)?;
        transaction.commit()?;
        Ok(revoked)
    }

    pub fn wal_file_is_reconciled(
        &self,
        path: &str,
        size_bytes: u64,
        modified_unix_ns: i64,
    ) -> Result<bool> {
        Ok(self
            .writer()?
            .query_row(
                "SELECT 1 FROM wal_reconciliation_files \
                 WHERE path=?1 AND size_bytes=?2 AND modified_unix_ns=?3",
                params![path, size_bytes, modified_unix_ns],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn mark_wal_file_reconciled(
        &self,
        path: &str,
        size_bytes: u64,
        modified_unix_ns: i64,
        now_ms: i64,
    ) -> Result<()> {
        self.writer()?.execute(
            "INSERT INTO wal_reconciliation_files(path,size_bytes,modified_unix_ns,scanned_unix_ms) \
             VALUES (?1,?2,?3,?4) ON CONFLICT(path) DO UPDATE SET \
             size_bytes=excluded.size_bytes,modified_unix_ns=excluded.modified_unix_ns,scanned_unix_ms=excluded.scanned_unix_ms",
            params![path, size_bytes, modified_unix_ns, now_ms],
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub fn fail_next_batch_commit(&self) {
        self.fail_next_batch_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn fail_next_timed_commit_phase(&self, phase: CommitBatchPhase) {
        self.fail_next_timed_commit_phase.store(
            match phase {
                CommitBatchPhase::WriterWait => 1,
                CommitBatchPhase::TransactionBegin => 2,
                CommitBatchPhase::RowWork => 3,
                CommitBatchPhase::Commit => 4,
            },
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    #[cfg(test)]
    fn take_timed_commit_failure(&self, phase: CommitBatchPhase) -> bool {
        let expected = match phase {
            CommitBatchPhase::WriterWait => 1,
            CommitBatchPhase::TransactionBegin => 2,
            CommitBatchPhase::RowWork => 3,
            CommitBatchPhase::Commit => 4,
        };
        self.fail_next_timed_commit_phase
            .compare_exchange(
                expected,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }

    #[cfg(test)]
    pub fn fail_next_enrollment_exchange_before_commit(&self) {
        self.fail_next_enrollment_exchange_phase
            .store(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn fail_next_enrollment_exchange_after_commit(&self) {
        self.fail_next_enrollment_exchange_phase
            .store(2, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    fn take_enrollment_exchange_failure(&self, phase: u8) -> bool {
        self.fail_next_enrollment_exchange_phase
            .compare_exchange(
                phase,
                0,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .is_ok()
    }

    #[cfg(test)]
    pub fn delete_batch_receipts(
        &self,
        user_id: Uuid,
        installation_id: Uuid,
        batch_id: Uuid,
    ) -> Result<()> {
        let mut connection = self.writer()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "DELETE FROM ingest_records WHERE user_id=?1 AND installation_id=?2 AND batch_id=?3",
            params![
                user_id.to_string(),
                installation_id.to_string(),
                batch_id.to_string()
            ],
        )?;
        transaction.execute(
            "DELETE FROM ingest_batches WHERE user_id=?1 AND installation_id=?2 AND batch_id=?3",
            params![
                user_id.to_string(),
                installation_id.to_string(),
                batch_id.to_string()
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Ceremony {
    pub email: String,
    pub invitation_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub installation_id: Option<Uuid>,
    pub state_json: String,
}

fn apply_exact_retirement(
    transaction: &rusqlite::Transaction<'_>,
    user_id: Uuid,
    email: &str,
    reason: &str,
    retired: bool,
    now_ms: i64,
) -> Result<()> {
    if retired {
        transaction.execute(
            "UPDATE users SET disabled=1,retired_unix_ms=?2,retirement_reason=?3 WHERE id=?1",
            params![user_id.to_string(), now_ms, reason],
        )?;
        transaction.execute(
            "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE user_id=?1",
            params![user_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "UPDATE installations SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE user_id=?1",
            params![user_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "UPDATE device_credentials SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE installation_id IN (SELECT id FROM installations WHERE user_id=?1)",
            params![user_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "UPDATE invitations SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE email=?1 AND consumed_unix_ms IS NULL",
            params![email, now_ms],
        )?;
        transaction.execute(
            "UPDATE password_reset_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE user_id=?1 AND consumed_unix_ms IS NULL",
            params![user_id.to_string(), now_ms],
        )?;
        transaction.execute(
            "UPDATE enrollment_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
             WHERE user_id=?1 AND consumed_unix_ms IS NULL",
            params![user_id.to_string(), now_ms],
        )?;
    } else {
        transaction.execute(
            "UPDATE users SET disabled=0,retired_unix_ms=NULL,retirement_reason=NULL WHERE id=?1",
            [user_id.to_string()],
        )?;
    }
    Ok(())
}

fn insert_session(
    transaction: &rusqlite::Transaction<'_>,
    session: &NewSession<'_>,
) -> Result<Uuid> {
    let user = query_user_by_id(transaction, session.user_id)?
        .filter(User::is_active)
        .ok_or_else(|| anyhow!("session user is unavailable"))?;
    if user.id != session.user_id {
        bail!("session user mismatch");
    }
    if let Some(installation_id) = session.installation_id {
        transaction.execute(
            "INSERT INTO installations(id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms) \
             VALUES (?1,?2,?3,?3,NULL) ON CONFLICT(id) DO NOTHING",
            params![
                installation_id.to_string(),
                session.user_id.to_string(),
                session.now_ms
            ],
        )?;
        let owner_and_revoked: (String, Option<i64>) = transaction.query_row(
            "SELECT user_id,revoked_unix_ms FROM installations WHERE id=?1",
            [installation_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if owner_and_revoked.0 != session.user_id.to_string() {
            bail!("installation belongs to another user");
        }
        if owner_and_revoked.1.is_some() {
            bail!("installation is revoked");
        }
    }
    let id = Uuid::new_v4();
    transaction.execute(
        "INSERT INTO sessions(id,user_id,installation_id,access_hash,refresh_hash,csrf_hash,family_id,parent_id,created_unix_ms,access_expires_unix_ms,refresh_expires_unix_ms,user_agent) \
         VALUES (?1,?2,?3,?4,?5,?6,?1,NULL,?7,?8,?9,?10)",
        params![
            id.to_string(),
            session.user_id.to_string(),
            session.installation_id.map(|value| value.to_string()),
            hash_secret(session.access_token),
            hash_secret(session.refresh_token),
            hash_secret(session.csrf_token),
            session.now_ms,
            session.access_expires_ms,
            session.refresh_expires_ms,
            session.user_agent
        ],
    )?;
    Ok(id)
}

const INSTALLATION_SUMMARY_COLUMNS: &str = "i.id,i.user_id,i.display_label,i.android_manufacturer,i.android_model,\
     i.android_sdk,i.app_package,i.app_version_name,i.app_version_code,\
     i.created_unix_ms,i.enrolled_unix_ms,i.last_seen_unix_ms,i.revoked_unix_ms";

fn query_installation_summaries(
    connection: &Connection,
    sql: &str,
    sql_params: &[&dyn rusqlite::ToSql],
) -> Result<Vec<InstallationSummary>> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement
        .query_map(sql_params, |row| {
            Ok(InstallationSummary {
                id: parse_uuid(row.get::<_, String>(0)?)?,
                user_id: parse_uuid(row.get::<_, String>(1)?)?,
                display_label: row.get(2)?,
                android_manufacturer: row.get(3)?,
                android_model: row.get(4)?,
                android_sdk: row.get(5)?,
                app_package: row.get(6)?,
                app_version_name: row.get(7)?,
                app_version_code: row.get(8)?,
                created_unix_ms: row.get(9)?,
                enrolled_unix_ms: row.get(10)?,
                last_seen_unix_ms: row.get(11)?,
                revoked_unix_ms: row.get(12)?,
                credentials: Vec::new(),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    let mut installations = Vec::with_capacity(rows.len());
    for mut installation in rows {
        let mut credentials = connection.prepare(
            "SELECT id,created_unix_ms,last_used_unix_ms,revoked_unix_ms \
             FROM device_credentials WHERE installation_id=?1 ORDER BY created_unix_ms,id",
        )?;
        installation.credentials = credentials
            .query_map([installation.id.to_string()], |row| {
                Ok(DeviceCredentialSummary {
                    id: parse_uuid(row.get::<_, String>(0)?)?,
                    created_unix_ms: row.get(1)?,
                    last_used_unix_ms: row.get(2)?,
                    revoked_unix_ms: row.get(3)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        installations.push(installation);
    }
    Ok(installations)
}

/// Revokes every session row in one rotation family and cascades to the
/// family's unconsumed enrollment tokens. Shared by logout, refresh-reuse
/// containment, and the user-scoped "Your devices" revocation so the family
/// semantics cannot drift apart.
fn revoke_session_family_rows(
    transaction: &rusqlite::Transaction<'_>,
    family: &str,
    now_ms: i64,
) -> Result<()> {
    transaction.execute(
        "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
         WHERE COALESCE(family_id,id)=?1",
        params![family, now_ms],
    )?;
    transaction.execute(
        "UPDATE enrollment_tokens SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) \
         WHERE consumed_unix_ms IS NULL AND revoked_unix_ms IS NULL \
           AND browser_session_id IN (SELECT id FROM sessions WHERE COALESCE(family_id,id)=?1)",
        params![family, now_ms],
    )?;
    Ok(())
}

/// Revokes one installation and every session bound to it, cutting device
/// authentication so the phone's next upload gets `device_reconnect_required`.
fn revoke_installation_rows(
    transaction: &rusqlite::Transaction<'_>,
    installation_id: Uuid,
    now_ms: i64,
) -> Result<bool> {
    let updated = transaction.execute(
        "UPDATE installations SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE id=?1",
        params![installation_id.to_string(), now_ms],
    )? == 1;
    if updated {
        transaction.execute(
            "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,?2) WHERE installation_id=?1",
            params![installation_id.to_string(), now_ms],
        )?;
    }
    Ok(updated)
}

fn row_to_user(row: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    Ok(User {
        id: parse_uuid(row.get::<_, String>(0)?)?,
        email: row.get(1)?,
        display_name: row.get(2)?,
        is_admin: row.get(3)?,
        disabled: row.get(4)?,
        retired_unix_ms: row.get(5)?,
    })
}

fn parse_uuid(value: String) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(&value).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Text,
            Box::new(err),
        )
    })
}

fn optional_uuid(value: Option<String>) -> rusqlite::Result<Option<Uuid>> {
    value.map(parse_uuid).transpose()
}

fn query_user_by_id(connection: &Connection, id: Uuid) -> Result<Option<User>> {
    connection
        .query_row(
            "SELECT id,email,display_name,is_admin,disabled,retired_unix_ms FROM users WHERE id=?1",
            [id.to_string()],
            row_to_user,
        )
        .optional()
        .context("query user by id")
}

fn query_user_by_email(connection: &Connection, email: &str) -> Result<Option<User>> {
    connection
        .query_row(
            "SELECT id,email,display_name,is_admin,disabled,retired_unix_ms FROM users WHERE email=?1",
            [email],
            row_to_user,
        )
        .optional()
        .context("query user by email")
}

fn update_ingest_ack_state(
    transaction: &rusqlite::Transaction<'_>,
    user_uuid: Uuid,
    installation_uuid: Uuid,
    first_source_seq: i64,
    last_source_seq: i64,
) -> Result<()> {
    let user_id = user_uuid.to_string();
    let installation_id = installation_uuid.to_string();
    transaction.execute(
        "INSERT INTO ingest_ack_state(user_id,installation_id,contiguous_through,max_seen_seq) \
         VALUES (?1,?2,-1,-1) ON CONFLICT(user_id,installation_id) DO NOTHING",
        params![user_id, installation_id],
    )?;
    let (_, previous_max): (i64, i64) = transaction.query_row(
        "SELECT contiguous_through,max_seen_seq FROM ingest_ack_state \
         WHERE user_id=?1 AND installation_id=?2",
        params![user_id, installation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    if first_source_seq > previous_max.saturating_add(1) {
        transaction.execute(
            "INSERT INTO ingest_missing_ranges(user_id,installation_id,start_seq,end_seq) \
             VALUES (?1,?2,?3,?4)",
            params![
                user_id,
                installation_id,
                previous_max.saturating_add(1),
                first_source_seq.saturating_sub(1)
            ],
        )?;
    }

    let mut statement = transaction.prepare(
        "SELECT start_seq,end_seq FROM ingest_missing_ranges \
         WHERE user_id=?1 AND installation_id=?2 AND start_seq<=?4 AND end_seq>=?3 \
         ORDER BY start_seq",
    )?;
    let overlaps = statement
        .query_map(
            params![user_id, installation_id, first_source_seq, last_source_seq],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (start, end) in overlaps {
        transaction.execute(
            "DELETE FROM ingest_missing_ranges WHERE user_id=?1 AND installation_id=?2 AND start_seq=?3",
            params![user_id, installation_id, start],
        )?;
        if start < first_source_seq {
            transaction.execute(
                "INSERT INTO ingest_missing_ranges(user_id,installation_id,start_seq,end_seq) VALUES (?1,?2,?3,?4)",
                params![user_id, installation_id, start, first_source_seq.saturating_sub(1)],
            )?;
        }
        if end > last_source_seq {
            transaction.execute(
                "INSERT INTO ingest_missing_ranges(user_id,installation_id,start_seq,end_seq) VALUES (?1,?2,?3,?4)",
                params![user_id, installation_id, last_source_seq.saturating_add(1), end],
            )?;
        }
    }

    let max_seen = previous_max.max(last_source_seq);
    let first_gap = transaction
        .query_row(
            "SELECT start_seq FROM ingest_missing_ranges WHERE user_id=?1 AND installation_id=?2 ORDER BY start_seq LIMIT 1",
            params![user_id, installation_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    let contiguous = first_gap
        .map(|start| start.saturating_sub(1))
        .unwrap_or(max_seen);
    transaction.execute(
        "UPDATE ingest_ack_state SET contiguous_through=?3,max_seen_seq=?4 \
         WHERE user_id=?1 AND installation_id=?2",
        params![user_id, installation_id, contiguous, max_seen],
    )?;
    Ok(())
}

pub fn normalize_email(email: &str) -> Result<String> {
    let normalized = email.trim().to_ascii_lowercase();
    if normalized.len() > 254
        || normalized.starts_with('@')
        || normalized.ends_with('@')
        || normalized.matches('@').count() != 1
    {
        bail!("invalid email address");
    }
    Ok(normalized)
}

pub fn hash_secret(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS users(
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  is_admin INTEGER NOT NULL DEFAULT 0,
  disabled INTEGER NOT NULL DEFAULT 0,
  retired_unix_ms INTEGER,
  retirement_reason TEXT,
  created_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_migrations(
  version INTEGER PRIMARY KEY,
  applied_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS invitations(
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  token_hash TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_unix_ms INTEGER NOT NULL,
  expires_unix_ms INTEGER NOT NULL,
  consumed_unix_ms INTEGER,
  consumed_by TEXT REFERENCES users(id),
  revoked_unix_ms INTEGER
);
CREATE TABLE IF NOT EXISTS passkeys(
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  credential_id TEXT NOT NULL UNIQUE,
  credential_json TEXT NOT NULL,
  created_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_ceremonies(
  challenge_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  email TEXT NOT NULL,
  invitation_id TEXT,
  user_id TEXT,
  installation_id TEXT,
  state_json TEXT NOT NULL,
  created_unix_ms INTEGER NOT NULL,
  expires_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS auth_ceremony_rate_events(
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  created_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS password_credentials(
  user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE RESTRICT,
  password_hash TEXT NOT NULL,
  created_unix_ms INTEGER NOT NULL,
  changed_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS password_reset_tokens(
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  token_hash TEXT NOT NULL UNIQUE,
  created_unix_ms INTEGER NOT NULL,
  expires_unix_ms INTEGER NOT NULL,
  consumed_unix_ms INTEGER,
  revoked_unix_ms INTEGER
);
CREATE TABLE IF NOT EXISTS auth_rate_events(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  scope TEXT NOT NULL,
  bucket_hash TEXT NOT NULL,
  created_unix_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions(
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  installation_id TEXT,
  access_hash TEXT NOT NULL UNIQUE,
  refresh_hash TEXT NOT NULL UNIQUE,
  csrf_hash TEXT NOT NULL,
  family_id TEXT NOT NULL,
  parent_id TEXT,
  created_unix_ms INTEGER NOT NULL,
  access_expires_unix_ms INTEGER NOT NULL,
  refresh_expires_unix_ms INTEGER NOT NULL,
  revoked_unix_ms INTEGER,
  refresh_used_unix_ms INTEGER,
  refresh_rotation_id TEXT,
  rotation_access_expires_unix_ms INTEGER,
  rotation_refresh_expires_unix_ms INTEGER,
  user_agent TEXT
);
CREATE TABLE IF NOT EXISTS installations(
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  created_unix_ms INTEGER NOT NULL,
  last_seen_unix_ms INTEGER NOT NULL,
  revoked_unix_ms INTEGER,
  display_label TEXT,
  android_manufacturer TEXT,
  android_model TEXT,
  android_sdk INTEGER,
  app_package TEXT,
  app_version_name TEXT,
  app_version_code INTEGER,
  enrolled_unix_ms INTEGER
);
CREATE TABLE IF NOT EXISTS enrollment_tokens(
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  browser_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE RESTRICT,
  token_hash TEXT NOT NULL UNIQUE,
  created_unix_ms INTEGER NOT NULL,
  expires_unix_ms INTEGER NOT NULL,
  consumed_unix_ms INTEGER,
  revoked_unix_ms INTEGER,
  consumed_installation_id TEXT REFERENCES installations(id) ON DELETE RESTRICT,
  consumed_exchange_id TEXT,
  credential_id TEXT REFERENCES device_credentials(id) ON DELETE RESTRICT
);
CREATE TABLE IF NOT EXISTS device_credentials(
  id TEXT PRIMARY KEY,
  installation_id TEXT NOT NULL REFERENCES installations(id) ON DELETE RESTRICT,
  credential_hash TEXT NOT NULL UNIQUE,
  created_unix_ms INTEGER NOT NULL,
  last_used_unix_ms INTEGER,
  revoked_unix_ms INTEGER
);
CREATE TABLE IF NOT EXISTS ingest_batches(
  user_id TEXT NOT NULL REFERENCES users(id),
  installation_id TEXT NOT NULL REFERENCES installations(id),
  batch_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  first_source_seq INTEGER NOT NULL,
  last_source_seq INTEGER NOT NULL,
  accepted_records INTEGER NOT NULL,
  received_unix_ms INTEGER NOT NULL,
  PRIMARY KEY(user_id,installation_id,batch_id)
);
CREATE TABLE IF NOT EXISTS ingest_records(
  user_id TEXT NOT NULL REFERENCES users(id),
  installation_id TEXT NOT NULL REFERENCES installations(id),
  source_seq INTEGER NOT NULL,
  record_id TEXT NOT NULL,
  checksum TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  PRIMARY KEY(user_id,installation_id,source_seq),
  UNIQUE(user_id,record_id)
);
CREATE TABLE IF NOT EXISTS ingest_ack_state(
  user_id TEXT NOT NULL REFERENCES users(id),
  installation_id TEXT NOT NULL REFERENCES installations(id),
  contiguous_through INTEGER NOT NULL,
  max_seen_seq INTEGER NOT NULL,
  PRIMARY KEY(user_id,installation_id)
);
CREATE TABLE IF NOT EXISTS ingest_missing_ranges(
  user_id TEXT NOT NULL REFERENCES users(id),
  installation_id TEXT NOT NULL REFERENCES installations(id),
  start_seq INTEGER NOT NULL,
  end_seq INTEGER NOT NULL,
  PRIMARY KEY(user_id,installation_id,start_seq),
  CHECK(start_seq<=end_seq)
);
CREATE TABLE IF NOT EXISTS wal_reconciliation_files(
  path TEXT PRIMARY KEY,
  size_bytes INTEGER NOT NULL,
  modified_unix_ns INTEGER NOT NULL,
  scanned_unix_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_access ON sessions(access_hash);
CREATE INDEX IF NOT EXISTS idx_sessions_refresh ON sessions(refresh_hash);
CREATE INDEX IF NOT EXISTS idx_invites_token ON invitations(token_hash);
CREATE INDEX IF NOT EXISTS idx_auth_ceremonies_email_expiry ON auth_ceremonies(email,expires_unix_ms);
CREATE INDEX IF NOT EXISTS idx_auth_ceremony_rate_time ON auth_ceremony_rate_events(created_unix_ms);
CREATE INDEX IF NOT EXISTS idx_auth_ceremony_rate_email_time ON auth_ceremony_rate_events(email,created_unix_ms);
CREATE INDEX IF NOT EXISTS idx_password_reset_token ON password_reset_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_password_reset_user_active ON password_reset_tokens(user_id,expires_unix_ms,consumed_unix_ms,revoked_unix_ms);
CREATE INDEX IF NOT EXISTS idx_auth_rate_kind_time ON auth_rate_events(kind,created_unix_ms);
CREATE INDEX IF NOT EXISTS idx_enrollment_token ON enrollment_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_enrollment_user_active ON enrollment_tokens(user_id,expires_unix_ms,consumed_unix_ms,revoked_unix_ms);
CREATE INDEX IF NOT EXISTS idx_device_credentials_hash ON device_credentials(credential_hash);
CREATE INDEX IF NOT EXISTS idx_device_credentials_installation ON device_credentials(installation_id,created_unix_ms);
CREATE UNIQUE INDEX IF NOT EXISTS idx_device_credentials_one_active ON device_credentials(installation_id) WHERE revoked_unix_ms IS NULL;
CREATE INDEX IF NOT EXISTS idx_ingest_gaps ON ingest_missing_ranges(user_id,installation_id,start_seq);
"#;

fn apply_schema_migrations(connection: &mut Connection) -> Result<()> {
    let transaction = connection
        .transaction()
        .context("begin cloud schema migration")?;
    transaction
        .execute_batch(SCHEMA)
        .context("apply additive cloud DB schema")?;
    ensure_column(&transaction, "users", "retired_unix_ms", "INTEGER")?;
    ensure_column(&transaction, "users", "retirement_reason", "TEXT")?;
    ensure_column(&transaction, "invitations", "revoked_unix_ms", "INTEGER")?;
    ensure_column(&transaction, "sessions", "csrf_hash", "TEXT")?;
    ensure_column(&transaction, "sessions", "family_id", "TEXT")?;
    ensure_column(&transaction, "sessions", "parent_id", "TEXT")?;
    ensure_column(&transaction, "sessions", "refresh_used_unix_ms", "INTEGER")?;
    ensure_column(&transaction, "sessions", "refresh_rotation_id", "TEXT")?;
    ensure_column(
        &transaction,
        "sessions",
        "rotation_access_expires_unix_ms",
        "INTEGER",
    )?;
    ensure_column(
        &transaction,
        "sessions",
        "rotation_refresh_expires_unix_ms",
        "INTEGER",
    )?;
    ensure_column(&transaction, "sessions", "user_agent", "TEXT")?;
    ensure_column(&transaction, "installations", "revoked_unix_ms", "INTEGER")?;
    ensure_column(&transaction, "installations", "display_label", "TEXT")?;
    ensure_column(
        &transaction,
        "installations",
        "android_manufacturer",
        "TEXT",
    )?;
    ensure_column(&transaction, "installations", "android_model", "TEXT")?;
    ensure_column(&transaction, "installations", "android_sdk", "INTEGER")?;
    ensure_column(&transaction, "installations", "app_package", "TEXT")?;
    ensure_column(&transaction, "installations", "app_version_name", "TEXT")?;
    ensure_column(&transaction, "installations", "app_version_code", "INTEGER")?;
    ensure_column(&transaction, "installations", "enrolled_unix_ms", "INTEGER")?;
    ensure_column(
        &transaction,
        "auth_rate_events",
        "scope",
        "TEXT NOT NULL DEFAULT 'account'",
    )?;
    transaction.execute(
        "CREATE INDEX IF NOT EXISTS idx_auth_rate_bucket_time \
         ON auth_rate_events(kind,scope,bucket_hash,created_unix_ms)",
        [],
    )?;
    transaction.execute(
        "UPDATE sessions SET family_id=id WHERE family_id IS NULL OR family_id=''",
        [],
    )?;
    transaction.execute(
        "UPDATE sessions SET csrf_hash=?1 WHERE csrf_hash IS NULL OR csrf_hash=''",
        [hash_secret("legacy-session-requires-fresh-login")],
    )?;
    // Legacy Android access/refresh sessions are a different credential class.
    // Revoke them once at cutover; retained browser session families stay valid.
    transaction.execute(
        "UPDATE sessions SET revoked_unix_ms=COALESCE(revoked_unix_ms,CAST(strftime('%s','now') AS INTEGER)*1000) \
         WHERE installation_id IS NOT NULL",
        [],
    )?;
    transaction.execute(
        "INSERT OR IGNORE INTO schema_migrations(version,applied_unix_ms) \
         VALUES (?1,CAST(strftime('%s','now') AS INTEGER)*1000)",
        [CURRENT_SCHEMA_VERSION],
    )?;
    transaction
        .commit()
        .context("commit cloud schema migration")?;
    Ok(())
}

fn ensure_column(connection: &Connection, table: &str, column: &str, sql_type: &str) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|existing| existing == column) {
        connection.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {sql_type}"),
            [],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDbFile(PathBuf);

    impl TestDbFile {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "training-sensors-cloud-db-{}.sqlite3",
                Uuid::new_v4()
            )))
        }
    }

    impl Drop for TestDbFile {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut path = self.0.as_os_str().to_os_string();
                path.push(suffix);
                let _ = std::fs::remove_file(PathBuf::from(path));
            }
        }
    }

    fn db() -> CloudDb {
        CloudDb::open(Path::new(":memory:")).unwrap()
    }

    #[test]
    fn password_login_debug_redacts_the_stored_hash_and_user_identity() {
        let sentinel = "sentinel-password-hash-or-email";
        let debug = format!(
            "{:?}",
            PasswordLogin {
                user: User {
                    id: Uuid::nil(),
                    email: sentinel.to_string(),
                    display_name: sentinel.to_string(),
                    is_admin: false,
                    disabled: false,
                    retired_unix_ms: None,
                },
                password_hash: Some(sentinel.to_string()),
            }
        );
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(sentinel));
    }

    #[test]
    fn file_db_disables_writer_autocheckpoint_and_runs_dedicated_passive_checkpoint() {
        let path = TestDbFile::new();
        let db = CloudDb::open(&path.0).unwrap();
        {
            let writer = db.writer().unwrap();
            let journal: String = writer
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .unwrap();
            let synchronous: i64 = writer
                .pragma_query_value(None, "synchronous", |row| row.get(0))
                .unwrap();
            let autocheckpoint: i64 = writer
                .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
                .unwrap();
            assert_eq!(journal, "wal");
            assert_eq!(synchronous, 2);
            assert_eq!(autocheckpoint, 0);
        }
        {
            let checkpoint = db.checkpoint().unwrap();
            let journal: String = checkpoint
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .unwrap();
            let synchronous: i64 = checkpoint
                .pragma_query_value(None, "synchronous", |row| row.get(0))
                .unwrap();
            let foreign_keys: i64 = checkpoint
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .unwrap();
            let autocheckpoint: i64 = checkpoint
                .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
                .unwrap();
            assert_eq!(journal, "wal");
            assert_eq!(synchronous, 2);
            assert_eq!(foreign_keys, 1);
            assert_eq!(autocheckpoint, 0);
        }
        {
            let request = db.request_reader.as_ref().unwrap().lock().unwrap();
            let maintenance = db.maintenance_reader.as_ref().unwrap().lock().unwrap();
            for reader in [&*request, &*maintenance] {
                assert!(reader.is_readonly("main").unwrap());
                let query_only: i64 = reader
                    .pragma_query_value(None, "query_only", |row| row.get(0))
                    .unwrap();
                assert_eq!(query_only, 1);
            }
        }
        db.ensure_admin("admin@example.com", 1).unwrap();
        let result = db.passive_checkpoint().unwrap();
        assert!(!result.busy);
        assert!(result.log_frames >= 0);
        assert_eq!(result.checkpointed_frames, result.log_frames);
        assert!(result.wal_bytes > 0);
        assert!(result.checkpoint_operation_ns > 0);
        assert!(result.wal_stat_ns > 0);
    }

    #[test]
    fn refresh_expired_browser_families_disappear_from_the_devices_listing() {
        let path = TestDbFile::new();
        let db = CloudDb::open(&path.0).unwrap();
        let admin = db.ensure_admin("admin@example.com", 1).unwrap();
        db.create_session(NewSession {
            user_id: admin.id,
            installation_id: None,
            access_token: "expiring-access",
            refresh_token: "expiring-refresh",
            csrf_token: "expiring-csrf",
            user_agent: Some("Test UA"),
            now_ms: 1_000,
            access_expires_ms: 61_000,
            refresh_expires_ms: 121_000,
        })
        .unwrap();
        let live = db.list_browser_session_devices(admin.id, 120_999).unwrap();
        assert_eq!(live.len(), 1);
        // An expired-but-unrevoked family can no longer authenticate, so listing it
        // as a signed-in device would be a lie.
        let expired = db.list_browser_session_devices(admin.id, 121_000).unwrap();
        assert!(expired.is_empty());
    }

    #[test]
    fn passive_checkpoint_returns_promptly_while_real_writer_transaction_is_open() {
        let path = TestDbFile::new();
        let db = std::sync::Arc::new(CloudDb::open(&path.0).unwrap());
        let admin = db.ensure_admin("admin@example.com", 1).unwrap();
        let session_id = db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "checkpoint-writer-access",
                refresh_token: "checkpoint-writer-refresh",
                csrf_token: "checkpoint-writer-csrf",
                user_agent: None,
                now_ms: 2,
                access_expires_ms: 60_002,
                refresh_expires_ms: 120_002,
            })
            .unwrap();

        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let (watchdog_cancel_tx, watchdog_cancel_rx) = std::sync::mpsc::sync_channel(1);
        let watchdog_release = release_tx.clone();
        let watchdog = std::thread::spawn(move || {
            if watchdog_cancel_rx
                .recv_timeout(Duration::from_secs(2))
                .is_err()
            {
                let _ = watchdog_release.send(());
            }
        });
        let writer_db = db.clone();
        let writer = std::thread::spawn(move || {
            writer_db.hold_uncommitted_session_revocation(session_id, 3, started_tx, release_rx)
        });
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer transaction started");

        let started = std::time::Instant::now();
        let result = db.passive_checkpoint();
        let elapsed = started.elapsed();

        // Always release the writer and stop its watchdog before asserting.
        let _ = release_tx.send(());
        let _ = watchdog_cancel_tx.send(());
        watchdog.join().expect("join writer watchdog");
        writer
            .join()
            .expect("join writer transaction")
            .expect("commit writer transaction");
        let result = result.expect("PASSIVE checkpoint result");
        assert!(
            elapsed < Duration::from_millis(500),
            "PASSIVE checkpoint waited for writer transaction: {elapsed:?}"
        );
        assert!(result.log_frames >= 0);
        assert!(result.checkpointed_frames >= 0);
    }

    fn ceremony<'a>(
        challenge_id: Uuid,
        email: &'a str,
        installation_id: Option<Uuid>,
        now_ms: i64,
        expires_ms: i64,
    ) -> NewCeremony<'a> {
        NewCeremony {
            challenge_id,
            kind: "authentication",
            email,
            invitation_id: None,
            user_id: None,
            installation_id,
            state_json: "{}",
            now_ms,
            expires_ms,
        }
    }

    #[test]
    fn ceremonies_are_purged_bounded_and_rate_limited_independent_of_installation() {
        let db = db();
        for index in 0..MAX_ACTIVE_CEREMONIES_PER_EMAIL {
            db.store_ceremony(ceremony(
                Uuid::new_v4(),
                "Admin@Example.com",
                Some(Uuid::new_v4()),
                index,
                10,
            ))
            .unwrap();
        }
        assert!(
            db.store_ceremony(ceremony(
                Uuid::new_v4(),
                "admin@example.com",
                Some(Uuid::new_v4()),
                9,
                100,
            ))
            .is_err(),
            "varying attacker installation UUIDs must not bypass the normalized-email cap"
        );
        assert_eq!(
            db.ceremony_row_counts().unwrap(),
            (
                MAX_ACTIVE_CEREMONIES_PER_EMAIL,
                MAX_ACTIVE_CEREMONIES_PER_EMAIL
            )
        );

        db.store_ceremony(ceremony(Uuid::new_v4(), "admin@example.com", None, 11, 100))
            .unwrap();
        assert_eq!(db.ceremony_row_counts().unwrap().0, 1);

        for index in 0..MAX_CEREMONY_STARTS_PER_EMAIL_PER_WINDOW {
            let id = Uuid::new_v4();
            db.store_ceremony(ceremony(
                id,
                "normal@example.com",
                Some(Uuid::new_v4()),
                100 + index,
                10_000,
            ))
            .unwrap();
            assert!(
                db.take_ceremony(id, "authentication", 100 + index)
                    .unwrap()
                    .is_some()
            );
        }
        assert!(
            db.store_ceremony(ceremony(
                Uuid::new_v4(),
                "normal@example.com",
                Some(Uuid::new_v4()),
                200,
                10_000,
            ))
            .is_err(),
            "deleting/finishing challenges must not bypass the start-rate limit"
        );
        let (active, rate_rows) = db.ceremony_row_counts().unwrap();
        assert!(active <= MAX_ACTIVE_CEREMONIES_GLOBAL);
        assert!(rate_rows <= MAX_CEREMONY_STARTS_GLOBAL_PER_WINDOW);

        db.store_ceremony(ceremony(
            Uuid::new_v4(),
            "normal@example.com",
            None,
            CEREMONY_RATE_WINDOW_MS + 201,
            CEREMONY_RATE_WINDOW_MS + 1_000,
        ))
        .unwrap();
        let (_, rate_rows_after_window) = db.ceremony_row_counts().unwrap();
        assert_eq!(rate_rows_after_window, 1);
    }

    #[test]
    fn invitation_is_exact_email_single_use_and_consumed_only_on_finish() {
        let db = db();
        let admin = db.ensure_admin("Admin@Example.com", 1).unwrap();
        let invite = db
            .issue_invitation("Person@Example.com", "secret", admin.id, 2, 100)
            .unwrap();
        assert_eq!(
            db.valid_invitation("secret", "person@example.com", 3)
                .unwrap(),
            Some(invite)
        );
        assert!(
            db.valid_invitation("secret", "other@example.com", 3)
                .unwrap()
                .is_none()
        );
        // Merely validating/options-start does not consume it.
        assert!(
            db.valid_invitation("secret", "person@example.com", 4)
                .unwrap()
                .is_some()
        );
        db.consume_invitation_and_create_user(NewRegistration {
            invitation_id: invite,
            requested_user_id: Uuid::new_v4(),
            email: "person@example.com",
            display_name: "Person",
            credential_id: "abc",
            passkey_json: r#"{"cred":{"cred_id":"abc"}}"#,
            now_ms: 5,
        })
        .unwrap();
        assert!(
            db.valid_invitation("secret", "person@example.com", 6)
                .unwrap()
                .is_none()
        );
        assert!(
            db.consume_invitation_and_create_user(NewRegistration {
                invitation_id: invite,
                requested_user_id: Uuid::new_v4(),
                email: "person@example.com",
                display_name: "Person",
                credential_id: "def",
                passkey_json: "{}",
                now_ms: 7,
            })
            .is_err()
        );
    }

    #[test]
    fn sessions_are_revocable_and_refresh_is_single_use() {
        let db = db();
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let session = db
            .create_session(NewSession {
                user_id: user.id,
                installation_id: None,
                access_token: "access",
                refresh_token: "refresh",
                csrf_token: "csrf",
                user_agent: None,
                now_ms: 2,
                access_expires_ms: 10,
                refresh_expires_ms: 20,
            })
            .unwrap();
        assert_eq!(
            db.authenticate_access("access", 3).unwrap().unwrap().user,
            user
        );
        db.revoke_session(session, 4).unwrap();
        assert!(db.authenticate_access("access", 5).unwrap().is_none());

        db.create_session(NewSession {
            user_id: user.id,
            installation_id: None,
            access_token: "a2",
            refresh_token: "r2",
            csrf_token: "c2",
            user_agent: None,
            now_ms: 5,
            access_expires_ms: 10,
            refresh_expires_ms: 20,
        })
        .unwrap();
        let rotation_id = Uuid::new_v4();
        let first = db
            .rotate_refresh(RefreshRotation {
                old_refresh: "r2",
                rotation_id,
                access_token: "a3",
                refresh_token: "r3",
                csrf_token: "c3",
                user_agent: None,
                now_ms: 6,
                access_expires_ms: 11,
                refresh_expires_ms: 30,
            })
            .unwrap()
            .unwrap();
        let retry = db
            .rotate_refresh(RefreshRotation {
                old_refresh: "r2",
                rotation_id,
                access_token: "a4",
                refresh_token: "r4",
                csrf_token: "c4",
                user_agent: None,
                now_ms: 7,
                access_expires_ms: 12,
                refresh_expires_ms: 31,
            })
            .unwrap()
            .unwrap();
        assert_eq!(retry.access_expires_ms, first.access_expires_ms);
        assert!(db.authenticate_access("a3", 8).unwrap().is_some());
        assert!(
            db.rotate_refresh(RefreshRotation {
                old_refresh: "r2",
                rotation_id: Uuid::new_v4(),
                access_token: "a5",
                refresh_token: "r5",
                csrf_token: "c5",
                user_agent: None,
                now_ms: 9,
                access_expires_ms: 14,
                refresh_expires_ms: 32,
            })
            .unwrap()
            .is_none()
        );
        assert!(db.authenticate_access("a3", 8).unwrap().is_none());

        let connection = db.writer().unwrap();
        let columns = connection
            .prepare("PRAGMA table_info(sessions)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            !columns
                .iter()
                .any(|column| column.contains("rotation_access_token"))
        );
        let stored: (String, String, String) = connection
            .query_row(
                "SELECT access_hash,refresh_hash,csrf_hash FROM sessions WHERE parent_id IS NOT NULL LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_ne!(stored.0, "a3");
        assert_ne!(stored.1, "r3");
        assert_ne!(stored.2, "c3");
    }

    #[test]
    fn contiguous_ack_and_missing_ranges_follow_durable_receipts() {
        let db = db();
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let installation = Uuid::new_v4();
        let batch_id = Uuid::new_v4();
        let records = vec![(2, Uuid::new_v4(), "x".to_string())];
        db.commit_batch(&CommittedBatch {
            user_id: user.id,
            installation_id: installation,
            batch_id,
            checksum: "batch",
            first_source_seq: 2,
            last_source_seq: 2,
            accepted_records: 1,
            received_unix_ms: 2,
            records: &records,
        })
        .unwrap();
        assert_eq!(db.contiguous_through(user.id, installation).unwrap(), -1);
        assert_eq!(
            db.missing_ranges(user.id, installation, 2).unwrap(),
            vec![(0, 1)]
        );
    }

    #[test]
    fn commit_timing_brackets_each_sqlite_phase_with_activity_samples() {
        let db = db();
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let installation = Uuid::new_v4();
        let records = vec![(0, Uuid::new_v4(), "record".to_string())];
        let mut sample = 0_u64;
        let mut now_ns = 0_u128;
        let mut observed = Vec::new();
        let timing = db
            .commit_batch_timed(
                &CommittedBatch {
                    user_id: user.id,
                    installation_id: installation,
                    batch_id: Uuid::new_v4(),
                    checksum: "batch",
                    first_source_seq: 0,
                    last_source_seq: 0,
                    accepted_records: 1,
                    received_unix_ms: 2,
                    records: &records,
                },
                || {
                    sample = sample.saturating_add(1);
                    (sample, false)
                },
                || {
                    let value = now_ns;
                    now_ns = now_ns.saturating_add(100);
                    value
                },
                |phase, timing| observed.push((phase, timing)),
            )
            .unwrap();
        assert_eq!(sample, 8, "each of four phases needs before/after samples");
        assert!(timing.writer_wait.checkpoint_overlap);
        assert!(timing.transaction_begin.checkpoint_overlap);
        assert!(timing.row_work.checkpoint_overlap);
        assert!(timing.commit.checkpoint_overlap);
        assert_eq!(timing.writer_wait.elapsed_ns, 100);
        assert_eq!(timing.transaction_begin.elapsed_ns, 100);
        assert_eq!(timing.row_work.elapsed_ns, 100);
        assert_eq!(timing.commit.elapsed_ns, 100);
        assert_eq!(observed.len(), 4);
    }

    #[test]
    fn commit_timing_observes_the_exact_phase_that_fails() {
        for (index, failing_phase) in [
            CommitBatchPhase::WriterWait,
            CommitBatchPhase::TransactionBegin,
            CommitBatchPhase::RowWork,
            CommitBatchPhase::Commit,
        ]
        .into_iter()
        .enumerate()
        {
            let db = db();
            let user = db.ensure_admin("admin@example.com", 1).unwrap();
            let records = vec![(0, Uuid::new_v4(), "record".to_string())];
            db.fail_next_timed_commit_phase(failing_phase);
            let mut now_ns = 0_u128;
            let mut observed = Vec::new();
            let result = db.commit_batch_timed(
                &CommittedBatch {
                    user_id: user.id,
                    installation_id: Uuid::new_v4(),
                    batch_id: Uuid::new_v4(),
                    checksum: "batch",
                    first_source_seq: 0,
                    last_source_seq: 0,
                    accepted_records: 1,
                    received_unix_ms: 2,
                    records: &records,
                },
                || (0, false),
                || {
                    let value = now_ns;
                    now_ns = now_ns.saturating_add(250);
                    value
                },
                |phase, timing| observed.push((phase, timing)),
            );
            assert!(result.is_err());
            assert_eq!(observed.len(), index + 1);
            assert_eq!(observed.last().map(|item| item.0), Some(failing_phase));
            assert!(observed.iter().all(|item| item.1.elapsed_ns == 250));
        }
    }

    #[test]
    fn million_sequence_ack_uses_persisted_watermark_and_bounded_gaps() {
        let db = db();
        let user = db.ensure_admin("admin@example.com", 1).unwrap();
        let installation = Uuid::new_v4();
        let records = vec![(1_000_000, Uuid::new_v4(), "x".to_string())];
        db.commit_batch(&CommittedBatch {
            user_id: user.id,
            installation_id: installation,
            batch_id: Uuid::new_v4(),
            checksum: "late",
            first_source_seq: 1_000_000,
            last_source_seq: 1_000_000,
            accepted_records: 1,
            received_unix_ms: 2,
            records: &records,
        })
        .unwrap();
        assert_eq!(db.contiguous_through(user.id, installation).unwrap(), -1);
        assert_eq!(
            db.missing_ranges(user.id, installation, 1_000_000).unwrap(),
            vec![(0, 999_999)]
        );

        // Exercise the interval state transition directly: its work is
        // independent of the million historical receipt rows it represents.
        let mut connection = db.writer().unwrap();
        let transaction = connection.transaction().unwrap();
        update_ingest_ack_state(&transaction, user.id, installation, 0, 999_999).unwrap();
        transaction.commit().unwrap();
        drop(connection);
        assert_eq!(
            db.contiguous_through(user.id, installation).unwrap(),
            1_000_000
        );
        assert!(
            db.missing_ranges(user.id, installation, 1_000_000)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn production_one_user_one_installation_migrates_transactionally_and_idempotently() {
        let path = TestDbFile::new();
        let user_id = Uuid::new_v4();
        let installation_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        {
            let connection = Connection::open(&path.0).unwrap();
            connection
                .execute_batch(
                    r#"
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE users(
                      id TEXT PRIMARY KEY,email TEXT NOT NULL UNIQUE,display_name TEXT NOT NULL,
                      is_admin INTEGER NOT NULL DEFAULT 0,disabled INTEGER NOT NULL DEFAULT 0,
                      created_unix_ms INTEGER NOT NULL
                    );
                    CREATE TABLE installations(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),
                      created_unix_ms INTEGER NOT NULL,last_seen_unix_ms INTEGER NOT NULL,
                      revoked_unix_ms INTEGER
                    );
                    CREATE TABLE sessions(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),installation_id TEXT,
                      access_hash TEXT NOT NULL UNIQUE,refresh_hash TEXT NOT NULL UNIQUE,csrf_hash TEXT NOT NULL,
                      family_id TEXT NOT NULL,parent_id TEXT,created_unix_ms INTEGER NOT NULL,
                      access_expires_unix_ms INTEGER NOT NULL,refresh_expires_unix_ms INTEGER NOT NULL,
                      revoked_unix_ms INTEGER,refresh_used_unix_ms INTEGER,refresh_rotation_id TEXT,
                      rotation_access_expires_unix_ms INTEGER,rotation_refresh_expires_unix_ms INTEGER
                    );
                    CREATE TABLE passkeys(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),
                      credential_id TEXT NOT NULL UNIQUE,credential_json TEXT NOT NULL,
                      created_unix_ms INTEGER NOT NULL
                    );
                    CREATE TABLE auth_rate_events(
                      id TEXT PRIMARY KEY,kind TEXT NOT NULL,bucket_hash TEXT NOT NULL,
                      created_unix_ms INTEGER NOT NULL
                    );
                    "#,
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) \
                     VALUES (?1,'alice@example.com','Alice',1,0,1)",
                    [user_id.to_string()],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO installations(id,user_id,created_unix_ms,last_seen_unix_ms) \
                     VALUES (?1,?2,1,1)",
                    params![installation_id.to_string(), user_id.to_string()],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO sessions(id,user_id,installation_id,access_hash,refresh_hash,csrf_hash,
                        family_id,created_unix_ms,access_expires_unix_ms,refresh_expires_unix_ms) \
                     VALUES (?1,?2,?3,'legacy-access','legacy-refresh','legacy-csrf',?1,1,999,999)",
                    params![
                        session_id.to_string(),
                        user_id.to_string(),
                        installation_id.to_string()
                    ],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO passkeys(id,user_id,credential_id,credential_json,created_unix_ms) \
                     VALUES (?1,?2,'legacy-credential','{}',1)",
                    params![Uuid::new_v4().to_string(), user_id.to_string()],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO auth_rate_events(id,kind,bucket_hash,created_unix_ms) \
                     VALUES (?1,'legacy','legacy-bucket',1)",
                    [Uuid::new_v4().to_string()],
                )
                .unwrap();
        }

        for attempt in 0..2 {
            let db = CloudDb::open(&path.0).unwrap();
            let alice = db.user_by_id(user_id).unwrap().unwrap();
            assert_eq!(alice.email, "alice@example.com");
            assert_eq!(alice.retired_unix_ms, None);
            let connection = db.writer().unwrap();
            let users: i64 = connection
                .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
                .unwrap();
            let installations: i64 = connection
                .query_row("SELECT COUNT(*) FROM installations", [], |row| row.get(0))
                .unwrap();
            let owner: String = connection
                .query_row(
                    "SELECT user_id FROM installations WHERE id=?1",
                    [installation_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!((users, installations), (1, 1));
            assert_eq!(owner, user_id.to_string());
            let user_shape: (String, String, String, i64, i64, i64) = connection
                .query_row(
                    "SELECT id,email,display_name,is_admin,disabled,created_unix_ms FROM users WHERE id=?1",
                    [user_id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(
                user_shape,
                (
                    user_id.to_string(),
                    "alice@example.com".to_string(),
                    "Alice".to_string(),
                    1,
                    0,
                    1,
                )
            );
            let installation_shape: (String, String, i64, i64, Option<i64>) = connection
                .query_row(
                    "SELECT id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms \
                     FROM installations WHERE id=?1",
                    [installation_id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .unwrap();
            assert_eq!(
                installation_shape,
                (installation_id.to_string(), user_id.to_string(), 1, 1, None,)
            );
            let revoked: Option<i64> = connection
                .query_row(
                    "SELECT revoked_unix_ms FROM sessions WHERE id=?1",
                    [session_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(revoked.is_some());
            let passkeys: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM passkeys WHERE user_id=?1 AND credential_id='legacy-credential'",
                    [user_id.to_string()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(passkeys, 1);
            let scope: String = connection
                .query_row(
                    "SELECT scope FROM auth_rate_events WHERE kind='legacy'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(scope, "account");
            let migrations: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version=2",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(migrations, 1, "migration attempt {attempt}");
            drop(connection);
            assert_eq!(db.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
            db.quick_check().unwrap();
        }
    }

    #[test]
    fn historical_eleven_user_thirty_three_installation_fixture_migrates_without_identity_loss() {
        let path = TestDbFile::new();
        let mut expected_users = Vec::new();
        let mut expected_installations = Vec::new();
        {
            let connection = Connection::open(&path.0).unwrap();
            connection
                .execute_batch(
                    r#"
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE users(
                      id TEXT PRIMARY KEY,email TEXT NOT NULL UNIQUE,display_name TEXT NOT NULL,
                      is_admin INTEGER NOT NULL DEFAULT 0,disabled INTEGER NOT NULL DEFAULT 0,
                      created_unix_ms INTEGER NOT NULL
                    );
                    CREATE TABLE installations(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),
                      created_unix_ms INTEGER NOT NULL,last_seen_unix_ms INTEGER NOT NULL,
                      revoked_unix_ms INTEGER
                    );
                    CREATE TABLE sessions(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),installation_id TEXT,
                      access_hash TEXT NOT NULL UNIQUE,refresh_hash TEXT NOT NULL UNIQUE,csrf_hash TEXT NOT NULL,
                      family_id TEXT NOT NULL,parent_id TEXT,created_unix_ms INTEGER NOT NULL,
                      access_expires_unix_ms INTEGER NOT NULL,refresh_expires_unix_ms INTEGER NOT NULL,
                      revoked_unix_ms INTEGER,refresh_used_unix_ms INTEGER,refresh_rotation_id TEXT,
                      rotation_access_expires_unix_ms INTEGER,rotation_refresh_expires_unix_ms INTEGER
                    );
                    CREATE TABLE passkeys(
                      id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),
                      credential_id TEXT NOT NULL UNIQUE,credential_json TEXT NOT NULL,
                      created_unix_ms INTEGER NOT NULL
                    );
                    CREATE TABLE auth_rate_events(
                      id TEXT PRIMARY KEY,kind TEXT NOT NULL,bucket_hash TEXT NOT NULL,
                      created_unix_ms INTEGER NOT NULL
                    );
                    "#,
                )
                .unwrap();
            for user_index in 0..11 {
                let user_id = Uuid::new_v4();
                let email = if user_index == 0 {
                    "alice@example.com".to_string()
                } else {
                    format!("historical-{user_index}@example.com")
                };
                let display_name = if user_index == 0 {
                    "Alice".to_string()
                } else {
                    format!("Historical {user_index}")
                };
                connection
                    .execute(
                        "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) \
                         VALUES (?1,?2,?3,?4,0,1)",
                        params![
                            user_id.to_string(),
                            email,
                            display_name,
                            i64::from(user_index == 0)
                        ],
                    )
                    .unwrap();
                expected_users.push((
                    user_id.to_string(),
                    email,
                    display_name,
                    i64::from(user_index == 0),
                    0_i64,
                ));
                let mut first_installation = None;
                for _ in 0..3 {
                    let installation_id = Uuid::new_v4();
                    first_installation.get_or_insert(installation_id);
                    connection
                        .execute(
                            "INSERT INTO installations(id,user_id,created_unix_ms,last_seen_unix_ms) \
                             VALUES (?1,?2,1,1)",
                            params![installation_id.to_string(), user_id.to_string()],
                        )
                        .unwrap();
                    expected_installations.push((
                        installation_id.to_string(),
                        user_id.to_string(),
                        1_i64,
                        1_i64,
                        None::<i64>,
                    ));
                }
                let session_id = Uuid::new_v4();
                connection
                    .execute(
                        "INSERT INTO sessions(id,user_id,installation_id,access_hash,refresh_hash,csrf_hash,
                            family_id,created_unix_ms,access_expires_unix_ms,refresh_expires_unix_ms) \
                         VALUES (?1,?2,?3,?4,?5,?6,?1,1,999,999)",
                        params![
                            session_id.to_string(),
                            user_id.to_string(),
                            first_installation.unwrap().to_string(),
                            format!("legacy-access-{user_index}"),
                            format!("legacy-refresh-{user_index}"),
                            format!("legacy-csrf-{user_index}")
                        ],
                    )
                    .unwrap();
                connection
                    .execute(
                        "INSERT INTO passkeys(id,user_id,credential_id,credential_json,created_unix_ms) \
                         VALUES (?1,?2,?3,'{}',1)",
                        params![
                            Uuid::new_v4().to_string(),
                            user_id.to_string(),
                            format!("legacy-credential-{user_index}")
                        ],
                    )
                    .unwrap();
            }
            connection
                .execute(
                    "INSERT INTO auth_rate_events(id,kind,bucket_hash,created_unix_ms) \
                     VALUES (?1,'legacy','legacy-bucket',1)",
                    [Uuid::new_v4().to_string()],
                )
                .unwrap();
        }
        expected_users.sort();
        expected_installations.sort();

        for attempt in 0..2 {
            let db = CloudDb::open(&path.0).unwrap();
            let connection = db.writer().unwrap();
            let mut users = connection
                .prepare("SELECT id,email,display_name,is_admin,disabled FROM users")
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<(String, String, String, i64, i64)>>>()
                .unwrap();
            users.sort();
            let mut installations = connection
                .prepare(
                    "SELECT id,user_id,created_unix_ms,last_seen_unix_ms,revoked_unix_ms \
                     FROM installations",
                )
                .unwrap()
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<(String, String, i64, i64, Option<i64>)>>>()
                .unwrap();
            installations.sort();
            assert_eq!(users, expected_users, "migration attempt {attempt}");
            assert_eq!(
                installations, expected_installations,
                "migration attempt {attempt}"
            );
            let revoked_sessions: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sessions WHERE revoked_unix_ms IS NOT NULL",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let passkeys: i64 = connection
                .query_row("SELECT COUNT(*) FROM passkeys", [], |row| row.get(0))
                .unwrap();
            let migrations: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version=?1",
                    [CURRENT_SCHEMA_VERSION],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!((users.len(), installations.len()), (11, 33));
            assert_eq!((revoked_sessions, passkeys, migrations), (11, 11, 1));
            drop(connection);
            assert_eq!(db.schema_version().unwrap(), CURRENT_SCHEMA_VERSION);
            db.quick_check().unwrap();
        }
    }

    #[test]
    fn exact_ten_user_retirement_is_atomic_reversible_and_never_touches_alice_or_data() {
        fn alice_row(
            db: &CloudDb,
            user_id: Uuid,
        ) -> (
            String,
            String,
            String,
            bool,
            bool,
            Option<i64>,
            Option<String>,
            i64,
        ) {
            db.writer()
                .unwrap()
                .query_row(
                    "SELECT id,email,display_name,is_admin,disabled,retired_unix_ms,retirement_reason,created_unix_ms \
                     FROM users WHERE id=?1",
                    [user_id.to_string()],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                        ))
                    },
                )
                .unwrap()
        }

        fn alice_data_counts(db: &CloudDb, user_id: Uuid) -> (i64, i64, i64, i64) {
            let connection = db.writer().unwrap();
            let count = |table: &str| {
                connection
                    .query_row(
                        &format!("SELECT COUNT(*) FROM {table} WHERE user_id=?1"),
                        [user_id.to_string()],
                        |row| row.get(0),
                    )
                    .unwrap()
            };
            (
                count("ingest_batches"),
                count("ingest_records"),
                count("ingest_ack_state"),
                count("installations"),
            )
        }

        let db = db();
        let alice = db.ensure_admin("alice@example.com", 1).unwrap();
        let alice_installation = Uuid::new_v4();
        db.insert_test_device_credential(
            alice.id,
            alice_installation,
            Uuid::new_v4(),
            "alice-protected-device-credential",
            2,
        )
        .unwrap();
        let alice_batch = Uuid::new_v4();
        let alice_records = vec![(0, Uuid::new_v4(), "alice-record".to_string())];
        db.commit_batch(&CommittedBatch {
            user_id: alice.id,
            installation_id: alice_installation,
            batch_id: alice_batch,
            checksum: "alice-batch",
            first_source_seq: 0,
            last_source_seq: 0,
            accepted_records: 1,
            received_unix_ms: 3,
            records: &alice_records,
        })
        .unwrap();

        let mut targets = Vec::new();
        {
            let connection = db.writer().unwrap();
            for index in 0..10 {
                let user_id = Uuid::new_v4();
                let email = format!("synthetic-{index}@example.com");
                connection
                    .execute(
                        "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) \
                         VALUES (?1,?2,?3,0,0,?4)",
                        params![
                            user_id.to_string(),
                            email,
                            format!("Synthetic {index}"),
                            10 + index
                        ],
                    )
                    .unwrap();
                targets.push((user_id, email));
            }
        }
        for (index, (user_id, _)) in targets.iter().enumerate() {
            db.insert_test_device_credential(
                *user_id,
                Uuid::new_v4(),
                Uuid::new_v4(),
                &format!("synthetic-device-credential-{index}"),
                100 + i64::try_from(index).unwrap(),
            )
            .unwrap();
        }
        let exact = targets
            .iter()
            .map(|(user_id, email)| (*user_id, email.as_str()))
            .collect::<Vec<_>>();
        let alice_row_before = alice_row(&db, alice.id);
        let alice_installations_before = db
            .list_installations()
            .unwrap()
            .into_iter()
            .filter(|installation| installation.user_id == alice.id)
            .collect::<Vec<_>>();
        let alice_data_before = alice_data_counts(&db, alice.id);
        let alice_receipt_before = db
            .stored_batch(alice.id, alice_installation, alice_batch)
            .unwrap();

        let mut wrong = exact.clone();
        wrong[0].1 = "wrong-target@example.com";
        assert!(
            db.set_users_retired_exact(&wrong, alice.id, &alice.email, "must roll back", true, 200,)
                .is_err()
        );
        assert_eq!(db.list_users().unwrap().len(), 11);
        assert_eq!(alice_row(&db, alice.id), alice_row_before);

        assert_eq!(
            db.set_users_retired_exact(
                &exact,
                alice.id,
                &alice.email,
                "historical synthetic fixture",
                true,
                201,
            )
            .unwrap(),
            10
        );
        assert_eq!(db.list_users().unwrap(), vec![alice.clone()]);
        for (user_id, _) in &targets {
            let target = db.user_by_id(*user_id).unwrap().unwrap();
            assert!(target.disabled);
            assert_eq!(target.retired_unix_ms, Some(201));
        }
        assert_eq!(alice_row(&db, alice.id), alice_row_before);
        assert_eq!(alice_data_counts(&db, alice.id), alice_data_before);
        assert_eq!(
            db.list_installations()
                .unwrap()
                .into_iter()
                .filter(|installation| installation.user_id == alice.id)
                .collect::<Vec<_>>(),
            alice_installations_before
        );
        assert_eq!(
            db.stored_batch(alice.id, alice_installation, alice_batch)
                .unwrap(),
            alice_receipt_before
        );

        assert_eq!(
            db.set_users_retired_exact(
                &exact,
                alice.id,
                &alice.email,
                "historical synthetic fixture",
                false,
                202,
            )
            .unwrap(),
            10
        );
        assert_eq!(db.list_users().unwrap().len(), 11);
        for (index, (user_id, _)) in targets.iter().enumerate() {
            let target = db.user_by_id(*user_id).unwrap().unwrap();
            assert!(target.is_active());
            assert!(
                db.authenticate_device(&format!("synthetic-device-credential-{index}"), 203)
                    .unwrap()
                    .is_none(),
                "reversal must not revive target {user_id}'s credential"
            );
        }
        assert_eq!(alice_row(&db, alice.id), alice_row_before);
        assert_eq!(alice_data_counts(&db, alice.id), alice_data_before);
        assert_eq!(
            db.stored_batch(alice.id, alice_installation, alice_batch)
                .unwrap(),
            alice_receipt_before
        );
        assert!(
            db.authenticate_device("alice-protected-device-credential", 203)
                .unwrap()
                .is_some()
        );
        db.quick_check().unwrap();
    }

    #[test]
    fn retirement_retains_receipts_and_requires_fresh_enrollment_after_reversal() {
        let db = db();
        let protected = db.ensure_admin("alice@example.com", 1).unwrap();
        let user_id = Uuid::new_v4();
        db.writer()
            .unwrap()
            .execute(
                "INSERT INTO users(id,email,display_name,is_admin,disabled,created_unix_ms) \
                 VALUES (?1,'retired@example.com','Retired',0,0,1)",
                [user_id.to_string()],
            )
            .unwrap();
        let user = db.user_by_id(user_id).unwrap().unwrap();
        let installation_id = Uuid::new_v4();
        let credential_id = Uuid::new_v4();
        db.insert_test_device_credential(
            user.id,
            installation_id,
            credential_id,
            "retirement-device-credential",
            2,
        )
        .unwrap();
        let batch_id = Uuid::new_v4();
        let records = vec![(0, Uuid::new_v4(), "retained-record".to_string())];
        db.commit_batch(&CommittedBatch {
            user_id: user.id,
            installation_id,
            batch_id,
            checksum: "retained-batch",
            first_source_seq: 0,
            last_source_seq: 0,
            accepted_records: 1,
            received_unix_ms: 3,
            records: &records,
        })
        .unwrap();
        assert_eq!(
            db.set_users_retired_exact(
                &[(user.id, user.email.as_str())],
                protected.id,
                &protected.email,
                "deterministic fixture",
                true,
                4,
            )
            .unwrap(),
            1
        );
        assert_eq!(db.list_users().unwrap(), vec![protected.clone()]);
        assert!(
            db.stored_batch(user.id, installation_id, batch_id)
                .unwrap()
                .is_some()
        );
        assert!(
            db.authenticate_device("retirement-device-credential", 5)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            db.set_users_retired_exact(
                &[(user.id, user.email.as_str())],
                protected.id,
                &protected.email,
                "deterministic fixture",
                false,
                6,
            )
            .unwrap(),
            1
        );
        assert_eq!(db.list_users().unwrap().len(), 2);
        assert!(
            db.stored_batch(user.id, installation_id, batch_id)
                .unwrap()
                .is_some()
        );
        assert!(
            db.authenticate_device("retirement-device-credential", 7)
                .unwrap()
                .is_none()
        );
        db.quick_check().unwrap();
    }
}
