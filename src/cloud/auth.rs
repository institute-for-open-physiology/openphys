use super::db::{
    AuthRateLimits, CloudDb, DevicePrincipal, EnrollmentExchange, EnrollmentExchangeError,
    Invitation, NewEnrollment, NewPasswordRegistration, NewSession, PasswordResetFinish,
    RefreshRotation, SessionUser, User, VerifiedPasswordLogin, hash_secret,
    is_terminal_auth_token_error, normalize_email,
};
use anyhow::{Context, Result, anyhow, bail};
use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use uuid::Uuid;

// Sessions persist indefinitely on signed-in devices per the 8 Aug 2026
// operator decision (docs/session_persistence_decision_20260808.md): long
// access tokens shrink refresh churn, year-long rotating refresh tokens keep
// idle devices signed in, and explicit sign-out plus admin revocation remain
// the only session-ending paths.
const ACCESS_LIFETIME_MS: i64 = 24 * 60 * 60 * 1_000;
const REFRESH_LIFETIME_MS: i64 = 365 * 24 * 60 * 60 * 1_000;
pub const INVITATION_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const PASSWORD_RESET_LIFETIME_MS: i64 = 60 * 60 * 1_000;
pub const ENROLLMENT_LIFETIME_MS: i64 = 10 * 60 * 1_000;

const PASSWORD_MIN_BYTES: usize = 12;
const PASSWORD_MAX_BYTES: usize = 128;
const ARGON2_MEMORY_KIB: u32 = 64 * 1_024;
const ARGON2_ITERATIONS: u32 = 3;
const ARGON2_LANES: u32 = 1;
const ARGON2_MIN_MEMORY_KIB: u32 = 8;
const ARGON2_MIN_ITERATIONS: u32 = 1;
const ARGON2_MIN_LANES: u32 = 1;
const ARGON2_MIN_OUTPUT_BYTES: usize = 16;
const ARGON2_MAX_OUTPUT_BYTES: usize = 32;
const ARGON2_MIN_SALT_BYTES: usize = 8;
const ARGON2_MAX_SALT_BYTES: usize = 32;
const AUTH_RATE_WINDOW_MS: i64 = 60_000;
const ARGON_MAX_CONCURRENT: usize = 2;
const PASSWORD_MUTATION_RATE_LIMITS: AuthRateLimits = AuthRateLimits {
    window_ms: AUTH_RATE_WINDOW_MS,
    global: 100,
    account: 8,
    client: 30,
};
const PUBLIC_LOOKUP_RATE_LIMITS: AuthRateLimits = AuthRateLimits {
    window_ms: AUTH_RATE_WINDOW_MS,
    global: 300,
    account: 10,
    client: 60,
};
const PASSWORD_RESET_REQUEST_RATE_LIMITS: AuthRateLimits = AuthRateLimits {
    window_ms: AUTH_RATE_WINDOW_MS,
    global: 100,
    account: 3,
    client: 20,
};

pub struct AuthService {
    db: Arc<CloudDb>,
    argon2: Argon2<'static>,
    argon_gate: ArgonGate,
    dummy_password_hash: String,
    token_derivation_secret: Vec<u8>,
    #[cfg(test)]
    login_after_verify_hook: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
}

struct ArgonGate {
    in_flight: std::sync::atomic::AtomicUsize,
    limit: usize,
}

struct ArgonPermit<'a> {
    gate: &'a ArgonGate,
}

impl ArgonGate {
    fn new(limit: usize) -> Self {
        Self {
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            limit,
        }
    }

    fn acquire(&self) -> Result<ArgonPermit<'_>> {
        let mut current = self.in_flight.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if current >= self.limit {
                bail!("password hashing capacity is temporarily unavailable");
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return Ok(ArgonPermit { gate: self }),
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for ArgonPermit<'_> {
    fn drop(&mut self) {
        self.gate
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub enum RefreshServiceError {
    Terminal,
    Internal(anyhow::Error),
}

#[derive(Debug)]
pub enum PasswordLoginError {
    InvalidCredentials,
    RateLimited,
    Internal(anyhow::Error),
}

#[derive(Debug)]
pub enum PasswordResetRequestError {
    RateLimited,
    Internal(anyhow::Error),
}

#[derive(Debug)]
pub enum PublicAuthError {
    Invalid,
    InvitationTerminal,
    PasswordResetTerminal,
    RateLimited,
    EnrollmentTerminal,
    Internal(anyhow::Error),
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordRegisterRequest {
    pub invitation_token: String,
    pub password: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordLoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordResetRequest {
    pub email: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordResetFinishRequest {
    pub reset_token: String,
    pub password: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshRequest {
    pub refresh_token: String,
    pub rotation_id: Uuid,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentExchangeRequest {
    pub enrollment_token: String,
    pub installation_id: Uuid,
    pub exchange_id: Uuid,
    pub display_label: Option<String>,
    pub android_manufacturer: String,
    pub android_model: String,
    pub android_sdk: i64,
    pub app_package: String,
    pub app_version_name: String,
    pub app_version_code: i64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacEnrollmentExchangeRequest {
    pub enrollment_token: String,
    pub installation_id: Uuid,
    pub exchange_id: Uuid,
    pub display_label: String,
    pub client_version: String,
}

struct EnrollmentMetadata<'a> {
    display_label: Option<&'a str>,
    android_manufacturer: Option<&'a str>,
    android_model: Option<&'a str>,
    android_sdk: Option<i64>,
    app_package: &'a str,
    app_version_name: &'a str,
    app_version_code: i64,
}

#[derive(Clone, Serialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub access_expires_at_unix_ms: i64,
    pub refresh_token: String,
    pub refresh_expires_at_unix_ms: i64,
    pub csrf_token: String,
}

#[derive(Clone)]
pub struct PasswordResetDelivery {
    pub email: String,
    pub reset_token: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Clone)]
pub struct EnrollmentCreation {
    pub enrollment_token: String,
    pub expires_at_unix_ms: i64,
}

#[derive(Clone, Serialize)]
pub struct DeviceCredentialResponse {
    pub installation_id: Uuid,
    pub credential_id: Uuid,
    pub device_credential: String,
}

macro_rules! impl_redacted_debug {
    ($($type:ty),+ $(,)?) => {$ (
        impl std::fmt::Debug for $type {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($type))
                    .field("secret_fields", &"[REDACTED]")
                    .finish()
            }
        }
    )+ };
}

impl_redacted_debug!(
    AuthService,
    PasswordRegisterRequest,
    PasswordLoginRequest,
    PasswordResetRequest,
    PasswordResetFinishRequest,
    RefreshRequest,
    EnrollmentExchangeRequest,
    MacEnrollmentExchangeRequest,
    TokenResponse,
    PasswordResetDelivery,
    EnrollmentCreation,
    DeviceCredentialResponse,
);

impl AuthService {
    pub fn new(db: Arc<CloudDb>, token_derivation_secret: &str) -> Result<Self> {
        if token_derivation_secret.len() < 32 {
            bail!("SENSORS_TOKEN_DERIVATION_SECRET must be at least 32 bytes");
        }
        let params = Params::new(ARGON2_MEMORY_KIB, ARGON2_ITERATIONS, ARGON2_LANES, Some(32))
            .map_err(|error| anyhow!("configure Argon2id parameters: {error}"))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let dummy_password_hash = hash_password_with(&argon2, "dummy-password-never-accepted")?;
        Ok(Self {
            db,
            argon2,
            argon_gate: ArgonGate::new(ARGON_MAX_CONCURRENT),
            dummy_password_hash,
            token_derivation_secret: token_derivation_secret.as_bytes().to_vec(),
            #[cfg(test)]
            login_after_verify_hook: Mutex::new(None),
        })
    }

    pub fn register_password(
        &self,
        request: PasswordRegisterRequest,
        client_key: &str,
        user_agent: Option<&str>,
        now_ms: i64,
    ) -> std::result::Result<TokenResponse, PublicAuthError> {
        if !self
            .admit_rate(
                "password_register",
                &request.invitation_token,
                client_key,
                now_ms,
                PASSWORD_MUTATION_RATE_LIMITS,
            )
            .map_err(PublicAuthError::Internal)?
        {
            return Err(PublicAuthError::RateLimited);
        }
        validate_password(&request.password).map_err(|_| PublicAuthError::Invalid)?;
        validate_secret_token("invitation", &request.invitation_token)
            .map_err(|_| PublicAuthError::InvitationTerminal)?;
        let invitation = self
            .db
            .invitation_by_token(&request.invitation_token, now_ms)
            .map_err(PublicAuthError::Internal)?
            .ok_or(PublicAuthError::InvitationTerminal)?;
        let existing = self
            .db
            .password_login(&invitation.email)
            .map_err(PublicAuthError::Internal)?;
        if existing
            .as_ref()
            .is_some_and(|login| !login.user.is_active() || login.password_hash.is_some())
        {
            return Err(PublicAuthError::InvitationTerminal);
        }
        let requested_user_id = existing
            .map(|login| login.user.id)
            .unwrap_or_else(Uuid::new_v4);
        let password_hash = self
            .hash_password(&request.password)
            .map_err(PublicAuthError::Internal)?;
        let tokens = new_token_response(now_ms);
        self.db
            .register_password(NewPasswordRegistration {
                invitation_token: &request.invitation_token,
                password_hash: &password_hash,
                requested_user_id,
                session: session_for(requested_user_id, &tokens, user_agent, now_ms),
            })
            .map_err(|error| {
                if is_terminal_auth_token_error(&error) {
                    PublicAuthError::InvitationTerminal
                } else {
                    PublicAuthError::Internal(error)
                }
            })?;
        Ok(tokens)
    }

    pub fn invitation_details(
        &self,
        invitation_token: &str,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<Invitation, PublicAuthError> {
        if !self
            .admit_rate(
                "invitation_details",
                invitation_token,
                client_key,
                now_ms,
                PUBLIC_LOOKUP_RATE_LIMITS,
            )
            .map_err(PublicAuthError::Internal)?
        {
            return Err(PublicAuthError::RateLimited);
        }
        validate_secret_token("invitation", invitation_token)
            .map_err(|_| PublicAuthError::InvitationTerminal)?;
        self.db
            .invitation_by_token(invitation_token, now_ms)
            .map_err(PublicAuthError::Internal)?
            .ok_or(PublicAuthError::InvitationTerminal)
    }

    pub fn login_password(
        &self,
        request: PasswordLoginRequest,
        client_key: &str,
        user_agent: Option<&str>,
        now_ms: i64,
    ) -> std::result::Result<TokenResponse, PasswordLoginError> {
        let normalized = normalize_email(&request.email).ok();
        let account_key = normalized.as_deref().unwrap_or("invalid-email");
        let admitted = self
            .admit_rate(
                "password_login",
                account_key,
                client_key,
                now_ms,
                PUBLIC_LOOKUP_RATE_LIMITS,
            )
            .map_err(PasswordLoginError::Internal)?;
        if !admitted {
            return Err(PasswordLoginError::RateLimited);
        }
        let login = match normalized.as_deref() {
            Some(email) => self
                .db
                .password_login(email)
                .map_err(PasswordLoginError::Internal)?,
            None => None,
        };
        let stored_hash = login
            .as_ref()
            .and_then(|login| login.password_hash.as_deref())
            .unwrap_or(&self.dummy_password_hash)
            .to_string();
        let verified = self
            .verify_password(&request.password, &stored_hash)
            .map_err(PasswordLoginError::Internal)?;
        let Some(login) = login
            .filter(|login| login.user.is_active() && login.password_hash.is_some() && verified)
        else {
            return Err(PasswordLoginError::InvalidCredentials);
        };
        #[cfg(test)]
        self.wait_after_password_verify()
            .map_err(PasswordLoginError::Internal)?;
        let replacement = if password_hash_needs_rehash(&stored_hash) {
            Some(
                self.hash_password(&request.password)
                    .map_err(PasswordLoginError::Internal)?,
            )
        } else {
            None
        };
        let tokens = new_token_response(now_ms);
        let issued = self
            .db
            .finish_verified_password_login(VerifiedPasswordLogin {
                user_id: login.user.id,
                verified_password_hash: &stored_hash,
                replacement_password_hash: replacement.as_deref(),
                session: session_for(login.user.id, &tokens, user_agent, now_ms),
            })
            .map_err(PasswordLoginError::Internal)?;
        if !issued {
            return Err(PasswordLoginError::InvalidCredentials);
        }
        Ok(tokens)
    }

    pub fn request_password_reset(
        &self,
        request: PasswordResetRequest,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<Option<PasswordResetDelivery>, PasswordResetRequestError> {
        let normalized = normalize_email(&request.email).ok();
        let account_key = normalized.as_deref().unwrap_or("invalid-email");
        let admitted = self
            .admit_rate(
                "password_reset_request",
                account_key,
                client_key,
                now_ms,
                PASSWORD_RESET_REQUEST_RATE_LIMITS,
            )
            .map_err(PasswordResetRequestError::Internal)?;
        if !admitted {
            return Err(PasswordResetRequestError::RateLimited);
        }
        let Some(email) = normalized else {
            return Ok(None);
        };
        let reset_token = random_token();
        let expires_at_unix_ms = now_ms.saturating_add(PASSWORD_RESET_LIFETIME_MS);
        let issued = self
            .db
            .issue_password_reset_for_email(&email, &reset_token, now_ms, expires_at_unix_ms)
            .map_err(PasswordResetRequestError::Internal)?;
        Ok(issued.then_some(PasswordResetDelivery {
            email,
            reset_token,
            expires_at_unix_ms,
        }))
    }

    pub fn finish_password_reset(
        &self,
        request: PasswordResetFinishRequest,
        client_key: &str,
        user_agent: Option<&str>,
        now_ms: i64,
    ) -> std::result::Result<TokenResponse, PublicAuthError> {
        if !self
            .admit_rate(
                "password_reset_finish",
                &request.reset_token,
                client_key,
                now_ms,
                PASSWORD_MUTATION_RATE_LIMITS,
            )
            .map_err(PublicAuthError::Internal)?
        {
            return Err(PublicAuthError::RateLimited);
        }
        validate_password(&request.password).map_err(|_| PublicAuthError::Invalid)?;
        validate_secret_token("password reset", &request.reset_token)
            .map_err(|_| PublicAuthError::PasswordResetTerminal)?;
        let user = self
            .db
            .password_reset_user(&request.reset_token, now_ms)
            .map_err(PublicAuthError::Internal)?
            .ok_or(PublicAuthError::PasswordResetTerminal)?;
        let password_hash = self
            .hash_password(&request.password)
            .map_err(PublicAuthError::Internal)?;
        let tokens = new_token_response(now_ms);
        self.db
            .finish_password_reset(PasswordResetFinish {
                reset_token: &request.reset_token,
                password_hash: &password_hash,
                session: session_for(user.id, &tokens, user_agent, now_ms),
            })
            .map_err(|error| {
                if is_terminal_auth_token_error(&error) {
                    PublicAuthError::PasswordResetTerminal
                } else {
                    PublicAuthError::Internal(error)
                }
            })?;
        Ok(tokens)
    }

    pub fn create_enrollment(
        &self,
        user: &User,
        browser_session_id: Uuid,
        now_ms: i64,
    ) -> Result<EnrollmentCreation> {
        if !user.is_active() {
            bail!("account is unavailable for enrollment");
        }
        let enrollment_token = random_token();
        let expires_at_unix_ms = now_ms.saturating_add(ENROLLMENT_LIFETIME_MS);
        self.db.create_enrollment(NewEnrollment {
            user_id: user.id,
            browser_session_id,
            token: &enrollment_token,
            now_ms,
            expires_ms: expires_at_unix_ms,
        })?;
        Ok(EnrollmentCreation {
            enrollment_token,
            expires_at_unix_ms,
        })
    }

    pub fn exchange_enrollment(
        &self,
        request: EnrollmentExchangeRequest,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<DeviceCredentialResponse, PublicAuthError> {
        validate_metadata(&request).map_err(|_| PublicAuthError::Invalid)?;
        self.exchange_validated_enrollment(
            &request.enrollment_token,
            request.installation_id,
            request.exchange_id,
            EnrollmentMetadata {
                display_label: request.display_label.as_deref(),
                android_manufacturer: Some(&request.android_manufacturer),
                android_model: Some(&request.android_model),
                android_sdk: Some(request.android_sdk),
                app_package: &request.app_package,
                app_version_name: &request.app_version_name,
                app_version_code: request.app_version_code,
            },
            client_key,
            now_ms,
        )
    }

    pub fn exchange_mac_enrollment(
        &self,
        request: MacEnrollmentExchangeRequest,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<DeviceCredentialResponse, PublicAuthError> {
        if request.display_label.trim().is_empty()
            || request.display_label.len() > 64
            || request.display_label.chars().any(char::is_control)
            || request.client_version.is_empty()
            || request.client_version.len() > 80
            || request.client_version.chars().any(char::is_control)
        {
            return Err(PublicAuthError::Invalid);
        }
        let label = format!("macOS · {}", request.display_label);
        self.exchange_validated_enrollment(
            &request.enrollment_token,
            request.installation_id,
            request.exchange_id,
            EnrollmentMetadata {
                display_label: Some(&label),
                android_manufacturer: None,
                android_model: None,
                android_sdk: None,
                app_package: super::db::MAC_APP_PACKAGE,
                app_version_name: &request.client_version,
                app_version_code: 1,
            },
            client_key,
            now_ms,
        )
    }

    fn exchange_validated_enrollment(
        &self,
        enrollment_token: &str,
        installation_id: Uuid,
        exchange_id: Uuid,
        metadata: EnrollmentMetadata<'_>,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<DeviceCredentialResponse, PublicAuthError> {
        if !self
            .admit_rate(
                "enrollment_exchange",
                enrollment_token,
                client_key,
                now_ms,
                PUBLIC_LOOKUP_RATE_LIMITS,
            )
            .map_err(PublicAuthError::Internal)?
        {
            return Err(PublicAuthError::RateLimited);
        }
        validate_secret_token("enrollment", enrollment_token)
            .map_err(|_| PublicAuthError::Invalid)?;
        let device_credential = self
            .derive_device_credential(enrollment_token, installation_id, exchange_id)
            .map_err(PublicAuthError::Internal)?;
        let proposed_credential_id = Uuid::new_v4();
        let result = self
            .db
            .exchange_enrollment(EnrollmentExchange {
                enrollment_token,
                exchange_id,
                installation_id,
                credential_id: proposed_credential_id,
                credential_hash: &hash_secret(&device_credential),
                display_label: metadata.display_label,
                android_manufacturer: metadata.android_manufacturer,
                android_model: metadata.android_model,
                android_sdk: metadata.android_sdk,
                app_package: metadata.app_package,
                app_version_name: metadata.app_version_name,
                app_version_code: metadata.app_version_code,
                now_ms,
            })
            .map_err(|error| match error {
                EnrollmentExchangeError::Invalid => PublicAuthError::EnrollmentTerminal,
                EnrollmentExchangeError::Internal(error) => PublicAuthError::Internal(error),
            })?;
        Ok(DeviceCredentialResponse {
            installation_id: result.installation_id,
            credential_id: result.credential_id,
            device_credential,
        })
    }

    /// Validates an enrollment link for the phone-browser bootstrap page
    /// without consuming its one-use exchange. Returns the link's expiry.
    pub fn preview_enrollment(
        &self,
        enrollment_token: &str,
        client_key: &str,
        now_ms: i64,
    ) -> std::result::Result<i64, PublicAuthError> {
        if !self
            .admit_rate(
                "enrollment_preview",
                enrollment_token,
                client_key,
                now_ms,
                PUBLIC_LOOKUP_RATE_LIMITS,
            )
            .map_err(PublicAuthError::Internal)?
        {
            return Err(PublicAuthError::RateLimited);
        }
        validate_secret_token("enrollment", enrollment_token)
            .map_err(|_| PublicAuthError::Invalid)?;
        self.db
            .preview_enrollment(enrollment_token, now_ms)
            .map_err(PublicAuthError::Internal)?
            .ok_or(PublicAuthError::Invalid)
    }

    pub fn authenticate_access(&self, token: &str, now_ms: i64) -> Result<Option<SessionUser>> {
        self.db.authenticate_access(token, now_ms)
    }

    pub fn authenticate_device(
        &self,
        credential: &str,
        now_ms: i64,
    ) -> Result<Option<DevicePrincipal>> {
        self.db.authenticate_device(credential, now_ms)
    }

    pub fn revoke_session_family(&self, session_id: Uuid, now_ms: i64) -> Result<bool> {
        self.db.revoke_session_family(session_id, now_ms)
    }

    pub fn refresh(
        &self,
        request: RefreshRequest,
        user_agent: Option<&str>,
        now_ms: i64,
    ) -> std::result::Result<TokenResponse, RefreshServiceError> {
        let access_expires_ms = now_ms.saturating_add(ACCESS_LIFETIME_MS);
        let refresh_expires_ms = now_ms.saturating_add(REFRESH_LIFETIME_MS);
        let proposed = self
            .derived_rotation_response(
                &request.refresh_token,
                request.rotation_id,
                access_expires_ms,
                refresh_expires_ms,
            )
            .map_err(RefreshServiceError::Internal)?;
        let result = self
            .db
            .rotate_refresh(RefreshRotation {
                old_refresh: &request.refresh_token,
                rotation_id: request.rotation_id,
                access_token: &proposed.access_token,
                refresh_token: &proposed.refresh_token,
                csrf_token: &proposed.csrf_token,
                user_agent,
                now_ms,
                access_expires_ms,
                refresh_expires_ms,
            })
            .map_err(RefreshServiceError::Internal)?
            .ok_or(RefreshServiceError::Terminal)?;
        self.derived_rotation_response(
            &request.refresh_token,
            request.rotation_id,
            result.access_expires_ms,
            result.refresh_expires_ms,
        )
        .map_err(RefreshServiceError::Internal)
    }

    pub fn refresh_csrf_matches(
        &self,
        refresh_token: &str,
        csrf_token: &str,
        rotation_id: Uuid,
        now_ms: i64,
    ) -> Result<bool> {
        self.db
            .refresh_csrf_matches(refresh_token, csrf_token, rotation_id, now_ms)
    }

    pub fn revoke_refresh_family(&self, refresh_token: &str, now_ms: i64) -> Result<bool> {
        self.db.revoke_refresh_family(refresh_token, now_ms)
    }

    #[cfg(test)]
    fn set_login_after_verify_hook(
        &self,
        arrived: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        *self
            .login_after_verify_hook
            .lock()
            .map_err(|_| anyhow!("login test-hook lock poisoned"))? = Some((arrived, release));
        Ok(())
    }

    #[cfg(test)]
    fn wait_after_password_verify(&self) -> Result<()> {
        let hook = self
            .login_after_verify_hook
            .lock()
            .map_err(|_| anyhow!("login test-hook lock poisoned"))?
            .take();
        if let Some((arrived, release)) = hook {
            arrived
                .send(())
                .map_err(|_| anyhow!("login test-hook arrival receiver closed"))?;
            release
                .recv()
                .map_err(|_| anyhow!("login test-hook release sender closed"))?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn issue_browser_tokens(&self, user: &User, now_ms: i64) -> Result<TokenResponse> {
        let response = new_token_response(now_ms);
        self.db
            .create_session(session_for(user.id, &response, None, now_ms))?;
        Ok(response)
    }

    fn hash_password(&self, password: &str) -> Result<String> {
        validate_password(password)?;
        let _permit = self.argon_gate.acquire()?;
        hash_password_with(&self.argon2, password)
    }

    fn verify_password(&self, password: &str, encoded: &str) -> Result<bool> {
        let Ok(parsed) = PasswordHash::new(encoded) else {
            return Ok(false);
        };
        if !stored_password_hash_is_within_limits(&parsed) {
            return Ok(false);
        }
        let _permit = self.argon_gate.acquire()?;
        Ok(self
            .argon2
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }

    fn admit_rate(
        &self,
        kind: &str,
        account_key: &str,
        client_key: &str,
        now_ms: i64,
        limits: AuthRateLimits,
    ) -> Result<bool> {
        let account_bucket = self.rate_bucket(b"account", account_key)?;
        let client_bucket = self.rate_bucket(b"client", client_key)?;
        self.db
            .admit_auth_attempt(kind, &account_bucket, &client_bucket, now_ms, limits)
    }

    fn rate_bucket(&self, label: &[u8], value: &str) -> Result<String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token_derivation_secret)
            .map_err(|_| anyhow!("invalid token derivation key"))?;
        mac.update(b"training-sensors-auth-rate-v1\0");
        mac.update(label);
        mac.update(b"\0");
        mac.update(value.as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }

    fn derive_device_credential(
        &self,
        enrollment_token: &str,
        installation_id: Uuid,
        exchange_id: Uuid,
    ) -> Result<String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.token_derivation_secret)
            .map_err(|_| anyhow!("invalid token derivation key"))?;
        mac.update(b"training-sensors-device-enrollment-v1\0");
        mac.update(enrollment_token.as_bytes());
        mac.update(b"\0");
        mac.update(installation_id.as_bytes());
        mac.update(exchange_id.as_bytes());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn derived_rotation_response(
        &self,
        old_refresh: &str,
        rotation_id: Uuid,
        access_expires_ms: i64,
        refresh_expires_ms: i64,
    ) -> Result<TokenResponse> {
        let derive =
            |label: &[u8]| -> Result<String> {
                let mut mac = Hmac::<Sha256>::new_from_slice(&self.token_derivation_secret)
                    .map_err(|_| anyhow!("invalid token derivation key"))?;
                mac.update(b"training-sensors-refresh-v1\0");
                mac.update(label);
                mac.update(b"\0");
                mac.update(old_refresh.as_bytes());
                mac.update(rotation_id.as_bytes());
                mac.update(&access_expires_ms.to_le_bytes());
                mac.update(&refresh_expires_ms.to_le_bytes());
                Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(mac.finalize().into_bytes()))
            };
        Ok(TokenResponse {
            access_token: derive(b"access")?,
            access_expires_at_unix_ms: access_expires_ms,
            refresh_token: derive(b"refresh")?,
            refresh_expires_at_unix_ms: refresh_expires_ms,
            csrf_token: derive(b"csrf")?,
        })
    }
}

fn session_for<'a>(
    user_id: Uuid,
    response: &'a TokenResponse,
    user_agent: Option<&'a str>,
    now_ms: i64,
) -> NewSession<'a> {
    NewSession {
        user_id,
        installation_id: None,
        access_token: &response.access_token,
        refresh_token: &response.refresh_token,
        csrf_token: &response.csrf_token,
        user_agent,
        now_ms,
        access_expires_ms: response.access_expires_at_unix_ms,
        refresh_expires_ms: response.refresh_expires_at_unix_ms,
    }
}

fn hash_password_with(argon2: &Argon2<'_>, password: &str) -> Result<String> {
    let mut salt_bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|error| anyhow!("encode Argon2id salt: {error}"))?;
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| anyhow!("hash password with Argon2id: {error}"))
}

pub(super) fn validate_password(password: &str) -> Result<()> {
    let length = password.len();
    if !(PASSWORD_MIN_BYTES..=PASSWORD_MAX_BYTES).contains(&length) || password.contains('\0') {
        bail!("password must be 12 through 128 UTF-8 bytes and contain no NUL");
    }
    Ok(())
}

fn validate_secret_token(label: &str, token: &str) -> Result<()> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .with_context(|| format!("{label} token is malformed"))?;
    if bytes.len() != 32 {
        bail!("{label} token is malformed");
    }
    Ok(())
}

fn validate_metadata(request: &EnrollmentExchangeRequest) -> Result<()> {
    if request.app_package == super::db::MAC_APP_PACKAGE {
        bail!("reserved native app identity");
    }
    fn bounded(label: &str, value: &str, maximum: usize, empty_allowed: bool) -> Result<()> {
        if value.len() > maximum || (!empty_allowed && value.is_empty()) || value.contains('\0') {
            bail!("{label} is invalid");
        }
        Ok(())
    }
    if let Some(label) = request.display_label.as_deref() {
        bounded("display_label", label, 80, true)?;
    }
    bounded(
        "android_manufacturer",
        &request.android_manufacturer,
        80,
        false,
    )?;
    bounded("android_model", &request.android_model, 80, false)?;
    bounded("app_package", &request.app_package, 200, false)?;
    bounded("app_version_name", &request.app_version_name, 80, false)?;
    if !(1..=10_000).contains(&request.android_sdk) || request.app_version_code < 0 {
        bail!("Android SDK or app version code is invalid");
    }
    Ok(())
}

fn password_hash_needs_rehash(encoded: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(encoded) else {
        return true;
    };
    let mut decoded_salt = [0_u8; 64];
    let salt_length = parsed
        .salt
        .and_then(|salt| salt.decode_b64(&mut decoded_salt).ok().map(<[u8]>::len));
    parsed.algorithm.as_str() != "argon2id"
        || parsed.version != Some(0x13)
        || parsed.params.iter().count() != 3
        || parsed.params.get_decimal("m") != Some(ARGON2_MEMORY_KIB)
        || parsed.params.get_decimal("t") != Some(ARGON2_ITERATIONS)
        || parsed.params.get_decimal("p") != Some(ARGON2_LANES)
        || parsed.hash.map(|hash| hash.len()) != Some(ARGON2_MAX_OUTPUT_BYTES)
        || salt_length != Some(16)
}

fn stored_password_hash_is_within_limits(parsed: &PasswordHash<'_>) -> bool {
    let memory = parsed.params.get_decimal("m");
    let iterations = parsed.params.get_decimal("t");
    let lanes = parsed.params.get_decimal("p");
    if parsed.algorithm.as_str() != "argon2id"
        || parsed.version != Some(0x13)
        || parsed.params.iter().count() != 3
        || !memory.is_some_and(|value| (ARGON2_MIN_MEMORY_KIB..=ARGON2_MEMORY_KIB).contains(&value))
        || !iterations
            .is_some_and(|value| (ARGON2_MIN_ITERATIONS..=ARGON2_ITERATIONS).contains(&value))
        || !lanes.is_some_and(|value| (ARGON2_MIN_LANES..=ARGON2_LANES).contains(&value))
        || !parsed.hash.is_some_and(|hash| {
            (ARGON2_MIN_OUTPUT_BYTES..=ARGON2_MAX_OUTPUT_BYTES).contains(&hash.len())
        })
    {
        return false;
    }
    let Some(salt) = parsed.salt else {
        return false;
    };
    let mut decoded = [0_u8; 64];
    salt.decode_b64(&mut decoded)
        .is_ok_and(|bytes| (ARGON2_MIN_SALT_BYTES..=ARGON2_MAX_SALT_BYTES).contains(&bytes.len()))
}

fn new_token_response(now_ms: i64) -> TokenResponse {
    TokenResponse {
        access_token: random_token(),
        access_expires_at_unix_ms: now_ms.saturating_add(ACCESS_LIFETIME_MS),
        refresh_token: random_token(),
        refresh_expires_at_unix_ms: now_ms.saturating_add(REFRESH_LIFETIME_MS),
        csrf_token: random_token(),
    }
}

pub fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn service() -> (Arc<CloudDb>, AuthService, User) {
        let db = Arc::new(CloudDb::open(Path::new(":memory:")).unwrap());
        let admin = db.ensure_admin("admin@example.com", 1).unwrap();
        let service = AuthService::new(
            db.clone(),
            "test-only-token-derivation-secret-at-least-32-bytes",
        )
        .unwrap();
        (db, service, admin)
    }

    #[test]
    fn argon2id_password_policy_hash_and_verify_are_exact() {
        let (_, service, _) = service();
        for invalid in [
            "short",
            "12345678901",
            &"x".repeat(129),
            "valid-password\0bad",
        ] {
            assert!(validate_password(invalid).is_err());
        }
        let password = "correct horse battery staple";
        let hash = service.hash_password(password).unwrap();
        assert!(hash.starts_with("$argon2id$v=19$m=65536,t=3,p=1$"));
        assert!(!hash.contains(password));
        assert!(service.verify_password(password, &hash).unwrap());
        assert!(!service.verify_password("wrong-password", &hash).unwrap());
        assert!(!password_hash_needs_rehash(&hash));

        let legacy_params = Params::new(8 * 1_024, 1, 1, Some(16)).unwrap();
        let legacy_argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, legacy_params);
        let legacy = hash_password_with(&legacy_argon2, password).unwrap();
        let parsed_legacy = PasswordHash::new(&legacy).unwrap();
        assert!(stored_password_hash_is_within_limits(&parsed_legacy));
        assert!(service.verify_password(password, &legacy).unwrap());
        assert!(password_hash_needs_rehash(&legacy));
    }

    #[test]
    fn extreme_or_unsupported_stored_phc_is_rejected_before_password_work() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("phc@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "original-phc-password".to_string(),
                },
                "phc-client",
                None,
                2,
            )
            .unwrap();
        let user = db.user_by_email("phc@example.com").unwrap().unwrap();
        let ordinary = service.hash_password("ordinary-phc-password").unwrap();
        let extreme = ordinary.replace("m=65536,t=3,p=1", "m=4294967295,t=3,p=1");
        let parsed = PasswordHash::new(&extreme).unwrap();
        assert!(!stored_password_hash_is_within_limits(&parsed));
        let unsupported = ordinary.replacen("$argon2id$", "$argon2i$", 1);
        assert!(!stored_password_hash_is_within_limits(
            &PasswordHash::new(&unsupported).unwrap()
        ));
        db.replace_password_hash_for_test(user.id, &extreme)
            .unwrap();

        assert!(matches!(
            service.login_password(
                PasswordLoginRequest {
                    email: user.email,
                    password: "ordinary-phc-password".to_string(),
                },
                "phc-client",
                None,
                3,
            ),
            Err(PasswordLoginError::InvalidCredentials)
        ));
    }

    #[test]
    fn argon2_gate_rejects_excess_work_without_blocking_a_worker() {
        let gate = ArgonGate::new(2);
        let first = gate.acquire().unwrap();
        let second = gate.acquire().unwrap();
        assert!(gate.acquire().is_err());
        drop(first);
        assert!(gate.acquire().is_ok());
        drop(second);
    }

    #[test]
    fn secret_bearing_debug_output_is_redacted() {
        let sentinel = "sentinel-password-token-credential";
        let values = [
            format!(
                "{:?}",
                PasswordRegisterRequest {
                    invitation_token: sentinel.to_string(),
                    password: sentinel.to_string(),
                }
            ),
            format!(
                "{:?}",
                PasswordLoginRequest {
                    email: sentinel.to_string(),
                    password: sentinel.to_string(),
                }
            ),
            format!(
                "{:?}",
                DeviceCredentialResponse {
                    installation_id: Uuid::nil(),
                    credential_id: Uuid::nil(),
                    device_credential: sentinel.to_string(),
                }
            ),
        ];
        for value in values {
            assert!(value.contains("[REDACTED]"));
            assert!(!value.contains(sentinel));
        }
        let (_, service, _) = service();
        assert!(!format!("{service:?}").contains("test-only-token-derivation-secret"));
    }

    #[test]
    fn disconnected_mac_rejects_consumed_retry_but_accepts_fresh_enrollment() {
        let (db, service, admin) = service();
        let session = db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "disconnect-browser",
                refresh_token: "disconnect-refresh",
                csrf_token: "disconnect-csrf",
                user_agent: None,
                now_ms: 1,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let enrollment = service.create_enrollment(&admin, session, 2).unwrap();
        let mut request = MacEnrollmentExchangeRequest {
            enrollment_token: enrollment.enrollment_token,
            installation_id: Uuid::new_v4(),
            exchange_id: Uuid::new_v4(),
            display_label: "Mac".into(),
            client_version: "1".into(),
        };
        let first = service
            .exchange_mac_enrollment(request.clone(), "mac-disconnect", 3)
            .unwrap();
        assert!(
            db.revoke_installation_for_user(request.installation_id, admin.id, 4)
                .unwrap()
        );
        assert!(
            service
                .authenticate_device(&first.device_credential, 5)
                .unwrap()
                .is_none()
        );
        assert!(
            service
                .exchange_mac_enrollment(request.clone(), "mac-disconnect", 5)
                .is_err()
        );
        request.enrollment_token = service
            .create_enrollment(&admin, session, 6)
            .unwrap()
            .enrollment_token;
        request.exchange_id = Uuid::new_v4();
        let fresh = service
            .exchange_mac_enrollment(request, "mac-disconnect", 7)
            .unwrap();
        assert_ne!(fresh.device_credential, first.device_credential);
        assert!(
            service
                .authenticate_device(&fresh.device_credential, 8)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn mac_enrollment_has_native_metadata_retry_and_platform_binding() {
        let (db, service, admin) = service();
        let session = db
            .create_session(NewSession {
                user_id: admin.id,
                installation_id: None,
                access_token: "mac-browser",
                refresh_token: "mac-refresh",
                csrf_token: "mac-csrf",
                user_agent: None,
                now_ms: 1,
                access_expires_ms: i64::MAX,
                refresh_expires_ms: i64::MAX,
            })
            .unwrap();
        let enrollment = service.create_enrollment(&admin, session, 2).unwrap();
        let request = MacEnrollmentExchangeRequest {
            enrollment_token: enrollment.enrollment_token,
            installation_id: Uuid::new_v4(),
            exchange_id: Uuid::new_v4(),
            display_label: "Research Mac".into(),
            client_version: "1.0".into(),
        };
        assert!(!format!("{request:?}").contains(&request.enrollment_token));
        let first = service
            .exchange_mac_enrollment(request.clone(), "mac", 3)
            .unwrap();
        let retry = service
            .exchange_mac_enrollment(request.clone(), "mac", 4)
            .unwrap();
        assert_eq!(first.device_credential, retry.device_credential);
        let installations = db.list_installations().unwrap();
        let mac = installations
            .iter()
            .find(|row| row.id == request.installation_id)
            .unwrap();
        assert_eq!(mac.display_label.as_deref(), Some("macOS · Research Mac"));
        assert_eq!(mac.android_manufacturer, None);
        assert_eq!(mac.android_model, None);
        assert_eq!(mac.android_sdk, None);
        assert_eq!(
            mac.app_package.as_deref(),
            Some(super::super::db::MAC_APP_PACKAGE)
        );
        let android = EnrollmentExchangeRequest {
            enrollment_token: request.enrollment_token.clone(),
            installation_id: request.installation_id,
            exchange_id: request.exchange_id,
            display_label: None,
            android_manufacturer: "Samsung".into(),
            android_model: "Galaxy".into(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".into(),
            app_version_name: "0.3.0".into(),
            app_version_code: 11,
        };
        assert!(service.exchange_enrollment(android, "android", 5).is_err());
        let mut changed = request.clone();
        changed.exchange_id = Uuid::new_v4();
        assert!(service.exchange_mac_enrollment(changed, "mac", 6).is_err());
        let mut invalid = request.clone();
        invalid.display_label = "bad\nlabel".into();
        assert!(service.exchange_mac_enrollment(invalid, "mac", 7).is_err());
        assert!(
            service
                .exchange_mac_enrollment(request.clone(), "mac", enrollment.expires_at_unix_ms)
                .is_err()
        );
        db.revoke_device_credential(first.credential_id, 8).unwrap();
        assert!(service.exchange_mac_enrollment(request, "mac", 9).is_err());
    }

    #[test]
    fn password_registration_login_reset_and_device_exchange_are_distinct() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation(
            "user@example.com",
            &invitation,
            admin.id,
            2,
            2 + INVITATION_LIFETIME_MS,
        )
        .unwrap();
        let registered = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "initial-password".to_string(),
                },
                "client-a",
                None,
                3,
            )
            .unwrap();
        assert!(
            db.authenticate_access(&registered.access_token, 4)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            service.login_password(
                PasswordLoginRequest {
                    email: "user@example.com".to_string(),
                    password: "wrong-password".to_string(),
                },
                "client-a",
                None,
                5,
            ),
            Err(PasswordLoginError::InvalidCredentials)
        ));
        let login = service
            .login_password(
                PasswordLoginRequest {
                    email: "USER@example.com".to_string(),
                    password: "initial-password".to_string(),
                },
                "client-a",
                None,
                6,
            )
            .unwrap();
        let delivery = service
            .request_password_reset(
                PasswordResetRequest {
                    email: "user@example.com".to_string(),
                },
                "client-a",
                7,
            )
            .unwrap()
            .unwrap();
        let reset = service
            .finish_password_reset(
                PasswordResetFinishRequest {
                    reset_token: delivery.reset_token,
                    password: "replacement-password".to_string(),
                },
                "client-a",
                None,
                8,
            )
            .unwrap();
        assert!(
            db.authenticate_access(&login.access_token, 9)
                .unwrap()
                .is_none()
        );
        let current = db
            .authenticate_access(&reset.access_token, 9)
            .unwrap()
            .unwrap();
        let enrollment = service
            .create_enrollment(&current.user, current.session_id, 10)
            .unwrap();
        let installation_id = Uuid::new_v4();
        let exchange_id = Uuid::new_v4();
        let request = EnrollmentExchangeRequest {
            enrollment_token: enrollment.enrollment_token,
            installation_id,
            exchange_id,
            display_label: Some("Galaxy".to_string()),
            android_manufacturer: "Samsung".to_string(),
            android_model: "SM-A546B".to_string(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".to_string(),
            app_version_name: "1.0".to_string(),
            app_version_code: 1,
        };
        let first = service
            .exchange_enrollment(request.clone(), "test-client", 11)
            .unwrap();
        let retry = service
            .exchange_enrollment(request, "test-client", 12)
            .unwrap();
        assert_eq!(first.device_credential, retry.device_credential);
        assert_eq!(first.credential_id, retry.credential_id);
        let principal = service
            .authenticate_device(&first.device_credential, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(principal.user.id, current.user.id);
        assert_eq!(principal.installation_id, installation_id);
    }

    #[test]
    fn refresh_retry_is_idempotent_but_different_replay_revokes_family() {
        let (db, service, user) = service();
        let initial = service.issue_browser_tokens(&user, 10).unwrap();
        let rotation_id = Uuid::new_v4();
        let request = RefreshRequest {
            refresh_token: initial.refresh_token.clone(),
            rotation_id,
        };
        let first = service.refresh(request.clone(), None, 20).unwrap();
        let retry = service.refresh(request, None, 20).unwrap();
        assert_eq!(first.access_token, retry.access_token);
        assert_eq!(first.refresh_token, retry.refresh_token);

        assert!(matches!(
            service.refresh(
                RefreshRequest {
                    refresh_token: initial.refresh_token,
                    rotation_id: Uuid::new_v4(),
                },
                None,
                21,
            ),
            Err(RefreshServiceError::Terminal)
        ));
        assert!(
            db.authenticate_access(&first.access_token, 21)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn password_reset_wins_race_after_old_password_verification() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("race@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let original = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "original-race-password".to_string(),
                },
                "race-client",
                None,
                2,
            )
            .unwrap();
        let delivery = service
            .request_password_reset(
                PasswordResetRequest {
                    email: "race@example.com".to_string(),
                },
                "race-client",
                3,
            )
            .unwrap()
            .unwrap();
        let service = Arc::new(service);
        let (arrived_tx, arrived_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        service
            .set_login_after_verify_hook(arrived_tx, release_rx)
            .unwrap();
        let login_service = service.clone();
        let old_login = std::thread::spawn(move || {
            login_service.login_password(
                PasswordLoginRequest {
                    email: "race@example.com".to_string(),
                    password: "original-race-password".to_string(),
                },
                "race-client",
                None,
                4,
            )
        });
        arrived_rx
            // Instrumented Argon2id runs can be heavily CPU-contended when the
            // full test binary executes in parallel. This remains a watchdog,
            // not an expected delay in the ordinary suite.
            .recv_timeout(std::time::Duration::from_secs(120))
            .expect("old-password login reached post-verification barrier");
        let reset = service
            .finish_password_reset(
                PasswordResetFinishRequest {
                    reset_token: delivery.reset_token,
                    password: "replacement-race-password".to_string(),
                },
                "race-client",
                None,
                5,
            )
            .unwrap();
        release_tx.send(()).unwrap();
        assert!(matches!(
            old_login.join().unwrap(),
            Err(PasswordLoginError::InvalidCredentials)
        ));
        assert!(
            db.authenticate_access(&original.access_token, 6)
                .unwrap()
                .is_none()
        );
        assert!(
            db.authenticate_access(&reset.access_token, 6)
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            service.login_password(
                PasswordLoginRequest {
                    email: "race@example.com".to_string(),
                    password: "original-race-password".to_string(),
                },
                "race-client",
                None,
                7,
            ),
            Err(PasswordLoginError::InvalidCredentials)
        ));
    }

    #[test]
    fn refreshed_session_regeneration_revokes_prior_family_enrollment() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("family@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let browser = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "family-enrollment-password".to_string(),
                },
                "family-client",
                None,
                2,
            )
            .unwrap();
        let original = db
            .authenticate_access(&browser.access_token, 3)
            .unwrap()
            .unwrap();
        let old_enrollment = service
            .create_enrollment(&original.user, original.session_id, 4)
            .unwrap();
        let refreshed = service
            .refresh(
                RefreshRequest {
                    refresh_token: browser.refresh_token,
                    rotation_id: Uuid::new_v4(),
                },
                None,
                5,
            )
            .unwrap();
        let current = db
            .authenticate_access(&refreshed.access_token, 6)
            .unwrap()
            .unwrap();
        assert_ne!(original.session_id, current.session_id);
        let replacement = service
            .create_enrollment(&current.user, current.session_id, 7)
            .unwrap();
        let request = |token: String, installation_id: Uuid| EnrollmentExchangeRequest {
            enrollment_token: token,
            installation_id,
            exchange_id: Uuid::new_v4(),
            display_label: Some("Galaxy A54".to_string()),
            android_manufacturer: "Samsung".to_string(),
            android_model: "SM-A546B".to_string(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".to_string(),
            app_version_name: "1.0".to_string(),
            app_version_code: 1,
        };
        assert!(matches!(
            service.exchange_enrollment(
                request(old_enrollment.enrollment_token, Uuid::new_v4()),
                "family-client",
                8,
            ),
            Err(PublicAuthError::EnrollmentTerminal)
        ));
        assert!(
            service
                .exchange_enrollment(
                    request(replacement.enrollment_token, Uuid::new_v4()),
                    "family-client",
                    9,
                )
                .is_ok()
        );
    }

    #[test]
    fn disabling_then_reenabling_user_requires_fresh_device_enrollment() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("disabled@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let browser = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "disabled-user-password".to_string(),
                },
                "disabled-client",
                None,
                2,
            )
            .unwrap();
        let current = db
            .authenticate_access(&browser.access_token, 3)
            .unwrap()
            .unwrap();
        let old_installation = Uuid::new_v4();
        let enrollment = service
            .create_enrollment(&current.user, current.session_id, 4)
            .unwrap();
        let exchange_request = |token: String, installation_id: Uuid| EnrollmentExchangeRequest {
            enrollment_token: token,
            installation_id,
            exchange_id: Uuid::new_v4(),
            display_label: Some("Galaxy A54".to_string()),
            android_manufacturer: "Samsung".to_string(),
            android_model: "SM-A546B".to_string(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".to_string(),
            app_version_name: "1.0".to_string(),
            app_version_code: 1,
        };
        let old = service
            .exchange_enrollment(
                exchange_request(enrollment.enrollment_token, old_installation),
                "disabled-client",
                5,
            )
            .unwrap();
        assert!(
            service
                .authenticate_device(&old.device_credential, 6)
                .unwrap()
                .is_some()
        );

        assert!(db.set_user_disabled(current.user.id, true, 7).unwrap());
        assert!(
            service
                .authenticate_device(&old.device_credential, 8)
                .unwrap()
                .is_none()
        );
        let revoked = db
            .list_installations()
            .unwrap()
            .into_iter()
            .find(|installation| installation.id == old_installation)
            .unwrap();
        assert!(revoked.revoked_unix_ms.is_some());
        assert!(
            revoked
                .credentials
                .iter()
                .all(|credential| credential.revoked_unix_ms.is_some())
        );

        assert!(db.set_user_disabled(current.user.id, false, 9).unwrap());
        assert!(
            service
                .authenticate_device(&old.device_credential, 10)
                .unwrap()
                .is_none()
        );
        let browser = service
            .login_password(
                PasswordLoginRequest {
                    email: current.user.email.clone(),
                    password: "disabled-user-password".to_string(),
                },
                "disabled-client",
                None,
                11,
            )
            .unwrap();
        let current = db
            .authenticate_access(&browser.access_token, 12)
            .unwrap()
            .unwrap();
        let fresh_installation = Uuid::new_v4();
        let enrollment = service
            .create_enrollment(&current.user, current.session_id, 13)
            .unwrap();
        let fresh = service
            .exchange_enrollment(
                exchange_request(enrollment.enrollment_token, fresh_installation),
                "disabled-client",
                14,
            )
            .unwrap();
        assert!(
            service
                .authenticate_device(&fresh.device_credential, 15)
                .unwrap()
                .is_some()
        );
        assert_ne!(old_installation, fresh_installation);
    }

    #[test]
    fn enrollment_failures_revocation_and_rotation_preserve_exact_recovery() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("device@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let browser = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "device-owner-password".to_string(),
                },
                "device-client",
                None,
                2,
            )
            .unwrap();
        let current = db
            .authenticate_access(&browser.access_token, 3)
            .unwrap()
            .unwrap();
        let installation_id = Uuid::new_v4();
        let exchange_request = |token: String, exchange_id: Uuid| EnrollmentExchangeRequest {
            enrollment_token: token,
            installation_id,
            exchange_id,
            display_label: Some("Galaxy A54".to_string()),
            android_manufacturer: "Samsung".to_string(),
            android_model: "SM-A546B".to_string(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".to_string(),
            app_version_name: "1.0".to_string(),
            app_version_code: 1,
        };

        let enrollment = service
            .create_enrollment(&current.user, current.session_id, 4)
            .unwrap();
        let request = exchange_request(enrollment.enrollment_token, Uuid::new_v4());
        db.fail_next_enrollment_exchange_before_commit();
        assert!(matches!(
            service.exchange_enrollment(request.clone(), "device-client", 5),
            Err(PublicAuthError::Internal(_))
        ));
        let first = service
            .exchange_enrollment(request, "device-client", 6)
            .unwrap();

        let ambiguous_enrollment = service
            .create_enrollment(&current.user, current.session_id, 7)
            .unwrap();
        let ambiguous_request =
            exchange_request(ambiguous_enrollment.enrollment_token, Uuid::new_v4());
        db.fail_next_enrollment_exchange_after_commit();
        assert!(matches!(
            service.exchange_enrollment(ambiguous_request.clone(), "device-client", 8),
            Err(PublicAuthError::Internal(_))
        ));
        service
            .revoke_session_family(current.session_id, 9)
            .unwrap();
        let recovered = service
            .exchange_enrollment(ambiguous_request, "device-client", 10)
            .unwrap();
        assert_ne!(first.credential_id, recovered.credential_id);
        assert!(
            service
                .authenticate_device(&recovered.device_credential, 11)
                .unwrap()
                .is_some()
        );
        db.revoke_device_credential(first.credential_id, 12)
            .unwrap();
        assert!(
            service
                .authenticate_device(&recovered.device_credential, 13)
                .unwrap()
                .is_some()
        );
        db.revoke_device_credential(recovered.credential_id, 14)
            .unwrap();
        assert!(
            service
                .authenticate_device(&recovered.device_credential, 15)
                .unwrap()
                .is_none()
        );

        let login = service
            .login_password(
                PasswordLoginRequest {
                    email: "device@example.com".to_string(),
                    password: "device-owner-password".to_string(),
                },
                "device-client",
                None,
                16,
            )
            .unwrap();
        let current = db
            .authenticate_access(&login.access_token, 17)
            .unwrap()
            .unwrap();
        let logout_enrollment = service
            .create_enrollment(&current.user, current.session_id, 18)
            .unwrap();
        service
            .revoke_session_family(current.session_id, 19)
            .unwrap();
        assert!(matches!(
            service.exchange_enrollment(
                exchange_request(logout_enrollment.enrollment_token, Uuid::new_v4()),
                "device-client",
                20,
            ),
            Err(PublicAuthError::EnrollmentTerminal)
        ));

        let login = service
            .login_password(
                PasswordLoginRequest {
                    email: "device@example.com".to_string(),
                    password: "device-owner-password".to_string(),
                },
                "device-client",
                None,
                21,
            )
            .unwrap();
        let current = db
            .authenticate_access(&login.access_token, 22)
            .unwrap()
            .unwrap();
        let reset_enrollment = service
            .create_enrollment(&current.user, current.session_id, 23)
            .unwrap();
        let reset = service
            .request_password_reset(
                PasswordResetRequest {
                    email: "device@example.com".to_string(),
                },
                "device-client",
                24,
            )
            .unwrap()
            .unwrap();
        service
            .finish_password_reset(
                PasswordResetFinishRequest {
                    reset_token: reset.reset_token,
                    password: "device-replacement-password".to_string(),
                },
                "device-client",
                None,
                25,
            )
            .unwrap();
        assert!(matches!(
            service.exchange_enrollment(
                exchange_request(reset_enrollment.enrollment_token, Uuid::new_v4()),
                "device-client",
                26,
            ),
            Err(PublicAuthError::EnrollmentTerminal)
        ));
    }

    #[test]
    fn public_token_rate_limits_are_atomic_under_concurrency() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("rate@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let service = Arc::new(service);
        let mut workers = Vec::new();
        for index in 0..20 {
            let service = service.clone();
            let invitation = invitation.clone();
            workers.push(std::thread::spawn(move || {
                service.invitation_details(&invitation, &format!("preview-client-{index}"), 2)
            }));
        }
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 10);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(PublicAuthError::RateLimited)))
                .count(),
            10
        );

        let browser = service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "rate-limit-password".to_string(),
                },
                "registration-client",
                None,
                3,
            )
            .unwrap();
        let current = db
            .authenticate_access(&browser.access_token, 4)
            .unwrap()
            .unwrap();
        let enrollment = service
            .create_enrollment(&current.user, current.session_id, 5)
            .unwrap();
        let request = EnrollmentExchangeRequest {
            enrollment_token: enrollment.enrollment_token,
            installation_id: Uuid::new_v4(),
            exchange_id: Uuid::new_v4(),
            display_label: None,
            android_manufacturer: "Samsung".to_string(),
            android_model: "SM-A546B".to_string(),
            android_sdk: 34,
            app_package: "com.vineyardfinance.sensors".to_string(),
            app_version_name: "1.0".to_string(),
            app_version_code: 1,
        };
        let mut workers = Vec::new();
        for index in 0..20 {
            let service = service.clone();
            let request = request.clone();
            workers.push(std::thread::spawn(move || {
                service.exchange_enrollment(request, &format!("exchange-client-{index}"), 6)
            }));
        }
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 10);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(PublicAuthError::RateLimited)))
                .count(),
            10
        );
        let credential_ids = results
            .iter()
            .filter_map(|result| result.as_ref().ok().map(|value| value.credential_id))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(credential_ids.len(), 1);
    }

    #[test]
    fn concurrent_single_use_registration_creates_exactly_one_account_session() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("single-use@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        let service = Arc::new(service);
        let mut workers = Vec::new();
        for index in 0..2 {
            let service = service.clone();
            let invitation = invitation.clone();
            workers.push(std::thread::spawn(move || {
                service.register_password(
                    PasswordRegisterRequest {
                        invitation_token: invitation,
                        password: "single-use-password".to_string(),
                    },
                    &format!("registration-client-{index}"),
                    None,
                    2,
                )
            }));
        }
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(PublicAuthError::InvitationTerminal)))
                .count(),
            1
        );
        assert!(
            !results
                .iter()
                .any(|result| matches!(result, Err(PublicAuthError::Internal(_))))
        );
        let successful = results.into_iter().find_map(Result::ok).unwrap();
        let user = db.user_by_email("single-use@example.com").unwrap().unwrap();
        assert!(user.is_active());
        assert!(
            db.authenticate_access(&successful.access_token, 3)
                .unwrap()
                .is_some()
        );
        assert!(db.invitation_by_token(&invitation, 3).unwrap().is_none());
    }

    #[test]
    fn concurrent_single_use_password_reset_has_one_terminal_loser() {
        let (db, service, admin) = service();
        let invitation = random_token();
        db.issue_invitation("reset-race@example.com", &invitation, admin.id, 1, i64::MAX)
            .unwrap();
        service
            .register_password(
                PasswordRegisterRequest {
                    invitation_token: invitation,
                    password: "reset-race-original".to_string(),
                },
                "reset-race-client",
                None,
                2,
            )
            .unwrap();
        let reset = service
            .request_password_reset(
                PasswordResetRequest {
                    email: "reset-race@example.com".to_string(),
                },
                "reset-race-client",
                3,
            )
            .unwrap()
            .unwrap();
        let service = Arc::new(service);
        let mut workers = Vec::new();
        for index in 0..2 {
            let service = service.clone();
            let reset_token = reset.reset_token.clone();
            workers.push(std::thread::spawn(move || {
                service.finish_password_reset(
                    PasswordResetFinishRequest {
                        reset_token,
                        password: "reset-race-replacement".to_string(),
                    },
                    &format!("reset-race-client-{index}"),
                    None,
                    4,
                )
            }));
        }
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(PublicAuthError::PasswordResetTerminal)))
                .count(),
            1
        );
        assert!(
            !results
                .iter()
                .any(|result| matches!(result, Err(PublicAuthError::Internal(_))))
        );
        let successful = results.into_iter().find_map(Result::ok).unwrap();
        assert!(
            db.authenticate_access(&successful.access_token, 5)
                .unwrap()
                .is_some()
        );
    }
}
