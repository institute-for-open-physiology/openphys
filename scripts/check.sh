#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== rustfmt check =="
cargo fmt --all -- --check

echo "== cargo build =="
cargo build

echo "== cargo test =="
cargo test

echo "== cargo clippy =="
cargo clippy --all-targets -- -D warnings

echo "== legacy dashboard absence =="
if [[ -e scripts/dashboard.py ]]; then
  echo "scripts/dashboard.py must not be active in the WAL-era architecture" >&2
  exit 1
fi

echo "== dashboard javascript =="
bash scripts/check_dashboard_js.sh

echo "== password authentication and fragment-only setup links =="
for endpoint in \
  '/api/v1/auth/password/register' \
  '/api/v1/auth/password/login' \
  '/api/v1/auth/password-reset/request' \
  '/api/v1/auth/password-reset/finish' \
  '/api/v1/device-enrollments' \
  '/api/v1/device-enrollments/exchange'
do
  rg -F "$endpoint" src/cloud/web.rs >/dev/null
done
rg -F 'type=password' src/cloud/web.rs >/dev/null
rg -F 'new TextEncoder().encode(value).length' src/cloud/web.rs >/dev/null
if rg -n 'type=password[^>]*(minlength|maxlength)' src/cloud/web.rs; then
  echo "HTML password length attributes count UTF-16; validation must use the server's UTF-8 byte policy" >&2
  exit 1
fi
rg -F 'location.hash.slice(1)' src/cloud/web.rs >/dev/null
rg -F '{}/invite#{token}' src/cloud/web.rs >/dev/null
rg -F '{}/reset-password#{}' src/cloud/web.rs >/dev/null
rg -F '{}/enroll#{}' src/cloud/web.rs >/dev/null

if rg -n '\?(invitation_token|reset_token|enrollment_token)=' src/cloud; then
  echo "invitation, reset, and enrollment secrets must use URL fragments, never query parameters" >&2
  exit 1
fi

if rg -n 'webauthn-rs|/api/v1/auth/(registration|authentication)' Cargo.toml src/cloud/web.rs; then
  echo "the public website must not retain mandatory passkey routes or dependencies" >&2
  exit 1
fi

if rg -n 'CloudAdminInvite|cloud-admin-invite|issue_invitation_from_cli' src; then
  echo "invitations must be delivered by the authenticated admin/Resend workflow, not a raw-token CLI" >&2
  exit 1
fi

if rg -n -U '(?s)(print|println|eprint|eprintln)!\s*\([^;]{0,512}(invitation_token|reset_token|enrollment_token|device_credential|access_token|refresh_token|token_derivation_secret|resend_api_key|(invitation|registration|reset|enrollment)_(url|link)|/(invite|reset-password|enroll)#|\{(token|password|credential|invitation_url|registration_url|reset_url|enrollment_url)\})' src/main.rs src/cloud; then
  echo "CLI/runtime output must not reveal raw authentication secrets" >&2
  exit 1
fi

echo "== python syntax =="
python3 -m py_compile scripts/*.py

echo "== seca USB capture, replay and upload =="
python3 -m unittest discover -s scripts -p 'test_seca_*.py'

echo "All local checks passed."
