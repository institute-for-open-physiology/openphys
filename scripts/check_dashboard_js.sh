#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v node >/dev/null 2>&1; then
  echo "node is required to validate the embedded dashboard JavaScript" >&2
  exit 1
fi

sed -n '/^const LIVE_WORKER_JS:.*r##"/,/^"##;$/p' src/live_server.rs \
  | sed '1d;$d' \
  | node --check -
sed -n '/^<script>$/,/^<\/script>$/p' src/live_server.rs \
  | sed '1d;$d' \
  | node --check -
node scripts/test_live_worker.js
node scripts/test_dashboard_stream_recovery.js
node scripts/test_admin_overview_stream_recovery.js
node scripts/test_dashboard_xss.js
node scripts/test_dashboard_link_banner.js
node scripts/test_session_bootstrap.js
node scripts/test_seca_dashboard_recovery.js
