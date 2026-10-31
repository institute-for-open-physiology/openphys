#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

find_tool() {
  local name="$1"
  if [[ -x "/opt/homebrew/opt/llvm/bin/$name" ]]; then
    printf '%s\n' "/opt/homebrew/opt/llvm/bin/$name"
  elif command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
  elif command -v xcrun >/dev/null 2>&1 && xcrun --find "$name" >/dev/null 2>&1; then
    xcrun --find "$name"
  else
    echo "Could not find $name. Install LLVM, e.g. brew install llvm." >&2
    exit 1
  fi
}

LLVM_PROFDATA="$(find_tool llvm-profdata)"
LLVM_COV="$(find_tool llvm-cov)"
OUT_BASE="${COVERAGE_OUT_DIR:-${TMPDIR:-/tmp}/training_sensors_coverage}"
# A PID-qualified directory lets independent worktrees/reviewers measure in parallel without one
# invocation deleting another invocation's Cargo target or raw profiles.
OUT_DIR="${OUT_BASE%/}.$$"
TARGET_DIR="$OUT_DIR/target"
PROFRAW_DIR="$OUT_DIR/profraw"
REPORT="$OUT_DIR/report_first_party.txt"

rm -rf "$OUT_DIR"
mkdir -p "$PROFRAW_DIR"

cleanup_large_coverage_artifacts() {
  rm -rf "$TARGET_DIR" "$PROFRAW_DIR"
  rm -f "$OUT_DIR/merged.profdata"
}
trap cleanup_large_coverage_artifacts EXIT

CARGO_INCREMENTAL=0 \
CARGO_TARGET_DIR="$TARGET_DIR" \
RUSTFLAGS="-Cinstrument-coverage" \
LLVM_PROFILE_FILE="$PROFRAW_DIR/%p-%m.profraw" \
  cargo test

"$LLVM_PROFDATA" merge -sparse "$PROFRAW_DIR"/*.profraw -o "$OUT_DIR/merged.profdata"
TEST_BIN="$(find "$TARGET_DIR/debug/deps" -maxdepth 1 -type f -perm +111 -name 'training_sensors-*' | head -n 1)"
if [[ -z "$TEST_BIN" ]]; then
  echo "Could not find training_sensors test binary under $TARGET_DIR/debug/deps" >&2
  exit 1
fi

"$LLVM_COV" report "$TEST_BIN" \
  --instr-profile="$OUT_DIR/merged.profdata" \
  --ignore-filename-regex='(rustlib|/\.cargo/registry/|vendor/|/target/)' \
  > "$REPORT"

cat "$REPORT"
echo "Coverage report written to $REPORT"
