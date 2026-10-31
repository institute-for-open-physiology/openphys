#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p target/macos
swiftc -swift-version 5 -profile-generate -profile-coverage-mapping \
  macos/SecaRecorder/RecorderState.swift macos/tests/RecorderStateTests.swift \
  -o target/macos/RecorderStateTests
LLVM_PROFILE_FILE="$PWD/target/macos/state.profraw" target/macos/RecorderStateTests
xcrun llvm-profdata merge -sparse target/macos/state.profraw -o target/macos/state.profdata
xcrun llvm-cov report target/macos/RecorderStateTests \
  -instr-profile target/macos/state.profdata macos/SecaRecorder/RecorderState.swift
scripts/build_seca_macos_app.sh
