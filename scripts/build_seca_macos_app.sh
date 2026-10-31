#!/bin/bash
# Developer-only native wrapper. Python remains an explicit local prerequisite.
set -euo pipefail
cd "$(dirname "$0")/.."
python_path="$(python3 -c 'import sys; assert sys.version_info >= (3,9); print(sys.executable)')"
app_path="${1:-$PWD/target/macos/Seca Recorder.app}"
mkdir -p "$app_path/Contents/MacOS" "$app_path/Contents/Resources/helpers"
swiftc -swift-version 5 -target "$(uname -m)-apple-macosx13.0" -O -parse-as-library \
  macos/SecaRecorder/RecorderState.swift macos/SecaRecorder/SecaRecorderApp.swift \
  -framework SwiftUI -framework AppKit -o "$app_path/Contents/MacOS/SecaRecorder"
for helper in seca_macos_bridge.py seca_usb_capture.py seca_replay.py seca_upload.py seca_enroll.py; do
  cp "scripts/$helper" "$app_path/Contents/Resources/helpers/$helper"
done
python3 - "$app_path" "$python_path" <<'PY'
import json, pathlib, plistlib, sys
app = pathlib.Path(sys.argv[1])
(app / 'Contents/Resources/runtime.json').write_text(json.dumps({'python': sys.argv[2]}))
with (app / 'Contents/Info.plist').open('wb') as stream:
    plistlib.dump({'CFBundleName':'Seca Recorder', 'CFBundleDisplayName':'seca USB ECG',
                  'CFBundleIdentifier':'com.vineyardfinance.seca-recorder-dev',
                  'CFBundleExecutable':'SecaRecorder', 'CFBundlePackageType':'APPL',
                  'CFBundleShortVersionString':'0.1.0', 'CFBundleVersion':'1',
                  'LSMinimumSystemVersion':'13.0', 'NSHighResolutionCapable':True,
                  'NSRemovableVolumesUsageDescription':'Read the connected seca recorder configuration and collect ECG recordings over USB.'}, stream)
PY
codesign --force --sign - "$app_path"
printf 'Developer app (ad-hoc signed, local Python required): %s\n' "$app_path"
