# seca USB recorder for macOS

This is a native SwiftUI developer app over the independently implemented Python
USB capture, replay, upload and Mac enrollment tools. It records at requested
1/2/4/8/16/32 kHz and uploads a saved session after Stop. It does not stream USB
recordings to Carolina while acquisition is still running.

Build on macOS 13 or newer with Xcode Command Line Tools and Python 3.9+:

```sh
scripts/check_seca_macos.sh
open 'target/macos/Seca Recorder.app'
```

The app is ad-hoc signed for local development. Its bundle contains helper copies
and the absolute path of the Python interpreter found at build time. Rebuild after
moving/removing that interpreter or changing helpers. This is not a notarized,
self-contained distribution; Python remains an explicit prerequisite.

Connect the seca by USB and stop other recording software first. Refresh the
recorder list, select the device and choose Allow USB Access. Accept macOS's
removable-volume prompt; this separate, cancellable step allows up to two minutes
and reads only bounded configuration bytes without starting acquisition. Then
select rate/duration and Start. Refresh or changing devices clears the app's access
readiness marker. Requested settings are separate
from confirmed readback. Stop interrupts the supervisor, which attempts standby
and closes durable local files before the app reports Saved. Failed cleanup leaves
an explicit uncertain-device message. Closing an active recording asks to stop it
and keeps the app open for cleanup. No device reads occur merely to draw the UI;
Refresh checks volume/file metadata, while the recording worker owns `ecg.dat`.
Stop targets only the supervisor PID; the USB worker ignores user SIGINT so it can
still receive standby/close. Actual USB operation watchdogs remain two seconds.

Use Carolina's Mac connection link to enroll this Mac separately from the phone.
Paste the one-use link into the native secure field. It is sent through a private
process pipe, never command arguments. Existing private enrollment state and exact
retry rules are reused. Use “Replace an expired pending link” only for an incomplete
exchange; a saved credential is never silently replaced. Initial production Mac
activation remains a separate coordinated deployment/identity action.

Select a closed recording and choose Upload / retry selected. Acknowledgment counts
reflect checked server receipts. Offline failures retain the recording/outbox and
retry the same immutable bytes. Refresh also finds retained recordings after app
restart; the recent-history list is bounded to 200 entries/100 KB. Choose the
pending recording to resume upload; older files remain available in Finder and
through the CLI. Pause upload safely interrupts preparation or network work while
retaining immutable chunks and atomic receipts. Progress freshness is bounded;
stalled uploads pause, while stalled recording requests graceful standby/close and
reports uncertain cleanup if it cannot finish. An incomplete recording
without a durable terminal event remains inspectable but cannot be uploaded by
this interface. All local recordings and acknowledged upload files remain under:

```text
~/Library/Application Support/TrainingSensors/seca-usb/
```

Open Carolina uses the normal browser session. Reveal recording opens its private
directory in Finder. The GUI does not duplicate Carolina's twelve-lead renderer.
Contact detection defaults to disabled; zero flags cannot then prove contact.
Constant groups are surfaced explicitly. A rate setting or a successful raw
recording establishes neither useful morphology nor electrical calibration,
effective bandwidth or pacemaker detection.
Replay report metadata contains an allowlist of acquisition facts; device network
and password settings remain only in the original private raw capture, not stdout
or audit summaries. A clean user Stop is distinguished from cleanup failure.

Deterministic checks cover the UI state model, helper progress/secret redaction,
mixed configuration fields, clean Stop metadata, replay, durable queues and exact
upload retry. Initial native launch was checked with no recorder: the window
opened, 32 kHz was selected and Start was disabled. Actual USB capture/Stop,
Mac enrollment and all-rate variable-input throughput remain coordinated live
acceptance steps; they are not implied by a successful build.
