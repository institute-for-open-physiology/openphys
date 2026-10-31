# iOS client

Approved screens: Now, History, Device, Sync.

- Record automatically when the H10 connects.
- Use bare charts, a compact heart with a soft glow, and bottom tabs.
- Explain each measurement in a short popover, at most two sentences. Drag to inspect values.
- Show movement as estimated 3D sensor tilt with three acceleration values. Tilt is less reliable during movement.
- Keep Device status dots aligned at the right and timeline dots neutral.
- Show a shared flow from the iPhone to each server. Animate recent acknowledged transfers; healthy syncing stays green.
- Use a compact server sheet with Pause sync, account, linked address, and an actions menu.
- Add timestamped events in one tap. Tap to rename; swipe left to delete with a red action. Show labels on the chart.
- Expire contact status when readings stop. Verify strap removal against live H10 data.
- Place Connect below the disconnected device; place a red Disconnect button at the bottom.
- Use white primary buttons with black text in dark mode, and the reverse in light mode.
- Use a subtly off-white light background with lighter surfaces and outline tab icons.
- Omit the Now heading and elapsed timer. The sensor shortcut opens Device; contact problems appear only when needed.
- Put low, high, and average on the heart-rate chart. Keep ECG intervals hidden until inspection; scroll at display cadence with a stable scale.
- Use a secondary + Add action for events and the label Delete in the editor.

## Bluetooth

- Show Bluetooth off below the heart with neutral text and icon, plus a short Control Center instruction.
- Show Open Settings for denied app permission. Use the public app-settings URL.
- Read Bluetooth availability directly from CoreBluetooth, including when collection is stopped. Keep the existing reconnect behavior.

## Apple Health

- Place Apple Health in the shared Sync flow, with matching row spacing, an outline heart, a Heart rate subtitle, and a native switch. Use no separate card or heading.
- Keep sleep import and Shortcuts outside this change.

## Battery and recovery

- Reduce background uploads and Health saves while preserving sensor readings.
- Improve connection recovery and verify behavior with the phone locked.
- Measure power use before claiming a battery-life improvement.

## Offline storage

- Keep queued recordings on disk and load only the next upload batch into memory.
- Preserve saved accounts, queue paths, sequence numbers, and exact retry batches.
- Warn when iPhone storage runs low. Never silently delete unsent recordings.
- Use a native alert: **Storage running low**, **Free up space to keep recording. Unsent readings stay on this iPhone.**, and **OK**.
- Test large backlogs, restart recovery, interrupted writes, and failed acknowledgements.

### Compression

- Compress iPhone upload queues losslessly. Decompress before upload; keep both server APIs unchanged.
- Keep existing recordings, accounts, sequence numbers, and exact retry batches.
- Preserve bounded memory and recovery after interrupted writes. Validate savings with real captured data.

## Watch companion

- Use the approved BPM-first layout: large BPM, a small three-second ECG, and quiet H10 connection status.
- Relay real H10 ECG samples through the iPhone. Clear stale readings and leave gaps when samples are missing.
- Keep event creation on the iPhone. Deliver any events already queued on the Watch.
- Keep recording and server syncing on the iPhone. No AI or standalone Watch recording.

Validation: Swift package tests, iPhone and Watch builds, simulator screenshots, and paired-device checks when available.

## Server

Use Training Sensors on both apps. A self-hosted home server can be added as a second server; restrict its access (for example to a Tailscale network). Preserve saved accounts and queues when changing the address.

Exchange a connection link before ending its login session. Preserve malformed PMD packets as warning records containing the original JSON. Repair a rejected batch only after HTTP 400 and a confirmed invalid PMD header or timestamp; keep its original file and persist a new batch ID before retrying. Preserve record IDs, sequence numbers, and valid records.

## Sharing

- Distribute iPhone and Watch builds through TestFlight using Apple distribution signing. Keep the existing bundle identifiers and HealthKit capability.

- Prepare a SideStore release package and update feed while Apple Developer enrollment is pending.
- Start fresh installs without servers. Keep live readings and local history available without an account.
- Preserve existing servers, credentials, queue identities, and recordings.
- Prevent unused upload queues from growing before a server is connected.
- Keep server syncing optional and app downloads separate from the private sensor server.
- Share the iPhone and Watch apps with testers through TestFlight.

### CI

- Fix the failing checks and automate app releases.
- Keep SideStore installation available while Apple Developer enrollment is pending.

### TestFlight runners

- A manual iOS workflow option uploads the iPhone and Watch apps to App Store Connect after build checks pass. Ordinary pushes and pull requests never upload.
- Runner signing uses an Apple signing certificate and an App Store Connect team API key from GitHub secrets. API-key uploads export a signed IPA, then use Apple’s `altool` uploader. The runner names the API key `AuthKey_<key ID>.p8`. Temporary credentials and the signing keychain are removed on exit.
- Missing credentials fail before signing. Beta review and tester invitations remain in App Store Connect.
