# Vendored `btleplug` CoreBluetooth patch

This project patches `btleplug` via Cargo's `[patch.crates-io]` mechanism:

```toml
[patch.crates-io]
btleplug = { path = "vendor/btleplug" }
```

## Why it exists

The upstream CoreBluetooth backend exposes RSSI through `PeripheralProperties::rssi`, but on macOS that value is primarily populated from discovery advertisements. After the collector connects and scanning stops, `properties().rssi` can remain cached/stale and is not reliable enough for range/dropout diagnostics.

The collector therefore needs a direct CoreBluetooth `CBPeripheral.readRSSI()` call while connected.

## Patch summary

The vendored patch adds a macOS/CoreBluetooth-specific `Peripheral::read_rssi() -> Result<i16>` method by wiring the existing CoreBluetooth delegate/event machinery:

- `vendor/btleplug/src/corebluetooth/peripheral.rs`
  - adds public async `read_rssi()` on the platform `Peripheral`.
- `vendor/btleplug/src/corebluetooth/internal.rs`
  - adds `CoreBluetoothMessage::ReadRssi` and `CoreBluetoothReply::Rssi(i16)`.
  - calls `CBPeripheral.readRSSI()` and resolves the pending reply on delegate callback.
- `vendor/btleplug/src/corebluetooth/central_delegate.rs`
  - handles `peripheral:didReadRSSI:error:` and emits an internal `RssiRead` event.

## Known limitations

- `readRSSI()` is still controlled by macOS/CoreBluetooth. Live tests showed it can vary near the laptop, but it can also become stale/quantized for long periods.
- RSSI should be treated as a rough diagnostic, not as a precise distance signal.
- The collector wraps every `read_rssi()` call in a short timeout and falls back to cached `properties().rssi` only for dashboard continuity.
- This is not a portable btleplug API. Linux/BlueZ support would need a separate implementation or an upstream API design.

## Maintenance notes

When updating `btleplug`, preserve the `read_rssi()` patch or remove collector usage of `Peripheral::read_rssi()` first. After any vendor update, run:

```bash
scripts/check.sh
```

and a live smoke test while wearing the H10:

```bash
scripts/live_h10_smoke.sh
```
