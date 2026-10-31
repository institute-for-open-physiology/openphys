# WAL format v1 contract

Date: 24 May 2026
Status: v1 implementation contract for the WAL refactor
Related design: `docs/wal_spool_architecture.md`, `docs/wal_parallel_implementation_plan.md`

This document freezes the on-disk WAL segment format, payload schema, path rules, and sidecar contract for the first WAL implementation. It is deliberately narrow: H10 raw capture plus manual marker events now, with room for phone-origin marker imports later.

## 1. Design summary

- A WAL segment is one local file with a fixed 64-byte binary header followed by length-prefixed binary frames.
- Every frame payload is a canonical UTF-8 JSON raw capture event.
- The WAL is the canonical raw capture source. Parsed ECG/ACC/HR rows and Parquet files are derived downstream by replay/archiver/live services.
- One segment belongs to exactly one `participant_id` and `source` path partition.
- `.open` files are owned only by that source's writer. Archiver consumes `.closed` only. Live service may tail `.open` read-only.
- `source=chest_h10` is owned by `training_sensors collect` under the collector lock.
- `source=manual_events` is owned by `training_sensors live` under the manual-events writer lock.
- `source=iphone_manual_events` is reserved for future phone/import tooling under its own writer lock.

## 2. Segment header layout

All multi-byte integers in the segment header and frame header are **little-endian**.

Each WAL file starts with exactly 64 bytes:

| Offset | Size | Type | Field | Required value / meaning |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | bytes | `magic` | ASCII `TSWALV1\n` (`54 53 57 41 4c 56 31 0a`) |
| 8 | 2 | `u16` | `header_len` | `64` |
| 10 | 2 | `u16` | `container_major` | `1` |
| 12 | 2 | `u16` | `container_minor` | `0` for this contract |
| 14 | 2 | `u16` | `frame_header_len` | `8` |
| 16 | 2 | `u16` | `payload_codec` | `1` = canonical JSON v1 |
| 18 | 2 | `u16` | `checksum_algorithm` | `1` = CRC-32C/Castagnoli over frame payload bytes |
| 20 | 4 | `u32` | `header_flags` | `0` in v1 |
| 24 | 16 | bytes | `segment_id` | raw RFC 4122 UUID bytes; must match filename UUID |
| 40 | 8 | `i64` | `segment_start_unix_ns` | UTC Unix nanoseconds used in the filename start timestamp |
| 48 | 16 | bytes | `reserved` | all zero in v1 |

The initial v1 reader must reject files with the wrong magic, unsupported `container_major`, `container_minor != 0`, `header_len != 64`, `frame_header_len != 8`, unknown `payload_codec`, unknown `checksum_algorithm`, non-zero `header_flags`, or non-zero reserved bytes. Future readers may explicitly opt in to accepting higher backward-compatible `container_minor` values after the implementation and tests are updated.

The first payload record should be `segment_started`, but parsers must rely on the binary header for container detection.

## 3. Frame layout and recovery semantics

After the 64-byte header, the file contains zero or more frames:

| Offset within frame | Size | Type | Field | Meaning |
| ---: | ---: | --- | --- | --- |
| 0 | 4 | `u32` | `payload_len` | number of payload bytes immediately following this frame header |
| 4 | 4 | `u32` | `payload_crc32c` | CRC-32C/Castagnoli of exactly those payload bytes |
| 8 | `payload_len` | bytes | `payload` | canonical JSON v1 record |

Rules:

- Byte order is little-endian for both frame header integers.
- `payload_len` excludes the 8-byte frame header.
- `payload_crc32c` is computed over payload bytes only, not over the frame header.
- CRC-32C parameters are the common Castagnoli form: polynomial `0x1EDC6F41` (reflected `0x82F63B78`), initial value `0xffffffff`, input reflected, output reflected, final XOR `0xffffffff`.
- `payload_len == 0` is invalid.
- v1 writers must not emit payloads larger than `16 MiB` (`16_777_216` bytes). Larger logical data must be split across records.
- No compression or encryption is applied inside v1 frames.

Recovery/truncation:

- Tail-tolerant readers of `.open` files read valid complete frames until EOF, an incomplete frame header, an incomplete payload, an excessive length, or a checksum mismatch. They do **not** truncate.
- Source-owner recovery of a stale `.open` file validates frames from the start and truncates at the first incomplete or bad frame. Complete frames before that point remain valid.
- A checksum mismatch in a `.closed` file is a fatal archive/replay error; normal archiver code must not repair or truncate `.closed` files.

## 4. Payload serialization: canonical JSON v1

Each frame payload is a single UTF-8 JSON object with no BOM and no trailing newline.

Canonical writer rules:

- JSON is minified: no insignificant whitespace.
- Object keys are emitted in the schema order listed in this document.
- Arbitrary diagnostic maps such as `collector_event.fields` must emit keys in lexicographic UTF-8 byte order.
- Integers are base-10 JSON numbers with no leading zeroes.
- Floats are not used in v1 payloads.
- Optional values are encoded as `null`; keys listed for a record body are still present.
- Strings are UTF-8 JSON strings. Writers should emit printable Unicode directly and escape only JSON-required characters (`"`, `\\`, and control characters).
- Binary fields are standard RFC 4648 Base64 strings **with `=` padding** and no whitespace. Empty bytes are `""`.
- Decoders may accept semantically equivalent JSON key order, but golden fixtures and writer tests must use the canonical order.

## 5. Common raw capture record envelope

Every payload has this top-level object, with keys in this order:

```json
{
  "schema_version": 1,
  "record_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "kind": "gatt_notification",
  "participant_id": "local",
  "source": "chest_h10",
  "writer_id": "training_sensors collect",
  "writer_epoch_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "session_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "connection_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  "collector_unix_ns": 1770000000123456789,
  "collector_monotonic_ns": 123456789,
  "physical_device_id": "h10_a1b2c3d4",
  "ble_peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "body": {}
}
```

Field rules:

| Field | Type | Rule |
| --- | --- | --- |
| `schema_version` | integer | `1` for all v1 payload records. |
| `record_id` | UUID string | Unique per WAL record. Lower-case hyphenated UUID text. |
| `kind` | string enum | One of the record kinds listed below. |
| `participant_id` | string | Path-safe participant ID. Current value: `local`. |
| `source` | string enum | Current values: `chest_h10`, `manual_events`; reserved future value: `iphone_manual_events`. Must match the path partition. |
| `writer_id` | string | Human-readable component, e.g. `training_sensors collect`, `training_sensors live`, `iphone importer`. |
| `writer_epoch_id` | UUID string | New UUID generated when a writer process/import epoch starts. Defines the monotonic timestamp epoch. |
| `session_id` | UUID string | Logical writer session/run. For `chest_h10`, this is the collector session. |
| `connection_id` | UUID string or `null` | BLE connection/attempt ID where applicable; `null` for manual marker records and pre-connection scanner records. |
| `collector_unix_ns` | integer `i64` | UTC Unix nanoseconds when the local writer observed/created the event. For H10 notifications this is the collector receive timestamp. For manual events this is live-service receive/write timestamp. |
| `collector_monotonic_ns` | integer `u64` | Monotonic nanoseconds since the writer process/import epoch represented by `writer_epoch_id`. |
| `physical_device_id` | string or `null` | Stable physical-device provenance where applicable, e.g. `h10_a1b2c3d4`. Not a path partition. |
| `ble_peripheral_id` | string or `null` | Platform BLE peripheral identifier where applicable. On macOS this is the lower-case CoreBluetooth peripheral UUID. |
| `body` | object | Variant-specific object. |

### Timestamp semantics

- `collector_unix_ns` is the wall-clock timestamp for cross-process and cross-source ordering.
- `collector_monotonic_ns` is for duration/order checks within one `writer_epoch_id` only. It must not be compared across different `writer_epoch_id` values.
- Writers must record the monotonic timestamp as close as practical to the Unix timestamp for the same event.
- Within one `writer_epoch_id`, `collector_monotonic_ns` should be non-decreasing in append order. If the platform clock violates this, the writer must log a `collector_event` with severity `error` or `critical` rather than silently hiding it.
- H10 PMD device timestamps remain inside raw PMD notification payload bytes and are parsed downstream. They are not duplicated in the common envelope.
- HR-service notifications do not have a device timestamp; use collector timestamps only.
- Manual marker `event_unix_ns` in the body may differ from `collector_unix_ns` when an event is imported or queued offline.

## 6. UUID, string, and binary encoding rules

- UUID JSON strings are lower-case RFC 4122 hyphenated text: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
- The segment header stores `segment_id` as the 16 RFC 4122 bytes corresponding to the filename UUID.
- BLE characteristic/service UUIDs are lower-case 128-bit UUID strings. Bluetooth 16-bit UUIDs must be expanded to the Bluetooth base UUID, e.g. Heart Rate Measurement is `00002a37-0000-1000-8000-00805f9b34fb`.
- Path partition values (`participant_id`, `source`) must match `[a-z0-9][a-z0-9_-]{0,63}`. No slash, dot, space, or uppercase in path partition values.
- `physical_device_id` should be lower-case and stable, e.g. `h10_a1b2c3d4`; serial numbers with vendor casing can appear in body fields such as service discovery metadata.
- Binary payloads in JSON use standard Base64 with padding. The companion `payload_len` field must equal the decoded byte length.
- Error/detail strings are diagnostic text, not machine-stable enums. Machine-stable classification must use explicit enum fields such as `kind`, `stage`, `reason`, or `event_name`.

## 7. Raw capture event kinds and body schemas

All body keys listed for a kind are required and emitted in the listed order. Optional fields use `null`.

The exact v1 `kind` enum is:

```text
segment_started
segment_closed
session_started
session_ended
collector_event
recovery_event
scan_attempt
scan_result
scan_error
connect_attempt
connect_result
connect_error
disconnect_attempt
disconnect_result
disconnect_error
central_event
rssi_sample
rssi_read_error
service_discovery_summary
subscription_attempt
subscription_result
subscription_error
gatt_notification
gatt_read_request
gatt_read_response
gatt_read_error
gatt_write_request
gatt_write_result
gatt_write_error
psftp_set_local_time_attempt
psftp_set_local_time_result
manual_marker
```

### Segment/session/recovery records

#### `segment_started`

```json
{
  "segment_id": "11111111-1111-4111-8111-111111111111",
  "segment_start_unix_ns": 1770000000000000000,
  "segment_start_monotonic_ns": 0,
  "segment_path": "data/wal/participant_id=local/source=chest_h10/date=2026-02-02/wal-20260202T160000.000000000Z-11111111-1111-4111-8111-111111111111.open",
  "wal_container_version": "1.0",
  "payload_codec": "canonical_json_v1",
  "checksum_algorithm": "crc32c",
  "rotation_reason": "new_session",
  "previous_segment_id": null
}
```

#### `segment_closed`

```json
{
  "segment_id": "11111111-1111-4111-8111-111111111111",
  "segment_end_unix_ns": 1770001800000000000,
  "reason": "rotation",
  "record_count_before_this": 1234,
  "size_bytes_before_this": 987654,
  "truncated_tail_bytes": 0
}
```

`reason` values: `rotation`, `ctrl_c`, `writer_shutdown`, `recovery`, `fatal_error`.

#### `session_started`

```json
{
  "software_name": "training_sensors",
  "software_version": "0.1.0",
  "git_commit": null,
  "hostname": "macbook",
  "pid": 12345,
  "target": "A1B2C3D4",
  "started_reason": "process_start",
  "config_json": "{}"
}
```

`config_json` is a minified JSON string for diagnostic configuration snapshots. It is intentionally not parsed by WAL readers.

#### `session_ended`

```json
{
  "reason": "ctrl_c",
  "message": "stopping streams and disconnecting"
}
```

#### `collector_event`

```json
{
  "event": "pmd_stream_start_confirmed",
  "severity": "info",
  "detail": "acc",
  "fields": {"measurement":"acc"}
}
```

`severity` values: `debug`, `info`, `warn`, `error`, `critical`. `fields` values must be JSON scalars or `null`.

#### `recovery_event`

```json
{
  "recovered_path": "data/wal/participant_id=local/source=chest_h10/date=2026-02-02/wal-...open",
  "action": "close_open",
  "valid_records": 123,
  "truncated_tail_bytes": 17,
  "message": "truncated partial tail and closed stale open segment"
}
```

`action` values: `close_open`, `truncate_tail`, `delete_tmp`, `skip`, `error`.

### Scan/connect/disconnect and central events

#### `scan_attempt`

```json
{
  "attempt_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "target": "A1B2C3D4",
  "scan_timeout_ms": 10000,
  "service_uuids": []
}
```

#### `scan_result`

```json
{
  "attempt_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "local_name": "Polar H10 A1B2C3D4",
  "rssi_dbm": -43,
  "manufacturer_data_b64": null,
  "service_uuids": ["0000180d-0000-1000-8000-00805f9b34fb"],
  "matched": true
}
```

#### `scan_error`

```json
{
  "attempt_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "stage": "adapter_peripherals",
  "error": "Runtime Error: ..."
}
```

#### `connect_attempt`

```json
{
  "attempt_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "local_name": "Polar H10 A1B2C3D4",
  "rssi_dbm": -43
}
```

#### `connect_result`

```json
{
  "attempt_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "connected": true,
  "elapsed_ms": 1732
}
```

#### `connect_error`

```json
{
  "attempt_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "stage": "connect",
  "error": "Failed to encrypt",
  "elapsed_ms": 5000
}
```

#### `disconnect_attempt`

```json
{
  "reason": "ctrl_c",
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687"
}
```

#### `disconnect_result`

```json
{
  "reason": "ctrl_c",
  "elapsed_ms": 95
}
```

#### `disconnect_error`

```json
{
  "reason": "cleanup_after_error",
  "error": "timeout",
  "elapsed_ms": 2000
}
```

#### `central_event`

```json
{
  "event_name": "device_disconnected",
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "detail": "CentralEvent::DeviceDisconnected",
  "rssi_dbm": null,
  "service_uuids": [],
  "raw_debug": "DeviceDisconnected(...)"
}
```

Required central event coverage includes at least discovered/updated/connected/disconnected events exposed by the BLE backend. `raw_debug` is diagnostic and not parser-stable.

### RSSI and service discovery

#### `rssi_sample`

```json
{
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "rssi_dbm": -57,
  "method": "corebluetooth_read_rssi",
  "age_ms": 0
}
```

`method` values: `corebluetooth_read_rssi`, `advertisement_cache`, `platform_property`, `unknown`.

#### `rssi_read_error`

```json
{
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "method": "corebluetooth_read_rssi",
  "error": "timeout",
  "elapsed_ms": 500
}
```

#### `service_discovery_summary`

```json
{
  "peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
  "service_count": 2,
  "services": [
    {
      "service_uuid": "0000180d-0000-1000-8000-00805f9b34fb",
      "characteristics": [
        {
          "characteristic_uuid": "00002a37-0000-1000-8000-00805f9b34fb",
          "properties": ["notify"]
        }
      ]
    }
  ],
  "device_info": {
    "manufacturer": "Polar Electro Oy",
    "model": "H10",
    "serial": "A1B2C3D4",
    "hardware_revision": "00760690.05",
    "firmware_revision": "5.0.0",
    "software_revision": "4.1.10"
  }
}
```

`device_info` may contain `null` values for unavailable fields, but the keys above must be present when the object is emitted.

### Subscription and GATT operations

#### `subscription_attempt`

```json
{
  "operation_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "characteristic_uuid": "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
  "notify_kind": "notify",
  "reason": "pmd_data"
}
```

#### `subscription_result`

```json
{
  "operation_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "characteristic_uuid": "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
  "elapsed_ms": 42
}
```

#### `subscription_error`

```json
{
  "operation_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
  "characteristic_uuid": "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
  "error": "Device disconnected",
  "elapsed_ms": 3000
}
```

#### `gatt_notification`

```json
{
  "characteristic_uuid": "fb005c82-02e7-f387-1cad-8acd2d8df0c8",
  "description": "pmd_data",
  "payload_b64": "AQIDBA==",
  "payload_len": 4
}
```

`description` values are diagnostic labels such as `pmd_data`, `pmd_control`, `heart_rate_measurement`, `battery_level`, `psftp_mtu`, or `unknown`.

#### `gatt_read_request`

```json
{
  "operation_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  "characteristic_uuid": "00002a19-0000-1000-8000-00805f9b34fb",
  "reason": "battery_level"
}
```

#### `gatt_read_response`

```json
{
  "operation_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  "characteristic_uuid": "00002a19-0000-1000-8000-00805f9b34fb",
  "reason": "battery_level",
  "payload_b64": "ZA==",
  "payload_len": 1
}
```

#### `gatt_read_error`

```json
{
  "operation_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
  "characteristic_uuid": "00002a19-0000-1000-8000-00805f9b34fb",
  "reason": "battery_level",
  "error": "timeout",
  "elapsed_ms": 2000
}
```

#### `gatt_write_request`

```json
{
  "operation_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  "characteristic_uuid": "fb005c81-02e7-f387-1cad-8acd2d8df0c8",
  "write_type": "with_response",
  "reason": "pmd_start_ecg",
  "payload_b64": "AgAAAA==",
  "payload_len": 4
}
```

`write_type` values: `with_response`, `without_response`.

#### `gatt_write_result`

```json
{
  "operation_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  "characteristic_uuid": "fb005c81-02e7-f387-1cad-8acd2d8df0c8",
  "elapsed_ms": 35
}
```

#### `gatt_write_error`

```json
{
  "operation_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  "characteristic_uuid": "fb005c81-02e7-f387-1cad-8acd2d8df0c8",
  "error": "Device disconnected",
  "elapsed_ms": 3000
}
```

### PS-FTP time-setting metadata/result

PS-FTP traffic is also captured through normal GATT write/notification records. These records add operation-level semantic metadata for H10 clock-setting replay/debugging.

#### `psftp_set_local_time_attempt`

```json
{
  "operation_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
  "local_time_iso8601": "2026-05-24T12:13:14.123456789+01:00",
  "local_time_unix_ns": 1779621194123456789,
  "tz_offset_minutes": 60,
  "dst_offset_minutes": null,
  "request_payload_b64": "AAAA",
  "control_characteristic_uuid": "fb005c51-02e7-f387-1cad-8acd2d8df0c8",
  "mtu_characteristic_uuid": "fb005c52-02e7-f387-1cad-8acd2d8df0c8"
}
```

#### `psftp_set_local_time_result`

```json
{
  "operation_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
  "success": true,
  "status_code": 0,
  "response_payload_b64": "AAAA",
  "error": null,
  "elapsed_ms": 121
}
```

### Manual and future phone-origin events

#### `manual_marker`

```json
{
  "event_id": "12345678-1234-4234-8234-123456789abc",
  "event_unix_ns": 1770000001123000000,
  "event_monotonic_ns": 987200000,
  "event_time_source": "dashboard_server_receive",
  "created_by": "dashboard",
  "label": "leave_building_start",
  "note": null,
  "client_id": "macbook_dashboard",
  "client_event_id": null,
  "dedupe_key": "12345678-1234-4234-8234-123456789abc"
}
```

`event_time_source` values: `dashboard_server_receive`, `phone_tap`, `import_receive`, `manual_backfill`.

`created_by` values: `dashboard`, `phone`, `import`, `cli`.

For future phone events under `source=iphone_manual_events`, `event_unix_ns` is the phone tap time when available, `collector_unix_ns` is the server/import receive/write time, and `dedupe_key` must be stable across retries. The default dedupe key is `event_id`. `event_monotonic_ns` is nullable for offline phone imports or manual backfills when no meaningful monotonic epoch is available; if present for phone data it is scoped to the phone/import event source, not to the server writer epoch.

## 8. Raw capture coverage checklist

A v1 H10 collector WAL implementation must emit enough records to cover:

- collector/session/segment start and stop;
- stale `.open` recovery actions;
- scan attempts, scan results, and scan errors;
- connect attempts, connect results, and connect errors;
- disconnect attempts, disconnect results, and disconnect errors;
- CoreBluetooth/btleplug central events, especially connect/disconnect events;
- RSSI samples and RSSI read errors;
- service discovery summaries, including characteristic UUIDs and properties;
- subscription attempts, results, and errors for HR, PMD data/control, and PS-FTP where applicable;
- GATT read requests, read responses, and read errors for important reads;
- GATT write requests, write results, and write errors, including PMD start/stop and PS-FTP writes;
- inbound GATT notifications for PMD data, PMD control, HR, battery notifications if subscribed, and PS-FTP notifications;
- PS-FTP local-time setting attempt metadata and result;
- generic collector events needed to understand watchdogs, stream gaps, cleanup, and archive-critical failures;
- manual marker records in the manual-events source.

## 9. Segment path and filename rules

Base layout:

```text
{data_dir}/wal/
  participant_id={participant_id}/
    source={source}/
      writer.lock                 # source-owned writer lock for non-H10 sources
      date=YYYY-MM-DD/
        wal-{START}-{SEGMENT_ID}.open
        wal-{START}-{END}-{SEGMENT_ID}.closed
        wal-{START}-{END}-{SEGMENT_ID}.archived.json
```

For `source=chest_h10`, the mandatory process lock is `{data_dir}/collector.lock`; the collector is the only writer/recovery owner for that source. Source-level `writer.lock` is optional for H10 in v1 but must not replace the collector lock.

For `source=manual_events` and `source=iphone_manual_events`, the mandatory writer lock is:

```text
{data_dir}/wal/participant_id={participant_id}/source={source}/writer.lock
```

Filename timestamp format:

```text
YYYYMMDDTHHMMSS.NNNNNNNNNZ
```

- UTC only.
- Exactly 9 fractional nanosecond digits.
- Example: `20260524T121314.123456789Z`.

Filename grammar:

```text
START      = YYYYMMDDTHHMMSS.NNNNNNNNNZ
END        = YYYYMMDDTHHMMSS.NNNNNNNNNZ
SEGMENT_ID = lower-case hyphenated UUID
open       = wal-{START}-{SEGMENT_ID}.open
closed     = wal-{START}-{END}-{SEGMENT_ID}.closed
sidecar    = wal-{START}-{END}-{SEGMENT_ID}.archived.json
```

Example:

```text
data/wal/participant_id=local/source=chest_h10/date=2026-05-24/
  wal-20260524T121314.123456789Z-550e8400-e29b-41d4-a716-446655440000.open
  wal-20260524T121314.123456789Z-20260524T124314.987654321Z-550e8400-e29b-41d4-a716-446655440000.closed
  wal-20260524T121314.123456789Z-20260524T124314.987654321Z-550e8400-e29b-41d4-a716-446655440000.archived.json
```

Collision avoidance:

- `SEGMENT_ID` is generated before creating the file and must be a random RFC 4122 UUID with at least UUIDv4-equivalent collision resistance.
- The filename includes both nanosecond start time and `SEGMENT_ID`, so repeated starts within the same second cannot collide.
- Writers must create new files with exclusive-create semantics and must never overwrite an existing WAL, sidecar, or archive output.
- If an `.open.tmp` or `.open` path already exists for a generated `SEGMENT_ID`, generate a new `SEGMENT_ID` and retry. If repeated exclusive-create attempts fail, exit loudly.
- A closed target path collision is a serious integrity error; do not overwrite it.

Date partition rule:

- `date=YYYY-MM-DD` is the UTC date of `START`, not `END`.
- A segment that crosses midnight remains in the start-date directory.
- Query/replay code must use sidecar/record timestamps for exact time overlap filtering, or include adjacent date partitions around a queried boundary.

## 10. File state transitions

### Opening a segment

1. Source writer holds its required OS advisory lock.
2. Compute `START`, `SEGMENT_ID`, and start-date directory.
3. Exclusive-create temp path:

   ```text
   wal-{START}-{SEGMENT_ID}.open.tmp
   ```

4. Write the 64-byte segment header, flush, fsync the file.
5. Atomically rename within the same directory:

   ```text
   wal-{START}-{SEGMENT_ID}.open.tmp
     -> wal-{START}-{SEGMENT_ID}.open
   ```

6. Fsync the parent directory.
7. Append frames to `.open`.

Readers and archivers must ignore `*.tmp` files.

### Rotating or cleanly closing a segment

1. Rotation is checked before appending a new normal record. If the current segment is older than the configured duration, close it first and put the new record in the next segment.
2. On clean rotation, Ctrl-C, writer shutdown, or fatal-error close, append `segment_closed` as the last frame before closing. If appending the close record itself fails, fail loudly and preserve the `.open` for next-start recovery rather than silently publishing a closed file without the close record. During recovery, append `segment_closed` when the validated file is safely writable; otherwise record the recovery outcome in the next active segment.
3. Flush and fsync the `.open` file.
4. Atomically rename within the same directory:

   ```text
   wal-{START}-{SEGMENT_ID}.open
     -> wal-{START}-{END}-{SEGMENT_ID}.closed
   ```

5. Fsync the parent directory.
6. The `.closed` file is immutable.

Production target rotation is approximately 30 minutes from segment start. Tests may configure short durations, byte thresholds, or one-record segments.

### Startup recovery of stale `.open`

Only the source owner may recover stale `.open` files, and only after acquiring that source's lock.

Recovery steps:

1. Scan owned source/date directories for `.open` and `.open.tmp`.
2. Collect recovery observations in memory while recovering stale files. The writer may also append recovery/close records to the recovered `.open` itself when the file is validated and writable.
3. Delete abandoned `.open.tmp` files. Writers must not append records while a file still has the `.open.tmp` suffix; by construction v1 writers rename to `.open` before appending normal frames, so deleting stale `.open.tmp` does not discard committed records.
4. Validate each `.open` file frame-by-frame.
5. Truncate incomplete/bad tail bytes, if any.
6. Append `segment_closed` with `reason: "recovery"` when safe.
7. Fsync file and rename to normal `.closed` using recovery time as `END`.
8. Fsync parent directory.
9. After opening the new active segment, emit `recovery_event` records summarizing any recovered `.open`, truncated tails, or deleted `.open.tmp` files.

Recovered files become ordinary `.closed` files. There is no special filename suffix for recovered segments; recovery is visible through `recovery_event`/`segment_closed` records and operational logs.

### Archiver sidecar transition

For each successfully archived `.closed` WAL, the archiver writes a colocated sidecar by temp-rename:

```text
wal-{START}-{END}-{SEGMENT_ID}.archived.json.tmp
  -> wal-{START}-{END}-{SEGMENT_ID}.archived.json
```

Rules:

- The archiver must hold `{data_dir}/archive/archiver.lock` before producing Parquet files or sidecars.
- The archiver never reads `.open` as an archive input and never renames/truncates/removes `.open`.
- If `.archived.json` exists and matches the `.closed` file SHA-256 and schema versions, skip the segment.
- If `.archived.json` exists but does not match, fail loudly and do not overwrite it.
- Stale `.archived.json.tmp` files are ignored by readers. The archiver may remove/recreate them on startup while holding the archiver lock.

### Parquet output temp transition

The archiver writes derived Parquet files with an archive-local temp suffix and atomic rename:

```text
part-{START}-{END}-{SEGMENT_ID}-{stream}.parquet.tmp
  -> part-{START}-{END}-{SEGMENT_ID}-{stream}.parquet
```

After the Task G switch-over, WAL-era Parquet output uses the production archive root `{data_dir}/archive/parquet/...`. Pre-switch validation namespaces such as `{data_dir}/archive_wal/parquet/...` must not be used by production `training_sensors archive`.

## 11. `.archived.json` sidecar schema v1

The sidecar is UTF-8 JSON. It may be pretty-printed; its SHA fields are over the referenced files, not the sidecar bytes.

Top-level field order for writers:

```json
{
  "sidecar_schema_version": 1,
  "status": "archived",
  "wal": {
    "path": "data/wal/participant_id=local/source=chest_h10/date=2026-05-24/wal-...closed",
    "filename": "wal-...closed",
    "participant_id": "local",
    "source": "chest_h10",
    "segment_id": "550e8400-e29b-41d4-a716-446655440000",
    "wal_container_major": 1,
    "wal_container_minor": 0,
    "payload_codec": "canonical_json_v1",
    "checksum_algorithm": "crc32c",
    "segment_start_unix_ns": 1770000000000000000,
    "segment_end_unix_ns": 1770001800000000000,
    "first_record_unix_ns": 1770000000001000000,
    "last_record_unix_ns": 1770001799999000000,
    "record_count": 12345,
    "size_bytes": 4567890,
    "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  },
  "source_provenance": {
    "physical_device_ids": ["h10_a1b2c3d4"],
    "ble_peripheral_ids": ["72585434-07e3-d5e3-5ace-4e6c70118687"],
    "device_serials": ["A1B2C3D4"],
    "device_models": ["H10"],
    "firmware_versions": ["5.0.0"]
  },
  "archiver": {
    "software_name": "training_sensors",
    "software_version": "0.1.0",
    "git_commit": null,
    "run_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "archived_at_unix_ns": 1770001810000000000,
    "archive_schema_version": 2
  },
  "outputs": [
    {
      "stream": "raw_gatt",
      "path": "data/archive/parquet/participant_id=local/stream=raw_gatt/date=2026-05-24/part-...parquet",
      "schema_version": 1,
      "row_count": 12345,
      "size_bytes": 234567,
      "sha256": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
      "first_record_unix_ns": 1770000000001000000,
      "last_record_unix_ns": 1770001799999000000
    }
  ],
  "recovery": {
    "was_recovered_open": false,
    "truncated_tail_bytes": 0,
    "recovery_events": []
  },
  "warnings": [],
  "errors": []
}
```

Required sidecar rules:

- `status` is `archived` only for complete successful sidecars. Failed archive attempts should not publish final `.archived.json`.
- `wal.sha256` is SHA-256 of the entire `.closed` WAL file, including the 64-byte header and all frames.
- `archiver.archive_schema_version=2` identifies the derived Parquet schema family introduced by the Android/cloud
  work. In v2 the `events` output has output `schema_version=2`, a required `severity` UTF-8 column, and a required
  `fields_json` UTF-8 column containing the canonical structured diagnostic map; other stream outputs remain at output
  `schema_version=1`. Existing archive schema-v1 event files remain immutable and omit both new columns. Dataset
  readers spanning v1 and v2 must union columns by name, treat missing v1 `severity` as `info`, and treat missing v1
  `fields_json` as `{}`; a rewrite of historical v1 files is not required.
- `wal.record_count` counts frames, not the binary header.
- `outputs` may be empty only if the WAL contained no archivable records; otherwise every derived Parquet output must be listed.
- `warnings` and `errors` are arrays of diagnostic strings. A successful sidecar should have `errors: []`.

## 12. Source ownership, recovery, and locking

### `source=chest_h10`

- Sole writer: `training_sensors collect`.
- Lock: `{data_dir}/collector.lock` using an OS advisory lock (`flock`/`fcntl` equivalent), acquired before WAL recovery, scanning, or connecting to the H10.
- Recovery: collector recovers only `participant_id=local/source=chest_h10` stale `.open` files after acquiring the collector lock.
- No other process may write, close, rename, truncate, or recover H10 WAL segments.

### `source=manual_events`

- Sole writer: `training_sensors live` when marker posting is enabled.
- Lock: `{data_dir}/wal/participant_id=local/source=manual_events/writer.lock` using an OS advisory lock.
- If the lock is unavailable, the live service must either fail loudly or run read-only with marker posting disabled and visible status.
- Recovery: the live service recovers only `source=manual_events` stale `.open` files after acquiring this lock.
- H10 collector must not write manual marker records.

### `source=iphone_manual_events`

- Reserved writer: future phone/import tool.
- Lock: `{data_dir}/wal/participant_id=local/source=iphone_manual_events/writer.lock` on the server/import side.
- Phone-origin events must have stable `event_id`/`dedupe_key` generated at tap time. Re-importing the same phone event must not create duplicate semantic events.
- `event_unix_ns` is the phone tap time when available; `collector_unix_ns` is the import/write time.
- Recovery is owned by the phone/import writer only after acquiring its source lock.
- The phone/import writer must never recover or modify `source=chest_h10` or `source=manual_events` files.

### `archive` and `live` readers

- Archiver lock: `{data_dir}/archive/archiver.lock`.
- Archiver consumes `.closed` only, writes Parquet `.tmp -> .parquet`, then sidecar `.archived.json.tmp -> .archived.json`.
- Live service may read `.open` and `.closed` from H10/manual sources read-only. It must tolerate partial tail frames and `.open -> .closed -> new .open` rotation.

## 13. Version and evolution policy

- Container format version is `container_major.container_minor` in the 64-byte header.
- v1 implementations must support `1.0`.
- The first implementation should accept `container_minor == 0` only. Accepting `1.x` with `x > 0` requires an explicit reader update and tests proving that the minor-version additions are safely ignored or handled.
- Increment `container_major` for any breaking change to header layout, frame layout, checksum semantics, or payload codec semantics.
- Increment `container_minor` only for backward-compatible additions that updated v1 readers can safely ignore or handle.
- `schema_version` inside payload records is the raw event schema version. It remains `1` for this contract.
- Adding a new record kind or adding optional body fields is allowed only if existing v1 readers can skip/ignore them without corrupting archive output. Writers for this project should not emit such additions until the implementation plan and tests are updated.
- Renaming/removing fields, changing timestamp units, changing Base64 rules, or changing body field meanings requires a new payload schema version.
- Sidecar schema evolves independently through `sidecar_schema_version`. A sidecar schema bump must not change WAL bytes.
- Readers should preserve unknown valid frames for replay and report unsupported record kinds clearly rather than silently treating a segment as fully archived.

## 14. Golden fixtures

These generated binary fixtures are normative for header, frame, CRC, and canonical JSON behavior. The Python code below is self-contained and writes two one-record WAL files: one H10 notification segment and one manual marker segment. They are intentionally minimal framing fixtures; production writers should also emit `segment_started` and session/close records as described above.

Expected file summaries:

| Fixture | Size | SHA-256 | Frame payload length | Frame CRC-32C |
| --- | ---: | --- | ---: | --- |
| `h10_one_notification.wal` | 735 | `ed692a999ee7351c0b083861be8ad9b86dcce7a4026d21fe8593413f39912b35` | 663 | `0xcba2f767` |
| `manual_one_marker.wal` | 856 | `5772347a8c90a455ceab282ffeb107e1e7a149dd10772a5e1b3bf1f5ccaf1ef6` | 784 | `0xd20c184f` |

Expected decoded records:

- `h10_one_notification.wal`: one `gatt_notification` record for `participant_id=local`, `source=chest_h10`, `physical_device_id=h10_a1b2c3d4`, characteristic `00002a37-0000-1000-8000-00805f9b34fb`, payload bytes `10 48`.
- `manual_one_marker.wal`: one `manual_marker` record for `participant_id=local`, `source=manual_events`, event ID `12345678-1234-4234-8234-123456789abc`, label `leave_building_start`.

```python
import base64
import hashlib
import json
import struct
import uuid


def crc32c(data: bytes) -> int:
    poly = 0x82F63B78
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
            crc &= 0xFFFFFFFF
    return crc ^ 0xFFFFFFFF


def header(segment_id: str, start_ns: int) -> bytes:
    return (
        b"TSWALV1\n"
        + struct.pack("<HHHHHHI", 64, 1, 0, 8, 1, 1, 0)
        + uuid.UUID(segment_id).bytes
        + struct.pack("<q", start_ns)
        + (b"\0" * 16)
    )


def frame(obj: dict) -> bytes:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return struct.pack("<II", len(payload), crc32c(payload)) + payload


def write_wal(path: str, segment_id: str, start_ns: int, records: list[dict]) -> None:
    data = header(segment_id, start_ns) + b"".join(frame(record) for record in records)
    with open(path, "wb") as f:
        f.write(data)
    print(path, len(data), hashlib.sha256(data).hexdigest())


h10_record = {
    "schema_version": 1,
    "record_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "kind": "gatt_notification",
    "participant_id": "local",
    "source": "chest_h10",
    "writer_id": "training_sensors collect",
    "writer_epoch_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    "session_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    "connection_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    "collector_unix_ns": 1770000000123456789,
    "collector_monotonic_ns": 123456789,
    "physical_device_id": "h10_a1b2c3d4",
    "ble_peripheral_id": "72585434-07e3-d5e3-5ace-4e6c70118687",
    "body": {
        "characteristic_uuid": "00002a37-0000-1000-8000-00805f9b34fb",
        "description": "heart_rate_measurement",
        "payload_b64": base64.b64encode(bytes([0x10, 0x48])).decode("ascii"),
        "payload_len": 2,
    },
}

manual_record = {
    "schema_version": 1,
    "record_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
    "kind": "manual_marker",
    "participant_id": "local",
    "source": "manual_events",
    "writer_id": "training_sensors live",
    "writer_epoch_id": "ffffffff-ffff-4fff-8fff-ffffffffffff",
    "session_id": "99999999-9999-4999-8999-999999999999",
    "connection_id": None,
    "collector_unix_ns": 1770000001123456789,
    "collector_monotonic_ns": 987654321,
    "physical_device_id": None,
    "ble_peripheral_id": None,
    "body": {
        "event_id": "12345678-1234-4234-8234-123456789abc",
        "event_unix_ns": 1770000001123000000,
        "event_monotonic_ns": 987200000,
        "event_time_source": "dashboard_server_receive",
        "created_by": "dashboard",
        "label": "leave_building_start",
        "note": None,
        "client_id": "macbook_dashboard",
        "client_event_id": None,
        "dedupe_key": "12345678-1234-4234-8234-123456789abc",
    },
}

write_wal(
    "h10_one_notification.wal",
    "11111111-1111-4111-8111-111111111111",
    1770000000000000000,
    [h10_record],
)
write_wal(
    "manual_one_marker.wal",
    "22222222-2222-4222-8222-222222222222",
    1770000001000000000,
    [manual_record],
)
```
