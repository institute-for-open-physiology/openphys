#!/usr/bin/env python3
"""Audit local ct331 journals and export full-rate, unscaled measured counts.

No device access, interpolation, filtering, baseline subtraction or calibration.
The observed decoder is intentionally independent of the Rust implementation.
Receiver clocks describe packet arrival, never individual sample acquisition time.
"""
from __future__ import annotations

import argparse
import base64
import csv
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import sys
from typing import TextIO

CHANNELS = ("V1", "V2", "V3", "V4", "V5", "V6", "I", "II")
RATES = (1000, 2000, 4000, 8000, 16000, 32000)
MAX_LINE_BYTES = 256 * 1024
MAX_CLOUD_BYTES = 2 * 1024 * 1024
MAX_DIAGNOSTICS = 100
COUNTER_MASK = (1 << 32) - 1
CSV_COLUMNS = ("record_index", "block_counter", "block_sample_index", "rx_unix_ns",
               "rx_monotonic_ns", "lead_off_mask", "acquisition", *CHANNELS)


class ReplayError(ValueError):
    """Malformed or unsupported capture/protocol evidence."""


@dataclass(frozen=True)
class DecodedBlock:
    counter: int
    channels: tuple[tuple[int, ...], ...]
    lead_off_mask: int | None
    widths: tuple[int, ...]
    lead_off_raw: int | None = None


@dataclass(frozen=True)
class DecodedPacket:
    blocks: tuple[DecodedBlock, ...]
    configurations: tuple[str, ...] = ()
    battery_percent: int | None = None
    charging: bool | None = None


def validate_frame(raw: bytes) -> None:
    """Validate exactly one frame, excluding any USB sector padding."""
    if not 11 <= len(raw) <= 65535:
        raise ReplayError("frame length outside 11..65535 bytes")
    if raw[:2] != b"\x55\xaa":
        raise ReplayError("frame magic mismatch")
    if int.from_bytes(raw[2:4], "big") != len(raw):
        raise ReplayError("declared frame length differs from captured bytes")
    if (sum(raw[:-2]) + int.from_bytes(raw[-2:], "big")) & 0xffff:
        raise ReplayError("frame checksum mismatch")


def decode_packet(raw: bytes) -> DecodedPacket:
    """Decode one RX frame; no cross-packet compression state is assumed.

    Each block contains 32 samples in each of eight measured channels. The frame
    counter identifies its final block; earlier blocks count backwards modulo 2^32.
    A whole malformed frame fails rather than returning a valid-looking prefix.
    """
    validate_frame(raw)
    payload = memoryview(raw)[9:-2]
    cursor = 0

    def consume(length: int) -> memoryview:
        nonlocal cursor
        if cursor + length > len(payload):
            raise ReplayError(f"truncated field at frame offset {cursor + 9}")
        part = payload[cursor:cursor + length]
        cursor += length
        return part

    pending: dict[int, tuple[tuple[int, ...], int]] = {}
    groups = []
    configurations = []
    contact = None
    battery = charging = None
    channel_ids = {0xc1 + i: i for i in range(6)} | {0xca: 6, 0xcb: 7}
    while cursor < len(payload):
        identifier = consume(1)[0]
        if identifier == ord("A"):
            length = int.from_bytes(consume(2), "big")
            configurations.append(bytes(consume(length)).decode("utf-8", errors="replace"))
        elif identifier == ord("O"):
            contact = consume(1)[0]
        elif identifier == ord("P"):
            status = consume(3)[2]
            battery, charging = status & 127, bool(status & 128)
        elif identifier in (ord("M"), ord("K")):
            consume(1)
        elif identifier in (ord("S"), ord("s")):
            consume(5)
        elif identifier in channel_ids:
            index = channel_ids[identifier]
            if (identifier == 0xc6 and pending) or index in pending:
                raise ReplayError("incomplete or repeated channel group")
            width = consume(1)[0]
            if width not in (0, 4, 8, 12, 16, 20, 24):
                raise ReplayError(f"unsupported delta width {width}")
            value = int.from_bytes(consume(3), "big", signed=True)
            packed_size = 48 if width == 12 else (31 * width + 7) // 8
            packed = int.from_bytes(consume(packed_size), "big")
            samples = [value]
            if width:
                sign = 1 << (width - 1)
                mask = (1 << width) - 1
                for position in range(31):
                    shift = packed_size * 8 - (position + 1) * width
                    bits = (packed >> shift) & mask
                    value += bits - (1 << width) if bits & sign else bits
                    if not -(1 << 31) <= value < (1 << 31):
                        raise ReplayError("decoded count exceeds signed 32-bit range")
                    samples.append(value)
            else:
                samples *= 32
            pending[index] = (tuple(samples), width)
            if identifier == 0xc1:
                if len(pending) != 8:
                    raise ReplayError("channel group ended before all eight channels arrived")
                groups.append((tuple(pending[i][0] for i in range(8)),
                               tuple(pending[i][1] for i in range(8)), contact))
                pending.clear()
        else:
            raise ReplayError(f"unknown field 0x{identifier:02x} at frame offset {cursor + 8}")
    if pending:
        raise ReplayError("unterminated channel group")
    counter = int.from_bytes(raw[4:8], "big")
    blocks = tuple(DecodedBlock(
        (counter - len(groups) + 1 + i) & COUNTER_MASK, channels,
        None if lead is None else (lead >> 2) | ((lead & 3) << 6), widths, lead,
    ) for i, (channels, widths, lead) in enumerate(groups))
    return DecodedPacket(blocks, tuple(configurations), battery, charging)


def _bounded_json(path: Path, limit: int) -> dict:
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ReplayError(f"JSON document exceeds {limit} bytes")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ReplayError("expected a JSON object")
    return result


def safe_metadata(metadata: dict) -> dict:
    """Reports retain acquisition facts, never wholesale device/network settings."""
    allowed = {"schema_version", "session_id", "transport", "requested_sample_rate_hz",
               "sample_rate_hz", "gain", "contact_detection", "start_unix_ns",
               "start_monotonic_ns", "requested_duration_s", "configuration_confirmed",
               "installation_id", "device_id", "batch_index", "end_reason"}
    scalar = lambda value: value is None or type(value) in (str, int, float, bool)
    result = {key: value for key, value in metadata.items() if key in allowed and scalar(value)}
    device_fields = {"sn", "mode", "samplerate", "gain", "openlead", "compressed"}
    for key in ("device_info", "observed_config"):
        settings = metadata.get(key)
        if isinstance(settings, dict):
            result[key] = {name: value for name, value in settings.items()
                           if name in device_fields and scalar(value)}
    return result


def _clock(record: dict, key: str) -> int:
    value = record.get(key)
    if type(value) is not int or value < 0:
        raise ReplayError(f"missing or invalid {key}")
    return value


def _payload(record: dict) -> bytes:
    hex_value, encoded = record.get("hex"), record.get("payload_b64")
    if (hex_value is None) == (encoded is None):
        raise ReplayError("wire entry must have exactly one of hex or payload_b64")
    if hex_value is not None:
        if not isinstance(hex_value, str) or len(hex_value) > 131070:
            raise ReplayError("invalid or oversized hexadecimal payload")
        if len(hex_value) % 2 or re.fullmatch(r"[0-9a-fA-F]*", hex_value) is None:
            raise ReplayError("invalid hexadecimal payload")
        return bytes.fromhex(hex_value)
    if not isinstance(encoded, str) or len(encoded) > 87380:
        raise ReplayError("invalid or oversized base64 payload")
    try:
        return base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise ReplayError(f"invalid base64 payload: {error}") from error


def audit_capture(path: str | Path, csv_file: TextIO | None = None,
                  sample_rate_hz: int | None = None) -> dict:
    """Return bounded diagnostics; malformed input sets ok=False, without hiding it.

    Accept a session directory, its records.jsonl, a legacy JSONL probe, or one
    cloud batch JSON object. Optional CSV includes every successfully decoded
    sample, even when lead-off is reported. On errors this is a partial export.
    """
    source = Path(path)
    report = {
        "schema_version": 1, "source": str(source.resolve()), "ok": True,
        "record_count": 0, "rx_packets": 0, "tx_packets": 0, "idle_packets": 0,
        "event_count": 0, "block_count": 0, "sample_count": 0,
        **{f"{state}_{quantity}": 0 for state in ("acquisition", "setup", "unknown")
           for quantity in ("rx_packets", "block_count", "sample_count")},
        "missing_blocks": 0, "sequence_errors": 0, "counter_wraps": 0,
        "error_count": 0, "errors": [], "discontinuities": [], "warnings": [],
        "closure": "unknown", "metadata": {}, "rate_evidence": [],
        "requested_sample_rate_hz": None, "observed_configured_rates_hz": [],
        "requested_observed_rate_conflict": False, "tx_contact_disabled_requested": False,
        "first_counter": None, "last_counter": None,
        "first_rx_unix_ns": None, "last_rx_unix_ns": None,
        "first_rx_monotonic_ns": None, "last_rx_monotonic_ns": None,
        "host_receive_duration_s": None, "nominal_sample_duration_s": None,
        "acquisition_nominal_sample_duration_s": None,
        "sample_count_per_receive_second": None, "compression_widths": {},
        "contact_flagged_blocks": 0, "contact_unknown_blocks": 0,
        "csv_rows": 0, "csv_is_partial": False,
        "notes": [
            "Counts are unscaled device integers; calibration and effective bandwidth are unverified.",
            "CSV receive clocks repeat for all samples in a packet and are not device sample timestamps.",
            "No samples are interpolated or removed for lead-off; contact-mask semantics require validation.",
            "CSV acquisition is true for capture-marked acquisition, false for setup, and blank when unknown; all samples remain exported.",
            "Nominal duration uses a requested setting, not proof of the delivered sample rate.",
            "Receive-time throughput includes transport buffering and cannot verify acquisition rate alone.",
        ],
    }
    rates: set[int] = set()
    configured: set[int] = set()
    evidence_keys: set[tuple] = set()
    previous_clock = previous_utc = previous_counter = None
    previous_acquisition = None
    ended = False
    session = False
    clock_valid = True
    writer = csv.writer(csv_file) if csv_file is not None else None
    if writer:
        writer.writerow(CSV_COLUMNS)

    def error(message: str, record_index: int | None = None) -> None:
        report["ok"] = False
        report["error_count"] += 1
        if len(report["errors"]) < MAX_DIAGNOSTICS:
            report["errors"].append({"record_index": record_index, "message": message})

    def rate(value: object, origin: str, requested: bool = True) -> None:
        if type(value) is not int or value not in RATES:
            error(f"unsupported sample-rate evidence from {origin}")
            return
        (rates if requested else configured).add(value)
        key = (origin, value)
        if key not in evidence_keys and len(report["rate_evidence"]) < MAX_DIAGNOSTICS:
            report["rate_evidence"].append({"origin": origin, "sample_rate_hz": value})
            evidence_keys.add(key)

    def settings(text: str, origin: str, requested: bool) -> None:
        if requested and re.search(r"(?:^|[\s\x00;,])openlead\s*=\s*0\b", text):
            report["tx_contact_disabled_requested"] = True
        for match in re.finditer(r"(?:^|[\s\x00;,])samplerate\s*=\s*(\d+)\b", text):
            rate(int(match[1]), origin, requested)

    def wire(record: dict, number: int) -> None:
        nonlocal previous_counter, previous_acquisition
        direction = record.get("direction")
        if direction not in ("rx", "tx"):
            raise ReplayError("wire direction must be rx or tx")
        acquisition = record.get("acquisition")
        if "acquisition" in record and type(acquisition) is not bool:
            raise ReplayError("wire acquisition must be a boolean when present")
        if direction == "tx" and acquisition is True:
            raise ReplayError("TX wire entry cannot be marked acquisition")
        raw = _payload(record)
        report[f"{direction}_packets"] += 1
        if direction == "rx":
            state = "unknown" if acquisition is None else "acquisition" if acquisition else "setup"
            report[f"{state}_rx_packets"] += 1
            # Setup may contain stale blocks with unrelated counters. Only compare
            # counters within a consistently classified RX segment; TX is neutral.
            if acquisition is not previous_acquisition:
                previous_counter = None
            previous_acquisition = acquisition
        validate_frame(raw)
        if direction == "tx":
            if len(raw) < 14 or int.from_bytes(raw[10:12], "big") != len(raw) - 14:
                raise ReplayError("transmit payload allocation length mismatch")
            if raw[9] == ord("W"):
                settings(raw[12:-2].decode("ascii", errors="replace"), "tx_setting", True)
            return
        packet = decode_packet(raw)
        for text in packet.configurations:
            settings(text, "rx_configuration", False)
        if len(raw) == 11:
            report["idle_packets"] += 1
        if packet.blocks:
            for kind in ("unix_ns", "monotonic_ns"):
                key = f"first_rx_{kind}"
                if report[key] is None:
                    report[key] = record[kind]
                report[f"last_rx_{kind}"] = record[kind]
        for block in packet.blocks:
            if report["first_counter"] is None:
                report["first_counter"] = block.counter
            if previous_counter is not None:
                advance = (block.counter - previous_counter) & COUNTER_MASK
                if 0 < advance < (1 << 31):
                    if block.counter < previous_counter:
                        report["counter_wraps"] += 1
                    if advance > 1:
                        report["missing_blocks"] += advance - 1
                        message = f"missing {advance - 1} blocks before counter {block.counter}"
                    else:
                        message = None
                else:
                    report["sequence_errors"] += 1
                    message = f"counter repeated/regressed or ambiguous: {previous_counter} -> {block.counter}"
                if message:
                    error(message, number)
                    if len(report["discontinuities"]) < MAX_DIAGNOSTICS:
                        report["discontinuities"].append({"previous": previous_counter,
                            "current": block.counter, "record_index": number, "advance_modulo_u32": advance})
            previous_counter = report["last_counter"] = block.counter
            report["block_count"] += 1
            report["sample_count"] += 32
            report[f"{state}_block_count"] += 1
            report[f"{state}_sample_count"] += 32
            if block.lead_off_mask is None:
                report["contact_unknown_blocks"] += 1
            elif block.lead_off_mask:
                report["contact_flagged_blocks"] += 1
            for width in block.widths:
                key = str(width)
                report["compression_widths"][key] = report["compression_widths"].get(key, 0) + 1
            if writer:
                for index in range(32):
                    writer.writerow((number, block.counter, index, record["unix_ns"], record["monotonic_ns"],
                                     block.lead_off_mask,
                                     "" if acquisition is None else str(acquisition).lower(),
                                     *(channel[index] for channel in block.channels)))
                    report["csv_rows"] += 1

    def process(record: object, number: int) -> None:
        nonlocal ended, previous_clock, previous_utc, clock_valid
        report["record_count"] += 1
        try:
            if not isinstance(record, dict):
                raise ReplayError("journal record must be an object")
            mono, utc = _clock(record, "monotonic_ns"), _clock(record, "unix_ns")
            if previous_clock is not None and mono < previous_clock:
                clock_valid = False
                error("monotonic clock regressed", number)
            if previous_utc is not None and utc < previous_utc and "UTC clock regressed" not in report["warnings"]:
                report["warnings"].append("UTC clock regressed")
            previous_clock, previous_utc = mono, utc
            kind = record.get("type", "wire")
            if ended:
                raise ReplayError("record found after terminal session_end")
            if kind == "wire":
                wire(record, number)
            elif kind == "event":
                report["event_count"] += 1
                event = record.get("event")
                if not isinstance(event, str) or not event:
                    raise ReplayError("event name is missing")
                if event == "session_end":
                    ended = True
                    user_stopped = (record.get("success") is False and record.get("end_reason") == "interrupted"
                                    and record.get("errors") == [])
                    report["closure"] = "clean" if record.get("success") is True or user_stopped else "unclean"
                    report["user_stopped"] = user_stopped
                    report["end_reason"] = record.get("end_reason")
                    if report["closure"] == "unclean":
                        error(f"capture ended unsuccessfully: {record.get('end_reason', 'unknown')}", number)
            else:
                raise ReplayError(f"unknown journal record type {kind!r}")
        except (ReplayError, ValueError, TypeError) as exc:
            error(str(exc), number)

    if sample_rate_hz is not None:
        rate(sample_rate_hz, "user_supplied", True)
    try:
        if source.is_dir() or (source.name == "records.jsonl" and (source.parent / "session.json").exists()):
            directory = source if source.is_dir() else source.parent
            metadata = _bounded_json(directory / "session.json", 65536)
            session = True
            if metadata.get("schema_version") != 1 or metadata.get("transport") != "usb":
                raise ReplayError("unsupported session schema or transport")
            report["metadata"] = safe_metadata(metadata)
            rate(metadata.get("requested_sample_rate_hz"), "session_requested", True)
            source = directory / "records.jsonl"
        if source.suffix == ".json":
            batch = _bounded_json(source, MAX_CLOUD_BYTES)
            if batch.get("schema_version") != 1 or not isinstance(batch.get("records"), list):
                raise ReplayError("unsupported cloud chunk schema")
            report["metadata"] = safe_metadata(batch)
            rate(batch.get("sample_rate_hz"), "cloud_metadata", True)
            for number, record in enumerate(batch["records"], 1):
                process(record, number)
            report["warnings"].append("Only this cloud chunk is audited; session continuity outside it is unknown.")
        else:
            with source.open("rb") as stream:
                number = 0
                while True:
                    raw = stream.readline(MAX_LINE_BYTES + 1)
                    if not raw:
                        break
                    number += 1
                    if len(raw) > MAX_LINE_BYTES:
                        while raw and not raw.endswith(b"\n"):
                            raw = stream.readline(MAX_LINE_BYTES + 1)
                        error("journal line exceeds byte limit", number)
                        continue
                    if not raw.endswith(b"\n"):
                        error("unterminated final JSONL record (capture may be truncated)", number)
                    try:
                        process(json.loads(raw), number)
                    except (ValueError, UnicodeError) as exc:
                        error(f"invalid JSON: {exc}", number)
    except (OSError, ValueError, UnicodeError) as exc:
        error(str(exc))
    if report["record_count"] == 0:
        error("capture contains no journal records")
    if report["sample_count"] == 0:
        report["warnings"].append("No ECG samples decoded; this audit does not demonstrate acquisition.")
    if session and not ended:
        report["closure"] = "unclean"
        error("session_end is missing; capture is incomplete or still running")
    elif not session and not ended:
        report["warnings"].append("Capture closure cannot be established from this legacy file or single cloud chunk.")
    if len(rates) == 1:
        report["requested_sample_rate_hz"] = next(iter(rates))
        report["nominal_sample_duration_s"] = report["sample_count"] / next(iter(rates))
        if report["acquisition_rx_packets"]:
            report["acquisition_nominal_sample_duration_s"] = report["acquisition_sample_count"] / next(iter(rates))
    elif len(rates) > 1:
        report["warnings"].append("Multiple requested sample rates occur; nominal duration is not assigned.")
    else:
        report["warnings"].append("Requested sample rate is unknown; no rate or nominal duration is assumed.")
    report["observed_configured_rates_hz"] = sorted(configured)
    if rates and configured and not configured.issubset(rates):
        report["requested_observed_rate_conflict"] = True
        report["nominal_sample_duration_s"] = None
        report["acquisition_nominal_sample_duration_s"] = None
        report["warnings"].append("Observed configuration includes a rate different from requested settings; "
                                  "it may precede configuration, but sample timing is not established.")
    if report["setup_sample_count"]:
        report["nominal_sample_duration_s"] = None
        report["warnings"].append("Setup ECG is retained separately; its acquisition rate is unknown, "
                                  "so total nominal sample duration is not assigned.")
    if clock_valid and report["first_rx_monotonic_ns"] is not None:
        elapsed = (report["last_rx_monotonic_ns"] - report["first_rx_monotonic_ns"]) / 1e9
        report["host_receive_duration_s"] = elapsed
        if elapsed > 0:
            report["sample_count_per_receive_second"] = report["sample_count"] / elapsed
    if report["metadata"].get("contact_detection") is False:
        report["warnings"].append("Contact detection was disabled; contact masks do not establish electrode quality.")
    elif report["tx_contact_disabled_requested"]:
        report["warnings"].append("A TX command requested contact detection disabled; "
                                  "contact masks do not establish electrode quality.")
    if report["contact_unknown_blocks"]:
        report["warnings"].append("Some blocks have no contact flag; unflagged does not prove good electrode contact.")
    report["csv_is_partial"] = csv_file is not None and not report["ok"]
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="session directory, journal JSONL, or single cloud chunk JSON")
    parser.add_argument("--csv", type=Path, help="create full-rate measured integer CSV (must not exist)")
    parser.add_argument("--summary", type=Path, help="also create audit JSON (must not exist)")
    parser.add_argument("--sample-rate-hz", type=int, choices=RATES, help="explicit requested rate if absent from journal")
    args = parser.parse_args(argv)
    outputs = []
    exit_code = 2
    try:
        # Check all outputs before creation, then use exclusive open for race safety.
        for destination in (args.csv, args.summary):
            if destination is not None and destination.exists():
                raise FileExistsError(f"refusing to overwrite {destination}")
        if args.csv is not None and args.summary is not None and args.csv.resolve() == args.summary.resolve():
            raise ValueError("CSV and summary must have different paths")
        csv_file = args.csv.open("x", newline="", encoding="utf-8") if args.csv else None
        if csv_file:
            outputs.append(csv_file)
        summary_file = args.summary.open("x", encoding="utf-8") if args.summary else None
        if summary_file:
            outputs.append(summary_file)
        report = audit_capture(args.input, csv_file, args.sample_rate_hz)
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if summary_file:
            summary_file.write(rendered)
        for output in outputs:
            output.flush()
            os.fsync(output.fileno())
        sys.stdout.write(rendered)
        exit_code = 0 if report["ok"] else 1
    except (OSError, ValueError) as exc:
        print(f"seca replay: {exc}", file=sys.stderr)
    finally:
        for output in outputs:
            try:
                output.close()
            except OSError as exc:
                print(f"seca replay: output close failed: {exc}", file=sys.stderr)
                exit_code = 2
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
