#!/usr/bin/env python3
"""Deterministic tests for protocol and audit failure paths; no sensor required."""
import base64
import contextlib
import csv
import io
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

import seca_replay as replay


def frame(payload=b"", counter=0):
    data = bytearray(b"\x55\xaa")
    data.extend(struct.pack(">HI", len(payload) + 11, counter))
    data.append(32)
    data.extend(payload)
    data.extend(struct.pack(">H", -sum(data) & 65535))
    return bytes(data)


def channel(identifier, width, base, deltas):
    encoded = bytearray([identifier, width])
    encoded.extend(base.to_bytes(3, "big", signed=True))
    if width:
        packed = 0
        for delta in deltas:
            packed = (packed << width) | (delta & ((1 << width) - 1))
        size = 48 if width == 12 else (31 * width + 7) // 8
        padding = size * 8 - 31 * width
        # Nonzero trailing padding must be ignored, including width12's extra byte.
        packed = (packed << padding) | ((1 << padding) - 1)
        encoded.extend(packed.to_bytes(size, "big"))
    return bytes(encoded)


def group(widths=(0, 4, 8, 12, 16, 20, 24, 4), contact=0):
    bases = (-8388608, 8388607, -2, 17, 0, 3, -400, 22)
    payload = bytearray(b"O" + bytes([contact])) if contact is not None else bytearray()
    expected = []
    streams = {}
    for index, width in enumerate(widths):
        if width:
            # Alternating largest signed magnitudes exercise sign extension.
            deltas = [-(1 << (width - 1)) if n % 2 == 0 else (1 << (width - 1)) - 1 for n in range(31)]
        else:
            deltas = [0] * 31
        values = [bases[index]]
        for delta in deltas:
            values.append(values[-1] + delta)
        expected.append(tuple(values))
        identifier = 0xc1 + index if index < 6 else 0xca + index - 6
        streams[index] = channel(identifier, width, bases[index], deltas)
    for index in (5, 6, 7, 1, 2, 3, 4, 0):
        payload.extend(streams[index])
    return bytes(payload), tuple(expected)


def command(text):
    payload = text.encode()
    raw = b"\x55\xaa" + struct.pack(">H", len(payload) + 14) + b"\x12\x34\x56\x78\0W" + struct.pack(">H", len(payload)) + payload
    return raw + struct.pack(">H", -sum(raw) & 65535)


def record(raw, mono=1000000000, direction="rx", **extra):
    return {"direction": direction, "unix_ns": 1700000000000000000 + mono,
            "monotonic_ns": mono, "hex": raw.hex(), **extra}


class DecoderTests(unittest.TestCase):
    def test_mixed_signed_widths_padding_bases_and_multiple_groups(self):
        payload, expected = group(contact=0x81)
        packet = replay.decode_packet(frame(payload * 2, 0))
        self.assertEqual([b.counter for b in packet.blocks], [0xffffffff, 0])
        for block in packet.blocks:
            self.assertEqual(block.channels, expected)
            self.assertEqual(block.widths, (0, 4, 8, 12, 16, 20, 24, 4))
            self.assertEqual(block.lead_off_raw, 0x81)
            self.assertEqual(block.lead_off_mask, 0x60)
        self.assertEqual(replay.decode_packet(frame()).blocks, ())

    def test_every_width_at_each_channel_position(self):
        for width in (0, 4, 8, 12, 16, 20, 24):
            payload, expected = group((width,) * 8, None)
            packet = replay.decode_packet(frame(payload, 42))
            self.assertEqual(packet.blocks[0].channels, expected)
            self.assertIsNone(packet.blocks[0].lead_off_mask)

    def test_status_configuration_and_opaque_fixed_fields(self):
        payload = b"A\0\x10samplerate=4000" + b"\0" + b"P\x10\0\xc9M\0K\x01S12345s67890"
        packet = replay.decode_packet(frame(payload))
        self.assertEqual(packet.configurations, ("samplerate=4000\0",))
        self.assertEqual(packet.battery_percent, 73)
        self.assertTrue(packet.charging)
        self.assertEqual(replay.decode_packet(frame(b"A\0\x01\xff")).configurations, ("\ufffd",))

    def test_framing_and_every_truncated_field_fail(self):
        good = frame(group()[0], 1)
        corrupt = bytearray(good)
        corrupt[13] ^= 1
        cases = [b"", good[:10], b"bad" + good[3:], good + b"\0", bytes(corrupt),
                 frame(b"A"), frame(b"A\0\x03ab"), frame(b"O"), frame(b"P12"),
                 frame(b"M"), frame(b"K"), frame(b"S1234"), frame(b"s"),
                 frame(b"?"), frame(b"\xc6\x07\0\0\0"), frame(b"\xc6\x08\0\0"),
                 frame(b"\xc6\x08\0\0\0\0"), frame(b"\xc6\0\0\0\0"),
                 frame(b"\xc1\0\0\0\0"), frame(b"\xc2\0\0\0\0" * 2),
                 frame(b"\xc2\0\0\0\0\xc6\0\0\0\0")]
        for raw in cases:
            with self.subTest(length=len(raw)), self.assertRaises(replay.ReplayError):
                replay.decode_packet(raw)


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = frame(group()[0], 1)

    def tearDown(self):
        self.temp.cleanup()

    def journal(self, entries, name="capture.jsonl", final_newline=True):
        path = self.root / name
        content = "\n".join(json.dumps(r) for r in entries)
        path.write_text(content + ("\n" if final_newline else ""))
        return path

    def session(self, entries, rate=32000, success=True):
        metadata = {"schema_version": 1, "transport": "usb", "requested_sample_rate_hz": rate,
                    "contact_detection": False, "session_id": "fake"}
        (self.root / "session.json").write_text(json.dumps(metadata))
        if success is not None:
            entries = entries + [{"type": "event", "event": "session_end", "success": success,
                                  "end_reason": "duration_elapsed" if success else "failure",
                                  "monotonic_ns": 9000000000, "unix_ns": 1700000009000000000}]
        self.journal(entries, "records.jsonl")
        return self.root

    def test_streaming_export_preserves_counts_and_packet_clock(self):
        source = self.journal([record(command("samplerate=4000"), 0, "tx"),
                               record(self.raw), record(frame(group()[0], 2), 2000000000)])
        output = io.StringIO()
        result = replay.audit_capture(source, output)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["sample_count"], 64)
        self.assertEqual(result["host_receive_duration_s"], 1)
        self.assertEqual(result["nominal_sample_duration_s"], .016)
        self.assertEqual(result["requested_sample_rate_hz"], 4000)
        rows = list(csv.DictReader(io.StringIO(output.getvalue())))
        self.assertEqual(len(rows), 64)
        self.assertEqual(rows[0]["V1"], "-8388608")
        self.assertEqual(rows[31]["I"], str(group()[1][6][31]))
        self.assertEqual(rows[0]["rx_unix_ns"], rows[31]["rx_unix_ns"])
        self.assertNotEqual(rows[31]["rx_unix_ns"], rows[32]["rx_unix_ns"])
        self.assertEqual(result["closure"], "unknown")
        self.assertEqual(result["compression_widths"]["4"], 4)

    def test_configuration_does_not_set_requested_rate(self):
        payload = b"samplerate=8000\r\nsamplerate=4000\n"
        source = self.journal([record(frame(b"A" + len(payload).to_bytes(2, "big") + payload)),
                               record(frame(), 2000000000)])
        result = replay.audit_capture(source)
        self.assertIsNone(result["requested_sample_rate_hz"])
        self.assertEqual(result["observed_configured_rates_hz"], [4000, 8000])
        self.assertEqual(result["idle_packets"], 1)
        self.assertIsNone(result["host_receive_duration_s"])

    def test_acquisition_segments_preserve_all_samples_without_cross_boundary_gaps(self):
        classified = [(100, False), (101, False), (0, True), (2, True),
                      (500, False), (501, False), (900, None), (901, None), (42, True)]
        entries = []
        for counter, acquisition in classified:
            if counter == 2:
                entries.append(record(command("samplerate=4000"), len(entries), "tx",
                                      acquisition=False))
            extra = {} if acquisition is None else {"acquisition": acquisition}
            entries.append(record(frame(group()[0], counter), len(entries), **extra))
        output = io.StringIO()
        result = replay.audit_capture(self.journal(entries), output)
        self.assertFalse(result["ok"])  # The real within-acquisition gap remains visible.
        self.assertEqual(result["missing_blocks"], 1)
        self.assertEqual(result["sequence_errors"], 0)
        self.assertEqual(result["sample_count"], 9 * 32)
        for state, count in (("setup", 4), ("acquisition", 3), ("unknown", 2)):
            self.assertEqual(result[f"{state}_rx_packets"], count)
            self.assertEqual(result[f"{state}_block_count"], count)
            self.assertEqual(result[f"{state}_sample_count"], count * 32)
        self.assertIsNone(result["nominal_sample_duration_s"])
        self.assertEqual(result["acquisition_nominal_sample_duration_s"], 96 / 4000)
        rows = list(csv.DictReader(io.StringIO(output.getvalue())))
        self.assertEqual(len(rows), 9 * 32)
        for block_index, (counter, acquisition) in enumerate(classified):
            for sample in range(32):
                row = rows[block_index * 32 + sample]
                self.assertEqual(int(row["block_counter"]), counter)
                self.assertEqual(row["acquisition"],
                                 "" if acquisition is None else str(acquisition).lower())
                self.assertEqual(tuple(int(row[name]) for name in replay.CHANNELS),
                                 tuple(channel[sample] for channel in group()[1]))

    def test_acquisition_flag_validation_and_legacy_unknown(self):
        for invalid in (None, 0, 1, "true", [], {}):
            with self.subTest(invalid=invalid):
                result = replay.audit_capture(self.journal([record(self.raw, acquisition=invalid)]))
                self.assertFalse(result["ok"])
                self.assertEqual(result["sample_count"], 0)
                self.assertIn("boolean", str(result["errors"]))
        result = replay.audit_capture(self.journal([
            record(command("samplerate=4000"), direction="tx", acquisition=True)]))
        self.assertFalse(result["ok"])
        self.assertIn("TX", str(result["errors"]))
        result = replay.audit_capture(self.journal([record(self.raw)]))
        self.assertTrue(result["ok"])
        self.assertEqual(result["unknown_sample_count"], 32)
        self.assertEqual(result["acquisition_sample_count"], 0)
        self.assertIsNone(result["acquisition_nominal_sample_duration_s"])

    def test_idle_and_malformed_boundary_do_not_bridge_setup_counter(self):
        for boundary in (frame(), b"bad frame"):
            with self.subTest(boundary=boundary):
                result = replay.audit_capture(self.journal([
                    record(frame(group()[0], 900), 1, acquisition=False),
                    record(boundary, 2, acquisition=True),
                    record(frame(group()[0], 0), 3, acquisition=True)]))
                self.assertEqual(result["sequence_errors"], 0)
                self.assertEqual(result["missing_blocks"], 0)
                self.assertEqual(result["setup_sample_count"], 32)
                self.assertEqual(result["acquisition_sample_count"], 32)
                self.assertEqual(result["ok"], boundary == frame())

    def test_requested_observed_rate_conflict_and_legacy_contact_caveat(self):
        config = b"samplerate=8000"
        source = self.journal([
            record(command("samplerate=4000"), 1, "tx"),
            record(command("openlead=0"), 2, "tx"),
            record(frame(b"A" + len(config).to_bytes(2, "big") + config), 3),
            record(self.raw, 4)])
        result = replay.audit_capture(source)
        self.assertTrue(result["ok"], result)
        self.assertTrue(result["requested_observed_rate_conflict"])
        self.assertTrue(result["tx_contact_disabled_requested"])
        self.assertIsNone(result["nominal_sample_duration_s"])
        self.assertIn("rate different", " ".join(result["warnings"]))
        self.assertIn("contact detection disabled", " ".join(result["warnings"]))

    def test_all_session_rates_and_terminal_event(self):
        for rate in replay.RATES:
            self.session([record(self.raw, type="wire")], rate)
            result = replay.audit_capture(self.root / "records.jsonl")
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["closure"], "clean")
            self.assertEqual(result["nominal_sample_duration_s"], 32 / rate)
            self.assertIn("Contact detection was disabled", " ".join(result["warnings"]))

    def test_missing_failed_and_nonterminal_closure(self):
        for success in (None, False):
            result = replay.audit_capture(self.session([record(self.raw)], success=success))
            self.assertFalse(result["ok"])
            self.assertEqual(result["closure"], "unclean")
        self.session([record(self.raw)])
        with (self.root / "records.jsonl").open("a") as handle:
            handle.write(json.dumps(record(self.raw, 10000000000)) + "\n")
        result = replay.audit_capture(self.root)
        self.assertFalse(result["ok"])
        self.assertIn("after terminal", str(result["errors"]))

    def test_wrap_gaps_duplicate_regression_ambiguous_jump(self):
        counters = [0xfffffffe, 0xffffffff, 0, 2, 2, 1, 0x80000001]
        result = replay.audit_capture(self.journal([
            record(frame(group()[0], n), i) for i, n in enumerate(counters)]))
        self.assertFalse(result["ok"])
        self.assertEqual(result["counter_wraps"], 1)
        self.assertEqual(result["missing_blocks"], 1)
        self.assertEqual(result["sequence_errors"], 3)
        self.assertEqual(result["sample_count"], 224)
        self.assertEqual(len(result["discontinuities"]), 4)

    def test_cloud_chunk_base64_and_limits(self):
        r = record(self.raw)
        r["payload_b64"] = base64.b64encode(bytes.fromhex(r.pop("hex"))).decode()
        path = self.root / "chunk.json"
        path.write_text(json.dumps({"schema_version": 1, "records": [r], "sample_rate_hz": 4000}))
        result = replay.audit_capture(path)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["sample_count"], 32)
        self.assertIn("Only this cloud chunk", " ".join(result["warnings"]))
        path.write_text("x" * (replay.MAX_CLOUD_BYTES + 1))
        self.assertFalse(replay.audit_capture(path)["ok"])

    def test_metadata_unknown_or_invalid_files(self):
        result = replay.audit_capture(self.journal([record(self.raw)]), sample_rate_hz=4000)
        self.assertEqual(result["requested_sample_rate_hz"], 4000)
        self.assertIsNone(replay.audit_capture(self.journal([record(self.raw)]))["nominal_sample_duration_s"])
        self.assertFalse(replay.audit_capture(self.root / "absent")["ok"])
        self.assertFalse(replay.audit_capture(self.journal([record(self.raw)]), sample_rate_hz=3)["ok"])
        self.session([record(self.raw)])
        (self.root / "session.json").write_text("[]")
        self.assertFalse(replay.audit_capture(self.root)["ok"])
        (self.root / "session.json").write_text('{"schema_version":2,"transport":"usb"}')
        self.assertFalse(replay.audit_capture(self.root)["ok"])

    def test_multiple_rate_requests_are_not_assigned_duration(self):
        result = replay.audit_capture(self.journal([record(command("samplerate=4000"), 1, "tx"),
            record(command("samplerate=8000"), 2, "tx"), record(self.raw, 3)]))
        self.assertTrue(result["ok"])
        self.assertIsNone(result["nominal_sample_duration_s"])
        self.assertIn("Multiple requested", " ".join(result["warnings"]))

    def test_bad_records_are_visible_and_partial_export_kept(self):
        entries = [record(self.raw), 4, {}, {**record(self.raw), "hex": "xyz"},
                   {**record(self.raw), "payload_b64": "abc="},
                   {**record(self.raw), "hex": None, "payload_b64": "!"},
                   {**record(self.raw), "hex": None, "payload_b64": 4},
                   {**record(self.raw), "hex": "aa" * 65536},
                   {**record(self.raw), "direction": "sideways"},
                   {**record(self.raw), "monotonic_ns": True},
                   {**record(self.raw), "type": "odd"},
                   {**record(self.raw), "type": "event"},
                   {**record(self.raw), "type": "event", "event": "opened"},
                   record(frame(), direction="tx")]
        output = io.StringIO()
        result = replay.audit_capture(self.journal(entries), output)
        self.assertFalse(result["ok"])
        self.assertEqual(result["sample_count"], 32)
        self.assertTrue(result["csv_is_partial"])
        self.assertEqual(result["event_count"], 2)

    def test_line_corruption_oversize_and_no_newline(self):
        path = self.root / "broken.jsonl"
        path.write_bytes(b"\xff\nnot json\n" + b"x" * (replay.MAX_LINE_BYTES + 1) + b"\n" +
                         json.dumps(record(self.raw)).encode())
        result = replay.audit_capture(path)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_count"], 4)
        self.assertEqual(result["sample_count"], 32)

    def test_monotonic_regression_invalidates_duration_utc_only_warns(self):
        one, two = record(self.raw, 2), record(frame(group()[0], 2), 1)
        result = replay.audit_capture(self.journal([one, two]))
        self.assertFalse(result["ok"])
        self.assertIsNone(result["host_receive_duration_s"])
        self.assertIn("UTC clock regressed", result["warnings"])
        two["monotonic_ns"] = 3
        self.assertTrue(replay.audit_capture(self.journal([one, two]))["ok"])

    def test_empty_capture_is_invalid(self):
        path = self.root / "empty.jsonl"
        path.write_bytes(b"")
        result = replay.audit_capture(path)
        self.assertFalse(result["ok"])
        self.assertIn("no journal records", str(result["errors"]))

    def test_diagnostics_remain_bounded(self):
        result = replay.audit_capture(self.journal([{}] * 120))
        self.assertEqual(result["error_count"], 120)
        self.assertEqual(len(result["errors"]), replay.MAX_DIAGNOSTICS)

    def test_contact_information_does_not_remove_raw_counts(self):
        result = replay.audit_capture(self.journal([
            record(frame(group(contact=0xff)[0], 1), 1),
            record(frame(group(contact=None)[0], 2), 2)]))
        self.assertEqual(result["sample_count"], 64)
        self.assertEqual(result["contact_flagged_blocks"], 1)
        self.assertEqual(result["contact_unknown_blocks"], 1)

    def test_cli_output_close_failure_is_nonzero_without_traceback(self):
        source = self.journal([record(self.raw)])
        original_open = Path.open

        class BadClose(io.StringIO):
            def fileno(self):
                return 0

            def close(self):
                raise OSError("close failed")

        sink = BadClose()

        def open_path(path, *args, **kwargs):
            return sink if path.name == "badclose.csv" else original_open(path, *args, **kwargs)

        errors = io.StringIO()
        with mock.patch("seca_replay.Path.open", open_path), mock.patch("seca_replay.os.fsync"), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(errors):
            status = replay.main([str(source), "--csv", str(self.root / "badclose.csv")])
        self.assertEqual(status, 2)
        self.assertIn("output close failed", errors.getvalue())
        self.assertNotIn("Traceback", errors.getvalue())

    def test_cli_success_failure_exclusive_output_and_storage_error(self):
        source = self.journal([record(self.raw)])
        csv_path, summary = self.root / "out.csv", self.root / "summary.json"
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(replay.main([str(source), "--csv", str(csv_path), "--summary", str(summary)]), 0)
            self.assertEqual(replay.main([str(source), "--csv", str(csv_path)]), 2)
            self.assertEqual(replay.main([str(source), "--csv", str(self.root / "same"), "--summary", str(self.root / "same")]), 2)
            self.assertEqual(replay.main([str(self.root / "absent")]), 1)
            with mock.patch("seca_replay.os.fsync", side_effect=OSError("disk failed")):
                self.assertEqual(replay.main([str(source), "--summary", str(self.root / "new.json")]), 2)
        self.assertEqual(json.loads(summary.read_text())["sample_count"], 32)
        self.assertEqual(len(csv_path.read_text().splitlines()), 33)

    def test_reports_allowlist_config_and_preserve_private_raw_metadata(self):
        source = self.session([record(self.raw)])
        path = source / "session.json"
        metadata = json.loads(path.read_text())
        metadata.update({"network_password": "TOP_SECRET", "device_info": {
            "sn": "synthetic", "samplerate": "32000", "wlanpw": "DEVICE_SECRET",
            "gain": {"nested_password": "NESTED_SECRET"}}, "observed_config": {
            "samplerate": "32000", "gain": "6", "wlanpw": "OBSERVED_SECRET"}})
        path.write_text(json.dumps(metadata))
        original = path.read_bytes()
        report = replay.audit_capture(source)
        encoded = json.dumps(report)
        self.assertNotIn("SECRET", encoded)
        self.assertEqual(report["metadata"]["device_info"], {"sn": "synthetic", "samplerate": "32000"})
        self.assertEqual(report["metadata"]["observed_config"], {"samplerate": "32000", "gain": "6"})
        summary = self.root / "public_summary.json"
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(replay.main([str(source), "--summary", str(summary)]), 0)
        self.assertNotIn("SECRET", stdout.getvalue() + summary.read_text())
        self.assertEqual(path.read_bytes(), original)
        chunk = self.root / "chunk.json"
        chunk.write_text(json.dumps({"schema_version": 1, "sample_rate_hz": 4000,
                                    "unknown_credential": "CLOUD_SECRET", "records": []}))
        self.assertNotIn("SECRET", json.dumps(replay.audit_capture(chunk)))
        metadata["requested_sample_rate_hz"] = {"password": "INVALID_SECRET"}
        path.write_text(json.dumps(metadata))
        self.assertNotIn("SECRET", json.dumps(replay.audit_capture(source)))

    def test_explicit_user_stop_is_clean_but_cleanup_error_is_not(self):
        source = self.session([record(self.raw)], success=False)
        path = source / "records.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[-1].update(end_reason="interrupted", errors=[])
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        result = replay.audit_capture(source)
        self.assertTrue(result["ok"])
        self.assertTrue(result["user_stopped"])
        self.assertEqual(result["closure"], "clean")
        self.assertFalse(rows[-1]["success"])
        rows[-1]["errors"] = ["standby failed"]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        self.assertFalse(replay.audit_capture(source)["ok"])
        del rows[-1]["errors"]
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        self.assertFalse(replay.audit_capture(source)["ok"])


if __name__ == "__main__":
    unittest.main()
