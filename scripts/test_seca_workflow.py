#!/usr/bin/env python3
"""Independent synthetic contracts for USB capture and lossless offline replay.

No real recordings or device access are used. The fixture encoder deliberately
uses a bit-string representation separate from the production decoder.
Run with: python3 -m unittest discover -s scripts -p 'test_seca*.py'
"""

import base64
import csv
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import seca_replay as replay
import seca_usb_capture as capture
import seca_upload as upload


RATES = (1000, 2000, 4000, 8000, 16000, 32000)
WIDTHS = (0, 4, 8, 12, 16, 20, 24)
CHANNEL_NAMES = ("V1", "V2", "V3", "V4", "V5", "V6", "I", "II")
CHANNEL_IDS = (0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xCA, 0xCB)
WIRE_ORDER = (5, 6, 7, 1, 2, 3, 4, 0)
BASES = (-8388608, 8388607, 0, -1, 1, -7654321, 7654321, -1234567)


def synthetic_frame(payload, counter=0):
    """Encode a response frame without using either implementation's helpers."""
    body = (b"\x55\xaa" + (len(payload) + 11).to_bytes(2, "big")
            + (counter % (1 << 32)).to_bytes(4, "big") + b"\x20" + payload)
    return body + ((65536 - sum(body)) % 65536).to_bytes(2, "big")


def synthetic_channel(width, base, seed=0):
    """Return encoded channel body plus expected signed sample values."""
    if width == 0:
        deltas = [0] * 31
        packed = b""
    else:
        minimum, maximum = -(1 << (width - 1)), (1 << (width - 1)) - 1
        choices = (minimum, maximum, -1, 0, 1, minimum + 1, maximum - 1)
        deltas = [choices[(index + seed) % len(choices)] for index in range(31)]
        bit_string = "".join(format(delta % (1 << width), f"0{width}b")
                             for delta in deltas)
        byte_count = 48 if width == 12 else (len(bit_string) + 7) // 8
        # Nonzero padding exposes a decoder which accidentally consumes padding.
        bit_string += ("1010" * 4)[:byte_count * 8 - len(bit_string)]
        packed = int(bit_string, 2).to_bytes(byte_count, "big")
    samples = [base]
    for delta in deltas:
        samples.append(samples[-1] + delta)
    return bytes([width]) + base.to_bytes(3, "big", signed=True) + packed, tuple(samples)


def synthetic_block(widths=(24, 20, 16, 12, 8, 4, 0, 12), mask=0, seed=0):
    channels = []
    expected = []
    for index, width in enumerate(widths):
        encoded, samples = synthetic_channel(width, BASES[index], seed + index)
        channels.append(bytes([CHANNEL_IDS[index]]) + encoded)
        expected.append(samples)
    payload = b""
    if mask is not None:
        wire_contact = ((mask << 2) | (mask >> 6)) & 255
        payload += b"O" + bytes([wire_contact])
    payload += b"".join(channels[index] for index in WIRE_ORDER)
    return payload, tuple(expected)


def wire_record(packet, *, direction="rx", tick=0):
    return {"type": "wire", "direction": direction, "hex": packet.hex(),
            "unix_ns": 1700000000000000000 + tick,
            "monotonic_ns": 1000000000 + tick}


def write_legacy(path, packets):
    """Write the old probe format, which has no session-end convention."""
    with path.open("w", encoding="utf-8") as output:
        for index, packet in enumerate(packets):
            record = wire_record(packet, tick=index * 1000000000)
            del record["type"]
            output.write(json.dumps(record) + "\n")


class IndependentDecoderTests(unittest.TestCase):
    def test_each_width_signed_extremes_and_nonzero_padding(self):
        for width in WIDTHS:
            with self.subTest(width=width):
                payload, expected = synthetic_block((width,) * 8, mask=None)
                decoded = replay.decode_packet(synthetic_frame(payload, counter=42))
                self.assertEqual(len(decoded.blocks), 1)
                block = decoded.blocks[0]
                self.assertEqual(block.counter, 42)
                self.assertEqual(block.channels, expected)
                self.assertEqual(block.widths, (width,) * 8)
                self.assertIsNone(block.lead_off_mask)

    def test_mixed_widths_multiple_blocks_counter_wrap_and_contacts(self):
        expected = []
        payload = b""
        for seed, mask in enumerate((1, 128, 255)):
            encoded, samples = synthetic_block(mask=mask, seed=seed)
            payload += encoded
            expected.append(samples)
        decoded = replay.decode_packet(synthetic_frame(payload, counter=0))
        self.assertEqual([block.counter for block in decoded.blocks],
                         [0xFFFFFFFE, 0xFFFFFFFF, 0])
        self.assertEqual([block.lead_off_mask for block in decoded.blocks], [1, 128, 255])
        self.assertEqual([block.channels for block in decoded.blocks], expected)

    def test_every_contact_bit_preserves_raw_samples(self):
        for index in range(8):
            with self.subTest(channel=index):
                payload, expected = synthetic_block(mask=1 << index)
                block = replay.decode_packet(synthetic_frame(payload)).blocks[0]
                self.assertEqual(block.lead_off_mask, 1 << index)
                self.assertEqual(block.channels, expected)

    def test_complete_groups_then_malformed_tail_rejects_packet(self):
        payload, _ = synthetic_block()
        malformed = (b"\xc6\x0c\x00", b"?", b"A\x00\x03x",
                     b"\xc6\x07\x00\x00\x00", b"P\x01")
        for tail in malformed:
            with self.subTest(tail=tail.hex()):
                with self.assertRaises(replay.ReplayError):
                    replay.decode_packet(synthetic_frame(payload + tail))

    def test_framing_mutation_detected_even_with_valid_signal_payload(self):
        payload, _ = synthetic_block()
        good = synthetic_frame(payload)
        variants = [good[:-1], good + b"\x00", b"\x00" + good[1:]]
        corrupted = bytearray(good)
        corrupted[30] ^= 0x80
        variants.append(bytes(corrupted))
        for raw in variants:
            with self.subTest(length=len(raw), prefix=raw[:4].hex()):
                with self.assertRaises(replay.ReplayError):
                    replay.decode_packet(raw)


class ReplayWorkflowTests(unittest.TestCase):
    def test_all_six_rates_export_exact_counts_and_packet_clocks(self):
        payload, expected = synthetic_block(mask=255)
        second, second_expected = synthetic_block(mask=0, seed=3)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            write_legacy(path, [synthetic_frame(payload, 7), synthetic_frame(second, 8)])
            for rate in RATES:
                with self.subTest(rate=rate):
                    output = io.StringIO(newline="")
                    summary = replay.audit_capture(path, csv_file=output, sample_rate_hz=rate)
                    self.assertTrue(summary["ok"], summary)
                    self.assertEqual(summary["sample_count"], 64)
                    self.assertEqual(summary["requested_sample_rate_hz"], rate)
                    self.assertEqual(summary["nominal_sample_duration_s"], 64 / rate)
                    self.assertEqual(summary["host_receive_duration_s"], 1.0)
                    self.assertEqual(summary["csv_rows"], 64)
                    self.assertNotIn("delivered_sample_rate_hz", summary)
                    reader = csv.DictReader(io.StringIO(output.getvalue()))
                    self.assertEqual(reader.fieldnames, ["record_index", "block_counter",
                        "block_sample_index", "rx_unix_ns", "rx_monotonic_ns", "lead_off_mask",
                        "acquisition", *CHANNEL_NAMES])
                    rows = list(reader)
                    self.assertEqual(len(rows), 64)
                    for packet_index, channels in enumerate((expected, second_expected)):
                        for index in range(32):
                            row = rows[packet_index * 32 + index]
                            self.assertEqual(tuple(int(row[name]) for name in CHANNEL_NAMES),
                                             tuple(channel[index] for channel in channels))
                            self.assertEqual(int(row["block_counter"]), 7 + packet_index)
                            self.assertEqual(int(row["block_sample_index"]), index)
                            self.assertEqual(int(row["rx_unix_ns"]),
                                             1700000000000000000 + packet_index * 1000000000)
                            self.assertEqual(int(row["rx_monotonic_ns"]),
                                             1000000000 + packet_index * 1000000000)
                            self.assertEqual(int(row["lead_off_mask"]), (255, 0)[packet_index])
                            self.assertEqual(row["acquisition"], "")

    def test_counter_audit_distinguishes_wrap_gap_duplicate_and_regression(self):
        payload, _ = synthetic_block()
        counters = (0xFFFFFFFE, 0xFFFFFFFF, 0, 3, 3, 2)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            write_legacy(path, [synthetic_frame(payload, n) for n in counters])
            summary = replay.audit_capture(path)
        self.assertEqual(summary["sample_count"], len(counters) * 32)
        self.assertEqual(summary["block_count"], len(counters))
        self.assertEqual(summary["counter_wraps"], 1)
        self.assertEqual(summary["missing_blocks"], 2)
        self.assertEqual(summary["sequence_errors"], 2)

    def test_unknown_rate_stays_unknown_despite_host_receive_interval(self):
        payload, _ = synthetic_block()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            write_legacy(path, [synthetic_frame(payload, n) for n in (1, 2)])
            summary = replay.audit_capture(path)
        self.assertTrue(summary["ok"], summary)
        self.assertIsNone(summary["requested_sample_rate_hz"])
        self.assertIsNone(summary["nominal_sample_duration_s"])
        self.assertEqual(summary["host_receive_duration_s"], 1.0)
        self.assertEqual(summary["closure"], "unknown")

    def test_malformed_packet_exports_no_valid_looking_prefix(self):
        payload, _ = synthetic_block()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            write_legacy(path, [synthetic_frame(payload + b"?")])
            output = io.StringIO(newline="")
            summary = replay.audit_capture(path, csv_file=output)
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["sample_count"], 0)
        self.assertEqual(summary["csv_rows"], 0)
        self.assertTrue(summary["csv_is_partial"])
        self.assertEqual(list(csv.DictReader(io.StringIO(output.getvalue()))), [])


class SyntheticTransport:
    """Stateful fake device: settings are observed before signal frames begin."""

    def __init__(self, packets):
        self.packets = list(packets)
        self.responses = []
        self.commands = []
        self.settings = {"sn": "synthetic-ct331", "samplerate": "4000", "gain": "6",
                         "openlead": "0", "mode": "standby"}
        self.closed = False
        self.elapsed = 0.0

    def identify(self):
        return dict(self.settings)

    def send_packet(self, packet):
        self.commands.append(packet)
        if packet[9] == ord("W"):
            text = packet[12:-2].decode("ascii").rstrip("\x00")
            key, value = text.strip().split("=", 1)
            self.settings[key] = value
            if key == "mode" and value == "run":
                self.responses.extend(self.packets)
        elif packet[9] == ord("R"):
            text = "\n".join(f"{key}={value}" for key, value in self.settings.items()).encode()
            self.responses.append(synthetic_frame(b"A" + len(text).to_bytes(2, "big") + text))

    def read_packet(self):
        self.elapsed += 0.001
        return self.responses.pop(0) if self.responses else synthetic_frame(b"")

    def close(self):
        self.closed = True


def run_synthetic_capture(config, fake):
    """Advance acquisition deadlines from fake reads, avoiding scheduler races."""
    with patch.object(capture.time, "monotonic", new=lambda: fake.elapsed):
        return capture.run_capture(config, transport_factory=lambda config: fake)


class CaptureReplayWorkflowTests(unittest.TestCase):
    def test_capture_journal_replays_full_rate_counts_without_hardware(self):
        payload, expected = synthetic_block(mask=1)
        second, second_expected = synthetic_block(mask=128, seed=2)
        packets = [synthetic_frame(payload, 31), synthetic_frame(second, 32)]
        for rate in RATES:
            with self.subTest(rate=rate), tempfile.TemporaryDirectory() as directory:
                destination = Path(directory) / "new-capture"
                fake = SyntheticTransport(packets)
                config = capture.CaptureConfig(output_dir=destination,
                    volume=Path(directory) / "unused-device", sample_rate_hz=rate,
                    duration_s=0.025, config_wait_s=0.01, command_delay_s=0,
                    idle_sleep_s=0)
                captured = run_synthetic_capture(config, fake)
                self.assertEqual(captured["acquisition_evidence"], "raw_transport_only")
                self.assertEqual(captured["malformed_rx_frames"], 0)
                self.assertTrue(fake.closed)
                self.assertEqual(fake.settings["mode"], "standby")
                metadata = json.loads((destination / "session.json").read_text())
                self.assertEqual(metadata["requested_sample_rate_hz"], rate)
                self.assertEqual(metadata["transport"], "usb")
                records = [json.loads(line) for line in
                           (destination / "records.jsonl").read_text().splitlines()]
                received = [bytes.fromhex(record["hex"]) for record in records
                            if record["type"] == "wire" and record["direction"] == "rx"]
                for raw in packets:
                    self.assertEqual(received.count(raw), 1)
                self.assertTrue(any(len(raw) == 11 for raw in received))
                self.assertEqual(records[-1]["event"], "session_end")
                self.assertTrue(records[-1]["success"])
                output = io.StringIO(newline="")
                summary = replay.audit_capture(destination, csv_file=output)
                self.assertTrue(summary["ok"], summary)
                self.assertEqual(summary["closure"], "clean")
                self.assertEqual(summary["sample_count"], 64)
                self.assertEqual(summary["acquisition_sample_count"], 64)
                self.assertEqual(summary["setup_sample_count"], 0)
                self.assertEqual(summary["requested_sample_rate_hz"], rate)
                rows = list(csv.DictReader(io.StringIO(output.getvalue())))
                self.assertEqual(len(rows), 64)
                for packet_index, channels in enumerate((expected, second_expected)):
                    for index in range(32):
                        self.assertEqual(rows[packet_index * 32 + index]["acquisition"], "true")
                        self.assertEqual(tuple(int(rows[packet_index * 32 + index][name])
                                               for name in CHANNEL_NAMES),
                                         tuple(channel[index] for channel in channels))

    def test_idle_capture_reports_raw_transport_without_acquisition_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "capture"
            fake = SyntheticTransport([])
            config = capture.CaptureConfig(output_dir=destination, volume=Path(directory),
                duration_s=0.005, config_wait_s=0.01, command_delay_s=0, idle_sleep_s=0)
            captured = run_synthetic_capture(config, fake)
            self.assertTrue(captured["success"])
            self.assertEqual(captured["acquisition_evidence"], "raw_transport_only")
            self.assertGreater(captured["idle_rx_frames"], 0)
            self.assertEqual(captured["malformed_rx_frames"], 0)
            summary = replay.audit_capture(destination)
            self.assertEqual(summary["sample_count"], 0)
            self.assertTrue(any("No ECG samples" in warning for warning in summary["warnings"]))

    def test_capture_refuses_existing_directory_without_sending_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "existing"
            destination.mkdir()
            sentinel = destination / "records.jsonl"
            sentinel.write_bytes(b"keep this archive\n")
            fake = SyntheticTransport([])
            config = capture.CaptureConfig(output_dir=destination, volume=Path(directory),
                                            duration_s=0.01, command_delay_s=0)
            with self.assertRaises((capture.CaptureError, FileExistsError)):
                capture.run_capture(config, transport_factory=lambda config: fake)
            self.assertEqual(sentinel.read_bytes(), b"keep this archive\n")
            self.assertEqual(fake.commands, [])

    def test_capture_prepare_cloud_replay_preserves_wire_bytes_and_metadata(self):
        payload, expected = synthetic_block(mask=64)
        packet = synthetic_frame(payload, 123)
        setup_payload, setup_expected = synthetic_block(mask=0, seed=4)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "capture"
            queue = Path(directory) / "queue"
            fake = SyntheticTransport([packet])
            # A stale setup block precedes the configuration acknowledgement;
            # its unrelated counter and samples must survive without a fake gap.
            fake.responses.append(synthetic_frame(setup_payload, 9000))
            config = capture.CaptureConfig(output_dir=destination,
                volume=Path(directory) / "unused-device", sample_rate_hz=32000,
                contact_detection=True, duration_s=0.015, config_wait_s=0.01,
                command_delay_s=0, idle_sleep_s=0)
            run_synthetic_capture(config, fake)
            journal = (destination / "records.jsonl").read_bytes()
            manifest = upload.prepare(destination, queue,
                                      "10000000-0000-0000-0000-000000000001")
            self.assertEqual(manifest["batch_count"], 1)
            batch = json.loads((queue / "0000000000.json").read_bytes())
            self.assertEqual((batch["sample_rate_hz"], batch["transport"], batch["gain"]),
                             (32000, "usb", 6))
            self.assertTrue(batch["contact_detection"])
            self.assertEqual(batch["end_reason"], "usb_capture_complete")
            originals = [row for row in map(json.loads, journal.splitlines())
                         if row["type"] == "wire"]
            self.assertEqual(len(batch["records"]), len(originals))
            for original, exported in zip(originals, batch["records"]):
                self.assertEqual(base64.b64decode(exported["payload_b64"]),
                                 bytes.fromhex(original["hex"]))
                for key in ("direction", "unix_ns", "monotonic_ns", "acquisition"):
                    self.assertEqual(exported[key], original[key])
            self.assertEqual((destination / "records.jsonl").read_bytes(), journal)
            output = io.StringIO(newline="")
            report = replay.audit_capture(queue / "0000000000.json", csv_file=output)
            self.assertTrue(report["ok"], report)
            self.assertEqual(report["sample_count"], 64)
            self.assertEqual(report["acquisition_sample_count"], 32)
            self.assertEqual(report["setup_sample_count"], 32)
            self.assertEqual(report["sequence_errors"], 0)
            self.assertEqual(report["missing_blocks"], 0)
            self.assertIsNone(report["nominal_sample_duration_s"])
            self.assertEqual(report["acquisition_nominal_sample_duration_s"], 32 / 32000)
            rows = list(csv.DictReader(io.StringIO(output.getvalue())))
            self.assertEqual(len(rows), 64)
            for group_index, channels in enumerate((setup_expected, expected)):
                for index in range(32):
                    row = rows[group_index * 32 + index]
                    self.assertEqual(row["acquisition"], ("false", "true")[group_index])
                    self.assertEqual(tuple(int(row[name]) for name in CHANNEL_NAMES),
                                     tuple(channel[index] for channel in channels))


class ReplayCliTests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run([sys.executable, str(Path(replay.__file__).resolve()),
                               *(str(argument) for argument in arguments)],
                              capture_output=True, text=True, timeout=10)

    def test_malformed_inputs_exit_nonzero_with_diagnostic(self):
        payload, _ = synthetic_block()
        good = synthetic_frame(payload)
        variants = (b'{"hex": "' + good.hex().encode("ascii"),
                    b'{"direction":"rx","hex":"not hex"}\n',
                    json.dumps(wire_record(good[:-1])).encode("ascii") + b"\n")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broken.jsonl"
            for raw in variants:
                with self.subTest(prefix=raw[:40]):
                    path.write_bytes(raw)
                    result = self.run_cli(path)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertTrue(result.stderr.strip() or result.stdout.strip())

    def test_existing_export_files_are_not_overwritten(self):
        payload, _ = synthetic_block()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.jsonl"
            write_legacy(path, [synthetic_frame(payload)])
            for option in ("--csv", "--summary"):
                output = Path(directory) / (option[2:] + ".sentinel")
                output.write_bytes(b"preserve existing output\n")
                result = self.run_cli(path, option, output)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(output.read_bytes(), b"preserve existing output\n")


if __name__ == "__main__":
    unittest.main()
