#!/usr/bin/env python3
"""Deterministic USB capture tests. Never open a physical seca endpoint."""

from dataclasses import replace
import ctypes
import json
import time
import mmap
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import seca_usb_capture as capture


def frame(payload=b"", counter=0):
    head = b"\x55\xaa" + (11 + len(payload)).to_bytes(2, "big") + counter.to_bytes(4, "big") + b"\x20" + payload
    return head + ((-sum(head)) & 65535).to_bytes(2, "big")


def config_frame(settings):
    text = "\r\n".join(f"{key}={value}" for key, value in settings.items()).encode()
    return frame(b"A" + len(text).to_bytes(2, "big") + text)


class FakeTransport:
    def __init__(self, config):
        self.config = config
        self.settings = {}
        self.pending = []
        self.commands = []
        self.closed = False
        self.running = False
        self.read_error = None
        self.standby_error = False
        self.close_error = False
        self.confirm = True
        self.identity = {"sn": "synthetic-test-device", "mode": "standby"}

    def identify(self):
        return self.identity

    def send_packet(self, packet):
        kind = packet[9:10]
        text = packet[12:-2].decode()
        self.commands.append((kind, text))
        if kind == b"R" and self.confirm:
            self.pending.append(config_frame(self.settings))
        if kind == b"W":
            key, _, value = text.partition("=")
            self.settings[key] = value
        if text == "mode=run":
            self.running = True
        if text == "mode=standby":
            self.running = False
            if self.standby_error:
                raise OSError("standby disconnected")

    def read_packet(self):
        if self.pending:
            return self.pending.pop(0)
        if self.running and self.read_error:
            raise self.read_error
        return frame()

    def close(self):
        self.closed = True
        if self.close_error:
            raise OSError("close failed")


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "session"
        self.config = capture.CaptureConfig(self.path, Path("/fake-device"), duration_s=.002,
                                           config_wait_s=.002, command_delay_s=0, idle_sleep_s=.001)
        self.device = FakeTransport(self.config)

    def run_fake(self, **kwargs):
        return capture.run_capture(self.config, transport_factory=lambda _: self.device, **kwargs)

    def records(self):
        return [json.loads(line) for line in (self.path / "records.jsonl").read_text().splitlines()]

    def test_success_identity_configuration_and_standby(self):
        summary = self.run_fake()
        self.assertTrue(summary["success"])
        self.assertEqual(summary["end_reason"], "duration_elapsed")
        self.assertTrue(self.device.closed)
        self.assertEqual(self.device.commands[-1], (b"W", "mode=standby"))
        meta = json.loads((self.path / "session.json").read_text())
        self.assertEqual(meta["device_info"]["sn"], "synthetic-test-device")
        self.assertEqual(meta["requested_sample_rate_hz"], 32000)
        self.assertTrue(meta["configuration_confirmed"])
        self.assertEqual(meta["observed_config"]["samplerate"], "32000")
        records = self.records()
        self.assertEqual(records[-1]["event"], "session_end")
        self.assertTrue(records[-1]["success"])
        self.assertTrue(any(r.get("hex") == frame().hex() for r in records))
        confirmed = next(r for r in records if r.get("event") == "configuration_confirmed")
        self.assertEqual(confirmed["details"]["sample_rate_hz"], 32000)
        self.assertTrue(all(r["unix_ns"] > 0 and r["monotonic_ns"] > 0 for r in records))
        wire = [r for r in records if r.get("type") == "wire"]
        self.assertTrue(all(isinstance(r["acquisition"], bool) for r in wire))
        self.assertTrue(all(not r["acquisition"] for r in wire if r["direction"] == "tx"))
        received = [r for r in wire if r["direction"] == "rx"]
        self.assertFalse(received[0]["acquisition"])
        self.assertTrue(all(r["acquisition"] for r in received[1:]))

    def test_contact_detection_and_all_rates(self):
        for rate in capture.RATES:
            with self.subTest(rate=rate):
                path = Path(self.temp.name) / str(rate)
                config = replace(self.config, output_dir=path, sample_rate_hz=rate, contact_detection=True)
                fake = FakeTransport(config)
                self.assertTrue(capture.run_capture(config, transport_factory=lambda _: fake)["success"])
                self.assertIn((b"W", "openlead=1"), fake.commands)

    def test_no_output_overwrite_before_device_access(self):
        self.path.mkdir()
        factory = MagicMock()
        with self.assertRaisesRegex(capture.CaptureError, "already exists"):
            capture.run_capture(self.config, transport_factory=factory)
        factory.assert_not_called()
        with self.assertRaises(FileExistsError):
            capture.Journal(self.path, {})

    def test_identity_rejected_before_writes(self):
        for identity in ({}, {"sn": "test", "mode": "run"}):
            self.device.identity = identity
            with self.assertRaises(capture.CaptureError):
                self.run_fake()
            self.assertEqual(self.device.commands, [])
            self.assertTrue(self.device.closed)
            self.assertFalse(self.path.exists())

    def test_configuration_unconfirmed_never_runs(self):
        self.device.confirm = False
        with self.assertRaisesRegex(capture.CaptureError, "configuration not confirmed"):
            self.run_fake()
        self.assertNotIn((b"W", "mode=run"), self.device.commands)
        self.assertEqual(self.device.commands[-1], (b"W", "mode=standby"))
        self.assertFalse(self.records()[-1]["success"])

    def test_mismatched_rate_never_starts_acquisition(self):
        original = self.device.send_packet
        def send(packet):
            original(packet)
            if packet[9:10] == b"R":
                wrong = dict(self.device.settings, samplerate="4000")
                self.device.pending[:] = [config_frame(wrong)]
        self.device.send_packet = send
        with self.assertRaisesRegex(capture.CaptureError, "configuration not confirmed"):
            self.run_fake()
        self.assertNotIn((b"W", "mode=run"), self.device.commands)
        self.assertTrue(all(not r["acquisition"] for r in self.records() if r["type"] == "wire"))
        metadata = json.loads((self.path / "session.json").read_text())
        self.assertFalse(metadata["configuration_confirmed"])
        self.assertEqual(metadata["observed_config"]["samplerate"], "4000")

    def test_failed_run_write_never_marks_acquisition(self):
        original = self.device.send_packet
        def send(packet):
            original(packet)
            if packet[12:-2] == b"mode=run":
                raise OSError("run write failed")
        self.device.send_packet = send
        with self.assertRaisesRegex(capture.CaptureError, "run write failed"):
            self.run_fake()
        self.assertTrue(all(not r["acquisition"] for r in self.records() if r["type"] == "wire"))
        self.assertEqual(self.device.commands[-1][1], "mode=standby")

    def test_disconnect_is_loud_and_cleanup_attempted(self):
        self.device.read_error = OSError("USB disconnected")
        with self.assertRaisesRegex(capture.CaptureError, "USB disconnected"):
            self.run_fake()
        self.assertEqual(self.device.commands[-1][1], "mode=standby")
        self.assertTrue(self.device.closed)
        self.assertFalse(self.records()[-1]["success"])

    def test_timeout_partial_bytes_are_retained(self):
        partial = b"\x55\xaa\x10\x00pending-data"
        self.device.read_error = capture.PartialReadError("USB timeout", partial)
        with self.assertRaisesRegex(capture.CaptureError, "USB timeout"):
            self.run_fake()
        self.assertTrue(any(r.get("hex") == partial.hex() for r in self.records()))
        self.assertTrue(any(r.get("event") == "partial_read" for r in self.records()))

    def test_malformed_packets_retained(self):
        original = self.device.send_packet
        def send(packet):
            original(packet)
            if packet[12:-2] == b"mode=run":
                self.device.pending.append(b"not a frame")
        self.device.send_packet = send
        with self.assertRaisesRegex(capture.CaptureError, "1 malformed RX"):
            self.run_fake()
        self.assertTrue(any(r.get("hex") == b"not a frame".hex() for r in self.records()))
        self.assertGreater(self.records()[-1]["valid_rx_frames"], 0)
        self.assertEqual(self.records()[-1]["malformed_rx_frames"], 1)
        self.assertEqual(self.records()[-1]["acquisition_evidence"], "raw_transport_only")
        self.assertTrue(any(r.get("event") == "malformed_frame" for r in self.records()))

    def test_storage_failure_does_not_suppress_standby(self):
        class BrokenJournal(capture.Journal):
            def wire(inner, direction, payload, *, acquisition=False):
                if direction == "rx":
                    raise OSError("disk full")
                super().wire(direction, payload, acquisition=acquisition)
        with self.assertRaisesRegex(capture.CaptureError, "disk full"):
            self.run_fake(journal_factory=BrokenJournal)
        self.assertEqual(self.device.commands[-1][1], "mode=standby")
        self.assertTrue(self.device.closed)
        self.assertFalse(self.records()[-1]["success"])

    def test_unwritable_journal_cleanup_failure_reported(self):
        class BrokenJournal(capture.Journal):
            def wire(inner, direction, payload, *, acquisition=False):
                if self.device.commands:
                    raise OSError("disk removed")
                super().wire(direction, payload, acquisition=acquisition)
            def event(inner, name, **kwargs):
                if name == "session_end":
                    raise OSError("end disk removed")
                super().event(name, **kwargs)
            def close(inner):
                super().close()
                raise OSError("close disk removed")
        self.device.standby_error = True
        self.device.close_error = True
        with self.assertRaises(capture.CaptureError) as caught:
            self.run_fake(journal_factory=BrokenJournal)
        message = str(caught.exception)
        for expected in ("standby journal", "standby cleanup failed", "transport close failed",
                         "session_end journal failed", "journal close failed"):
            self.assertIn(expected, message)
        self.assertEqual(self.device.commands[-1][1], "mode=standby")

    def test_stop_event_during_acquisition(self):
        stopped = threading.Event()
        original = self.device.read_packet
        def read():
            packet = original()
            if self.device.running:
                stopped.set()
            return packet
        self.device.read_packet = read
        result = self.run_fake(stop_event=stopped)
        self.assertFalse(result["success"])
        self.assertEqual(result["end_reason"], "interrupted")
        self.assertEqual(self.device.commands[-1][1], "mode=standby")

    def test_stop_before_start_and_pacing(self):
        stopped = threading.Event()
        stopped.set()
        self.config = replace(self.config, command_delay_s=.001)
        result = self.run_fake(stop_event=stopped)
        self.assertEqual(result["end_reason"], "interrupted")
        self.assertEqual(self.device.commands, [])

    def test_command_delay_executes_without_missing_cleanup(self):
        self.config = replace(self.config, command_delay_s=.00001)
        self.assertTrue(self.run_fake()["success"])

    def test_native_platform_refused(self):
        with patch.object(capture.sys, "platform", "linux"):
            with self.assertRaisesRegex(capture.CaptureError, "macOS"):
                capture.run_capture(self.config)
        self.assertFalse(self.path.exists())

    def test_native_factory_lock_and_open_failure(self):
        lock = MagicMock()
        with patch.object(capture.sys, "platform", "darwin"), patch.object(capture, "DeviceLock", return_value=lock), patch.object(capture, "BoundedUSB", side_effect=OSError("open failed")):
            with self.assertRaisesRegex(capture.CaptureError, "open failed"):
                capture.run_capture(self.config)
        lock.acquire.assert_called_once()
        lock.close.assert_called_once()

    def test_validation(self):
        for fields in ({"sample_rate_hz": 500}, {"duration_s": 0}, {"duration_s": float("inf")},
                       {"duration_s": 86401}, {"io_timeout_s": -1}, {"config_wait_s": 0},
                       {"command_delay_s": -1}, {"idle_sleep_s": float("nan")}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                replace(self.config, **fields).validate()


class ProtocolJournalTests(unittest.TestCase):
    def test_known_read_command(self):
        self.assertEqual(capture.command_packet("", b"R").hex(), "55aa000e1234567800520000fd8d")
        with self.assertRaises(ValueError):
            capture.command_packet("", b"X")
        with self.assertRaises(ValueError):
            capture.command_packet("a" * 500)
        with self.assertRaises(UnicodeEncodeError):
            capture.command_packet("é")

    def test_parse_settings_and_response_validation(self):
        self.assertEqual(capture.parse_settings("SN = test\r\nGAIN=6\x00\nnoise\n=x"), {"sn": "test", "gain": "6"})
        self.assertEqual(capture.configuration_reply(config_frame({"samplerate": 32000})), {"samplerate": "32000"})
        self.assertIsNone(capture.configuration_reply(frame(b"A\x00\x04x")))
        self.assertIsNone(capture.configuration_reply(frame()))
        samples = [(b"", "short_frame"), (b"x" * 11, "bad_magic"),
                   (b"\x55\xaa\x00\x20" + b"x" * 7, "length_mismatch"),
                   (frame()[:-2] + b"\0\0", "checksum_mismatch")]
        for packet, error in samples:
            self.assertEqual(capture.frame_problem(packet), error)
        self.assertIsNone(capture.frame_problem(frame()))

    def test_partial_write_and_sync_failures(self):
        with patch.object(capture.os, "write", side_effect=[2, 1]), patch.object(capture.os, "fsync") as sync:
            capture._write_all(11, b"abc")
            sync.assert_called_once_with(11)
        with patch.object(capture.os, "write", return_value=0):
            with self.assertRaisesRegex(OSError, "no progress"):
                capture._write_all(11, b"abc")
        with patch.object(capture.os, "write", return_value=3), patch.object(capture.os, "fsync", side_effect=OSError("sync failed")):
            with self.assertRaisesRegex(OSError, "sync failed"):
                capture._write_all(11, b"abc")

    def test_journal_directory_sync_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(capture, "_sync_directory", side_effect=OSError("dir sync")):
                with self.assertRaisesRegex(OSError, "dir sync"):
                    capture.Journal(Path(directory) / "new", {})

    def test_lock_contention_and_close(self):
        lock = capture.DeviceLock()
        with patch.object(capture.os, "open", return_value=42), patch.object(capture.fcntl, "flock"), patch.object(capture.os, "close") as close:
            lock.acquire()
            lock.close()
            lock.close()
            close.assert_called_once_with(42)
        with patch.object(capture.os, "open", return_value=43), patch.object(capture.fcntl, "flock", side_effect=BlockingIOError()), patch.object(capture.os, "close") as close:
            with self.assertRaisesRegex(capture.CaptureError, "another cooperating"):
                lock.acquire()
            close.assert_called_once_with(43)


class NativeTransportTests(unittest.TestCase):
    def make_native(self):
        native = capture.NativeUSB.__new__(capture.NativeUSB)
        native.buf = mmap.mmap(-1, 131072)
        self.addCleanup(native.buf.close)
        native.address = ctypes.addressof(ctypes.c_char.from_buffer(native.buf))
        native.rfd = None
        native.volume = Path("/never-opened")
        return native

    def fake_reads(self, native, parts):
        parts = iter(parts)
        def read(offset, size):
            part = next(parts)
            if isinstance(part, Exception):
                raise part
            self.assertLessEqual(len(part), size)
            native.buf[offset:offset + len(part)] = part
            return len(part)
        native._read = read

    def test_native_platform_and_ordinary_folder_refused(self):
        with patch.object(capture.sys, "platform", "linux"):
            with self.assertRaisesRegex(capture.CaptureError, "macOS"):
                capture.NativeUSB(Path("/tmp"))
        with patch.object(capture.sys, "platform", "darwin"), patch.object(capture.os.path, "ismount", return_value=False):
            with self.assertRaisesRegex(capture.CaptureError, "mounted device"):
                capture.NativeUSB(Path("/Volumes/fake"))

    def test_native_init_and_identity_with_only_temporary_files(self):
        with patch.object(capture.sys, "platform", "darwin"), patch.object(capture.os.path, "ismount", return_value=True):
            native = capture.NativeUSB(Path("/Volumes/fake"))
        with tempfile.TemporaryDirectory() as directory:
            native.volume = Path(directory)
            (native.volume / "ecg.txt").write_text("sn=fake\r\nmode=standby")
            (native.volume / "ecg.dat").write_bytes(b"test")
            with patch.object(capture.fcntl, "fcntl"):
                self.assertEqual(native.identify()["sn"], "fake")
            native.close()
            native = self.make_native()
            native.volume = Path(directory)
            with patch.object(capture.fcntl, "fcntl", side_effect=OSError("fcntl failed")):
                with self.assertRaisesRegex(OSError, "fcntl failed"):
                    native.identify()
            self.assertIsNone(native.rfd)
            (native.volume / "ecg.txt").write_text("mode=standby")
            with self.assertRaisesRegex(capture.CaptureError, "serial number"):
                native.identify()

    def test_read_padding_idle_malformed_and_continuation(self):
        native = self.make_native()
        packet = frame(b"a" * 2600)
        self.fake_reads(native, [packet[:2048], packet[2048:] + bytes(1024 - len(packet[2048:]))])
        progress = []
        self.assertEqual(native.read_packet(progress.append), packet)
        self.assertEqual(len(progress), 2)
        for raw in (frame() + bytes(2048 - 11), b"x", b"garbage", b"\x55\xaa\x00\x04" + b"junk"):
            self.fake_reads(native, [raw])
            expected = frame() if raw.startswith(frame()) else raw
            self.assertEqual(native.read_packet(), expected)

    def test_read_continuation_failure_keeps_first_chunk(self):
        native = self.make_native()
        prefix = b"\x55\xaa\x20\x00" + bytes(2044)
        self.fake_reads(native, [prefix, OSError("removed")])
        with self.assertRaises(capture.PartialReadError) as caught:
            native.read_packet()
        self.assertEqual(caught.exception.payload, prefix)

    def test_uncached_read_errors(self):
        native = self.make_native()
        native.libc = SimpleNamespace(pread=MagicMock(return_value=12))
        self.assertEqual(native._read(0, 2048), 12)
        for result in (0, -1):
            native.libc.pread.return_value = result
            with self.assertRaises(OSError):
                native._read(0, 2048)

    def test_sector_write_and_short_write(self):
        native = self.make_native()
        native.libc = SimpleNamespace(pwrite=MagicMock(return_value=512))
        packet = capture.command_packet("mode=standby")
        with patch.object(capture.os, "open", return_value=20), patch.object(capture.os, "close"), patch.object(capture.fcntl, "fcntl") as fcntl:
            native.send_packet(packet)
            self.assertEqual(native.buf[:512], packet + bytes(512 - len(packet)))
            self.assertEqual(fcntl.call_args_list[-1].args, (20, capture.F_FULLFSYNC))
            native.libc.pwrite.return_value = 511
            with self.assertRaisesRegex(OSError, "511 of 512"):
                native.send_packet(packet)
        with self.assertRaises(ValueError):
            native.send_packet(bytes(513))


def synthetic_blocking_worker(connection, _volume):
    connection.send((True, None))
    while True:
        operation, _arguments = connection.recv()
        if operation == "read_packet":
            connection.send((None, b"partial-sector"))
            time.sleep(60)
        else:
            connection.send((True, {"sn": "fake"}))


class SyntheticSignalDevice:
    def __init__(self, _volume):
        pass
    def identify(self):
        return {"sn": "synthetic-only"}
    def send_packet(self, packet):
        return packet[12:-2].decode("ascii")
    def close(self):
        pass


def synthetic_signal_worker(connection, volume):
    # Exercise the real worker loop/handlers with no physical device calls.
    capture.NativeUSB = SyntheticSignalDevice
    capture._device_worker(connection, volume)


class WorkerSignalTests(unittest.TestCase):
    def test_sigint_does_not_kill_worker_before_standby_and_close(self):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        worker = context.Process(target=synthetic_signal_worker, args=(child, "/synthetic-only"))
        worker.start()
        child.close()
        try:
            self.assertTrue(parent.poll(3))
            self.assertEqual(parent.recv(), (True, None))
            os.kill(worker.pid, signal.SIGINT)
            parent.send(("send_packet", (capture.command_packet("mode=standby"),)))
            self.assertTrue(parent.poll(3))
            self.assertEqual(parent.recv(), (True, "mode=standby"))
            parent.send(("close", ()))
            self.assertTrue(parent.poll(3))
            self.assertEqual(parent.recv(), (True, None))
            worker.join(3)
            self.assertEqual(worker.exitcode, 0)
        finally:
            parent.close()
            if worker.is_alive():
                worker.kill()
            worker.join(3)


class BoundedTests(unittest.TestCase):
    def test_actual_subprocess_hang_is_killed_and_partial_retained(self):
        config = capture.CaptureConfig(Path("/unused"), Path("/unused"), io_timeout_s=2)
        with patch.object(capture, "_device_worker", synthetic_blocking_worker):
            wrapped = capture.BoundedUSB(config)
        self.addCleanup(wrapped.close)
        wrapped.timeout = .03
        start = time.monotonic()
        with self.assertRaises(capture.PartialReadError) as caught:
            wrapped.read_packet()
        self.assertEqual(caught.exception.payload, b"partial-sector")
        self.assertLess(time.monotonic() - start, 1)
        self.assertFalse(wrapped.process.is_alive())

    def wrapper(self, replies=()):
        wrapped = capture.BoundedUSB.__new__(capture.BoundedUSB)
        wrapped.timeout = .01
        wrapped.dead = False
        wrapped.connection = MagicMock()
        wrapped.connection.poll.return_value = True
        wrapped.connection.recv.side_effect = list(replies)
        wrapped.process = MagicMock()
        wrapped.process.is_alive.return_value = False
        return wrapped

    def test_forwarding_and_progress(self):
        wrapped = self.wrapper([(True, {"sn": "fake"}), (True, None), (None, b"partial"), (True, b"packet"), (True, None)])
        self.assertEqual(wrapped.identify(), {"sn": "fake"})
        wrapped.send_packet(b"tx")
        self.assertEqual(wrapped.read_packet(), b"packet")
        wrapped.close()
        self.assertTrue(wrapped.dead)
        with self.assertRaisesRegex(capture.CaptureError, "standby is unconfirmed"):
            wrapped.send_packet(b"tx")

    def test_timeout_partial_timeout_eof_and_error(self):
        wrapped = self.wrapper()
        wrapped.connection.poll.return_value = False
        with self.assertRaisesRegex(capture.CaptureError, "timeout"):
            wrapped.read_packet()
        wrapped = self.wrapper([(None, b"retained")])
        wrapped.connection.poll.side_effect = [True, False]
        with self.assertRaises(capture.PartialReadError) as caught:
            wrapped.read_packet()
        self.assertEqual(caught.exception.payload, b"retained")
        for partial in (None, b"first"):
            wrapped = self.wrapper()
            wrapped.connection.recv.side_effect = ([(None, partial), EOFError()] if partial else EOFError())
            with self.assertRaisesRegex(capture.CaptureError, "exited"):
                wrapped.read_packet()
        for partial in (None, b"first"):
            wrapped = self.wrapper([(False, ("OSError", "removed", partial))])
            with self.assertRaisesRegex(capture.CaptureError, "removed"):
                wrapped.read_packet()

    def test_send_failure_and_unkillable_worker(self):
        wrapped = self.wrapper()
        wrapped.connection.send.side_effect = BrokenPipeError()
        with self.assertRaisesRegex(capture.CaptureError, "unavailable"):
            wrapped.read_packet()
        wrapped = self.wrapper()
        wrapped.process.is_alive.side_effect = [True, True, False]
        wrapped._abort()
        wrapped.process.terminate.assert_called_once()
        wrapped.process.kill.assert_called_once()
        wrapped = self.wrapper()
        wrapped.process.is_alive.return_value = True
        with self.assertRaisesRegex(capture.CaptureError, "could not be killed"):
            wrapped._abort()

    def test_constructor_success_and_failure_without_process(self):
        context = MagicMock()
        parent, child = MagicMock(), MagicMock()
        context.Pipe.return_value = (parent, child)
        parent.poll.return_value = True
        parent.recv.return_value = (True, None)
        context.Process.return_value.is_alive.return_value = False
        config = capture.CaptureConfig(Path("/unused"), Path("/unused"))
        with patch.object(capture.multiprocessing, "get_context", return_value=context):
            wrapped = capture.BoundedUSB(config)
            wrapped.close()
            parent.recv.return_value = (False, ("OSError", "open failed", None))
            with self.assertRaisesRegex(capture.CaptureError, "open failed"):
                capture.BoundedUSB(config)

    def test_worker_commands_read_and_failures(self):
        device = MagicMock()
        device.identify.return_value = {"sn": "fake"}
        device.read_packet.side_effect = lambda progress: (progress(b"raw"), b"raw")[1]
        connection = MagicMock()
        connection.recv.side_effect = [("identify", ()), ("read_packet", ()), ("send_packet", (b"bad",)), ("close", ())]
        device.send_packet.side_effect = capture.PartialReadError("partial", b"part")
        with patch.object(capture, "NativeUSB", return_value=device):
            capture._device_worker(connection, "/unused")
        self.assertIn(unittest.mock.call((None, b"raw")), connection.send.call_args_list)
        self.assertIn(unittest.mock.call((False, ("PartialReadError", "partial", b"part"))), connection.send.call_args_list)
        device.close.assert_called_once()
        connection = MagicMock()
        connection.recv.side_effect = EOFError()
        with patch.object(capture, "NativeUSB", return_value=device):
            capture._device_worker(connection, "/unused")
        with patch.object(capture, "NativeUSB", side_effect=OSError("open failed")):
            capture._device_worker(connection, "/unused")


class MainTests(unittest.TestCase):
    def test_main_success_interrupt_and_failure(self):
        args = ["--volume", "/unused", "--output-dir", "/unused"]
        with patch.object(capture, "run_capture", return_value={"success": True}), patch("builtins.print"):
            self.assertEqual(capture.main(args), 0)
        def interrupted(_config, stop_event):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            self.assertTrue(stop_event.is_set())
            return {"success": False}
        with patch.object(capture, "run_capture", side_effect=interrupted), patch("builtins.print"):
            self.assertEqual(capture.main(args), 130)
        with patch.object(capture, "run_capture", side_effect=capture.CaptureError("failed")), patch("builtins.print"):
            self.assertEqual(capture.main(args), 1)


if __name__ == "__main__":
    unittest.main()
