#!/usr/bin/env python3
"""Record seca USB bytes locally; no clinical interpretation or bandwidth guarantee.

USB framing and command fields follow observations of the owner's existing bridge
and probe. This independently implemented CLI never imports vendor software.
Native I/O runs in a subprocess so a stuck removable-volume syscall is bounded.
Other software does not honor our lock: stop it and put the device in standby first.
"""

from __future__ import annotations

import argparse
import ctypes
from dataclasses import dataclass
import fcntl
import json
import math
import mmap
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

from seca_replay import ReplayError, decode_packet

RATES = (1000, 2000, 4000, 8000, 16000, 32000)
F_RDAHEAD, F_NOCACHE, F_FULLFSYNC = 45, 48, 51


class CaptureError(RuntimeError):
    """A capture or its cleanup did not finish successfully."""


class PartialReadError(CaptureError):
    def __init__(self, message, payload):
        super().__init__(message)
        self.payload = payload


@dataclass(frozen=True)
class CaptureConfig:
    output_dir: Path
    volume: Path
    sample_rate_hz: int = 32000
    duration_s: float = 10.0
    contact_detection: bool = False
    io_timeout_s: float = 2.0
    config_wait_s: float = 1.0
    command_delay_s: float = 0.03
    idle_sleep_s: float = 0.005

    def validate(self):
        if self.sample_rate_hz not in RATES:
            raise ValueError(f"sample rate must be one of {RATES}")
        for name in ("duration_s", "io_timeout_s", "config_wait_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.duration_s > 86400:
            raise ValueError("duration must be at most 86400 seconds")
        for name in ("command_delay_s", "idle_sleep_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")


def command_packet(text, kind=b"W"):
    if kind not in (b"W", b"R"):
        raise ValueError("only observed W and R commands are supported")
    payload = text.encode("ascii")
    length = len(payload) + 14
    if length > 512:
        raise ValueError("command exceeds one sector")
    head = (b"\x55\xaa" + length.to_bytes(2, "big") +
            bytes.fromhex("1234567800") + kind +
            len(payload).to_bytes(2, "big") + payload)
    return head + ((-sum(head)) & 65535).to_bytes(2, "big")


def parse_settings(text):
    settings = {}
    for line in text.replace("\x00", "").replace("\r", "\n").split("\n"):
        key, sep, value = line.partition("=")
        if sep and key.strip():
            settings[key.strip().lower()] = value.strip()
    return settings


def frame_problem(packet):
    if len(packet) < 11:
        return "short_frame"
    if packet[:2] != b"\x55\xaa":
        return "bad_magic"
    if int.from_bytes(packet[2:4], "big") != len(packet):
        return "length_mismatch"
    if int.from_bytes(packet[-2:], "big") != (-sum(packet[:-2])) & 65535:
        return "checksum_mismatch"
    return None


def configuration_reply(packet):
    """Walk whole validated fields; never mistake compressed data for A text."""
    try:
        replies = decode_packet(packet).configurations
    except ReplayError:
        return None
    return parse_settings(replies[-1]) if replies else None


def _write_all(fd, data):
    remaining = memoryview(data)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("archive write made no progress")
        remaining = remaining[written:]
    os.fsync(fd)


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Journal:
    """Exclusive session directory and an fsync per append; failures propagate."""

    def __init__(self, output_dir, metadata):
        self.path = Path(output_dir)
        self.fd = None
        self.metadata = dict(metadata)
        self.path.mkdir(mode=0o700)
        self._save_metadata(initial=True)
        self.fd = os.open(self.path / "records.jsonl", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            _sync_directory(self.path)
            _sync_directory(self.path.parent)
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise

    def _save_metadata(self, initial=False):
        path = self.path / ("session.json" if initial else "session.json.tmp")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            _write_all(fd, (json.dumps(self.metadata, sort_keys=True) + "\n").encode())
        finally:
            os.close(fd)
        if not initial:
            os.replace(path, self.path / "session.json")
            _sync_directory(self.path)

    def update_metadata(self, **fields):
        self.metadata.update(fields)
        self._save_metadata()

    def append(self, entry):
        stamped = {"unix_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(), **entry}
        _write_all(self.fd, (json.dumps(stamped, sort_keys=True) + "\n").encode())

    def wire(self, direction, payload, *, acquisition=False):
        self.append({"type": "wire", "direction": direction, "hex": payload.hex(),
                     "acquisition": bool(acquisition and direction == "rx")})

    def event(self, name, **details):
        self.append({"type": "event", "event": name, **details})

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)


class DeviceLock:
    """All our USB instances share one stable lock; never unlink a held lock."""

    def __init__(self):
        self.fd = None

    def acquire(self):
        path = Path("/tmp") / f"seca-usb-capture-{os.getuid()}.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(fd)
            raise CaptureError(f"another cooperating capture owns {path}")
        self.fd = fd

    def close(self):
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)


class NativeUSB:
    """Child-process-only uncached sector I/O. No reads or writes on import."""

    def __init__(self, volume):
        if sys.platform != "darwin":
            raise CaptureError("native USB capture requires macOS")
        self.volume = Path(volume).resolve()
        if self.volume.parent != Path("/Volumes") or not os.path.ismount(self.volume):
            raise CaptureError("volume must be a mounted device directly under /Volumes")
        self.rfd = None
        self.buf = mmap.mmap(-1, 131072)
        self.address = ctypes.addressof(ctypes.c_char.from_buffer(self.buf))
        self.libc = ctypes.CDLL(None, use_errno=True)
        for name in ("pread", "pwrite"):
            function = getattr(self.libc, name)
            function.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_longlong]
            function.restype = ctypes.c_ssize_t

    def identify(self):
        # Read identity before commands mutate ecg.txt. The parent bounds this call.
        with (self.volume / "ecg.txt").open("rb") as stream:
            info = parse_settings(stream.read(4096).decode("latin-1"))
        if not info.get("sn") or not (self.volume / "ecg.dat").is_file():
            raise CaptureError("volume has no recognized seca serial number/data endpoint")
        self.rfd = os.open(self.volume / "ecg.dat", os.O_RDONLY)
        try:
            fcntl.fcntl(self.rfd, F_NOCACHE, 1)
            fcntl.fcntl(self.rfd, F_RDAHEAD, 0)
        except BaseException:
            os.close(self.rfd)
            self.rfd = None
            raise
        return info

    def _read(self, offset, size):
        got = self.libc.pread(self.rfd, self.address + offset, size, 0)
        if got < 0:
            raise OSError(ctypes.get_errno(), "uncached ecg.dat read failed")
        if got == 0:
            raise OSError("empty device read: disconnected or endpoint unavailable")
        return got

    def read_packet(self, progress=None):
        got = self._read(0, 2048)
        if progress is not None:
            progress(bytes(self.buf[:got]))
        if got < 4 or self.buf[:2] != b"\x55\xaa":
            return bytes(self.buf[:got])
        length = int.from_bytes(self.buf[2:4], "big")
        if length < 11:
            return bytes(self.buf[:got])
        while got < length:
            size = min(2048, ((length - got + 511) // 512) * 512)
            try:
                amount = self._read(got, size)
                if progress is not None:
                    progress(bytes(self.buf[got:got + amount]))
                got += amount
            except Exception as exc:
                raise PartialReadError(str(exc), bytes(self.buf[:got])) from exc
        # Only plausible framing permits removing transport-sector padding.
        return bytes(self.buf[:length])

    def send_packet(self, packet):
        if len(packet) > 512:
            raise ValueError("command exceeds one sector")
        self.buf[:512] = packet + bytes(512 - len(packet))
        fd = os.open(self.volume / "ecg.txt", os.O_WRONLY)
        try:
            fcntl.fcntl(fd, F_NOCACHE, 1)
            written = self.libc.pwrite(fd, self.address, 512, 0)
            if written != 512:
                raise OSError(f"command write was {written} of 512 bytes (errno {ctypes.get_errno()})")
            fcntl.fcntl(fd, F_FULLFSYNC)
        finally:
            os.close(fd)

    def close(self):
        try:
            if self.rfd is not None:
                fd, self.rfd = self.rfd, None
                os.close(fd)
        finally:
            self.buf.close()


def _device_worker(connection, volume):
    # User cancellation belongs to the supervisor, which must still be able to
    # request standby/close. Terminal Ctrl-C can target the whole process group.
    previous_interrupt = signal.signal(signal.SIGINT, signal.SIG_IGN)
    device = None
    try:
        device = NativeUSB(volume)
        connection.send((True, None))
        while True:
            operation, argument = connection.recv()
            try:
                if operation == "read_packet":
                    result = device.read_packet(progress=lambda chunk: connection.send((None, chunk)))
                else:
                    result = getattr(device, operation)(*argument)
                connection.send((True, result))
            except Exception as exc:
                connection.send((False, (type(exc).__name__, str(exc), getattr(exc, "payload", None))))
            if operation == "close":
                device = None
                break
    except (EOFError, BrokenPipeError):
        pass  # The parent has exited or explicitly killed this worker.
    except Exception as exc:
        connection.send((False, (type(exc).__name__, str(exc), None)))
    finally:
        try:
            if device is not None:
                device.close()
        finally:
            connection.close()
            signal.signal(signal.SIGINT, previous_interrupt)


class BoundedUSB:
    """One process/one operation at a time; a timeout aborts, never reconnects."""

    def __init__(self, config):
        self.timeout = config.io_timeout_s
        context = multiprocessing.get_context("spawn")
        self.connection, child = context.Pipe()
        self.process = context.Process(target=_device_worker, args=(child, str(config.volume)), daemon=True)
        self.process.start()
        child.close()
        self.dead = False
        try:
            self._receive("open")
        except BaseException:
            self._abort()
            raise

    def _abort(self):
        self.dead = True
        self.connection.close()
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=0.2)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=0.2)
        if self.process.is_alive():
            raise CaptureError("USB worker could not be killed; device ownership is uncertain")
        self.process.join(timeout=0)

    def _receive(self, operation):
        deadline = time.monotonic() + self.timeout
        partial = bytearray()
        while True:
            if not self.connection.poll(max(0, deadline - time.monotonic())):
                self._abort()
                message = f"USB {operation} timeout after {self.timeout:g}s; worker aborted"
                if partial:
                    raise PartialReadError(message, bytes(partial))
                raise CaptureError(message)
            try:
                ok, result = self.connection.recv()
            except EOFError as exc:
                self._abort()
                message = f"USB worker exited during {operation}"
                if partial:
                    raise PartialReadError(message, bytes(partial)) from exc
                raise CaptureError(message) from exc
            if ok is not None:
                break
            partial.extend(result)
        if not ok:
            name, message, partial = result
            if partial is not None:
                raise PartialReadError(f"USB {operation}: {name}: {message}", partial)
            raise CaptureError(f"USB {operation}: {name}: {message}")
        return result

    def _call(self, operation, *arguments):
        if self.dead:
            raise CaptureError(f"cannot {operation}: USB worker was aborted; standby is unconfirmed")
        try:
            self.connection.send((operation, arguments))
        except (OSError, EOFError) as exc:
            self._abort()
            raise CaptureError(f"USB worker unavailable for {operation}") from exc
        return self._receive(operation)

    def identify(self):
        return self._call("identify")

    def send_packet(self, packet):
        return self._call("send_packet", packet)

    def read_packet(self):
        return self._call("read_packet")

    def close(self):
        try:
            if not self.dead:
                self._call("close")
        finally:
            self._abort()


def run_capture(config, *, transport_factory=None, journal_factory=Journal, stop_event=None):
    """Capture with injectable synchronous fakes. Native calls are process-bounded.

    A fake factory(config) implements identify, send_packet, read_packet and close;
    injected fakes own their timeout behavior. No hardware/lock is used for fakes.
    Returns session_end details; failures raise CaptureError after cleanup. Signals
    represented by stop_event yield success=False/end_reason=interrupted.
    """
    config.validate()
    if Path(config.output_dir).exists():
        raise CaptureError(f"output already exists: {config.output_dir}")
    stop_event = stop_event if stop_event is not None else threading.Event()
    transport = journal = lock = None
    touched = False
    acquisition = False
    errors = []
    reason = "failure"
    packets = valid_frames = malformed_frames = idle_frames = received_bytes = 0
    try:
        if transport_factory is None:
            if sys.platform != "darwin":
                raise CaptureError("native USB capture requires macOS")
            lock = DeviceLock()
            lock.acquire()
        transport = (transport_factory or BoundedUSB)(config)
        info = transport.identify()
        if not info.get("sn"):
            raise CaptureError("device identity has no serial number")
        if info.get("mode", "standby").lower() != "standby":
            raise CaptureError("device is not in standby; stop the other recorder first")
        metadata = dict(schema_version=1, session_id=str(uuid.uuid4()), transport="usb",
                        requested_sample_rate_hz=config.sample_rate_hz, gain=6,
                        contact_detection=config.contact_detection, device_info=info,
                        start_unix_ns=time.time_ns(), start_monotonic_ns=time.monotonic_ns(),
                        requested_duration_s=config.duration_s, observed_config={},
                        configuration_confirmed=False)
        journal = journal_factory(config.output_dir, metadata)
        journal.event("session_start")

        def send(text, kind=b"W"):
            nonlocal touched
            packet = command_packet(text, kind)
            journal.wire("tx", packet)
            touched = True  # The write may succeed before a subsequent syscall fails.
            transport.send_packet(packet)
            if config.command_delay_s:
                stop_event.wait(config.command_delay_s)

        def record_rx(packet):
            nonlocal packets, valid_frames, malformed_frames, idle_frames, received_bytes
            journal.wire("rx", packet, acquisition=acquisition)
            packets += 1
            received_bytes += len(packet)
            problem = frame_problem(packet)
            if problem:
                malformed_frames += 1
                journal.event("malformed_frame", problem=problem)
            else:
                valid_frames += 1
                idle_frames += len(packet) == 11
            return packet

        def read():
            try:
                packet = transport.read_packet()
            except PartialReadError as exc:
                record_rx(exc.payload)
                journal.event("partial_read", error=str(exc))
                raise
            return record_rx(packet)

        expected = {"samplerate": str(config.sample_rate_hz), "gain": "6",
                    "openlead": "1" if config.contact_detection else "0"}
        for key, value in expected.items():
            if stop_event.is_set():
                break
            send(f"{key}={value}")
        if not stop_event.is_set():
            send("", b"R")
            deadline = time.monotonic() + config.config_wait_s
            observed = None
            confirmed = False
            while time.monotonic() < deadline and not stop_event.is_set():
                packet = read()
                fields = configuration_reply(packet)
                if fields is not None:
                    observed = fields
                    journal.event("config_observed", settings=observed)
                    confirmed = all(observed.get(key) == value for key, value in expected.items())
                    journal.update_metadata(observed_config=observed, configuration_confirmed=confirmed)
                    if confirmed:
                        journal.event("configuration_confirmed", details={
                            "sample_rate_hz": config.sample_rate_hz, "gain": 6,
                            "contact_detection": config.contact_detection})
                        break
                if config.idle_sleep_s and len(packet) <= 11:
                    stop_event.wait(config.idle_sleep_s)
            if not confirmed and not stop_event.is_set():
                safe_observed = {key: (observed or {}).get(key) for key in expected}
                raise CaptureError(f"configuration not confirmed: requested {expected}, observed {safe_observed}")
        if not stop_event.is_set():
            send("mode=run")
            acquisition = True
            journal.event("acquisition_start", requested_sample_rate_hz=config.sample_rate_hz)
            deadline = time.monotonic() + config.duration_s
            while time.monotonic() < deadline and not stop_event.is_set():
                packet = read()
                if config.idle_sleep_s and len(packet) <= 11:
                    stop_event.wait(config.idle_sleep_s)
        reason = "interrupted" if stop_event.is_set() else "duration_elapsed"
    except Exception as exc:
        errors.append(f"{type(exc).__name__}: {exc}")
    finally:
        if malformed_frames:
            errors.append(f"{malformed_frames} malformed RX frame(s); raw bytes retained for replay")
        # A failed archive write must never prevent the physical standby attempt.
        if transport is not None and touched:
            standby = command_packet("mode=standby")
            try:
                journal.wire("tx", standby)
            except Exception as exc:
                errors.append(f"standby journal: {exc}")
            try:
                transport.send_packet(standby)
            except Exception as exc:
                errors.append(f"standby cleanup failed (device state unconfirmed): {exc}")
        for name, resource in (("transport", transport), ("lock", lock)):
            if resource is not None:
                try:
                    resource.close()
                except Exception as exc:
                    errors.append(f"{name} close failed: {exc}")
        summary = dict(success=not errors and reason == "duration_elapsed",
                       end_reason="failure" if errors else reason, errors=errors.copy(),
                       received_records=packets, received_bytes=received_bytes,
                       valid_rx_frames=valid_frames, malformed_rx_frames=malformed_frames,
                       idle_rx_frames=idle_frames, acquisition_evidence="raw_transport_only")
        if journal is not None:
            try:
                journal.event("session_end", **summary)
            except Exception as exc:
                errors.append(f"session_end journal failed: {exc}")
            try:
                journal.close()
            except Exception as exc:
                errors.append(f"journal close failed: {exc}")
    if errors:
        raise CaptureError("; ".join(errors))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume", type=Path, required=True, help="mounted seca volume, e.g. /Volumes/ct 331")
    parser.add_argument("--output-dir", type=Path, required=True, help="new directory; parent must exist")
    parser.add_argument("--sample-rate", type=int, choices=RATES, default=32000)
    parser.add_argument("--duration", type=float, default=10.0, help="seconds of acquisition, max 86400")
    parser.add_argument("--contact-detection", action="store_true", help="send openlead=1 (default: 0)")
    parser.add_argument("--io-timeout", type=float, default=2.0, help="timeout per USB operation in seconds")
    args = parser.parse_args(argv)
    config = CaptureConfig(args.output_dir, args.volume, args.sample_rate, args.duration,
                           args.contact_detection, args.io_timeout)
    stopped = threading.Event()
    handlers = {}
    for number in (signal.SIGINT, signal.SIGTERM):
        handlers[number] = signal.signal(number, lambda _signal, _frame: stopped.set())
    try:
        print("USB access must be exclusive; other software does not honor this tool's lock.", file=sys.stderr)
        result = run_capture(config, stop_event=stopped)
        print(json.dumps({"output_dir": str(args.output_dir.resolve()), **result}, sort_keys=True))
        return 0 if result["success"] else 130
    except (CaptureError, ValueError, OSError) as exc:
        print(f"capture failed: {exc}", file=sys.stderr)
        return 1
    finally:
        for number, handler in handlers.items():
            signal.signal(number, handler)


if __name__ == "__main__":
    raise SystemExit(main())
