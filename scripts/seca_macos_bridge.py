#!/usr/bin/env python3
"""Versioned, nonsecret JSON-line bridge for the developer macOS seca app.

Raw bytes and full configuration stay in private capture files. Progress is
coalesced off the capture thread; a blocked UI pipe cannot block USB archival.
"""
from __future__ import annotations

import argparse
from collections import deque
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

import seca_enroll as enrollment
import seca_upload as uploader
from seca_replay import ReplayError, decode_packet
from seca_usb_capture import CaptureConfig, CaptureError, Journal, RATES, run_capture

DEFAULT_STATE = Path.home() / "Library/Application Support/TrainingSensors/seca-usb"


class BridgeError(ValueError):
    pass


class Events:
    """Bounded asynchronous output. Lifecycle overflow fails loudly, never drops."""
    def __init__(self, stream=None):
        self.stream = stream or sys.stdout
        self.condition = threading.Condition()
        self.pending = deque()
        self.progress = None
        self.closed = False
        self.error = None
        self.thread = threading.Thread(target=self._write, daemon=True)
        self.thread.start()

    def emit(self, event, **fields):
        row = {"schema_version": 1, "event": event, **fields}
        with self.condition:
            if self.error:
                raise BridgeError("app status pipe failed; local files retained")
            if event == "progress":
                self.progress = row
            else:
                if len(self.pending) >= 32:
                    raise BridgeError("app lifecycle queue full; local files retained")
                self.pending.append(row)
            self.condition.notify()

    def _write(self):
        try:
            while True:
                with self.condition:
                    self.condition.wait_for(lambda: self.closed or self.pending or self.progress is not None)
                    if self.pending:
                        row = self.pending.popleft()
                    elif self.progress is not None:
                        row, self.progress = self.progress, None
                    elif self.closed:
                        return
                self.stream.write(json.dumps(row, separators=(",", ":")) + "\n")
                self.stream.flush()
        except Exception:
            with self.condition:
                self.error = True

    def close(self):
        with self.condition:
            self.progress = None
            self.closed = True
            self.condition.notify()
        self.thread.join(3)
        if self.thread.is_alive() or self.error:
            raise BridgeError("app status did not drain; inspect retained local recording")


def state_directory(path):
    root = enrollment.private_directory(Path(path))
    for name in ("recordings", "outboxes"):
        enrollment.private_directory(root / name)
    return root


def terminal(path):
    """Only inspect the bounded last line; never load a long recording into RAM."""
    try:
        with (path / "records.jsonl").open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - 16384))
            raw = stream.read(16384)
        if not raw.endswith(b"\n"):
            return None
        row = json.loads(raw.splitlines()[-1])
        return row if row.get("type") == "event" and row.get("event") == "session_end" else None
    except (OSError, ValueError, IndexError, AttributeError):
        return None


def snapshot(root, volumes_root=Path("/Volumes")):
    volumes = []
    if volumes_root.exists():
        for path in sorted(volumes_root.iterdir()):
            # Directory metadata only. The capture worker owns identity/data I/O.
            if (path / "ecg.txt").is_file() and (path / "ecg.dat").is_file():
                volumes.append(str(path))
    credentials = root / "credential.json"
    installation = None
    if credentials.exists():
        saved = enrollment.read_private(credentials)
        installation = str(enrollment.checked_uuid(saved.get("installation_id")))
        enrollment.validate_credential(saved, installation)
    sessions = []
    session_bytes = 0
    for path in sorted((root / "recordings").iterdir(), reverse=True):
        if path.is_symlink() or not path.is_dir():
            continue
        end = terminal(path)
        item = {"name": path.name, "path": str(path), "closed": end is not None,
                "uploadable": uploadable(path),
                "end_reason": end.get("end_reason", "unknown") if end else "incomplete"}
        size = len(json.dumps(item).encode()) + 1
        if len(sessions) >= 200 or session_bytes + size > 100000:
            break
        sessions.append(item)
        session_bytes += size
    return {"volumes": volumes, "installation_id": installation, "sessions": sessions}


def selected_session(root, name):
    if not name or Path(name).name != name or name in (".", ".."):
        raise BridgeError("invalid recording selection")
    path = root / "recordings" / name
    if path.is_symlink() or not path.is_dir():
        raise BridgeError("recording no longer exists")
    return path


def request_usb_access(volume, events, *, volume_root=Path("/Volumes"), ismount=os.path.ismount):
    """Trigger first-access permission separately; never open the ECG data stream."""
    path = Path(volume).resolve()
    if path.parent != volume_root.resolve() or not ismount(path):
        raise BridgeError("select a mounted seca volume directly under /Volumes")
    events.emit("access_waiting")
    try:
        with (path / "ecg.txt").open("rb") as stream:
            stream.read(4096)  # Identity content is discarded, never logged/exported.
    except PermissionError:
        raise BridgeError("USB access denied. Allow removable-volume access in System Settings > Privacy & Security > Files and Folders.") from None
    events.emit("access_granted", volume=str(path))


def uploadable(path):
    try:
        return terminal(path) is not None and uploader.read_json(path / "session.json").get("configuration_confirmed") is True
    except (OSError, ValueError):
        return False


class LiveJournal(Journal):
    def __init__(self, path, metadata, events):
        super().__init__(path, metadata)
        self.events = events
        self.bytes = self.groups = self.missing = self.errors = self.constant = 0
        self.previous = None
        self.started = time.monotonic()
        self.last_status = 0.0
        self.events.emit("session", path=str(path), serial=metadata["device_info"]["sn"],
                         requested_rate_hz=metadata["requested_sample_rate_hz"])

    def wire(self, direction, payload, *, acquisition=False):
        super().wire(direction, payload, acquisition=acquisition)
        if direction != "rx":
            return
        self.bytes += len(payload)
        if acquisition:
            try:
                for block in decode_packet(payload).blocks:
                    if self.previous is not None:
                        delta = (block.counter - self.previous) & 0xffffffff
                        if delta != 1:
                            self.errors += 1
                            if 1 < delta < (1 << 31):
                                self.missing += delta - 1
                    self.previous = block.counter
                    self.groups += 1
                    self.constant += all(len(set(channel)) == 1 for channel in block.channels)
            except ReplayError:
                self.errors += 1
        now = time.monotonic()
        if now - self.last_status >= .5:
            self.last_status = now
            self.events.emit("progress", received_bytes=self.bytes, groups=self.groups,
                             missing_groups=self.missing, decode_or_sequence_errors=self.errors,
                             constant_groups=self.constant, elapsed_s=now - self.started)

    def event(self, name, **fields):
        super().event(name, **fields)
        if name == "configuration_confirmed":
            self.events.emit("confirmed", **fields["details"])
        elif name == "acquisition_start":
            self.events.emit("recording")
        elif name == "session_end":
            # The final result is emitted only after run_capture closes the journal.
            self.events.emit("closing", received_bytes=self.bytes, groups=self.groups,
                             missing_groups=self.missing, decode_or_sequence_errors=self.errors,
                             constant_groups=self.constant, elapsed_s=time.monotonic() - self.started)


def record(root, args, events, runner=run_capture):
    path = root / "recordings" / (time.strftime("%Y%m%d-%H%M%S-") + str(uuid.uuid4()))
    stopped = threading.Event()
    handlers = {number: signal.signal(number, lambda *_: stopped.set())
                for number in (signal.SIGINT, signal.SIGTERM)}
    events.emit("configuring", requested_rate_hz=args.rate)
    try:
        config = CaptureConfig(path, Path(args.volume), args.rate, args.duration, args.contact)
        result = runner(config, journal_factory=lambda p, m: LiveJournal(p, m, events), stop_event=stopped)
        clean_stop = result["end_reason"] == "interrupted" and result["errors"] == []
        events.emit("saved", path=str(path), user_stopped=clean_stop, uploadable=uploadable(path), **result)
    finally:
        for number, previous in handlers.items():
            signal.signal(number, previous)


def send_session(root, name, base_url, events, prepare=uploader.prepare, upload=uploader.upload):
    source = selected_session(root, name)
    if terminal(source) is None:
        raise BridgeError("recording has no durable terminal event; retained for inspection")
    credential = root / "credential.json"
    saved = enrollment.read_private(credential)
    installation = str(enrollment.checked_uuid(saved.get("installation_id")))
    enrollment.validate_credential(saved, installation)
    if saved.get("server_origin") != enrollment.origin(base_url):
        raise BridgeError("credential belongs to a different Carolina origin")
    # A per-recording directory permits safe retry after a partial preparation.
    container = enrollment.private_directory(root / "outboxes" / name)
    outbox = None
    for candidate in sorted(container.iterdir()):
        if not candidate.is_symlink() and (candidate / "ready.json").is_file():
            manifest = uploader.read_json(candidate / "ready.json", 16384)
            if manifest.get("installation_id") == installation:
                outbox = candidate
                break
    if outbox is None:
        outbox = container / str(uuid.uuid4())
        events.emit("preparing")
        prepare(source, outbox, installation, progress=lambda batches:
                events.emit("progress", prepared_batches=batches))
    events.emit("uploading")
    result = upload(outbox, base_url, credential, progress=lambda acknowledged, total:
                    events.emit("progress", acknowledged=acknowledged, batches=total))
    events.emit("acknowledged", **result)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--base-url", default=enrollment.DEFAULT_ORIGIN)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("state")
    access = commands.add_parser("access")
    access.add_argument("--volume", required=True)
    rec = commands.add_parser("record")
    rec.add_argument("--volume", required=True)
    rec.add_argument("--rate", type=int, choices=RATES, default=32000)
    rec.add_argument("--duration", type=float, default=60)
    rec.add_argument("--contact", action="store_true")
    send = commands.add_parser("upload")
    send.add_argument("--session", required=True)
    connect = commands.add_parser("enroll")
    connect.add_argument("--replace-link", action="store_true")
    args = parser.parse_args(argv)
    events = Events()
    result = 0
    try:
        root = state_directory(args.state_dir)
        if args.command == "state":
            events.emit("state", **snapshot(root))
        elif args.command == "access":
            request_usb_access(args.volume, events)
        elif args.command == "record":
            record(root, args, events)
        elif args.command == "upload":
            send_session(root, args.session, args.base_url, events)
        else:
            # Parent supplies one short JSON object then closes stdin. No TTY and
            # no secret-bearing argv/environment, logs or bridge status fields.
            raw = sys.stdin.buffer.readline(16385)
            if len(raw) > 16384 or not raw.endswith(b"\n"):
                raise BridgeError("missing or oversized connection input")
            request = json.loads(raw)
            link = request.get("link")
            if not isinstance(link, str) or not link:
                raise BridgeError("missing connection link")
            _, installation = enrollment.enroll(root, args.base_url, label="Mac USB recorder",
                                                 reader=lambda _: link, replace_link=args.replace_link)
            events.emit("enrolled", installation_id=installation)
    except Exception as error:
        # Enrollment helper exceptions are already deliberately redacted. Capture
        # configuration mismatch only includes samplerate/gain/openlead values.
        safe = isinstance(error, (BridgeError, CaptureError, uploader.UploadError, enrollment.EnrollmentError))
        events.emit("error", message=str(error) if safe else type(error).__name__, files_retained=True)
        result = 1
    finally:
        try:
            events.close()
        except BridgeError:
            result = 1
    return result


if __name__ == "__main__":
    raise SystemExit(main())
