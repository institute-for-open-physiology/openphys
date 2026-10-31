#!/usr/bin/env python3
"""Prepare immutable USB capture chunks and explicitly upload them to Carolina.

The raw capture and prepared chunks are never deleted. A receipt is durable only
after the server acknowledges the exact transmitted body. No network operation
occurs during preparation. See docs/seca_usb_tools.md for the enrollment contract.
"""

import argparse
import base64
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid


MAX_BODY = 2 * 1024 * 1024
MAX_LINE = 256 * 1024
CHUNK_TARGET = 768 * 1024
RATES = (1000, 2000, 4000, 8000, 16000, 32000)


class UploadError(ValueError):
    """A local or remote invariant failed; retain the queue for inspection/retry."""


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def digest(body):
    return hashlib.sha256(body).hexdigest()


def sync_directory(path):
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_new(path, body):
    """Publish a complete fsynced file without overwriting an existing one."""
    temporary = path.with_name("." + path.name + "." + str(uuid.uuid4()) + ".tmp")
    try:
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(str(temporary), str(path))
        sync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()
            sync_directory(path.parent)


def read_bounded(path, limit):
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise UploadError("file exceeds supported size: " + path.name)
    return body


def read_json(path, limit=MAX_BODY):
    value = json.loads(read_bounded(path, limit))
    if not isinstance(value, dict):
        raise UploadError("JSON file must contain an object: " + path.name)
    return value


def canonical_uuid(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError, TypeError) as exc:
        raise UploadError("invalid installation or session UUID") from exc


def integer(value, name, low, high):
    if type(value) is not int or not low <= value <= high:
        raise UploadError("invalid " + name)
    return value


def wire_record(row):
    if row.get("direction") not in ("rx", "tx"):
        raise UploadError("invalid wire direction")
    acquisition = row.get("acquisition")
    if type(acquisition) is not bool or (acquisition and row["direction"] != "rx"):
        raise UploadError("wire acquisition must be a boolean and false for TX")
    unix_ns = integer(row.get("unix_ns"), "wire Unix clock", 946684800000000000, 2**63 - 1)
    mono = integer(row.get("monotonic_ns"), "wire monotonic clock", 0, 2**64 - 1)
    try:
        raw = bytes.fromhex(row["hex"])
    except (ValueError, KeyError, TypeError) as exc:
        raise UploadError("invalid wire hex") from exc
    if not 1 <= len(raw) <= 65535:
        raise UploadError("wire payload cannot fit cloud archive; original retained locally")
    return {"unix_ns": unix_ns, "monotonic_ns": mono,
            "direction": row["direction"], "acquisition": acquisition,
            "payload_b64": base64.b64encode(raw).decode("ascii")}


def capture_rows(path):
    """Bound memory and reject a truncated final line, rather than dropping it."""
    with path.open("rb") as stream:
        line_number = 0
        while True:
            line = stream.readline(MAX_LINE + 1)
            if not line:
                break
            line_number += 1
            if len(line) > MAX_LINE or not line.endswith(b"\n"):
                raise UploadError("oversize or incomplete journal line %d" % line_number)
            try:
                row = json.loads(line)
            except (ValueError, UnicodeError) as exc:
                raise UploadError("invalid journal JSON at line %d" % line_number) from exc
            if not isinstance(row, dict):
                raise UploadError("journal entry must be an object")
            yield row


def prepare(capture, outbox, installation_id, progress=None):
    """Seal a closed local session as a new upload queue. Never modify the input."""
    capture, outbox = Path(capture), Path(outbox)
    meta = read_json(capture / "session.json")
    if meta.get("schema_version") != 1 or meta.get("transport") != "usb":
        raise UploadError("expected a version 1 USB session")
    rate = meta.get("requested_sample_rate_hz")
    if type(rate) is not int or rate not in RATES or meta.get("gain") != 6:
        raise UploadError("unsupported requested rate or gain")
    contact = meta.get("contact_detection")
    if type(contact) is not bool:
        raise UploadError("missing contact-detection metadata")
    info = meta.get("device_info", {})
    device = info.get("sn") if isinstance(info, dict) else None
    if not isinstance(device, str) or not 1 <= len(device) <= 96:
        raise UploadError("missing or invalid device serial")
    header = {"schema_version": 1,
              "installation_id": canonical_uuid(installation_id),
              "session_id": canonical_uuid(meta.get("session_id")),
              "batch_index": 0, "device_id": device,
              "sample_rate_hz": rate, "gain": 6, "transport": "usb",
              "contact_detection": contact, "end_reason": None, "records": []}
    # The requested setting alone cannot justify labeling uploaded samples. The
    # capture process emits a confirmation after a configuration read succeeds.
    confirmed = False
    acquisition_started = False
    closed = None
    previous = None
    total_records = 0
    batch_count = 0
    queue_size = 0
    outbox.mkdir(mode=0o700)
    sync_directory(outbox.parent)

    def seal(reason=None):
        nonlocal batch_count, queue_size
        header["batch_index"] = batch_count
        header["end_reason"] = reason
        body = encode(header)
        if len(body) > MAX_BODY:
            raise UploadError("prepared batch exceeds cloud limit")
        write_new(outbox / ("%010d.json" % batch_count), body)
        batch_count += 1
        header["records"] = []
        queue_size = 0
        if progress:
            progress(batch_count)

    for row in capture_rows(capture / "records.jsonl"):
        if closed is not None:
            raise UploadError("journal contains records after session_end")
        kind = row.get("type")
        if kind == "event":
            details = row.get("details", {})
            if row.get("event") == "configuration_confirmed":
                observed = details if isinstance(details, dict) else {}
                if (observed.get("sample_rate_hz") != rate or observed.get("gain") != 6
                        or observed.get("contact_detection") is not contact):
                    raise UploadError("confirmed configuration differs from session metadata")
                confirmed = True
            elif row.get("event") == "session_end":
                closed = row
            elif row.get("event") == "acquisition_start":
                if not confirmed or acquisition_started:
                    raise UploadError("invalid acquisition-start boundary")
                acquisition_started = True
            continue
        if kind != "wire":
            raise UploadError("unknown journal record type")
        record = wire_record(row)
        if record["acquisition"] and not acquisition_started:
            raise UploadError("acquisition packet precedes confirmed run boundary")
        if previous is not None and record["monotonic_ns"] < previous:
            raise UploadError("journal monotonic clock regressed")
        previous = record["monotonic_ns"]
        size = len(encode(record)) + 1
        if header["records"] and (len(header["records"]) >= 256 or queue_size + size > CHUNK_TARGET):
            seal()
        header["records"].append(record)
        queue_size += size
        total_records += 1
    if closed is None:
        raise UploadError("capture has no terminal event; queue remains unready")
    if not confirmed:
        raise UploadError("capture rate/gain never confirmed; queue remains unready")
    details = closed.get("details", {})
    success = closed.get("success", details.get("success") if isinstance(details, dict) else None)
    if type(success) is not bool:
        raise UploadError("terminal event lacks explicit success state")
    terminal_reason = closed.get("end_reason", details.get("end_reason") if isinstance(details, dict) else None)
    terminal_errors = closed.get("errors", details.get("errors") if isinstance(details, dict) else None)
    user_stopped = not success and terminal_reason == "interrupted" and terminal_errors == []
    reason = ("usb_capture_complete" if success else "usb_capture_user_stopped" if user_stopped
              else "usb_capture_failed_see_local_journal")
    seal(reason)
    manifest = {"schema_version": 1, "installation_id": header["installation_id"],
                "session_id": header["session_id"], "batch_count": batch_count,
                "record_count": total_records, "capture_success": success,
                "capture_user_stopped": user_stopped}
    # Only this final marker makes the queue uploadable. Partial preparation is
    # intentionally retained for inspection; retry preparation in a fresh dir.
    write_new(outbox / "ready.json", encode(manifest))
    return manifest


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def endpoint(base, allow_local_http=False):
    parsed = urllib.parse.urlsplit(base)
    local = parsed.hostname in ("localhost", "127.0.0.1", "::1")
    if parsed.scheme != "https" and not (allow_local_http and local and parsed.scheme == "http"):
        raise UploadError("upload requires HTTPS (loopback HTTP is test-only)")
    if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise UploadError("base URL must contain only a server origin")
    if parsed.path not in ("", "/"):
        raise UploadError("base URL must contain only a server origin")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "/api/v1/seca/batches", "", ""))


def post(url, body, credential, timeout):
    request = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json", "Authorization": "Device " + credential})
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
            if response.status != 200:
                raise UploadError("unexpected upload HTTP status %s" % response.status)
            raw = response.read(16385)
    except urllib.error.HTTPError as exc:
        # Do not log remote response bodies: they can echo credentials/input.
        raise UploadError("upload rejected (HTTP %d); all files retained" % exc.code) from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise UploadError("upload transport failed; all files retained for exact retry") from None
    if len(raw) > 16384:
        raise UploadError("oversized upload acknowledgement")
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise UploadError("invalid upload acknowledgement") from None


def upload(outbox, base_url, credential_file, timeout=20.0, allow_local_http=False, sender=post, progress=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise UploadError("upload timeout must be finite and positive")
    outbox = Path(outbox)
    url = endpoint(base_url, allow_local_http)
    credentials = read_json(Path(credential_file), 16384)
    credential = credentials.get("device_credential")
    if not isinstance(credential, str) or not credential or any(ord(c) < 33 or ord(c) > 126 for c in credential):
        raise UploadError("invalid device credential file")
    installation = canonical_uuid(credentials.get("installation_id"))
    manifest = read_json(outbox / "ready.json", 16384)
    if manifest.get("schema_version") != 1 or manifest.get("installation_id") != installation:
        raise UploadError("queue and credential installation do not match")
    session = canonical_uuid(manifest.get("session_id"))
    count = integer(manifest.get("batch_count"), "batch count", 1, 999999999)
    lock_fd = os.open(str(outbox / ".upload.lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise UploadError("another upload is using this queue") from None
        receipts = outbox / "receipts"
        if not receipts.exists():
            receipts.mkdir(mode=0o700)
            sync_directory(outbox)
        uploaded = 0
        for index in range(count):
            name = "%010d.json" % index
            body = read_bounded(outbox / name, MAX_BODY)
            batch = json.loads(body)
            if (not isinstance(batch, dict) or batch.get("session_id") != session or batch.get("installation_id") != installation
                    or batch.get("batch_index") != index or batch.get("transport") != "usb"):
                raise UploadError("queue batch identity changed")
            expected = {"session_id": session, "batch_index": index, "sha256": digest(body)}
            receipt = receipts / name
            saved = {"endpoint": url, "ack": expected}
            if receipt.exists():
                if read_json(receipt, 16384) != saved:
                    raise UploadError("receipt does not match server/body; refusing to skip batch")
                if progress:
                    progress(index + 1, count)
                continue
            actual = sender(url, body, credential, timeout)
            if not isinstance(actual, dict) or any(actual.get(k) != v for k, v in expected.items()):
                raise UploadError("upload acknowledgement mismatch; all files retained")
            write_new(receipt, encode(saved))
            uploaded += 1
            if progress:
                progress(index + 1, count)
        return {"session_id": session, "batches_acknowledged": count,
                "newly_uploaded": uploaded, "local_files_retained": True}
    finally:
        os.close(lock_fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare", help="Create an immutable queue; no network")
    prep.add_argument("capture", type=Path)
    prep.add_argument("outbox", type=Path)
    prep.add_argument("--installation-id", required=True)
    send = commands.add_parser("upload", help="Upload an existing queue; retain local files")
    send.add_argument("outbox", type=Path)
    send.add_argument("--base-url", required=True)
    send.add_argument("--credential-file", required=True, type=Path)
    send.add_argument("--timeout", type=float, default=20.0)
    send.add_argument("--allow-local-http", action="store_true", help="loopback test servers only")
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(args.capture, args.outbox, args.installation_id)
        else:
            result = upload(args.outbox, args.base_url, args.credential_file,
                            args.timeout, args.allow_local_http)
        print(json.dumps(result))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        # Paths and protocol errors are actionable; credential file parse errors
        # must never print any file contents or request headers.
        message = str(exc) if isinstance(exc, UploadError) else type(exc).__name__
        print("seca upload failed: " + message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
