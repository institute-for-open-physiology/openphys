#!/usr/bin/env python3
"""Privately enroll a native Mac installation for the seca USB uploader."""
import argparse
import base64
import fcntl
import getpass
import http.client
import json
import os
from pathlib import Path
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings

DEFAULT_ORIGIN = "https://sensors-poc.vineyard-finance.com"
VERSION = "1.0"


class EnrollmentError(Exception):
    pass


def hidden_link_prompt(prompt):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            return getpass.getpass(prompt)
    except getpass.GetPassWarning:
        raise EnrollmentError("Cannot read the link privately; run this command in an interactive terminal.") from None


def checked_uuid(value):
    if not isinstance(value, str):
        raise EnrollmentError("Enrollment UUID must be a string.")
    try:
        return uuid.UUID(value)
    except ValueError:
        raise EnrollmentError("Enrollment UUID is malformed.") from None


def origin(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise EnrollmentError("The server must be an HTTPS origin without a path.")
    return value.rstrip("/")


def token_from_link(link, base_url):
    parsed = urllib.parse.urlsplit(link.strip())
    if (f"{parsed.scheme}://{parsed.netloc}" != base_url or parsed.path != "/enroll"
            or parsed.query or parsed.username or parsed.password):
        raise EnrollmentError("Use a connection link from the configured server.")
    token = parsed.fragment
    try:
        decoded = base64.b64decode(token + "=", altchars=b"-_", validate=True)
    except (ValueError, UnicodeError) as exc:
        raise EnrollmentError("The connection link is malformed.") from exc
    if len(token) != 43 or len(decoded) != 32 or base64.urlsafe_b64encode(decoded).decode().rstrip("=") != token:
        raise EnrollmentError("The connection link is malformed.")
    return token


def mkdir_durable(path):
    if path.parent != path:
        mkdir_durable(path.parent)
    if not path.is_dir():
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            if not path.is_dir():
                raise
    # Existing entries also need synchronization after an ambiguous prior failure.
    if path.parent != path:
        sync_directory(path.parent)


def private_directory(path):
    path = Path(path).expanduser().absolute()
    mkdir_durable(path)
    metadata = path.lstat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077):
        raise EnrollmentError("State directory must be owned by you and mode 0700.")
    return path


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def read_private(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(fd)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077 or metadata.st_size > 16384):
            raise EnrollmentError("Enrollment files must be private, owner-owned regular files.")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            value = json.load(stream)
        if not isinstance(value, dict):
            raise EnrollmentError("Enrollment state is invalid.")
        # A prior create may have become visible before its fsync failed. Reassert
        # durability before using a pending identity or accepting saved credentials.
        os.fsync(fd)
        sync_directory(path.parent)
        return value
    finally:
        os.close(fd)


def write_private_new(path, value, replace=False):
    # Link rather than replace: existing state/credentials must never be overwritten.
    temp = path.parent / ("." + uuid.uuid4().hex + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(json.dumps(value, separators=(",", ":")).encode())
            stream.flush()
            os.fsync(fd)
        if replace:
            os.replace(temp, path)
        else:
            os.link(temp, path)
        sync_directory(path.parent)
    finally:
        os.close(fd)
        temp.unlink(missing_ok=True)
        sync_directory(path.parent)


def validate_credential(value, installation):
    if value.get("installation_id") != installation:
        raise EnrollmentError("Server returned a different installation identity.")
    try:
        checked_uuid(value["credential_id"])
        checked_uuid(value["installation_id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise EnrollmentError("Server credential identity is malformed.") from exc
    credential = value.get("device_credential")
    if (not isinstance(credential, str) or not 16 <= len(credential) <= 1024
            or any(ord(c) < 33 or ord(c) > 126 for c in credential)):
        raise EnrollmentError("Server credential is malformed.")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def exchange(base_url, request):
    body = json.dumps(request, separators=(",", ":")).encode()
    req = urllib.request.Request(base_url + "/api/v1/device-enrollments/exchange-macos",
                                 data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=20) as response:
            raw = response.read(16385)
        if len(raw) > 16384:
            raise EnrollmentError("Server response exceeds the expected size.")
        return json.loads(raw)
    except urllib.error.HTTPError as exc:
        raise EnrollmentError(f"Enrollment failed (HTTP {exc.code}); pending identity retained.") from None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, http.client.HTTPException) as exc:
        # Never echo a response body, request URL, or transport exception containing secrets.
        raise EnrollmentError("Enrollment response unavailable; run again to retry the same identity.") from None


def enroll(state_dir, base_url=DEFAULT_ORIGIN, label="Mac USB collector", reader=hidden_link_prompt, sender=exchange, replace_link=False):
    base_url = origin(base_url)
    if not label.strip() or len(label.encode()) > 64 or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in label):
        raise EnrollmentError("Choose a nonempty label of at most 64 UTF-8 bytes without control characters.")
    directory = private_directory(state_dir)
    lock = os.open(directory / "enrollment.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(lock)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
            raise EnrollmentError("Enrollment lock must be a private owner-owned file.")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise EnrollmentError("Another enrollment process is using this state directory.") from None
        return enroll_locked(directory, base_url, label, reader, sender, replace_link)
    finally:
        os.close(lock)


def enroll_locked(directory, base_url, label, reader, sender, replace_link):
    pending_path, output = directory / "pending.json", directory / "credential.json"
    if output.exists():
        if replace_link:
            raise EnrollmentError("A credential is already saved; replacement is only for incomplete enrollment.")
        saved = read_private(output)
        validate_credential(saved, saved.get("installation_id"))
        if saved.get("server_origin") != base_url:
            raise EnrollmentError("Existing credential belongs to another server.")
        if pending_path.exists():
            pending = read_private(pending_path)
            pending_request = pending.get("request")
            if not isinstance(pending_request, dict) or pending_request.get("installation_id") != saved["installation_id"]:
                raise EnrollmentError("Pending and saved installation identities disagree.")
            pending_path.unlink()
            sync_directory(directory)
        return output, saved["installation_id"]
    if replace_link and not pending_path.exists():
        raise EnrollmentError("There is no incomplete enrollment to replace.")
    if pending_path.exists():
        pending = read_private(pending_path)
        if pending.get("server_origin") != base_url:
            raise EnrollmentError("Pending enrollment belongs to another server.")
        request = pending.get("request")
        if not isinstance(request, dict):
            raise EnrollmentError("Pending enrollment state is invalid.")
        try:
            checked_uuid(request["installation_id"])
            checked_uuid(request["exchange_id"])
            token_from_link(base_url + "/enroll#" + request["enrollment_token"], base_url)
        except (KeyError, TypeError, ValueError) as exc:
            raise EnrollmentError("Pending enrollment state is invalid.") from exc
    else:
        token = token_from_link(reader("Paste the Mac connection link (hidden): "), base_url)
        request = {"enrollment_token": token, "installation_id": str(uuid.uuid4()),
                   "exchange_id": str(uuid.uuid4()), "display_label": label, "client_version": VERSION}
        write_private_new(pending_path, {"server_origin": base_url, "request": request})
    if replace_link:
        # Explicit recovery from an expired/invalid pending link preserves the Mac
        # installation. A new exchange rotates any credential from a lost response.
        request = dict(request)
        request["enrollment_token"] = token_from_link(reader("Paste a fresh Mac connection link (hidden): "), base_url)
        request["exchange_id"] = str(uuid.uuid4())
        write_private_new(pending_path, {"server_origin": base_url, "request": request}, replace=True)
    response = sender(base_url, request)
    if not isinstance(response, dict):
        raise EnrollmentError("Server response is malformed.")
    validate_credential(response, request["installation_id"])
    saved = {key: response[key] for key in ("installation_id", "credential_id", "device_credential")}
    saved["server_origin"] = base_url
    write_private_new(output, saved)
    pending_path.unlink()
    sync_directory(directory)
    return output, saved["installation_id"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path.home() / "Library/Application Support/TrainingSensors/seca-usb")
    parser.add_argument("--base-url", default=DEFAULT_ORIGIN)
    parser.add_argument("--label", default="Mac USB collector")
    parser.add_argument("--replace-link", action="store_true", help="replace an expired pending link while retaining this Mac installation")
    args = parser.parse_args(argv)
    try:
        output, installation = enroll(args.state_dir, args.base_url, args.label, replace_link=args.replace_link)
        print(f"Mac installation: {installation}\nPrivate credential file: {output}")
        return 0
    except (EnrollmentError, OSError, ValueError, EOFError, KeyboardInterrupt) as exc:
        # Generic OS/JSON errors may include private file contents. Only our
        # fixed, sanitized EnrollmentError messages are suitable for printing.
        message = str(exc) if isinstance(exc, EnrollmentError) else "Could not complete enrollment; private state retained for retry."
        print(message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
