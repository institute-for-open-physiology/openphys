"""Durability, retry and HTTP contract tests; all recordings/credentials synthetic."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, HTTPServer

import seca_upload as upload


INSTALLATION = "10000000-0000-0000-0000-000000000001"
SESSION = "20000000-0000-0000-0000-000000000002"


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.capture = self.root / "capture"
        self.capture.mkdir()
        self.outbox = self.root / "outbox"
        self.credentials = self.root / "credential.json"
        self.credentials.write_bytes(upload.encode({"installation_id": INSTALLATION,
                                                   "device_credential": "test-only-secret"}))
        self.metadata = {"schema_version": 1, "transport": "usb", "session_id": SESSION,
                         "requested_sample_rate_hz": 32000, "gain": 6,
                         "contact_detection": False, "device_info": {"sn": "synthetic"}}

    def wire(self, index=0, raw=b"\x55\xaa\x00\x0b\x00\x00\x00\x00\x00\xff\xf6"):
        return {"type": "wire", "direction": "rx", "acquisition": True, "unix_ns": 1792580000000000000 + index,
                "monotonic_ns": 10000 + index, "hex": raw.hex()}

    def capture_files(self, rows=None, success=True):
        if rows is None:
            rows = [self.wire()]
        events = [{"type": "event", "event": "configuration_confirmed",
                   "details": {"sample_rate_hz": self.metadata["requested_sample_rate_hz"], "gain": 6,
                               "contact_detection": self.metadata["contact_detection"]}}]
        events.append({"type": "event", "event": "acquisition_start"})
        events.extend(rows)
        events.append({"type": "event", "event": "session_end", "success": success})
        (self.capture / "session.json").write_bytes(upload.encode(self.metadata))
        (self.capture / "records.jsonl").write_bytes(b"".join(upload.encode(r) + b"\n" for r in events))

    def prepare(self, rows=None):
        self.capture_files(rows)
        return upload.prepare(self.capture, self.outbox, INSTALLATION)

    def sender(self, url, body, credential, timeout):
        self.assertEqual(url, "https://carolina.invalid/api/v1/seca/batches")
        self.assertEqual(credential, "test-only-secret")
        self.assertGreater(timeout, 0)
        batch = json.loads(body)
        return {"session_id": batch["session_id"], "batch_index": batch["batch_index"],
                "sha256": hashlib.sha256(body).hexdigest()}

    def send(self, sender=None, **kwargs):
        return upload.upload(self.outbox, "https://carolina.invalid", self.credentials,
                             sender=sender or self.sender, **kwargs)

    def test_all_rates_prepare_and_resume_keep_every_original_file(self):
        for rate in upload.RATES:
            with self.subTest(rate=rate):
                self.metadata["requested_sample_rate_hz"] = rate
                self.outbox = self.root / str(rate)
                manifest = self.prepare([self.wire(i) for i in range(260)])
                self.assertEqual(manifest["batch_count"], 2)
                self.assertEqual(manifest["record_count"], 260)
                original = (self.capture / "records.jsonl").read_bytes()
                first = upload.read_json(self.outbox / "0000000000.json")
                self.assertEqual((first["transport"], first["sample_rate_hz"]), ("usb", rate))
                self.assertEqual(len(first["records"]), 256)
                result = self.send()
                self.assertEqual(result["newly_uploaded"], 2)
                self.assertEqual(self.send()["newly_uploaded"], 0)
                self.assertEqual((self.capture / "records.jsonl").read_bytes(), original)
                self.assertTrue((self.outbox / "0000000001.json").exists())

    def test_size_sealing_is_bounded_and_failed_capture_is_labelled(self):
        self.capture_files([self.wire(i, b"x" * 65535) for i in range(20)], success=False)
        manifest = upload.prepare(self.capture, self.outbox, INSTALLATION)
        self.assertFalse(manifest["capture_success"])
        self.assertGreater(manifest["batch_count"], 1)
        batches = [upload.read_json(self.outbox / ("%010d.json" % i)) for i in range(manifest["batch_count"])]
        self.assertEqual(sum(len(b["records"]) for b in batches), 20)
        self.assertEqual(batches[-1]["end_reason"], "usb_capture_failed_see_local_journal")
        self.assertTrue(all(len(upload.encode(b)) <= upload.MAX_BODY for b in batches))

    def test_prepare_does_not_overwrite_or_mark_invalid_capture_ready(self):
        self.prepare()
        with self.assertRaises(FileExistsError):
            upload.prepare(self.capture, self.outbox, INSTALLATION)
        mutations = {
            "truncated": lambda b: b[:-1],
            "unclosed": lambda b: b[:b.rfind(b'{"type":"event","event":"session_end"')],
            "after_close": lambda b: b + upload.encode(self.wire()) + b"\n",
            "unconfirmed": lambda b: b.replace(b'configuration_confirmed', b'configuration_requested'),
            "wrong_config": lambda b: b.replace(b'"sample_rate_hz":32000', b'"sample_rate_hz":4000'),
            "wrong_contact": lambda b: b.replace(b'"contact_detection":false', b'"contact_detection":true'),
            "missing_start": lambda b: b.replace(b'acquisition_start', b'other_event'),
            "before_config": lambda b: b.replace(b'configuration_confirmed', b'configuration_pending'),
            "no_success": lambda b: b.replace(b',"success":true', b''),
            "unknown": lambda b: b.replace(b'"type":"wire"', b'"type":"mystery"'),
            "bad_json": lambda b: b'nope\n' + b,
            "array": lambda b: b'[]\n' + b,
            "oversize": lambda b: b'x' * (upload.MAX_LINE + 1) + b'\n',
        }
        journal = self.capture / "records.jsonl"
        original = journal.read_bytes()
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                queue = self.root / name
                journal.write_bytes(mutate(original))
                with self.assertRaises(upload.UploadError):
                    upload.prepare(self.capture, queue, INSTALLATION)
                self.assertFalse((queue / "ready.json").exists())

    def test_invalid_metadata(self):
        for key, value in [("schema_version", 2), ("transport", "bluetooth"),
                           ("requested_sample_rate_hz", True), ("gain", 7),
                           ("contact_detection", 0), ("session_id", "bad"),
                           ("device_info", {}), ("device_info", []), ("device_info", {"sn": "x" * 97})]:
            with self.subTest(key=key, value=value):
                original = self.metadata.copy()
                self.metadata[key] = value
                self.capture_files()
                with self.assertRaises(upload.UploadError):
                    upload.prepare(self.capture, self.outbox, INSTALLATION)
                self.metadata = original

    def test_bad_wire_metadata_and_clock_regression(self):
        for key, value in [("direction", "other"), ("unix_ns", 4), ("monotonic_ns", -1),
                           ("unix_ns", True), ("hex", "zz"), ("hex", ""), ("hex", None),
                           ("acquisition", "true"), ("acquisition", None), ("direction", "tx")]:
            with self.subTest(key=key, value=value):
                row = self.wire()
                row[key] = value
                with self.assertRaises(upload.UploadError):
                    upload.wire_record(row)
        with self.assertRaises(upload.UploadError):
            upload.wire_record(self.wire(raw=b"x" * 65536))
        self.capture_files([self.wire(2), self.wire(1)])
        with self.assertRaisesRegex(upload.UploadError, "regressed"):
            upload.prepare(self.capture, self.outbox, INSTALLATION)

    def test_ambiguous_network_failure_retries_identical_bytes(self):
        self.prepare([self.wire(i) for i in range(257)])
        transmitted = []
        def ambiguous(url, body, credential, timeout):
            transmitted.append(body)
            if len(transmitted) == 2:
                raise upload.UploadError("simulated response lost after server committed")
            return self.sender(url, body, credential, timeout)
        with self.assertRaises(upload.UploadError):
            self.send(ambiguous)
        self.assertTrue((self.outbox / "receipts/0000000000.json").exists())
        retried = []
        def retry(*args):
            retried.append(args[1])
            return self.sender(*args)
        self.assertEqual(self.send(retry)["newly_uploaded"], 1)
        self.assertEqual(transmitted[1], retried[0])

    def test_ack_mismatch_never_marks_batch_acknowledged(self):
        self.prepare()
        for wrong in [None, [], {}, {"session_id": SESSION, "batch_index": 0, "sha256": "wrong"}]:
            with self.subTest(ack=wrong):
                with self.assertRaisesRegex(upload.UploadError, "mismatch"):
                    self.send(lambda *args: wrong)
                self.assertFalse((self.outbox / "receipts/0000000000.json").exists())

    def test_ack_storage_failure_keeps_batch_for_retry(self):
        self.prepare()
        with patch.object(upload, "write_new", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.send()
        self.assertTrue((self.outbox / "0000000000.json").exists())
        self.assertEqual(self.send()["newly_uploaded"], 1)

    def test_changed_receipt_or_body_and_concurrent_upload_refused(self):
        self.prepare()
        lock = os.open(str(self.outbox / ".upload.lock"), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            upload.fcntl.flock(lock, upload.fcntl.LOCK_EX | upload.fcntl.LOCK_NB)
            with self.assertRaisesRegex(upload.UploadError, "another upload"):
                self.send()
        finally:
            os.close(lock)
        self.send()
        body_path = self.outbox / "0000000000.json"
        body = body_path.read_bytes()
        body_path.write_bytes(body + b" ")
        with self.assertRaisesRegex(upload.UploadError, "receipt"):
            self.send()
        body_path.write_bytes(body.replace(b'"transport":"usb"', b'"transport":"bluetooth"'))
        with self.assertRaisesRegex(upload.UploadError, "identity"):
            self.send()

    def test_config_and_input_bounds(self):
        self.prepare()
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(upload.UploadError):
                self.send(timeout=timeout)
        self.credentials.write_bytes(upload.encode({"device_credential": "bad\nheader", "installation_id": INSTALLATION}))
        with self.assertRaises(upload.UploadError):
            self.send()
        self.credentials.write_bytes(upload.encode({"device_credential": "test", "installation_id": SESSION}))
        with self.assertRaisesRegex(upload.UploadError, "installation"):
            self.send()
        with self.assertRaises(upload.UploadError):
            upload.read_bounded(self.outbox / "ready.json", 2)
        with self.assertRaises(upload.UploadError):
            upload.canonical_uuid(None)
        with self.assertRaises(upload.UploadError):
            upload.integer(True, "count", 0, 2)
        target = self.root / "immutable"
        upload.write_new(target, b"first")
        with self.assertRaises(FileExistsError):
            upload.write_new(target, b"second")
        self.assertEqual(target.read_bytes(), b"first")
        for content in (b"[]", b"null", b'"secret-never-printed"'):
            target.write_bytes(content)
            with self.assertRaisesRegex(upload.UploadError, "must contain an object"):
                upload.read_json(target)

    def test_origin_validation(self):
        for origin in ("http://remote.invalid", "https://user:password@x", "https://x?q=1",
                       "https://x#secret", "https://x/path", "https:///", "ftp://localhost"):
            with self.subTest(origin=origin), self.assertRaises(upload.UploadError):
                upload.endpoint(origin, allow_local_http=True)
        self.assertEqual(upload.endpoint("http://127.0.0.1:8000", True),
                         "http://127.0.0.1:8000/api/v1/seca/batches")

    def test_cli_success_and_redacted_failures(self):
        self.capture_files()
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(upload.main(["prepare", str(self.capture), str(self.outbox),
                                          "--installation-id", INSTALLATION]), 0)
            self.assertEqual(upload.main(["prepare", str(self.capture), str(self.outbox),
                                          "--installation-id", INSTALLATION]), 1)
            with patch.object(upload, "upload", return_value={"ok": True}):
                self.assertEqual(upload.main(["upload", str(self.outbox), "--base-url", "https://x",
                                              "--credential-file", str(self.credentials)]), 0)
            with patch.object(upload, "upload", side_effect=upload.UploadError("queue remains")):
                self.assertEqual(upload.main(["upload", str(self.outbox), "--base-url", "https://x",
                                              "--credential-file", str(self.credentials)]), 1)
        self.assertNotIn("test-only-secret", out.getvalue() + err.getvalue())

    def test_http_exact_body_and_no_redirect_or_error_body_leak(self):
        self.prepare()
        mode = ["ok"]
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                requests.append((body, self.headers.get("Authorization")))
                if mode[0] == "redirect":
                    self.send_response(307)
                    self.send_header("Location", "/must-not-follow")
                    self.end_headers()
                    return
                if mode[0] == "reject":
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(b"test-only-secret")
                    return
                self.send_response(200)
                self.end_headers()
                if mode[0] == "oversize":
                    self.wfile.write(b"x" * 16385)
                elif mode[0] == "malformed":
                    self.wfile.write(b"not-json")
                else:
                    batch = json.loads(body)
                    self.wfile.write(upload.encode({"session_id": batch["session_id"],
                                                   "batch_index": batch["batch_index"],
                                                   "sha256": upload.digest(body)}))
        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(worker.join, 2)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = "http://127.0.0.1:%d/api/v1/seca/batches" % server.server_port
        body = (self.outbox / "0000000000.json").read_bytes()
        result = upload.post(url, body, "test-only-secret", 2)
        self.assertEqual(result["sha256"], upload.digest(body))
        self.assertEqual(requests[-1], (body, "Device test-only-secret"))
        for value in ("redirect", "reject", "oversize", "malformed"):
            mode[0] = value
            before = len(requests)
            with self.subTest(mode=value), self.assertRaises(upload.UploadError) as caught:
                upload.post(url, body, "test-only-secret", 2)
            self.assertNotIn("test-only-secret", str(caught.exception))
            self.assertEqual(len(requests), before + 1)
        with patch.object(upload.urllib.request, "build_opener", side_effect=OSError("network")):
            with self.assertRaisesRegex(upload.UploadError, "transport failed"):
                upload.post(url, body, "test-only-secret", 2)


if __name__ == "__main__":
    unittest.main()
