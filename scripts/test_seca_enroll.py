import base64
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import seca_enroll as enroll

TOKEN = base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip("=")
LINK = enroll.DEFAULT_ORIGIN + "/enroll#" + TOKEN


def response(request):
    return {"installation_id": request["installation_id"], "credential_id": str(uuid.uuid4()),
            "device_credential": "private-credential-sentinel-value"}


class EnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "mac"

    def test_private_durable_success_and_existing_credential_no_network(self):
        def sender(base, request):
            pending = enroll.read_private(self.directory / "pending.json")
            self.assertEqual(pending["request"], request)
            self.assertNotIn("android_sdk", request)
            return response(request)
        output, installation = enroll.enroll(self.directory, reader=lambda _: LINK, sender=sender)
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertFalse((self.directory / "pending.json").exists())
        self.assertEqual(enroll.read_private(output)["installation_id"], installation)
        self.assertEqual(enroll.enroll(self.directory, sender=lambda *_: self.fail("unexpected network")),
                         (output, installation))

    def test_lost_response_retry_reuses_identity_and_expired_replacement_preserves_installation(self):
        attempts = []
        def fail(base, request):
            attempts.append(request.copy())
            raise enroll.EnrollmentError("simulated ambiguous response")
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory, reader=lambda _: LINK, sender=fail)
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory, reader=lambda _: self.fail("must reuse pending link"), sender=fail)
        self.assertEqual(attempts[0], attempts[1])
        fresh = base64.urlsafe_b64encode(bytes(reversed(range(32)))).decode().rstrip("=")
        def success(base, request):
            self.assertEqual(request["installation_id"], attempts[0]["installation_id"])
            self.assertNotEqual(request["exchange_id"], attempts[0]["exchange_id"])
            self.assertEqual(request["enrollment_token"], fresh)
            return response(request)
        enroll.enroll(self.directory, reader=lambda _: enroll.DEFAULT_ORIGIN + "/enroll#" + fresh,
                      sender=success, replace_link=True)
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory, replace_link=True)

    def test_bad_response_keeps_pending_and_never_creates_credential(self):
        for bad in [[], {}, {"installation_id": str(uuid.uuid4())}]:
            with self.assertRaises(enroll.EnrollmentError):
                enroll.enroll(self.directory, reader=lambda _: LINK, sender=lambda *_: bad)
        self.assertTrue((self.directory / "pending.json").exists())
        self.assertFalse((self.directory / "credential.json").exists())

    def test_unsafe_permissions_symlink_and_wrong_origin_rejected(self):
        self.directory.mkdir(mode=0o755)
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory)
        self.directory.chmod(0o700)
        path = self.directory / "pending.json"
        path.write_text("{}")
        path.chmod(0o644)
        with self.assertRaises(enroll.EnrollmentError):
            enroll.read_private(path)
        link = self.directory / "link"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            enroll.read_private(link)
        with self.assertRaises(enroll.EnrollmentError):
            enroll.token_from_link("https://evil.test/enroll#" + TOKEN, enroll.DEFAULT_ORIGIN)
        for url in ["http://example.test", "https://u:p@example.test", "https://example.test/path"]:
            with self.assertRaises(enroll.EnrollmentError): enroll.origin(url)
        for token in ["bad!", "", TOKEN + "="]:
            with self.assertRaises(enroll.EnrollmentError):
                enroll.token_from_link(enroll.DEFAULT_ORIGIN + "/enroll#" + token, enroll.DEFAULT_ORIGIN)

    def test_exclusive_write_and_output_secrecy(self):
        enroll.private_directory(self.directory)
        path = self.directory / "pending.json"
        enroll.write_private_new(path, {"original": True})
        with self.assertRaises(FileExistsError): enroll.write_private_new(path, {"new": True})
        self.assertEqual(enroll.read_private(path), {"original": True})
        with patch.object(enroll, "enroll", side_effect=ValueError("private-credential-sentinel-value")), \
                patch("sys.stderr", new_callable=io.StringIO) as error:
            self.assertEqual(enroll.main(["--state-dir", str(self.directory)]), 1)
        self.assertNotIn("private-credential", error.getvalue())

    def test_numeric_uuid_and_echoing_prompt_fallback_fail_safely(self):
        for value in [None, 3, {}, [], True]:
            with self.assertRaises(enroll.EnrollmentError): enroll.checked_uuid(value)
            with self.assertRaises(enroll.EnrollmentError):
                enroll.validate_credential({"installation_id":str(uuid.uuid4()),"credential_id":value},"different")
        installation=str(uuid.uuid4())
        with self.assertRaises(enroll.EnrollmentError):
            enroll.validate_credential({"installation_id":installation,"credential_id":7},installation)
        enroll.private_directory(self.directory)
        enroll.write_private_new(self.directory/"pending.json", {"server_origin":enroll.DEFAULT_ORIGIN,
            "request":{"installation_id":5,"exchange_id":str(uuid.uuid4()),"enrollment_token":TOKEN}})
        with self.assertRaises(enroll.EnrollmentError): enroll.enroll(self.directory)
        import warnings
        def unsafe_prompt(_):
            warnings.warn("would echo",enroll.getpass.GetPassWarning)
            self.fail("must not reach an echoing input read")
        with patch("getpass.getpass",side_effect=unsafe_prompt):
            with self.assertRaisesRegex(enroll.EnrollmentError,"privately"):
                enroll.hidden_link_prompt("hidden")

    def test_incomplete_http_response_and_ancestor_sync_failure_preserve_retry(self):
        import http.client
        with patch("urllib.request.build_opener") as opener:
            opener.return_value.open.side_effect=http.client.IncompleteRead(b"secret-partial")
            with self.assertRaises(enroll.EnrollmentError) as captured: enroll.exchange(enroll.DEFAULT_ORIGIN,{})
            self.assertNotIn("secret-partial",str(captured.exception))
        with patch.object(enroll,"sync_directory",side_effect=OSError("sync failed")):
            with self.assertRaises(OSError):
                enroll.enroll(self.directory/"nested",reader=lambda _:self.fail("must sync before asking for token"))
        synced=[]
        original=enroll.sync_directory
        with patch.object(enroll,"sync_directory",side_effect=lambda path:(synced.append(path),original(path))):
            enroll.enroll(self.directory/"nested",reader=lambda _:LINK,sender=lambda _,request:response(request))
        self.assertIn(self.directory.parent,synced)
        self.assertIn(self.directory,synced)

    def test_transport_response_bounds_and_timeout_are_redacted(self):
        for raw, expected_error in [(b'{"ok":true}', False), (b'x' * 16385, True), (b'private-secret-bad-json', True)]:
            with patch("urllib.request.build_opener") as opener:
                opener.return_value.open.return_value.__enter__.return_value.read.return_value = raw
                if expected_error:
                    with self.assertRaises(enroll.EnrollmentError) as captured:
                        enroll.exchange(enroll.DEFAULT_ORIGIN, {})
                    self.assertNotIn("private-secret", str(captured.exception))
                else:
                    self.assertEqual(enroll.exchange(enroll.DEFAULT_ORIGIN, {}), {"ok": True})
                self.assertEqual(opener.return_value.open.call_args.kwargs["timeout"], 20)
        with patch("urllib.request.build_opener") as opener:
            opener.return_value.open.side_effect=TimeoutError("private-secret")
            with self.assertRaises(enroll.EnrollmentError): enroll.exchange(enroll.DEFAULT_ORIGIN,{})

    def test_concurrent_enrollment_and_bad_credentials_are_rejected(self):
        def sender(base, request):
            with self.assertRaisesRegex(enroll.EnrollmentError,"Another enrollment"):
                enroll.enroll(self.directory, sender=lambda *_: self.fail("parallel request"))
            return response(request)
        enroll.enroll(self.directory, reader=lambda _: LINK, sender=sender)
        for bad in [{"installation_id":"x"}, {"installation_id":str(uuid.uuid4()),"credential_id":str(uuid.uuid4()),"device_credential":"bad\nsecret"}]:
            with self.assertRaises(enroll.EnrollmentError): enroll.validate_credential(bad,bad["installation_id"])
        for bad_label in ["", "bad\x85label"]:
            with self.assertRaises(enroll.EnrollmentError): enroll.enroll(self.directory,label=bad_label)
        with patch.object(enroll,"enroll",return_value=(self.directory/"credential.json","public-id")), patch("sys.stdout",new_callable=io.StringIO) as out:
            self.assertEqual(enroll.main([]),0)
        self.assertIn("public-id",out.getvalue())
        self.assertNotIn("private-credential",out.getvalue())

    def test_crash_after_output_commit_recovers_pending_cleanup(self):
        def sender(base, request):
            # Simulate a process that received and durably saved the response, but
            # died before removing its pending identity.
            value=response(request);value["server_origin"]=base
            enroll.write_private_new(self.directory/"credential.json",value)
            raise enroll.EnrollmentError("simulated crash")
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory,reader=lambda _:LINK,sender=sender)
        self.assertTrue((self.directory/"pending.json").exists())
        enroll.enroll(self.directory,sender=lambda *_:self.fail("must use saved response"))
        self.assertFalse((self.directory/"pending.json").exists())
        with self.assertRaises(enroll.EnrollmentError):
            enroll.enroll(self.directory,base_url="https://another.test")

    def test_redirects_refused_and_http_errors_redacted(self):
        self.assertIsNone(enroll.NoRedirect().redirect_request(None, None, 302, None, None, "https://evil.test"))
        import urllib.error
        error = urllib.error.HTTPError("secret-url", 400, "secret body", None, None)
        with patch("urllib.request.build_opener") as opener:
            opener.return_value.open.side_effect = error
            with self.assertRaisesRegex(enroll.EnrollmentError, r"HTTP 400") as captured:
                enroll.exchange(enroll.DEFAULT_ORIGIN, {"enrollment_token": TOKEN})
        self.assertNotIn("secret", str(captured.exception))


if __name__ == "__main__":
    unittest.main()
