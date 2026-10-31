import os
import pathlib
import plistlib
import tempfile
import unittest
from unittest import mock

import testflight


class TestFlightTests(unittest.TestCase):
    def test_passes_api_authentication_to_archive_and_export(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            key = root / "AuthKey.p8"
            key.write_text("test fixture, not a private key")
            environment = {
                "ASC_KEY_ID": "KEY1234567",
                "ASC_ISSUER_ID": "test-issuer",
                "ASC_PRIVATE_KEY_PATH": str(key),
            }

            def export_ipa(command: list[str], **kwargs: object) -> None:
                if "-exportArchive" in command:
                    export = root / "dist/TestFlight/export"
                    export.mkdir(parents=True, exist_ok=True)
                    (export / "TrainingSensors.ipa").write_bytes(b"signed IPA fixture")

            with (
                mock.patch.object(
                    testflight, "__file__", str(root / "scripts/testflight.py")
                ),
                mock.patch.dict(os.environ, environment, clear=True),
                mock.patch(
                    "sys.argv", ["testflight.py", "--team", "TEAM123456", "--upload"]
                ),
                mock.patch.object(
                    testflight.subprocess, "run", side_effect=export_ipa
                ) as run,
            ):
                testflight.main()
            for call in run.call_args_list[1:3]:
                command = call.args[0]
                self.assertEqual(
                    command[command.index("-authenticationKeyPath") + 1], str(key)
                )
                self.assertIn("-allowProvisioningUpdates", command)
                self.assertIn("KEY1234567", command)
                self.assertIn("test-issuer", command)
            options = plistlib.loads(
                (root / "dist/TestFlight/ExportOptions.plist").read_bytes()
            )
            self.assertEqual(options["destination"], "export")
            upload = run.call_args_list[3].args[0]
            self.assertEqual(upload[:3], ["xcrun", "altool", "--upload-app"])
            self.assertEqual(upload[upload.index("--p8-file-path") + 1], str(key))
            self.assertIn("KEY1234567", upload)
            self.assertIn("test-issuer", upload)
            self.assertEqual(options["method"], "app-store-connect")

    def test_rejects_partial_api_credentials_before_build(self) -> None:
        with (
            mock.patch.dict(os.environ, {"ASC_KEY_ID": "KEY1234567"}, clear=True),
            mock.patch("sys.argv", ["testflight.py", "--team", "TEAM123456"]),
            mock.patch.object(testflight.subprocess, "run") as run,
            mock.patch("sys.stderr"),
        ):
            with self.assertRaises(SystemExit):
                testflight.main()
            run.assert_not_called()

    def test_rejects_missing_api_key_file_before_build(self) -> None:
        with (
            mock.patch.dict(
                os.environ,
                {
                    "ASC_KEY_ID": "KEY1234567",
                    "ASC_ISSUER_ID": "test-issuer",
                    "ASC_PRIVATE_KEY_PATH": "/nonexistent/AuthKey.p8",
                },
                clear=True,
            ),
            mock.patch("sys.argv", ["testflight.py", "--team", "TEAM123456"]),
            mock.patch.object(testflight.subprocess, "run") as run,
            mock.patch("sys.stderr"),
        ):
            with self.assertRaises(SystemExit):
                testflight.main()
            run.assert_not_called()

    def test_failed_export_never_uploads(self) -> None:
        import subprocess

        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            key = root / "AuthKey.p8"
            key.write_text("test fixture")
            environment = {
                "ASC_KEY_ID": "KEY1234567",
                "ASC_ISSUER_ID": "test-issuer",
                "ASC_PRIVATE_KEY_PATH": str(key),
            }

            def fail_export(command: list[str], **kwargs: object) -> None:
                if "-exportArchive" in command:
                    raise subprocess.CalledProcessError(70, command)

            with (
                mock.patch.object(
                    testflight, "__file__", str(root / "scripts/testflight.py")
                ),
                mock.patch.dict(os.environ, environment, clear=True),
                mock.patch(
                    "sys.argv", ["testflight.py", "--team", "TEAM123456", "--upload"]
                ),
                mock.patch.object(
                    testflight.subprocess, "run", side_effect=fail_export
                ) as run,
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    testflight.main()
                self.assertEqual(len(run.call_args_list), 3)
