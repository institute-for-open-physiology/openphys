import json
import pathlib
import plistlib
import tempfile
import unittest
import zipfile

from package import package_app, write_source


class PackageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.app = self.root / "TrainingSensors.app"
        self.app.mkdir()
        self.info = {
            "CFBundleIdentifier": "test.sensors",
            "CFBundleExecutable": "TrainingSensors",
            "CFBundleShortVersionString": "1.2.3",
            "CFBundleVersion": "42",
            "CFBundleSupportedPlatforms": ["iPhoneOS"],
            "MinimumOSVersion": "17.0",
            "NSBluetoothAlwaysUsageDescription": "Connect to the H10.",
        }
        (self.app / "Info.plist").write_bytes(plistlib.dumps(self.info))
        (self.app / "TrainingSensors").write_bytes(b"test executable")
        (self.app / "TrainingSensors").chmod(0o755)
        self.icon = self.root / "input.png"
        self.icon.write_bytes(b"icon")

    def test_package_excludes_signing_and_watch_files_and_keeps_executable_mode(
        self,
    ) -> None:
        for name in [
            "Watch/Companion.app/Info.plist",
            "_CodeSignature/CodeResources",
            "embedded.mobileprovision",
        ]:
            path = self.app / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"must not ship")
        ipa = package_app(self.app, self.root / "release")
        with zipfile.ZipFile(ipa) as archive:
            self.assertEqual(
                set(archive.namelist()),
                {
                    "Payload/TrainingSensors.app/Info.plist",
                    "Payload/TrainingSensors.app/TrainingSensors",
                },
            )
            self.assertTrue(
                archive.getinfo(
                    "Payload/TrainingSensors.app/TrainingSensors"
                ).external_attr
                >> 16
                & 0o111
            )
        self.assertTrue((self.app / "Watch/Companion.app/Info.plist").exists())

    def test_feed_matches_packaged_version_size_permissions_and_download(self) -> None:
        ipa = package_app(self.app, self.root / "release")
        feed = write_source(ipa, self.icon, "https://downloads.example/sensors")
        source = json.loads(feed.read_text())
        app = source["apps"][0]
        version = app["versions"][0]
        self.assertEqual(app["bundleIdentifier"], self.info["CFBundleIdentifier"])
        self.assertEqual(version["version"], "1.2.3")
        self.assertEqual(version["buildVersion"], "42")
        self.assertEqual(version["size"], ipa.stat().st_size)
        self.assertEqual(version["minOSVersion"], "17.0")
        self.assertRegex(version["date"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(
            version["downloadURL"],
            "https://downloads.example/sensors/TrainingSensors-1.2.3-42.ipa",
        )
        self.assertEqual(
            app["appPermissions"]["privacy"],
            {"NSBluetoothAlwaysUsageDescription": "Connect to the H10."},
        )
        self.assertEqual((ipa.parent / "icon.png").read_bytes(), self.icon.read_bytes())

    def test_simulator_build_is_rejected(self) -> None:
        self.info["CFBundleSupportedPlatforms"] = ["iPhoneSimulator"]
        (self.app / "Info.plist").write_bytes(plistlib.dumps(self.info))
        with self.assertRaisesRegex(ValueError, "physical iPhone"):
            package_app(self.app, self.root / "release")

    def test_feed_discloses_health_capability_and_write_permission(self) -> None:
        self.info["NSHealthUpdateUsageDescription"] = "Save H10 heart rate to Health."
        (self.app / "Info.plist").write_bytes(plistlib.dumps(self.info))
        entitlements = self.root / "Health.entitlements"
        entitlements.write_bytes(
            plistlib.dumps({"com.apple.developer.healthkit": True})
        )
        ipa = package_app(self.app, self.root / "release")
        feed = write_source(
            ipa, self.icon, "https://downloads.example/sensors", entitlements
        )
        permissions = json.loads(feed.read_text())["apps"][0]["appPermissions"]
        self.assertEqual(permissions["entitlements"], ["com.apple.developer.healthkit"])
        self.assertEqual(
            permissions["privacy"]["NSHealthUpdateUsageDescription"],
            self.info["NSHealthUpdateUsageDescription"],
        )

    def test_download_url_rejects_credentials_and_non_https(self) -> None:
        ipa = package_app(self.app, self.root / "release")
        for url in [
            "http://example.com",
            "https://user:secret@example.com",
            "https://example.com?token=secret",
        ]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                write_source(ipa, self.icon, url)


if __name__ == "__main__":
    unittest.main()
