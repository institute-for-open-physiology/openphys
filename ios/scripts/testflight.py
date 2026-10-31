#!/usr/bin/env python3
"""Archive the iPhone and Watch apps with Apple distribution signing."""

import argparse
import os
import pathlib
import plistlib
import re
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team", required=True, help="Your Apple Developer Team ID")
    parser.add_argument(
        "--upload", action="store_true", help="Upload to App Store Connect"
    )
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Z0-9]{10}", args.team):
        parser.error("Use the ten-character Apple Developer Team ID.")

    authentication: list[str] = []
    credentials = [
        os.environ.get(name, "")
        for name in ("ASC_KEY_ID", "ASC_ISSUER_ID", "ASC_PRIVATE_KEY_PATH")
    ]
    if any(credentials):
        if not all(credentials):
            parser.error("Set all three ASC API credential variables.")
        key_id, issuer_id, key_path = credentials
        if not pathlib.Path(key_path).is_file():
            parser.error("ASC_PRIVATE_KEY_PATH must point to the API key file.")
        authentication = [
            "-authenticationKeyID",
            key_id,
            "-authenticationKeyIssuerID",
            issuer_id,
            "-authenticationKeyPath",
            key_path,
        ]

    project = pathlib.Path(__file__).resolve().parents[1]
    output = project / "dist" / "TestFlight"
    output.mkdir(parents=True, exist_ok=True)
    archive = output / "TrainingSensors.xcarchive"
    options = output / "ExportOptions.plist"
    options.write_bytes(
        plistlib.dumps(
            {
                "method": "app-store-connect",
                "destination": "upload"
                if args.upload and not authentication
                else "export",
                "signingStyle": "automatic",
                "teamID": args.team,
                "manageAppVersionAndBuildNumber": False,
                "uploadSymbols": True,
            }
        )
    )
    subprocess.run(["xcodegen", "generate"], cwd=project, check=True)
    subprocess.run(
        [
            "xcodebuild",
            "archive",
            "-project",
            "TrainingSensors.xcodeproj",
            "-scheme",
            "TrainingSensors",
            "-configuration",
            "Release",
            "-destination",
            "generic/platform=iOS",
            "-archivePath",
            str(archive),
            "-allowProvisioningUpdates",
            *authentication,
            "CODE_SIGN_STYLE=Automatic",
            f"DEVELOPMENT_TEAM={args.team}",
        ],
        cwd=project,
        check=True,
    )
    subprocess.run(
        [
            "xcodebuild",
            "-exportArchive",
            "-archivePath",
            str(archive),
            "-exportOptionsPlist",
            str(options),
            "-exportPath",
            str(output / "export"),
            "-allowProvisioningUpdates",
            *authentication,
        ],
        cwd=project,
        check=True,
    )

    if args.upload and authentication:
        (ipa,) = (output / "export").glob("*.ipa")
        subprocess.run(
            [
                "xcrun",
                "altool",
                "--upload-app",
                "--type",
                "ios",
                "--file",
                str(ipa),
                "--apiKey",
                key_id,
                "--apiIssuer",
                issuer_id,
                "--p8-file-path",
                key_path,
            ],
            cwd=project,
            check=True,
        )


if __name__ == "__main__":
    main()
