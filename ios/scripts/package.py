#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# ///
"""Build an iPhone IPA and prepare a SideStore feed without uploading files."""

import argparse
import datetime
import hashlib
import html
import json
import pathlib
import plistlib
import shutil
import subprocess
import tempfile
import urllib.parse
import zipfile


def package_app(
    app: pathlib.Path, output: pathlib.Path, entitlements: pathlib.Path | None = None
) -> pathlib.Path:
    info = plistlib.loads((app / "Info.plist").read_bytes())
    if info.get("CFBundleSupportedPlatforms") != ["iPhoneOS"]:
        raise ValueError("Build for a physical iPhone, not the simulator.")
    if not (app / info["CFBundleExecutable"]).is_file():
        raise ValueError("App executable is missing.")
    version = info["CFBundleShortVersionString"]
    build = info["CFBundleVersion"]
    output.mkdir(parents=True, exist_ok=True)
    ipa = output / f"TrainingSensors-{version}-{build}.ipa"
    with tempfile.TemporaryDirectory(prefix="training-sensors-package-") as temporary:
        root = pathlib.Path(temporary)
        # SideStore signs with the tester's account. Ship only the iPhone app.
        shutil.copytree(
            app,
            root / "Payload" / app.name,
            ignore=shutil.ignore_patterns(
                "Watch", "_CodeSignature", "embedded.mobileprovision"
            ),
        )
        # An ad-hoc signature carries capabilities into SideStore's signing step.
        if entitlements is not None:
            subprocess.run(
                [
                    "codesign",
                    "--force",
                    "--sign",
                    "-",
                    "--generate-entitlement-der",
                    "--entitlements",
                    str(entitlements),
                    str(root / "Payload" / app.name),
                ],
                check=True,
            )
        with zipfile.ZipFile(
            root / ipa.name, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            for path in sorted((root / "Payload").rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(root))
        shutil.copyfile(root / ipa.name, ipa)
    return ipa


def write_source(
    ipa: pathlib.Path,
    icon: pathlib.Path,
    base_url: str,
    entitlements: pathlib.Path | None = None,
) -> pathlib.Path:
    url = urllib.parse.urlsplit(base_url)
    if (
        url.scheme != "https"
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ValueError(
            "Use an HTTPS download directory without credentials, a query or a fragment."
        )
    base_url = base_url.rstrip("/") + "/"
    with zipfile.ZipFile(ipa) as archive:
        info_path = next(
            name
            for name in archive.namelist()
            if name.startswith("Payload/")
            and name.count("/") == 2
            and name.endswith("/Info.plist")
        )
        info = plistlib.loads(archive.read(info_path))
    output = ipa.parent
    shutil.copyfile(icon, output / "icon.png")
    source = {
        "name": "Training Sensors",
        "identifier": info["CFBundleIdentifier"] + ".source",
        "sourceURL": base_url + "source.json",
        "apps": [
            {
                "name": "Training Sensors",
                "bundleIdentifier": info["CFBundleIdentifier"],
                "developerName": "Training Sensors",
                "localizedDescription": "Live Polar H10 heart rate, ECG and movement. Works offline; server sync is optional. iPhone only.",
                "iconURL": base_url + "icon.png",
                "versions": [
                    {
                        "version": info["CFBundleShortVersionString"],
                        "buildVersion": info["CFBundleVersion"],
                        "date": datetime.datetime.now(datetime.timezone.utc)
                        .date()
                        .isoformat(),
                        "downloadURL": base_url + urllib.parse.quote(ipa.name),
                        "size": ipa.stat().st_size,
                        "minOSVersion": info["MinimumOSVersion"],
                    }
                ],
                "appPermissions": {
                    "entitlements": sorted(plistlib.loads(entitlements.read_bytes()))
                    if entitlements
                    else [],
                    "privacy": {
                        key: value
                        for key, value in info.items()
                        if key.endswith("UsageDescription")
                    },
                },
            }
        ],
        "news": [],
    }
    feed = output / "source.json"
    feed.write_text(json.dumps(source, indent=2) + "\n")
    link = "sidestore://source?url=" + urllib.parse.quote(
        base_url + "source.json", safe=""
    )
    (output / "install-link.txt").write_text(link + "\n")
    page = render_install_page(link, ipa.name, info["MinimumOSVersion"])
    (output / "index.html").write_text(page)
    return feed


def render_install_page(link: str, filename: str, minimum_ios: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>Training Sensors</title>
<style>
  :root {{ color-scheme: light dark; font-family: -apple-system, BlinkMacSystemFont, sans-serif; }}
  body {{ margin: 0; background: #f7f7f7; color: #171717; }}
  main {{ max-width: 340px; margin: 12vh auto; padding: 24px; }}
  img {{ width: 76px; height: 76px; border-radius: 18px; }}
  h1 {{ font-size: 30px; letter-spacing: -1px; margin: 26px 0 12px; }}
  p {{ line-height: 1.6; color: #707070; margin: 0 0 24px; }}
  a {{ color: inherit; text-underline-offset: 4px; }}
  .install {{ display: block; text-align: center; padding: 17px; border-radius: 14px;
    background: #171717; color: white; font-weight: 600; text-decoration: none; margin: 32px 0 20px; }}
  .small {{ font-size: 13px; }}
  @media (prefers-color-scheme: dark) {{
    body {{ background: #090909; color: #f5f5f5; }}
    p {{ color: #999; }}
    .install {{ background: #f5f5f5; color: #090909; }}
  }}
</style>
<main>
  <img src="icon.png" alt="">
  <h1>Training Sensors</h1>
  <p>Live heart rate, ECG and movement from your Polar H10.</p>
  <p class="small">iPhone · iOS {html.escape(minimum_ios)} or later · No account needed</p>
  <a class="install" href="{html.escape(link, quote=True)}">Install with SideStore</a>
  <p class="small">First, <a href="https://docs.sidestore.io/docs/installation/install">set up SideStore</a>.
  Add this source, then install Training Sensors.</p>
  <p class="small">Open the app, allow Bluetooth, wet the strap pads and wear your H10.</p>
  <a class="small" href="{urllib.parse.quote(filename)}" download>Download IPA</a>
</main>
</html>
"""


def main() -> None:
    project = pathlib.Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, default=project / "dist")
    parser.add_argument(
        "--base-url",
        help="Prepare source.json for this HTTPS directory. Files are not published.",
    )
    args = parser.parse_args()
    subprocess.run(["xcodegen", "generate"], cwd=project, check=True)
    derived = project / "DerivedData" / "SideStore"
    subprocess.run(
        [
            "xcodebuild",
            "-project",
            "TrainingSensors.xcodeproj",
            "-scheme",
            "TrainingSensors",
            "-configuration",
            "Release",
            "-destination",
            "generic/platform=iOS",
            "-derivedDataPath",
            str(derived),
            "CODE_SIGNING_ALLOWED=NO",
            "CODE_SIGNING_REQUIRED=NO",
            "build",
        ],
        cwd=project,
        check=True,
    )
    app = derived / "Build" / "Products" / "Release-iphoneos" / "TrainingSensors.app"
    entitlements = project / "TrainingSensors/TrainingSensors.entitlements"
    ipa = package_app(app, args.output, entitlements)
    files = [ipa]
    if args.base_url:
        files.append(
            write_source(
                ipa,
                project
                / "TrainingSensors/Assets.xcassets/AppIcon.appiconset/AppIcon.png",
                args.base_url,
                entitlements,
            )
        )
        files.append(args.output / "icon.png")
        files.append(args.output / "index.html")
    checksums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in files
    )
    (args.output / "SHA256SUMS").write_text(checksums)
    print(f"Prepared {ipa.resolve()} ({ipa.stat().st_size:,} bytes)")
    if args.base_url:
        print(
            "Feed prepared. Install and update links work after these files are hosted."
        )


if __name__ == "__main__":
    main()
