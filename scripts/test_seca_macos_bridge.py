"""Native helper contracts, private state and cancellation; never touch hardware."""
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import seca_macos_bridge as bridge
import seca_upload as upload
from seca_usb_capture import configuration_reply
from test_seca_replay import frame, group
from test_seca_usb_capture import FakeTransport
from test_seca_upload import INSTALLATION, SESSION


class Collector:
    def __init__(self):
        self.rows = []
    def emit(self, event, **fields):
        self.rows.append({"event": event, **fields})
    def close(self):
        pass


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = bridge.state_directory(Path(self.temp.name) / "private")
        self.events = Collector()

    def credentials(self):
        value = {"installation_id": INSTALLATION, "credential_id": SESSION,
                 "device_credential": "synthetic-only-credential", "server_origin": "https://carolina.invalid"}
        bridge.enrollment.write_private_new(self.root / "credential.json", value)
        return value

    def closed(self, name="recording", reason="interrupted", errors=None):
        path = self.root / "recordings" / name
        path.mkdir(mode=0o700)
        rows = [{"type": "event", "event": "configuration_confirmed", "details": {
            "sample_rate_hz": 32000, "gain": 6, "contact_detection": False}},
            {"type": "event", "event": "acquisition_start"},
            {"type": "event", "event": "session_end", "success": False,
             "end_reason": reason, "errors": errors or []}]
        (path / "records.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        (path / "session.json").write_text(json.dumps({"schema_version": 1, "transport": "usb",
            "session_id": SESSION, "requested_sample_rate_hz": 32000, "gain": 6,
            "contact_detection": False, "configuration_confirmed": True, "device_info": {"sn": "synthetic"}}))
        return path

    def test_configuration_fields_are_walked_without_payload_search(self):
        text = b"samplerate=32000\r\ngain=6\r\nopenlead=0"
        a = b"A" + len(text).to_bytes(2, "big") + text
        payload, _ = group()
        self.assertEqual(configuration_reply(frame(b"P\0\0\x64M\0" + payload + a))["gain"], "6")
        self.assertIsNone(configuration_reply(frame(b"Z" + a)))
        self.assertIsNone(configuration_reply(frame(a + b"Z")))
        self.assertIsNone(configuration_reply(frame(b"A\xff\xff" + text)))

    def test_snapshot_discovery_private_identity_and_closed_state(self):
        self.credentials()
        self.closed()
        volumes = self.root / "volumes"
        volume = volumes / "seca"
        volume.mkdir(parents=True)
        (volume / "ecg.txt").write_text("not read by discovery")
        (volume / "ecg.dat").write_text("not read by discovery")
        result = bridge.snapshot(self.root, volumes)
        self.assertEqual(result["installation_id"], INSTALLATION)
        self.assertEqual(result["volumes"], [str(volume)])
        self.assertTrue(result["sessions"][0]["closed"])
        self.assertTrue(result["sessions"][0]["uploadable"])
        self.assertNotIn("synthetic-only-credential", json.dumps(result))
        self.assertEqual(bridge.snapshot(self.root, volumes / "missing")["volumes"], [])

    def test_terminal_missing_torn_wrong_type_and_bounded_large_file(self):
        source = self.root / "recordings" / "broken"
        source.mkdir()
        self.assertIsNone(bridge.terminal(source))
        path = source / "records.jsonl"
        for raw in (b"", b"{}", b"[]\n", b"{}\n", b"bad\n"):
            path.write_bytes(raw)
            self.assertIsNone(bridge.terminal(source))
        end = {"type": "event", "event": "session_end"}
        path.write_bytes(b"x" * 200000 + b"\n" + json.dumps(end).encode() + b"\n")
        self.assertEqual(bridge.terminal(source), end)
        self.assertFalse(bridge.uploadable(source))

    def test_many_sessions_remain_within_native_status_frame_limit(self):
        for index in range(201):
            (self.root / "recordings" / (str(index).zfill(3) + "x" * 240)).mkdir()
        result = bridge.snapshot(self.root, self.root / "no_volumes")
        self.assertGreater(len(result["sessions"]), 50)
        self.assertLess(len(result["sessions"]), 200)
        self.assertLess(len(json.dumps(result).encode()), 101000)

    def test_usb_permission_probe_reads_only_configuration_and_discards_content(self):
        volumes = self.root / "Volumes"
        volume = volumes / "fake"
        volume.mkdir(parents=True)
        (volume / "ecg.txt").write_text("wlanpw=NEVER_EMIT_THIS\r\n")
        # No ecg.dat exists; probing must neither require nor create/read it.
        bridge.request_usb_access(volume, self.events, volume_root=volumes, ismount=lambda _: True)
        self.assertEqual(self.events.rows[-1], {"event": "access_granted", "volume": str(volume.resolve())})
        self.assertNotIn("NEVER_EMIT_THIS", json.dumps(self.events.rows))
        self.assertFalse((volume / "ecg.dat").exists())
        for target, mount in ((volume, False), (self.root, True)):
            with self.assertRaises(bridge.BridgeError):
                bridge.request_usb_access(target, self.events, volume_root=volumes, ismount=lambda _: mount)
        with patch.object(Path, "open", side_effect=PermissionError("secret path")), self.assertRaisesRegex(bridge.BridgeError, "Files and Folders"):
            bridge.request_usb_access(volume, self.events, volume_root=volumes, ismount=lambda _: True)

    def test_session_selection_rejects_traversal_and_symlinks(self):
        good = self.closed()
        self.assertEqual(bridge.selected_session(self.root, "recording"), good)
        (self.root / "recordings" / "alias").symlink_to(good)
        for name in ("", "..", ".", "../recording", "missing", "alias"):
            with self.subTest(name=name), self.assertRaises(bridge.BridgeError):
                bridge.selected_session(self.root, name)
        self.assertEqual(len(bridge.snapshot(self.root, self.root / "missing")["sessions"]), 1)

    def test_live_journal_preserves_data_counts_gaps_and_redacts_config(self):
        path = self.root / "recordings" / "live"
        metadata = {"device_info": {"sn": "synthetic", "network_key": "secret"}, "requested_sample_rate_hz": 4000}
        journal = bridge.LiveJournal(path, metadata, self.events)
        data, _ = group((0,) * 8)
        journal.wire("tx", b"secret", acquisition=True)
        journal.wire("rx", frame(data, 1), acquisition=False)
        journal.wire("rx", frame(data, 1), acquisition=True)
        journal.wire("rx", frame(data, 3), acquisition=True)
        journal.wire("rx", frame(data, 3), acquisition=True)
        journal.wire("rx", b"bad", acquisition=True)
        journal.event("config_observed", settings={"network_key": "secret"})
        journal.event("configuration_confirmed", details={"sample_rate_hz": 4000, "gain": 6, "contact_detection": False})
        journal.event("acquisition_start")
        journal.event("session_end")
        journal.close()
        self.assertEqual((journal.groups, journal.missing, journal.errors, journal.constant), (3, 1, 3, 3))
        self.assertNotIn("secret", json.dumps(self.events.rows))
        rows = [json.loads(line) for line in (path / "records.jsonl").read_text().splitlines()]
        self.assertFalse(rows[0]["acquisition"])
        self.assertTrue(rows[2]["acquisition"])

    def test_record_with_fake_transport_and_intentional_stop(self):
        args = SimpleNamespace(volume="/fake", rate=4000, duration=.002, contact=False)
        from dataclasses import replace
        from seca_usb_capture import run_capture
        def run(config, **kwargs):
            config = replace(config, command_delay_s=0, config_wait_s=.01, idle_sleep_s=.001)
            device = FakeTransport(config)
            return run_capture(config, transport_factory=lambda _: device, **kwargs)
        bridge.record(self.root, args, self.events, runner=run)
        self.assertTrue(self.events.rows[-1]["success"])
        self.assertTrue(self.events.rows[-1]["uploadable"])
        def stop(config, **kwargs):
            kwargs["stop_event"].set()
            return {"success": False, "end_reason": "interrupted", "errors": []}
        bridge.record(self.root, args, self.events, runner=stop)
        self.assertTrue(self.events.rows[-1]["user_stopped"])
        self.assertFalse(self.events.rows[-1]["uploadable"])

    def test_clean_stop_prepare_retains_original_result_and_failure_stays_failure(self):
        source = self.closed()
        progress = []
        manifest = upload.prepare(source, self.root / "outboxes" / "good", INSTALLATION, progress=progress.append)
        self.assertEqual(progress, [1])
        self.assertFalse(manifest["capture_success"])
        self.assertTrue(manifest["capture_user_stopped"])
        batch = upload.read_json(self.root / "outboxes" / "good" / "0000000000.json")
        self.assertEqual(batch["end_reason"], "usb_capture_user_stopped")
        self.assertFalse(bridge.terminal(source)["success"])
        source = self.closed("failed", errors=["standby failed"])
        manifest = upload.prepare(source, self.root / "outboxes" / "failed", INSTALLATION)
        self.assertFalse(manifest["capture_user_stopped"])
        batch = upload.read_json(self.root / "outboxes" / "failed" / "0000000000.json")
        self.assertEqual(batch["end_reason"], "usb_capture_failed_see_local_journal")

    def test_upload_prepare_retry_and_existing_receipt_progress(self):
        self.credentials()
        self.closed()
        def sender(url, body, credential, timeout):
            batch = json.loads(body)
            return {"session_id": batch["session_id"], "batch_index": batch["batch_index"], "sha256": upload.digest(body)}
        def send(*args, **kwargs):
            return upload.upload(*args, **kwargs, sender=sender)
        bridge.send_session(self.root, "recording", "https://carolina.invalid", self.events, upload=send)
        self.assertEqual(self.events.rows[-1]["newly_uploaded"], 1)
        bridge.send_session(self.root, "recording", "https://carolina.invalid", self.events, upload=send)
        self.assertEqual(self.events.rows[-1]["newly_uploaded"], 0)
        self.assertEqual(self.events.rows[-2]["acknowledged"], 1)
        with self.assertRaisesRegex(bridge.BridgeError, "different"):
            bridge.send_session(self.root, "recording", "https://other.invalid", self.events)
        (self.root / "recordings" / "incomplete").mkdir()
        with self.assertRaisesRegex(bridge.BridgeError, "terminal"):
            bridge.send_session(self.root, "incomplete", "https://carolina.invalid", self.events)

    def test_main_state_record_upload_and_enrollment_secret_input(self):
        args = ["--state-dir", str(self.root)]
        with patch.object(bridge, "Events", return_value=self.events):
            self.assertEqual(bridge.main(args + ["state"]), 0)
            with patch.object(bridge, "record") as record:
                self.assertEqual(bridge.main(args + ["record", "--volume", "/fake"]), 0)
                record.assert_called_once()
            with patch.object(bridge, "request_usb_access") as access:
                self.assertEqual(bridge.main(args + ["access", "--volume", "/fake"]), 0)
                access.assert_called_once()
            with patch.object(bridge, "send_session") as send:
                self.assertEqual(bridge.main(args + ["upload", "--session", "sample"]), 0)
                send.assert_called_once()
            token = "https://carolina.invalid/enroll#PRIVATE"
            def enroll(*a, **kw):
                self.assertEqual(kw["reader"]("ignored"), token)
                self.assertTrue(kw["replace_link"])
                return Path("credential"), INSTALLATION
            with patch.object(bridge.enrollment, "enroll", side_effect=enroll), patch.object(bridge.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(json.dumps({"link": token}).encode() + b"\n"))):
                self.assertEqual(bridge.main(args + ["enroll", "--replace-link"]), 0)
            self.assertEqual(self.events.rows[-1]["installation_id"], INSTALLATION)
            self.assertNotIn("PRIVATE", json.dumps(self.events.rows))

    def test_main_redacted_input_and_helper_failures(self):
        args = ["--state-dir", str(self.root), "enroll"]
        for raw in (b"{}\n", b"no newline", b"x" * 16385, b"{secret}\n"):
            with patch.object(bridge, "Events", return_value=self.events), patch.object(bridge.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(raw))):
                self.assertEqual(bridge.main(args), 1)
        self.assertNotIn("secret", json.dumps(self.events.rows))
        with patch.object(bridge, "Events", return_value=self.events), patch.object(bridge, "snapshot", side_effect=bridge.CaptureError("safe capture failure")):
            self.assertEqual(bridge.main(["--state-dir", str(self.root), "state"]), 1)
        self.assertEqual(self.events.rows[-1]["message"], "safe capture failure")
        with patch.object(bridge, "Events", return_value=self.events), patch.object(self.events, "close", side_effect=bridge.BridgeError("failed")):
            self.assertEqual(bridge.main(["--state-dir", str(self.root), "state"]), 1)

    def test_async_events_coalesce_and_drain_terminal(self):
        stream = io.StringIO()
        events = bridge.Events(stream)
        events.emit("state", marker=1)
        for n in range(100):
            events.emit("progress", count=n)
        events.emit("saved")
        events.close()
        rows = [json.loads(line) for line in stream.getvalue().splitlines()]
        self.assertEqual(rows[0]["event"], "state")
        self.assertIn("saved", [r["event"] for r in rows])
        self.assertTrue(all(r["schema_version"] == 1 for r in rows))

    def test_output_failure_and_overflow_are_loud(self):
        class Broken:
            def write(self, _):
                raise OSError("broken")
        events = bridge.Events(Broken())
        events.emit("saved")
        events.thread.join(1)
        with self.assertRaises(bridge.BridgeError):
            events.emit("state")
        with self.assertRaises(bridge.BridgeError):
            events.close()
        # Prevent writer startup to deterministically exercise the bounded queue.
        with patch.object(threading.Thread, "start"), patch.object(threading.Thread, "join"), patch.object(threading.Thread, "is_alive", return_value=False):
            events = bridge.Events(io.StringIO())
            for _ in range(32):
                events.emit("state")
            with self.assertRaisesRegex(bridge.BridgeError, "full"):
                events.emit("state")
            events.close()


if __name__ == "__main__":
    unittest.main()
