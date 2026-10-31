"""Archive replay and acquisition-only selection; never select for normal morphology."""

from pathlib import Path
import hashlib, json
import numpy as np
import pandas as pd


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_recording(config):
    root = Path(config["archive_dir"])
    start = pd.Timestamp(config["session_start"])
    if start.tz is None:
        raise ValueError("session_start requires a timezone offset")
    offset = pd.Timestamp("2000-01-01T00:00:00Z").value - int(
        start.utcoffset().total_seconds() * 1e9
    )
    result = {}
    hashes = {}
    for kind in ["ecg", "acc", "hr"]:
        path = root / (kind + ".parquet")
        d = pd.read_parquet(path)
        hashes[kind] = digest(path)
        if set(d.sensor_id) != {config["sensor_id"]}:
            raise ValueError("Recording must contain only the configured sensor")
        if kind != "hr":
            if set(d.device_time_epoch) != {"h10_local_since_2000"}:
                raise ValueError("Unsupported device time epoch")
            ns = d.device_time_ns.to_numpy(dtype=np.int64) + offset
        else:
            ns = d.collector_unix_ns.to_numpy(dtype=np.int64)
        d = d.copy()
        d["t_s"] = (ns - start.value) / 1e9
        # Real surrounding data retained for delineation; no gap filling or sorting.
        result[kind] = d[(d.t_s >= -35) & (d.t_s < 275)].copy()
    for k, d in result.items():
        if d.empty or d.t_s.min() > 0.05 or d.t_s.max() < 239.95:
            raise ValueError(k + " does not cover the full four-minute replay")
    result["source"] = {
        "hashes": hashes,
        "session_start": start.isoformat(),
        "sensor_id": config["sensor_id"],
        "wall_clock": "H10 local-since-2000 converted with the configured date offset; HR uses receipt time.",
        "protocol_observed": False,
        "note": "Historical replay. Sitting, music, consent and actual relaxation were not observed.",
    }
    return result


def evaluate_candidate(e, a, h, start, protocol):
    end = start + protocol["strip_duration_s"]
    e = e[(e.t_s >= start) & (e.t_s < end)]
    a = a[(a.t_s >= start) & (a.t_s < end)]
    # Contact samples include nearest preceding status within 2 seconds.
    h = h[(h.t_s >= start - 2) & (h.t_s < end)]
    reasons = []
    motion = None
    maxdev = None
    if (
        len(e) < 1200
        or e.empty
        or e.t_s.iloc[0] - start > 0.02
        or end - e.t_s.iloc[-1] > 0.02
    ):
        reasons.append("ECG coverage")
    if len(e) > 1:
        dt = np.diff(e.device_time_ns.to_numpy(dtype=np.int64)) / 1e9
        if np.any((dt <= 0) | (dt > 0.012)):
            reasons.append("ECG discontinuity")
        if e[["session_id", "connection_id"]].drop_duplicates().shape[0] != 1:
            reasons.append("ECG connection change")
        if not np.isfinite(e.ecg_uv).all() or np.ptp(e.ecg_uv) < 20:
            reasons.append("Missing or flat ECG")
        # Gross clipping is an acquisition problem. Do not reject wide/ectopic beats or ST changes.
        if np.max(np.abs(e.ecg_uv)) >= 32000:
            reasons.append("ECG clipping")
    if (
        len(a) < 1700
        or a.empty
        or a.t_s.iloc[0] - start > 0.03
        or end - a.t_s.iloc[-1] > 0.03
    ):
        reasons.append("ACC coverage")
    elif np.any(
        (np.diff(a.device_time_ns) <= 0) | (np.diff(a.device_time_ns) > 20_000_000)
    ):
        reasons.append("ACC discontinuity")
    elif not np.isfinite(a[["x_mg", "y_mg", "z_mg"]].to_numpy(float)).all():
        reasons.append("Missing ACC values")
    else:
        xyz = a[["x_mg", "y_mg", "z_mg"]].to_numpy(float)
        dev = np.linalg.norm(xyz - np.median(xyz, axis=0), axis=1)
        motion = float(np.sqrt(np.mean(dev**2)))
        maxdev = float(np.max(dev))
        if (
            motion > protocol["motion_rms_max_mg"]
            or maxdev > protocol["motion_peak_max_mg"]
        ):
            reasons.append("Movement above prototype threshold")
        if a[["session_id", "connection_id"]].drop_duplicates().shape[0] != 1:
            reasons.append("ACC connection change")
        if len(e) and tuple(a[["session_id", "connection_id"]].iloc[0]) != tuple(
            e[["session_id", "connection_id"]].iloc[0]
        ):
            reasons.append("ECG/ACC identity mismatch")
    if (
        h.empty
        or not h.sensor_contact_supported.eq(True).fillna(False).all()
        or not h.sensor_contact_detected.eq(True).fillna(False).all()
    ):
        reasons.append("Contact not continuously confirmed")
    elif (
        len(h) < 2
        or h.t_s.iloc[0] > start
        or end - h.t_s.iloc[-1] > 2
        or np.any((np.diff(h.t_s) <= 0) | (np.diff(h.t_s) > 2))
    ):
        reasons.append("Contact status stale")
    elif len(e) and any(
        tuple(row) != tuple(e[["session_id", "connection_id"]].iloc[0])
        for row in h[["session_id", "connection_id"]].to_numpy()
    ):
        reasons.append("Contact identity mismatch")
    return {
        "start_s": start,
        "end_s": end,
        "accepted": not reasons,
        "reasons": reasons,
        "motion_rms_mg": motion,
        "motion_peak_mg": maxdev,
        "ecg_samples": len(e),
        "acc_samples": len(a),
    }


def select_window(recording, protocol):
    if [
        protocol[k]
        for k in [
            "session_duration_s",
            "settling_s",
            "search_end_s",
            "strip_duration_s",
        ]
    ] != [240, 120, 180, 10]:
        raise ValueError("This protocol version requires 240/120/180/10 second timing")
    candidates = [
        evaluate_candidate(
            recording["ecg"], recording["acc"], recording["hr"], start, protocol
        )
        for start in range(120, 171)
    ]
    eligible = [c for c in candidates if c["accepted"]]
    # Rank by chest movement only; no ECG morphology or derived interval enters ranking.
    chosen = (
        min(eligible, key=lambda c: (c["motion_rms_mg"], c["start_s"]))
        if eligible
        else None
    )
    return {
        "protocol": protocol,
        "candidates": candidates,
        "chosen": chosen,
        "selection_rule": "Lowest ACC vector RMS among acquisition-valid 10 s windows fully within [120,180); earliest tie.",
        "status": "selected" if chosen else "repeat_acquisition",
        "limitations": "Prototype motion thresholds; low chest motion does not prove no limb movement, electrical noise or physiological rest.",
    }


def make_context(recording, selection):
    if not selection["chosen"]:
        raise ValueError("No acceptable window; repeat acquisition")
    start = selection["chosen"]["start_s"]
    e = recording["ecg"]
    e = e[(e.t_s >= start - 30) & (e.t_s < start + 40)].copy()
    dt = np.diff(e.device_time_ns.to_numpy(dtype=np.int64)) / 1e9
    if (
        len(e) < 8000
        or np.any((dt <= 0) | (dt > 0.012))
        or e[["session_id", "connection_id"]].drop_duplicates().shape[0] != 1
    ):
        raise ValueError(
            "Selected strip has no continuous 70 s context; no fabricated padding permitted"
        )
    e["relative_s"] = e.t_s - start
    view = e[(e.relative_s >= 0) & (e.relative_s < 10)].copy()
    native = (
        e[(e.relative_s >= -1.2) & (e.relative_s < 11.2)].copy().reset_index(drop=True)
    )
    fs = (len(e) - 1) / (
        (int(e.device_time_ns.iloc[-1]) - int(e.device_time_ns.iloc[0])) / 1e9
    )
    return {"context": e, "view": view, "native": native, "fs": fs, "start_s": start}
