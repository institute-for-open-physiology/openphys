"""Independent tool adapters. Context is real; interpolation adds no bandwidth."""

from pathlib import Path
import os, subprocess, time, warnings, shutil
import pywt
import numpy as np
from scipy.io import savemat, loadmat
import neurokit2 as nk
import wfdb
from .core import write_json, digest

LANDMARKS = [
    "P_on",
    "P_peak",
    "P_off",
    "QRS_on",
    "Q_peak",
    "R_peak",
    "S_peak",
    "QRS_off",
    "T_on",
    "T_peak",
    "T_off",
]
NK_NAMES = [
    "ECG_P_Onsets",
    "ECG_P_Peaks",
    "ECG_P_Offsets",
    "ECG_R_Onsets",
    "ECG_Q_Peaks",
    "ECG_R_Peaks",
    "ECG_S_Peaks",
    "ECG_R_Offsets",
    "ECG_T_Onsets",
    "ECG_T_Peaks",
    "ECG_T_Offsets",
]


def blank(fid):
    return {"fid_s": float(fid), "marks": dict.fromkeys(LANDMARKS), "notes": []}


def finite_time(v, start, one_based=False):
    return (
        float((v - int(one_based)) / 500 + start)
        if np.isfinite(v) and (v > 0 if one_based else v >= 0)
        else None
    )


def wfdb_beats(events):
    """Associate closed wave groups to QRS cycles, without midpoint clipping of T."""
    groups = []
    opened = {}
    for stamp, label in events:
        family = label.split("_")[0]
        if label.endswith("_on"):
            opened[family] = {
                "on": stamp,
                "peaks": [],
                "fid": [],
                "family": family,
            }
        elif label.endswith("_off") and family in opened:
            group = opened.pop(family)
            group["off"] = stamp
            groups.append(group)
        elif label.endswith("_peak") and family in opened:
            opened[family]["peaks"].append(stamp)
        elif label == "fid" and "QRS" in opened:
            opened["QRS"]["fid"].append(stamp)
    fids = [s for s, label in events if label == "fid"]
    beats = [blank(fid) for fid in fids]
    seen = set()
    for group in groups:
        family = group["family"]
        if family == "QRS":
            matches = [
                i for i, fid in enumerate(fids) if group["on"] <= fid <= group["off"]
            ]
            i = matches[0] if len(matches) == 1 else None
        elif family == "P":
            i = next((i for i, fid in enumerate(fids) if fid > group["off"]), None)
        else:
            i = next(
                (i for i in range(len(fids) - 1, -1, -1) if fids[i] < group["on"]),
                None,
            )
        if i is None:
            continue
        b = beats[i]
        b.setdefault("raw_wavegroups", []).append(group)
        duplicate = (i, family) in seen
        seen.add((i, family))
        for suffix in ["on", "off"]:
            key = family + "_" + suffix
            if duplicate:
                b["notes"].append(
                    "Multiple " + key + " wave groups; measurement unavailable"
                )
                b["marks"][key] = None
            else:
                b["marks"][key] = group[suffix]
        if duplicate and family in ["P", "T"]:
            b["marks"][family + "_peak"] = None
        elif family in ["P", "T"]:
            if len(group["peaks"]) == 1:
                b["marks"][family + "_peak"] = group["peaks"][0]
            elif len(group["peaks"]) > 1:
                b["notes"].append(
                    "Multiple "
                    + family
                    + " peaks retained in raw_wavegroups; no single peak chosen"
                )
    return beats


def run_all(context, config, out):
    out = Path(out)
    out.mkdir()
    dep = Path(config["dependency_root"])
    native = context["context"]
    start = float(np.ceil(native.relative_s.iloc[0] * 500) / 500)
    t = np.arange(start, native.relative_s.iloc[-1], 1 / 500)
    signal = np.interp(t, native.relative_s, native.ecg_uv / 1000)
    savemat(out / "context.mat", {"signal": signal[:, None], "fs": 500})
    wfdb.wrsamp(
        "context",
        fs=500,
        units=["mV"],
        sig_name=["H10"],
        p_signal=signal[:, None],
        fmt=["16"],
        adc_gain=[1000],
        baseline=[0],
        write_dir=str(out),
    )
    cleaned = nk.ecg_clean(signal, sampling_rate=500)
    _, peaks = nk.ecg_peaks(cleaned, sampling_rate=500)
    r = peaks["ECG_R_Peaks"]
    compat = out / "octave_compat"
    compat.mkdir()
    for p in Path(__file__).with_name("octave_compat").glob("*.m"):
        shutil.copy2(p, compat / p.name)
    filters = {}
    for name in ["haar", "rbio3.3"]:
        wave = pywt.Wavelet(name)
        key = name.replace(".", "_")
        filters[key + "_lo"] = wave.dec_lo
        filters[key + "_hi"] = wave.dec_hi
    savemat(compat / "wavelet_filters.mat", filters)
    runs = []
    for method in ["dwt", "cwt", "peak", "prominence"]:
        run = {
            "id": "neurokit_" + method,
            "name": "NeuroKit2 " + method.upper(),
            "version": nk.__version__,
            "status": "failed",
            "beats": [],
            "warnings": [],
            "preprocessing": "Linear interpolation to 500 Hz, NeuroKit cleaning; native bandwidth remains ~65 Hz. Positive R detector.",
        }
        begin = time.monotonic()
        try:
            with warnings.catch_warnings(record=True) as caught:
                _, info = nk.ecg_delineate(
                    cleaned, r, sampling_rate=500, method=method, check=False
                )
            run["warnings"] = [str(w.message) for w in caught]
            info["ECG_R_Peaks"] = r
            for i, rp in enumerate(r):
                b = blank(rp / 500 + start)
                for label, key in zip(LANDMARKS, NK_NAMES):
                    vals = list(info.get(key, []))
                    # CWT reports P of the following beat and T of preceding beat between R pairs.
                    if method == "cwt" and len(vals) == len(r) - 1:
                        vals = (
                            ([np.nan] + vals)
                            if key.startswith("ECG_P_")
                            else (vals + [np.nan])
                            if key.startswith("ECG_T_")
                            else vals
                        )
                    b["marks"][label] = (
                        finite_time(vals[i], start) if i < len(vals) else None
                    )
                run["beats"].append(b)
            run["status"] = "ok"
        except Exception as exc:
            run["error"] = f"{type(exc).__name__}: {exc}"
        run["elapsed_s"] = time.monotonic() - begin
        runs.append(run)
    for external in [False, True]:
        rid = "ecgpuwave_" + ("external" if external else "internal")
        ext = "gpuin" if external else "gpu"
        run = {
            "id": rid,
            "name": "ecgpuwave "
            + ("NeuroKit fiducials" if external else "internal detector"),
            "version": "1.3.4",
            "status": "failed",
            "beats": [],
            "warnings": ["QRS fiducial is not necessarily a positive R peak."],
            "preprocessing": "500 Hz interpolated raw polarity, WFDB int16 at 1 µV resolution, internal filtering.",
        }
        begin = time.monotonic()
        try:
            binary = dep / "vendor/ecgpuwave-1.3.4/ecgpuwave"
            run["binary_sha256"] = digest(binary)
            cmd = [str(binary), "-r", "context", "-a", ext, "-s", "0"]
            if external:
                wfdb.wrann(
                    "context", "nkr", r, symbol=["N"] * len(r), write_dir=str(out)
                )
                cmd += ["-i", "nkr"]
            p = subprocess.run(
                cmd,
                cwd=out,
                capture_output=True,
                text=True,
                timeout=90,
                env={**os.environ, "WFDB": "."},
            )
            (out / (rid + ".log")).write_text(p.stdout + p.stderr)
            if p.returncode:
                raise RuntimeError("Executable failed; see log")
            a = wfdb.rdann(str(out / "context"), ext)
            events = []
            for s, symbol, num in zip(a.sample, a.symbol, a.num):
                label = {"p": "P_peak", "t": "T_peak", "N": "fid"}.get(symbol)
                if symbol in ["(", ")"]:
                    label = {0: "P", 1: "QRS", 2: "T"}.get(int(num), "?") + (
                        "_on" if symbol == "(" else "_off"
                    )
                if label:
                    events.append((float(s / 500 + start), label))
            write_json(out / (rid + "_events.json"), events)
            run["beats"] = wfdb_beats(events)
            run["status"] = "ok"
        except Exception as exc:
            run["error"] = f"{type(exc).__name__}: {exc}"
        run["elapsed_s"] = time.monotonic() - begin
        runs.append(run)
    for method in ["wavedet", "ecgdeli", "ecgdeli_positive"]:
        run = {
            "id": method,
            "name": {
                "wavedet": "Wavedet / ECGkit",
                "ecgdeli": "ECGdeli default",
                "ecgdeli_positive": "ECGdeli positive R",
            }[method],
            "status": "failed",
            "beats": [],
            "warnings": [],
            "preprocessing": "500 Hz interpolated raw polarity, toolbox filtering; one real lead; Octave 11.3.",
        }
        if method.startswith("ecgdeli"):
            run["warnings"].append(
                "Experimental Octave SWT/wextend/nanmean compatibility layer; MATLAB parity has not been established. QRS boundaries use fixed round(25 ms × fs) extensions from toolkit Q/S positions (26 ms at 500 Hz), not independently observed physiological onsets/offsets. Q/S may be fallback positions and default R may be the dominant negative deflection; Q/R/S voltage labels follow toolkit conventions."
            )
        begin = time.monotonic()
        try:
            vendor = dep / "vendor" / ("ecg-kit" if method == "wavedet" else "ECGdeli")
            p = subprocess.run(
                ["git", "-C", str(vendor), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            run["version"] = p.stdout.strip() if not p.returncode else "unknown"
            if method.startswith("ecgdeli"):
                run["compatibility_hashes"] = {
                    p.name: digest(p) for p in compat.iterdir() if p.is_file()
                }
            cmd = [
                "octave",
                "--quiet",
                "--no-gui",
                str(Path(__file__).with_name("octave_adapter.m")),
                str(dep),
                str(out / "context.mat"),
                str(out / (method + ".mat")),
                method,
                str(compat),
            ]
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            (out / (method + ".log")).write_text(p.stdout + p.stderr)
            if p.returncode:
                raise RuntimeError("Octave failed; see log")
            d = loadmat(out / (method + ".mat"), simplify_cells=True)["single"]
            if method == "wavedet":
                if isinstance(d, list):
                    d = d[0]
                keys = [
                    "Pon",
                    "P",
                    "Poff",
                    "QRSon",
                    "Q",
                    "R",
                    "S",
                    "QRSoff",
                    "Ton",
                    "T",
                    "Toff",
                ]
                fids = np.atleast_1d(d["qrs"])
                for i, fid in enumerate(fids):
                    b = blank((fid - 1) / 500 + start)
                    for label, k in zip(LANDMARKS, keys):
                        vals = np.atleast_1d(d.get(k, []))
                        b["marks"][label] = (
                            finite_time(vals[i], start, True) if i < len(vals) else None
                        )
                    run["beats"].append(b)
            else:
                d = np.atleast_2d(d)
                for row in d:
                    b = blank((row[5] - 1) / 500 + start)
                    for label, col in zip(
                        LANDMARKS, [0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11]
                    ):
                        b["marks"][label] = finite_time(row[col], start, True)
                    run["beats"].append(b)
                if method == "ecgdeli":
                    run["warnings"].append(
                        "Default R field may denote the dominant negative QRS deflection in this lead."
                    )
            run["status"] = "ok"
        except Exception as exc:
            run["error"] = f"{type(exc).__name__}: {exc}"
        run["elapsed_s"] = time.monotonic() - begin
        runs.append(run)
    for run in runs:
        run["resampling_hz"] = 500
        run["context_start_s"] = start
        run["visible_beats"] = sum(0 <= b["fid_s"] < 10 for b in run["beats"])
        run["coverage"] = {
            k: sum(
                b["marks"][k] is not None for b in run["beats"] if 0 <= b["fid_s"] < 10
            )
            for k in LANDMARKS
        }
    write_json(out / "runs.json", runs)
    return runs
