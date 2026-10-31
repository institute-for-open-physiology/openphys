#!/usr/bin/env python3
"""Local IGV-like H10 sleep data explorer for RR/ECG artifact investigation.

Run:
  python3 scripts/h10_sleep_explorer.py --port 8766

Open:
  http://127.0.0.1:8766/

This is an ignored analysis prototype. It serves a track-based browser viewer with
whole-night/detail/focus linked ranges, ECG peak detection, ECG-derived RR, and
approximate H10 HR-service RR overlays.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

TZ = ZoneInfo("Europe/London")
WINDOW_START = datetime(2026, 5, 31, 0, 0, tzinfo=TZ)
WINDOW_END = datetime(2026, 5, 31, 7, 0, tzinfo=TZ)
WINDOW_SECONDS = int((WINDOW_END - WINDOW_START).total_seconds())
DEVICE_ORIGIN_NAIVE = datetime(2000, 1, 1)
DEFAULT_ARCHIVE_ROOT = Path("data/archive/parquet/participant_id=local")
DEFAULT_ANALYSIS_DIR = Path("data/analysis/sleep_20260530_20260531")

POSTURE_COLOR = {
    "prone": "rgba(126,87,194,0.12)",
    "left_side": "rgba(44,160,44,0.12)",
    "right_side": "rgba(31,119,180,0.12)",
    "supine": "rgba(214,39,40,0.16)",
    "unknown/upright": "rgba(140,140,140,0.12)",
    "missing": "rgba(17,17,17,0.10)",
}

EXAMPLES = [
    {"label": "Noisy/post-reconnect artifact", "center_s": 13 * 60 + 55, "detail_s": 10 * 60, "focus_s": 10, "kind": "noisy"},
    {"label": "Quiet prone comparator", "center_s": 30 * 60, "detail_s": 10 * 60, "focus_s": 10, "kind": "quiet"},
    {"label": "High SDNN 04:25", "center_s": 4 * 3600 + 25 * 60 + 45, "detail_s": 15 * 60, "focus_s": 10, "kind": "noisy"},
    {"label": "Quiet nearby 04:20", "center_s": 4 * 3600 + 20 * 60, "detail_s": 15 * 60, "focus_s": 10, "kind": "quiet"},
    {"label": "High RMSSD 05:32", "center_s": 5 * 3600 + 32 * 60 + 48, "detail_s": 15 * 60, "focus_s": 10, "kind": "noisy"},
    {"label": "Quiet right-side 02:07", "center_s": 2 * 3600 + 7 * 60 + 30, "detail_s": 15 * 60, "focus_s": 10, "kind": "quiet"},
    {"label": "High RMSSD 05:50", "center_s": 5 * 3600 + 50 * 60 + 25, "detail_s": 15 * 60, "focus_s": 10, "kind": "noisy"},
    {"label": "Quiet left-side 06:25", "center_s": 6 * 3600 + 25 * 60, "detail_s": 15 * 60, "focus_s": 10, "kind": "quiet"},
]


def unix_ns(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return int(dt.timestamp() * 1_000_000_000)


def device_ns(local_naive: datetime) -> int:
    return int((local_naive - DEVICE_ORIGIN_NAIVE).total_seconds() * 1_000_000_000)


def sec_from_unix_ns(ns: np.ndarray | pd.Series) -> np.ndarray:
    return (np.asarray(ns, dtype=np.float64) - float(unix_ns(WINDOW_START))) / 1e9


def sec_from_device_ns(ns: np.ndarray | pd.Series) -> np.ndarray:
    return (np.asarray(ns, dtype=np.float64) - float(device_ns(WINDOW_START.replace(tzinfo=None)))) / 1e9


def read_parquet_stream(root: Path, stream: str, columns: list[str]) -> pd.DataFrame:
    paths = sorted((root / f"stream={stream}").rglob("*.parquet"))
    frames = []
    for path in paths:
        frames.append(pq.read_table(path, columns=columns).to_pandas())
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.lower().isin(["true", "1"])


def finite_or_none(x):
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def minmax_bin(t: np.ndarray, y: np.ndarray, start: float, end: float, max_points: int) -> dict:
    mask = (t >= start) & (t <= end)
    tt = t[mask]
    yy = y[mask]
    if len(tt) == 0:
        return {"mode": "raw", "t": [], "y": []}
    if len(tt) <= max_points:
        return {"mode": "raw", "t": tt.astype(float).round(4).tolist(), "y": yy.astype(float).tolist()}
    bins = max(50, max_points // 2)
    edges = np.linspace(start, end, bins + 1)
    idx = np.searchsorted(edges, tt, side="right") - 1
    idx = np.clip(idx, 0, bins - 1)
    x_out = []
    lo = []
    hi = []
    for b in range(bins):
        vals = yy[idx == b]
        if vals.size == 0:
            continue
        x_out.append((edges[b] + edges[b + 1]) / 2)
        lo.append(float(np.min(vals)))
        hi.append(float(np.max(vals)))
    return {"mode": "minmax", "t": np.round(x_out, 4).tolist(), "lo": lo, "hi": hi}


def sample_series(t: np.ndarray, y: np.ndarray, start: float, end: float, max_points: int) -> dict:
    mask = (t >= start) & (t <= end)
    tt = t[mask]
    yy = y[mask]
    if len(tt) > max_points:
        take = np.linspace(0, len(tt) - 1, max_points).astype(int)
        tt = tt[take]
        yy = yy[take]
    return {"t": tt.astype(float).round(4).tolist(), "y": yy.astype(float).tolist()}


@dataclass
class ExplorerData:
    archive_root: Path
    analysis_dir: Path
    ecg_t: np.ndarray
    ecg_y: np.ndarray
    acc_t: np.ndarray
    acc_x: np.ndarray
    acc_y: np.ndarray
    acc_z: np.ndarray
    rr_t: np.ndarray
    rr_ms: np.ndarray
    rr_valid_nn: np.ndarray
    rr_valid_range: np.ndarray
    rr_posture: list[str]
    hrv_windows: pd.DataFrame
    peaks_t: np.ndarray
    peaks_y: np.ndarray
    peaks_score: np.ndarray
    ecg_rr_ms: np.ndarray
    posture_bins: list[dict]
    summary: dict

    @classmethod
    def load(cls, archive_root: Path, analysis_dir: Path) -> "ExplorerData":
        t0 = time.time()
        if not archive_root.exists():
            raise RuntimeError(f"Archive root not found: {archive_root}\nExpected the temporary WAL-derived archive from the sleep analysis.")
        print(f"Loading data from {archive_root} ...", file=sys.stderr)
        summary_path = analysis_dir / "summary.json"
        summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}

        rr_path = analysis_dir / "rr_with_posture.csv"
        if not rr_path.exists():
            raise RuntimeError(f"Missing RR analysis CSV: {rr_path}")
        rr = pd.read_csv(rr_path)
        rr["valid_nn"] = bool_series(rr["valid_nn"])
        rr["valid_range"] = bool_series(rr["valid_range"])
        rr = rr[(rr["rr_end_unix_ns"] >= unix_ns(WINDOW_START)) & (rr["rr_end_unix_ns"] <= unix_ns(WINDOW_END))].copy()
        rr["t_s"] = sec_from_unix_ns(rr["rr_end_unix_ns"])
        rr["five_min_bin"] = ((rr["rr_end_unix_ns"] - unix_ns(WINDOW_START)) // (300 * 1_000_000_000)).astype(int)
        hrv_windows = compute_hrv_windows(rr)

        bins_path = analysis_dir / "posture_10s_bins.csv"
        bins = pd.read_csv(bins_path) if bins_path.exists() else pd.DataFrame()
        posture_bins = []
        if not bins.empty:
            for row in bins.itertuples(index=False):
                start = int(row.bin) * 10
                posture_bins.append({"start_s": start, "end_s": start + 10, "posture": str(row.posture)})

        ecg = read_parquet_stream(archive_root, "ecg", ["device_time_ns", "ecg_uv"])
        ecg = ecg[(ecg["device_time_ns"] >= device_ns(WINDOW_START.replace(tzinfo=None))) & (ecg["device_time_ns"] <= device_ns(WINDOW_END.replace(tzinfo=None)))].sort_values("device_time_ns")
        ecg_t = sec_from_device_ns(ecg["device_time_ns"])
        ecg_y = ecg["ecg_uv"].to_numpy(np.float32)
        print(f"Loaded ECG samples: {len(ecg_y):,}", file=sys.stderr)

        acc = read_parquet_stream(archive_root, "acc", ["device_time_ns", "x_mg", "y_mg", "z_mg"])
        acc = acc[(acc["device_time_ns"] >= device_ns(WINDOW_START.replace(tzinfo=None))) & (acc["device_time_ns"] <= device_ns(WINDOW_END.replace(tzinfo=None)))].sort_values("device_time_ns")
        acc_t = sec_from_device_ns(acc["device_time_ns"])
        acc_x = -acc["x_mg"].to_numpy(np.float32)  # dashboard convention
        acc_y = acc["y_mg"].to_numpy(np.float32)
        acc_z = acc["z_mg"].to_numpy(np.float32)
        print(f"Loaded ACC samples: {len(acc_t):,}", file=sys.stderr)

        peaks_t, peaks_y, peaks_score = detect_ecg_nadirs(ecg_t, ecg_y)
        ecg_rr_ms = np.empty_like(peaks_t, dtype=np.float32)
        ecg_rr_ms[:] = np.nan
        if len(peaks_t) > 1:
            ecg_rr_ms[1:] = np.diff(peaks_t) * 1000.0
        print(f"Detected ECG nadirs: {len(peaks_t):,}; median ECG RR={np.nanmedian(ecg_rr_ms):.1f} ms", file=sys.stderr)
        print(f"Load/precompute complete in {time.time() - t0:.1f}s", file=sys.stderr)

        return cls(
            archive_root=archive_root,
            analysis_dir=analysis_dir,
            ecg_t=ecg_t,
            ecg_y=ecg_y,
            acc_t=acc_t,
            acc_x=acc_x,
            acc_y=acc_y,
            acc_z=acc_z,
            rr_t=rr["t_s"].to_numpy(np.float64),
            rr_ms=rr["rr_ms"].to_numpy(np.float32),
            rr_valid_nn=rr["valid_nn"].to_numpy(bool),
            rr_valid_range=rr["valid_range"].to_numpy(bool),
            rr_posture=[str(x) for x in rr.get("posture", pd.Series([""] * len(rr))).tolist()],
            hrv_windows=hrv_windows,
            peaks_t=peaks_t,
            peaks_y=peaks_y,
            peaks_score=peaks_score,
            ecg_rr_ms=ecg_rr_ms,
            posture_bins=posture_bins,
            summary=summary,
        )

    def api_window(self, start: float, end: float, max_points: int = 2200, include: set[str] | None = None) -> dict:
        start = max(0.0, float(start))
        end = min(float(WINDOW_SECONDS), float(end))
        if end <= start:
            end = min(float(WINDOW_SECONDS), start + 1.0)
        span = end - start
        max_points = int(max(200, min(max_points, 20000)))
        include = include or {"ecg", "acc", "rr", "peaks", "hrv", "posture"}

        if "ecg" in include:
            ecg = minmax_bin(self.ecg_t, self.ecg_y, start, end, max_points)
            # For focus windows include enough raw data for exact peak overlay even if max_points was low.
            if span <= 180:
                ecg = minmax_bin(self.ecg_t, self.ecg_y, start, end, 25000)
        else:
            ecg = {"mode": "raw", "t": [], "y": []}

        if "acc" in include:
            acc = {
                "x": sample_series(self.acc_t, self.acc_x, start, end, max_points),
                "y": sample_series(self.acc_t, self.acc_y, start, end, max_points),
                "z": sample_series(self.acc_t, self.acc_z, start, end, max_points),
            }
        else:
            acc = {"x": {"t": [], "y": []}, "y": {"t": [], "y": []}, "z": {"t": [], "y": []}}

        if "rr" in include:
            rr_mask = (self.rr_t >= start) & (self.rr_t <= end)
            rr_idx = np.flatnonzero(rr_mask)
            rr_limit = max_points if span > 3600 else 8000
            if len(rr_idx) > rr_limit:
                rr_idx = rr_idx[np.linspace(0, len(rr_idx) - 1, rr_limit).astype(int)]
        else:
            rr_idx = np.array([], dtype=int)

        if "peaks" in include:
            peaks_mask = (self.peaks_t >= start) & (self.peaks_t <= end)
            peaks_idx = np.flatnonzero(peaks_mask)
            peak_limit = max_points if span > 3600 else 5000
            if len(peaks_idx) > peak_limit:
                peaks_idx = peaks_idx[np.linspace(0, len(peaks_idx) - 1, peak_limit).astype(int)]
        else:
            peaks_idx = np.array([], dtype=int)

        if "hrv" in include:
            hrv = self.hrv_windows[(self.hrv_windows["center_s"] >= start - 300) & (self.hrv_windows["center_s"] <= end + 300)]
        else:
            hrv = pd.DataFrame()
        posture = [p for p in self.posture_bins if p["end_s"] >= start and p["start_s"] <= end] if "posture" in include else []

        return {
            "start_s": start,
            "end_s": end,
            "span_s": span,
            "include": sorted(include),
            "ecg": ecg,
            "acc": acc,
            "rr": {
                "t": np.round(self.rr_t[rr_idx], 4).tolist(),
                "rr_ms": self.rr_ms[rr_idx].astype(float).tolist(),
                "valid_nn": self.rr_valid_nn[rr_idx].astype(bool).tolist(),
                "valid_range": self.rr_valid_range[rr_idx].astype(bool).tolist(),
            },
            "peaks": {
                "t": np.round(self.peaks_t[peaks_idx], 4).tolist(),
                "ecg_uv": self.peaks_y[peaks_idx].astype(float).tolist(),
                "score": self.peaks_score[peaks_idx].astype(float).tolist(),
                "ecg_rr_ms": [finite_or_none(x) for x in self.ecg_rr_ms[peaks_idx]],
            },
            "hrv": {
                "center_s": hrv["center_s"].astype(float).round(4).tolist() if not hrv.empty else [],
                "rmssd_ms": hrv["rmssd_ms"].astype(float).round(4).tolist() if not hrv.empty else [],
                "sdnn_ms": hrv["sdnn_ms"].astype(float).round(4).tolist() if not hrv.empty else [],
                "posture": hrv["posture"].astype(str).tolist() if not hrv.empty else [],
            },
            "posture": posture,
        }


def compute_hrv_windows(rr: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for b, group in rr.groupby("five_min_bin"):
        if b < 0 or b >= WINDOW_SECONDS // 300:
            continue
        valid = group[group["valid_nn"]]
        if len(valid) < 30:
            continue
        values = valid["rr_ms"].to_numpy(float)
        diffs = np.diff(values)
        counts = group.get("posture", pd.Series([""] * len(group))).value_counts(dropna=False)
        posture = str(counts.index[0]) if len(counts) else ""
        rows.append(
            {
                "bin": int(b),
                "start_s": float(b * 300),
                "center_s": float(b * 300 + 150),
                "n": int(len(values)),
                "rmssd_ms": float(np.sqrt(np.mean(diffs * diffs))) if len(diffs) else float("nan"),
                "sdnn_ms": float(np.std(values, ddof=1)) if len(values) > 1 else float("nan"),
                "mean_hr_bpm": float(60000.0 / np.mean(values)),
                "rr_min_ms": float(np.min(values)),
                "rr_max_ms": float(np.max(values)),
                "posture": posture,
                "invalid_count": int((~group["valid_nn"]).sum()),
            }
        )
    return pd.DataFrame(rows)


def detect_ecg_nadirs(t: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Detect dominant negative ECG nadirs with a simple adaptive threshold.

    This is a diagnostic detector, not a clinical ECG algorithm. It intentionally
    marks the raw ECG event used to compute ECG-derived RR so false positives are
    visible in the explorer.
    """
    if len(t) < 10:
        return np.array([]), np.array([]), np.array([])
    y64 = y.astype(np.float64)
    # Rolling mean baseline proxy; 0.8 s is long relative to QRS and short enough
    # to follow large sleep/contact baseline wander.
    sample_rate = 1.0 / np.nanmedian(np.diff(t[: min(len(t), 5000)]))
    win = int(max(51, min(201, round(sample_rate * 0.8))))
    if win % 2 == 0:
        win += 1
    baseline = pd.Series(y64).rolling(win, center=True, min_periods=1).mean().to_numpy()
    qrs = y64 - baseline
    # User's H10 wearing orientation usually has useful deep negative nadirs.
    score = -qrs
    med = float(np.nanmedian(score))
    mad = float(np.nanmedian(np.abs(score - med)))
    p90 = float(np.nanpercentile(score, 90))
    # Robust global threshold; low enough for low-amplitude clean periods, high
    # enough to avoid small baseline noise.
    threshold = max(med + 4.0 * 1.4826 * mad, p90, 120.0)
    local = (score[1:-1] > score[:-2]) & (score[1:-1] >= score[2:]) & (score[1:-1] >= threshold)
    cand = np.flatnonzero(local) + 1
    if cand.size == 0:
        return np.array([]), np.array([]), np.array([])
    refractory = max(35, int(round(sample_rate * 0.30)))
    chosen = []
    i = 0
    while i < len(cand):
        j = i + 1
        best = cand[i]
        while j < len(cand) and cand[j] - cand[i] <= refractory:
            if score[cand[j]] > score[best]:
                best = cand[j]
            j += 1
        if chosen and best - chosen[-1] < refractory:
            if score[best] > score[chosen[-1]]:
                chosen[-1] = best
        else:
            chosen.append(best)
        i = j
    chosen = np.asarray(chosen, dtype=int)
    return t[chosen].astype(np.float64), y[chosen].astype(np.float32), score[chosen].astype(np.float32)


INDEX_HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>H10 IGV-like sleep data explorer</title>
<style>
:root { --bg:#fafafa; --fg:#151515; --muted:#60656b; --border:#d8dee4; --card:#fff; }
body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:var(--bg); color:var(--fg); }
header { padding:14px 18px; border-bottom:1px solid var(--border); background:white; position:sticky; top:0; z-index:5; }
h1 { font-size:20px; margin:0 0 8px; }
.controls { display:flex; gap:8px; flex-wrap:wrap; align-items:center; font-size:13px; }
.viewport-controls { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:8px; font-size:13px; }
button { border:1px solid var(--border); background:#fff; border-radius:7px; padding:4px 7px; cursor:pointer; }
button:hover { background:#eef6ff; }
button.small { padding:2px 6px; font-size:12px; }
input { width:70px; }
main { padding:8px 14px 28px; }
.track { background:white; border:1px solid var(--border); border-radius:8px; margin:7px 0; overflow:hidden; }
.track-title { display:flex; justify-content:space-between; align-items:center; gap:8px; padding:4px 8px; font-size:12px; background:#f6f8fa; border-bottom:1px solid var(--border); }
.track-title .hint { color:var(--muted); font-size:11px; flex:1; }
.track-buttons { white-space:nowrap; }
canvas { display:block; width:100%; cursor:crosshair; }
.status { color:var(--muted); font-size:12px; margin-top:6px; }
.legend { display:inline-flex; gap:12px; flex-wrap:wrap; color:var(--muted); }
.badge { display:inline-block; padding:2px 6px; border-radius:4px; border:1px solid var(--border); background:#fff; }
</style>
</head>
<body>
<header>
<h1>H10 sleep data explorer — linked tracks, ECG peak RR overlay</h1>
<div class="controls">
  <span class="badge">Normal trackpad scroll = page scroll</span>
  <span class="badge">Drag overview → set detail</span>
  <span class="badge">Drag detail/focus track → set focus</span>
  <label>detail s <input id="detailDur" type="number" value="900" min="30" max="7200"></label>
  <label>focus s <input id="focusDur" type="number" value="60" min="5" max="600"></label>
  <button onclick="resetViews()">Reset</button>
  <span id="examples"></span>
</div>
<div class="viewport-controls">
  <strong>Detail</strong>
  <button onclick="panView('detail',-0.8)">←</button><button onclick="panView('detail',0.8)">→</button>
  <button onclick="zoomView('detail',0.5)">Zoom in</button><button onclick="zoomView('detail',2)">Zoom out</button>
  <button onclick="setViewDuration('detail')">Set detail seconds</button>
  <strong>Focus</strong>
  <button onclick="panView('focus',-0.8)">←</button><button onclick="panView('focus',0.8)">→</button>
  <button onclick="zoomView('focus',0.5)">Zoom in</button><button onclick="zoomView('focus',2)">Zoom out</button>
  <button onclick="setViewDuration('focus')">Set focus seconds</button>
</div>
<div class="status" id="status">Loading…</div>
</header>
<main id="tracks"></main>
<script>
const EXAMPLES = __EXAMPLES__;
const POSTURE_COLORS = __POSTURE_COLORS__;
const WINDOW_SECONDS = __WINDOW_SECONDS__;
let state = {
  overview: {start: 0, end: WINDOW_SECONDS},
  detail: {start: 5*3600+25*60, end: 5*3600+55*60},
  focus: {start: 5*3600+32*60+18, end: 5*3600+33*60+18},
};
const tracks = [
  {id:'overview_rr', title:'Whole night RR + 5-min HRV overview', view:'overview', type:'rrhrv', height:105, hint:'Red=detail, orange=focus. Drag to set detail.'},
  {id:'detail_rr', title:'Detail RR + HRV', view:'detail', type:'rrhrv', height:100, hint:'Orange=focus. Drag to set focus.'},
  {id:'detail_ecg', title:'Detail ECG envelope + detected ECG nadirs', view:'detail', type:'ecg', height:105, hint:'Envelope when zoomed out; red dots=ECG nadirs.'},
  {id:'focus_ecg', title:'Focus ECG/envelope with ECG-derived RR and H10 RR overlay', view:'focus', type:'ecg_focus', height:155, hint:'Blue labels=ECG RR. Orange labels=approx H10 RR.'},
  {id:'focus_rr_compare', title:'Focus RR comparison: ECG-derived vs H10 HR-service RR', view:'focus', type:'rr_compare', height:95, hint:'Green=ECG-derived, orange=H10 approximate.'},
  {id:'focus_acc', title:'Focus accelerometer', view:'focus', type:'acc', height:85, hint:'Display X=-rawX, plus raw Y/Z.'},
];
let cache = new Map();
let canvases = new Map();
let drag = null;
function timeLabel(s) {
  s = Math.max(0, Math.min(WINDOW_SECONDS, s));
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = Math.floor(s%60);
  return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
}
function rangeLabel(r) { return `${timeLabel(r.start)}–${timeLabel(r.end)} (${Math.round(r.end-r.start)}s)`; }
function setStatus(msg) {
  document.getElementById('status').innerHTML = `${msg}<br>overview ${rangeLabel(state.overview)} · detail ${rangeLabel(state.detail)} · focus ${rangeLabel(state.focus)}`;
}
function resetViews() {
  state.detail = {start: 5*3600+25*60, end: 5*3600+55*60};
  state.focus = {start: 5*3600+32*60+18, end: 5*3600+33*60+18};
  renderAll();
}
function setExample(ex) {
  const detail = Number(document.getElementById('detailDur').value || ex.detail_s || 900);
  const focus = Number(document.getElementById('focusDur').value || ex.focus_s || 60);
  state.detail = clampRange(ex.center_s - detail/2, ex.center_s + detail/2);
  state.focus = clampRange(ex.center_s - focus/2, ex.center_s + focus/2);
  renderAll();
}
function zoomView(view, factor) {
  const r = state[view];
  const center = (r.start + r.end) / 2;
  const minSpan = view === 'focus' ? 2 : 10;
  const maxSpan = view === 'focus' ? 3600 : WINDOW_SECONDS;
  const span = Math.max(minSpan, Math.min(maxSpan, (r.end - r.start) * factor));
  state[view] = clampRange(center - span / 2, center + span / 2);
  renderAll();
}
function panView(view, fraction) {
  const r = state[view];
  const span = r.end - r.start;
  const delta = span * fraction;
  state[view] = clampRange(r.start + delta, r.end + delta);
  renderAll();
}
function setViewDuration(view) {
  const input = view === 'focus' ? document.getElementById('focusDur') : document.getElementById('detailDur');
  const duration = Math.max(view === 'focus' ? 2 : 10, Number(input.value || (view === 'focus' ? 60 : 900)));
  const r = state[view];
  const center = (r.start + r.end) / 2;
  state[view] = clampRange(center - duration / 2, center + duration / 2);
  renderAll();
}
function trackControlHtml(view) {
  if (view === 'overview') return '';
  return `<span class="track-buttons"><button class="small" onclick="panView('${view}',-0.8)">←</button><button class="small" onclick="panView('${view}',0.8)">→</button><button class="small" onclick="zoomView('${view}',0.5)">+</button><button class="small" onclick="zoomView('${view}',2)">−</button></span>`;
}
function clampRange(start,end) {
  let span = Math.max(1, end-start);
  start = Math.max(0, start); end = start + span;
  if (end > WINDOW_SECONDS) { end = WINDOW_SECONDS; start = Math.max(0, end-span); }
  return {start,end};
}
function init() {
  const exSpan = document.getElementById('examples');
  for (const ex of EXAMPLES) {
    const b = document.createElement('button');
    b.textContent = (ex.kind === 'noisy' ? '⚠ ' : '✓ ') + ex.label;
    b.onclick = () => setExample(ex);
    exSpan.appendChild(b);
  }
  const root = document.getElementById('tracks');
  for (const tr of tracks) {
    const div = document.createElement('div'); div.className='track';
    div.innerHTML = `<div class="track-title"><strong>${tr.title}</strong><span class="hint">${tr.hint}</span>${trackControlHtml(tr.view)}</div><canvas height="${tr.height}" style="height:${tr.height}px"></canvas>`;
    root.appendChild(div);
    const c = div.querySelector('canvas');
    canvases.set(tr.id, {canvas:c, track:tr});
    c.addEventListener('mousedown', ev => onMouseDown(ev,tr));
    c.addEventListener('mousemove', ev => onMouseMove(ev,tr));
    c.addEventListener('mouseup', ev => onMouseUp(ev,tr));
    c.addEventListener('mouseleave', ev => onMouseUp(ev,tr));
  }
  window.addEventListener('resize', renderAll);
  renderAll();
}
function viewRequestConfig(view) {
  if (view === 'overview') return {include:'rr,hrv,posture', maxPoints:1400};
  if (view === 'detail') return {include:'rr,hrv,posture,ecg,peaks', maxPoints:1600};
  return {include:'rr,hrv,posture,ecg,peaks,acc', maxPoints:2600};
}
async function fetchWindow(view) {
  const r = state[view];
  const cfg = viewRequestConfig(view);
  const key = `${view}:${r.start.toFixed(3)}:${r.end.toFixed(3)}:${cfg.include}:${cfg.maxPoints}`;
  if (cache.has(key)) return cache.get(key);
  const url = `/api/window?start=${r.start}&end=${r.end}&max_points=${cfg.maxPoints}&include=${encodeURIComponent(cfg.include)}`;
  const data = await fetch(url).then(r => r.json());
  cache.set(key, data);
  if (cache.size > 24) cache.delete(cache.keys().next().value);
  return data;
}
async function renderAll() {
  setStatus('Rendering…');
  await Promise.all(['overview','detail','focus'].map(v => fetchWindow(v)));
  for (const {canvas,track} of canvases.values()) await renderTrack(canvas, track);
  setStatus('Ready. ECG peak detector is diagnostic; H10 RR overlay uses approximate HR-notification timing.');
}
function setupCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = Math.round(rect.width*dpr); canvas.height = Math.round(rect.height*dpr);
  const ctx = canvas.getContext('2d'); ctx.setTransform(dpr,0,0,dpr,0,0);
  return {ctx, w:rect.width, h:rect.height};
}
function xScale(r,w,padL=58,padR=46) { return s => padL + (s-r.start)/(r.end-r.start)*(w-padL-padR); }
function yScale(min,max,h,padT=12,padB=24) { return y => padT + (max-y)/(max-min)*(h-padT-padB); }
function drawAxes(ctx,w,h,r,ymin,ymax,rightLabel=null) {
  ctx.strokeStyle='#e5e7eb'; ctx.lineWidth=1; ctx.fillStyle='#555'; ctx.font='11px sans-serif';
  const x = xScale(r,w); const y = yScale(ymin,ymax,h);
  const span = r.end-r.start;
  let step = span<=30 ? 5 : span<=120 ? 10 : span<=900 ? 60 : span<=3600 ? 300 : 1800;
  let first = Math.ceil(r.start/step)*step;
  ctx.textAlign='center'; ctx.textBaseline='top';
  for(let s=first; s<=r.end; s+=step){ const xx=x(s); ctx.beginPath(); ctx.moveTo(xx,8); ctx.lineTo(xx,h-22); ctx.stroke(); ctx.fillText(timeLabel(s),xx,h-18); }
  ctx.textAlign='right'; ctx.textBaseline='middle';
  for(let i=0;i<=4;i++){ const val=ymin+(ymax-ymin)*i/4; const yy=y(val); ctx.beginPath(); ctx.moveTo(54,yy); ctx.lineTo(w-42,yy); ctx.stroke(); ctx.fillText(Math.round(val),52,yy); }
  ctx.strokeStyle='#111'; ctx.strokeRect(58,8,w-104,h-32);
}
function drawPosture(ctx,w,h,r,data) {
  const x=xScale(r,w);
  for(const p of data.posture || []){
    const a=Math.max(p.start_s,r.start), b=Math.min(p.end_s,r.end); if(b<a) continue;
    ctx.fillStyle=POSTURE_COLORS[p.posture] || 'rgba(150,150,150,.08)'; ctx.fillRect(x(a),8,Math.max(1,x(b)-x(a)),h-32);
  }
}
function drawNestedMarkers(ctx,w,h,track) {
  const r = state[track.view]; const x=xScale(r,w);
  const marks=[];
  if(track.view==='overview') { marks.push(['detail','rgba(255,0,0,.16)',state.detail]); marks.push(['focus','rgba(255,165,0,.30)',state.focus]); }
  if(track.view==='detail') { marks.push(['focus','rgba(255,165,0,.30)',state.focus]); }
  for(const [name,color,m] of marks){
    if(m.end<r.start || m.start>r.end) continue;
    const a=Math.max(m.start,r.start), b=Math.min(m.end,r.end);
    ctx.fillStyle=color; ctx.fillRect(x(a),8,Math.max(2,x(b)-x(a)),h-32);
    ctx.strokeStyle=color.replace('.16','.8').replace('.30','.9'); ctx.strokeRect(x(a),8,Math.max(2,x(b)-x(a)),h-32);
  }
}
async function renderTrack(canvas,track) {
  const {ctx,w,h}=setupCanvas(canvas); const r=state[track.view]; const data=await fetchWindow(track.view);
  ctx.clearRect(0,0,w,h); drawPosture(ctx,w,h,r,data); drawNestedMarkers(ctx,w,h,track);
  if(track.type==='rrhrv') drawRRHRV(ctx,w,h,r,data);
  else if(track.type==='ecg') drawECG(ctx,w,h,r,data,false);
  else if(track.type==='ecg_focus') drawECG(ctx,w,h,r,data,true);
  else if(track.type==='rr_compare') drawRRCompare(ctx,w,h,r,data);
  else if(track.type==='acc') drawACC(ctx,w,h,r,data);
  ctx.fillStyle='#333'; ctx.font='11px sans-serif'; ctx.textAlign='left'; ctx.textBaseline='top'; ctx.fillText(rangeLabel(r),62,10);
}
function drawRRHRV(ctx,w,h,r,data) {
  drawAxes(ctx,w,h,r,400,1300);
  const x=xScale(r,w), y=yScale(400,1300,h);
  const rr=data.rr;
  for(let i=0;i<rr.t.length;i++){
    ctx.fillStyle=rr.valid_nn[i] ? 'rgba(40,100,170,.45)' : 'rgba(214,96,77,.9)';
    ctx.beginPath(); ctx.arc(x(rr.t[i]), y(rr.rr_ms[i]), rr.valid_nn[i]?1.6:3.2, 0, 2*Math.PI); ctx.fill();
  }
  const y2=yScale(0,90,h);
  for(const [arr,color] of [[data.hrv.rmssd_ms,'#1B7837'],[data.hrv.sdnn_ms,'#E66101']]){
    ctx.strokeStyle=color; ctx.lineWidth=1.2; ctx.beginPath();
    for(let i=0;i<data.hrv.center_s.length;i++){ const xx=x(data.hrv.center_s[i]), yy=y2(arr[i]); if(i===0)ctx.moveTo(xx,yy); else ctx.lineTo(xx,yy); }
    ctx.stroke();
    ctx.fillStyle=color; for(let i=0;i<data.hrv.center_s.length;i++){ ctx.beginPath(); ctx.arc(x(data.hrv.center_s[i]), y2(arr[i]), 2.5,0,2*Math.PI); ctx.fill(); }
  }
  ctx.fillStyle='#1B7837'; ctx.fillText('RMSSD',w-104,14); ctx.fillStyle='#E66101'; ctx.fillText('SDNN',w-104,29); ctx.fillStyle='#386CB0'; ctx.fillText('RR ms',62,26);
}
function drawECG(ctx,w,h,r,data,focus) {
  const ecg=data.ecg; const x=xScale(r,w);
  let ymin=-2000, ymax=1200;
  if(ecg.mode==='raw' && ecg.y.length){ ymin=quantile(ecg.y,.01); ymax=quantile(ecg.y,.99); const m=(ymax-ymin)*.15||100; ymin-=m; ymax+=m; }
  else if(ecg.mode==='minmax' && ecg.lo.length){ ymin=quantile(ecg.lo,.01); ymax=quantile(ecg.hi,.99); const m=(ymax-ymin)*.15||100; ymin-=m; ymax+=m; }
  drawAxes(ctx,w,h,r,ymin,ymax); const y=yScale(ymin,ymax,h);
  ctx.strokeStyle='#111'; ctx.lineWidth=0.8; ctx.beginPath();
  if(ecg.mode==='raw'){
    for(let i=0;i<ecg.t.length;i++){ const xx=x(ecg.t[i]), yy=y(ecg.y[i]); if(i===0)ctx.moveTo(xx,yy); else ctx.lineTo(xx,yy); }
    ctx.stroke();
  } else {
    for(let i=0;i<ecg.t.length;i++){ const xx=x(ecg.t[i]); ctx.moveTo(xx,y(ecg.lo[i])); ctx.lineTo(xx,y(ecg.hi[i])); } ctx.stroke();
  }
  const peaks=data.peaks;
  ctx.fillStyle='#D62728';
  for(let i=0;i<peaks.t.length;i++){ const xx=x(peaks.t[i]), yy=y(peaks.ecg_uv[i]); ctx.beginPath(); ctx.arc(xx,yy,focus?3.2:2.2,0,2*Math.PI); ctx.fill(); }
  if(focus) drawPeakRRLabels(ctx,w,h,r,data,ymin,ymax);
  const modeText = ecg.mode === 'raw' ? 'raw ECG µV' : 'ECG min/max envelope µV';
  ctx.fillStyle='#111'; ctx.fillText(`${modeText}; red dots=detected ECG nadirs`,62,26);
}
function drawPeakRRLabels(ctx,w,h,r,data,ymin,ymax){
  const x=xScale(r,w), y=yScale(ymin,ymax,h); const peaks=data.peaks; const rr=data.rr;
  ctx.font='10px sans-serif'; ctx.textAlign='center';
  // ECG-derived RR labels between detected nadirs.
  for(let i=1;i<peaks.t.length;i++){
    const rrms=peaks.ecg_rr_ms[i]; if(!rrms || rrms<350 || rrms>2000) continue;
    const mid=(peaks.t[i-1]+peaks.t[i])/2; const yy=Math.min(ymax, Math.max(ymin, (peaks.ecg_uv[i-1]+peaks.ecg_uv[i])/2));
    ctx.strokeStyle='rgba(56,108,176,.18)'; ctx.beginPath(); ctx.moveTo(x(peaks.t[i-1]),y(peaks.ecg_uv[i-1])); ctx.lineTo(x(peaks.t[i]),y(peaks.ecg_uv[i])); ctx.stroke();
    ctx.fillStyle='#1f5a9d'; ctx.fillText(Math.round(rrms), x(mid), Math.max(18,y(yy)-14));
  }
  // H10 HR-service RR approximate overlay. Map each approximate RR endpoint to nearest detected ECG peak if nearby.
  for(let i=0;i<rr.t.length;i++){
    if(rr.rr_ms[i] < 350 || rr.rr_ms[i] > 2200) continue;
    const tx=rr.t[i]; let nearest=-1, best=1e9;
    for(let j=0;j<peaks.t.length;j++){ const d=Math.abs(peaks.t[j]-tx); if(d<best){best=d; nearest=j;} }
    ctx.fillStyle=rr.valid_nn[i] ? '#D95F02' : '#D6604D';
    const xx=x(tx); ctx.beginPath(); ctx.moveTo(xx,12); ctx.lineTo(xx-4,21); ctx.lineTo(xx+4,21); ctx.closePath(); ctx.fill();
    ctx.fillText('H'+Math.round(rr.rr_ms[i]), xx, 24);
    if(nearest>=0 && best<0.35){ ctx.strokeStyle='rgba(217,95,2,.55)'; ctx.beginPath(); ctx.moveTo(xx,34); ctx.lineTo(x(peaks.t[nearest]), y(peaks.ecg_uv[nearest])); ctx.stroke(); }
  }
}
function drawRRCompare(ctx,w,h,r,data){
  drawAxes(ctx,w,h,r,400,1500); const x=xScale(r,w), y=yScale(400,1500,h);
  const peaks=data.peaks, rr=data.rr;
  ctx.strokeStyle='#1B7837'; ctx.lineWidth=1.0; ctx.beginPath(); let started=false;
  for(let i=0;i<peaks.t.length;i++){ const v=peaks.ecg_rr_ms[i]; if(!v || v<300 || v>2200) continue; const xx=x(peaks.t[i]), yy=y(v); if(!started){ctx.moveTo(xx,yy); started=true;} else ctx.lineTo(xx,yy); }
  ctx.stroke(); ctx.fillStyle='#1B7837'; for(let i=0;i<peaks.t.length;i++){ const v=peaks.ecg_rr_ms[i]; if(!v||v<300||v>2200) continue; ctx.beginPath(); ctx.arc(x(peaks.t[i]),y(v),2.4,0,2*Math.PI); ctx.fill(); }
  ctx.fillStyle='#D95F02'; for(let i=0;i<rr.t.length;i++){ if(rr.rr_ms[i]<300||rr.rr_ms[i]>2200) continue; ctx.beginPath(); ctx.arc(x(rr.t[i]),y(rr.rr_ms[i]),rr.valid_nn[i]?2.5:4,0,2*Math.PI); ctx.fill(); }
  ctx.fillStyle='#1B7837'; ctx.fillText('ECG-derived RR',62,26); ctx.fillStyle='#D95F02'; ctx.fillText('H10 RR approximate',180,26);
}
function drawACC(ctx,w,h,r,data){
  drawAxes(ctx,w,h,r,-1100,1100); const x=xScale(r,w), y=yScale(-1100,1100,h);
  const series=[['x','#D95F02','display X=-rawX'],['y','#1B9E77','raw Y'],['z','#7570B3','raw Z']];
  for(const [k,color,label] of series){ const s=data.acc[k]; ctx.strokeStyle=color; ctx.lineWidth=0.8; ctx.beginPath(); for(let i=0;i<s.t.length;i++){ const xx=x(s.t[i]), yy=y(s.y[i]); if(i===0)ctx.moveTo(xx,yy); else ctx.lineTo(xx,yy); } ctx.stroke(); }
  ctx.fillStyle='#D95F02'; ctx.fillText('X',62,26); ctx.fillStyle='#1B9E77'; ctx.fillText('Y',85,26); ctx.fillStyle='#7570B3'; ctx.fillText('Z',108,26);
}
function quantile(arr,q){ if(!arr.length) return 0; const a=Array.from(arr).sort((x,y)=>x-y); const pos=(a.length-1)*q; const lo=Math.floor(pos), hi=Math.ceil(pos); return lo===hi?a[lo]:a[lo]*(hi-pos)+a[hi]*(pos-lo); }
function canvasSec(ev,tr){ const c=ev.currentTarget, rect=c.getBoundingClientRect(); const r=state[tr.view]; const x=ev.clientX-rect.left; const padL=58,padR=46; return r.start + Math.max(0,Math.min(1,(x-padL)/(rect.width-padL-padR)))*(r.end-r.start); }
function onMouseDown(ev,tr){ drag={track:tr, startX:ev.clientX, startS:canvasSec(ev,tr), curS:canvasSec(ev,tr)}; }
function onMouseMove(ev,tr){ if(drag && drag.track.id===tr.id) drag.curS=canvasSec(ev,tr); }
function onMouseUp(ev,tr){ if(!drag || drag.track.id!==tr.id) return; const a=Math.min(drag.startS,drag.curS), b=Math.max(drag.startS,drag.curS); const span=b-a; if(span>1){ if(tr.view==='overview') state.detail=clampRange(a,b); else if(tr.view==='detail') state.focus=clampRange(a,b); else if(tr.view==='focus') state.focus=clampRange(a,b); } else { const dur=tr.view==='overview'?Number(detailDur.value||900):Number(focusDur.value||60); const center=drag.curS; if(tr.view==='overview') state.detail=clampRange(center-dur/2,center+dur/2); else state.focus=clampRange(center-dur/2,center+dur/2); } drag=null; renderAll(); }
init();
</script>
</main>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    data: ExplorerData | None = None

    def log_message(self, fmt: str, *args):
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def send_json(self, obj: dict):
        payload = json.dumps(obj, allow_nan=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def send_html(self, text: str):
        payload = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                html = INDEX_HTML.replace("__EXAMPLES__", json.dumps(EXAMPLES)).replace("__POSTURE_COLORS__", json.dumps(POSTURE_COLOR)).replace("__WINDOW_SECONDS__", str(WINDOW_SECONDS))
                return self.send_html(html)
            if parsed.path == "/api/window":
                if self.data is None:
                    raise RuntimeError("data not loaded")
                qs = parse_qs(parsed.query)
                start = float(qs.get("start", [0])[0])
                end = float(qs.get("end", [WINDOW_SECONDS])[0])
                max_points = int(float(qs.get("max_points", [2200])[0]))
                include_raw = qs.get("include", [""])[0]
                include = {part.strip() for part in include_raw.split(",") if part.strip()} or None
                return self.send_json(self.data.api_window(start, end, max_points, include))
            if parsed.path == "/api/summary":
                if self.data is None:
                    raise RuntimeError("data not loaded")
                return self.send_json({"summary": self.data.summary, "examples": EXAMPLES})
            self.send_response(404)
            self.end_headers()
        except Exception as exc:
            payload = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)


def main():
    parser = argparse.ArgumentParser(description="Serve an IGV-like H10 ECG/RR explorer for the analysed sleep night")
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--analysis-dir", type=Path, default=DEFAULT_ANALYSIS_DIR)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()

    Handler.data = ExplorerData.load(args.archive_root, args.analysis_dir)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving H10 explorer at http://{args.host}:{args.port}/", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping explorer", file=sys.stderr)


if __name__ == "__main__":
    main()
