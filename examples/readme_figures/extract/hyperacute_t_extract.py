"""Extract the de-identified data behind the README hyperacute-T-wave figure (rest vs near-maximum
exercise, Polar H10).

Run privately by the maintainer, against the private H10 archive and analysis code; writes only
data/hyperacute_t/ (relative time, no timestamps, no device or participant identifiers).

    python extract/hyperacute_t_extract.py \
        --twave-dir <private T-wave analysis dir (tt_twave_packet.py)> \
        [--rest-offset-s 0] [--peak-offset-s 2]

The two 4 s display windows are the private analysis' rest and sprint-finish windows, shifted by
the given offsets (the README uses the sprint-finish window 2 s later than the analysis default).
Each window is analysed with a 0.8 s margin either side, exactly as in the private analysis.

What is exported, per panel (rest / exercise):
  ecg_<panel>.csv    raw H10 samples of the analysed window (display window + margins);
                     time_s = seconds from the start of the analysed window (H10 device clock)
  beats_<panel>.csv  per detected beat: detector fiducial and QRS segment (slope-energy
                     segmentation of the kalidas2017/rodrigues2021 consensus) as sample indices
                     into the ecg file, and the preceding R-R interval
  metadata.json      sampling rates, window geometry, context

The public figure script derives R / S / QRS onset / J point / T peak from these; this script
checks that derivation against the private pipeline before writing anything.
"""
import argparse
import datetime as dt
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DEFAULT = os.path.normpath(os.path.join(HERE, "..", "data", "hyperacute_t"))
DISP = 4.0

ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
ap.add_argument("--twave-dir", required=True)
ap.add_argument("--rest-offset-s", type=float, default=0.0)
ap.add_argument("--peak-offset-s", type=float, default=2.0)
ap.add_argument("--out", default=OUT_DEFAULT)
a = ap.parse_args()
out = os.path.abspath(a.out)

sys.path.insert(0, os.path.join(HERE, "..", "figures"))
import _h10_common as C  # noqa: E402  (the public geometry, checked below)
sys.path.insert(0, os.path.abspath(a.twave_dir))
import tt_twave_packet as P  # noqa: E402  (private; changes cwd to the archive root on import)


def shifted(cfg, dt_s):
    start = (dt.datetime.fromisoformat(cfg["start"]) + dt.timedelta(seconds=dt_s)).isoformat()
    return dict(cfg, start=start, disp=DISP)


def public_geometry(raw, t, fs, beats):
    """The derivation figures/hyperacute_t.py performs (incl. the 2-sample Q-wave onset extension)."""
    G = C.Geometry(raw, t, fs)
    outb = []
    for b in beats:
        on0, off0 = b["seg"] if b["seg"] else (b["fi"], b["fi"])
        g = G.beat_from_segment(on0, off0)
        i, steps = g["on"], 0
        while i > 0 and steps < 2 and raw[i - 1] > raw[i]:
            i -= 1; steps += 1
        g["on"], g["dur"] = i, (t[g["roff"]] - t[i]) * 1000.0
        outb.append(g)
    return outb


os.makedirs(out, exist_ok=True)
panels = {}
for name, cfg in (("rest", shifted(P.REST, a.rest_offset_s)), ("exercise", shifted(P.PEAK, a.peak_offset_s))):
    res = P.analyse_strip(cfg)
    fs, raw, t, beats = res["fs"], res["raw"], res["t"], res["acc"]
    for b, g in zip(beats, public_geometry(raw, t, fs, beats)):
        assert (b["Rx"], b["Sx"], b["on"], b["roff"]) == (g["Rx"], g["Sx"], g["on"], g["roff"]), name
        assert abs(b["Rt"] - g["Rt"]) < 1e-9 and abs(b["dur"] - g["dur"]) < 1e-6, name
    with open(os.path.join(out, f"ecg_{name}.csv"), "w") as f:
        f.write("time_s,ecg_uV\n")
        for ti, yi in zip(t, raw):
            f.write(f"{ti:.9f},{yi:g}\n")
    with open(os.path.join(out, f"beats_{name}.csv"), "w") as f:
        f.write("fiducial_sample,qrs_on_sample,qrs_off_sample,rr_prev_ms\n")
        for b in beats:
            on0, off0 = b["seg"] if b["seg"] else ("", "")
            rr = b.get("rr_prev", np.nan)
            rr_s = f"{rr:.6f}" if np.isfinite(rr) else ""
            f.write(f"{b['fi']},{on0},{off0},{rr_s}\n")
    panels[name] = dict(sampling_rate_hz=fs, n_samples=len(t), n_beats=len(beats))
    print(f"{name}: {len(t)} samples, {len(beats)} beats, fs {fs:.4f} Hz; geometry port matches")

meta = {
    "sensor": "Polar H10 chest strap",
    "lead": "single lead (H10 chest-strap electrodes)",
    "units": {"time_s": "seconds from the start of each analysed window (H10 device clock)",
              "ecg_uV": "microvolts, raw (unfiltered), true polarity"},
    "context": {"rest": "rest before a 5 km rowing time trial (~82 bpm)",
                "exercise": "sprint finish of the same 5 km rowing time trial (~175 bpm)"},
    "panels": panels,
    "margin_s": P.E.MARGIN,
    "display_s": DISP,
    "beats_note": "sample indices are 0-based rows of ecg_<panel>.csv; empty qrs_on/off = no segment "
                  "found (the fiducial is used instead); rr_prev_ms = R-R to the previous detected beat.",
}
json.dump(meta, open(os.path.join(out, "metadata.json"), "w"), indent=1, ensure_ascii=False)
print("->", out)
