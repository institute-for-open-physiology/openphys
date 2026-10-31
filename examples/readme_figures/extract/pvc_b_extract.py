"""Extract the de-identified data behind README figure B (three PVCs in recovery from exercise).

Run privately by the maintainer, against the private H10 recording and analysis code; writes only
data/pvc_b/ (relative time, no timestamps, no device or participant identifiers).

    python extract/pvc_b_extract.py \
        --analysis-dir <private ECG analysis dir (cardiology_consensus_qrs.py)> \
        --npz <private saved recording window (.npz: device_time_ns, collector_unix_ns, ecg_uv)>

What is exported:
  ecg.csv        raw H10 samples covering the displayed window plus a margin; time_s = seconds from
                 the start of the analysed recovery recording (H10 device clock), as on the figure axis
  beats.csv      per beat near the window: QRS segment from the slope-energy segmentation of the
                 two-detector consensus (kalidas2017 + rodrigues2021; rodrigues-only beats dropped as
                 noise), as sample indices into ecg.csv, and whether the beat is one of the
                 hand-reviewed PVCs
  metadata.json  sampling rate, displayed window, context
"""
import argparse
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DEFAULT = os.path.normpath(os.path.join(HERE, "..", "data", "pvc_b"))
WINDOW = (358.2, 369.45)     # displayed window, seconds into the analysed recording
MARGIN_S = 3.0
# Hand-reviewed PVCs (premature + full compensatory pause + wide monomorphic QRS): kalidas2017
# fiducial sample indices into the analysed recording.
PVC_FI = [46760, 47138, 47690, 55464, 74707]

ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
ap.add_argument("--analysis-dir", required=True)
ap.add_argument("--npz", required=True)
ap.add_argument("--out", default=OUT_DEFAULT)
a = ap.parse_args()
out, npz = os.path.abspath(a.out), os.path.abspath(a.npz)

sys.path.insert(0, os.path.abspath(a.analysis_dir))
from cardiology_consensus_qrs import analyse  # noqa: E402  (private)

res = analyse(dict(key="pvc_b", mode="prefetch", npz=npz, secs=0.0))
fs, raw, t, beats = res["fs"], res["raw"], res["t"], res["acc"]

# beats considered by the figure: consensus + kalidas-only beats within 1 s of the window, plus
# the hand-reviewed PVCs
t_lo, t_hi = WINDOW
near = lambda fi: t_lo - 1.0 <= fi / fs <= t_hi + 1.0
sel = sorted({(int(b["fi"]), int(b["fi"]) in PVC_FI) for b in beats
              if b["kind"] != "rodrigues_only" and near(int(b["fi"]))}
             | {(fi, True) for fi in PVC_FI if near(fi)})
by_fi = {int(b["fi"]): b for b in beats}

j0 = int(np.searchsorted(t, t_lo - MARGIN_S))
j1 = int(np.searchsorted(t, t_hi + MARGIN_S))
rows = []
for fi, is_pvc in sel:
    b = by_fi.get(fi)
    assert b is not None and b["seg"], f"beat at sample {fi} has no QRS segment"
    on, off = b["seg"]
    assert j0 + 12 <= on and off < j1
    rows.append((on - j0, off - j0, int(is_pvc)))

os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "ecg.csv"), "w") as f:
    f.write("time_s,ecg_uV\n")
    for ti, yi in zip(t[j0:j1], raw[j0:j1]):
        f.write(f"{ti:.9f},{yi:g}\n")
with open(os.path.join(out, "beats.csv"), "w") as f:
    f.write("qrs_on_sample,qrs_off_sample,pvc\n")
    for r in rows:
        f.write("%d,%d,%d\n" % r)

meta = {
    "sensor": "Polar H10 chest strap",
    "lead": "single lead (H10 chest-strap electrodes)",
    "units": {"time_s": "seconds from the start of the analysed recovery recording (H10 device clock)",
              "ecg_uV": "microvolts, raw (unfiltered), true polarity"},
    "sampling_rate_hz": fs,
    "context": "recovery, about 4 minutes after a 5 km rowing time trial; three monomorphic "
               "premature ventricular contractions",
    "display_window_s": list(WINDOW),
    "beats_note": "beats.csv sample indices are 0-based rows of ecg.csv. qrs_on/off = QRS segment from "
                  "the detector consensus + slope-energy segmentation; pvc = 1 for hand-reviewed PVCs.",
}
json.dump(meta, open(os.path.join(out, "metadata.json"), "w"), indent=1, ensure_ascii=False)
print(f"wrote {j1 - j0} samples ({t[j0]:.3f}-{t[j1 - 1]:.3f} s), {len(rows)} beats "
      f"({sum(r[2] for r in rows)} PVC) -> {out}")
