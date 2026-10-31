"""Extract the de-identified data behind README figure A (single PVC during near-maximum exercise).

Run privately by the maintainer, against the private H10 archive and analysis code; writes only
data/pvc_a/ (relative time, no timestamps, no device or participant identifiers).

    python extract/pvc_a_extract.py \
        --analysis-dir <private ECG analysis dir (report_clean.py, cardiology_consensus_qrs.py)> \
        --marks <hand-review marks JSON> --marks-key <window id in that JSON> [--config-index 0]

What is exported:
  ecg.csv        raw H10 samples of the analysed window (time_s from the window start, device clock)
  metadata.json  sampling rate, displayed window, and the hand-reviewed beat marks for the window
                 (per beat: QRS window(s), R, S, beat type N/V; the ventricular beat carries two
                 windows - the QRS-proper core and the complex up to its return to baseline)
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DEFAULT = os.path.normpath(os.path.join(HERE, "..", "data", "pvc_a"))

ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
ap.add_argument("--analysis-dir", required=True)
ap.add_argument("--marks", required=True)
ap.add_argument("--marks-key", required=True)
ap.add_argument("--config-index", type=int, default=0)
ap.add_argument("--out", default=OUT_DEFAULT)
a = ap.parse_args()
out = os.path.abspath(a.out)
marks_path = os.path.abspath(a.marks)

sys.path.insert(0, os.path.abspath(a.analysis_dir))
import report_clean as rc  # noqa: E402  (private; changes cwd to the archive root on import)

r = rc.build(dict(rc.CONFIG[a.config_index], export=True))
fs, raw, t, i0 = r["fs"], r["raw"], r["t"], r["i0"]
off = float(t[i0])                  # the review tool's window origin, in analysed-window seconds

marks = []
for m in json.load(open(marks_path))[a.marks_key]["marks"]:
    if m["kind"] == "QRS":
        marks.append(dict(kind="QRS", t0=round(m["t0"] + off, 6), t1=round(m["t1"] + off, 6)))
    elif m["kind"] in ("R", "S"):
        marks.append(dict(kind=m["kind"], t=round(m["t"] + off, 6)))
    elif m["kind"] == "type":
        marks.append(dict(kind="type", t=round(m["t"] + off, 6), label=m["label"]))

os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "ecg.csv"), "w") as f:
    f.write("time_s,ecg_uV\n")
    for ti, yi in zip(t, raw):
        f.write(f"{ti:.9f},{yi:g}\n")

meta = {
    "sensor": "Polar H10 chest strap",
    "lead": "single lead (H10 chest-strap electrodes)",
    "units": {"time_s": "seconds from the start of the analysed window (H10 device clock)",
              "ecg_uV": "microvolts, raw (unfiltered), true polarity"},
    "sampling_rate_hz": fs,
    "context": "near-maximum exercise on a rowing ergometer (heart rate ~160 bpm); one premature "
               "ventricular contraction",
    "display_window_s": [round(float(r["t_lo"]), 6), round(float(r["t_hi"]), 6)],
    "hand_reviewed": {
        "marks": marks,
        "marks_note": "times in time_s; each beat = QRS window(s) + R + S + type (N sinus, V ventricular). "
                      "The V beat has two QRS windows: QRS-proper core and complex to baseline return.",
        "pvc_qrs_core_s": [7.859, 8.020],
        "next_sinus_qrs_before_s": 8.43,
    },
}
json.dump(meta, open(os.path.join(out, "metadata.json"), "w"), indent=1, ensure_ascii=False)
print(f"wrote {len(t)} samples ({t[0]:.3f}-{t[-1]:.3f} s), {len(marks)} marks -> {out}")
