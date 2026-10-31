"""README figure: hyperacute T waves during near-maximum exercise (Polar H10).

Two 4 s panels on the same µV and time scale - rest and the sprint finish of a 5 km rowing time
trial - each with: grey QRS band (onset -> J point) and QRS duration beneath, R-R ladder, and the
R (red), S (blue) and T-peak (orange) marks. Reads data/hyperacute_t/.

    python figures/hyperacute_t.py [output.png]   # default: <repo>/docs/images/hyperacute_t_exercise_polar_h10.png
"""
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import _h10_common as C

TITLE = "Hyperacute T waves during near-maximum exercise (Polar H10, single lead)"
PANELS = [("rest", "Baseline: rest"), ("exercise", "Near-maximum exercise: 5 km time-trial finish")]

meta = C.load_meta("hyperacute_t")
MARGIN, DISP = meta["margin_s"], meta["display_s"]


def analyse(name):
    """Raw trace + per-beat geometry (R, S, QRS onset/J point, duration, preceding R-R)."""
    t, raw = C.load_ecg("hyperacute_t", f"ecg_{name}.csv")
    fs = meta["panels"][name]["sampling_rate_hz"]
    G = C.Geometry(raw, t, fs)
    beats = []
    for row in C.load_csv("hyperacute_t", f"beats_{name}.csv"):
        fi = int(row["fiducial_sample"])
        on0, off0 = (int(row["qrs_on_sample"]), int(row["qrs_off_sample"])) if row["qrs_on_sample"] else (fi, fi)
        b = G.beat_from_segment(on0, off0)
        # extend the onset left through the Q downstroke (max 2 samples, ~15 ms), so the band
        # does not clip the Q wave
        i, steps = b["on"], 0
        while i > 0 and steps < 2 and raw[i - 1] > raw[i]:
            i -= 1; steps += 1
        b["on"], b["dur"] = i, (t[b["roff"]] - t[i]) * 1000.0
        b["rr_prev"] = float(row["rr_prev_ms"]) if row["rr_prev_ms"] else np.nan
        beats.append(b)
    return dict(t=t, raw=raw, fs=fs, beats=beats)


def t_peak(res, b):
    """T peak (sample, value): the largest |deflection| from the pre-QRS baseline in the ST-T band
    (60 ms after the S nadir .. 0.85 x the preceding R-R, at most 460 ms)."""
    raw, fs = res["raw"], res["fs"]
    s, on = b["Sx"], b["on"]
    base = float(np.median(raw[max(0, on - int(0.030 * fs)):on + 1])) if on > 2 else 0.0
    rr = b["rr_prev"] if np.isfinite(b["rr_prev"]) else 380.0
    lo = s + int(0.060 * fs)
    hi = min(len(raw) - 1, s + int(min(0.85 * rr / 1000.0, 0.46) * fs))
    if hi - lo < 3:
        return None
    k = int(np.argmax(np.abs(raw[lo:hi] - base)))
    return lo + k, raw[lo + k]


def strip(ax, res, ymin, ymax):
    raw, t, fs = res["raw"], res["t"], res["fs"]
    t_lo, t_hi = MARGIN, MARGIN + DISP
    nodes = [b for b in res["beats"] if t_lo + 0.03 <= t[b["Rx"]] <= t_hi - 0.15]   # whole beats
    i0, i1 = int(np.searchsorted(t, t_lo)), int(np.searchsorted(t, t_hi))
    x = t - t_lo
    span = ymax - ymin                      # shared across panels -> label rows line up
    for b in nodes:
        ax.axvspan(x[b["on"]], x[b["roff"]], color=C.GREY, alpha=0.95, zorder=0)
    ax.plot(x[i0:i1], raw[i0:i1], color="black", lw=1.4, zorder=4)
    y_arrow = ymax + 0.18 * span
    for b in nodes:
        ax.axvline(b["Rt"] - t_lo, color=C.DASH, ls=(0, (4, 3)), lw=0.9, zorder=1)
    for a, b in zip(nodes[:-1], nodes[1:]):
        x1, x2 = a["Rt"] - t_lo, b["Rt"] - t_lo
        ax.annotate("", xy=(x2, y_arrow), xytext=(x1, y_arrow),
                    arrowprops=dict(arrowstyle="<->", color="#222", lw=1.1), zorder=5)
        ax.text(0.5 * (x1 + x2), y_arrow + 0.012 * span, f"{(b['Rt'] - a['Rt']) * 1000:.0f} ms",
                ha="center", va="bottom", fontsize=7.6, color="#222", clip_on=True,
                bbox=dict(facecolor="white", edgecolor="none", pad=1.0), zorder=7)
    tamp, ramp = [], []
    for b in nodes:
        ax.plot(b["Rt"] - t_lo, raw[b["Rx"]], ".", color=C.REDR, ms=5, zorder=6)
        ax.text(b["Rt"] - t_lo, raw[b["Rx"]] + 0.035 * span, "R", ha="center", va="bottom", fontsize=8.5,
                color=C.REDR, fontweight="bold", zorder=7)
        ax.plot(x[b["Sx"]], raw[b["Sx"]], ".", color=C.BLUES, ms=4.5, zorder=6)
        tp = t_peak(res, b)
        if tp is not None:
            ax.plot(x[tp[0]], tp[1], "o", color=C.ORANGE_T, ms=5.5, zorder=7, mec="white", mew=0.6)
            ax.text(x[tp[0]], tp[1] + 0.035 * span, "T", ha="center", va="bottom", fontsize=8.5,
                    color="#9a5600", fontweight="bold", zorder=7)
            base = float(np.median(raw[max(0, b["on"] - int(0.030 * fs)):b["on"] + 1]))
            tamp.append(tp[1] - base); ramp.append(raw[b["Rx"]] - base)
        wide = b["dur"] >= C.WIDE_QRS_MS
        ax.text(b["Rt"] - t_lo, ymin - 0.115 * span, f"{b['dur']:.0f} ms", ha="center", va="top",
                fontsize=7.6, color=("#b30000" if wide else "#3a7a3a"),
                fontweight=("bold" if wide else "normal"))
    ax.set_xlim(0, DISP); ax.set_xlabel("seconds within window")
    ax.spines[["top", "right"]].set_visible(False)
    return nodes, tamp, ramp


ress = [analyse(name) for name, _ in PANELS]
win = [r["raw"][(r["t"] >= MARGIN) & (r["t"] <= MARGIN + DISP)] for r in ress]
lo, hi = float(min(w.min() for w in win)), float(max(w.max() for w in win))

fig, axes = plt.subplots(1, 2, figsize=(13, 6.5), sharey=True)
for ax, res, (_, title) in zip(axes, ress, PANELS):
    nodes, tamp, ramp = strip(ax, res, lo, hi)
    hr = 60000 / np.median(np.diff([b["Rt"] for b in nodes]) * 1000)
    ax.set_title(f"{title} (~{hr:.0f} bpm)", fontsize=11.5)
    print(f"{title}: {len(nodes)} beats, HR {hr:.0f}, median T {np.median(tamp) / 1000:.2f} mV, "
          f"median R {np.median(ramp) / 1000:.2f} mV (both from the pre-QRS baseline), "
          f"QRS {min(b['dur'] for b in nodes):.0f}-{max(b['dur'] for b in nodes):.0f} ms")
span = hi - lo
axes[0].set_ylim(lo - 0.21 * span, hi + 0.32 * span)
axes[0].set_ylabel("ECG (µV, true polarity)")
fig.suptitle(TITLE, fontsize=12.5, fontweight="bold")
fig.tight_layout()
out = C.output_path(sys.argv, "hyperacute_t_exercise_polar_h10.png")
fig.savefig(out, dpi=200, metadata={"Software": None})
print("->", out)
