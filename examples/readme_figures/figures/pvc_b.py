"""README figure B: three premature ventricular contractions in recovery from exercise (Polar H10).

Reads data/pvc_b/ (raw ECG + per-beat QRS segments + hand-reviewed PVC flags) and draws:
  - grey band per sinus QRS; solid orange = ventricular complex (knee where the trace leaves
    baseline -> R' + 50 ms), pale orange = on to the end of its (negative) T wave
  - S-to-S interval ladder between sinus beats (PVCs skipped -> the interval spanning each PVC shows
    the compensatory pause) and purple coupling arrows (last sinus S -> ventricular S)
  - R / S / R' marks and the duration of each ventricular complex

    python figures/pvc_b.py [output.png]      # default: <repo>/docs/images/pvc_b_recovery_polar_h10.png
"""
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import _h10_common as C

TITLE = "B) Three premature ventricular contractions in recovery from exercise (Polar H10, single lead)"
V_TAIL_S = 0.050   # ventricular complex assumed to end ~50 ms after the R' notch

t, raw, meta = C.load("pvc_b")
fs = meta["sampling_rate_hz"]
G = C.Geometry(raw, t, fs)
t_lo, t_hi = meta["display_window_s"]

bs = []
for row in C.load_csv("pvc_b", "beats.csv"):
    on, off, is_pvc = int(row["qrs_on_sample"]), int(row["qrs_off_sample"]), row["pvc"] == "1"
    sx = on + int(np.argmin(raw[on:off + 1]))                   # S nadir in the QRS segment
    if is_pvc:
        # R' = the notch: first local max on the upstroke after the deep S. The complex starts at
        # the sub-sample knee where the trace leaves baseline by 5 % of the S depth, ends R' + 50 ms.
        i = sx
        while not (raw[i] > 0 and raw[i] > raw[i + 1]):
            i += 1
        rx = i
        Rt = G.tt(G.subpeak(rx, +1))
        base = float(np.median(raw[on - 12:on - 2]))
        thr = base - 0.05 * (base - raw[sx])
        j = on - 3
        while raw[j + 1] > thr:
            j += 1
        t_on = t[j] + (raw[j] - thr) / (raw[j] - raw[j + 1]) * (t[j + 1] - t[j])
        t_end = Rt + V_TAIL_S
    else:
        rx = on + int(np.argmax(raw[on:max(sx, on + 1) + 1]))     # sinus: R precedes S
        Rt, t_on, t_end = G.tt(G.subpeak(rx, +1)), t[on], t[off]
    bs.append(dict(on=on, roff=off, sx=sx, rx=rx, Rt=Rt, St=G.tt(G.subpeak(sx, -1)),
                   t_on=t_on, t_end=t_end, dur=(t_end - t_on) * 1000.0, pvc=is_pvc))

i0, i1 = int(np.searchsorted(t, t_lo)), int(np.searchsorted(t, t_hi))
ymin, ymax = float(raw[i0:i1].min()), float(raw[i0:i1].max())
span = ymax - ymin

fig, ax = plt.subplots(figsize=(13, 6.5))
for b in bs:
    ax.axvspan(b["t_on"], b["t_end"], color=(C.ORANGE if b["pvc"] else C.GREY), alpha=0.95, zorder=0,
               lw=(1.2 if b["pvc"] else None))
ax.plot(t[i0:i1], raw[i0:i1], color="black", lw=1.5, zorder=4)

# S-S ladder over sinus beats + coupling arrows
nodes = [b for b in bs if not b["pvc"] and t_lo <= b["St"] <= t_hi]
fids = [b["St"] for b in nodes]
y_arrow = ymax + 0.16 * span
for xv in fids:
    ax.axvline(xv, color=C.DASH, ls=(0, (4, 3)), lw=0.9, zorder=1)
for x1, x2 in zip(fids[:-1], fids[1:]):
    ax.annotate("", xy=(x2, y_arrow), xytext=(x1, y_arrow),
                arrowprops=dict(arrowstyle="<->", color="#222", lw=1.3), zorder=5)
    C.label_box(ax.text(0.5 * (x1 + x2), y_arrow + 0.015 * span, f"{(x2 - x1) * 1000:.0f} ms",
                        ha="center", va="bottom", fontsize=8.5, color="#222", clip_on=True))
for v in [b for b in bs if b["pvc"] and t_lo <= b["St"] <= t_hi]:
    prior = [b["St"] for b in nodes if b["St"] < v["St"]]
    if not prior:
        continue
    lastN, y_c = max(prior), ymax + 0.055 * span
    ax.axvline(v["St"], color=C.COUPLE, ls=(0, (4, 3)), lw=0.9, zorder=1)
    ax.annotate("", xy=(v["St"], y_c), xytext=(lastN, y_c),
                arrowprops=dict(arrowstyle="<->", color=C.COUPLE, lw=1.3), zorder=5)
    C.label_box(ax.text(0.5 * (lastN + v["St"]), y_c + 0.015 * span,
                        f"coupling {(v['St'] - lastN) * 1000:.0f} ms", ha="center", va="bottom",
                        fontsize=8.5, color=C.COUPLE, clip_on=True))
    print(f"coupling {(v['St'] - lastN) * 1000:.0f} ms")

# R / S marks
for b in bs:
    if not (t_lo <= b["Rt"] <= t_hi):
        continue
    ax.plot(b["Rt"], raw[b["rx"]], ".", color=C.REDR, ms=5, zorder=6, clip_on=True)
    ax.plot(t[b["sx"]], raw[b["sx"]], ".", color=C.BLUES, ms=5, zorder=6, clip_on=True)
    if b["pvc"]:   # R' sits on a steep upstroke: label left of the trace
        ax.text(b["Rt"] - 0.025, raw[b["rx"]] + 0.03 * span, "R'", ha="right", va="center",
                fontsize=8.5, color=C.REDR, fontweight="bold", zorder=6, clip_on=True)
    else:
        ax.text(b["Rt"], raw[b["rx"]] + 0.03 * span, "R", ha="center", va="bottom",
                fontsize=8.5, color=C.REDR, zorder=6, clip_on=True)
    ax.text(t[b["sx"]], raw[b["sx"]] - 0.03 * span, "S", ha="center", va="top",
            fontsize=8.5, color=C.BLUES, zorder=6, clip_on=True)

ax.set_title(TITLE, fontsize=12.5, fontweight="bold")
ax.set_xlabel("seconds within window")
ax.set_ylabel("ECG (µV, true polarity)")
ax.set_xlim(t_lo, t_hi)
ax.set_ylim(ymin - 0.21 * span, ymax + 0.28 * span)
ax.spines[["top", "right"]].set_visible(False)

for b in [b for b in bs if b["pvc"]]:
    # pale halo: end of the ventricular complex -> end of its (negative) T wave, where the slow
    # recovery after the post-R' trough comes back to within 10 % of the trough depth of the
    # pre-PVC baseline
    Rp = b["sx"] + int(np.argmax(raw[b["sx"]:b["sx"] + int(0.17 * fs)]))
    base = float(np.median(raw[b["on"] - 12:b["on"] - 2]))
    tr = Rp + int(np.argmin(raw[Rp:Rp + int(0.25 * fs)]))
    thr = base - 0.10 * (base - raw[tr])
    j = tr
    while raw[j + 1] < thr:
        j += 1
    t_T = t[j] + (thr - raw[j]) / (raw[j + 1] - raw[j]) * (t[j + 1] - t[j])
    ax.axvspan(b["t_end"], t_T, color=C.ORANGE_WIDE, alpha=0.9, zorder=0, lw=0)
    # duration of the ventricular complex, under the S
    y_d = ymin - 0.08 * span
    ax.annotate("", xy=(b["t_end"], y_d), xytext=(b["t_on"], y_d),
                arrowprops=dict(arrowstyle="<->", color="#222", lw=1.3, shrinkA=0, shrinkB=0), zorder=5)
    C.label_box(ax.text(0.5 * (b["t_on"] + b["t_end"]), y_d - 0.015 * span, f"{b['dur']:.0f} ms",
                        ha="center", va="top", fontsize=8.5, color="#222"))
    print(f"PVC R' {b['Rt']:.3f} s: complex {b['dur']:.0f} ms, T-wave end {t_T:.3f} s")

print("S-S ms", [round((b2 - b1) * 1000) for b1, b2 in zip(fids[:-1], fids[1:])])
fig.tight_layout()
out = C.output_path(sys.argv, "pvc_b_recovery_polar_h10.png")
fig.savefig(out, dpi=200, metadata={"Software": None})
print("->", out)
