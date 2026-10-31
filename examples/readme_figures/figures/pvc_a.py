"""README figure A: a premature ventricular contraction during near-maximum exercise (Polar H10).

Reads data/pvc_a/ (raw ECG + hand-reviewed beat marks) and draws:
  - grey band per sinus QRS, solid orange = ventricular QRS core, pale orange = rest of the
    ventricular complex up to the end of its (negative) T wave
  - S-to-S interval ladder between sinus beats (the PVC is skipped, so the interval spanning it
    shows the compensatory pause) and a purple coupling arrow (last sinus S -> ventricular S)
  - R / S / R' marks and the duration of the ventricular QRS core

    python figures/pvc_a.py [output.png]      # default: <repo>/docs/images/pvc_a_during_exercise_polar_h10.png
"""
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import _h10_common as C

TITLE = "A) Premature ventricular contraction during near-maximum exercise (Polar H10, single lead)"

t, raw, meta = C.load("pvc_a")
fs = meta["sampling_rate_hz"]
G = C.Geometry(raw, t, fs)
hr = meta["hand_reviewed"]
CORE = tuple(hr["pvc_qrs_core_s"])
t_lo, t_hi = meta["display_window_s"]
i0, i1 = int(np.searchsorted(t, t_lo)), int(np.searchsorted(t, t_hi))


def snap_ext(time, kind, W=12):
    """Nudge a hand-placed time to the nearest local max/min within +-W samples."""
    si = int(np.clip(np.searchsorted(t, time), 1, len(raw) - 2))
    best = None
    for j in range(max(1, si - W), min(len(raw) - 1, si + W)):
        is_ext = (raw[j] >= raw[j - 1] and raw[j] >= raw[j + 1]) if kind == "max" \
            else (raw[j] <= raw[j - 1] and raw[j] <= raw[j + 1])
        if is_ext and (best is None or abs(j - si) < best[0]):
            best = (abs(j - si), j)
    j = best[1] if best else si
    return t[j], float(raw[j])


def reviewed_beats(marks):
    """Group the hand-review marks per beat: QRS window(s) sharing an onset, plus R, S and type."""
    types = [(m["t"], m["label"]) for m in marks if m["kind"] == "type"]
    Rmarks = [m["t"] for m in marks if m["kind"] == "R"]
    Smarks = [m["t"] for m in marks if m["kind"] == "S"]
    groups = {}
    for m in marks:
        if m["kind"] == "QRS":
            groups.setdefault(round(m["t0"], 3), []).append((m["t0"], m["t1"]))
    beats = []
    for k in sorted(groups):
        wins = sorted(groups[k], key=lambda w: w[1] - w[0])          # narrowest (core) first
        lab = min(types, key=lambda x: abs(x[0] - wins[0][0]))[1] if types else "N"
        x0, x1 = wins[-1]
        beats.append(dict(x0=x0, x1=x1, s0=int(np.searchsorted(t, x0)), s1=int(np.searchsorted(t, x1)),
                          lab=lab, wins=wins,
                          Rmark=next((r for r in Rmarks if x0 <= r <= x1), None),
                          Smark=next((s for s in Smarks if x0 <= s <= x1), None)))
    return beats


rb = reviewed_beats(hr["marks"])
for b in rb:
    sx = b["s0"] + int(np.argmin(raw[b["s0"]:b["s1"]]))
    b["Sx"], b["St"] = sx, G.tt(G.subpeak(sx, -1))
    if b["lab"] == "V":                      # ventricular beat: the reviewer's explicit S and R'
        st = b["Smark"] if b["Smark"] is not None else t[sx]
        if b["Rmark"] is not None:
            rtv = b["Rmark"]
        else:
            w = min(len(raw), sx + int(0.17 * fs))
            rtv = t[sx + int(np.argmax(raw[sx:w]))] if w > sx else t[sx]
        b["Sxy"], b["Rxy"] = snap_ext(st, "min"), snap_ext(rtv, "max")
        b["Scouple"] = b["Sxy"][0]
    else:
        rx = b["s0"] + int(np.argmax(raw[b["s0"]:max(sx, b["s0"] + 1) + 1]))
        b["Rx"], b["Rt"] = rx, G.tt(G.subpeak(rx, +1))
sinus = [b for b in rb if b["lab"] != "V"]
pvcs = [b for b in rb if b["lab"] == "V"]

x, y = t[i0:i1], raw[i0:i1]
ymin, ymax = float(y.min()), float(y.max())
span = ymax - ymin

# end of the PVC's (negative) T wave: where the recovery from the post-R' trough comes back to
# within 10 % of the trough depth of baseline. At ~160 bpm the next P wave is buried and the PVC
# lands on the previous T, so baseline = the level just before the next sinus QRS.
nxt = hr["next_sinus_qrs_before_s"]
base = float(np.median(y[(x > nxt - 0.08) & (x < nxt)]))
w = (x > CORE[1]) & (x < nxt)
xi, yi = x[w], y[w]
k = int(np.argmin(yi))
thr = base - 0.10 * (base - yi[k])
j = k + int(np.argmax(yi[k:] >= thr))
T_END = xi[j - 1] + (thr - yi[j - 1]) / (yi[j] - yi[j - 1]) * (xi[j] - xi[j - 1])

fig, ax = plt.subplots(figsize=(13, 6.5))
for b in rb:
    if b["lab"] == "V":
        (n0, n1), (w0, _) = b["wins"][0], b["wins"][-1]
        ax.axvspan(w0, T_END, color=C.ORANGE_WIDE, alpha=0.9, zorder=0)
        ax.axvspan(n0, n1, color=C.ORANGE, alpha=0.95, zorder=0, edgecolor=C.ORANGE_EDGE, lw=1.2)
    else:
        ax.axvspan(b["x0"], b["x1"], color=C.GREY, alpha=0.95, zorder=0)
ax.plot(x, y, color="black", lw=1.5, zorder=4)

# S-S ladder over the sinus beats + coupling arrow
y_arrow = ymax + 0.16 * span
fids = [b["St"] for b in sinus]
for xv in fids:
    ax.axvline(xv, color=C.DASH, ls=(0, (4, 3)), lw=0.9, zorder=1)
for x1, x2 in zip(fids[:-1], fids[1:]):
    ax.annotate("", xy=(x2, y_arrow), xytext=(x1, y_arrow),
                arrowprops=dict(arrowstyle="<->", color="#222", lw=1.3), zorder=5)
    C.label_box(ax.text(0.5 * (x1 + x2), y_arrow + 0.015 * span, f"{(x2 - x1) * 1000:.0f} ms",
                        ha="center", va="bottom", fontsize=8.5, color="#222", clip_on=True))
for v in pvcs:
    vS = v["Scouple"]
    prior = [b["St"] for b in sinus if b["St"] < vS]
    if prior:
        lastN, y_c = max(prior), ymax + 0.055 * span
        ax.axvline(vS, color=C.COUPLE, ls=(0, (4, 3)), lw=0.9, zorder=1)
        ax.annotate("", xy=(vS, y_c), xytext=(lastN, y_c),
                    arrowprops=dict(arrowstyle="<->", color=C.COUPLE, lw=1.3), zorder=5)
        C.label_box(ax.text(0.5 * (lastN + vS), y_c + 0.015 * span, f"coupling {(vS - lastN) * 1000:.0f} ms",
                            ha="center", va="bottom", fontsize=8.5, color=C.COUPLE, clip_on=True))
        print(f"coupling {(vS - lastN) * 1000:.0f} ms")

# R / S marks (S label and R' only on the ventricular beat)
for b in rb:
    if b["lab"] == "V":
        (sxt, syy), (rxt, ryy) = b["Sxy"], b["Rxy"]
        ax.plot(sxt, syy, ".", color=C.BLUES, ms=5, zorder=6, clip_on=True)
        ax.text(sxt, syy - 0.03 * span, "S", ha="center", va="top", fontsize=8.5,
                color=C.BLUES, zorder=6, clip_on=True)
        ax.plot(rxt, ryy, ".", color=C.REDR, ms=5, zorder=6, clip_on=True)
        ax.text(rxt - 0.012, ryy + 0.03 * span, "R′", ha="right", va="center", fontsize=8.5,  # left of the upstroke
                color=C.REDR, fontweight="bold", zorder=6, clip_on=True)
    else:
        ax.plot(t[b["Sx"]], raw[b["Sx"]], ".", color=C.BLUES, ms=5, zorder=6, clip_on=True)
        ax.plot(b["Rt"], raw[b["Rx"]], ".", color=C.REDR, ms=5, zorder=6, clip_on=True)
        ax.text(b["Rt"], raw[b["Rx"]] + 0.03 * span, "R", ha="center", va="bottom",
                fontsize=8, color=C.REDR, zorder=6, clip_on=True)

ax.set_title(TITLE, fontsize=12.5, fontweight="bold")
ax.set_xlabel("seconds within window")
ax.set_ylabel("ECG (µV, true polarity)")
ax.set_xlim(t_lo, t_hi)
ax.set_ylim(ymin - 0.21 * span, ymax + 0.28 * span)
ax.spines[["top", "right"]].set_visible(False)

# duration of the ventricular QRS core, under the trace
yl0, yl1 = ax.get_ylim()
span2 = (yl1 - yl0) / 1.46
y_d = yl0 + 0.18 * span2 - 0.08 * span2
ax.annotate("", xy=(CORE[1], y_d), xytext=(CORE[0], y_d),
            arrowprops=dict(arrowstyle="<->", color="#222", lw=1.3, shrinkA=0, shrinkB=0), zorder=5)
C.label_box(ax.text(0.5 * sum(CORE), y_d - 0.015 * span2, f"{(CORE[1] - CORE[0]) * 1000:.0f} ms",
                    ha="center", va="top", fontsize=8.5, color="#222"))

ss = [round((b2 - b1) * 1000) for b1, b2 in zip(fids[:-1], fids[1:])]
print(f"S-S ms {ss}; PVC core {(CORE[1] - CORE[0]) * 1000:.0f} ms; T-wave end {T_END:.3f} s")
fig.tight_layout()
out = C.output_path(sys.argv, "pvc_a_during_exercise_polar_h10.png")
fig.savefig(out, dpi=200, metadata={"Software": None})
print("->", out)
