"""README figure: frontal QRS axis lying down vs while running (seca 331, Mason-Likar placement for both).

Top: the same 2.95 s window length in both conditions (3 beats lying down, 8 while running), all 12 leads; every
beat shown is part of its condition's average; QRS shaded across all 12 leads. Bottom: 12-lead MEAN beats
(pre-QRS reference = mean of 10..0.5 ms before the global QRS onset; mean over the 523 lying-down / 256 included
running beats), aligned on the lead-II R peak, with each condition's global QRS window and the signed-area axis +
vector magnitude over that window.

Global QRS (earliest onset / latest offset over all leads) from the spatial velocity (SV) of the 8 independent
leads (I, II, V1-V6; 100 Hz low-pass): onset = last point before the QRS where SV <= baseline + 5 uV/ms, offset
(J point) = first point after the QRS where SV <= baseline + 20 uV/ms (higher threshold because the ST segment
slopes); baseline = median SV over -110..-70 ms. The running offset is set by eye to +33 ms (the S upstroke bends
gradually into the upsloping ST segment; the threshold marks the start of the bend).

Reads data/qrs_axis/ (see extract/qrs_axis_extract.py); writes docs/images/qrs_axis_exercise_seca_331.png unless
an output path is given.

Usage: python figures/qrs_axis.py [output.png]
"""
import sys
from pathlib import Path

import numpy as np
import scipy.signal as ss
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data" / "qrs_axis"
OUT = HERE.parents[2] / "docs" / "images" / "qrs_axis_exercise_seca_331.png"
TITLE = "QRS axis {rest} at rest, indeterminate while running (seca 331, 12-lead ECG)"
FS = 2000
LEADS = ["I", "II", "III", "aVR", "aVL", "aVF", "V1", "V2", "V3", "V4", "V5", "V6"]
INDEPENDENT = [0, 1, 6, 7, 8, 9, 10, 11]                    # I, II, V1-V6
REST_C, EX_C = "#2c6fbb", "#d97a00"

ZB = np.load(DATA / "beats.npz")
ZS = np.load(DATA / "strips.npz")
assert [str(s) for s in ZS["leads"]] == LEADS


def beats_mV(key, mask=None):
    """(n_beats, n_samples, 12) mV; limb leads III/aVR/aVL/aVF derived from I and II."""
    c = ZB[f"{key}_counts"].astype(np.float64) + ZB[f"{key}_offset"][:, None, :]
    if mask is not None:
        c = c[mask]
    m = c / float(ZB["adc_gain_per_mV"])
    I, II = m[..., 0], m[..., 1]
    out = np.empty(m.shape[:2] + (12,))
    out[..., INDEPENDENT] = m
    out[..., 2], out[..., 3], out[..., 4], out[..., 5] = II - I, -(I + II) / 2, I - II / 2, II - I / 2
    return out


def mean_beat(raw, off_override=None):
    T = ZB["time_ms"]; ref = (T >= -85) & (T <= -68)
    y = (raw - raw[:, ref].mean(1)[:, None, :]).mean(0)
    low = ss.sosfiltfilt(ss.butter(3, 100, fs=FS, output="sos"), y * 1000, axis=0)
    sv = np.sqrt((np.gradient(low, T, axis=0)[:, INDEPENDENT] ** 2).sum(1))      # uV/ms
    base = np.median(sv[(T >= -110) & (T <= -70)])
    pk = int(np.argmax(np.where((T > -40) & (T < 20), sv, 0)))
    i = pk
    while sv[i] > base + 5: i -= 1
    j = pk
    while sv[j] > base + 20: j += 1
    on, off = T[i], (T[j] if off_override is None else off_override)
    # voltage reference: mean of the samples from 10 ms to 0.5 ms before the global QRS onset, removed per lead
    # as a constant. The -85..-68 ms window above only seeds the onset search (SV is offset-invariant).
    y = y - y[(T >= on - 10) & (T <= on - 0.5)].mean(0)
    v = np.c_[y[:, 0], (2 * y[:, 1] - y[:, 0]) / np.sqrt(3)]; m = (T >= on) & (T <= off)
    a = np.trapezoid(v[m], T[m], axis=0)
    rr = np.flatnonzero((T > -40) & (T < 10)); r0 = T[rr[np.argmax(y[rr, 1])]]
    return T - r0, y, on - r0, off - r0, float(np.degrees(np.arctan2(a[1], a[0]))), len(raw), float(np.hypot(*a)), (on, off)


RAW_R = beats_mV("rest")
RAW_E = beats_mV("run", ZB["run_included"])
Tr, yr, on_r, off_r, ax_r, n_r, mag_r, qrs_r = mean_beat(RAW_R)
Te, ye, on_e, off_e, ax_e, n_e, mag_e, qrs_e = mean_beat(RAW_E, off_override=33.0)
print(f"rest n={n_r} QRS {on_r:.1f}..{off_r:.1f} ({off_r - on_r:.0f} ms) axis {ax_r:.1f} |A| {mag_r:.2f} mV*ms; "
      f"run n={n_e} QRS {on_e:.1f}..{off_e:.1f} ({off_e - on_e:.0f} ms) axis {ax_e:.1f} |A| {mag_e:.2f} mV*ms")
STRIPS = [("rest", "Rest, lying down (~62 bpm)", REST_C), ("run", "Running (~165 bpm)", EX_C)]

fig = plt.figure(figsize=(13, 18))
outer = fig.add_gridspec(2, 1, height_ratios=[12, 7.2], hspace=0.10)
top = outer[0].subgridspec(12, 2, hspace=0.08, wspace=0.06)
bot = outer[1].subgridspec(1, 2, width_ratios=[1.75, 1], wspace=0.10)
mb = bot[0].subgridspec(6, 2, hspace=0.12, wspace=0.18)

# --- top: 3-beat 12-lead strips (display band 0.5-100 Hz) ---
sos = ss.butter(2, [0.5, 100], "bandpass", fs=FS, output="sos")
strip_axes = np.empty((12, 2), object)
WIN = None                                   # window length (samples), set by the rest panel: 3 beats
for c, (key, title, col) in enumerate(STRIPS):
    pad, win = int(ZS["pad_samples"]), int(ZS["window_samples"])
    d = ZS[f"{key}_counts"]
    x = np.where(d == np.iinfo(np.int32).min, np.nan, d / float(ZS["adc_gain_per_mV"]))
    s = ZS[f"{key}_fiducials_s"]                              # beat fiducials, seconds from the window start
    ok = np.isfinite(x).all(1)
    assert ok[pad:-pad].all()
    l0 = pad - int(np.argmin(ok[:pad][::-1])) if not ok[:pad].all() else 0      # finite run around window
    r1 = pad + win + (int(np.argmin(ok[-pad:])) if not ok[-pad:].all() else pad)
    x = ss.sosfiltfilt(sos, x[l0:r1], axis=0)[pad - l0:pad - l0 + win]
    t = np.arange(len(x)) / FS
    qrs = qrs_r if key == "rest" else qrs_e                  # QRS window rel. to the beat fiducial (ms)
    bands = [(sj + qrs[0] / 1000, sj + qrs[1] / 1000) for sj in s]
    print(key, len(s), "beats in strip")
    for r in range(12):
        ax = fig.add_subplot(top[r, c], sharex=strip_axes[0, c] if r else None,
                             sharey=strip_axes[r, 0] if c else None)
        strip_axes[r, c] = ax
        for b0, b1 in bands:
            ax.axvspan(b0, b1, color=col, alpha=0.16, zorder=0, lw=0)
        ax.plot(t, x[:, r], color="black", lw=0.7, zorder=4)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(labelsize=7)
        ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(2))
        if c == 0:
            ax.set_ylabel(LEADS[r], rotation=0, ha="right", va="center", fontsize=10, fontweight="bold")
        else:
            ax.tick_params(labelleft=False)
        if r < 11:
            ax.tick_params(labelbottom=False)
    strip_axes[0, c].set_title(title, fontsize=11.5, color=col, fontweight="bold")
    strip_axes[0, c].set_xlim(0, t[-1])
    strip_axes[11, c].set_xlabel("seconds")
fig.text(0.004, 0.71, "mV (display filter 0.5–100 Hz)", rotation=90, va="center", fontsize=9.5)

# --- bottom left: 12-lead mean beats, R-aligned ---
w_r, w_e = (Tr >= -100) & (Tr <= 290), (Te >= -100) & (Te <= 290)
for i, lead in enumerate(LEADS):
    ax = fig.add_subplot(mb[i % 6, i // 6])
    ax.axhline(0, color="#9aa0a6", lw=0.7, ls=(0, (4, 3)))
    ax.plot(Tr[w_r], yr[w_r, i] * 1000, color=REST_C, lw=1.5)
    ax.plot(Te[w_e], ye[w_e, i] * 1000, color=EX_C, lw=1.5)
    ax.axvspan(on_r, off_r, ymin=0.5, ymax=1.0, color=REST_C, alpha=0.16, zorder=0, lw=0)   # rest QRS: upper half
    ax.axvspan(on_e, off_e, ymin=0.0, ymax=0.5, color=EX_C, alpha=0.18, zorder=0, lw=0)     # run QRS: lower half
    ax.text(0.02, 0.88, lead, transform=ax.transAxes, fontsize=9.5, fontweight="bold", va="top")
    ax.set_xlim(-100, 290)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=7)
    ax.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(3))
    if i % 6 < 5:
        ax.tick_params(labelbottom=False)
    else:
        ax.set_xlabel("ms from R peak", fontsize=9)
    if i == 0:
        ax.set_title(f"Mean heartbeat (rest n={n_r}, running n={n_e}); µV", fontsize=11, loc="left")
    if i == 6:
        ax.plot([], [], color=REST_C, lw=1.5, label="rest"); ax.plot([], [], color=EX_C, lw=1.5, label="running")
        ax.legend(loc="lower right", frameon=False, fontsize=8.5)

# --- bottom right: hexaxial dial ---
d = fig.add_subplot(bot[1]); d.set_aspect("equal"); d.axis("off")
def xy(deg, r=1.0):
    return r * np.cos(np.radians(deg)), -r * np.sin(np.radians(deg))
th = np.linspace(-90, -30, 60); d.fill(*zip(*([(0, 0)] + [xy(t_) for t_ in th])), color="#eceff3", zorder=0)
th = np.linspace(-30, 90, 120); d.fill(*zip(*([(0, 0)] + [xy(t_) for t_ in th])), color="#eef5ea", zorder=0)
th = np.linspace(90, 180, 90); d.fill(*zip(*([(0, 0)] + [xy(t_) for t_ in th])), color="#f1edf7", zorder=0)
th = np.linspace(-180, -90, 90); d.fill(*zip(*([(0, 0)] + [xy(t_) for t_ in th])), color="#f7eeee", zorder=0)
d.text(*xy(135, 0.62), "right axis\ndeviation", ha="center", va="center", fontsize=9, color="#6d5a8a")
d.text(*xy(-135, 0.62), "extreme\naxis", ha="center", va="center", fontsize=9, color="#8a5a5a")
cc = np.linspace(0, 360, 361); d.plot(*xy(cc), color="#9aa0a6", lw=1.0)
for deg in range(-150, 181, 30):
    d.plot(*zip((0, 0), xy(deg)), color="#d0d4d9", lw=0.8, zorder=1)
    d.text(*xy(deg, 1.14), (f"{deg:+d}°" if deg not in (0, 180) else f"{deg}°").replace("-", "−"),
           ha="center", va="center", fontsize=8.5, color="#555")
d.text(*xy(-58, 0.84), "left axis\ndeviation", ha="center", va="center", fontsize=9, color="#6b7280")
d.text(*xy(30, 0.62), "normal", ha="center", va="center", fontsize=9, color="#3a7a3a")
def angle_spread(raw, on, off):
    """Running axis over QRS boundaries within +-2 ms (onset) and +-4 ms (offset) of the chosen window,
    same pre-QRS reference rule; returns (min, max) angle in degrees and the magnitudes."""
    T = ZB["time_ms"]
    y0 = raw.mean(0)
    angs, mags = [], []
    for o in (on - 2, on, on + 2):
        y = y0 - y0[(T >= o - 10) & (T <= o - 0.5)].mean(0)
        v = np.c_[y[:, 0], (2 * y[:, 1] - y[:, 0]) / np.sqrt(3)]
        for f_ in (off - 4, off, off + 4):
            m = (T >= o) & (T <= f_); a = np.trapezoid(v[m], T[m], axis=0)
            angs.append(np.degrees(np.arctan2(a[1], a[0]))); mags.append(np.hypot(*a))
    return min(angs), max(angs), min(mags), max(mags)


lo_e, hi_e, mlo_e, mhi_e = angle_spread(RAW_E, *qrs_e)   # logged only
lo_r, hi_r, _, _ = angle_spread(RAW_R, *qrs_r)
print(f"axis spread over +-2/+-4 ms boundaries: rest {lo_r:.1f}..{hi_r:.1f}; run {lo_e:.1f}..{hi_e:.1f} "
      f"(|A| {mlo_e:.2f}..{mhi_e:.2f} mV*ms)")
# rest: arrow, length = 0.92 of the dial radius (= its integrated vector, 18 mV*ms)
d.annotate("", xy=xy(ax_r, 0.92), xytext=(0, 0),
           arrowprops=dict(arrowstyle="-|>", color=REST_C, lw=2.6, mutation_scale=18), zorder=5)
lx, ly = xy(ax_r, 0.80)
d.text(lx + 0.06, ly, f"rest {ax_r:+.0f}°\n{mag_r:.1f} mV·ms", ha="left", va="center", fontsize=10,
       color=REST_C, fontweight="bold", bbox=dict(facecolor="white", edgecolor="none", pad=1.2), zorder=6)
# running: the net vector nearly cancels -> indeterminate. Draw it to the same scale (a stub) plus the fan
# of directions it takes when the QRS boundaries move by a few ms.
Le = 0.92 * mag_e / mag_r
d.plot(*zip((0, 0), xy(ax_e, max(Le, 0.03))), color=EX_C, lw=3.2, solid_capstyle="round", zorder=5)
d.text(0, -1.38, f"running: indeterminate axis\n{mag_e:.1f} mV·ms (orange stub at centre)",
       ha="center", va="center", fontsize=10, color=EX_C, fontweight="bold", zorder=6)
d.set_xlim(-1.3, 1.3); d.set_ylim(-1.65, 1.3)
d.set_title("Integrated frontal-plane QRS vector\n(signed QRS area, leads I + II;\nlength = vector magnitude, same scale)", fontsize=11)

fig.suptitle(TITLE.format(rest=f"{ax_r:+.0f}°", run=f"{ax_e:+.0f}°".replace("-", "−")),
             fontsize=13, fontweight="bold", y=0.995)
fig.subplots_adjust(left=0.085, right=0.985, top=0.965, bottom=0.03)
out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=200, metadata={"Software": None})
print("->", out)
