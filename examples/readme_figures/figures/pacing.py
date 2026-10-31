"""README figure: ventricular pacing spikes in all 12 leads (seca 331, native 32 kHz, unfiltered).

Left: three paced beats per lead; right: one spike zoomed in (-3..+8 ms) per lead.
Reads data/pacing/ (see extract/pacing_extract.py); writes docs/images/pacing_spikes_seca_331_12lead.png
unless an output path is given.

Usage: python figures/pacing.py [output.png]
"""
import csv
import json
import sys
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams["path.simplify"] = False          # keep the 1 ms spikes when drawing 32 kHz data
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data" / "pacing"
OUT = HERE.parents[2] / "docs" / "images" / "pacing_spikes_seca_331_12lead.png"
TITLE = "Ventricular pacing spikes before every heartbeat, in all 12 leads (seca 331)"
ORDER = ["I", "II", "III", "aVR", "aVL", "aVF", "V1", "V2", "V3", "V4", "V5", "V6"]
SPIKE = "#c2185b"

meta = json.loads((DATA / "metadata.json").read_text())
z = np.load(DATA / "ecg_window.npz")
FS, UV_PER_COUNT = int(z["fs"]), float(z["uv_per_count"])
W0, x = int(z["start_sample"]), z["counts"]
CH = {str(n): i for i, n in enumerate(z["channels"])}
FIRST_BEAT = meta["figure"]["first_beat"]
ZOOM_BEAT = meta["figure"]["zoom_beat"]
Z_PRE, Z_POST = -meta["figure"]["zoom_ms"][0] / 1000, meta["figure"]["zoom_ms"][1] / 1000

pulses = {int(b["beat"]): int(b["pulse_native_sample"]) for b in csv.DictReader(open(DATA / "beats.csv"))}
T0 = pulses[FIRST_BEAT] / FS - meta["figure"]["lead_in_s"]
T1 = T0 + meta["figure"]["width_s"]


def twelve(i0, i1):
    """mV for the 12 standard leads (i0, i1 = samples from the start of the recording);
    limb leads III/aVR/aVL/aVF derived from I and II."""
    assert W0 <= i0 and i1 <= W0 + len(x), "outside the published window"
    m = x[i0 - W0:i1 - W0].astype(np.float64) * UV_PER_COUNT / 1000.0
    I, II = m[:, CH["I"]], m[:, CH["II"]]
    L = {"I": I, "II": II, "III": II - I, "aVR": -(I + II) / 2, "aVL": I - II / 2, "aVF": II - I / 2}
    L.update({v: m[:, CH[v]] for v in ["V1", "V2", "V3", "V4", "V5", "V6"]})
    return L


i0, i1 = int(T0 * FS), int(T1 * FS)
strip = twelve(i0, i1)
t = np.arange(i0, i1) / FS - T0
spk = [s / FS - T0 for s in pulses.values() if i0 <= s < i1]
ps = pulses[ZOOM_BEAT]
z0, z1 = ps - int(Z_PRE * FS), ps + int(Z_POST * FS)
zoom = twelve(z0, z1)
zt = (np.arange(z0, z1) - ps) / FS * 1000.0

fig, axs = plt.subplots(12, 2, figsize=(13, 13), sharex="col",
                        gridspec_kw=dict(width_ratios=[3, 1], hspace=0.12, wspace=0.10))
for r, lead in enumerate(ORDER):
    a, zx = axs[r]
    y = strip[lead] - np.median(strip[lead])
    a.plot(t, y, color="black", lw=0.45, zorder=4)
    for s in spk:
        a.axvline(s, color=SPIKE, ls=(0, (3, 3)), lw=0.7, alpha=0.6, zorder=1)
    yz = zoom[lead] - np.median(zoom[lead][: int(0.002 * FS)])
    zx.plot(zt, yz, color="black", lw=0.8, zorder=4)
    zx.axvline(0, color=SPIKE, ls=(0, (3, 3)), lw=0.7, alpha=0.6, zorder=1)
    a.set_ylabel(lead, rotation=0, ha="right", va="center", fontsize=10.5, fontweight="bold")
    for ax_ in (a, zx):
        ax_.spines[["top", "right"]].set_visible(False)
        ax_.tick_params(labelsize=7.5)
        ax_.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(3))
a0, z0_ = axs[0]
a0.set_xlim(0, T1 - T0)
z0_.set_xlim(zt[0], zt[-1])
lo, hi = a0.get_ylim()
for s1, s2 in zip(spk[:-1], spk[1:]):                   # spike-to-spike interval on the top row
    a0.annotate("", xy=(s2, hi), xytext=(s1, hi), annotation_clip=False,
                arrowprops=dict(arrowstyle="<->", color="#222", lw=1.1), zorder=5)
    a0.text(0.5 * (s1 + s2), hi, f"{(s2 - s1) * 1000:.0f} ms", ha="center", va="center", fontsize=8.5,
            color="#222", bbox=dict(facecolor="white", edgecolor="none", pad=1.2), zorder=7, clip_on=False)
a0.set_title("Three paced beats, all 12 leads (pink dashed line = pacing spike)", fontsize=11.5, pad=14)
z0_.set_title("One spike, zoomed in", fontsize=11.5, pad=14)
axs[-1, 0].set_xlabel("seconds")
axs[-1, 1].set_xlabel("milliseconds from spike")
fig.text(0.012, 0.5, "mV (unfiltered, 32 000 samples/s)", rotation=90, va="center", fontsize=10)
fig.suptitle(TITLE, fontsize=12.5, fontweight="bold", y=0.985)
fig.subplots_adjust(left=0.08, right=0.985, top=0.935, bottom=0.045)

out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=200, metadata={"Software": None})
print(f"{len(pulses)} paced beats in the recording; spike-to-spike intervals shown (ms):",
      [round((b - a_) * 1000) for a_, b in zip(spk[:-1], spk[1:])], "->", out)
