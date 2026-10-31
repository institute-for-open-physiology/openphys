"""Unlabelled native-sample figures for independent visual annotation."""

from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import numpy as np


def images(context, out):
    out = Path(out)
    out.mkdir()
    d = context["native"]
    t = d.relative_s.to_numpy()
    v = d.ecg_uv.to_numpy() / 1000
    files = []
    spans = [(0, 10)] + [(s, s + 2.2) for s in [-1.2, 0.8, 2.8, 4.8, 6.8, 8.8]]
    for n, (lo, hi) in enumerate(spans):
        fig, ax = plt.subplots(figsize=(16, 5), dpi=120)
        mask = (t >= lo) & (t <= hi)
        ax.plot(
            t[mask],
            v[mask],
            color="#172534",
            lw=1,
            marker="." if n else None,
            markersize=3,
        )
        ax.set_xlim(lo, hi)
        ax.set_xlabel("Seconds relative to selected strip start")
        ax.set_ylabel("mV • native polarity, unfiltered")
        ax.xaxis.set_major_locator(MultipleLocator(0.2 if n else 1))
        ax.xaxis.set_minor_locator(MultipleLocator(0.04 if n else 0.2))
        ax.yaxis.set_major_locator(MultipleLocator(0.5))
        ax.yaxis.set_minor_locator(MultipleLocator(0.1))
        ax.grid(which="major", color="#de949a", alpha=0.7)
        ax.grid(which="minor", color="#eecdd0", alpha=0.6)
        ax.set_title(
            "Unlabelled ECG — "
            + (
                "10-second view"
                if n == 0
                else f"Enlargement {n}; dots are original samples"
            )
        )
        if n:
            ax2 = ax.secondary_xaxis(
                "top",
                functions=(
                    lambda x: np.interp(x, t, np.arange(len(t))),
                    lambda x: np.interp(x, np.arange(len(t)), t),
                ),
            )
            ax2.set_xlabel("Native sample index (same coordinates as CSV)")
            ax2.xaxis.set_major_locator(MultipleLocator(20))
        fig.tight_layout()
        p = out / f"panel_{n}.png"
        fig.savefig(p)
        plt.close(fig)
        files.append(p)
    return files


def beat_window(beat, t):
    """Beat plot limits, widened for long PR/QT so no landmark or range is cropped."""
    ends = [x for x in beat["marks"].values() if x is not None] + [
        x for r in beat.get("ranges", {}).values() if r for x in r
    ]
    lo = max(t[0], min([beat["fid_s"] - 0.30, *ends]) - 0.06)
    hi = min(t[-1], max([beat["fid_s"] + 0.52, *ends]) + 0.06)
    return float(lo), float(hi)


def annotated_images(context, runs, out, every_beat=False):
    """Machine overview/examples, or every Astra draft beat with amplitude enlargement."""
    out = Path(out)
    out.mkdir()
    native = context["native"]
    t = native.relative_s.to_numpy()
    v = native.ecg_uv.to_numpy() / 1000
    files = []
    for run in runs:
        beats = [b for b in run["beats"] if 0 <= b["fid_s"] < 10]
        if not beats:
            continue  # Failure/missing annotations are carried in the evidence JSON.
        groups = (
            [beats[i : i + 2] for i in range(0, len(beats), 2)]
            if every_beat
            else [[beats[i] for i in sorted({0, len(beats) // 2, len(beats) - 1})]]
        )
        for page, group in enumerate(groups):
            fig, axes = plt.subplots(
                len(group) * 2, 1, figsize=(16, len(group) * 5), dpi=120
            )
            for row, beat in enumerate(group):
                lo, hi = beat_window(beat, t)
                mask = (t >= lo) & (t <= hi)
                for zoom in [0, 1]:
                    ax = axes[2 * row + zoom]
                    ax.plot(
                        t[mask],
                        v[mask],
                        ".-",
                        color="#172534",
                        linewidth=1,
                        markersize=3,
                    )
                    for key, x in beat["marks"].items():
                        if x is None or not lo <= x <= hi:
                            continue
                        color = (
                            "#9853aa"
                            if key.startswith("P_")
                            else "#00816c"
                            if key.startswith("T_")
                            else "#167bb0"
                        )
                        y = np.interp(x, t, v)
                        bounds = beat.get("ranges", {}).get(key)
                        if bounds:
                            ax.hlines(y, *bounds, color=color, linewidth=2, alpha=0.5)
                        ax.scatter([x], [y], s=25, color=color, zorder=4)
                        ax.axvline(x, color=color, alpha=0.3, linewidth=0.7)
                        ax.annotate(
                            key,
                            (x, y),
                            xytext=(2, 8 if key.endswith("peak") else -15),
                            textcoords="offset points",
                            fontsize=8,
                            color=color,
                        )
                    if zoom:
                        # Amplitude zoom is labelled; full-amplitude trace is immediately above.
                        baseline = float(
                            np.median(v[(t >= lo) & (t < beat["fid_s"] - 0.08)])
                        )
                        ax.set_ylim(baseline - 0.2, baseline + 0.65)
                    ax.set_xlim(lo, hi)
                    ax.set_ylabel(
                        "mV" + (" · amplitude zoom" if zoom else " · full amplitude")
                    )
                    # Astra revises in native sample indices, as in the unlabelled panels.
                    top = ax.secondary_xaxis(
                        "top",
                        functions=(
                            lambda x: np.interp(x, t, np.arange(len(t))),
                            lambda x: np.interp(x, np.arange(len(t)), t),
                        ),
                    )
                    top.xaxis.set_major_locator(MultipleLocator(10))
                    ax.set_xlabel("Seconds relative to strip start")
                    ax.grid(alpha=0.25)
                    ax.set_title(
                        f"{run['name']} · fiducial {beat['fid_s']:.4f} s", fontsize=11
                    )
            fig.tight_layout()
            path = out / f"{run['id']}_{page}.png"
            fig.savefig(path)
            plt.close(fig)
            files.append(path)
    return files
