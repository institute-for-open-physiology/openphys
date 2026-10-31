"""Shared helpers for the Polar H10 README figures (pvc_a, pvc_b, hyperacute_t).

Everything here works on the de-identified extracts in ../data/<id>/:
  ecg.csv        time_s,ecg_uV   raw single-lead H10 ECG (unfiltered, true polarity), time in seconds
                                 relative to the start of the analysed window (H10 device clock)
  metadata.json  sensor / sampling rate / units / context + the delineation the figure needs

The QRS geometry below (sub-sample R/S, QRS onset, J point, T peak) is the method used for the
README figures: QRS onset = start of the steepest upstroke into R, J point = end of the steep
S upstroke, both walked out to 18 % of the peak slope; R and S timed by a parabolic vertex fit.
"""
import csv
import json
import os

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.normpath(os.path.join(HERE, "..", "data"))
IMAGES = os.path.normpath(os.path.join(HERE, "..", "..", "..", "docs", "images"))

WIDE_QRS_MS = 120.0          # clinical threshold: QRS >= 120 ms = wide

# palette
ORANGE, GREY = "#f6b24a", "#e9ebee"            # ventricular QRS core / sinus QRS band
ORANGE_WIDE, ORANGE_EDGE = "#ffe6c2", "#d98a1f"  # ventricular complex -> end of its T wave
REDR, BLUES = "#c0392b", "#2c6fbb"            # R / S marks
ORANGE_T = "#d97a00"                          # T peak
COUPLE = "#6c3483"                            # coupling arrow (last normal S -> ventricular S)
DASH = "#9aa0a6"


def load_ecg(fig_id, name="ecg.csv"):
    """-> (t, raw) from data/<fig_id>/<name>."""
    t, raw = [], []
    with open(os.path.join(DATA, fig_id, name)) as f:
        for row in csv.DictReader(f):
            t.append(float(row["time_s"])); raw.append(float(row["ecg_uV"]))
    return np.asarray(t), np.asarray(raw)


def load_meta(fig_id):
    return json.load(open(os.path.join(DATA, fig_id, "metadata.json")))


def load(fig_id):
    """-> (t, raw, meta) for data/<fig_id>/."""
    t, raw = load_ecg(fig_id)
    return t, raw, load_meta(fig_id)


def load_csv(fig_id, name):
    with open(os.path.join(DATA, fig_id, name)) as f:
        return list(csv.DictReader(f))


def output_path(argv, png_name):
    """Optional first CLI argument overrides the default docs/images/<png_name>."""
    return argv[1] if len(argv) > 1 else os.path.join(IMAGES, png_name)


class Geometry:
    """Sample-level QRS geometry on one raw trace."""

    def __init__(self, raw, t, fs):
        self.raw, self.t, self.fs = raw, t, fs

    def subpeak(self, i, sign=+1):
        """Parabolic-vertex refinement of a local extremum at sample i (sign -1 for a minimum)."""
        raw = self.raw
        if i <= 0 or i >= len(raw) - 1:
            return float(i)
        y0, y1, y2 = sign * raw[i - 1], sign * raw[i], sign * raw[i + 1]
        den = y0 - 2 * y1 + y2
        return i + float(np.clip(0.5 * (y0 - y2) / den if den != 0 else 0.0, -1, 1))

    def tt(self, fidx):
        """Fractional sample index -> time (linear interpolation of the device clock)."""
        t = self.t
        i = min(int(np.floor(fidx)), len(t) - 2)
        return t[i] + (fidx - i) * (t[i + 1] - t[i])

    def j_point(self, sx, max_ms=110):
        """QRS offset = end of the steep S upstroke (slope falls below 18 % of its peak)."""
        raw, fs = self.raw, self.fs
        end = min(len(raw) - 1, sx + int(max_ms / 1000.0 * fs))
        if end - sx < 3:
            return sx + 1
        up = np.diff(raw[sx:end + 1])
        pk = int(np.argmax(up))
        if up[pk] <= 0:
            return sx + 1
        thr = 0.18 * up[pk]
        for j in range(pk, len(up)):
            if up[j] < thr:
                return sx + j
        return sx + len(up)

    def q_onset(self, rx, max_ms=80):
        """QRS onset = start of the steepest rise into R, walked back to 18 % of its slope."""
        raw, fs = self.raw, self.fs
        start = max(1, rx - int(max_ms / 1000.0 * fs))
        up = np.diff(raw[start:rx + 1])
        if len(up) < 2 or up.max() <= 0:
            return max(0, rx - 1)
        pk = int(np.argmax(up))
        thr = 0.18 * up[pk]
        j = pk
        while j > 0 and up[j] >= thr:
            j -= 1
        return start + j + 1

    def beat_from_segment(self, on0, off0):
        """R / S / onset / J point for a beat whose QRS segment (sample indices) is known."""
        raw, t = self.raw, self.t
        sx = on0 + int(np.argmin(raw[on0:off0 + 1]))
        rx = on0 + int(np.argmax(raw[on0:max(sx, on0 + 1) + 1]))
        on, roff = self.q_onset(rx), self.j_point(sx)
        return dict(Sx=sx, Rx=rx, on=on, roff=roff, Rt=self.tt(self.subpeak(rx, +1)),
                    St=self.tt(self.subpeak(sx, -1)), dur=(t[roff] - t[on]) * 1000.0)


def label_box(txt):
    """White box behind a label so it stays legible where it crosses dashed verticals."""
    txt.set_bbox(dict(facecolor="white", edgecolor="none", pad=1.2))
    txt.set_zorder(7)
