"""Extract the de-identified data behind docs/images/pacing_spikes_seca_331_12lead.png.

Run privately by the maintainer against the decoded seca 331 USB recording (not public). Writes:

  data/pacing/ecg_window.npz  native 32 kHz samples (int16 ADC counts, 8 recorded channels) for the
                              three paced beats shown in the figure plus 0.5 s margin on each side
  data/pacing/beats.csv       one row per paced beat of the whole 10-minute recording (608 beats);
                              times are seconds from the start of the recording
  data/pacing/metadata.json   sensor, units, channel order, figure parameters

Only relative time is written: no dates, clock times, names, device serials or record identifiers.

Usage:
  python extract/pacing_extract.py --native-counts <native_counts.npy> --beats <beats.csv> [--out data/pacing]
"""
import argparse
import csv
import json
from pathlib import Path

import numpy as np

FS, UV_PER_COUNT = 32000, 12.208
CHANNELS = ["V1", "V2", "V3", "V4", "V5", "V6", "I", "II"]
FIRST_BEAT = 204       # first of 3 paced beats with no off-spike artefact in any lead
LEAD_IN, WIDTH = 0.27, 2.95
MARGIN = 0.5
BEAT_COLUMNS = ["beat", "pulse_s", "pulse_native_sample", "s_fiducial_s", "previous_rr_s", "pulse_to_s_ms",
                "pulse_vector_cosine", "qrs_template_cosine", "pulse_hf_amplitude_mv",
                "chest_channels_over_0_5mv", "assessment"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--native-counts", required=True, help="decoded int32 samples, shape (n, 8), 32 kHz")
    ap.add_argument("--beats", required=True, help="beat table of the pacing analysis (CSV)")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "pacing"))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    x = np.load(args.native_counts, mmap_mode="r")
    assert x.shape[1] == len(CHANNELS), x.shape
    beats = list(csv.DictReader(open(args.beats)))
    pulses = {int(b["beat"]): int(b["pulse_native_sample"]) for b in beats}

    t0 = pulses[FIRST_BEAT] / FS - LEAD_IN
    w0 = int(t0 * FS) - int(MARGIN * FS)
    w1 = int((t0 + WIDTH) * FS) + int(MARGIN * FS)
    win = np.asarray(x[w0:w1])
    assert np.abs(win).max() < 2 ** 15, "window does not fit int16"
    np.savez_compressed(out / "ecg_window.npz", counts=win.astype(np.int16),
                        start_sample=np.int64(w0), fs=np.int64(FS), uv_per_count=np.float64(UV_PER_COUNT),
                        channels=np.array(CHANNELS))

    with open(out / "beats.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=BEAT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for b in beats:
            w.writerow(b)

    meta = {
        "sensor": "seca CT331 (custo cardio 300 BT), USB acquisition",
        "context": "resting 12-lead recording (about 10 minutes) of a person with a cardiac pacemaker; "
                   "every QRS is preceded by a ventricular pacing spike",
        "sampling_rate_hz": FS,
        "units": "ADC counts; multiply by uv_per_count for microvolts",
        "uv_per_count": UV_PER_COUNT,
        "filtering": "none (native samples as decoded from the device stream)",
        "recorded_channels": CHANNELS,
        "derived_limb_leads": {"III": "II - I", "aVR": "-(I + II) / 2", "aVL": "I - II / 2", "aVF": "II - I / 2"},
        "time_reference": "seconds / samples from the start of the recording; no wall-clock time is included",
        "window": {"start_sample": w0, "n_samples": int(win.shape[0]),
                   "note": "only this window of the recording is published (the figure's 2.95 s plus 0.5 s "
                           "margin each side); the full recording is not included"},
        "n_paced_beats": len(beats),
        "figure": {"first_beat": FIRST_BEAT, "zoom_beat": FIRST_BEAT + 1, "lead_in_s": LEAD_IN,
                   "width_s": WIDTH, "zoom_ms": [-3, 8]},
    }
    (out / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"window {win.shape} -> {out}; {len(beats)} beats")


if __name__ == "__main__":
    main()
