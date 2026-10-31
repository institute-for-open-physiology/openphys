"""Extract the de-identified data behind docs/images/qrs_axis_exercise_seca_331.png.

Run privately by the maintainer against the (non-public) seca 331 WFDB records and the beat arrays of the
exercise analysis. Needs wfdb in addition to numpy. Writes:

  data/qrs_axis/strips.npz    raw 12-lead samples (int32 digital units, no filtering) for the two 3-beat /
                              8-beat windows shown at the top of the figure, with 2 s of padding on each side
                              (for the display filter), and the beat fiducials inside each window
  data/qrs_axis/beats.npz     every beat that enters the two mean heartbeats (523 lying down, 270 candidates
                              while running of which 256 are included), -300..+500 ms around each beat
                              fiducial, leads I, II, V1-V6, rounded to the device's digital unit
  data/qrs_axis/metadata.json sensor, units, lead order, electrode placement, analysis parameters

Only relative time is written: no dates, clock times, names, birth dates, device serials or record identifiers.

Usage:
  python extract/qrs_axis_extract.py --sync-dir <analysis/out/sync> \
      --rest-record <WFDB record, lying down> --run-record <WFDB record, exercise> [--out data/qrs_axis]
"""
import argparse
import gzip
import json
from pathlib import Path

import numpy as np
import wfdb

FS = 2000
LEADS = ["I", "II", "III", "aVR", "aVL", "aVF", "V1", "V2", "V3", "V4", "V5", "V6"]
INDEPENDENT = [0, 1, 6, 7, 8, 9, 10, 11]                    # I, II, V1-V6
REST_FILE, RUN_FILE = "supine_ML_comparison_beats.npz", "rate_bucket_162_172_beats.npz"
REST_K, RUN_K = 502, 120       # first beat of each strip: the calmest stretch (smallest beat-to-mean mismatch)
PAD = 2 * FS


def load_ledger(path):
    return json.loads(gzip.decompress(Path(path).read_bytes()))["beats"]


def strip(record, beats, skey, k, win):
    """Window starting 0.40 RR before beat k; win = None -> 3 beats long (rest panel sets the length)."""
    rr = beats[k + 1][skey] - beats[k][skey]
    a = int(beats[k][skey] - 0.40 * rr)
    if win is None:
        win = int(beats[k + 2][skey] + 0.75 * rr) - a
    b_ = a + win
    inside = [b for b in beats if a <= b[skey] < b_]
    assert all(b.get("included", True) for b in inside), "beat outside the average"
    rec = wfdb.rdrecord(str(record), sampfrom=a - PAD, sampto=b_ + PAD, physical=False, return_res=32)
    assert rec.sig_name == LEADS, rec.sig_name
    assert all(v == 0 for v in rec.baseline)
    assert len(set(rec.adc_gain)) == 1
    fid = np.array([(b[skey] - a) / FS for b in inside])
    return rec.d_signal.astype(np.int32), float(rec.adc_gain[0]), fid, win


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sync-dir", required=True, help="analysis output directory with the beat arrays/ledgers")
    ap.add_argument("--rest-record", required=True, help="WFDB record name (path without extension), lying down")
    ap.add_argument("--run-record", required=True, help="WFDB record name (path without extension), exercise")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "qrs_axis"))
    args = ap.parse_args()
    S, out = Path(args.sync_dir), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    rest_ledger = load_ledger(S / "rate_bucket_supine.json.gz")
    run_ledger = load_ledger(S / "rate_bucket.json.gz")

    # --- raw strips ---
    rest_x, gain, rest_fid, win = strip(args.rest_record, rest_ledger, "source_sample", REST_K, None)
    run_x, gain2, run_fid, _ = strip(args.run_record, run_ledger, "sample", RUN_K, win)
    assert gain == gain2
    np.savez_compressed(out / "strips.npz", rest_counts=rest_x, run_counts=run_x,
                        rest_fiducials_s=rest_fid, run_fiducials_s=run_fid,
                        pad_samples=np.int64(PAD), window_samples=np.int64(win),
                        fs=np.int64(FS), adc_gain_per_mV=np.float64(gain), leads=np.array(LEADS))

    # --- beat arrays (analysis beats are sub-sample aligned floats; round to the device's digital unit) ---
    zr, ze = np.load(S / REST_FILE), np.load(S / RUN_FILE)
    assert np.allclose(zr["samples"], [b["source_sample"] for b in rest_ledger])
    assert np.allclose(ze["samples"], [b["sample"] for b in run_ledger])
    assert (ze["included"] == np.array([b["included"] for b in run_ledger])).all()
    assert np.array_equal(zr["time_ms"], ze["time_ms"])
    packed = {}
    for key, z in (("rest", zr), ("run", ze)):
        c = np.round(z["raw_mV"][..., INDEPENDENT] * gain).astype(np.int64)
        off = np.round(np.median(c, axis=1)).astype(np.int64)            # per beat, per lead DC offset
        d = c - off[:, None, :]
        assert np.abs(d).max() < 2 ** 15
        packed[f"{key}_counts"] = d.astype(np.int16)
        packed[f"{key}_offset"] = off.astype(np.int32)
    rr_prev = np.array([b["rr_prev_ms"] for b in run_ledger])
    np.savez_compressed(out / "beats.npz", time_ms=zr["time_ms"], adc_gain_per_mV=np.float64(gain),
                        leads=np.array([LEADS[i] for i in INDEPENDENT]),
                        run_included=ze["included"],
                        rest_hr_bpm=np.array([b["HR_bpm"] for b in rest_ledger]),
                        run_hr_bpm=np.array([b["hr"] for b in run_ledger]),
                        run_rr_prev_ms=rr_prev, **packed)

    meta = {
        "sensor": "seca CT331 (custo cardio 300 BT), 12-lead ECG",
        "electrode_placement": "torso (Mason-Likar) for both recordings, unchanged between them",
        "sampling_rate_hz": FS,
        "units": "digital units; divide by adc_gain_per_mV for millivolts (one unit = 0.763 uV)",
        "adc_gain_per_mV": gain,
        "time_reference": "relative only (seconds within each strip, milliseconds from each beat fiducial); "
                          "no wall-clock time is included",
        "strips": {
            "leads": LEADS,
            "missing_value": int(np.iinfo(np.int32).min),
            "filtering": "none; the figure applies a 0.5-100 Hz display band-pass",
            "layout": "pad_samples of padding, window_samples shown, pad_samples of padding",
            "rest": {"context": "lying down, about 62 bpm", "beats_in_window": int(len(rest_fid))},
            "run": {"context": "running, about 165 bpm", "beats_in_window": int(len(run_fid))},
        },
        "beats": {
            "leads": [LEADS[i] for i in INDEPENDENT],
            "derived_limb_leads": {"III": "II - I", "aVR": "-(I + II) / 2", "aVL": "I - II / 2",
                                   "aVF": "II - I / 2"},
            "encoding": "value = counts + offset[:, None, :] (int16 + int32 per-beat, per-lead offset)",
            "note": "the analysis beat arrays are aligned to sub-sample fiducials (interpolated); they are "
                    "published rounded to the device's digital unit (<= 0.38 uV error)",
            "rest": {"context": "lying down", "n_beats": int(zr["raw_mV"].shape[0])},
            "run": {"context": "running, heart rate 162-172 bpm bucket", "n_candidates": int(ze["raw_mV"].shape[0]),
                    "n_included": int(ze["included"].sum()),
                    "inclusion": "beats excluded from the average for noise / morphology QC are marked "
                                 "run_included = False"},
        },
        "analysis": {
            "pre_qrs_seed_reference_ms": [-85, -68],
            "spatial_velocity": "3rd-order Butterworth 100 Hz low-pass of the mean beat; leads I, II, V1-V6",
            "sv_baseline_ms": [-110, -70],
            "qrs_onset_threshold_uV_per_ms": 5,
            "qrs_offset_threshold_uV_per_ms": 20,
            "run_qrs_offset_override_ms": 33.0,
            "run_qrs_offset_override_reason": "the S upstroke bends gradually into the upsloping ST segment; "
                                              "the threshold marks the start of the bend (set by eye on review)",
            "voltage_reference": "mean of the samples 10 ms to 0.5 ms before the global QRS onset",
            "cross_check_gates_ms": {"rest": [-64, 45], "run": [-60, 50]},
        },
    }
    (out / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    print("strips", rest_x.shape, run_x.shape, "beats", zr["raw_mV"].shape, ze["raw_mV"].shape, "->", out)


if __name__ == "__main__":
    main()
