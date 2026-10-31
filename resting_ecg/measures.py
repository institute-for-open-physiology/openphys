"""Deterministic, auditable single-lead measurements; no normality-based exclusions."""

import numpy as np

PAIRS = {
    "P_ms": ("P_on", "P_off"),
    "PR_ms": ("P_on", "QRS_on"),
    "QRS_ms": ("QRS_on", "QRS_off"),
    "QT_ms": ("QRS_on", "T_off"),
    "JT_ms": ("QRS_off", "T_off"),
    "T_ms": ("T_on", "T_off"),
}


def audit_run(run):
    """Preserve upstream marks, but exclude impossible phase/beat associations from use."""
    beats = run["beats"]
    count = 0
    for i, b in enumerate(beats):
        raw = b.get("raw_marks", b["marks"]).copy()
        b["raw_marks"] = raw
        b["marks"] = raw.copy()
        issues = []
        fid = b["fid_s"]
        left = beats[i - 1]["fid_s"] if i else -np.inf
        right = beats[i + 1]["fid_s"] if i + 1 < len(beats) else np.inf
        on, off = raw.get("QRS_on"), raw.get("QRS_off")

        def reject(key, reason):
            b["marks"][key] = None
            issues.append(
                key + ": " + reason + "; upstream value retained in raw_marks"
            )

        for key, x in raw.items():
            if x is None:
                continue
            if not np.isfinite(x) or not left < x < right:
                reject(key, "outside own QRS cycle")
                continue
            if key == "QRS_on" and x > fid:
                reject(key, "after fiducial")
            elif key == "QRS_off" and x < fid:
                reject(key, "before fiducial")
            elif key.startswith("P_") and x >= (on if on is not None else fid):
                reject(key, "not before QRS onset")
            elif key.startswith("T_") and x <= (off if off is not None else fid):
                reject(key, "not after QRS offset")
            elif key in ["Q_peak", "R_peak", "S_peak"] and (
                (on is not None and x < on) or (off is not None and x > off)
            ):
                reject(key, "outside annotated QRS")
        for keys in [
            ("P_on", "P_peak", "P_off"),
            ("Q_peak", "R_peak", "S_peak"),
            ("T_on", "T_peak", "T_off"),
        ]:
            known = [(k, b["marks"][k]) for k in keys if b["marks"][k] is not None]
            if any(x[1] >= y[1] for x, y in zip(known, known[1:])):
                for key, _ in known:
                    reject(key, "contradictory within-wave order")
        b["landmark_issues"] = issues
        count += len(issues) if 0 <= fid < 10 else 0
    run["landmark_issue_count"] = count
    run["coverage"] = {
        k: sum(b["marks"][k] is not None for b in beats if 0 <= b["fid_s"] < 10)
        for k in run.get("coverage", {})
    }
    return run


def corrections(qt_ms, rr_ms):
    if qt_ms is None or rr_ms is None or qt_ms <= 0 or rr_ms <= 0:
        return dict.fromkeys(["QTcB_ms", "QTcF_ms", "QTcFramingham_ms", "QTcHodges_ms"])
    rr = rr_ms / 1000
    return {
        "QTcB_ms": qt_ms / np.sqrt(rr),
        "QTcF_ms": qt_ms / np.cbrt(rr),
        "QTcFramingham_ms": qt_ms + 154 * (1 - rr),
        "QTcHodges_ms": qt_ms + 1.75 * (60 / rr - 60),
    }


def summary(values):
    v = [float(x) for x in values if x is not None and np.isfinite(x)]
    return {
        "n": len(v),
        "median": float(np.median(v)) if v else None,
        "min": min(v) if v else None,
        "max": max(v) if v else None,
    }


def measure(run, context):
    d = context["context"]
    t = d.relative_s.to_numpy()
    v = d.ecg_uv.to_numpy() / 1000
    rows = []
    previous = None
    for beat in run["beats"]:
        fid = beat["fid_s"]
        marks = beat["marks"]
        r = {
            "fid_s": fid,
            "issues": list(beat.get("notes", [])) + beat.get("landmark_issues", []),
            "bounds": {},
        }
        rr = (
            (fid - previous) * 1000 if previous is not None and fid > previous else None
        )
        previous = fid
        if not 0 <= fid < 10:
            continue
        r["RR_ms"] = rr
        r["HR_bpm"] = 60000 / rr if rr else None
        q_on, q_off = marks.get("QRS_on"), marks.get("QRS_off")
        q_order = (
            q_on is not None
            and q_off is not None
            and q_on <= fid <= q_off
            and q_on < q_off
        )
        if not q_order:
            r["issues"].append(
                "QRS boundaries missing or do not enclose fiducial; dependent intervals unavailable"
            )
        for name, (a, b) in PAIRS.items():
            x, y = marks.get(a), marks.get(b)
            valid = x is not None and y is not None and x < y
            if name in ["PR_ms", "QRS_ms", "QT_ms", "JT_ms"]:
                valid = valid and q_order
            if name in ["QT_ms", "JT_ms"] and valid:
                valid = y > q_off
            r[name] = (y - x) * 1000 if valid else None
            if x is not None and y is not None and not valid:
                r["issues"].append("Invalid landmark order for " + name)
            bounds = beat.get("ranges", {})
            if valid and bounds.get(a) is not None and bounds.get(b) is not None:
                lower = (bounds[b][0] - bounds[a][1]) * 1000
                upper = (bounds[b][1] - bounds[a][0]) * 1000
                if lower <= 0:
                    r["issues"].append(
                        name
                        + ": landmark uncertainty overlaps; interval bounds indeterminate"
                    )
                else:
                    r["bounds"][name] = [lower, upper]
        r.update(corrections(r["QT_ms"], rr))
        if rr and r["bounds"].get("QT_ms"):
            for correction in [
                "QTcB_ms",
                "QTcF_ms",
                "QTcFramingham_ms",
                "QTcHodges_ms",
            ]:
                r["bounds"][correction] = [
                    corrections(qt, rr)[correction] for qt in r["bounds"]["QT_ms"]
                ]
        # PR segment only. Never substitute an S/R deflection as an isoelectric reference.
        poff = marks.get("P_off")
        baseline = None
        if poff is not None and q_on is not None and poff < q_on:
            mask = (t >= poff + 0.008) & (t <= q_on - 0.008)
            if mask.sum() >= 2:
                baseline = float(np.median(v[mask]))
        r["baseline_mV"] = baseline
        if baseline is None:
            r["issues"].append("PR baseline unavailable; voltage measurements missing")
        for key in ["P_peak", "Q_peak", "R_peak", "S_peak", "T_peak"]:
            x = marks.get(key)
            r[key + "_mV"] = (
                float(v[np.argmin(abs(t - x))] - baseline)
                if x is not None and baseline is not None and t[0] <= x <= t[-1]
                else None
            )
        for name, offset in [("J_mV", 0), ("ST60_mV", 0.060), ("ST80_mV", 0.080)]:
            r[name] = None
            if q_order and baseline is not None:
                x = q_off + offset
                ton = marks.get("T_on")
                # Do not label a point within the annotated T wave as ST.
                if offset and (ton is None or x >= ton):
                    r["issues"].append(
                        name + ": T onset unknown or point enters T wave"
                    )
                    continue
                if t[0] <= x <= t[-1]:
                    r[name] = float(v[np.argmin(abs(t - x))] - baseline)
        rows.append(r)
    keys = list(PAIRS) + [
        "RR_ms",
        "HR_bpm",
        "QTcB_ms",
        "QTcF_ms",
        "QTcFramingham_ms",
        "QTcHodges_ms",
        "P_peak_mV",
        "Q_peak_mV",
        "R_peak_mV",
        "S_peak_mV",
        "T_peak_mV",
        "J_mV",
        "ST60_mV",
        "ST80_mV",
    ]
    stats = {k: summary([r[k] for r in rows]) for k in keys}
    rr = [r["RR_ms"] for r in rows if r["RR_ms"] is not None]
    return {
        "units": "Intervals ms, heart rate bpm, voltage mV",
        "beats": rows,
        "summary": stats,
        "beat_count": len(rows),
        "rate_from_mean_RR_bpm": 60000 / np.mean(rr) if rr else None,
        "rr_range_ms": max(rr) - min(rr) if rr else None,
        "method_notes": [
            "Medians across visible beats; complete surrounding preceding fiducial used for first RR when available.",
            "QT correction per beat uses its preceding RR; no QT hysteresis correction. Bazett QT/sqrt(RR), Fridericia QT/cbrt(RR); QT in ms, RR in seconds.",
            "Framingham QT+154*(1-RR); Hodges QT+1.75*(HR-60), in ms. QTc uncertainty bounds hold RR fixed.",
            "Voltages from raw native samples relative to median PR segment (P offset+8 ms to QRS onset−8 ms). No lead-axis or ST diagnostic claim.",
            "ST60/ST80 unavailable when point reaches annotated T onset or T onset is unknown. J uses nearest native sample.",
            "No normal-range filtering or ectopy removal. Invalid/missing boundaries remain missing; disagreement is not error versus a reference.",
            "Single-lead local intervals, not 12-lead global intervals. Native temporal spacing is ~7.7 ms; visual uncertainty can exceed this.",
        ],
    }
