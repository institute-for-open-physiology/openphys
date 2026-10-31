"""Offline Plotly review of saved annotations; never re-label or alter measurements."""

from pathlib import Path
import copy, json
import numpy as np
import pandas as pd
from plotly.offline import get_plotlyjs
from .delineators import LANDMARKS
from .repeatability import compare, signature


def payload(context, runs, astra):
    native = context["native"]
    t = native.relative_s.to_numpy()
    v = native.ecg_uv.to_numpy() / 1000
    methods = []
    for run in ([astra] if astra else []) + runs:
        beats = []
        for b in run["beats"]:
            if not 0 <= b["fid_s"] < 10:
                continue
            marks = {}
            for key, x in b["marks"].items():
                if x is None or not t[0] <= x <= t[-1]:
                    continue
                nearest = int(np.argmin(abs(t - x)))
                bounds = b.get("ranges", {}).get(key)
                marks[key] = {
                    "seconds": x,
                    "mv": float(np.interp(x, t, v)),
                    "nearest_native_index": nearest,
                    "native_time_s": float(t[nearest]),
                    "range": bounds,
                }
            row = next(
                (
                    m
                    for m in run.get("measurements", {}).get("beats", [])
                    if m["fid_s"] == b["fid_s"]
                ),
                {},
            )
            beats.append(
                {
                    "fid_s": b["fid_s"],
                    "marks": marks,
                    "confidence": b.get("confidence", "tool output"),
                    "notes": b.get("notes", []) + b.get("landmark_issues", []),
                    "measurements": {
                        k: row.get(k)
                        for k in ["PR_ms", "QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
                    },
                }
            )
        methods.append(
            {
                "id": run["id"],
                "name": run["name"],
                "status": run["status"],
                "beats": beats,
                "summary": run.get("measurements", {}).get("summary", {}),
                "warnings": run.get("warnings", []),
                "error": run.get("error"),
                "preprocessing": run["preprocessing"],
            }
        )
    return {
        "times": t.tolist(),
        "millivolts": v.tolist(),
        "methods": methods,
        "landmarks": LANDMARKS,
        "native_hz": context["fs"],
    }


def extra_repeats(out, context):
    """Only expose older annotations after binding the identical-input audit to this report."""
    out = Path(out).resolve()
    path = out / "repeatability.json"
    paths = []
    if path.exists():
        evidence = json.loads(path.read_text())
        paths = [Path(r["path"]).resolve() for r in evidence["runs"]]
        if not paths or paths[-1] != out:
            raise ValueError("Repeated annotations belong to a different target report")
        compare(paths)
        paths = paths[:-1]
    comparisons = out / "annotation_comparisons.json"
    if comparisons.exists():
        evidence = json.loads(comparisons.read_text())
        if Path(evidence["target"]).resolve() != out:
            raise ValueError(
                "Annotation comparisons belong to a different target report"
            )
        paths.extend(Path(p).resolve() for p in evidence["references"])
    paths = list(dict.fromkeys(p for p in paths if p != out))
    if not paths:
        return []
    current = signature(out)
    d = context["native"]
    result = []
    for root in paths:
        saved = pd.read_csv(root / "native.csv")
        if (
            len(saved) != len(d)
            or not np.array_equal(saved.ecg_uv, d.ecg_uv)
            or not np.allclose(saved.relative_s, d.relative_s, rtol=0, atol=1e-12)
        ):
            raise ValueError("Plot waveform differs from repeated-annotation source")
        reference = signature(root)
        if reference["model"] != current["model"]:
            raise ValueError("Cannot overlay annotations from a different model")
        # Display-only comparison on identical native samples; differences are labelled.
        differences = [
            label
            for label, key in [
                ("different workflow", "stages"),
                ("different effort", "reasoning_effort"),
                ("different prompt", "prompt_sha256"),
            ]
            if reference[key] != current[key]
        ]
        a = copy.deepcopy(json.loads((root / "astra.json").read_text()))
        a["id"] = "astra_" + root.name
        a["name"] = (
            "Astra " + root.name + " · " + (", ".join(differences) or "same prompt")
        )
        result.append(a)
    return result


def html(context, runs, astra, out):
    data = payload(context, runs + extra_repeats(out, context), astra)
    encoded = json.dumps(data, allow_nan=False).replace("<", "\\u003c")
    controls = """<section id="annotation-review" class="no-print">
      <div class="section-title"><h2>ECG and annotations</h2><span>Original samples · seconds / mV · interactive Plotly</span></div>
      <div class="plot-controls"><label>Annotation method<select id="plot-method"></select></label><label>Compare with<select id="plot-compare"><option value="">No overlay</option></select></label><label>View<select id="plot-beat"><option value="full">Full 10 seconds</option></select></label></div>
      <div class="actions"><button id="plot-prev">Previous beat</button><button id="plot-next">Next beat</button><button id="plot-full">Full strip</button><button id="plot-qt">Compare QT endings</button><label class="inline-check"><input type="checkbox" id="plot-uncertainty" checked>Show visual uncertainty</label></div>
      <p class="small">Hover a mark for its name, exact time, native sample and annotation notes. Click a mark or choose a beat to inspect it. Drag to zoom; double-click to reset. Legend clicks toggle landmarks. Hollow orange marks are the comparison.</p>
      <div id="ecg-plot" aria-label="Interactive ECG with visible annotation landmarks"></div>
      <p id="plot-summary" role="status"></p><div id="plot-beat-notes"></div>
      <p class="small">QT = T end − QRS onset. Moving T end later lengthens QT directly; QRS offset changes QRS/JT, not QT. Visual ranges are not confidence intervals. <a href="https://www.ahajournals.org/doi/10.1161/CIRCULATIONAHA.108.191096">AHA/ACCF/HRS measurement definitions</a>. Method selection changes the plot only; the original Astra medians and interpretation below remain unchanged.</p>
    </section>"""
    return (
        controls
        + "<script>"
        + get_plotlyjs()
        + "</script><script>const ecgPlotData="
        + encoded
        + ";</script><script>"
        + Path(__file__).with_name("interactive.js").read_text()
        + "</script>"
    )
