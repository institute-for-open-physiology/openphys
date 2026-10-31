"""Optional local validation supplement. Prior outputs never enter annotation requests."""

from pathlib import Path
from html import escape
import argparse, json, os
import numpy as np
import pandas as pd
from .core import write_json, digest


STAGES = ["astra_machine_review", "astra_draft", "astra_annotation"]


def signature(root):
    """Settings and source inputs of a run's Astra annotation requests.

    Machine-informed runs replay their own earlier responses and draft plots into later
    stages, so only the first stage's data/images and every stage's instructions are inputs.
    """
    root = Path(root)
    stages = STAGES if (root / STAGES[0]).is_dir() else STAGES[-1:]
    manifests = [
        json.loads((root / s / "request_manifest.json").read_text()) for s in stages
    ]
    first = manifests[0]
    result = {
        "stages": stages,
        "model": first["model"],
        "reasoning_effort": first["reasoning_effort"],
        "image_hashes": first["image_hashes"],
        "input_sha256": first["input_sha256"],
        "prompt_sha256": [m["prompt_sha256"] for m in manifests],
    }
    if len(stages) > 1:
        # The draft request text is fixed; plot-review text and draft plots come from code.
        provenance = root / "provenance.json"
        code = (
            json.loads(provenance.read_text())["source_files"]
            if provenance.exists()
            else {}
        )
        result["draft_input_sha256"] = manifests[1]["input_sha256"]
        result["annotation_code_sha256"] = {
            k: code.get(k) for k in ["informed.py", "figures.py"]
        }
    return result


def compare(runs):
    baseline = None
    rows = []
    for run in runs:
        root = Path(run)
        current = signature(root)
        if baseline is None:
            baseline = current
        if current != baseline:
            raise ValueError(
                "Repeatability requires identical model settings, prompt, native input and image hashes"
            )
        a = json.loads((root / "astra.json").read_text())
        metadata = json.loads(
            (root / "astra_annotation/response_metadata.json").read_text()
        )
        if metadata["model"] != baseline["model"]:
            raise ValueError("Response model differs")
        rows.append(
            {
                "run": root.name,
                "path": str(root.resolve()),
                "response_id": metadata["response_id"],
                **{
                    k: a["measurements"]["summary"][k]["median"]
                    for k in ["QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
                },
            }
        )
    if len(rows) < 2 or len({r["response_id"] for r in rows}) != len(rows):
        raise ValueError("Need at least two distinct fresh responses")
    ranges = {
        k: [
            min(r[k] for r in rows if r[k] is not None),
            max(r[k] for r in rows if r[k] is not None),
        ]
        for k in ["QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
        if any(r[k] is not None for r in rows)
    }
    d = pd.read_csv(Path(runs[0]) / "native.csv")
    spacing = float(np.median(np.diff(d.relative_s)) * 1000)
    unresolved = any(
        hi - lo > 2 * spacing
        for k, (lo, hi) in ranges.items()
        if k in ["QRS_ms", "QT_ms"]
    )
    return {
        "identical_inputs": baseline,
        "runs": rows,
        "median_ranges_ms": ranges,
        "native_sample_spacing_ms": spacing,
        "unresolved": unresolved,
        "gate": "Prototype repeatability gate: QRS or QT median spread greater than two native samples. This is an engineering check, not a clinical threshold or accuracy estimate.",
    }


def attach(report_run, runs):
    root = Path(report_run).resolve()
    if not runs or Path(runs[-1]).resolve() != root:
        raise ValueError(
            "Report must be the final/current run in the comparison; unrelated evidence cannot be attached"
        )
    result = compare(runs)
    write_json(root / "repeatability.json", result)
    rows = "".join(
        '<tr><td><a href="'
        + escape(
            os.path.relpath(Path(r["path"]) / "astra_annotation/response.txt", root)
        )
        + '">'
        + escape(r["run"])
        + "</a></td>"
        + "".join(
            "<td>" + ("—" if r[k] is None else f"{r[k]:.0f}") + "</td>"
            for k in ["QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
        )
        + "</tr>"
        for r in result["runs"]
    )
    section = (
        '<section id="repeatability" class="alert"><h2>Repeated Astra annotations need review</h2><p>'
        + str(len(result["runs"]))
        + " independent runs used identical instructions, native samples, first-stage images, model and effort. Their T-end choices differed. The numbers below are each run’s annotation-derived medians; they are not validated clinical estimates. The main printout uses the latest run, without choosing the most reassuring answer.</p><table><thead><tr><th>Fresh request</th><th>QRS, ms</th><th>QT, ms</th><th>QTcF, ms</th><th>QTcB, ms</th></tr></thead><tbody>"
        + rows
        + '</tbody></table><p><strong>Resolve the landmark disagreement before using these values for an exercise decision.</strong> <a href="repeatability.json">Full comparison and matching-input evidence</a>.</p></section>'
    )
    html = (root / "report.html").read_text()
    if 'id="repeatability"' in html:
        raise ValueError(
            "Supplement already attached; render the base report again before updating"
        )
    html = html.replace(
        '<section class="printout">', section + '<section class="printout">', 1
    )
    # Persist the validation gate without changing the original model response.
    gate = (
        "<script>runData.repeatability_unresolved="
        + str(result["unresolved"]).lower()
        + ";reviewStatus();</script>"
    )
    html = html.replace("</body>", gate + "</body>")
    (root / "report.html").write_text(html)
    write_json(
        root / "repeatability_provenance.json",
        {
            "script_sha256": digest(__file__),
            "report_sha256": digest(root / "report.html"),
            "input_runs": [str(Path(p).resolve()) for p in runs],
        },
    )
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report", type=Path, required=True)
    p.add_argument("runs", type=Path, nargs="+")
    a = p.parse_args()
    result = attach(a.report, a.runs)
    print(json.dumps(result["median_ranges_ms"], indent=2))


if __name__ == "__main__":
    main()
