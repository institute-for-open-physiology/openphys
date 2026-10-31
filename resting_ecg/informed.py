"""Machine review, annotation and visual revision within one replayed conversation."""

from pathlib import Path
import json
from .astra import request, normalize
from .core import write_json, digest
from .figures import annotated_images
from .measures import audit_run, measure


def rounded(value):
    """0.1 ms / 0.1 µV precision; the native sample spacing is ~7.7 ms."""
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {k: rounded(v) for k, v in value.items()}
    if isinstance(value, list):
        return [rounded(v) for v in value]
    return value


def machine_evidence(runs, native, out=None):
    """Every machine beat and measurement inside the waveform Astra is shown.

    Delineators run on ~70 s of real context; beats outside the supplied samples and
    images cannot be inspected, so sending them only dilutes the request.
    """
    lo, hi = float(native.relative_s.iloc[0]), float(native.relative_s.iloc[-1])
    result = []
    for run in runs:
        item = {
            k: run.get(k)
            for k in [
                "id",
                "name",
                "status",
                "version",
                "preprocessing",
                "warnings",
                "error",
                "landmark_issue_count",
            ]
        }
        if item["error"] and out is not None:
            # Tool errors can embed this run's local paths; keep the request run-independent.
            item["error"] = item["error"].replace(str(out), "<run>")
        item["beats"] = [b for b in run["beats"] if lo <= b["fid_s"] <= hi]
        measurements = run.get("measurements", {})
        item["measurement_summary"] = measurements.get("summary", {})
        item["measurements_per_beat"] = [
            {k: v for k, v in b.items() if k != "bounds"}
            for b in measurements.get("beats", [])
        ]
        result.append(rounded(item))
    return result


def prior_turns(root, images):
    return [
        {
            "role": "user",
            "text": (root / "prompt.md").read_text()
            + "\n\n"
            + (root / "input.txt").read_text(),
            "images": list(map(str, images)),
        },
        {
            "role": "assistant",
            "text": (root / "response.txt").read_text(),
            "images": [],
        },
    ]


def annotate_informed(context, runs, config, panels, out):
    out = Path(out)
    prompts = Path(__file__).with_name("prompts")
    native = context["native"]
    csv = native[["relative_s", "ecg_uv"]].copy()
    csv.columns = ["seconds", "microvolts"]
    csv.index.name = "sample_index"
    evidence = machine_evidence(runs, native, out)
    write_json(out / "machine_evidence.json", evidence)
    text = (
        f"Native effective sample rate: {context['fs']:.6f} Hz. Selected view: [0,10) seconds. Machine landmarks are seconds on this same time axis. Images show original native samples; labelled machine figures show representative beats, and JSON contains every machine beat within the supplied samples.\n\n"
        + csv.to_csv(float_format="%.6f")
        + "\nMachine annotations and measurements:\n"
        + json.dumps(evidence, allow_nan=False, separators=(",", ":"))
    )
    machine_panels = annotated_images(context, runs, out / "machine_figures")
    initial_images = list(panels) + machine_panels
    review_dir = out / "astra_machine_review"
    review = request(
        config,
        (prompts / "review_machines.md").read_text(),
        text,
        initial_images,
        review_dir,
    )
    ids = [r["id"] for r in runs]
    if (
        not isinstance(review.get("paragraph"), str)
        or not isinstance(review.get("methods"), list)
        or sorted(m.get("id", "") for m in review["methods"]) != sorted(ids)
        or not all(isinstance(m.get("assessment"), str) for m in review["methods"])
        or not isinstance(review.get("disagreements"), list)
        or not all(isinstance(s, str) for s in review["disagreements"])
    ):
        raise ValueError(
            "Machine review must assess every supplied method, including failures"
        )
    history = prior_turns(review_dir, initial_images)
    draft_dir = out / "astra_draft"
    draft_raw = request(
        config,
        (prompts / "annotate.md").read_text(),
        "Produce your Astra annotation of this ECG using the waveform and machine delineations we just reviewed.",
        [],
        draft_dir,
        history=history,
    )
    draft = audit_run(normalize(draft_raw, native))
    draft["id"], draft["name"] = "astra_draft", "Astra draft (before plot review)"
    draft["preprocessing"] = (
        "Unfiltered native ECG; visual annotation after reviewing every machine "
        "delineation, before reviewing plots of this draft."
    )
    draft["measurements"] = measure(draft, context)
    write_json(out / "astra_draft.json", draft)
    draft_panels = annotated_images(
        context, [draft], out / "draft_figures", every_beat=True
    )
    history += prior_turns(draft_dir, [])
    final_dir = out / "astra_annotation"
    final_raw = request(
        config,
        (prompts / "review_annotation.md").read_text(),
        "These plots show your draft on the original samples.\n"
        + json.dumps({"draft_measurements": draft["measurements"]}, allow_nan=False),
        draft_panels,
        final_dir,
        history=history,
    )
    if (
        not isinstance(final_raw.get("review_notes"), str)
        or not final_raw["review_notes"].strip()
    ):
        raise ValueError("Final annotation needs a public visual-review note")
    final = audit_run(normalize(final_raw, native))
    final["review_notes"] = final_raw["review_notes"]
    final["measurements"] = measure(final, context)
    write_json(
        out / "annotation_workflow.json",
        {
            "workflow": "machine-informed-v1",
            "stages": ["astra_machine_review", "astra_draft", "astra_annotation"],
            "machine_ids": ids,
            "machine_evidence_sha256": digest(out / "machine_evidence.json"),
            "review_notes": final_raw["review_notes"],
            "prior_astra_runs_supplied": False,
            "note": "All machine evidence and this run's prior public responses are replayed explicitly. Final annotation reviewed against plots of the draft; this is not clinical validation.",
        },
    )
    return final
