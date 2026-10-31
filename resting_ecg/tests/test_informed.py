import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from resting_ecg.astra import request
from resting_ecg.delineators import LANDMARKS
from resting_ecg.informed import annotate_informed, machine_evidence
from resting_ecg.measures import audit_run, measure

FS = 130


def context():
    t = np.arange(-30 * FS, 40 * FS) / FS
    d = pd.DataFrame({"relative_s": t, "ecg_uv": 800 * np.sin(2 * np.pi * t)})
    native = d[(d.relative_s >= -1.2) & (d.relative_s < 11.2)].reset_index(drop=True)
    return {"context": d, "native": native, "fs": FS}


def tool_runs(ctx):
    def beat(fid):
        marks = dict.fromkeys(LANDMARKS)
        marks.update(QRS_on=fid - 0.04, R_peak=fid, QRS_off=fid + 0.05)
        marks.update(T_peak=fid + 0.2, T_off=fid + 0.3)
        return {"fid_s": fid, "marks": marks, "notes": []}

    ok = {
        "id": "tool",
        "name": "Tool",
        "status": "ok",
        "version": "1",
        "preprocessing": "raw",
        "warnings": [],
        "error": None,
        "beats": [beat(-20.0), beat(0.5), beat(1.5), beat(2.5)],
        "coverage": dict.fromkeys(LANDMARKS, 0),
    }
    failed = {
        "id": "failed_tool",
        "name": "Failed tool",
        "status": "failed",
        "version": "1",
        "preprocessing": "raw",
        "warnings": ["upstream warning"],
        "error": "RuntimeError: octave crashed",
        "beats": [],
        "coverage": dict.fromkeys(LANDMARKS, 0),
    }
    for run in [ok, failed]:
        run["measurements"] = measure(audit_run(run), ctx)
    return [ok, failed]


def index(ctx, seconds):
    return int(np.argmin(abs(ctx["native"].relative_s.to_numpy() - seconds)))


def annotation(ctx, qrs_off_s, notes=None):
    beats = []
    for fid_s in [0.5, 1.5, 2.5]:
        fid = index(ctx, fid_s)
        marks = dict.fromkeys(LANDMARKS)
        off = index(ctx, fid_s + qrs_off_s)
        marks.update(
            QRS_on=[fid - 5, fid - 6, fid - 4],
            R_peak=[fid, fid, fid],
            QRS_off=[off, off - 1, off + 1],
            T_off=[fid + 40, fid + 38, fid + 42],
        )
        beats.append(
            {"fid_index": fid, "confidence": "medium", "notes": "", "marks": marks}
        )
    result = {
        "method": "Astra",
        "overall_quality": "fixture",
        "convention": "native",
        "observations": [],
        "unassessable": [],
        "beats": beats,
    }
    if notes is not None:
        result["review_notes"] = notes
    return result


class FakeAstra:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, config, prompt, text, images, out, history=()):
        out = Path(out)
        out.mkdir()
        response = self.responses.pop(0)
        (out / "prompt.md").write_text(prompt)
        (out / "input.txt").write_text(text)
        (out / "response.txt").write_text(json.dumps(response))
        self.calls.append(
            {
                "prompt": prompt,
                "text": text,
                "images": list(map(str, images)),
                "history": list(history),
            }
        )
        return response


def review(ids):
    return {
        "paragraph": "Machine review fixture.",
        "methods": [{"id": i, "assessment": "fixture"} for i in ids],
        "disagreements": ["QRS offsets differ"],
    }


def run(tmp_path, monkeypatch, responses):
    ctx = context()
    runs = tool_runs(ctx)
    panels = []
    for n in range(2):
        p = tmp_path / f"panel_{n}.png"
        p.write_bytes(b"panel")
        panels.append(p)
    fake = FakeAstra(responses)
    monkeypatch.setattr("resting_ecg.informed.request", fake)
    out = tmp_path / "run"
    out.mkdir()
    return ctx, runs, panels, fake, out


def test_every_machine_result_reaches_review_then_draft_then_plot_review(
    tmp_path, monkeypatch
):
    ctx = context()
    ids = ["tool", "failed_tool"]
    ctx, runs, panels, fake, out = run(
        tmp_path,
        monkeypatch,
        [
            review(ids),
            annotation(ctx, 0.08),
            annotation(ctx, 0.05, "Moved QRS offset before the notch."),
        ],
    )
    final = annotate_informed(ctx, runs, {}, panels, out)

    first, draft, plot_review = fake.calls
    evidence = json.loads(
        first["text"].split("Machine annotations and measurements:\n")[1]
    )
    assert [e["id"] for e in evidence] == ids
    assert evidence[1]["error"] == "RuntimeError: octave crashed"
    # Context beats outside the supplied samples/images are not sent.
    assert [b["fid_s"] for b in evidence[0]["beats"]] == [0.5, 1.5, 2.5]
    assert "sample_index,seconds,microvolts" in first["text"]
    assert first["history"] == []
    assert first["images"][:2] == list(map(str, panels))
    assert [Path(p).name for p in first["images"][2:]] == ["tool_0.png"]

    assert [t["role"] for t in draft["history"]] == ["user", "assistant"]
    assert draft["history"][0]["text"].startswith(first["prompt"])
    assert draft["history"][0]["images"] == first["images"]
    assert json.loads(draft["history"][1]["text"]) == review(ids)
    assert draft["images"] == [] and "annotation" in draft["prompt"].lower()

    assert [t["role"] for t in plot_review["history"]] == [
        "user",
        "assistant",
        "user",
        "assistant",
    ]
    assert [Path(p).name for p in plot_review["images"]] == [
        "astra_draft_0.png",
        "astra_draft_1.png",
    ]
    assert "draft_measurements" in plot_review["text"]

    # The revised annotation, not the draft, is what gets measured.
    assert final["id"] == "astra" and final["name"] == "Astra annotation"
    assert final["review_notes"] == "Moved QRS offset before the notch."
    saved_draft = json.loads((out / "astra_draft.json").read_text())
    assert saved_draft["name"].startswith("Astra draft")
    assert (
        final["measurements"]["summary"]["QRS_ms"]["median"]
        < saved_draft["measurements"]["summary"]["QRS_ms"]["median"]
    )
    workflow = json.loads((out / "annotation_workflow.json").read_text())
    assert workflow["machine_ids"] == ids and workflow["stages"][-1] == (
        "astra_annotation"
    )


def test_review_must_cover_every_method_including_failures(tmp_path, monkeypatch):
    ctx, runs, panels, fake, out = run(tmp_path, monkeypatch, [review(["tool"])])
    with pytest.raises(ValueError, match="every supplied method"):
        annotate_informed(ctx, runs, {}, panels, out)
    assert len(fake.calls) == 1


def test_no_silent_fallback_to_draft_without_plot_review_notes(tmp_path, monkeypatch):
    ctx = context()
    ctx, runs, panels, fake, out = run(
        tmp_path,
        monkeypatch,
        [
            review(["tool", "failed_tool"]),
            annotation(ctx, 0.08),
            annotation(ctx, 0.05, "  "),
        ],
    )
    with pytest.raises(ValueError, match="visual-review note"):
        annotate_informed(ctx, runs, {}, panels, out)
    assert (out / "astra_draft.json").exists()
    assert not (out / "annotation_workflow.json").exists()


def test_evidence_is_rounded_and_keeps_failed_methods():
    ctx = context()
    runs = tool_runs(ctx)
    runs[0]["beats"][1]["marks"]["QRS_on"] = 0.46000000000000003
    evidence = machine_evidence(runs, ctx["native"])
    assert evidence[0]["beats"][0]["marks"]["QRS_on"] == 0.46
    assert evidence[1]["beats"] == [] and evidence[1]["status"] == "failed"


def test_request_replays_history_as_conversation(tmp_path, monkeypatch):
    from resting_ecg.tests.test_workflow import Response

    monkeypatch.setattr(
        "resting_ecg.astra.credentials", lambda _: ("SECRET", "ACCOUNT")
    )
    first = tmp_path / "panel.png"
    first.write_bytes(b"first")
    later = tmp_path / "draft.png"
    later.write_bytes(b"draft")
    sent = []

    def post(*a, **k):
        sent.append(k["json"])
        return Response(
            [
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "reasoning",
                        "id": "rs_1",
                        "summary": [{"type": "summary_text", "text": "Checked P"}],
                    },
                },
                {"type": "response.output_text.done", "text": '{"ok":true}'},
                {
                    "type": "response.completed",
                    "response": {"status": "completed", "model": "gpt-6-astra"},
                },
            ]
        )

    monkeypatch.setattr("requests.post", post)
    config = {"model": "gpt-6-astra", "external_model_authorized": True}
    history = [
        {"role": "user", "text": "review", "images": [str(first)]},
        {"role": "assistant", "text": "reviewed", "images": []},
    ]
    assert request(config, "p", "annotate", [later], tmp_path / "r", history) == {
        "ok": True
    }
    messages = sent[0]["input"]
    assert [m["role"] for m in messages] == ["user", "assistant", "user"]
    assert messages[1]["content"][0]["type"] == "output_text"
    assert messages[1]["phase"] == "final_answer"
    assert len(messages[0]["content"]) == 2 and len(messages[2]["content"]) == 2
    manifest = json.loads((tmp_path / "r/request_manifest.json").read_text())
    assert set(manifest["image_hashes"]) == {"panel.png", "draft.png"}
    assert "SECRET" not in "".join(
        p.read_text() for p in (tmp_path / "r").iterdir() if p.is_file()
    )
    summary = json.loads((tmp_path / "r/reasoning_summary.json").read_text())
    assert summary["available"] and summary["summaries"] == [["Checked P"]]
    # Two different images with the same name would make the manifest ambiguous.
    other = tmp_path / "other"
    other.mkdir()
    (other / "panel.png").write_bytes(b"different")
    with pytest.raises(ValueError, match="Conflicting image names"):
        request(config, "p", "t", [other / "panel.png"], tmp_path / "c", history)


def test_beat_plots_widen_for_long_pr_and_qt_and_stay_within_samples():
    from resting_ecg.figures import beat_window

    t = context()["native"].relative_s.to_numpy()
    marks = dict.fromkeys(LANDMARKS)
    marks.update(P_on=4.55, R_peak=5.0, T_off=5.72)
    beat = {"fid_s": 5.0, "marks": marks, "ranges": {"T_off": [5.70, 5.80]}}
    assert beat_window(beat, t) == pytest.approx((4.49, 5.86))
    marks.update(P_on=None, T_off=None)
    assert beat_window({"fid_s": 5.0, "marks": marks}, t) == pytest.approx((4.64, 5.58))
    marks.update(R_peak=11.1, T_off=11.19)
    assert beat_window({"fid_s": 11.1, "marks": marks}, t)[1] == t[-1]


def test_evidence_window_edges_and_run_paths(tmp_path):
    ctx = context()
    runs = tool_runs(ctx)
    lo, hi = ctx["native"].relative_s.iloc[0], ctx["native"].relative_s.iloc[-1]
    edge = dict(runs[0]["beats"][1], fid_s=float(lo))
    outside = dict(runs[0]["beats"][1], fid_s=float(lo) - 0.01)
    late = dict(runs[0]["beats"][1], fid_s=float(hi) + 0.01)
    runs[0]["beats"] = [outside, edge, late]
    runs[1]["error"] = f"TimeoutExpired: {tmp_path}/delineators/ecgdeli.m timed out"
    evidence = machine_evidence(runs, ctx["native"], tmp_path)
    assert [b["fid_s"] for b in evidence[0]["beats"]] == [round(float(lo), 4)]
    assert (
        evidence[1]["error"] == "TimeoutExpired: <run>/delineators/ecgdeli.m timed out"
    )
