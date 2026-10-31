from copy import deepcopy
import json
import numpy as np
import pandas as pd
import pytest
from resting_ecg.interactive import payload, extra_repeats


def fixture():
    native = pd.DataFrame(
        {"relative_s": np.arange(0, 2, 0.01), "ecg_uv": np.arange(200) * 10}
    )
    b = {
        "fid_s": 0.5,
        "marks": {"QRS_on": 0.41, "T_off": 0.813, "P_on": None},
        "ranges": {"QRS_on": [0.4, 0.42]},
        "notes": ["draft"],
        "confidence": "medium",
    }
    run = {
        "id": "astra",
        "name": "Astra",
        "status": "ok",
        "beats": [b],
        "preprocessing": "raw",
        "measurements": {
            "beats": [{"fid_s": 0.5, "QT_ms": 403.0, "QRS_ms": 70.0}],
            "summary": {"QT_ms": {"median": 403.0}},
        },
    }
    return {"native": native, "fs": 100}, run


def test_plot_preserves_samples_landmarks_and_uncertainty_without_rewriting():
    ctx, a = fixture()
    before = deepcopy(a)
    d = payload(ctx, [], a)
    assert d["times"] == ctx["native"].relative_s.tolist()
    assert d["millivolts"] == (ctx["native"].ecg_uv / 1000).tolist()
    marks = d["methods"][0]["beats"][0]["marks"]
    assert marks["QRS_on"]["seconds"] == 0.41 and marks["QRS_on"]["range"] == [
        0.4,
        0.42,
    ]
    assert (
        marks["T_off"]["seconds"] == 0.813
        and marks["T_off"]["nearest_native_index"] == 81
    )
    assert marks["T_off"]["mv"] == pytest.approx(0.813)
    assert "P_on" not in marks and a == before
    assert d["methods"][0]["beats"][0]["measurements"]["QT_ms"] == 403.0


def test_failed_and_missing_annotations_remain_missing():
    ctx, a = fixture()
    a.update(status="failed", beats=[], error="fixture failure")
    d = payload(ctx, [a], None)
    assert d["methods"][0]["beats"] == [] and d["methods"][0]["status"] == "failed"


def test_repeat_overlay_requires_same_report_before_reading_other_files(tmp_path):
    ctx, _ = fixture()
    assert extra_repeats(tmp_path, ctx) == []
    (tmp_path / "repeatability.json").write_text(
        json.dumps({"runs": [{"path": str(tmp_path / "wrong")}]})
    )
    with pytest.raises(ValueError, match="different target"):
        extra_repeats(tmp_path, ctx)


def test_cross_prompt_overlay_requires_matching_waveform_and_keeps_version_label(
    tmp_path,
):
    ctx, a = fixture()
    current = tmp_path / "current"
    old = tmp_path / "old"
    manifest = {
        "model": "gpt-6-astra",
        "reasoning_effort": "high",
        "input_sha256": "same-input",
        "image_hashes": {"a": "same-image"},
        "prompt_sha256": "short",
    }
    for root in [current, old]:
        (root / "astra_annotation").mkdir(parents=True)
        (root / "astra_annotation/request_manifest.json").write_text(
            json.dumps(manifest)
        )
        (root / "astra.json").write_text(json.dumps(a))
        ctx["native"].to_csv(root / "native.csv", index=False)
    manifest["prompt_sha256"] = "long"
    (old / "astra_annotation/request_manifest.json").write_text(json.dumps(manifest))
    (current / "annotation_comparisons.json").write_text(
        json.dumps({"target": str(current), "references": [str(old)]})
    )
    before = (old / "astra.json").read_bytes()
    extra = extra_repeats(current, ctx)
    assert (
        len(extra) == 1
        and "different prompt" in extra[0]["name"]
        and extra[0]["beats"] == a["beats"]
    )
    assert (old / "astra.json").read_bytes() == before
    # Display comparisons label workflow/effort differences instead of hiding them.
    (old / "astra_machine_review").mkdir()
    (old / "astra_draft").mkdir()
    for stage in ["astra_machine_review", "astra_draft"]:
        (old / stage / "request_manifest.json").write_text(
            json.dumps({**manifest, "reasoning_effort": "xhigh"})
        )
    assert extra_repeats(current, ctx)[0]["name"] == (
        "Astra old · different workflow, different effort, different prompt"
    )
    (old / "astra_machine_review/request_manifest.json").write_text(
        json.dumps({**manifest, "model": "other-model"})
    )
    with pytest.raises(ValueError, match="different model"):
        extra_repeats(current, ctx)
    (old / "astra_machine_review/request_manifest.json").write_text(
        json.dumps(manifest)
    )
    other = ctx["native"].copy()
    other.loc[3, "ecg_uv"] = 999
    other.to_csv(old / "native.csv", index=False)
    with pytest.raises(ValueError, match="Plot waveform"):
        extra_repeats(current, ctx)
    ctx["native"].to_csv(old / "native.csv", index=False)
    ctx["native"].loc[0, "ecg_uv"] = 999
    with pytest.raises(ValueError, match="Plot waveform"):
        extra_repeats(current, ctx)


def test_context_beats_and_outside_source_marks_are_not_plotted_as_visible_beats():
    ctx, a = fixture()
    context_beat = deepcopy(a["beats"][0])
    context_beat["fid_s"] = -0.2
    a["beats"].insert(0, context_beat)
    a["beats"][1]["marks"]["P_on"] = -0.1
    d = payload(ctx, [], a)
    assert len(d["methods"][0]["beats"]) == 1
    assert d["methods"][0]["beats"][0]["fid_s"] == 0.5
    assert "P_on" not in d["methods"][0]["beats"][0]["marks"]
