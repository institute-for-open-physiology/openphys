import base64, json, time
from pathlib import Path
import numpy as np
import pandas as pd
import pytest
from resting_ecg.core import (
    evaluate_candidate,
    select_window,
    make_context,
    load_recording,
    digest,
    write_json,
)
from resting_ecg.delineators import LANDMARKS, finite_time
from resting_ecg.measures import corrections, measure, summary
from resting_ecg.astra import normalize, request, credentials

PROTOCOL = json.loads(Path("resting_ecg/protocol.json").read_text())


@pytest.fixture
def recording():
    result = {}
    for kind, fs in [("ecg", 130), ("acc", 200), ("hr", 1)]:
        t = np.arange(-35, 275, 1 / fs)
        d = pd.DataFrame(
            {
                "t_s": t,
                "device_time_ns": np.rint(t * 1e9).astype("int64"),
                "session_id": 1,
                "connection_id": 1,
            }
        )
        if kind == "ecg":
            d["ecg_uv"] = 500 * np.sin(2 * np.pi * t) + 100
        if kind == "acc":
            d["x_mg"] = 0.0
            d["y_mg"] = 0.0
            d["z_mg"] = 1000.0
        if kind == "hr":
            d["sensor_contact_supported"] = True
            d["sensor_contact_detected"] = True
        result[kind] = d
    return result


def test_selection_uses_motion_not_morphology(recording):
    a = select_window(recording, PROTOCOL)
    assert a["chosen"]["start_s"] == 120 and len(a["candidates"]) == 51
    assert a["candidates"][-1]["end_s"] == 180
    e = recording["ecg"]
    e.loc[(e.t_s > 122) & (e.t_s < 123), "ecg_uv"] = (
        9000  # Broad grossly abnormal-looking morphology, not clipped.
    )
    b = select_window(recording, PROTOCOL)
    assert b["chosen"] == a["chosen"]
    ctx = make_context(recording, b)
    assert 129 < ctx["fs"] < 131 and len(ctx["view"]) == 1300


def test_no_quiet_window_does_not_force_a_pass(recording):
    a = recording["acc"]
    a["x_mg"] = 200 * np.sin(a.t_s * 10)
    result = select_window(recording, PROTOCOL)
    assert result["status"] == "repeat_acquisition" and result["chosen"] is None
    with pytest.raises(ValueError):
        make_context(recording, result)
    with pytest.raises(ValueError):
        select_window(recording, {**PROTOCOL, "settling_s": 119})


@pytest.mark.parametrize(
    "problem,reason",
    [
        ("ecg_gap", "ECG discontinuity"),
        ("acc_gap", "ACC discontinuity"),
        ("contact", "Contact not continuously confirmed"),
        ("contact_nan", "Contact not continuously confirmed"),
        ("acc_nan", "Missing ACC values"),
        ("flat", "Missing or flat ECG"),
        ("clipped", "ECG clipping"),
        ("connection", "ECG connection change"),
        ("identity", "ECG/ACC identity mismatch"),
        ("stale_contact", "Contact status stale"),
        ("contact_identity", "Contact identity mismatch"),
        ("empty_ecg", "ECG coverage"),
        ("empty_acc", "ACC coverage"),
    ],
)
def test_acquisition_rejections(recording, problem, reason):
    e, a, h = recording["ecg"], recording["acc"], recording["hr"]
    if problem == "ecg_gap":
        e = e[~((e.t_s > 123) & (e.t_s < 123.2))]
    elif problem == "acc_gap":
        a = a[~((a.t_s > 123) & (a.t_s < 123.2))]
    elif problem == "contact":
        h.loc[h.t_s == 123, "sensor_contact_detected"] = False
    elif problem == "contact_nan":
        h["sensor_contact_detected"] = h.sensor_contact_detected.astype("boolean")
        h.loc[h.t_s == 123, "sensor_contact_detected"] = pd.NA
    elif problem == "acc_nan":
        a.loc[(a.t_s > 123) & (a.t_s < 123.2), "x_mg"] = np.nan
    elif problem == "flat":
        e["ecg_uv"] = 0
    elif problem == "clipped":
        e.loc[(e.t_s > 123) & (e.t_s < 123.2), "ecg_uv"] = 32767
    elif problem == "connection":
        e.loc[e.t_s > 123, "connection_id"] = 2
    elif problem == "identity":
        a["connection_id"] = 3
    elif problem == "stale_contact":
        h = h[~h.t_s.between(121, 125)]
    elif problem == "contact_identity":
        h["connection_id"] = 3
    elif problem == "empty_ecg":
        e = e.iloc[0:0]
    elif problem == "empty_acc":
        a = a.iloc[0:0]
    c = evaluate_candidate(e, a, h, 120, PROTOCOL)
    assert not c["accepted"] and reason in c["reasons"]


def test_context_does_not_fabricate_padding(recording):
    s = select_window(recording, PROTOCOL)
    recording["ecg"] = recording["ecg"][recording["ecg"].t_s > 110]
    with pytest.raises(ValueError, match="context"):
        make_context(recording, s)


def test_archive_time_conversion_and_validation(tmp_path, recording):
    start = pd.Timestamp("2026-09-23T14:43:20+01:00")
    offset = pd.Timestamp("2000-01-01T00:00:00Z").value - 3600 * 10**9
    for kind, d in recording.items():
        d["sensor_id"] = "test"
        d["device_time_epoch"] = "h10_local_since_2000"
        d["device_time_ns"] = d.device_time_ns + start.value - offset
        d["collector_unix_ns"] = np.rint(d.t_s * 1e9).astype("int64") + start.value
        d.to_parquet(tmp_path / (kind + ".parquet"))
    c = {
        "archive_dir": str(tmp_path),
        "sensor_id": "test",
        "session_start": start.isoformat(),
    }
    r = load_recording(c)
    assert abs(r["ecg"].t_s.iloc[0] + 35) < 1e-6
    assert (
        len(r["source"]["hashes"]) == 3 and len(digest(tmp_path / "ecg.parquet")) == 64
    )
    with pytest.raises(ValueError, match="timezone"):
        load_recording({**c, "session_start": "2026-09-23"})
    with pytest.raises(ValueError, match="sensor"):
        load_recording({**c, "sensor_id": "other"})


@pytest.fixture
def annotated():
    t = np.arange(-1, 12, 0.001)
    ctx = {"context": pd.DataFrame({"relative_s": t, "ecg_uv": 100 * np.sin(t * 10)})}
    marks = dict.fromkeys(LANDMARKS)
    marks.update(
        P_on=0.08,
        P_peak=0.12,
        P_off=0.15,
        QRS_on=0.20,
        R_peak=0.23,
        S_peak=0.25,
        QRS_off=0.30,
        T_on=0.45,
        T_peak=0.5,
        T_off=0.60,
    )
    return {
        "beats": [
            {"fid_s": -0.55, "marks": dict.fromkeys(LANDMARKS)},
            {
                "fid_s": 0.25,
                "marks": marks,
                "ranges": {
                    k: [v - 0.005, v + 0.005] for k, v in marks.items() if v is not None
                },
            },
        ]
    }, ctx


def test_intervals_corrections_and_uncertainty(annotated):
    run, ctx = annotated
    r = measure(run, ctx)["beats"][0]
    assert r["PR_ms"] == pytest.approx(120)
    assert r["QRS_ms"] == pytest.approx(100)
    assert r["QT_ms"] == pytest.approx(400)
    assert r["RR_ms"] == pytest.approx(800)
    assert r["QTcF_ms"] == pytest.approx(400 / 0.8 ** (1 / 3))
    assert r["QTcB_ms"] == pytest.approx(400 / 0.8**0.5)
    assert r["QTcFramingham_ms"] == pytest.approx(430.8)
    assert r["QTcHodges_ms"] == pytest.approx(426.25)
    assert r["bounds"]["QT_ms"] == pytest.approx([390, 410])
    assert r["ST60_mV"] is not None and r["J_mV"] is not None
    assert (
        corrections(None, 800)["QTcB_ms"] is None
        and corrections(400, -1)["QTcB_ms"] is None
    )
    assert summary([None, np.nan, 1, 3]) == {
        "n": 2,
        "median": 2.0,
        "min": 1.0,
        "max": 3.0,
    }


def test_invalid_boundaries_are_not_silently_fixed(annotated):
    run, ctx = annotated
    run["beats"][1]["marks"]["QRS_off"] = 0.18
    r = measure(run, ctx)["beats"][0]
    assert r["QRS_ms"] is None and r["QT_ms"] is None and r["QTcF_ms"] is None
    assert any("order" in i or "enclose" in i for i in r["issues"])


def test_no_baseline_no_amplitude_and_no_st_inside_t(annotated):
    run, ctx = annotated
    run["beats"][1]["marks"]["T_on"] = 0.34
    r = measure(run, ctx)["beats"][0]
    assert r["ST60_mV"] is None
    run["beats"][1]["marks"]["P_off"] = None
    r = measure(run, ctx)["beats"][0]
    assert r["baseline_mV"] is None and r["R_peak_mV"] is None


def annotation():
    return {
        "method": "Astra visual draft",
        "overall_quality": "draft",
        "convention": "native",
        "observations": [],
        "unassessable": [],
        "beats": [
            {
                "fid_index": 5,
                "confidence": "medium",
                "notes": "test",
                "marks": {k: [5, 4, 6] for k in LANDMARKS},
            }
        ],
    }


def test_astra_indices_preserved_with_ranges():
    d = pd.DataFrame({"relative_s": np.arange(20) * 0.01})
    a = annotation()
    r = normalize(a, d)
    assert r["beats"][0]["marks"]["T_off"] == 0.05 and r["beats"][0]["ranges"][
        "T_off"
    ] == [0.04, 0.06]
    a["beats"][0]["marks"]["T_off"] = None
    assert normalize(a, d)["beats"][0]["marks"]["T_off"] is None


@pytest.mark.parametrize(
    "problem",
    [
        "bad_fid",
        "reordered",
        "float",
        "outside",
        "reversed_range",
        "missing",
        "confidence",
        "no_beats",
    ],
)
def test_bad_astra_schema_fails(problem):
    a = annotation()
    d = pd.DataFrame({"relative_s": np.arange(20) * 0.01})
    if problem == "bad_fid":
        a["beats"][0]["fid_index"] = 30
    elif problem == "reordered":
        a["beats"] *= 2
    elif problem == "float":
        a["beats"][0]["marks"]["P_on"] = [5.0, 4, 6]
    elif problem == "outside":
        a["beats"][0]["marks"]["P_on"] = [5, -1, 6]
    elif problem == "reversed_range":
        a["beats"][0]["marks"]["P_on"] = [5, 6, 4]
    elif problem == "missing":
        del a["beats"][0]["marks"]["P_on"]
    elif problem == "confidence":
        a["beats"][0]["confidence"] = "certain"
    elif problem == "no_beats":
        a["beats"] = []
    with pytest.raises(ValueError):
        normalize(a, d)


class Response:
    status_code = 200

    def __init__(self, events):
        self.events = events

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_lines(self):
        yield b"event: ignored"
        for event in self.events:
            yield b"data: " + json.dumps(event).encode()


@pytest.mark.parametrize("mode", ["delta", "done", "item", "full"])
def test_sse_completed_without_final_output_is_captured(tmp_path, monkeypatch, mode):
    monkeypatch.setattr(
        "resting_ecg.astra.credentials", lambda _: ("SECRET", "ACCOUNT")
    )
    final = {
        "type": "response.completed",
        "response": {"status": "completed", "model": "gpt-6-astra", "output": []},
    }
    message = {
        "type": "message",
        "content": [{"type": "output_text", "text": '{"ok":true}'}],
    }
    event = {
        "delta": {"type": "response.output_text.delta", "delta": '{"ok":true}'},
        "done": {"type": "response.output_text.done", "text": '{"ok":true}'},
        "item": {
            "type": "response.output_item.done",
            "item": message,
            "output_index": 1,
        },
    }
    if mode == "full":
        final["response"]["output"] = [message]
        events = [final]
    else:
        events = [event[mode], final]

    def post(*args, **kwargs):
        assert (
            kwargs["json"]["input"][0]["role"] == "user"
            and kwargs["json"]["store"] is False
        )
        return Response(events)

    monkeypatch.setattr("requests.post", post)
    result = request(
        {"model": "gpt-6-astra", "external_model_authorized": True},
        "prompt",
        "input",
        [],
        tmp_path / "request",
    )
    assert result == {"ok": True}
    for p in (tmp_path / "request").iterdir():
        assert "SECRET" not in p.read_text() and "ACCOUNT" not in p.read_text()


def test_expired_auth_is_not_refreshed(tmp_path):
    token = (
        "header."
        + base64.urlsafe_b64encode(json.dumps({"exp": time.time() - 100}).encode())
        .decode()
        .rstrip("=")
        + ".signature"
    )
    p = tmp_path / "auth.json"
    write_json(p, {"tokens": {"access_token": token, "account_id": "test"}})
    with pytest.raises(RuntimeError, match="expired"):
        credentials(p)
    assert finite_time(501, 0, True) == 1 and finite_time(np.nan, 0) is None


def test_overlap_uncertainty_is_indeterminate_not_a_zero_qt(annotated):
    run, ctx = annotated
    run["beats"][1]["ranges"]["QRS_on"] = [0.1, 0.55]
    run["beats"][1]["ranges"]["T_off"] = [0.5, 0.9]
    r = measure(run, ctx)["beats"][0]
    assert "QT_ms" not in r["bounds"] and "QTcF_ms" not in r["bounds"]
    assert any("overlaps" in s for s in r["issues"])


def test_upstream_sentinel_and_contradictions_are_retained_but_not_measured(annotated):
    from resting_ecg.measures import audit_run

    run, ctx = annotated
    run["coverage"] = {k: 0 for k in LANDMARKS}
    run["beats"][1]["marks"]["Q_peak"] = -29.748
    run["beats"][1]["marks"]["P_off"] = 0.22
    audit_run(run)
    b = run["beats"][1]
    assert b["marks"]["Q_peak"] is None and b["raw_marks"]["Q_peak"] == -29.748
    assert b["marks"]["P_off"] is None and run["coverage"]["Q_peak"] == 0
    m = measure(run, ctx)
    assert m["summary"]["Q_peak_mV"]["n"] == 0
    assert any("P_off" in i for i in m["beats"][0]["issues"])


def test_no_visible_astra_beat_rejected():
    a = annotation()
    d = pd.DataFrame({"relative_s": np.arange(20) * 0.01 - 2})
    with pytest.raises(ValueError, match="in-view"):
        normalize(a, d)


def test_astra_no_qrs_rejected():
    a = annotation()
    a["beats"][0]["marks"]["QRS_off"] = None
    with pytest.raises(ValueError, match="measurable"):
        normalize(a, pd.DataFrame({"relative_s": np.arange(20) * 0.01}))


def test_wfdb_groups_preserve_t_after_rr_midpoint_and_biphasic_peaks():
    from resting_ecg.delineators import wfdb_beats

    events = [
        (0.48, "P_on"),
        (0.51, "P_peak"),
        (0.54, "P_off"),
        (0.58, "QRS_on"),
        (0.616, "fid"),
        (0.67, "QRS_off"),
        (0.75, "T_on"),
        (0.85, "T_peak"),
        (0.9, "T_peak"),
        (1.012, "T_off"),
        (1.2, "P_on"),
        (1.25, "P_peak"),
        (1.3, "P_off"),
        (1.35, "QRS_on"),
        (1.392, "fid"),
        (1.43, "QRS_off"),
    ]
    b = wfdb_beats(events)
    assert b[0]["marks"]["T_off"] == 1.012 and b[1]["marks"]["P_on"] == 1.2
    assert b[0]["marks"]["T_peak"] is None and b[0]["raw_wavegroups"][-1]["peaks"] == [
        0.85,
        0.9,
    ]
    assert b[0]["marks"]["QRS_on"] == 0.58


def test_duplicate_wfdb_groups_never_reenable_a_mark():
    from resting_ecg.delineators import wfdb_beats

    events = [
        (0.2, "P_on"),
        (0.21, "P_peak"),
        (0.22, "P_off"),
        (0.3, "P_on"),
        (0.31, "P_peak"),
        (0.32, "P_off"),
        (0.4, "P_on"),
        (0.41, "P_peak"),
        (0.42, "P_off"),
        (0.5, "fid"),
    ]
    b = wfdb_beats(events)[0]
    assert (
        b["marks"]["P_on"] is None
        and b["marks"]["P_off"] is None
        and b["marks"]["P_peak"] is None
    )


@pytest.mark.parametrize(
    "events,error",
    [
        ([{"type": "response.failed"}], "returned"),
        ([], "No completed"),
        (
            [
                {
                    "type": "response.completed",
                    "response": {"status": "completed", "model": "wrong"},
                }
            ],
            "Unexpected",
        ),
        (
            [
                {
                    "type": "response.completed",
                    "response": {"status": "completed", "model": "gpt-6-astra"},
                }
            ],
            "no output",
        ),
    ],
)
def test_sse_failure_paths(tmp_path, monkeypatch, events, error):
    monkeypatch.setattr(
        "resting_ecg.astra.credentials", lambda _: ("SECRET", "ACCOUNT")
    )
    monkeypatch.setattr("requests.post", lambda *a, **k: Response(events))
    with pytest.raises(RuntimeError, match=error):
        request(
            {"model": "gpt-6-astra", "external_model_authorized": True},
            "prompt",
            "input",
            [],
            tmp_path / "req",
        )


def test_request_authorization_and_model_are_explicit(tmp_path):
    with pytest.raises(ValueError, match="authorization"):
        request({"model": "gpt-6-astra"}, "p", "t", [], tmp_path / "a")
    with pytest.raises(ValueError, match="explicitly requested"):
        request(
            {"model": "other", "external_model_authorized": True},
            "p",
            "t",
            [],
            tmp_path / "b",
        )


def test_request_image_and_fenced_json(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "resting_ecg.astra.credentials", lambda _: ("SECRET", "ACCOUNT")
    )
    img = tmp_path / "panel.png"
    img.write_bytes(b"example_image_fixture")

    def post(*a, **k):
        assert k["json"]["input"][0]["content"][1]["image_url"].startswith(
            "data:image/png;base64,"
        )
        return Response(
            [
                {
                    "type": "response.output_text.done",
                    "text": '```json\n{"ok":true}\n```',
                },
                {
                    "type": "response.completed",
                    "response": {"status": "completed", "model": "gpt-6-astra"},
                },
            ]
        )

    monkeypatch.setattr("requests.post", post)
    assert request(
        {"model": "gpt-6-astra", "external_model_authorized": True},
        "p",
        "t",
        [img],
        tmp_path / "req",
    ) == {"ok": True}


def test_interpretation_schema(monkeypatch, tmp_path):
    from resting_ecg.astra import interpret

    native = pd.DataFrame({"relative_s": np.arange(20) * 0.01, "ecg_uv": np.arange(20)})
    a = normalize(annotation(), native)
    result = {
        "paragraph": "fixture interpretation",
        "findings": [],
        "limitations": [],
        "review_priority": "routine_review",
    }
    monkeypatch.setattr("resting_ecg.astra.request", lambda *args: result)
    assert interpret(a, {}, [], {}, tmp_path) == result
    result["review_priority"] = "cleared"
    with pytest.raises(ValueError, match="priority"):
        interpret(a, {}, [], {}, tmp_path)
    result["findings"] = "not an array"
    with pytest.raises(ValueError, match="schema"):
        interpret(a, {}, [], {}, tmp_path)


def test_repeatability_requires_identical_inputs_and_distinct_responses(tmp_path):
    from resting_ecg.repeatability import compare

    roots = []
    for i, qt in enumerate([420, 530]):
        r = tmp_path / f"run{i}"
        (r / "astra_annotation").mkdir(parents=True)
        roots.append(r)
        write_json(
            r / "astra_annotation/request_manifest.json",
            {
                "model": "gpt-6-astra",
                "reasoning_effort": "high",
                "image_hashes": {"a": "hash"},
                "prompt_sha256": "prompt",
                "input_sha256": "input",
            },
        )
        write_json(
            r / "astra_annotation/response_metadata.json",
            {"model": "gpt-6-astra", "response_id": str(i)},
        )
        write_json(
            r / "astra.json",
            {
                "measurements": {
                    "summary": {
                        k: {"median": qt if k != "QRS_ms" else 100}
                        for k in ["QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
                    }
                }
            },
        )
        pd.DataFrame({"relative_s": np.arange(100) / 130}).to_csv(
            r / "native.csv", index=False
        )
    c = compare(roots)
    assert c["unresolved"] and c["median_ranges_ms"]["QT_ms"] == [420, 530]
    p = roots[1] / "astra_annotation/request_manifest.json"
    d = json.loads(p.read_text())
    d["image_hashes"]["a"] = "different"
    write_json(p, d)
    with pytest.raises(ValueError, match="identical"):
        compare(roots)
    d["image_hashes"]["a"] = "hash"
    write_json(p, d)
    write_json(
        roots[1] / "astra_annotation/response_metadata.json",
        {"model": "gpt-6-astra", "response_id": "0"},
    )
    with pytest.raises(ValueError, match="distinct"):
        compare(roots)


def test_machine_informed_repeatability_ignores_own_replayed_history(tmp_path):
    from resting_ecg.repeatability import STAGES, compare

    roots = []
    for i in range(2):
        r = tmp_path / f"run{i}"
        roots.append(r)
        for n, stage in enumerate(STAGES):
            (r / stage).mkdir(parents=True)
            write_json(
                r / stage / "request_manifest.json",
                {
                    "model": "gpt-6-astra",
                    "reasoning_effort": "xhigh",
                    # Later stages carry this run's own draft, so their inputs differ.
                    "image_hashes": {"a": "hash" if n == 0 else f"draft{i}"},
                    "prompt_sha256": f"prompt{n}",
                    "input_sha256": ["input", "draft request", f"history{i}"][n],
                },
            )
        write_json(
            r / "astra_annotation/response_metadata.json",
            {"model": "gpt-6-astra", "response_id": str(i)},
        )
        write_json(
            r / "astra.json",
            {
                "measurements": {
                    "summary": {
                        k: {"median": 100 + i}
                        for k in ["QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]
                    }
                }
            },
        )
        pd.DataFrame({"relative_s": np.arange(100) / 130}).to_csv(
            r / "native.csv", index=False
        )
    c = compare(roots)
    assert not c["unresolved"] and c["identical_inputs"]["stages"] == STAGES
    for stage, key in [
        ("astra_draft", "prompt_sha256"),
        ("astra_draft", "input_sha256"),
        ("astra_machine_review", "input_sha256"),
        ("astra_machine_review", "image_hashes"),
    ]:
        p = roots[1] / stage / "request_manifest.json"
        original = json.loads(p.read_text())
        write_json(p, {**original, key: "changed"})
        with pytest.raises(ValueError, match="identical"):
            compare(roots)
        write_json(p, original)
    write_json(roots[1] / "provenance.json", {"source_files": {"informed.py": "new"}})
    with pytest.raises(ValueError, match="identical"):
        compare(roots)


def test_repeatability_cannot_attach_evidence_to_an_unrelated_report(tmp_path):
    from resting_ecg.repeatability import attach

    report = tmp_path / "unrelated"
    report.mkdir()
    (report / "report.html").write_text("original")
    with pytest.raises(ValueError, match="final/current"):
        attach(report, [tmp_path / "recording_a1", tmp_path / "recording_a2"])
    assert (report / "report.html").read_text() == "original" and not (
        report / "repeatability.json"
    ).exists()
