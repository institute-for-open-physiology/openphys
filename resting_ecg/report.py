"""Offline operator report with calibrated SVG and explicit, exportable human review."""

from pathlib import Path
from html import escape as esc
import json, math
import numpy as np
from .core import write_json
from .interactive import html as interactive_html

LABELS = {
    "P_ms": "P duration",
    "PR_ms": "PR",
    "QRS_ms": "QRS",
    "QT_ms": "QT",
    "JT_ms": "JT",
    "T_ms": "T duration",
    "RR_ms": "RR",
    "HR_bpm": "Heart rate",
    "QTcB_ms": "QTc Bazett",
    "QTcF_ms": "QTc Fridericia",
    "QTcFramingham_ms": "QTc Framingham",
    "QTcHodges_ms": "QTc Hodges",
    "P_peak_mV": "P amplitude",
    "Q_peak_mV": "Q amplitude",
    "R_peak_mV": "R amplitude",
    "S_peak_mV": "S amplitude",
    "T_peak_mV": "T amplitude",
    "J_mV": "J level",
    "ST60_mV": "ST at J + 60 ms",
    "ST80_mV": "ST at J + 80 ms",
}


def fmt(v, dec=0):
    return "—" if v is None else f"{v:.{dec}f}"


def strip(context, run=None):
    d = context["view"]
    t = d.relative_s.to_numpy()
    v = d.ecg_uv.to_numpy() / 1000
    top = math.ceil((float(max(v)) + 0.2) * 2) / 2
    bottom = math.floor((float(min(v)) - 0.2) * 2) / 2
    height = (top - bottom) * 10
    total = height + 19
    parts = [
        f'<svg class="ecg" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 270 {total}" role="img" aria-label="Ten-second ECG, 25 millimetres per second, 10 millimetres per millivolt">',
        f'<rect width="270" height="{total}" fill="white"/>',
    ]
    for x in np.arange(0, 250.01, 1):
        major = abs(x % 5) < 0.01
        parts.append(
            f'<path d="M{x + 12:.2f} 7v{height}" stroke="{("#e3abb1" if major else "#f4dce0")}" stroke-width="{0.13 if major else 0.07}"/>'
        )
    for y in np.arange(0, height + 0.01, 1):
        major = abs(y % 5) < 0.01
        parts.append(
            f'<path d="M12 {y + 7:.2f}h250" stroke="{("#e3abb1" if major else "#f4dce0")}" stroke-width="{0.13 if major else 0.07}"/>'
        )
    coords = " ".join(
        f"{'M' if i == 0 else 'L'}{12 + 25 * x:.3f},{7 + 10 * (top - y):.3f}"
        for i, (x, y) in enumerate(zip(t, v))
    )
    parts.append(
        f'<path d="{coords}" fill="none" stroke="#192733" stroke-width=".19" stroke-linejoin="round"/>'
    )
    # Standard 1 mV calibration pulse, 200 ms wide.
    zero = 7 + top * 10
    parts.append(
        f'<path d="M2 {zero}h1v-10h5v10h2" stroke="#192733" stroke-width=".25" fill="none"/>'
    )
    for sec in range(11):
        parts.append(
            f'<text x="{12 + 25 * sec}" y="{height + 12}" font-size="2.4" text-anchor="middle" fill="#405467">{sec}</text>'
        )
    parts.append(
        f'<text x="12" y="{height + 17}" font-size="2.4" fill="#405467">Seconds · H10 nonstandard chest lead · native polarity · unfiltered · 1 mV calibration</text>'
    )
    if run:
        for number, b in enumerate(run["beats"]):
            if not 0 <= b["fid_s"] < 10:
                continue
            for k, s in b["marks"].items():
                if s is None or not 0 <= s < 10:
                    continue
                x = 12 + 25 * s
                y = 7 + 10 * (top - np.interp(s, t, v))
                color = (
                    "#8353a3"
                    if k.startswith("P_")
                    else "#078275"
                    if k.startswith("T_")
                    else "#1765b1"
                )
                title = esc(f"{k} · {s:.6f} s · " + run["name"])
                if k.endswith(("_on", "_off")):
                    parts.append(
                        f'<path d="M{x:.3f} 7v{height}" stroke="{color}" stroke-width=".16" opacity=".55"><title>{title}</title></path>'
                    )
                else:
                    parts.append(
                        f'<circle cx="{x:.3f}" cy="{y:.3f}" r=".6" fill="{color}"><title>{title}</title></circle>'
                    )
                bounds = b.get("ranges", {}).get(k)
                if bounds and 0 <= bounds[0] <= bounds[1] < 10:
                    parts.append(
                        f'<path d="M{12 + 25 * bounds[0]:.3f} {y:.3f}H{12 + 25 * bounds[1]:.3f}" stroke="{color}" stroke-width=".5" opacity=".6"/>'
                    )
    return "".join(parts) + "</svg>"


def table(measurements):
    rows = []
    for key, s in measurements.get("summary", {}).items():
        dec = 3 if key.endswith("mV") else 0
        unit = "mV" if dec else "bpm" if key.endswith("bpm") else "ms"
        rows.append(
            f"<tr><th>{esc(LABELS.get(key, key))}</th><td>{fmt(s['median'], dec)} {unit}</td><td>{fmt(s['min'], dec)}–{fmt(s['max'], dec)}</td><td>{s['n']}</td></tr>"
        )
    return (
        "<table><thead><tr><th>Measurement</th><th>Median</th><th>Beat range</th><th>Measured beats</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def overview(recording, selection):
    e = recording["ecg"]
    e = e[(e.t_s >= 0) & (e.t_s < 240)]
    parts = [
        '<svg viewBox="0 0 960 150" role="img" aria-label="Four-minute ECG overview, compressed time scale">',
        '<rect width="960" height="150" fill="#f1f5f7"/>',
    ]
    for start, end, color in [
        (0, 120, "#e7edf1"),
        (120, 180, "#e3f1eb"),
        (180, 240, "#eef1f4"),
    ]:
        parts.append(
            f'<rect x="{start * 4}" width="{(end - start) * 4}" height="150" fill="{color}"/>'
        )
    # Min/max per screen column preserves spikes; explicitly not diagnostic scale.
    for i in range(960):
        vals = e[(e.t_s >= i / 4) & (e.t_s < (i + 1) / 4)].ecg_uv.to_numpy() / 1000
        if len(vals):
            parts.append(
                f'<path d="M{i} {75 - np.clip(max(vals), -5, 5) * 12}V{75 - np.clip(min(vals), -5, 5) * 12}" stroke="#507386" stroke-width=".7"/>'
            )
    c = selection["chosen"]
    parts.append(
        f'<rect x="{c["start_s"] * 4}" width="40" height="150" fill="none" stroke="#087f73" stroke-width="3"/>'
    )
    parts.append("</svg>")
    return "".join(parts)


def failed_report(out, selection):
    rows = "".join(
        f"<li>{c['start_s']}–{c['end_s']} s: {esc(', '.join(c['reasons']))}</li>"
        for c in selection["candidates"]
    )
    reason = selection.get("analysis_error", "No eligible 10-second interval.")
    Path(out, "report.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8"><title>ECG acquisition needs review</title><h1>Recording / analysis requires review</h1><p>'
        + esc(reason)
        + "</p><p>No completed analysis or exercise decision generated. Resolve the acquisition or analysis issue and repeat.</p><ul>"
        + rows
        + "</ul></html>"
    )


def astra_stages(out, astra):
    """Machine review, draft and plot-review evidence from the annotation conversation."""
    review_path = out / "astra_machine_review/response.json"
    draft_path = out / "astra_draft.json"
    if not astra or not review_path.exists() or not draft_path.exists():
        return [], ""
    review = json.loads(review_path.read_text())
    draft = json.loads(draft_path.read_text())
    names = {r["id"]: r["name"] for r in json.loads((out / "methods.json").read_text())}
    methods = "".join(
        f"<li><b>{esc(names.get(m['id'], m['id']))}</b>: {esc(m['assessment'])}</li>"
        for m in review["methods"]
    )
    rows = "".join(
        f"<tr><th>{esc(r['name'])}</th>"
        + "".join(
            f"<td>{fmt(r['measurements']['summary'][k]['median'])}</td>"
            for k in ["PR_ms", "QRS_ms", "QT_ms", "QTcF_ms"]
        )
        + "</tr>"
        for r in [draft, astra]
    )
    stage_links = " · ".join(
        f"{label}: "
        + ", ".join(
            f'<a href="{stage}/{n}">{n.split(".")[0]}</a>'
            for n in ["prompt.md", "input.txt", "response.txt"]
        )
        for label, stage in [
            ("Machine review", "astra_machine_review"),
            ("Draft", "astra_draft"),
            ("Plot review", "astra_annotation"),
        ]
    )
    html = (
        """<section class="no-print technical"><details><summary>How Astra annotated this strip</summary><p>One conversation, three steps: Astra first reviewed every machine delineation (waveform, landmarks, measurements, tool issues and labelled plots), then drew its own annotation, then checked plots of that draft and returned the final annotation used above. The interpretation is a separate request.</p><h3>1 · Review of the machine delineations</h3><p>"""
        + esc(review["paragraph"])
        + "</p><ul>"
        + methods
        + "</ul><h3>Disagreements Astra identified</h3><ul>"
        + "".join("<li>" + esc(x) + "</li>" for x in review["disagreements"])
        + """</ul><h3>2–3 · Draft and plot review</h3><p>"""
        + esc(astra.get("review_notes", ""))
        + """</p><div class="table-scroll"><table><thead><tr><th>Annotation</th><th>PR</th><th>QRS</th><th>QT</th><th>QTcF</th></tr></thead><tbody>"""
        + rows
        + """</tbody></table></div><p class="small">Medians in ms. Select “Astra draft” in the plot above to compare it with the final annotation beat by beat. Exact instructions and raw responses: """
        + stage_links
        + """ · <a href="machine_evidence.json">machine evidence sent</a> · <a href="annotation_workflow.json">workflow record</a>.</p></details></section>"""
    )
    return [draft], html


def render(
    out,
    config,
    recording,
    selection,
    context,
    runs,
    astra,
    interpretation,
    model_error=None,
):
    out = Path(out)
    allruns = runs + ([astra] if astra else [])
    drafts, stages_html = astra_stages(out, astra)
    m = astra["measurements"] if astra else {}
    summ = m.get("summary", {})
    c = selection["chosen"]
    participant = esc(config.get("participant_id", "DEMO"))
    interp = interpretation or {
        "paragraph": "Astra interpretation unavailable. Operator review and repeat analysis required.",
        "findings": [],
        "limitations": [],
        "review_priority": "repeat_recording",
    }
    write_json(out / "measurements.json", {r["id"]: r["measurements"] for r in allruns})
    write_json(out / "interpretation.json", interp)
    cards = ""
    for key in ["HR_bpm", "PR_ms", "QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]:
        s = summ.get(key, {})
        cards += f'<div class="metric"><span>{esc(LABELS[key])}</span><strong>{fmt(s.get("median"))}<small>{"bpm" if key == "HR_bpm" else "ms"}</small></strong><em>{s.get("n", 0)} measured beats</em></div>'
    comparisons = ""
    comp_rows = ""
    for r in allruns:
        stats = r["measurements"]["summary"]
        cov = " · ".join(
            f"{k}: {v}/{r['visible_beats']}" for k, v in r.get("coverage", {}).items()
        )
        comp_rows += (
            f"<tr><th>{esc(r['name'])}</th><td>{esc(r['status'])}</td>"
            + "".join(
                f"<td>{fmt(stats[k]['median'])} <small>({stats[k]['n']})</small></td>"
                for k in ["PR_ms", "QRS_ms", "QT_ms", "QTcF_ms"]
            )
            + "</tr>"
        )
        details = "".join("<li>" + esc(str(w)) + "</li>" for w in r.get("warnings", []))
        if r.get("error"):
            details += "<li>" + esc(r["error"]) + "</li>"
        problems = [
            f"{b['fid_s']:.3f} s: " + i
            for b in r["measurements"]["beats"]
            for i in b["issues"]
        ]
        details += f"<li>{r.get('landmark_issue_count', 0)} contradictory/out-of-cycle landmark values excluded from measurements; original values retained in raw_marks.</li>"
        details += "".join("<li>" + esc(x) + "</li>" for x in problems)
        comparisons += f'<details class="method"><summary>{esc(r["name"])} <span>{r["visible_beats"]} visible fiducials · {esc(r["status"])}</span></summary><p>{esc(r["preprocessing"])}</p><p class="small">P purple · QRS blue · T green. Open this method in the main interactive plot to inspect every available landmark and its timing.</p><button data-show-method="{esc(r["id"])}">Show annotations in Plotly</button><p class="small">{esc(cov)}</p><ul>{details}</ul>{table(r["measurements"])}</details>'
    issues = "".join(
        "<li>" + esc(x) + "</li>" for x in interp["findings"] + interp["limitations"]
    )
    bounds = []
    for key in ["PR_ms", "QRS_ms", "QT_ms", "QTcF_ms", "QTcB_ms"]:
        values = [b["bounds"][key] for b in m.get("beats", []) if key in b["bounds"]]
        if values:
            bounds.append(
                f"{LABELS[key]}: {np.median([v[0] for v in values]):.0f}–{np.median([v[1] for v in values]):.0f} ms"
            )
    selection_rows = "".join(
        f"<tr><td>{x['start_s']}–{x['end_s']} s</td><td>{fmt(x['motion_rms_mg'], 1)}</td><td>{'Eligible' if x['accepted'] else esc(', '.join(x['reasons']))}</td></tr>"
        for x in selection["candidates"]
    )
    source = esc(
        config.get("fixture_note", "Historical replay; rest protocol not observed.")
    )
    errors = f'<p class="alert">{esc(model_error)}</p>' if model_error else ""
    reviewdata = json.dumps(
        {
            "participant_id": config.get("participant_id", "DEMO"),
            "session_start": config["session_start"],
            "window": c,
            "astra_priority": interp["review_priority"],
            "model_complete": bool(
                astra
                and interpretation
                and m.get("summary", {}).get("QRS_ms", {}).get("n", 0)
            ),
            "findings": interp["findings"],
        },
        allow_nan=False,
    ).replace("<", "\\u003c")
    html = (
        """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Resting ECG · operator review</title><style>"""
        + Path(__file__).with_name("report.css").read_text()
        + """</style></head><body>
    <nav class="no-print"><a href="#">REST / ECG</a><div><button id="audience">Participant view</button><button id="print-strip">Print ECG</button><a class="button" href="measurements.json" download>Export measurements</a></div></nav>
    <main><header><div><p class="eyebrow">RESTING ASSESSMENT · HISTORICAL REPLAY</p><h1>"""
        + participant
        + """<span>Resting single-lead ECG</span></h1><p>Four-minute session · Polar H10 · 10-second measurement<br><span class="small">Replay session: """
        + esc(config["session_start"])
        + """</span></p></div><div class="review-badge">OPERATOR REVIEW PENDING<br><small>No exercise decision recorded</small></div></header>
    <section class="session no-print"><div><b>00:00–02:00</b><span>Settling period</span></div><div class="active"><b>02:00–03:00</b><span>Find a quiet 10 seconds</span></div><div><b>03:00–04:00</b><span>Complete resting session</span></div></section>
    <section class="quality"><b>Acquisition checks passed</b><span>Selected """
        + f"{c['start_s'] // 60:02}:{c['start_s'] % 60:02}–{c['end_s'] // 60:02}:{c['end_s'] % 60:02}"
        + f""" · {c["ecg_samples"]} native ECG samples · motion RMS {c["motion_rms_mg"]:.1f} mg · contact confirmed</span></section>
    <p class="small no-print">"""
        + source
        + """ Low chest motion is an acquisition check, not proof of physiological rest or ECG normality.</p>
    <p class="review-suggestion">Machine review suggestion: """
        + esc(interp["review_priority"].replace("_", " "))
        + """ · operator decision remains pending</p><section class="printout">"""
        + interactive_html(context, runs + drafts, astra, out)
        + """<div class="section-title print-only"><h2>10-second ECG</h2><span>25 mm/s · 10 mm/mV at 100% print scale</span></div><div class="strip-scroll print-only">"""
        + strip(context)
        + """</div><p class="measurement-label">Annotation-derived values · verify QRS/T boundaries before clinical use</p><div class="metrics">"""
        + cards
        + """</div><p class="small">Astra annotation · medians of individual beats · native sample spacing """
        + f"{1000 / context['fs']:.2f}"
        + """ ms · single-lead intervals</p>
    <p class="small">Visual landmark bounds, medians of lower/upper endpoints (not confidence intervals): """
        + esc("; ".join(bounds) or "Unavailable")
        + """</p><div class="interpretation"><h2>Astra interpretation · review draft</h2><p>"""
        + esc(interp["paragraph"])
        + """</p></div><p class="small">This research prototype has not been validated for exercise clearance. A short single-lead recording cannot exclude intermittent arrhythmia, long QT or ischemia. Reviewer: ____________________ Date/time: ____________________</p></section>"""
        + errors
        + """
    <section class="operator no-print"><div class="section-title"><h2>Operator review</h2><span>Record the decision after reviewing the original strip</span></div><p>Symptoms and review start unanswered. “Proceed” records the operator’s decision in this simulation; the model does not grant clearance.</p><div class="form-grid">
    <label>Symptom review<select id="symptoms"><option value="unknown">Not yet assessed</option><option value="negative">No concerning symptoms identified</option><option value="concerning">Exertional or other concerning symptoms</option><option value="urgent">Current symptoms requiring urgent assessment</option></select></label>
    <label>ECG review<select id="ecg-review"><option value="unknown">Not yet reviewed</option><option value="acceptable">No major flag identified by reviewer</option><option value="uncertain">Unable to assess / uncertain finding</option><option value="abnormal">Concerning finding — clinical review</option><option value="urgent">Suspected sustained dangerous rhythm</option><option value="poor">Repeat: inadequate recording</option></select></label>
    <label>Operator name<input id="reviewer" autocomplete="off" placeholder="Required to record a decision"></label><label>Review notes<textarea id="review-notes" placeholder="Findings, symptom details and next steps"></textarea></label>
    <label>Operator decision<select id="decision"><option value="pending">Decision pending</option><option value="proceed">Proceed to exercise briefing</option><option value="defer">Defer exercise; request clinical review</option><option value="repeat">Repeat ECG acquisition</option><option value="urgent">Stop pathway; urgent assessment</option></select></label></div>
    <p id="review-state" role="status">Complete symptom and ECG review.</p><div class="actions"><button id="save-review" disabled>Export operator decision</button><button id="letter-button" disabled>Prepare GP-review letter</button></div><p class="small">Review entries stay on this page until exported; reloading clears them. No exercise equipment is connected. Current concerning chest symptoms or a suspected sustained dangerous rhythm require urgent assessment under the local protocol, not a routine letter alone. <a href="https://www.nhs.uk/conditions/chest-pain/">NHS chest-pain guidance</a>.</p></section>
    <section class="no-print technical"><details><summary>Measurements and annotation review</summary><p>All calculated measurements are shown below. Missing or invalid landmarks remain unavailable. Beat ranges are observed spread, not accuracy estimates.</p>"""
        + table(m)
        + """<ul>"""
        + "".join("<li>" + esc(x) + "</li>" for x in m.get("method_notes", []))
        + """</ul><ul>"""
        + issues
        + """</ul><h3>Method comparison</h3><p>Values are medians in ms; parentheses show measured beats. No expert-adjudicated reference is available, so this is a comparison of outputs, not a ranking of diagnostic accuracy.</p><div class="table-scroll"><table><thead><tr><th>Method</th><th>Run</th><th>PR</th><th>QRS</th><th>QT</th><th>QTcF</th></tr></thead><tbody>"""
        + comp_rows
        + """</tbody></table></div>"""
        + comparisons
        + """</details></section>"""
        + stages_html
        + """
    <section class="no-print technical"><details><summary>Acquisition, full-session overview and provenance</summary><p>Compressed 4-minute ECG overview; min/max retained per display column. The outline marks the selected window. This overview is not at diagnostic scale; the exported archive preserves every sample. Events outside the selected ten seconds have not been adjudicated.</p>"""
        + overview(recording, selection)
        + """<p>Selection used motion, contact and continuity, never normal-looking morphology or interval values. First 120 seconds excluded. Every candidate is fully inside seconds 120–180. Fixed prototype limits: RMS ≤20 mg; peak deviation ≤80 mg. These thresholds need prospective validation.</p><table><thead><tr><th>Session interval</th><th>Motion RMS, mg</th><th>Result</th></tr></thead><tbody>"""
        + selection_rows
        + """</tbody></table><p><a href="native.csv" download>Native samples</a> · <a href="ecg_session.parquet" download>Full session ECG</a> · <a href="acc_session.parquet" download>Full session motion</a> · <a href="hr_session.parquet" download>Full session contact/HR</a> · <a href="provenance.json">Run provenance</a> · <a href="source.json">Source hashes</a> · <a href="selection.json">Candidate audit</a> · <a href="methods.json">All automated annotations</a> · <a href="astra.json">Astra annotations</a></p><p>Astra annotated this strip in one three-step conversation (see “How Astra annotated this strip”); the interpretation is a separate fresh request that receives the final annotation, all machine measurements and the unlabelled waveform images. Interpretation: <a href="astra_interpretation/prompt.md">prompt</a>, <a href="astra_interpretation/input.txt">input</a>, <a href="astra_interpretation/response.txt">raw response</a>. Annotation image hashes: <a href="astra_annotation/request_manifest.json">request manifest</a>.</p></details></section>
    <section id="gp-letter" hidden><p class="eyebrow">DRAFT · REQUEST FOR CLINICAL REVIEW</p><h1>Resting ECG review request</h1><p>Participant reference: """
        + participant
        + """<br>Patient name / DOB: ___________________________<br>GP practice: ___________________________</p><p>Dear colleague,</p><p>Exercise testing has been deferred following an operator review of a resting single-lead ECG and symptom screen. Please review the findings and advise on appropriate assessment before exercise testing is reconsidered. This research recording is not a diagnostic 12-lead ECG.</p><p id="letter-notes"></p><p>Machine-generated tracing description: """
        + esc(interp["paragraph"])
        + """</p><p>Please find the ECG printout attached. The reviewer should verify this draft, add the relevant history and arrange an appropriate urgency of care before giving it to the participant.</p><p id="letter-reviewer"></p><p>Signature: _____________________ Date: ______________</p><button id="print-letter" class="no-print">Print draft letter</button></section>
    <footer>REST / ECG · Prototype 0.1 · DEMONSTRATION · No automatic exercise clearance</footer></main><script>const runData="""
        + reviewdata
        + """;</script><script>"""
        + Path(__file__).with_name("report.js").read_text()
        + """</script></body></html>"""
    )
    (out / "report.html").write_text(html)
