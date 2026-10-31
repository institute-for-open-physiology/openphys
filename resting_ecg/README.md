# Resting ECG workflow

A standalone **research prototype**: replay four minutes of Polar H10 ECG,
accelerometer and contact data; select a quiet ten-second strip; run independent
delineators; have Astra review them, annotate the original samples and check its own plotted draft; calculate
measurements; request a separate interpretation; render an offline operator HTML
and a calibrated printable ECG. It does not control exercise equipment or grant
automatic exercise clearance. Human-labeller-level performance is an unverified
future goal, not a demonstrated property of this prototype.

## Run from scratch

From this repository root, using Python 3.13:

```sh
python3 -m venv .venv-resting-ecg
.venv-resting-ecg/bin/pip install -r resting_ecg/requirements.txt
.venv-resting-ecg/bin/python -m resting_ecg \
  --config /absolute/private/path/config.json \
  --out /absolute/private/path/new_run
```

Copy `config.example.json` to a private directory and set the paths and sensor ID.
The output directory must not exist: a run never silently reuses old annotations.
The entrypoint uses no conversation state and never reads earlier model results.
All inputs, figures, annotations and health results should remain in ignored
`data/analysis/` or another private directory, not tracked source. The subject ID
is a pseudonym; no demographics, historical diagnosis or prior ECG is supplied to
Astra. `source_mode` is documentary; this version supports archive replay only.

Each input directory contains `ecg.parquet`, `acc.parquet`, `hr.parquet` with the
collector schema (device timestamp, local-since-2000 epoch, sensor/session/
connection identity, ECG µV, acceleration mg; HR receipt time and contact flags).
Configure an explicitly offset session start in the recording's local time.
This version supports a session entirely within one UTC offset; it does not infer
historical device timezone changes. ECG/ACC device clock and HR receipt clock
are not independently synchronized by this script. Keep clock-offset uncertainty
in mind when validating movement/contact correlation prospectively.

The replay requires four-minute coverage and real continuous ~70-second context
around the selected window. Device-time gaps or identity changes are never filled
with invented data. Session ECG/ACC/HR are exported unchanged apart from adding
relative time. The first 120 seconds are excluded; 51 one-second-stepped candidates
fit entirely into [120,180). Motion is RMS distance from the ten-second median
three-axis acceleration, with a peak-deviation limit. Candidates also require ECG,
ACC and contact continuity. Lowest eligible RMS wins; ties choose the earlier
window. **No normal-looking shape or interval enters this selection.** Thresholds
20 mg RMS / 80 mg peak are engineering defaults requiring validation. No eligible
window produces an operator failure report, not a forced result. A quiet interval
does not prove physiological rest or exclude artifacts. The four-minute overview
is compressed; events outside the selected strip are not adjudicated.

## External dependencies

`dependency_root` must contain these installed tools (not previous results):

- `vendor/ecgpuwave-1.3.4/ecgpuwave`, built against WFDB. Official source:
  https://physionet.org/content/ecgpuwave/1.3.4/
- `vendor/ecg-kit/`, https://github.com/marianux/ecg-kit (Wavedet interface).
- `vendor/ECGdeli/`, https://github.com/KIT-IBT/ECGdeli.
- The bundled `resting_ecg/octave_compat/` provides narrow experimental adapters;
  wavelet coefficients are generated from pinned PyWavelets into each run. This
  is not a claim that stock Octave or MATLAB is equivalent. Exact adapter hashes
  and vendor revisions are saved with each run.

Octave must be on PATH, with `signal`, `image` and `statistics` packages installed.
The implementation uses each toolbox's individual-lead path; it does not duplicate
one channel into twelve. Wavedet/ECGdeli MATLAB parity has not been established.
Nine automated variants are attempted: NeuroKit DWT/CWT/peak/prominence,
ecgpuwave internal/NeuroKit fiducials, Wavedet, ECGdeli default/positive-R option.
Individual tool failures remain visible. Common setup failures produce a failure
report. Interpolation to 500 Hz serves tool compatibility and adds no information
above the original ~65 Hz Nyquist frequency. Full tool logs/raw outputs are retained.

## Astra requests and credentials

The owner must authorize external processing of the recording before setting
`external_model_authorized: true`. The model is explicitly `gpt-6-astra`.
The client uses the installed Codex login through the existing Codex Responses
transport at `chatgpt.com/backend-api/codex/responses`; this is an account-specific
integration, not the public OpenAI API contract. An expired login fails explicitly:
refresh it through Codex. The workflow does not refresh/rotate shared credentials.
Tokens remain in memory and are never copied into artifacts or logged. HTTP
connect/read and total-stream time limits bound requests.

Astra annotates in one conversation with three requests, replayed explicitly
(`store: false`; no server-side conversation state), reproducing how the owner
originally obtained a good annotation:

1. **Machine review** (`prompts/review_machines.md`): seven unlabelled PNG panels,
   the native-index/time/µV CSV, every delineator's landmarks, measurements,
   warnings and failures for beats inside the supplied samples, and labelled plots
   of representative machine beats. Astra assesses each method and their disagreements.
2. **Draft** (`prompts/annotate.md`): with that review in context, Astra returns
   per-beat native-index landmarks and plausible visual ranges.
3. **Plot review** (`prompts/review_annotation.md`): every draft beat is plotted at
   full and enlarged amplitude on the original samples; Astra reviews the plots and
   returns the final annotation plus `review_notes`. No fallback to the draft occurs
   if this step fails.

Python validates indices, temporal association and schemas, and calculates
intervals and voltages. A separate fresh request (`prompts/interpret.md`) gets the
unlabelled waveform images, the final annotation, deterministic measurements and
comparator summaries/issues and returns the interpretation. For every request, the
full prompt, input text, replayed transcript, response text, image hashes, model
ID, response ID, usage and elapsed time are saved. An API-provided reasoning
summary is requested and saved when returned; private reasoning is not available.
No earlier run's output ever enters a request. The private run config sets
`model_effort`; the owner's preferred interactive annotation used `xhigh`.

A completed request does not prove a valid annotation: absent in-view QRS
annotations or unusable boundaries cannot unlock the operator Proceed path.
Values that contradict beat/phase association are retained under `raw_marks`,
excluded from calculations, and shown as issues. No normal-range filtering removes
wide beats or long intervals. Bounds are plausible annotation ranges, not confidence
intervals. Missing quantities stay null. QT correction is per beat with preceding
RR, and QTc bounds hold RR fixed. No QT adaptation/hysteresis model is applied.

## Operator and participant output

Open `report.html`. The default view starts with symptom assessment, ECG review
and decision **pending**. The main screen shows an offline Plotly plot with Astra landmarks visible. Use
the method selectors, comparison overlay, beat navigation, QT rulers and hover
labels to review each boundary. Visual ranges can be toggled; no annotation or
measurement is modified by these controls. Method accordions retain coverage,
invalid-mark issues and all measurements. The paper grid is confined to the
calibrated print view. Download JSON
and native CSV for auditing. The participant-view button hides technical/operator
controls; it does not change findings.

The operator can document: proceed to the exercise briefing, defer and request
clinical review, repeat acquisition, or urgent assessment. Proceed needs a negative
symptom screen, an acceptable human ECG review, a named reviewer and completed,
routine-review model analysis. Nonroutine model findings cannot be silently
acknowledged away in this prototype; resolution requires clinical review/repeat
analysis. Urgent symptoms or suspected sustained dangerous rhythm select the urgent
path, for which a routine GP letter is not offered. A nonurgent deferral can prepare
a printable **draft** GP-review letter; verify/add patient details, clinical history,
urgency and signature before issue. Nothing is sent or physically printed by the
workflow. The HTML buttons invoke the browser's print dialog. Operator decisions
are local until exported as JSON; page reload clears them. These limited screen
choices are not a complete approved study eligibility or emergency protocol.

Print ECG: A4 landscape, 100% scale, no browser headers/footers. The SVG trace is
250 mm over ten seconds (25 mm/s) and 10 mm/mV, with a 1 mV calibration pulse.
Printer scaling can change physical dimensions; verify with a ruler before use.
The GP letter prints separately. Normal-looking single-lead findings cannot exclude
intermittent arrhythmia, long QT or ischemia, nor establish fitness for exertion.
Chest-pain escalation reference: https://www.nhs.uk/conditions/chest-pain/.

## Verification

```sh
python -m pip install -r resting_ecg/requirements-dev.txt
python -m pytest resting_ecg/tests -q \
  --cov=resting_ecg.core --cov=resting_ecg.measures --cov=resting_ecg.astra \
  --cov=resting_ecg.interactive --cov=resting_ecg.informed
python resting_ecg/check_report.py /absolute/private/path/new_run
```

The browser check needs Playwright and a local Chrome installation. Pure logic
regressions exercise selection failures, abnormal-looking morphology retention,
timing, invalid annotations, uncertainty, formula units and credential-safe SSE
parsing. Tool adapters and rendering are additionally covered by actual archive
replay, raw-output checks and browser/print inspection; see `docs/test_coverage.md`.
No collector/runtime changes, deployment or live-sensor interruption are involved.


## Optional identical-input repeatability audit

Independent fresh requests can produce materially different landmarks even with
identical images, samples, prompt, model and effort. This was observed in the
prototype replay. Preserve all runs; do not choose the lowest QT or closest match
to another method. To add a local comparison to the latest HTML (without sending
prior responses to Astra):

```sh
python -m resting_ecg.repeatability --report /private/run04 \
  /private/run02 /private/run03 /private/run04
```

For machine-informed runs, later stages replay each run's own responses and draft
plots, so the check compares model/effort, the first stage's data and images, and
every stage's prompt. This checks the input/settings hashes and distinct response IDs, shows each run's
medians and links its raw response, and blocks the prototype Proceed option if QRS
or QT medians differ by more than two native sample periods. That is an engineering
repeatability check, not a diagnostic tolerance or accuracy estimate. The report
continues to show the latest run; it does not select a preferred annotation. The printout includes a compact repeatability warning; the full run table stays in
the operator HTML.

ECGdeli-specific caveat: QRS boundaries in this upstream revision are constructed
by extending its Q/S positions by `round(25 ms * fs)` each side (26 ms at 500 Hz).
They are not independent onset/offset observations. Q/S can be fallbacks and the
default R field can denote a dominant negative deflection; inspect the original
landmarks before interpreting its Q/R/S amplitudes.


## Brief prompts and annotation comparison

The clinical prompts now follow the owner's original brief requests: visual ECG
annotation using ECG standards, then machine-style interpretation. JSON fields and
native index/range conventions are an output contract, not instructions about
where QRS or T should end. The superseded detailed prompts remain in
`prompts/archive/`; each run retains the exact version it received.

To display an older annotation on exactly the same source waveform, a private run
can contain `annotation_comparisons.json` with `target` (this report's absolute run
path) and `references` (absolute run-directory paths). Regenerating the report
validates the target, the model and that each reference's saved native samples
equal the current waveform. Differences in workflow, reasoning effort or prompt
are allowed **only for this display comparison** and are named in the Plotly label
(for example “different workflow, different effort”). Prior annotations enter the
display only, never a new annotation request. The separate repeatability check
still requires identical settings, first-stage inputs and prompts for every stage.

Plotly JavaScript is bundled from the pinned local Python package directly into
HTML; no CDN or external waveform request is required. Plain seconds/mV axes are
for on-screen review. Print ECG continues to use its calibrated SVG, not the
interactive plot's pixel dimensions. No shorter endpoint is automatically treated
as more accurate, and no cross-prompt comparison changes the stored measurements.
