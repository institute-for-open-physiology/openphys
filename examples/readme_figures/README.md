# Data and scripts behind the README figures

Every figure in the top-level [README](../../README.md) is generated from the de-identified data in
this directory by the scripts in `figures/`.

```bash
python3 -m venv .venv && .venv/bin/pip install -r examples/readme_figures/requirements.txt
for f in pvc_a pvc_b hyperacute_t pacing qrs_axis; do
  .venv/bin/python examples/readme_figures/figures/$f.py      # writes docs/images/<name>.png
done
```

Each script also accepts an output path as its first argument.

| Figure | Script | Data | Sensor |
|---|---|---|---|
| [`pvc_a_during_exercise_polar_h10.png`](../../docs/images/pvc_a_during_exercise_polar_h10.png) | `figures/pvc_a.py` | `data/pvc_a/` | Polar H10, 130 Hz single lead |
| [`pvc_b_recovery_polar_h10.png`](../../docs/images/pvc_b_recovery_polar_h10.png) | `figures/pvc_b.py` | `data/pvc_b/` | Polar H10, 130 Hz single lead |
| [`hyperacute_t_exercise_polar_h10.png`](../../docs/images/hyperacute_t_exercise_polar_h10.png) | `figures/hyperacute_t.py` | `data/hyperacute_t/` | Polar H10, 130 Hz single lead |
| [`pacing_spikes_seca_331_12lead.png`](../../docs/images/pacing_spikes_seca_331_12lead.png) | `figures/pacing.py` | `data/pacing/` | seca CT331, 32 kHz native, 12-lead |
| [`qrs_axis_exercise_seca_331.png`](../../docs/images/qrs_axis_exercise_seca_331.png) | `figures/qrs_axis.py` | `data/qrs_axis/` | seca CT331, 2 kHz, 12-lead |

## Layout

- `data/<figure>/` – the raw, unfiltered samples for the windows shown (plus a margin for display
  filtering), the beat annotations each figure needs, and `metadata.json` (sensor, sampling rate,
  units, lead order, context and analysis parameters). CSV for the small single-lead windows, `.npz`
  for the multi-lead ones.
- `figures/` – public plotting scripts; they depend only on numpy, scipy and matplotlib.
- `extract/` – the scripts the maintainers ran against the private study archives to produce
  `data/`. They are published for transparency; source locations are command-line arguments and
  the archives themselves are not public.

Beat detection and delineation for the Polar H10 figures came from a larger private analysis
pipeline; its results (beat sample indices, QRS boundaries) and the hand-reviewed annotations are
exported into `data/` rather than recomputed, and the geometry drawn on the figures is recomputed
from them in `figures/_h10_common.py`.

## Privacy

Participants are identified in this directory only by figure. Times are relative (seconds within the
window, or from the start of the recording); no names, dates, clock times, device serial numbers or
record identifiers are included. Only short windows are published: 6–17 s for the Polar H10
figures, 4 s for the pacing figure, and for the QRS axis figure two 7 s strips plus the individual
beats (−300 to +500 ms) that make up the mean heartbeats.

## Licence

Data and scripts are covered by the repository's [MIT licence](../../LICENSE.md).
