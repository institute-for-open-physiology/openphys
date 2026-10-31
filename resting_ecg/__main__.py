from pathlib import Path
import argparse, json, platform, subprocess, importlib.metadata, datetime
from .core import load_recording, select_window, make_context, write_json, digest
from .delineators import run_all
from .figures import images
from .measures import measure, audit_run
from .astra import interpret
from .informed import annotate_informed


def main():
    parser = argparse.ArgumentParser(
        description="Fresh four-minute archive replay and single-lead ECG report"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    protocol = json.loads(Path(__file__).with_name("protocol.json").read_text())
    recording = load_recording(config)
    selection = select_window(recording, protocol)
    write_json(out / "selection.json", selection)
    write_json(
        out / "source.json",
        {**recording["source"], "fixture_note": config.get("fixture_note", "")},
    )
    for kind in ["ecg", "acc", "hr"]:
        d = recording[kind]
        d[(d.t_s >= 0) & (d.t_s < 240)].to_parquet(
            out / (kind + "_session.parquet"), index=False
        )
    versions = {
        p: importlib.metadata.version(p)
        for p in [
            "numpy",
            "scipy",
            "pandas",
            "pyarrow",
            "matplotlib",
            "neurokit2",
            "PyWavelets",
            "wfdb",
            "requests",
            "plotly",
        ]
    }
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10
    ).stdout.strip()
    write_json(
        out / "provenance.json",
        {
            "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "python": platform.python_version(),
            "packages": versions,
            "git_base": sha,
            "source_files": {
                str(p.relative_to(Path(__file__).parent)): digest(p)
                for p in Path(__file__).parent.rglob("*")
                if p.is_file()
                and p.suffix in [".py", ".m", ".md", ".json", ".js", ".css", ".txt"]
            },
            "participant_id": config.get("participant_id", "DEMO"),
            "protocol": protocol,
            "model": config["model"],
        },
    )
    if not selection["chosen"]:
        from .report import failed_report

        failed_report(out, selection)
        print(out / "report.html", flush=True)
        return
    try:
        context = make_context(recording, selection)
        context["native"].to_csv(out / "native.csv", index_label="sample_index")
        print("Selected window:", selection["chosen"], flush=True)
        runs = run_all(context, config, out / "delineators")
    except Exception as exc:
        from .report import failed_report

        selection["analysis_error"] = f"{type(exc).__name__}: {exc}"
        write_json(out / "selection.json", selection)
        failed_report(out, selection)
        print(out / "report.html", flush=True)
        return
    for run in runs:
        run["measurements"] = measure(audit_run(run), context)
    write_json(out / "methods.json", runs)
    print("Delineators:", [(r["id"], r["status"]) for r in runs], flush=True)
    panels = images(context, out / "figures")
    astra = None
    interpretation = None
    model_error = None
    try:
        print(
            "Starting Astra machine review, informed annotation and visual revision",
            flush=True,
        )
        astra = annotate_informed(context, runs, config, panels, out)
        write_json(out / "astra.json", astra)
        if not astra["measurements"]["summary"]["QRS_ms"]["n"]:
            raise ValueError(
                "No valid in-view Astra QRS intervals after temporal audit"
            )
        print(
            "Astra annotation complete. Starting separate interpretation request",
            flush=True,
        )
        interpretation = interpret(
            astra,
            astra["measurements"],
            runs,
            config,
            out / "astra_interpretation",
            images=panels,
        )
    except Exception as exc:
        model_error = f"{type(exc).__name__}: {exc}"
        write_json(out / "model_error.json", {"error": model_error})
        print(model_error, flush=True)
    from .report import render

    render(
        out,
        config,
        recording,
        selection,
        context,
        runs,
        astra,
        interpretation,
        model_error,
    )
    print(out / "report.html", flush=True)


if __name__ == "__main__":
    main()
