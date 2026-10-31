"""Fresh Codex Responses requests. Credentials stay in memory, never in artifacts."""

from pathlib import Path
import base64, json, time
import requests
from .core import write_json, digest
from .delineators import LANDMARKS

ENDPOINT = "https://chatgpt.com/backend-api/codex/responses"


def credentials(path):
    d = json.loads(Path(path).expanduser().read_text())
    t = d.get("tokens", d)
    token = t["access_token"]
    account = t["account_id"]
    payload = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "==="))
    if payload.get("exp", 0) < time.time() + 300:
        raise RuntimeError(
            "Codex access token expired or expiring; authenticate with Codex and retry. No shared credential was rotated."
        )
    return token, account


def request(config, prompt, text, images, out, history=()):
    out = Path(out)
    out.mkdir()
    if config.get("external_model_authorized") is not True:
        raise ValueError("Explicit external model authorization required in config")
    if config["model"] != "gpt-6-astra":
        raise ValueError("This run requires the explicitly requested gpt-6-astra model")
    token, account = credentials(
        config.get("codex_auth_file", Path.home() / ".codex/auth.json")
    )
    transcript = list(history) + [
        {"role": "user", "text": text, "images": list(map(str, images))}
    ]
    messages = []
    image_hashes = {}
    for turn in transcript:
        assistant = turn["role"] == "assistant"
        content = [
            {"type": "output_text" if assistant else "input_text", "text": turn["text"]}
        ]
        for p in turn.get("images", []):
            name = Path(p).name
            sha = digest(p)
            if name in image_hashes and image_hashes[name] != sha:
                raise ValueError("Conflicting image names in request history")
            image_hashes[name] = sha
            content.append(
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,"
                    + base64.b64encode(Path(p).read_bytes()).decode(),
                    "detail": "high",
                }
            )
        message = {"role": turn["role"], "content": content}
        if assistant:
            message["phase"] = "final_answer"
        messages.append(message)
    payload = {
        "model": config["model"],
        "store": False,
        "stream": True,
        "instructions": prompt,
        "input": messages,
        "reasoning": {"effort": config.get("model_effort", "high"), "summary": "auto"},
        "text": {"verbosity": "low"},
    }
    (out / "prompt.md").write_text(prompt)
    (out / "input.txt").write_text(text)
    write_json(out / "history.json", list(history))
    write_json(out / "transcript.json", transcript)
    write_json(
        out / "request_manifest.json",
        {
            "model": payload["model"],
            "reasoning_effort": payload["reasoning"]["effort"],
            "endpoint": ENDPOINT,
            "conversation": "New workflow; earlier stages replayed explicitly"
            if history
            else "fresh request; no history or prior response id",
            "image_hashes": image_hashes,
            "history_sha256": digest(out / "history.json"),
            "transcript_sha256": digest(out / "transcript.json"),
            "reasoning_summary_requested": "auto",
            "prompt_sha256": digest(out / "prompt.md"),
            "input_sha256": digest(out / "input.txt"),
        },
    )
    begin = time.monotonic()
    complete = None
    deltas = []
    done_text = []
    items = {}
    event_types = set()
    summaries = {}
    headers = {
        "Authorization": "Bearer " + token,
        "chatgpt-account-id": account,
        "originator": "pi",
        "User-Agent": "resting-ecg-research/0.1",
        "OpenAI-Beta": "responses=experimental",
        "Accept": "text/event-stream",
    }
    with requests.post(
        ENDPOINT, headers=headers, json=payload, stream=True, timeout=(20, 180)
    ) as response:
        if response.status_code != 200:
            raise RuntimeError(
                f"Codex request failed with HTTP {response.status_code}; response body omitted to protect credentials"
            )
        for line in response.iter_lines():
            if time.monotonic() - begin > 900:
                raise TimeoutError("Astra exceeded 15 minute total request limit")
            if not line.startswith(b"data:"):
                continue
            data = line[5:].strip()
            if data == b"[DONE]":
                break
            event = json.loads(data)
            event_types.add(event.get("type", "unknown"))
            if (
                event.get("type") == "response.output_item.done"
                and event.get("item", {}).get("type") == "reasoning"
            ):
                item = event["item"]
                summaries[item.get("id", str(event.get("output_index")))] = [
                    s.get("text", "")
                    for s in item.get("summary", [])
                    if s.get("type") == "summary_text"
                ]
            if event.get("type") == "response.output_text.delta":
                deltas.append(event.get("delta", ""))
            if event.get("type") == "response.output_text.done":
                done_text.append(event.get("text", ""))
            if (
                event.get("type") == "response.output_item.done"
                and event.get("item", {}).get("type") == "message"
            ):
                items[event.get("output_index", len(items))] = event["item"]
            if event.get("type") == "response.completed":
                complete = event["response"]
                break
            if event.get("type") in ["error", "response.failed", "response.incomplete"]:
                raise RuntimeError("Astra returned " + event["type"])
    if not complete or complete.get("status") != "completed":
        raise RuntimeError("No completed Astra response")
    reported_model = complete.get("model", "")
    if not reported_model.startswith(config["model"]):
        raise RuntimeError("Unexpected response model: " + reported_model)
    output = "".join(
        c.get("text", "")
        for o in complete.get("output", [])
        if o.get("type") == "message"
        for c in o.get("content", [])
        if c.get("type") == "output_text"
    )
    item_text = "".join(
        c.get("text", "")
        for i in sorted(items)
        for c in items[i].get("content", [])
        if c.get("type") == "output_text"
    )
    output = output or item_text or "".join(done_text) or "".join(deltas)
    (out / "response.txt").write_text(output)
    for item in complete.get("output", []):
        if item.get("type") == "reasoning":
            summaries[item.get("id", "completed")] = [
                s.get("text", "")
                for s in item.get("summary", [])
                if s.get("type") == "summary_text"
            ]
    write_json(
        out / "reasoning_summary.json",
        {
            "available": any(summaries.values()),
            "summaries": list(summaries.values()),
            "note": "API-provided summary, not private internal reasoning.",
        },
    )
    write_json(
        out / "response_metadata.json",
        {
            "model": reported_model,
            "response_id": complete.get("id"),
            "status": complete["status"],
            "usage": complete.get("usage"),
            "elapsed_s": time.monotonic() - begin,
            "event_types": sorted(event_types),
        },
    )
    if not output:
        raise RuntimeError(
            "Completed response had no output text; inspect event types in metadata"
        )
    stripped = output.strip()
    if stripped.startswith("```"):
        stripped = "\n".join(stripped.splitlines()[1:-1])
    result = json.loads(stripped)
    write_json(out / "response.json", result)
    return result


def normalize(annotation, native):
    if not isinstance(annotation, dict) or not annotation.get("beats"):
        raise ValueError("Astra returned no beats")
    for k in [
        "method",
        "overall_quality",
        "convention",
        "observations",
        "unassessable",
    ]:
        if k not in annotation:
            raise ValueError("Missing annotation field " + k)
    if not all(
        isinstance(annotation[k], str)
        for k in ["method", "overall_quality", "convention"]
    ) or not all(
        isinstance(annotation[k], list)
        and all(isinstance(x, str) for x in annotation[k])
        for k in ["observations", "unassessable"]
    ):
        raise ValueError("Incorrect annotation narrative schema")
    t = native.relative_s.to_numpy()
    beats = []
    previous = -1
    for b in annotation["beats"]:
        fid = b["fid_index"]
        if type(fid) is not int or not 0 <= fid < len(t) or fid <= previous:
            raise ValueError("Invalid or unordered Astra fiducial")
        previous = fid
        if b["confidence"] not in ["high", "medium", "low"] or not isinstance(
            b["notes"], str
        ):
            raise ValueError("Invalid confidence or notes")
        if set(b["marks"]) != set(LANDMARKS):
            raise ValueError("Incorrect landmark schema")
        marks = {}
        ranges = {}
        for k, v in b["marks"].items():
            if v is None:
                marks[k] = None
                ranges[k] = None
                continue
            if (
                not isinstance(v, list)
                or len(v) != 3
                or any(type(i) is not int for i in v)
                or not 0 <= v[1] <= v[0] <= v[2] < len(t)
            ):
                raise ValueError("Invalid index/range for " + k)
            marks[k] = float(t[v[0]])
            ranges[k] = [float(t[v[1]]), float(t[v[2]])]
        beats.append(
            {
                "fid_s": float(t[fid]),
                "marks": marks,
                "ranges": ranges,
                "confidence": b["confidence"],
                "notes": [b["notes"]],
            }
        )
    if not any(0 <= b["fid_s"] < 10 for b in beats):
        raise ValueError("No in-view Astra fiducial; unusable annotation")
    if not any(
        0 <= b["fid_s"] < 10
        and b["marks"]["QRS_on"] is not None
        and b["marks"]["QRS_off"] is not None
        and b["marks"]["QRS_on"] <= b["fid_s"] <= b["marks"]["QRS_off"]
        for b in beats
    ):
        raise ValueError("No measurable in-view QRS; unusable annotation")
    return {
        "id": "astra",
        "name": "Astra annotation",
        "status": "ok",
        "version": "gpt-6-astra",
        "beats": beats,
        "warnings": [annotation["overall_quality"], annotation["convention"]],
        "preprocessing": "Unfiltered native ECG; visual annotation after reviewing every machine delineation, then revised against plots of its own draft.",
        "observations": annotation["observations"],
        "unassessable": annotation["unassessable"],
        "visible_beats": sum(0 <= b["fid_s"] < 10 for b in beats),
        "coverage": {
            k: sum(b["marks"][k] is not None for b in beats if 0 <= b["fid_s"] < 10)
            for k in LANDMARKS
        },
    }


def interpret(astra, measurements, runs, config, out, images=()):
    data = {
        "astra_annotation": astra,
        "astra_measurements": measurements,
        "comparators": [
            {
                "name": r["name"],
                "status": r["status"],
                "summary": r.get("measurements", {}).get("summary"),
                "warnings": r["warnings"],
                "landmark_issue_count": r.get("landmark_issue_count", 0),
                "issues": sorted(
                    set(
                        i
                        for b in r.get("measurements", {}).get("beats", [])
                        for i in b["issues"]
                    )
                ),
            }
            for r in runs
        ],
    }
    result = request(
        config,
        Path(__file__).with_name("prompts").joinpath("interpret.md").read_text(),
        json.dumps(data, allow_nan=False),
        images,
        out,
    )
    if not isinstance(result.get("paragraph"), str) or not all(
        isinstance(result.get(k), list) and all(isinstance(x, str) for x in result[k])
        for k in ["findings", "limitations"]
    ):
        raise ValueError("Invalid interpretation schema")
    if result.get("review_priority") not in [
        "routine_review",
        "repeat_recording",
        "prompt_clinical_review",
        "urgent_assessment",
    ]:
        raise ValueError("Invalid review priority")
    return result
