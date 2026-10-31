import json

from resting_ecg.report import astra_stages

HOSTILE = '<img src=x onerror="alert(1)">'


def summary():
    return {
        "summary": {
            k: {"median": 100.0} for k in ["PR_ms", "QRS_ms", "QT_ms", "QTcF_ms"]
        }
    }


def test_annotation_conversation_is_escaped_and_draft_offered_for_plotting(tmp_path):
    (tmp_path / "astra_machine_review").mkdir()
    (tmp_path / "astra_machine_review/response.json").write_text(
        json.dumps(
            {
                "paragraph": HOSTILE,
                "methods": [{"id": "wavedet", "assessment": HOSTILE}],
                "disagreements": [HOSTILE],
            }
        )
    )
    draft = {"id": "astra_draft", "name": HOSTILE, "measurements": summary()}
    (tmp_path / "astra_draft.json").write_text(json.dumps(draft))
    (tmp_path / "methods.json").write_text(
        json.dumps([{"id": "wavedet", "name": "Wavedet"}])
    )
    astra = {"name": "Astra annotation", "review_notes": HOSTILE}
    astra["measurements"] = summary()
    drafts, html = astra_stages(tmp_path, astra)
    assert drafts == [draft]
    assert "<img" not in html and html.count("&lt;img") == 5
    assert "<b>Wavedet</b>" in html
    assert 'href="astra_annotation/response.txt"' in html


def test_missing_stage_files_or_failed_annotation_add_nothing(tmp_path):
    astra = {"name": "Astra annotation", "measurements": summary()}
    assert astra_stages(tmp_path, astra) == ([], "")
    assert astra_stages(tmp_path, None) == ([], "")
