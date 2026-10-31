"""Offline browser, decision/export and physical-print smoke test on an actual report."""

from pathlib import Path
import json, sys
from playwright.sync_api import sync_playwright

root = Path(sys.argv[1]).resolve()
errors = []
external = []
with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    page = browser.new_page(
        viewport={"width": 1440, "height": 1080}, device_scale_factor=1
    )
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on(
        "request",
        lambda r: (
            external.append(r.url) if r.url.startswith(("https:", "http:")) else None
        ),
    )
    page.goto((root / "report.html").as_uri())
    page.wait_for_load_state("load")
    page.wait_for_function("document.getElementById('ecg-plot').dataset.ready==='true'")
    assert page.locator("#symptoms").input_value() == "unknown"
    assert (
        page.locator("#save-review").is_disabled()
        and page.locator("#letter-button").is_disabled()
    )
    page.screenshot(path=str(root / "operator.png"), full_page=True)
    assert page.locator(".metric").count() == 6
    assert page.locator(".method").count() == 10
    # No accidental clearance after symptom/ECG review if the model has a concern.
    page.locator("#reviewer").fill("DEMO reviewer")
    page.locator("#symptoms").select_option("negative")
    page.locator("#ecg-review").select_option("acceptable")
    page.locator("#decision").select_option("proceed")
    priority = page.evaluate("runData.astra_priority")
    expected_block = priority != "routine_review" or page.evaluate(
        "!!runData.repeatability_unresolved || !runData.model_complete"
    )
    assert page.locator("#save-review").is_disabled() == expected_block
    page.locator("#ecg-review").select_option("abnormal")
    page.locator("#decision").select_option("defer")
    assert (
        page.locator("#save-review").is_enabled()
        and page.locator("#letter-button").is_enabled()
    )
    page.locator("#review-notes").fill(
        "Demonstration review only. Verify uncertain boundaries before clinical use."
    )
    with page.expect_download() as dl:
        page.locator("#save-review").click()
    exported = json.loads(Path(dl.value.path()).read_text())
    assert exported["demo"] is True and exported["decision"] == "defer"
    page.locator("#letter-button").click()
    assert page.locator("#gp-letter").is_visible()
    page.locator("#symptoms").select_option("urgent")
    assert (
        page.locator("#letter-button").is_disabled()
        and page.locator("#gp-letter").is_hidden()
    )
    page.locator("#decision").select_option("proceed")
    assert page.locator("#save-review").is_disabled()
    page.locator("#decision").select_option("urgent")
    assert page.locator("#save-review").is_enabled()
    # Explicitly exercise the prototype's routine-analysis path in browser memory only.
    page.evaluate(
        "runData.astra_priority='routine_review';runData.repeatability_unresolved=false"
    )
    page.locator("#symptoms").select_option("negative")
    page.locator("#ecg-review").select_option("acceptable")
    page.locator("#decision").select_option("proceed")
    assert page.locator("#save-review").is_enabled()
    page.evaluate("runData.model_complete=false")
    page.locator("#reviewer").fill("DEMO reviewer changed")
    assert page.locator("#save-review").is_disabled()
    page.reload()
    page.locator("#audience").click()
    assert page.locator(".operator").is_hidden()
    page.locator("#audience").click()
    page.locator(".technical>details").first.locator("summary").first.click()
    page.locator(".method").last.locator("summary").click()
    page.locator(".method").last.locator("[data-show-method]").click()
    assert page.locator("#plot-method").input_value() == "astra"
    assert page.locator("#ecg-plot").is_visible()
    page.locator("#plot-qt").click()
    page.wait_for_function(
        "document.getElementById('ecg-plot').layout.shapes.length >= 3"
    )
    page.wait_for_function("document.getElementById('ecg-plot').dataset.ready==='true'")
    page.evaluate(
        """() => { const g=document.getElementById('ecg-plot'); const i=g.data.findIndex(t=>t.name==='T_off'); Plotly.Fx.hover(g,[{curveNumber:i,pointNumber:0}]); }"""
    )
    assert "T_off" in page.locator("#ecg-plot .hoverlayer").text_content()
    assert (
        "Nearest native sample" in page.locator("#ecg-plot .hoverlayer").text_content()
    )
    page.evaluate("Plotly.Fx.unhover(document.getElementById('ecg-plot'))")
    page.locator("#plot-next").click()
    assert page.locator("#plot-beat").input_value() == "1"
    page.locator("#plot-prev").click()
    assert page.locator("#plot-beat").input_value() == "0"
    page.locator("#plot-uncertainty").uncheck()
    page.wait_for_function(
        "document.getElementById('ecg-plot').data.filter(t=>t.error_x).every(t=>!t.error_x.visible)"
    )
    page.locator("#plot-uncertainty").check()
    page.locator("#plot-full").click()
    page.wait_for_function(
        "document.getElementById('ecg-plot').layout.xaxis.range[1]===10"
    )
    page.locator("#plot-qt").click()
    page.locator("#annotation-review").screenshot(path=str(root / "astra_review.png"))
    page.reload()
    page.emulate_media(media="print")
    svg_width = page.locator(".print-only svg").evaluate(
        "(el)=>el.getBoundingClientRect().width"
    )
    assert abs(svg_width - 270 * 96 / 25.4) < 0.1
    page.pdf(
        path=str(root / "ecg_printout.pdf"),
        prefer_css_page_size=True,
        print_background=True,
        display_header_footer=False,
    )
    page.emulate_media(media="screen")
    page.set_viewport_size({"width": 390, "height": 844})
    page.wait_for_function("document.documentElement.scrollWidth<=window.innerWidth")
    page.screenshot(path=str(root / "mobile.png"), full_page=True)
    assert page.evaluate("document.documentElement.scrollWidth<=window.innerWidth")
    assert not errors, errors
    assert not external, external
    browser.close()
print(
    json.dumps(
        {
            "status": "passed",
            "javascript_errors": errors,
            "external_requests": external,
            "print_svg_width_mm": 270,
            "trace_width_mm": 250,
            "screenshot_files": ["operator.png", "astra_review.png", "mobile.png"],
            "pdf": "ecg_printout.pdf",
        },
        indent=2,
    )
)
