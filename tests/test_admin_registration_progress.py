from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORE_JS = ROOT / "static" / "js" / "core.js"
ACCOUNTS_HTML = ROOT / "static" / "admin" / "accounts.html"
ADMIN_CSS = ROOT / "static" / "css" / "admin-antd.css"


def run_normalizer(payload: dict[str, object]) -> dict[str, int]:
    source = CORE_JS.read_text(encoding="utf-8")
    match = re.search(
        r"function normalizeRegProgress\(stats = \{\}\) \{.*?\n\}",
        source,
        flags=re.DOTALL,
    )
    assert match, "normalizeRegProgress must remain a standalone pure function"
    script = f"{match.group(0)}\nconsole.log(JSON.stringify(normalizeRegProgress({json.dumps(payload)})));"
    result = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_progress_renders_cancelled_and_unattempted_separately() -> None:
    html = ACCOUNTS_HTML.read_text(encoding="utf-8")

    assert 'id="reg-stat-stop"' in html
    assert '>已取消<' in html
    assert 'id="reg-stat-probing"' in html
    assert '>测活中<' in html
    assert 'id="reg-stat-unattempted"' in html
    assert '>未尝试<' in html


def test_registration_workbench_is_fluid_and_settings_use_dialog() -> None:
    html = ACCOUNTS_HTML.read_text(encoding="utf-8")
    css = ADMIN_CSS.read_text(encoding="utf-8")

    assert '<dialog id="reg-settings-dialog"' in html
    assert 'id="btn-open-reg-settings"' in html
    assert html.index('id="reg-settings-dialog"') < html.index('id="reg-mail-provider"')
    assert html.index('id="reg-session-box"') < html.index('id="btn-start-reg"')
    assert 'id="reg-session-box" class="g2a-subcard g2a-reg-progress"' in html
    assert 'body[data-page="accounts"] .g2a-content' in css
    assert 'flex-wrap: nowrap;' in css
    assert 'flex: 0 0 auto;' in css


def test_progress_log_explains_auto_stop_and_rate_limit_reset() -> None:
    source = CORE_JS.read_text(encoding="utf-8")

    assert "自动停止: ${String(batch.stop_reason)}" in source
    assert "限流恢复时间: ${resetText}" in source
    assert "batchTerminal ? batchProgress.unattempted : 0" in source
    assert "s && s.probe && s.probe.pending" in source
    assert '"reg-stat-probing": probing' in source


def test_done_placeholder_is_a_terminal_success() -> None:
    source = CORE_JS.read_text(encoding="utf-8")

    assert 'const REG_TERMINAL_OK = new Set(["success", "completed", "imported", "done"]);' in source
    assert "list.length ? list : (finished ? [] : placeholderSessions)" in source


def test_stopped_unscheduled_jobs_are_not_counted_as_failures() -> None:
    progress = run_normalizer(
        {
            "total": 500,
            "success": 5,
            "failed": 495,
            "running": 0,
            "probing": 4,
            "cancelled_count": 1,
            "remaining": 321,
            "status": "cancelled",
        }
    )

    assert progress == {
        "total": 500,
        "success": 5,
        "fail": 173,
        "running": 0,
        "probing": 4,
        "cancelled": 1,
        "unattempted": 321,
    }


def test_progress_accepts_new_and_legacy_counter_names() -> None:
    current = run_normalizer(
        {
            "count": 20,
            "imported": 4,
            "error": 3,
            "running": 2,
            "probing": 3,
            "cancelled": 1,
            "not_attempted": 10,
        }
    )
    legacy = run_normalizer(
        {
            "count": 10,
            "ok": 2,
            "fail": 3,
            "running": 0,
            "probing": 0,
            "stop": 1,
            "status": "stopped",
        }
    )

    assert current == {
        "total": 20,
        "success": 4,
        "fail": 3,
        "running": 2,
        "probing": 3,
        "cancelled": 1,
        "unattempted": 10,
    }
    assert legacy == {
        "total": 10,
        "success": 2,
        "fail": 3,
        "running": 0,
        "probing": 0,
        "cancelled": 1,
        "unattempted": 4,
    }
