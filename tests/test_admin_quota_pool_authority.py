import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORE_JS = ROOT / "static" / "js" / "core.js"


def _extract_function(source: str, name: str) -> str:
    start = source.index(f"function {name}(")
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"unterminated function: {name}")


def _quota_patch(quota: dict, current_pool: dict | None = None) -> dict:
    source = CORE_JS.read_text(encoding="utf-8")
    function = _extract_function(source, "poolPatchFromQuotaResult")
    script = f"""
{function}
const quota = {json.dumps(quota)};
const currentPool = {json.dumps(current_pool)};
process.stdout.write(JSON.stringify(poolPatchFromQuotaResult(quota, currentPool)));
"""
    result = subprocess.run(
        ["node", "-"],
        input=script,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def _merged_quota_patch(previous: dict, current: dict, current_pool: dict) -> dict:
    source = CORE_JS.read_text(encoding="utf-8")
    merge_function = _extract_function(source, "mergeQuotaSnapClient")
    patch_function = _extract_function(source, "poolPatchFromQuotaResult")
    script = f"""
{merge_function}
{patch_function}
const previous = {json.dumps(previous)};
const current = {json.dumps(current)};
const currentPool = {json.dumps(current_pool)};
const merged = mergeQuotaSnapClient(previous, current);
process.stdout.write(JSON.stringify(poolPatchFromQuotaResult(merged, currentPool)));
"""
    result = subprocess.run(
        ["node", "-"], input=script, text=True, capture_output=True, check=True
    )
    return json.loads(result.stdout)


def _probe_patch(response: dict) -> dict:
    source = CORE_JS.read_text(encoding="utf-8")
    function = _extract_function(source, "poolPatchFromProbeResponse")
    script = f"""
{function}
const response = {json.dumps(response)};
process.stdout.write(JSON.stringify(poolPatchFromProbeResponse(response)));
"""
    result = subprocess.run(
        ["node", "-"], input=script, text=True, capture_output=True, check=True
    )
    return json.loads(result.stdout)


def test_healthy_quota_without_authoritative_pool_cannot_enable_account() -> None:
    patch = _quota_patch(
        {"account_id": "a1", "ok": True, "remaining": 10},
        {"enabled": False, "pool_status": "disabled", "disabled_reason": "admin"},
    )

    assert patch["last_quota"]["ok"] is True
    assert "enabled" not in patch
    assert "pool_status" not in patch
    assert "disabled_reason" not in patch


def test_exhausted_quota_does_not_overwrite_durable_disabled_or_admin_lock() -> None:
    patch = _quota_patch(
        {"account_id": "a1", "ok": True, "exhausted": True, "source": "free_tokens"},
        {"enabled": False, "pool_status": "disabled", "admin_locked": True},
    )

    assert "enabled" not in patch
    assert "pool_status" not in patch
    assert "admin_locked" not in patch
    assert patch["last_quota"]["exhausted"] is True


def test_exhausted_quota_can_paint_cooldown_for_non_disabled_account() -> None:
    patch = _quota_patch(
        {"account_id": "a1", "ok": True, "exhausted": True, "source": "free_tokens"},
        {"enabled": True, "pool_status": "normal"},
    )

    assert patch["pool_status"] == "cooldown"
    assert patch["in_cooldown"] is True
    assert "enabled" not in patch
    assert "disabled_for_quota" not in patch


def test_explicit_authoritative_pool_is_the_only_source_of_enabled_state() -> None:
    patch = _quota_patch(
        {
            "account_id": "a1",
            "ok": True,
            "pool_authoritative": True,
            "pool": {"enabled": True, "pool_status": "normal", "in_cooldown": False},
        },
        {"enabled": False, "pool_status": "disabled"},
    )

    assert patch["enabled"] is True
    assert patch["pool_status"] == "normal"
    assert patch["in_cooldown"] is False


def test_old_authoritative_pool_is_not_reused_by_later_quota_only_response() -> None:
    patch = _merged_quota_patch(
        {
            "account_id": "a1",
            "ok": True,
            "pool_authoritative": True,
            "pool": {"enabled": True, "pool_status": "normal"},
        },
        {"account_id": "a1", "ok": True, "remaining": 9},
        {"enabled": False, "pool_status": "disabled"},
    )

    assert "enabled" not in patch
    assert "pool_status" not in patch


def test_successful_probe_without_authoritative_pool_cannot_enable_or_clear_cooldown() -> None:
    patch = _probe_patch(
        {
            "ok": True,
            "result": {
                "available": True,
                "outcome": "success",
                "probe_status": "ok",
                "model": "grok-4.5",
            },
            "pool": {
                "account_id": "a1",
                "probe_succeeded": True,
                "pool_authoritative": False,
            },
        }
    )

    assert "enabled" not in patch
    assert "pool_status" not in patch
    assert "in_cooldown" not in patch
    assert patch["last_probe_status"] == "ok"
