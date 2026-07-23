from __future__ import annotations

from contextlib import nullcontext

from grok2api.pool.auth import GrokCredentials


class _FakeResponse:
    def __init__(self, lines: list[str], status_code: int = 200) -> None:
        self.status_code = status_code
        self._lines = lines

    def iter_lines(self):
        yield from self._lines

    def read(self) -> bytes:
        return b""


class _FakeClient:
    def __init__(self, lines: list[str]) -> None:
        self.response = _FakeResponse(lines)
        self.request_json = None

    def stream(self, _method, _url, *, headers, json):
        self.request_json = json
        return nullcontext(self.response)


def _creds() -> GrokCredentials:
    return GrokCredentials(
        token="test-token", email="a@example.com", auth_key="account-1"
    )


def test_probe_consumes_complete_stream_and_rejects_late_failure(monkeypatch) -> None:
    from grok2api.pool import account_pool, model_health

    client = _FakeClient(
        [
            'data: {"type":"response.output_text.delta","delta":"OK"}',
            'data: {"type":"response.failed","response":{"error":{"message":"capacity"}}}',
        ]
    )
    calls: list[str] = []
    monkeypatch.setattr(
        account_pool,
        "record_model_probe_outcome",
        lambda *a, **k: calls.append("record"),
    )
    monkeypatch.setattr(
        account_pool, "recover_model_probe", lambda *a, **k: calls.append("recover")
    )
    monkeypatch.setattr(
        account_pool,
        "clear_account_cooldown",
        lambda *a, **k: calls.append("clear"),
    )
    monkeypatch.setattr(model_health, "_save_last_probe", lambda *a, **k: None)

    result = model_health.probe_model_for_creds(
        _creds(), "grok-4.5", auto_disable=False, client=client
    )

    assert client.request_json["messages"][0]["content"] == "Reply with exactly OK."
    assert result["outcome"] == "failure"
    assert result["probe_status"] == "fail"
    assert result["ok"] is False
    assert result["available"] is False
    assert calls == []


def test_inconclusive_probe_has_no_account_status_side_effects(monkeypatch) -> None:
    from grok2api.pool import account_pool, model_health

    client = _FakeClient(["data: [DONE]"])
    calls: list[str] = []
    monkeypatch.setattr(
        account_pool,
        "record_model_probe_outcome",
        lambda *a, **k: calls.append("record"),
    )
    monkeypatch.setattr(
        account_pool, "recover_model_probe", lambda *a, **k: calls.append("recover")
    )
    monkeypatch.setattr(
        account_pool,
        "clear_account_cooldown",
        lambda *a, **k: calls.append("clear"),
    )
    monkeypatch.setattr(model_health, "_save_last_probe", lambda *a, **k: None)

    result = model_health.probe_model_for_creds(
        _creds(), "grok-4.5", auto_disable=True, client=client
    )

    assert result["outcome"] == "inconclusive"
    assert result["ok"] is False
    assert result["available"] is False
    assert calls == []


def test_probe_snapshot_persists_three_state_status(monkeypatch) -> None:
    from grok2api.admin import settings_store
    from grok2api.pool import model_health

    patches: list[dict] = []
    monkeypatch.setattr(settings_store, "get_account_pool_meta", lambda _aid: {})
    monkeypatch.setattr(
        settings_store,
        "patch_account_pool_meta",
        lambda _aid, patch: patches.append(patch),
    )

    model_health._save_last_probe(
        "account-1",
        {
            "ok": False,
            "available": False,
            "outcome": "inconclusive",
            "probe_status": "inconclusive",
            "model": "grok-4.5",
            "error": "stream ended without completion",
        },
    )

    assert patches[-1]["last_probe_status"] == "inconclusive"
    assert patches[-1]["last_probe"]["probe_status"] == "inconclusive"
    assert patches[-1]["last_probe"]["ok"] is False


def test_success_does_not_clear_quota_or_free_usage_cooldown(monkeypatch) -> None:
    from grok2api.admin import settings_store
    from grok2api.pool import account_pool, model_health

    client = _FakeClient(
        [
            'data: {"choices":[{"delta":{"content":"OK"},"finish_reason":null}]}',
            'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}',
            "data: [DONE]",
        ]
    )
    calls: list[str] = []
    monkeypatch.setattr(
        settings_store,
        "get_account_pool_meta",
        lambda _aid: {
            "disabled_for_quota": True,
            "quota_source": "billing",
            "cooldown_code": "free_usage_exhausted",
            "cooldown_until": 9_999_999_999,
            "blocked_models": {"grok-4.5": {"source": "temp_usage"}},
        },
    )
    monkeypatch.setattr(settings_store, "patch_account_pool_meta", lambda *a, **k: None)
    monkeypatch.setattr(
        account_pool,
        "record_model_probe_outcome",
        lambda *a, **k: calls.append("record"),
    )
    monkeypatch.setattr(
        account_pool,
        "clear_account_cooldown",
        lambda *a, **k: calls.append("clear"),
    )
    monkeypatch.setattr(
        account_pool, "recover_model_probe", lambda *a, **k: calls.append("recover")
    )

    result = model_health.probe_model_for_creds(
        _creds(), "grok-4.5", auto_disable=True, client=client
    )

    assert result["outcome"] == "success"
    assert result["probe_status"] == "ok"
    assert "recover" in calls
    assert "record" not in calls
    assert "clear" not in calls


def test_inconclusive_result_remains_eligible_for_background_recheck() -> None:
    from grok2api.pool import model_health

    assert model_health._is_recoverable_probe_result(
        {"outcome": "inconclusive", "probe_status": "inconclusive", "available": False}
    ) is True


def test_elapsed_temporary_cooldown_recovers_without_model_probe(monkeypatch) -> None:
    from grok2api.admin import settings_store
    from grok2api.pool import account_pool

    observed: list[float] = []
    monkeypatch.setattr(
        account_pool,
        "get_account_pool_meta",
        lambda _aid: {
            "enabled": True,
            "disabled_for_quota": False,
            "cooldown_until": 1,
            "cooldown_count": 2,
            "pool_status": "cooldown",
            "blocked_models": {"other-model": {"until": 9_999_999_999}},
        },
    )
    monkeypatch.setattr(
        settings_store,
        "expire_account_cooldown_atomic",
        lambda _aid, until: observed.append(until)
        or {
            "enabled": True,
            "disabled_for_quota": False,
            "blocked_models": {"other-model": {"until": 9_999_999_999}},
            "pool_status": "model_blocked",
            "cooldown_count": 0,
        },
    )

    result = account_pool.maybe_expire_cooldown("account-1")

    assert result is not None
    assert observed == [1.0]
    assert result["pool_status"] == "model_blocked"
    assert result["cooldown_count"] == 0


def test_real_free_usage_write_uses_bounded_ttl_and_expires(monkeypatch) -> None:
    from grok2api.admin import settings_store
    from grok2api.pool import account_pool

    now = [1_000.0]
    saved_meta: dict = {
        "cooldown_until": now[0] + account_pool.PROBE_HOLD_COOLDOWN_SEC,
        "cooldown_count": 1,
        "pool_status": "cooldown",
    }
    monkeypatch.setattr(account_pool, "_now", lambda: now[0])
    monkeypatch.setattr(account_pool, "get_account_pool_meta", lambda _aid: dict(saved_meta))
    monkeypatch.setattr(account_pool, "block_model", lambda *a, **k: None)
    monkeypatch.setattr(account_pool, "release_account_pick", lambda *a, **k: None)
    monkeypatch.setattr(account_pool, "invalidate_pool_summary_cache", lambda: None)

    def save(_aid, patch):
        saved_meta.update(patch)
        return dict(saved_meta)

    monkeypatch.setattr(account_pool, "patch_account_pool_meta", save)

    def expire(_aid, observed_until):
        if float(saved_meta.get("cooldown_until")) != float(observed_until):
            return None
        for key in ("cooldown_until", "cooldown_reason", "cooldown_code", "status_stack"):
            saved_meta.pop(key, None)
        saved_meta["cooldown_count"] = 0
        saved_meta["pool_status"] = "normal"
        return dict(saved_meta)

    monkeypatch.setattr(settings_store, "expire_account_cooldown_atomic", expire)
    written = account_pool.apply_free_usage_cooldown(
        "account-1",
        error="subscription:free-usage-exhausted",
        status_code=429,
        model="grok-4.5",
    )

    assert written is not None
    assert written["cooldown_until"] <= now[0] + account_pool.FREE_USAGE_COOLDOWN_MAX_SEC
    assert written["cooldown_until"] < now[0] + account_pool.PROBE_HOLD_COOLDOWN_SEC

    now[0] = float(written["cooldown_until"]) + 1
    expired = account_pool.maybe_expire_cooldown("account-1", dict(saved_meta))
    assert expired is not None
    assert saved_meta.get("cooldown_until") is None


def test_free_usage_failure_preserves_quota_disabled_state(monkeypatch) -> None:
    from grok2api.pool import account_pool

    patches: list[dict] = []
    monkeypatch.setattr(
        account_pool,
        "get_account_pool_meta",
        lambda _aid: {
            "enabled": False,
            "disabled_for_quota": True,
            "disabled_reason": "billing quota",
            "quota_source": "billing",
        },
    )
    monkeypatch.setattr(account_pool, "block_model", lambda *a, **k: None)
    monkeypatch.setattr(account_pool, "release_account_pick", lambda *a, **k: None)
    monkeypatch.setattr(account_pool, "invalidate_pool_summary_cache", lambda: None)
    monkeypatch.setattr(
        account_pool,
        "patch_account_pool_meta",
        lambda _aid, patch: patches.append(dict(patch)) or dict(patch),
    )

    account_pool.apply_free_usage_cooldown(
        "account-1",
        error="subscription:free-usage-exhausted",
        status_code=429,
        model="grok-4.5",
    )

    assert patches
    assert "enabled" not in patches[-1]
    assert "disabled_for_quota" not in patches[-1]
    assert "disabled_reason" not in patches[-1]


def test_atomic_model_recovery_preserves_other_state(monkeypatch) -> None:
    from grok2api.admin import settings_store

    data = {
        "account_pool": {
            "account-1": {
                "enabled": True,
                "disabled_for_quota": False,
                "pool_status": "cooldown",
                "cooldown_until": 9_999_999_999,
                "cooldown_count": 2,
                "probe_fail_streak": 3,
                "blocked_models": {"target": {}, "other": {}},
            }
        }
    }
    monkeypatch.setattr(settings_store, "_pg_settings", lambda: None)
    monkeypatch.setattr(settings_store, "_load", lambda: data)
    monkeypatch.setattr(settings_store, "_save", lambda *a, **k: None)

    result = settings_store.recover_model_probe_atomic("account-1", "target")

    assert result is not None
    assert result["probe_fail_streak"] == 0
    assert result["blocked_models"] == {"other": {}}
    assert result["cooldown_count"] == 2
    assert result["pool_status"] == "cooldown"


def test_atomic_cooldown_expiry_rejects_newer_version(monkeypatch) -> None:
    from grok2api.admin import settings_store

    data = {
        "account_pool": {
            "account-1": {
                "enabled": True,
                "pool_status": "cooldown",
                "cooldown_until": 200.0,
                "cooldown_count": 1,
            }
        }
    }
    monkeypatch.setattr(settings_store, "_pg_settings", lambda: None)
    monkeypatch.setattr(settings_store, "_load", lambda: data)
    monkeypatch.setattr(settings_store, "_save", lambda *a, **k: None)
    monkeypatch.setattr(settings_store.time, "time", lambda: 300.0)

    assert settings_store.expire_account_cooldown_atomic("account-1", 100.0) is None
    assert data["account_pool"]["account-1"]["cooldown_until"] == 200.0
