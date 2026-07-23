from __future__ import annotations


def test_empty_billing_object_is_not_healthy() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing({})

    assert result["ok"] is False
    assert result["available"] is False


def test_billing_error_envelope_is_not_healthy() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing({"error": {"message": "billing unavailable"}})

    assert result["ok"] is False
    assert result["available"] is False
    assert "billing unavailable" in result["error"]


def test_billing_message_or_detail_envelope_is_not_healthy() -> None:
    from grok2api.pool.quota import normalize_billing

    for payload in (
        {"message": "billing unavailable"},
        {"detail": "temporary upstream failure"},
        {"ok": False, "message": "not ready"},
    ):
        result = normalize_billing(payload)
        assert result["ok"] is False
        assert result["available"] is False


def test_billing_requires_recognized_schema() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing({"config": {"unexpected": "value"}})

    assert result["ok"] is False
    assert result["available"] is False


def test_valid_zero_value_billing_schema_remains_healthy() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing(
        {"config": {"monthlyLimit": {"val": 0}, "used": {"val": 0}}}
    )

    assert result["ok"] is True
    assert result["available"] is True
    assert result["unlimited_or_free"] is True


def test_valid_on_demand_pair_is_healthy() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing(
        {"config": {"onDemandCap": {"val": 20}, "onDemandUsed": {"val": 5}}}
    )

    assert result["ok"] is True
    assert result["schema_valid"] is True
    assert result["quota_state"] == "healthy"


def test_billing_rejects_known_fields_with_invalid_values() -> None:
    from grok2api.pool.quota import normalize_billing

    result = normalize_billing({"config": {"monthlyLimit": {"val": "unknown"}}})

    assert result["ok"] is False
    assert result["available"] is False


def test_billing_rejects_negative_nan_and_infinite_values() -> None:
    from grok2api.pool.quota import normalize_billing

    for value in (True, False, -1, float("nan"), float("inf"), float("-inf")):
        result = normalize_billing({"config": {"monthlyLimit": {"val": value}}})
        assert result["ok"] is False
        assert result["available"] is False


def test_billing_requires_a_complete_limit_and_usage_pair() -> None:
    from grok2api.pool.quota import normalize_billing

    for config in (
        {"used": {"val": 0}},
        {"prepaidBalance": {"val": 10}},
        {"monthlyLimit": {"val": 10}},
        {"onDemandCap": {"val": 10}},
    ):
        result = normalize_billing({"config": config})
        assert result["ok"] is False
        assert result["schema_valid"] is False


def test_billing_rejects_present_but_malformed_config() -> None:
    from grok2api.pool.quota import normalize_billing

    for malformed in (None, [], "corrupt", 0):
        result = normalize_billing(
            {
                "config": malformed,
                "monthlyLimit": {"val": 10},
                "used": {"val": 1},
            }
        )
        assert result["ok"] is False
        assert result["schema_valid"] is False


def test_billing_rejects_malformed_optional_history() -> None:
    from grok2api.pool.quota import normalize_billing

    base = {"monthlyLimit": {"val": 10}, "used": {"val": 1}}
    for history in (True, 1, "bad", {"year": 2026}):
        result = normalize_billing({"config": {**base, "history": history}})
        assert result["ok"] is False
    result = normalize_billing(
        {"config": {**base, "history": [{"billingCycle": "bad"}]}}
    )
    assert result["ok"] is False


def test_healthy_billing_does_not_reenable_model_health_disabled_account(
    monkeypatch,
) -> None:
    from grok2api.pool import account_pool

    patches: list[dict] = []
    monkeypatch.setattr(
        account_pool,
        "get_account_pool_meta",
        lambda _aid: {
            "enabled": False,
            "disabled_source": "model_health",
            "disabled_for_quota": False,
        },
    )
    monkeypatch.setattr(
        account_pool,
        "patch_account_pool_meta",
        lambda _aid, patch: patches.append(patch),
    )

    account_pool.save_quota_snapshot(
        "account-1",
        {"ok": True, "exhausted": False, "display": {"summary": "额度可用"}},
    )

    assert patches
    assert "enabled" not in patches[-1]
