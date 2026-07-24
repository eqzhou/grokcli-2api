from __future__ import annotations

import pytest


def test_manual_oauth_is_default_on_with_explicit_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.upstream import grok_build_adapter as adapter

    monkeypatch.delenv("GROK2API_REG_MANUAL_OAUTH", raising=False)
    assert adapter._manual_oauth_enabled()
    monkeypatch.setenv("GROK2API_REG_MANUAL_OAUTH", "0")
    assert not adapter._manual_oauth_enabled()


def test_authorized_email_must_match_expected_registration_email() -> None:
    from grok2api.upstream.oidc_auth import authorized_email_matches

    assert authorized_email_matches("Alias+One@outlook.com", "alias+one@outlook.com")
    assert authorized_email_matches("", "someone@example.com")
    assert not authorized_email_matches("alias+one@outlook.com", "alias+two@outlook.com")
    assert not authorized_email_matches("alias+one@outlook.com", "")


def test_cancel_device_authorization_is_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    from grok2api.upstream import oidc_auth

    monkeypatch.setattr(oidc_auth, "_device_mirror", lambda *_args, **_kwargs: None)
    with oidc_auth._lock:
        oidc_auth._device_sessions["manual-1"] = {
            "id": "manual-1",
            "status": "waiting_user",
            "message": "waiting",
        }

    result = oidc_auth.cancel_device_authorization(
        "manual-1", reason="registration stopped"
    )

    assert result["ok"] is True
    assert result["status"] == "cancelled"
    assert result["error"] == "registration stopped"
    with oidc_auth._lock:
        oidc_auth._device_sessions.pop("manual-1", None)


def test_device_worker_rejects_wrong_authorized_email_before_upsert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.store import sidecar_owner
    from grok2api.upstream import oidc_auth

    class Response:
        status_code = 200
        text = '{"access_token":"redacted"}'

        @staticmethod
        def json() -> dict:
            return {"access_token": "redacted", "refresh_token": "redacted"}

    class Client:
        def __init__(self, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        @staticmethod
        def post(*_args, **_kwargs):
            return Response()

    upserts: list[str] = []
    monkeypatch.setattr(sidecar_owner, "owner_lease_valid", lambda: True)
    monkeypatch.setattr(oidc_auth.httpx, "Client", Client)
    monkeypatch.setattr(
        oidc_auth,
        "entry_from_token_response",
        lambda _body: (
            "account-wrong",
            {"key": "redacted", "email": "wrong@example.com"},
        ),
    )
    monkeypatch.setattr(
        oidc_auth, "upsert_entry", lambda account_id, _entry: upserts.append(account_id)
    )
    monkeypatch.setattr(oidc_auth, "_device_mirror", lambda *_args, **_kwargs: None)
    with oidc_auth._lock:
        oidc_auth._device_sessions["bound-1"] = {
            "id": "bound-1",
            "status": "waiting_user",
            "device_code": "device-code",
            "client_id": "client-id",
            "interval": 3,
            "expires_at": 9_999_999_999,
            "expected_email": "alias+one@outlook.com",
            "source": "register-email-manual",
        }

    oidc_auth._device_poll_worker("bound-1")

    assert upserts == []
    state = oidc_auth.get_device_session("bound-1")
    assert state is not None
    assert state["status"] == "error"
    assert "does not match" in state["error"]
    with oidc_auth._lock:
        oidc_auth._device_sessions.pop("bound-1", None)


def test_manual_oauth_wait_returns_matching_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.upstream import grok_build_adapter as adapter
    from grok2api.upstream import oidc_auth

    states = iter(
        [
            {"status": "waiting_user", "message": "waiting"},
            {
                "status": "success",
                "account_id": "account-1",
                "email": "alias+one@outlook.com",
            },
        ]
    )
    cancelled: list[str] = []
    updates: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        oidc_auth,
        "start_device_authorization",
        lambda **kwargs: {
            "ok": True,
            "session_id": "device-1",
            "user_code": "ABCD-EFGH",
            "verification_url": "https://accounts.x.ai/oauth2/device?user_code=ABCD-EFGH",
            "status": "waiting_user",
            "expected_email": kwargs.get("expected_email"),
        },
    )
    monkeypatch.setattr(oidc_auth, "get_device_session", lambda _sid: next(states))
    monkeypatch.setattr(
        oidc_auth,
        "cancel_device_authorization",
        lambda sid, **_kwargs: cancelled.append(sid),
    )
    monkeypatch.setattr(adapter.time, "sleep", lambda _seconds: None)

    result = adapter._wait_for_manual_oauth(
        email="alias+one@outlook.com",
        check_cancel=lambda: None,
        update=lambda status, message, **fields: updates.append(
            (status, {"message": message, **fields})
        ),
    )

    assert result["account_id"] == "account-1"
    assert cancelled == []
    assert updates[0][0] == "waiting_manual_oauth"
    assert updates[0][1]["manual_oauth_user_code"] == "ABCD-EFGH"
    assert updates[0][1]["manual_oauth_verification_url"].startswith("https://")


def test_manual_oauth_wait_cancels_device_session_on_registration_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.upstream import grok_build_adapter as adapter
    from grok2api.upstream import oidc_auth

    cancelled: list[str] = []
    monkeypatch.setattr(
        oidc_auth,
        "start_device_authorization",
        lambda **_kwargs: {
            "ok": True,
            "session_id": "device-stop",
            "user_code": "STOP-CODE",
            "verification_url": "https://accounts.x.ai/oauth2/device",
            "status": "waiting_user",
        },
    )
    monkeypatch.setattr(
        oidc_auth,
        "cancel_device_authorization",
        lambda sid, **_kwargs: cancelled.append(sid),
    )

    checks = 0

    def stop() -> None:
        nonlocal checks
        checks += 1
        if checks >= 3:
            raise adapter._RegCancelled("stopped")

    with pytest.raises(adapter._RegCancelled):
        adapter._wait_for_manual_oauth(
            email="alias+one@outlook.com",
            check_cancel=stop,
            update=lambda *_args, **_kwargs: None,
        )

    assert cancelled == ["device-stop"]


def test_stop_registration_immediately_cancels_manual_device_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.upstream import grok_build_adapter as adapter
    from grok2api.upstream import oidc_auth

    cancelled: list[tuple[str, str]] = []
    session = {
        "id": "reg-stop",
        "status": "waiting_manual_oauth",
        "email": "alias+one@outlook.com",
        "manual_oauth_session_id": "device-stop-now",
    }
    monkeypatch.setattr(adapter, "_load_reg_sess", lambda _sid: dict(session))
    monkeypatch.setattr(adapter, "_mirror_reg_sess", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        oidc_auth,
        "cancel_device_authorization",
        lambda sid, *, reason: cancelled.append((sid, reason)),
    )
    with adapter._lock:
        adapter._sessions["reg-stop"] = dict(session)

    result = adapter.stop_registration_session("reg-stop")

    assert result["ok"] is True
    assert result["status"] == "stopping"
    assert cancelled == [("device-stop-now", "registration stopped by administrator")]
    with adapter._lock:
        adapter._sessions.pop("reg-stop", None)


def test_manual_oauth_credentials_only_available_while_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from grok2api.upstream import grok_build_adapter as adapter

    waiting = {
        "id": "reg-1",
        "status": "waiting_manual_oauth",
        "email": "alias+one@outlook.com",
        "password": "secret-password",
        "manual_oauth_session_id": "device-1",
    }
    monkeypatch.setattr(adapter, "_load_reg_sess", lambda _sid: dict(waiting))

    result = adapter.get_registration_manual_oauth_credentials("reg-1")

    assert result == {
        "ok": True,
        "session_id": "reg-1",
        "email": "alias+one@outlook.com",
        "password": "secret-password",
    }

    waiting["status"] = "imported"
    denied = adapter.get_registration_manual_oauth_credentials("reg-1")
    assert denied["ok"] is False
    assert "not waiting" in denied["error"]


def test_registration_poll_never_exposes_manual_login_password() -> None:
    from grok2api.upstream import grok_build_adapter as adapter

    public = adapter._compact_session(
        {
            "id": "reg-1",
            "status": "waiting_manual_oauth",
            "email": "alias+one@outlook.com",
            "password": "must-not-leak",
            "manual_oauth_user_code": "ABCD-EFGH",
            "manual_oauth_verification_url": "https://accounts.x.ai/oauth2/device",
        }
    )

    assert "password" not in public
    assert public["manual_oauth_user_code"] == "ABCD-EFGH"


def test_admin_registration_page_has_manual_oauth_controls() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    html = (root / "static" / "admin" / "accounts.html").read_text(encoding="utf-8")
    js = (root / "static" / "js" / "core.js").read_text(encoding="utf-8")

    assert 'id="reg-manual-oauth"' in html
    assert 'id="reg-manual-oauth-code"' in html
    assert 'id="reg-manual-oauth-url"' in html
    assert 'id="btn-copy-reg-manual-credentials"' in html
    assert "renderRegManualOAuth" in js
