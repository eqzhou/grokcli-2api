from __future__ import annotations

import time
from email.utils import formatdate
from unittest.mock import patch

import httpx


def test_rate_limit_reset_accepts_epoch_delta_and_http_date() -> None:
    from grok2api.upstream.moemail import rate_limit_reset_at

    now = 1_800_000_000.0
    assert rate_limit_reset_at({"x-ratelimit-reset": "1800000120"}, now=now) == now + 120
    assert rate_limit_reset_at({"x-ratelimit-reset": "45"}, now=now) == now + 45
    http_date = formatdate(now + 90, usegmt=True)
    assert rate_limit_reset_at({"retry-after": http_date}, now=now) == now + 90


def test_mailbox_provider_error_preserves_rate_limit_metadata() -> None:
    from grok2api.upstream.moemail import MailboxProviderError

    reset_at = time.time() + 30
    exc = MailboxProviderError(
        provider="tempmail",
        operation="create",
        status_code=429,
        detail="rate limited",
        rate_limit_reset_at=reset_at,
    )

    assert exc.rate_limited is True
    assert exc.status_code == 429
    assert exc.rate_limit_reset_at == reset_at
    assert "TempMail.lol create failed 429" in str(exc)


def test_tempmail_429_reads_x_ratelimit_reset_header() -> None:
    from grok2api.upstream import moemail

    reset_at = int(time.time()) + 75
    response = httpx.Response(
        429,
        text='{"error":"Rate limited (free)"}',
        headers={"x-ratelimit-reset": str(reset_at)},
        request=httpx.Request("POST", "https://api.tempmail.lol/v2/inbox/create"),
    )

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def post(self, *_args, **_kwargs):
            return response

    with patch.object(moemail.httpx, "Client", return_value=FakeClient()):
        try:
            moemail.tempmail_create_mailbox()
        except moemail.MailboxProviderError as exc:
            assert exc.status_code == 429
            assert exc.rate_limit_reset_at == reset_at
        else:
            raise AssertionError("expected a structured mailbox rate-limit error")


def test_all_mailbox_create_paths_preserve_429_metadata(monkeypatch) -> None:
    from grok2api.upstream import moemail

    reset_at = int(time.time()) + 90
    response = httpx.Response(
        429,
        text='{"error":"rate limited"}',
        headers={"x-ratelimit-reset": str(reset_at)},
        request=httpx.Request("POST", "https://mail.example/create"),
    )

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def post(self, *_args, **_kwargs):
            return response

        def get(self, *_args, **_kwargs):
            return response

    monkeypatch.setenv("GROK2API_MOEMAIL_CREATE_RETRIES", "1")
    calls = (
        lambda: moemail.moemail_create_mailbox(api_key="mk_test", domain="mail.test"),
        lambda: moemail.yyds_create_mailbox(api_key="AC-test", domain="mail.test"),
        lambda: moemail.gptmail_create_mailbox(
            api_key="sk-test", name="user", domain="mail.test"
        ),
        lambda: moemail.cfmail_create_mailbox(
            api_key="admin-test", domain="mail.test"
        ),
    )

    with patch.object(moemail.httpx, "Client", return_value=FakeClient()):
        for call in calls:
            try:
                call()
            except moemail.MailboxProviderError as exc:
                assert exc.status_code == 429
                assert exc.rate_limit_reset_at == reset_at
            else:
                raise AssertionError("expected structured 429 metadata")


def test_moemail_429_is_not_retried_before_batch_circuit_breaker(monkeypatch) -> None:
    from grok2api.upstream import moemail

    response = httpx.Response(
        429,
        text="rate limited",
        headers={"retry-after": "60"},
        request=httpx.Request("POST", "https://mail.example/create"),
    )

    class FakeClient:
        calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def post(self, *_args, **_kwargs):
            self.calls += 1
            return response

    fake = FakeClient()
    monkeypatch.setenv("GROK2API_MOEMAIL_CREATE_RETRIES", "4")
    with patch.object(moemail.httpx, "Client", return_value=fake):
        try:
            moemail.moemail_create_mailbox(api_key="mk_test", domain="mail.test")
        except moemail.MailboxProviderError:
            pass
        else:
            raise AssertionError("expected structured 429 metadata")

    assert fake.calls == 1


def test_registration_failure_classification() -> None:
    from grok2api.upstream.grok_build_adapter import _classify_registration_failure

    mailbox = _classify_registration_failure(
        {"ok": False, "error_kind": "mailbox", "error": "provider unavailable"}
    )
    assert mailbox["kind"] == "mailbox_error"
    assert mailbox["stop_immediately"] is True

    limited = _classify_registration_failure(
        {
            "ok": False,
            "error_kind": "mailbox_rate_limit",
            "status_code": 429,
            "rate_limit_reset_at": 1_800_000_100,
            "error": "rate limited",
        }
    )
    assert limited["kind"] == "rate_limit"
    assert limited["stop_immediately"] is False
    assert limited["rate_limit_reset_at"] == 1_800_000_100

    risk = _classify_registration_failure(
        {
            "ok": False,
            "error": "xAI sign-up page blocked by Cloudflare (HTTP 403): abusive traffic",
        }
    )
    assert risk["kind"] == "xai_risk_control"
    assert risk["stop_immediately"] is True

    ordinary = _classify_registration_failure(
        {"ok": False, "error": "turnstile token solve failed"}
    )
    assert ordinary["kind"] == "failure"
    assert ordinary["stop_immediately"] is False


def test_empty_xai_email_validation_response_is_a_stop_error() -> None:
    from grok2api.upstream.grok_build_adapter import (
        _classify_registration_failure,
        _email_validation_send_failure,
    )

    class Result:
        ok = False
        http_status = 200
        grpc_status = None
        raw = b""

    message = _email_validation_send_failure(Result())
    assert message is not None
    assert "xAI email validation rejected" in message
    failure = _classify_registration_failure({"ok": False, "error": message})
    assert failure["kind"] == "xai_risk_control"
    assert failure["stop_immediately"] is True


def test_successful_xai_email_validation_response_continues() -> None:
    from grok2api.upstream.grok_build_adapter import _email_validation_send_failure

    class Result:
        ok = True
        http_status = 200
        grpc_status = 0
        raw = b"response"

    assert _email_validation_send_failure(Result()) is None


def test_batch_progress_separates_cancelled_and_unattempted() -> None:
    from grok2api.upstream.grok_build_adapter import _registration_progress_counts

    progress = _registration_progress_counts(
        total=500,
        imported=5,
        failed=174,
        cancelled=1,
        running=0,
    )

    assert progress == {
        "total": 500,
        "imported": 5,
        "error": 174,
        "cancelled": 1,
        "running": 0,
        "done": 180,
        "unattempted": 320,
    }


def test_batch_stats_returns_persisted_unattempted() -> None:
    from grok2api.upstream import grok_build_adapter as gba

    stats = gba._batch_stats(
        [],
        batch={
            "count": 500,
            "imported": 5,
            "error": 174,
            "cancelled": 1,
            "running": 0,
            "done": 180,
            "unattempted": 320,
            "probing": 4,
            "probe_owner_pid": gba.os.getpid(),
            "probe_updated_at": time.time(),
            "status": "cancelled",
        },
    )

    assert stats["cancelled"] == 1
    assert stats["unattempted"] == 320
    assert stats["error"] == 174
    assert stats["probing"] == 4


def test_probe_counters_are_persisted_at_batch_level() -> None:
    from grok2api.upstream import grok_build_adapter as gba

    sid = "probe-session-test"
    bid = "probe-batch-test"
    session = {"id": sid, "batch_id": bid}
    batch = {"id": bid, "probing": 0, "probe_ok_count": 0, "probe_fail_count": 0}
    with (
        patch.object(gba, "_load_reg_sess", return_value=session),
        patch.object(gba, "_load_reg_batch", return_value=batch),
        patch.object(gba, "_mirror_reg_batch") as mirror,
    ):
        gba._batches[bid] = dict(batch)
        gba._update_batch_probe_counters(sid, pending_delta=2)
        gba._update_batch_probe_counters(
            sid, pending_delta=-2, ok_delta=1, fail_delta=1
        )

    stored = gba._batches.pop(bid)
    assert stored["probing"] == 0
    assert stored["probe_pending_count"] == 0
    assert stored["probe_ok_count"] == 1
    assert stored["probe_fail_count"] == 1
    assert mirror.call_count == 2


def test_orphaned_probe_counters_are_cleared_after_process_restart() -> None:
    from grok2api.upstream import grok_build_adapter as gba

    bid = "orphaned-probe-batch"
    batch = {
        "id": bid,
        "probing": 3,
        "probe_pending_count": 3,
        "probe_fail_count": 1,
        "probe_owner_pid": gba.os.getpid() + 1000,
        "probe_updated_at": time.time() - gba._POST_IMPORT_PROBE_STALE_SEC - 1,
        "session_ids": ["orphaned-session"],
    }
    session = {
        "id": "orphaned-session",
        "probe": {"pending": True, "ok": 0, "fail": 0},
    }
    with (
        patch.object(gba, "_load_reg_sess", return_value=session),
        patch.object(gba, "_mirror_reg_sess") as mirror_session,
        patch.object(gba, "_mirror_reg_batch") as mirror,
    ):
        recovered = gba._recover_orphaned_probe_counters(batch)

    assert recovered["probing"] == 0
    assert recovered["probe_pending_count"] == 0
    assert recovered["probe_fail_count"] == 4
    assert recovered["probe_recovery_reason"] == "probe worker timed out"
    mirror.assert_called_once()
    mirrored_session = mirror_session.call_args.args[1]
    assert mirrored_session["probe"]["pending"] is False
    assert "timed out" in mirrored_session["probe"]["error"]


def test_fresh_probe_owned_by_sibling_worker_is_not_cleared() -> None:
    from grok2api.upstream import grok_build_adapter as gba

    batch = {
        "id": "sibling-probe-batch",
        "probing": 2,
        "probe_pending_count": 2,
        "probe_owner_pid": gba.os.getpid() + 1000,
        "probe_updated_at": time.time(),
    }
    with patch.object(gba, "_mirror_reg_batch") as mirror:
        recovered = gba._recover_orphaned_probe_counters(batch)

    assert recovered["probing"] == 2
    assert recovered["probe_pending_count"] == 2
    mirror.assert_not_called()
