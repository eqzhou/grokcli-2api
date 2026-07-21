from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch


GROK_BUILD_AUTH = Path(__file__).resolve().parents[1] / "grok-build-auth"
if str(GROK_BUILD_AUTH) not in sys.path:
    sys.path.insert(0, str(GROK_BUILD_AUTH))

from xconsole_client.tempmail_transport import TempmailInbox


class _EmptyResponse:
    status_code = 200

    @staticmethod
    def json() -> dict[str, list]:
        return {"emails": []}


class _FakeSession:
    def __init__(self) -> None:
        self.closed = False
        self.polled = threading.Event()

    def get(self, *args, **kwargs) -> _EmptyResponse:
        self.polled.set()
        return _EmptyResponse()

    def close(self) -> None:
        self.closed = True


def test_close_prevents_session_recreation() -> None:
    inbox = TempmailInbox(api_key="test-key")
    session = _FakeSession()
    inbox._http = session

    with patch("xconsole_client.tempmail_transport.requests.Session") as factory:
        inbox.close()

        try:
            inbox._session()
        except RuntimeError as exc:
            assert "closed" in str(exc).lower()
        else:
            raise AssertionError("closed inbox recreated its HTTP session")

    assert session.closed
    factory.assert_not_called()


def test_close_wakes_wait_for_code_promptly() -> None:
    inbox = TempmailInbox(api_key="test-key", interval=10.0, timeout=30.0)
    inbox._created = True
    inbox.address = "test@example.com"
    inbox.token = "token"
    session = _FakeSession()
    inbox._http = session
    errors: list[BaseException] = []

    def wait() -> None:
        try:
            inbox.wait_for_code()
        except BaseException as exc:  # capture the worker result for the assertion
            errors.append(exc)

    worker = threading.Thread(target=wait, daemon=True)
    worker.start()
    assert session.polled.wait(0.5), "wait_for_code never started polling"

    started = time.monotonic()
    inbox.close()
    worker.join(0.5)
    elapsed = time.monotonic() - started

    assert not worker.is_alive(), f"wait_for_code did not stop promptly ({elapsed:.3f}s)"
    assert errors and isinstance(errors[0], RuntimeError)
    assert "closed" in str(errors[0]).lower()
