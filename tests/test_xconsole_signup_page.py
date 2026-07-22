import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
GROK_BUILD_AUTH = ROOT / "grok-build-auth"
if str(GROK_BUILD_AUTH) not in sys.path:
    sys.path.insert(0, str(GROK_BUILD_AUTH))

from xconsole_client.client import XConsoleAuthClient  # noqa: E402


class _BlockedTransport:
    cookies = {}

    def request(self, method, url, *, headers, body=None):
        del method, url, headers, body
        return (
            403,
            {"server": "cloudflare", "cf-ray": "test-SJC"},
            [],
            b"<html><body><p>Blocked due to abusive traffic patterns</p></body></html>",
        )


def test_signup_page_surfaces_cloudflare_block_reason() -> None:
    client = XConsoleAuthClient(transport="urllib")
    client._t = _BlockedTransport()

    with pytest.raises(RuntimeError, match="Blocked due to abusive traffic patterns"):
        client.load_signup_page()


def test_empty_signup_page_reports_missing_chunks_without_executor_error() -> None:
    client = XConsoleAuthClient(transport="urllib")

    with pytest.raises(RuntimeError, match="no JavaScript chunks"):
        client._scrape_action_id("<html></html>")
