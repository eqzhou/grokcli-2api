import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GROK_BUILD_AUTH = ROOT / "grok-build-auth"
if str(GROK_BUILD_AUTH) not in sys.path:
    sys.path.insert(0, str(GROK_BUILD_AUTH))


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


def test_sso_backup_is_private(tmp_path) -> None:
    from xconsole_client.sso import save_sso

    path = save_sso("header.payload.signature", output_dir=tmp_path / "sso")

    assert _mode(path) == 0o600
    assert _mode(path.parent) == 0o700


def test_oauth_records_are_private(tmp_path) -> None:
    from xconsole_client.xai_oauth import save_oauth_record

    path = save_oauth_record(
        {"access_token": "secret", "refresh_token": "refresh"},
        output_dir=tmp_path / "oauth",
    )

    assert _mode(path) == 0o600
    assert _mode(path.parent) == 0o700


def test_external_auth_directory_mode_is_preserved(tmp_path) -> None:
    from xconsole_client.xai_oauth import save_cliproxyapi_auth_record

    target = tmp_path / "shared-auth"
    target.mkdir(mode=0o755)
    path = save_cliproxyapi_auth_record(
        {"access_token": "secret", "refresh_token": "refresh"}, auth_dir=target
    )

    assert _mode(path) == 0o600
    assert _mode(target) == 0o755


def test_registration_backup_is_private(tmp_path, monkeypatch) -> None:
    from grok2api.upstream import grok_build_adapter as adapter

    monkeypatch.setattr(adapter, "REGISTER_SSO_DIR", tmp_path / "register_sso")
    saved = adapter._persist_registration_sso(
        sid="session", email="user@example.com", password="secret", sso="cookie"
    )
    path = Path(saved)

    assert path.exists()
    assert _mode(path) == 0o600
    assert _mode(path.parent) == 0o700


def test_sensitive_registration_debugging_is_opt_in() -> None:
    source = (ROOT / "grok2api" / "upstream" / "grok_build_adapter.py").read_text(
        encoding="utf-8"
    )

    assert 'os.environ.get("GROK2API_REG_DEBUG_SENSITIVE", "") == "1"' in source
    assert "sso[:60]" not in source
    assert "sso[:24]" not in source


def test_sso_redirects_only_allow_trusted_https_hosts() -> None:
    from xconsole_client.sso import _is_trusted_sso_url

    assert _is_trusted_sso_url("https://auth.grokusercontent.com/set-cookie?q=token")
    assert _is_trusted_sso_url("https://accounts.x.ai/sign-in")
    assert not _is_trusted_sso_url("http://auth.grokusercontent.com/set-cookie")
    assert not _is_trusted_sso_url("https://127.0.0.1/internal")
    assert not _is_trusted_sso_url("https://accounts.x.ai.evil.example/set-cookie")


def test_sso_extractor_follows_only_trusted_redirects() -> None:
    from xconsole_client.sso import SSOExtractor

    first = "https://auth.x.ai/set-cookie?q=eyJhbGc.e30.sig"
    second = "https://auth.grokusercontent.com/set-cookie?q=eyJhbGc.e30.sig"
    calls = []

    def request(method, url, *, headers, body=None):
        del method, headers, body
        calls.append(url)
        if url == first:
            return 303, {"location": second}, [], b""
        if url == second:
            return 200, {}, ["sso=eyJhbGc.e30.signature; Path=/"], b""
        if url == "https://auth.grokusercontent.com/set-cookie":
            return 200, {}, [], b""
        raise AssertionError(f"unexpected URL: {url}")

    extractor = SSOExtractor(request, lambda: {}, {})
    token = extractor.extract(first, save=False)

    assert token == "eyJhbGc.e30.signature"
    assert first in calls
    assert second in calls
    assert calls.index(second) > calls.index(first)
