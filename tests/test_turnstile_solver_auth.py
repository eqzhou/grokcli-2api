from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "turnstile-solver" / "solver_auth.py"
)
SPEC = importlib.util.spec_from_file_location("turnstile_solver_auth", MODULE_PATH)
assert SPEC and SPEC.loader
AUTH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUTH)


def test_client_key_sources_and_constant_time_validation() -> None:
    assert AUTH.supplied_client_key("Bearer header-key", "query-key", "body-key") == "header-key"
    assert AUTH.supplied_client_key(None, "query-key", "body-key") == "query-key"
    assert AUTH.supplied_client_key(None, None, "body-key") == "body-key"
    assert AUTH.client_key_allowed("secret", "secret")
    assert not AUTH.client_key_allowed("secret", "wrong")
    assert not AUTH.client_key_allowed("secret", "")


def test_missing_server_key_keeps_loopback_mode_compatible() -> None:
    assert AUTH.client_key_allowed("", "")
    assert AUTH.client_key_allowed(None, None)


def test_all_task_routes_use_only_the_global_guard() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "turnstile-solver" / "api_solver.py"
    ).read_text(encoding="utf-8")
    assert "self.app.before_request(self._authorize_request)" in source
    assert "_check_client_key" not in source
