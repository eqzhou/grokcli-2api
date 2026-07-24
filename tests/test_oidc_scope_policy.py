"""Regression tests for the xAI device-flow scope policy.

New-account token exchange is rejected when both conversation scopes are
requested.  Keep the application default and the standalone converter's
fallback pinned to the known-good minimal scope set.
"""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_BASE_SCOPES = (
    "openid",
    "profile",
    "email",
    "offline_access",
    "grok-cli:access",
    "api:access",
)
CONVERSATION_SCOPES = {"conversations:read", "conversations:write"}


def _oidc_scope_defaults(path: Path) -> list[tuple[str, ...]]:
    """Return literal defaults from ``GROK2API_OIDC_SCOPES`` getenv calls."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    defaults: list[tuple[str, ...]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or len(node.args) < 2:
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "os"
            and node.func.attr == "getenv"
        ):
            continue
        if not (
            isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "GROK2API_OIDC_SCOPES"
        ):
            continue
        default = ast.literal_eval(node.args[1])
        defaults.append(tuple(default.split()))
    return defaults


def test_project_config_uses_only_known_good_base_oidc_scopes() -> None:
    defaults = _oidc_scope_defaults(ROOT / "grok2api" / "config.py")

    assert defaults == [EXPECTED_BASE_SCOPES]
    assert CONVERSATION_SCOPES.isdisjoint(defaults[0])


def test_standalone_sso_converter_fallback_matches_project_scope_default() -> None:
    config_defaults = _oidc_scope_defaults(ROOT / "grok2api" / "config.py")
    fallback_defaults = _oidc_scope_defaults(ROOT / "scripts" / "sso_to_auth_json.py")

    assert fallback_defaults == config_defaults == [EXPECTED_BASE_SCOPES]
    assert CONVERSATION_SCOPES.isdisjoint(fallback_defaults[0])
