"""Authentication helpers shared by all Turnstile solver endpoints."""

from __future__ import annotations

import hmac


def bearer_token(header: str | None) -> str:
    value = str(header or "").strip()
    scheme, separator, token = value.partition(" ")
    if separator and scheme.lower() == "bearer":
        return token.strip()
    return ""


def supplied_client_key(
    authorization: str | None,
    query_key: str | None,
    body_key: str | None,
) -> str:
    return bearer_token(authorization) or str(query_key or "").strip() or str(body_key or "").strip()


def client_key_allowed(expected: str | None, supplied: str | None) -> bool:
    wanted = str(expected or "").strip()
    if not wanted:
        return True
    candidate = str(supplied or "").strip()
    return bool(candidate) and hmac.compare_digest(candidate, wanted)
