"""Process-generation lease for Python registration/SSO/device sidecars."""

from __future__ import annotations

import os
import threading
import time
import uuid

OWNER_LEASE_SEC = 45
_OWNER_ID = f"{os.getpid()}-{uuid.uuid4().hex}"
_started = False
_lock = threading.RLock()
_lease_healthy = False


def current_owner_id() -> str:
    return _OWNER_ID


def _lease_key(owner_id: str) -> str:
    from grok2api.store.redis_client import key

    return key("sidecar", "owner", owner_id)


def _renew_once() -> bool:
    global _lease_healthy
    from grok2api.store.redis_client import redis_url, set_ex

    if not redis_url():
        with _lock:
            _lease_healthy = True
        return True
    try:
        ok = bool(set_ex(_lease_key(_OWNER_ID), _OWNER_ID, OWNER_LEASE_SEC))
    except Exception:
        ok = False
    with _lock:
        _lease_healthy = ok
    return ok


def start_heartbeat() -> bool:
    """Publish this process lease and keep it alive until process exit."""
    global _started
    with _lock:
        if _started:
            return owner_lease_valid()
        _started = True

    initial_ok = _renew_once()

    def _loop() -> None:
        while True:
            time.sleep(max(5.0, OWNER_LEASE_SEC / 3.0))
            _renew_once()

    threading.Thread(
        target=_loop,
        daemon=True,
        name="g2a-sidecar-owner",
    ).start()
    return initial_ok


def owner_lease_valid() -> bool:
    """Whether this process may start/continue shared sidecar work."""
    try:
        from grok2api.store.redis_client import redis_url

        if not redis_url():
            return True
    except Exception:
        return False
    with _lock:
        return bool(_lease_healthy)


def owner_alive(owner_id: str) -> bool | None:
    """True=live lease, False=confirmed absent, None=Redis unknown."""
    owner = str(owner_id or "").strip()
    if not owner:
        return False
    if owner == _OWNER_ID:
        return owner_lease_valid()
    try:
        from grok2api.store.redis_client import get_str, redis_url

        if not redis_url():
            return False
        return get_str(_lease_key(owner)) == owner
    except Exception:
        return None
