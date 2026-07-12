"""Admin API routes: setup, login, keys, accounts, pool, quota, models, status."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from fastapi import APIRouter, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

import account_pool
import accounts
import apikeys
import conversation_affinity
import model_health
import quota
import token_maintainer
from auth import AuthError, load_credentials, load_credentials_by_id
import config as _config
import sso_to_auth_json as sso_import

# Registration adapter: dongguatanglinux/grok-build-auth protocol client.
try:
    import grok_build_adapter as reg_adapter
except Exception as _reg_import_err:  # noqa: BLE001
    reg_adapter = None  # type: ignore[assignment]
    _REG_IMPORT_ERROR = str(_reg_import_err)
else:
    _REG_IMPORT_ERROR = None
from config import (
    CLI_VERSION,
    DEFAULT_MODEL,
    PUBLIC_BASE_URL,
    REQUIRE_API_KEY,
    UPSTREAM_BASE,
)
from models import load_models_from_cache, resolve_model, sync_models_from_upstream
from settings_store import (
    VALID_ACCOUNT_MODES,
    change_admin_password,
    create_session_token,
    get_account_mode,
    get_public_settings,
    get_registration_config,
    is_setup_needed,
    resolve_registration_inputs,
    revoke_session,
    set_account_mode,
    set_model_health_enabled,
    set_registration_config,
    set_token_maintain_enabled,
    set_admin_password,
    update_runtime_settings,
    verify_admin_password,
    verify_session_token,
)

router = APIRouter(prefix="/admin/api", tags=["admin"])


def _usage_light() -> dict[str, Any]:
    """Best-effort today/lifetime usage for status cards."""
    try:
        import usage_stats

        return usage_stats.light_snapshot()
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:200], "today_requests": 0, "today_tokens": 0, "total_tokens": 0}


# ── request bodies ──────────────────────────────────────────────────────────


class SetupBody(BaseModel):
    password: str = Field(min_length=4, max_length=128)


class LoginBody(BaseModel):
    password: str = Field(min_length=1, max_length=128)


class CreateKeyBody(BaseModel):
    name: str = Field(default="default", max_length=64)
    note: str = Field(default="", max_length=256)


class UpdateKeyBody(BaseModel):
    name: str | None = Field(default=None, max_length=64)
    note: str | None = Field(default=None, max_length=256)
    enabled: bool | None = None


class LoginModeBody(BaseModel):
    """Device-code only (OAuth / local CLI login removed)."""

    mode: str = Field(default="device", pattern="^(device|oauth)$")
    capture: bool | None = None


class FeatureToggleBody(BaseModel):
    enabled: bool


class AccountModeBody(BaseModel):
    mode: str = Field(description="round_robin | random | least_used")


class ChangePasswordBody(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=4, max_length=128)
    confirm_password: str | None = Field(default=None, max_length=128)


class RuntimeSettingsBody(BaseModel):
    """Partial update for admin system settings page."""

    account_mode: str | None = None
    token_maintain_enabled: bool | None = None
    model_health_enabled: bool | None = None
    reasoning_compat: str | None = Field(
        default=None, description="off | think_tag | content"
    )
    outbound_max_tools: int | None = Field(default=None, ge=0, le=64)
    outbound_tool_gap_sec: float | None = Field(default=None, ge=0.0, le=2.0)
    history_compact_enabled: bool | None = None
    sse_keepalive: float | None = Field(default=None, ge=2.0, le=120.0)
    conversation_affinity_enabled: bool | None = None
    default_model: str | None = Field(default=None, max_length=128)
    # Pool rotation / cooldown policy
    cooldown_default_sec: float | None = Field(default=None, ge=1, le=600)
    cooldown_auth_sec: float | None = Field(default=None, ge=5, le=1800)
    cooldown_rate_limit_sec: float | None = Field(default=None, ge=5, le=1800)
    cooldown_server_error_sec: float | None = Field(default=None, ge=1, le=600)
    cooldown_max_sec: float | None = Field(default=None, ge=30, le=3600)
    soft_model_block_ttl_sec: float | None = Field(default=None, ge=30, le=3600)
    durable_model_block_ttl_sec: float | None = Field(default=None, ge=60, le=86400)
    probe_fail_kick_streak: int | None = Field(default=None, ge=1, le=20)
    probe_fail_disable_streak: int | None = Field(default=None, ge=2, le=50)
    probe_kick_cooldown_sec: float | None = Field(default=None, ge=30, le=7200)
    max_failover_attempts: int | None = Field(default=None, ge=1, le=64)
    pool_policy: dict[str, Any] | None = None


class KickAccountBody(BaseModel):
    reason: str = Field(default="手动移出轮询", max_length=300)
    # >0: temporary cooldown kick; null/0: disable account from pool
    cooldown_sec: float | None = Field(default=None, ge=0, le=3600)


class AccountEnabledBody(BaseModel):
    enabled: bool = True


class ImportAuthBody(BaseModel):
    """Import token / auth.json content for Linux servers."""
    payload: str | dict = Field(
        description="JWT string, single entry JSON, or full auth.json map"
    )
    merge: bool = Field(default=True, description="Merge into existing auth.json")


class ImportSsoBody(BaseModel):
    """Import one or more xAI SSO cookies via HTTP device flow."""
    sso_cookies: list[str] = Field(
        min_length=1,
        description="SSO cookie JWTs; one per line, supports email----pass----sso lines",
    )
    merge: bool = Field(default=True, description="Merge into existing auth.json")
    delay: int = Field(default=0, ge=0, le=300, description="Seconds between accounts")
    max_workers: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Concurrent import threads (hard-capped to avoid WSL freezes)",
    )


class EmailRegistrationBody(BaseModel):
    """Start email-assisted accounts.x.ai registration."""

    provider: str = Field(default="moemail", pattern="^moemail$")
    protocol: str = Field(default="grpc", pattern="^grpc$")
    email: str | None = Field(default=None, max_length=256)
    mailbox_id: str | None = Field(default=None, max_length=256)
    prefix: str | None = Field(default=None, max_length=64)
    domain: str | None = Field(default=None, max_length=128)
    # MoeMail official presets only: 1h / 24h / 3d / permanent(0)
    expiry_ms: int | None = Field(default=None, ge=0, le=259200000)
    api_key: str | None = Field(default=None, max_length=512)
    captcha_provider: str | None = Field(
        default=None,
        pattern="^(local|yescaptcha)$",
        description="Turnstile provider: local solver or YesCaptcha",
    )
    local_solver_url: str | None = Field(
        default=None,
        max_length=256,
        description="Local Turnstile Solver base URL, e.g. http://127.0.0.1:5072",
    )
    yescaptcha_key: str | None = Field(default=None, max_length=512)
    base_url: str | None = Field(default=None, max_length=256)
    proxy: str | None = Field(default=None, max_length=512)
    proxy_username: str | None = Field(default=None, max_length=256)
    proxy_password: str | None = Field(default=None, max_length=512)
    # Multi-thread batch registration
    count: int | None = Field(
        default=1,
        ge=1,
        description="How many accounts to register (batch/multi-thread; no hard cap, concurrency limits parallelism)",
    )
    concurrency: int | None = Field(
        default=5,
        ge=1,
        le=10,
        description="Max concurrent registration workers",
    )
    stagger_ms: int | None = Field(
        default=400,
        ge=0,
        le=10000,
        description="Stagger delay between worker starts (ms)",
    )


class EmailRegistrationProxyTestBody(BaseModel):
    proxy: str | None = Field(default=None, max_length=512)
    proxy_username: str | None = Field(default=None, max_length=256)
    proxy_password: str | None = Field(default=None, max_length=512)


class RegistrationConfigBody(BaseModel):
    """Persist protocol-registration form (MoeMail / captcha / proxy)."""

    base_url: str | None = Field(default=None, max_length=256)
    api_key: str | None = Field(default=None, max_length=512)
    domain: str | None = Field(default=None, max_length=128)
    prefix: str | None = Field(default=None, max_length=64)
    expiry_ms: int | None = Field(default=None, ge=0, le=259200000)
    captcha_provider: str | None = Field(
        default=None,
        pattern="^(local|yescaptcha)$",
        description="Turnstile provider: local solver or YesCaptcha",
    )
    local_solver_url: str | None = Field(
        default=None,
        max_length=256,
        description="Local Turnstile Solver base URL",
    )
    yescaptcha_key: str | None = Field(default=None, max_length=512)
    proxy: str | None = Field(default=None, max_length=512)
    proxy_username: str | None = Field(default=None, max_length=256)
    proxy_password: str | None = Field(default=None, max_length=512)
    count: int | None = Field(default=None, ge=1, le=10000)
    concurrency: int | None = Field(default=None, ge=1, le=10)
    stagger_ms: int | None = Field(default=None, ge=0, le=10000)


class RefreshBody(BaseModel):
    force: bool = Field(
        default=True,
        description="True = refresh all tokens; False = only near-expiry",
    )
    ids: list[str] = Field(
        default_factory=list,
        max_length=2000,
        description="Optional account ids to renew; empty = all accounts",
    )


class AccountBulkExportBody(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=2000)
    include_secrets: bool = Field(
        default=True,
        description="Include access/refresh tokens (required for re-import)",
    )


class AccountProbeBody(BaseModel):
    """Per-account model connectivity probe."""

    model: str | None = Field(
        default=None, description="Model id; default DEFAULT_MODEL / PROBE_MODELS"
    )
    auto_disable: bool | None = Field(
        default=None,
        description="On hard error: block model / disable account (default config)",
    )


class AccountProbeBatchBody(BaseModel):
    """Probe selected accounts (multi-select)."""

    ids: list[str] = Field(default_factory=list, max_length=500)
    model: str | None = Field(
        default=None, description="Model id; default DEFAULT_MODEL / PROBE_MODELS"
    )
    auto_disable: bool | None = Field(default=None)


# ── auth helpers ────────────────────────────────────────────────────────────

ADMIN_COOKIE = "g2a_admin"
ADMIN_COOKIE_MAX_AGE = 7 * 24 * 3600  # match redis/file session TTL


def _set_admin_cookie(response: Response, token: str) -> None:
    """Persist admin session in HttpOnly cookie so page navigations keep auth
    even if localStorage is missing / cleared, until cookie or server session expires.
    """
    if not token:
        return
    response.set_cookie(
        key=ADMIN_COOKIE,
        value=token,
        max_age=ADMIN_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=False,  # allow plain http admin deployments
        path="/",
    )


def _clear_admin_cookie(response: Response) -> None:
    response.delete_cookie(key=ADMIN_COOKIE, path="/")




def _extract_session(request: Request, x_admin_token: str | None) -> str | None:
    if x_admin_token:
        return x_admin_token.strip()
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return request.cookies.get("g2a_admin")


def require_admin(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
) -> str:
    token = _extract_session(request, x_admin_token)
    if verify_session_token(token):
        return token or ""
    raise HTTPException(status_code=401, detail="Admin authentication required")


def _client_ip(request: Request | None) -> str | None:
    if request is None:
        return None
    try:
        xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
        if xff:
            return xff[:80]
        if request.client and request.client.host:
            return str(request.client.host)[:80]
    except Exception:
        return None
    return None


def audit_log(
    request: Request | None,
    *,
    action: str,
    summary: str = "",
    target_type: str | None = None,
    target_id: str | None = None,
    detail: dict[str, Any] | None = None,
    ok: bool = True,
    actor: str = "admin",
) -> None:
    """Best-effort write of an admin operation log to PostgreSQL."""
    try:
        from store.audit_pg import write_log

        write_log(
            action=action,
            summary=summary,
            actor=actor,
            target_type=target_type,
            target_id=target_id,
            detail=detail,
            ip=_client_ip(request),
            user_agent=(request.headers.get("user-agent") if request is not None else None),
            ok=ok,
        )
    except Exception:
        pass


# ── public (no session) ─────────────────────────────────────────────────────


def _is_loopback_host(host: str | None) -> bool:
    h = (host or "").strip().lower().strip("[]")
    return h in {"", "127.0.0.1", "localhost", "0.0.0.0", "::", "::1"}


def _request_public_origin(request: Request | None = None) -> str | None:
    """Best-effort public origin for admin/UI links.

    Priority:
      1) GROK2API_PUBLIC_BASE_URL (explicit)
      2) current request Host / X-Forwarded-* (public reverse-proxy friendly)
    Never invent 127.0.0.1 when the request itself is non-loopback.
    """
    configured = (PUBLIC_BASE_URL or getattr(_config, "PUBLIC_BASE_URL", "") or "").strip()
    if configured:
        return configured.rstrip("/")

    if request is None:
        return None

    xf_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    xf_host = (request.headers.get("x-forwarded-host") or "").split(",")[0].strip()
    host_header = (request.headers.get("host") or "").strip()
    host = xf_host or host_header
    if not host:
        return None

    # Drop default ports for cleaner links.
    host_only = host
    if host_only.endswith(":80") or host_only.endswith(":443"):
        maybe = host_only.rsplit(":", 1)[0]
        if maybe and "/" not in maybe:
            # keep IPv6 bracket forms untouched if present
            if not (maybe.startswith("[") and not host_only.startswith("[")):
                host_only = maybe

    scheme = xf_proto or (request.url.scheme or "http")
    if scheme not in ("http", "https"):
        scheme = "http"
    return f"{scheme}://{host_only}".rstrip("/")


def _public_api_base(request: Request | None = None) -> str:
    origin = _request_public_origin(request)
    if origin:
        return f"{origin}/v1"

    # Fallback only when no request context (startup / offline tools).
    host = str(getattr(_config, "HOST", "") or "")
    port = int(getattr(_config, "PORT", 3000) or 3000)
    if _is_loopback_host(host) or host in ("0.0.0.0", "::"):
        display = "127.0.0.1"
    else:
        display = host
    return f"http://{display}:{port}/v1"


_status_store_cache: dict[str, Any] | None = None
_status_store_at = 0.0
_status_reg_cache: dict[str, Any] | None = None
_status_reg_at = 0.0


@router.get("/status")
async def admin_status(request: Request):
    """Fast admin heartbeat.

    Avoid account acquire() and full account dumps — those dominate latency with
    500+ accounts and make UI auto-refresh feel stuck.
    """
    global _status_store_cache, _status_store_at, _status_reg_cache, _status_reg_at
    setup = is_setup_needed()
    account = accounts.account_status(include_accounts=False)
    key_stats = apikeys.stats()
    pool = account_pool.pool_summary(include_accounts=False)

    # Derive credentials snapshot from counts (no acquire/OIDC).
    creds_ok = int(account.get("active_count") or pool.get("live") or 0) > 0
    creds_email = None

    host = _config.HOST
    port = _config.PORT
    public_origin = _request_public_origin(request)
    api_base = _public_api_base(request)

    now = time.time()
    reg_status: dict[str, Any]
    if _status_reg_cache is not None and now - _status_reg_at < 30:
        reg_status = dict(_status_reg_cache)
    else:
        reg_status = {"available": False}
        try:
            if reg_adapter is not None:
                reg_status = reg_adapter.registration_available()
            else:
                reg_status = {"available": False, "error": _REG_IMPORT_ERROR}
        except Exception as e:  # noqa: BLE001
            reg_status = {"available": False, "error": str(e)}
        _status_reg_cache = dict(reg_status)
        _status_reg_at = now

    try:
        from app import APP_VERSION as _app_ver
    except Exception:
        _app_ver = "unknown"

    if _status_store_cache is not None and now - _status_store_at < 5:
        store_info = dict(_status_store_cache)
    else:
        store_info = {}
        try:
            from store import store_status
            store_info = store_status()
        except Exception as e:  # noqa: BLE001
            store_info = {"backend": "unknown", "error": str(e)}
        _status_store_cache = dict(store_info)
        _status_store_at = now

    return {
        "ok": True,
        "setup_needed": setup,
        "version": _app_ver,
        "store": store_info,
        "cli_version": CLI_VERSION,
        "host": host,
        "port": port,
        "public_origin": public_origin,
        "upstream": UPSTREAM_BASE,
        "default_model": DEFAULT_MODEL,
        "require_api_key_mode": REQUIRE_API_KEY,
        "api_base": api_base,
        "credentials_ok": creds_ok,
        "credentials_email": creds_email,
        "account_mode": get_account_mode(),
        "accounts": account,
        "pool": {
            "mode": pool.get("mode"),
            "total": pool.get("total"),
            "live": pool.get("live"),
            "enabled": pool.get("enabled"),
            "in_cooldown": pool.get("in_cooldown"),
            "quota_disabled": pool.get("quota_disabled"),
            "model_blocked": pool.get("model_blocked"),
            "source": pool.get("source") or "postgres",
        },
        "keys": key_stats,
        "models_count": len(load_models_from_cache()),
        "settings": get_public_settings(),
        "token_maintainer": token_maintainer.status(light=True),
        "model_health": model_health.status(light=True),
        "conversation_affinity": conversation_affinity.status(),
        "registration": reg_status,
        "usage": _usage_light(),
    }


@router.post("/setup")
async def admin_setup(body: SetupBody):
    if not is_setup_needed():
        raise HTTPException(status_code=400, detail="Already set up")
    try:
        set_admin_password(body.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    token = create_session_token()
    resp = JSONResponse({"ok": True, "token": token, "message": "Admin password created"})
    _set_admin_cookie(resp, token)
    return resp


@router.post("/login")
async def admin_login(body: LoginBody, request: Request):
    if is_setup_needed():
        raise HTTPException(status_code=400, detail="Setup required first")
    if not verify_admin_password(body.password):
        raise HTTPException(status_code=401, detail="Invalid password")
    token = create_session_token()
    audit_log(request, action="admin.login", summary="管理员登录成功")
    resp = JSONResponse({"ok": True, "token": token})
    _set_admin_cookie(resp, token)
    return resp


@router.get("/session")
async def admin_session(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Cheap session probe: 200 if cookie/header token still valid."""
    token = _extract_session(request, x_admin_token)
    if verify_session_token(token):
        # sliding refresh already happens inside verify_session_token
        return {"ok": True, "authenticated": True}
    raise HTTPException(status_code=401, detail="Admin authentication required")


@router.post("/logout")
async def admin_logout(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    token = _extract_session(request, x_admin_token)
    revoke_session(token)
    resp = JSONResponse({"ok": True})
    _clear_admin_cookie(resp)
    return resp


# ── protected ───────────────────────────────────────────────────────────────


@router.get("/dashboard")
async def dashboard(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    full: bool = False,
):
    """Overview payload.

    Default is summary-only (fast). Pass ?full=1 only when a page truly needs
    embedded account rows (legacy). Accounts page should use /accounts.
    """
    require_admin(request, x_admin_token)
    account = accounts.account_status(include_accounts=bool(full))
    key_stats = apikeys.stats()
    # Overview uses light pool summary (DB snapshot). full=1 still embeds accounts.
    pool = account_pool.pool_summary(include_accounts=bool(full))
    # Do NOT call load_credentials()/OIDC on overview refresh — it can stall and
    # surface browser "Failed to fetch" when a worker is busy.
    cred = {
        "email": None,
        "active_count": account.get("active_count"),
        "account_count": account.get("account_count"),
        "ok": int(account.get("active_count") or pool.get("live") or 0) > 0,
    }
    host = _config.HOST
    port = _config.PORT
    public_origin = _request_public_origin(request)
    return {
        "credentials": cred,
        "accounts": account,
        "pool": pool,
        "keys": key_stats,
        "account_mode": get_account_mode(),
        "account_modes": list(VALID_ACCOUNT_MODES),
        "models": load_models_from_cache(),
        "settings": get_public_settings(),
        "host": host,
        "port": port,
        "public_origin": public_origin,
        "api_base": _public_api_base(request),
        "cli_version": CLI_VERSION,
        "upstream": UPSTREAM_BASE,
        "default_model": DEFAULT_MODEL,
        "token_maintainer": token_maintainer.status(light=True),
        "model_health": model_health.status(light=True),
        "conversation_affinity": conversation_affinity.status(),
        "usage": _usage_light(),
        "full": bool(full),
    }


@router.get("/keys")
async def list_keys(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    return {
        "keys": apikeys.list_keys(),
        "stats": apikeys.stats(),
        "store_source": apikeys.keys_store_source(),
        "store_backend": apikeys.keys_store_source(),
    }


@router.post("/keys")
async def create_key(
    body: CreateKeyBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    rec = apikeys.create_key(body.name, body.note)
    audit_log(
        request,
        action="keys.create",
        summary=f"创建 API Key：{rec.get('name') or body.name}",
        target_type="api_key",
        target_id=rec.get("id"),
        detail={"name": rec.get("name"), "prefix": rec.get("prefix")},
    )
    return {"ok": True, "key": rec}


@router.post("/keys/{key_id}/regenerate")
async def regenerate_key(
    key_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    rec = apikeys.regenerate_key(key_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Key not found")
    audit_log(
        request,
        action="keys.regenerate",
        summary=f"重建 API Key：{rec.get('name') or key_id}",
        target_type="api_key",
        target_id=key_id,
        detail={"name": rec.get("name"), "prefix": rec.get("prefix")},
    )
    return {"ok": True, "key": rec}


@router.patch("/keys/{key_id}")
async def update_key(
    key_id: str,
    body: UpdateKeyBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    rec = None
    if body.enabled is not None:
        rec = apikeys.set_enabled(key_id, body.enabled)
        if not rec:
            raise HTTPException(status_code=404, detail="Key not found")
    if body.name is not None or body.note is not None:
        rec = apikeys.update_key(key_id, name=body.name, note=body.note)
        if not rec:
            raise HTTPException(status_code=404, detail="Key not found")
    if rec is None:
        for k in apikeys.list_keys():
            if k["id"] == key_id:
                rec = k
                break
    if rec is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"ok": True, "key": rec}


@router.delete("/keys/{key_id}")
async def delete_key(
    key_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    if not apikeys.delete_key(key_id):
        raise HTTPException(status_code=404, detail="Key not found")
    audit_log(
        request,
        action="keys.delete",
        summary=f"删除 API Key：{key_id}",
        target_type="api_key",
        target_id=key_id,
    )
    return {"ok": True}


def _sort_account_rows(rows: list[dict], sort_key: str) -> list[dict]:
    """In-memory sort for file-backend /accounts fallback (mirrors PG sort keys)."""
    key = str(sort_key or "newest")

    def _f(v, default=0.0):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except Exception:
            return default

    def _s(v):
        return str(v or "").lower()

    def sorter(row: dict):
        p = row.get("_pool") or {}
        exp = _f(row.get("expires_at"), 0.0)
        upd = _f(row.get("updated_at") or row.get("create_time"), 0.0)
        used = _f(p.get("last_used_at"), 0.0)
        reqs = _f(p.get("request_count"), 0.0)
        email = _s(row.get("email"))
        rid = _s(row.get("id"))
        in_cd = 0 if p.get("in_cooldown") else 1
        disabled = 0 if (p.get("enabled") is False or p.get("disabled_for_quota")) else 1
        if key == "oldest":
            return (upd, rid)
        if key == "expires_desc":
            return (-exp, -upd)
        if key == "expires_asc":
            return (exp if exp else 1e18, -upd)
        if key == "email_asc":
            return (email, rid)
        if key == "email_desc":
            return (email, rid)  # reverse later
        if key == "last_used_desc":
            return (-used, -upd)
        if key == "last_used_asc":
            return (used if used else 1e18, -upd)
        if key == "requests_desc":
            return (-reqs, -upd)
        if key == "cooldown_first":
            return (in_cd, -upd)
        if key == "disabled_first":
            return (disabled, -upd)
        # newest (default)
        return (-upd, rid)

    reverse = key == "email_desc"
    try:
        return sorted(rows, key=sorter, reverse=reverse)
    except Exception:
        return rows


@router.get("/accounts")
async def list_accounts_route(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    page: int = 1,
    page_size: int = 25,
    q: str = "",
    sort: str = "newest",
    summary: bool = False,
):
    """List accounts with server-side SQL pagination (PostgreSQL path).

    - default: paged rows for admin UI (fast first paint)
    - summary=1: counts only
    - page_size<=0 or page_size>=10000: return all summary rows (export/legacy)
    - sort: newest|oldest|expires_desc|expires_asc|email_asc|email_desc|
            last_used_desc|last_used_asc|requests_desc|cooldown_first|disabled_first
    """
    require_admin(request, x_admin_token)

    try:
        from store.accounts_pg import normalize_account_sort

        sort_key = normalize_account_sort(sort)
    except Exception:
        sort_key = (sort or "newest").strip().lower() or "newest"

    if summary:
        status = accounts.account_status(include_accounts=False)
        status["pool"] = account_pool.pool_summary(include_accounts=False)
        status["page"] = 1
        status["page_size"] = 0
        status["total"] = int(status.get("account_count") or 0)
        status["total_pages"] = 1
        status["q"] = (q or "").strip()
        status["sort"] = sort_key
        return status

    # Fast path: page in PostgreSQL, attach pool meta only for current page.
    try:
        from store.accounts_pg import enabled as pg_on, list_account_summaries
        from store.settings_pg import get_pool_meta_many, pool_counts
        from settings_store import get_account_mode
        import time as _time

        if pg_on():
            paged = list_account_summaries(
                q=q, page=page, page_size=page_size, sort=sort_key
            )
            page_items = list(paged.get("accounts") or [])
            ids = [str(a.get("id")) for a in page_items if a.get("id")]
            pool_map = get_pool_meta_many(ids) if ids else {}
            now = _time.time()
            # Optional redis overlay only for the current page (not whole pool).
            try:
                from store.pool_redis import merge_pool_meta
            except Exception:
                merge_pool_meta = None  # type: ignore
            for item in page_items:
                aid = item.get("id")
                meta = dict(pool_map.get(aid) or {})
                if merge_pool_meta is not None and aid:
                    try:
                        meta = merge_pool_meta(str(aid), meta)
                    except Exception:
                        pass
                blocked = meta.get("blocked_models") or {}
                if not isinstance(blocked, dict):
                    blocked = {}
                cd_until = meta.get("cooldown_until")
                in_cd = False
                cd_left = 0.0
                if cd_until is not None:
                    try:
                        cd_left = max(0.0, float(cd_until) - now)
                        in_cd = cd_left > 0
                    except Exception:
                        in_cd = False
                        cd_left = 0.0
                item["_pool"] = {
                    "id": aid,
                    "enabled": bool(meta.get("enabled", True)),
                    "weight": int(meta.get("weight") or 1),
                    "request_count": int(meta.get("request_count") or 0),
                    "success_count": int(meta.get("success_count") or 0),
                    "fail_count": int(meta.get("fail_count") or 0),
                    "last_used_at": meta.get("last_used_at"),
                    "last_error": meta.get("last_error"),
                    "cooldown_until": cd_until,
                    "cooldown_remaining_sec": cd_left,
                    "disabled_for_quota": bool(meta.get("disabled_for_quota")),
                    "disabled_reason": meta.get("disabled_reason"),
                    "quota_disabled_at": meta.get("quota_disabled_at"),
                    "quota_source": meta.get("quota_source"),
                    "last_quota": meta.get("last_quota"),
                    "last_probe": meta.get("last_probe"),
                    "blocked_model_ids": list(blocked.keys()),
                    "in_cooldown": in_cd,
                }

            counts = pool_counts()
            # live ≈ account rows with non-expired token; use page totals for chips
            total = int(paged.get("total") or 0)
            # cheap live estimate from account_status counts path
            base_counts = accounts.account_status(include_accounts=False)
            return {
                "store_source": "postgres",
                "store_backend": "postgres",
                "auth_file": base_counts.get("auth_file"),
                "auth_file_exists": base_counts.get("auth_file_exists"),
                "auth_file_role": "mirror",
                "logged_in": base_counts.get("logged_in"),
                "account_count": base_counts.get("account_count") or total,
                "active_count": base_counts.get("active_count") or total,
                "account_mode": get_account_mode(),
                "platform": base_counts.get("platform"),
                "is_linux": base_counts.get("is_linux"),
                "is_headless": base_counts.get("is_headless"),
                "native_oidc_available": base_counts.get("native_oidc_available"),
                "multi_account": True,
                "accounts": page_items,
                "pool": {
                    "mode": get_account_mode(),
                    "total": counts.get("total") or total,
                    "live": base_counts.get("active_count") or total,
                    "enabled": counts.get("enabled") or total,
                    "in_cooldown": counts.get("in_cooldown") or 0,
                    "quota_disabled": counts.get("quota_disabled") or 0,
                    "model_blocked": counts.get("model_blocked") or 0,
                },
                "page": paged.get("page") or 1,
                "page_size": paged.get("page_size") or page_size,
                "total": total,
                "total_pages": paged.get("total_pages") or 1,
                "q": (q or "").strip(),
                "sort": paged.get("sort") or sort_key,
                "paged": True,
                "fast_path": True,
            }
    except Exception:
        # Fall through to legacy full-scan path (file backend / unexpected errors).
        pass

    # Legacy path: build full merged rows then filter/page (file backend).
    base = accounts.account_status(include_accounts=True)
    pool_full = account_pool.pool_summary(include_accounts=True)
    pool_accounts = list(pool_full.get("accounts") or [])
    pool_map = {a.get("id"): a for a in pool_accounts if isinstance(a, dict) and a.get("id")}
    rows = []
    for acc in (base.get("accounts") or []):
        if not isinstance(acc, dict):
            continue
        aid = acc.get("id")
        p = dict(pool_map.get(aid) or {"id": aid})
        item = dict(acc)
        item["_pool"] = {
            "id": p.get("id") or aid,
            "enabled": p.get("enabled", True),
            "weight": p.get("weight", 1),
            "request_count": p.get("request_count", 0),
            "success_count": p.get("success_count", 0),
            "fail_count": p.get("fail_count", 0),
            "last_used_at": p.get("last_used_at"),
            "last_error": p.get("last_error"),
            "cooldown_until": p.get("cooldown_until"),
            "disabled_for_quota": p.get("disabled_for_quota"),
            "disabled_reason": p.get("disabled_reason"),
            "quota_disabled_at": p.get("quota_disabled_at"),
            "quota_source": p.get("quota_source"),
            "last_quota": p.get("last_quota"),
            "last_probe": p.get("last_probe"),
            "blocked_model_ids": p.get("blocked_model_ids") or [],
            "in_cooldown": p.get("in_cooldown", False),
        }
        rows.append(item)

    query = (q or "").strip().lower()
    if query:
        filtered = []
        for arow in rows:
            p = arow.get("_pool") or {}
            hay = " ".join([
                str(arow.get("email") or ""),
                str(arow.get("id") or ""),
                str(arow.get("user_id") or ""),
                "expired 已过期" if arow.get("expired") else "valid 有效",
                "disabled 已禁用" if p.get("enabled") is False else "enabled 启用",
                "cooldown 冷却" if p.get("in_cooldown") else "",
                "quota 额度禁用 耗尽" if p.get("disabled_for_quota") else "",
                str(p.get("last_error") or ""),
                " ".join(p.get("blocked_model_ids") or []),
            ]).lower()
            if query in hay:
                filtered.append(arow)
        rows = filtered

    rows = _sort_account_rows(rows, sort_key)

    total = len(rows)
    try:
        page_size_i = int(page_size)
    except Exception:
        page_size_i = 25
    try:
        page_i = max(1, int(page))
    except Exception:
        page_i = 1

    if page_size_i <= 0 or page_size_i >= 10000:
        page_items = rows
        page_i = 1
        page_size_i = total or 0
        total_pages = 1
    else:
        page_size_i = max(1, min(200, page_size_i))
        total_pages = max(1, (total + page_size_i - 1) // page_size_i)
        page_i = min(page_i, total_pages)
        start = (page_i - 1) * page_size_i
        page_items = rows[start:start + page_size_i]

    out = {
        "store_source": base.get("store_source") or base.get("store_backend") or "file",
        "store_backend": base.get("store_backend") or base.get("store_source") or "file",
        "auth_file": base.get("auth_file"),
        "auth_file_exists": base.get("auth_file_exists"),
        "auth_file_role": base.get("auth_file_role") or "mirror",
        "logged_in": base.get("logged_in"),
        "account_count": base.get("account_count"),
        "active_count": base.get("active_count"),
        "account_mode": base.get("account_mode"),
        "platform": base.get("platform"),
        "is_linux": base.get("is_linux"),
        "is_headless": base.get("is_headless"),
        "native_oidc_available": base.get("native_oidc_available"),
        "multi_account": base.get("multi_account"),
        "accounts": page_items,
        "pool": {
            "mode": pool_full.get("mode"),
            "total": pool_full.get("total"),
            "live": pool_full.get("live"),
            "enabled": pool_full.get("enabled"),
            "in_cooldown": pool_full.get("in_cooldown"),
            "quota_disabled": pool_full.get("quota_disabled"),
            "model_blocked": pool_full.get("model_blocked"),
        },
        "page": page_i,
        "page_size": page_size_i,
        "total": total,
        "total_pages": total_pages,
        "q": (q or "").strip(),
        "sort": sort_key,
        "paged": True,
        "fast_path": False,
    }
    return out


@router.post("/accounts/login")
async def account_login(
    body: LoginModeBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    return accounts.start_login(body.mode, capture=body.capture)


@router.get("/accounts/login/sessions")
async def login_sessions(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    return {"sessions": accounts.list_login_sessions()}


@router.get("/accounts/login/sessions/{session_id}")
async def login_session_status(
    session_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    sess = accounts.get_login_session(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail="Login session not found")
    return sess


@router.post("/accounts/import")
async def import_account(
    body: ImportAuthBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Import JWT / auth.json JSON body (API / script). Prefer file upload for UI."""
    require_admin(request, x_admin_token)
    result = accounts.import_auth_payload(body.payload, merge=body.merge)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "import failed")
    return result


@router.post("/accounts/import-sso")
async def import_sso(
    body: ImportSsoBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """
    Import accounts from xAI SSO cookies via pure HTTP device flow.
    Each SSO cookie is validated, used to authorize a device code, and the
    resulting access_token / refresh_token is merged into auth.json.
    """
    require_admin(request, x_admin_token)

    def _parse_sso_lines(lines: list[str]) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for raw in lines:
            for line in raw.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                email = ""
                if "----" in line:
                    parts = line.split("----")
                    email = parts[0].strip()
                    line = parts[-1].strip()
                elif ":" in line and not line.startswith("eyJ"):
                    parts = line.rsplit(":", 1)
                    email = parts[0].strip()
                    line = parts[-1].strip()
                out.append((email, line))
        return out

    sso_items = _parse_sso_lines(body.sso_cookies)
    sso_cookies = [sso for _, sso in sso_items]
    if not sso_cookies:
        raise HTTPException(status_code=400, detail="No valid SSO cookies provided")

    results: list[dict[str, Any]] = []
    imported: list[dict[str, Any]] = []
    ok = 0
    fail = 0

    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        from config import SSO_IMPORT_WORKERS
    except Exception:
        SSO_IMPORT_WORKERS = 4
    # Hard cap regardless of client-supplied max_workers
    workers = min(int(body.max_workers), int(SSO_IMPORT_WORKERS), max(1, len(sso_items)))
    # If delay is large, prefer fewer workers to keep pacing meaningful.
    if body.delay and body.delay >= 2:
        workers = min(workers, 2)

    # Stage 1: convert SSO -> token entries concurrently (network bound)
    # Stage 2: merge all successful entries with ONE auth write (storage bound)
    pending_entries: list[dict[str, Any]] = []

    def _convert_one(args: tuple[int, str, str]) -> dict[str, Any]:
        i, email_hint, sso = args
        if body.delay > 0 and i > 1:
            # tiny staggered start only (seconds), not cumulative per index
            time.sleep(min(float(body.delay), 3.0) * (((i - 1) % max(1, workers)) / max(1.0, float(workers))))
        item: dict[str, Any] = {
            "index": i,
            "sso_hint": (sso[:12] + "...") if len(sso) > 12 else sso,
        }
        try:
            token = sso_import.sso_to_token(sso)
            if not token:
                item["status"] = "failed"
                item["error"] = "device flow failed or invalid sso"
                return item
            _key, entry = sso_import.token_to_auth_entry(token, email=email_hint)
            item["status"] = "converted"
            item["entry"] = {
                "key": entry["key"],
                "auth_mode": entry.get("auth_mode", "oidc"),
                "email": entry.get("email", email_hint),
                "refresh_token": entry.get("refresh_token", ""),
                "expires_at": entry.get("expires_at"),
                "oidc_issuer": entry.get("oidc_issuer", sso_import.OIDC_ISSUER),
                "oidc_client_id": entry.get(
                    "oidc_client_id", sso_import.GROK_CLI_CLIENT_ID
                ),
                "user_id": entry.get("user_id") or entry.get("principal_id"),
            }
            return item
        except Exception as e:  # noqa: BLE001
            item["status"] = "failed"
            item["error"] = str(e)
            return item

    converted: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sso-import-") as ex:
        futs = [
            ex.submit(_convert_one, (i, e, s))
            for i, (e, s) in enumerate(sso_items, 1)
        ]
        for fut in as_completed(futs):
            converted.append(fut.result())

    # preserve input order in results
    converted.sort(key=lambda x: int(x.get("index") or 0))
    for item in converted:
        if item.get("status") != "converted":
            fail += 1
            results.append({k: v for k, v in item.items() if k != "entry"})
            continue
        pending_entries.append(item["entry"])
        results.append({k: v for k, v in item.items() if k != "entry"})

    if pending_entries:
        # one storage write for all converted accounts
        bulk = accounts.import_auth_payloads_bulk(pending_entries, merge=body.merge)
        if not bulk.get("ok"):
            # mark all converted as failed import
            for item in results:
                if item.get("status") is None:
                    continue
            # fallback: mark recent converted rows failed
            for item in results:
                if "account_id" not in item and item.get("status") not in ("failed", "ok"):
                    item["status"] = "failed"
                    item["error"] = bulk.get("error") or "bulk import failed"
                    fail += 1
        else:
            # map imported by email/user roughly
            imp = bulk.get("imported") or []
            # attach in order as best-effort
            imp_iter = iter(imp)
            for item in results:
                if item.get("status") == "failed":
                    continue
                info = next(imp_iter, None)
                if not info:
                    item["status"] = "ok"
                    ok += 1
                    continue
                item["status"] = "ok"
                item["account_id"] = info.get("id")
                item["email"] = info.get("email") or item.get("email")
                item["user_id"] = info.get("user_id")
                item["expires_at"] = info.get("expires_at")
                item["has_refresh_token"] = info.get("has_refresh_token")
                ok += 1
                imported.append({
                    "id": info.get("id"),
                    "email": info.get("email"),
                    "user_id": info.get("user_id"),
                    "expires_at": info.get("expires_at"),
                    "has_refresh_token": info.get("has_refresh_token"),
                })

    # recount fail for non-ok
    fail = sum(1 for x in results if x.get("status") != "ok")
    ok = sum(1 for x in results if x.get("status") == "ok")

    return {
        "ok": fail == 0,
        "message": f"SSO 导入完成：{ok} 成功, {fail} 失败（workers={workers}）",
        "total": len(sso_cookies),
        "success": ok,
        "fail": fail,
        "imported": imported,
        "results": results,
        "workers": workers,
        "delay": body.delay,
    }


def _require_register_adapter():
    if reg_adapter is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "注册机模块不可用: "
                f"{_REG_IMPORT_ERROR or 'grok_build_adapter import failed'}. "
                "请确认 grok-build-auth/ 已完整检出，并执行: pip install -r requirements.txt "
                "(协议注册依赖 curl_cffi/requests；Turnstile 需要 YesCaptcha；邮箱使用 MoeMail)"
            ),
        )
    probe = getattr(reg_adapter, "registration_available", None)
    if callable(probe):
        st = probe()
        if not st.get("available"):
            raise HTTPException(
                status_code=503,
                detail=st.get("error")
                or "注册组件不可用，请检查 grok-build-auth 与 YesCaptcha/MoeMail 配置",
            )
    return reg_adapter


def _registration_cfg_from_body(body: EmailRegistrationBody | RegistrationConfigBody) -> dict:
    return {
        "base_url": body.base_url,
        "api_key": getattr(body, "api_key", None),
        "domain": getattr(body, "domain", None),
        "prefix": getattr(body, "prefix", None),
        "expiry_ms": getattr(body, "expiry_ms", None),
        "captcha_provider": getattr(body, "captcha_provider", None),
        "local_solver_url": getattr(body, "local_solver_url", None),
        "yescaptcha_key": getattr(body, "yescaptcha_key", None),
        "proxy": body.proxy,
        "proxy_username": getattr(body, "proxy_username", None),
        "proxy_password": getattr(body, "proxy_password", None),
        "count": getattr(body, "count", None),
        "concurrency": getattr(body, "concurrency", None),
        "stagger_ms": getattr(body, "stagger_ms", None),
    }


@router.get("/accounts/register-email/config")
async def get_email_registration_config(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Load protocol registration form config (DB + env defaults)."""
    require_admin(request, x_admin_token)
    cfg = get_registration_config(include_secrets=True)
    return {
        "ok": True,
        "config": cfg,
        "source": "database" if _registration_has_db_row() else "env",
    }


def _registration_has_db_row() -> bool:
    try:
        from settings_store import _get_setting_value

        return isinstance(_get_setting_value("registration_config", None), dict)
    except Exception:
        return False


@router.put("/accounts/register-email/config")
async def put_email_registration_config(
    body: RegistrationConfigBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Save protocol registration config to database (and apply to runtime)."""
    require_admin(request, x_admin_token)
    try:
        cfg = set_registration_config(
            body.model_dump(exclude_none=False),
            replace=False,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"保存注册配置失败: {e}") from e
    audit_log(
        request,
        action="register.config_save",
        summary="保存协议注册配置",
        target_type="registration",
        detail={"keys": [k for k, v in body.model_dump(exclude_none=False).items() if v not in (None, "")]},
    )
    return {"ok": True, "config": cfg, "message": "注册配置已保存到数据库"}


@router.post("/accounts/register-email")
async def start_email_registration(
    body: EmailRegistrationBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Start protocol registration (grok-build-auth) + MoeMail + SSO import.

    Supports multi-thread batch via count/concurrency/stagger_ms.
    Non-empty form fields override the saved DB/env config; empty fields fall
    back to the persisted registration_config. Successful starts also auto-save
    non-secret form defaults (and any newly provided secrets) to the DB.
    """
    require_admin(request, x_admin_token)
    adapter = _require_register_adapter()
    overrides = _registration_cfg_from_body(body)
    resolved = resolve_registration_inputs(overrides)

    # Auto-persist: keep last used form values so next open works after restart.
    try:
        set_registration_config(resolved, replace=False)
    except Exception:
        pass

    try:
        result = adapter.start_registration(
            proxy=resolved.get("proxy") or None,
            moemail_api_key=resolved.get("api_key") or None,
            moemail_base_url=resolved.get("base_url") or None,
            prefix=resolved.get("prefix") or None,
            domain=resolved.get("domain") or None,
            expiry_ms=resolved.get("expiry_ms"),
            captcha_provider=resolved.get("captcha_provider") or None,
            local_solver_url=resolved.get("local_solver_url") or None,
            yescaptcha_key=resolved.get("yescaptcha_key") or None,
            count=resolved.get("count"),
            concurrency=resolved.get("concurrency"),
            stagger_ms=resolved.get("stagger_ms"),
        )
    except TypeError:
        # Older adapter without batch kwargs.
        try:
            result = adapter.start_registration(
                proxy=resolved.get("proxy") or None,
                moemail_api_key=resolved.get("api_key") or None,
                moemail_base_url=resolved.get("base_url") or None,
                prefix=resolved.get("prefix") or None,
                domain=resolved.get("domain") or None,
                captcha_provider=resolved.get("captcha_provider") or None,
                local_solver_url=resolved.get("local_solver_url") or None,
                yescaptcha_key=resolved.get("yescaptcha_key") or None,
            )
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "start failed")
    return result


@router.post("/accounts/register-email/test-proxy")
async def test_email_registration_proxy(
    body: EmailRegistrationProxyTestBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    from moemail import test_xai_proxy

    resolved = resolve_registration_inputs(
        {
            "proxy": body.proxy,
            "proxy_username": body.proxy_username,
            "proxy_password": body.proxy_password,
        }
    )
    return test_xai_proxy(
        proxy=resolved.get("proxy") or None,
        proxy_username=resolved.get("proxy_username") or None,
        proxy_password=resolved.get("proxy_password") or None,
    )


@router.post("/register-email/test-proxy")
async def test_email_registration_proxy_unscoped(
    body: EmailRegistrationProxyTestBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    # Alias of /accounts/register-email/test-proxy for older UI paths.
    return await test_email_registration_proxy(body, request, x_admin_token)


@router.get("/accounts/register-email/sessions")
async def list_email_registration_sessions(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    if reg_adapter is None:
        return {"sessions": [], "error": _REG_IMPORT_ERROR}
    out = reg_adapter.list_registration_sessions()
    try:
        st = reg_adapter.registration_available()
        if isinstance(out, dict):
            out["adapter_build"] = st.get("adapter_build")
            out["available"] = st.get("available")
            out["engine"] = st.get("engine") or "dongguatanglinux/grok-build-auth"
            out["yescaptcha_configured"] = st.get("yescaptcha_configured")
            out["captcha_provider"] = st.get("captcha_provider")
            out["local_solver_configured"] = st.get("local_solver_configured")
            out["local_solver_url"] = st.get("local_solver_url")
    except Exception:
        pass
    return out


@router.get("/accounts/register-email/batches/{batch_id}")
async def get_email_registration_batch(
    batch_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    adapter = _require_register_adapter()
    getter = getattr(adapter, "get_registration_batch", None)
    if not callable(getter):
        raise HTTPException(status_code=404, detail="batch API not available")
    result = getter(batch_id)
    if not result:
        raise HTTPException(status_code=404, detail="registration batch not found")
    return result


@router.get("/accounts/register-email/sessions/{session_id}")
async def get_email_registration_session(
    session_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    include_auth_json: int = 0,
):
    require_admin(request, x_admin_token)
    adapter = _require_register_adapter()
    result = adapter.get_registration_session(
        session_id,
        include_auth_json=bool(include_auth_json),
    )
    if not result:
        raise HTTPException(status_code=404, detail="registration session not found")
    return result


@router.post("/accounts/import-file")
async def import_account_file(
    request: Request,
    file: UploadFile = File(..., description="auth.json or export JSON"),
    merge: str = Form(default="true"),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Import accounts from a JSON file upload (no paste / textarea)."""
    require_admin(request, x_admin_token)
    merge_flag = str(merge).strip().lower() not in ("0", "false", "no", "off")
    try:
        raw_bytes = await file.read()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"read file failed: {e}") from e
    if not raw_bytes:
        raise HTTPException(status_code=400, detail="empty file")
    if len(raw_bytes) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="file too large (max 8MB)")
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise HTTPException(
            status_code=400, detail=f"file must be UTF-8 JSON: {e}"
        ) from e
    text = text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty file content")
    try:
        payload: Any = json.loads(text)
    except json.JSONDecodeError as e:
        if text.startswith("eyJ") and "." in text and "\n" not in text:
            payload = text
        else:
            raise HTTPException(status_code=400, detail=f"invalid JSON: {e}") from e
    result = accounts.import_auth_payload(payload, merge=merge_flag)
    if not result.get("ok"):
        raise HTTPException(
            status_code=400, detail=result.get("error") or "import failed"
        )
    result["filename"] = file.filename
    return result



@router.post("/accounts/import-files")
async def import_account_files_bulk(
    request: Request,
    files: list[UploadFile] = File(..., description="one or more auth.json files"),
    merge: str = Form(default="true"),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Bulk JSON import: parse all files then one storage write."""
    require_admin(request, x_admin_token)
    merge_flag = str(merge).strip().lower() not in ("0", "false", "no", "off")
    if not files:
        raise HTTPException(status_code=400, detail="no files")
    if len(files) > 200:
        raise HTTPException(status_code=400, detail="too many files (max 200)")

    payloads: list[Any] = []
    file_meta: list[dict[str, Any]] = []
    for f in files:
        try:
            raw_bytes = await f.read()
        except Exception as e:  # noqa: BLE001
            file_meta.append({"filename": f.filename, "ok": False, "error": f"read failed: {e}"})
            continue
        if not raw_bytes:
            file_meta.append({"filename": f.filename, "ok": False, "error": "empty file"})
            continue
        if len(raw_bytes) > 8 * 1024 * 1024:
            file_meta.append({"filename": f.filename, "ok": False, "error": "file too large (max 8MB)"})
            continue
        try:
            text = raw_bytes.decode("utf-8-sig").strip()
        except UnicodeDecodeError as e:
            file_meta.append({"filename": f.filename, "ok": False, "error": f"utf-8 required: {e}"})
            continue
        if not text:
            file_meta.append({"filename": f.filename, "ok": False, "error": "empty content"})
            continue
        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError:
            if text.startswith("eyJ") and "." in text and "\n" not in text:
                payload = text
            else:
                file_meta.append({"filename": f.filename, "ok": False, "error": "invalid JSON"})
                continue
        payloads.append(payload)
        file_meta.append({"filename": f.filename, "ok": True})

    if not payloads:
        raise HTTPException(status_code=400, detail="no valid JSON files")

    result = accounts.import_auth_payloads_bulk(payloads, merge=merge_flag)
    result["file_meta"] = file_meta
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "import failed")
    return result


def _export_response(
    result: dict[str, Any],
    *,
    download: bool,
    filename_prefix: str = "grok2api-auth-export",
) -> Response:
    payload = {
        "exported_at": result.get("exported_at") or time.time(),
        "source": "grokcli-2api",
        "count": result.get("count", 0),
        "auth": result.get("auth") or {},
    }
    if result.get("selected") is not None:
        payload["selected"] = result.get("selected")
    if result.get("missing") is not None:
        payload["missing"] = result.get("missing")
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    if not download:
        return JSONResponse(content=payload)
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    count = int(result.get("count") or 0)
    filename = f"{filename_prefix}-{count}-{ts}.json"
    return Response(
        content=body.encode("utf-8"),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@router.get("/accounts/export")
async def export_accounts(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    download: int = 1,
):
    """
    Export full auth.json (with tokens) for backup / migration.
    download=1 → attachment; download=0 → JSON body.
    """
    require_admin(request, x_admin_token)
    result = accounts.export_auth_payload(include_secrets=True)
    return _export_response(result, download=bool(download))


@router.post("/accounts/export-batch")
async def export_accounts_batch(
    body: AccountBulkExportBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    download: int = 1,
):
    """Export selected accounts only (multi-select)."""
    require_admin(request, x_admin_token)
    ids = [str(x).strip() for x in (body.ids or []) if str(x).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")
    if len(ids) > 2000:
        raise HTTPException(status_code=400, detail="too many ids (max 2000)")
    result = accounts.export_auth_payload(
        include_secrets=bool(body.include_secrets),
        account_ids=ids,
    )
    if not result.get("count"):
        raise HTTPException(status_code=404, detail="no matching accounts to export")
    return _export_response(
        result,
        download=bool(download),
        filename_prefix="grok2api-auth-export-selected",
    )


@router.post("/accounts/logout")
async def account_logout(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    result = accounts.run_logout()
    audit_log(
        request,
        action="accounts.logout_all",
        summary=result.get("message") if isinstance(result, dict) else "清空账号池",
        target_type="pool",
        detail=result if isinstance(result, dict) else None,
        ok=bool((result or {}).get("ok", True)) if isinstance(result, dict) else True,
    )
    return result


@router.delete("/accounts/{account_id:path}")
async def delete_account(
    account_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    if not accounts.remove_account(account_id):
        raise HTTPException(status_code=404, detail="Account not found")
    audit_log(
        request,
        action="accounts.delete",
        summary=f"删除账号：{account_id}",
        target_type="account",
        target_id=account_id,
    )
    return {"ok": True}


class AccountBulkDeleteBody(BaseModel):
    ids: list[str] = Field(default_factory=list, max_length=2000)


@router.post("/accounts/delete-batch")
async def delete_accounts_batch(
    body: AccountBulkDeleteBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Delete multiple accounts from durable store (PostgreSQL + file mirror)."""
    require_admin(request, x_admin_token)
    ids = [str(x).strip() for x in (body.ids or []) if str(x).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")
    if len(ids) > 2000:
        raise HTTPException(status_code=400, detail="too many ids (max 2000)")
    result = accounts.remove_accounts(ids)
    audit_log(
        request,
        action="accounts.delete_batch",
        summary=f"批量删除账号 {result.get('removed_count') or len(result.get('removed') or [])} 个",
        target_type="account",
        detail={
            "requested": result.get("requested"),
            "removed_count": result.get("removed_count"),
            "missing_count": result.get("missing_count"),
            "removed_sample": (result.get("removed") or [])[:20],
        },
    )
    return {"ok": True, **result}


@router.patch("/accounts/{account_id:path}/enabled")
async def set_account_enabled_route(
    account_id: str,
    body: AccountEnabledBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    found = any(a["id"] == account_id for a in accounts.list_accounts())
    if not found:
        raise HTTPException(status_code=404, detail="Account not found")
    rec = account_pool.set_account_enabled(account_id, body.enabled)
    audit_log(
        request,
        action="accounts.set_enabled",
        summary=f"{'启用' if body.enabled else '禁用'}账号：{account_id}",
        target_type="account",
        target_id=account_id,
        detail={"enabled": bool(body.enabled)},
    )
    return {"ok": True, "account": rec}


@router.get("/accounts/quota")
async def list_accounts_quota(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    cached: bool = False,
    refresh: bool = False,
):
    """Quota list.

    - cached=1/refresh=0: return DB-cached last_quota only (fast)
    - default/refresh=1: query upstream and persist snapshots
    """
    require_admin(request, x_admin_token)
    if cached and not refresh:
        return quota.list_cached_quotas(include_expired=True)
    return await quota.fetch_all_quotas(include_expired=False)


def _probe_result_with_pool(account_id: str, probe: dict[str, Any] | None) -> dict[str, Any]:
    """Attach durable pool status so admin UI can patch rows without reload."""
    out = dict(probe or {})
    aid = str(out.get("account_id") or account_id or "").strip()
    if not aid:
        return out
    try:
        state = account_pool.get_account_pool_state() if hasattr(account_pool, "get_account_pool_state") else None
    except Exception:
        state = None
    try:
        from settings_store import get_account_pool_state

        if state is None:
            state = get_account_pool_state()
        meta = (state or {}).get(aid) or {}
        if not isinstance(meta, dict):
            meta = {}
        # Prefer account_pool helper for derived flags when available.
        try:
            pool_view = account_pool._pool_meta(aid, {aid: meta})  # noqa: SLF001
        except Exception:
            pool_view = meta
        out["pool"] = {
            "id": aid,
            "enabled": pool_view.get("enabled", True),
            "in_cooldown": bool(pool_view.get("in_cooldown")),
            "pool_status": pool_view.get("pool_status") or "normal",
            "cooldown_count": int(pool_view.get("cooldown_count") or 0),
            "cooldown_until": pool_view.get("cooldown_until"),
            "cooldown_sec": pool_view.get("cooldown_sec"),
            "cooldown_reason": pool_view.get("cooldown_reason"),
            "cooldown_code": pool_view.get("cooldown_code"),
            "cooldown_model": pool_view.get("cooldown_model"),
            "cooldown_tokens_actual": pool_view.get("cooldown_tokens_actual"),
            "cooldown_tokens_limit": pool_view.get("cooldown_tokens_limit"),
            "status_stack": pool_view.get("status_stack") or [],
            "last_error": pool_view.get("last_error"),
            "last_probe": pool_view.get("last_probe"),
            "last_probe_status": pool_view.get("last_probe_status"),
            "blocked_model_ids": pool_view.get("blocked_model_ids")
            or list((pool_view.get("blocked_models") or {}).keys()),
            "consecutive_fails": pool_view.get("consecutive_fails") or 0,
            "probe_fail_streak": pool_view.get("probe_fail_streak") or 0,
            "disabled_for_quota": bool(pool_view.get("disabled_for_quota")),
        }
        # Convenience top-level mirrors for older UI.
        out["pool_status"] = out["pool"]["pool_status"]
        out["in_cooldown"] = out["pool"]["in_cooldown"]
        out["cooldown_count"] = out["pool"]["cooldown_count"]
    except Exception:
        pass
    return out


@router.post("/accounts/{account_id:path}/probe")
async def account_probe(
    account_id: str,
    body: AccountProbeBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Probe a single account with one model (connectivity / error check)."""
    require_admin(request, x_admin_token)
    model = resolve_model(body.model) if body.model else None
    try:
        result = await asyncio.to_thread(
            model_health.probe_single_account,
            account_id,
            model,
            auto_disable=body.auto_disable,
            source="manual",
        )
    except AuthError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:400]) from e
    return _probe_result_with_pool(account_id, result if isinstance(result, dict) else {"ok": False})


@router.post("/accounts/probe-batch")
async def accounts_probe_batch(
    body: AccountProbeBatchBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Probe selected accounts; return per-account results + durable pool status."""
    require_admin(request, x_admin_token)
    ids = [str(x).strip() for x in (body.ids or []) if str(x).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="ids is empty")
    if len(ids) > 500:
        raise HTTPException(status_code=400, detail="too many ids (max 500)")
    model = resolve_model(body.model) if body.model else None

    def _run() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for aid in ids:
            try:
                r = model_health.probe_single_account(
                    aid,
                    model,
                    auto_disable=body.auto_disable,
                    source="manual_batch",
                )
                out.append(_probe_result_with_pool(aid, r if isinstance(r, dict) else {"ok": False}))
            except Exception as e:  # noqa: BLE001
                out.append(
                    _probe_result_with_pool(
                        aid,
                        {
                            "ok": False,
                            "account_id": aid,
                            "error": str(e)[:300],
                            "result": {"available": False, "error": str(e)[:300]},
                        },
                    )
                )
        return out

    results = await asyncio.to_thread(_run)
    ok_n = sum(1 for r in results if r.get("ok"))
    cool_n = sum(1 for r in results if (r.get("pool") or {}).get("in_cooldown"))
    audit_log(
        request,
        action="accounts.probe_batch",
        summary=f"批量模型探测：成功 {ok_n}/{len(results)} · 冷却 {cool_n}",
        target_type="pool",
        detail={"count": len(results), "ok": ok_n, "cooldown": cool_n},
    )
    return {
        "ok": True,
        "count": len(results),
        "available_count": ok_n,
        "unavailable_count": len(results) - ok_n,
        "cooldown_count": cool_n,
        "results": results,
    }


@router.post("/accounts/probe-all")
async def accounts_probe_all(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Run model probe for every live account (same as background cycle)."""
    require_admin(request, x_admin_token)
    result = await asyncio.to_thread(model_health.run_once, source="manual_all")
    audit_log(
        request,
        action="accounts.probe_all",
        summary=(
            f"全部模型探测：可用 {result.get('available_count')}/{result.get('count')}"
            if isinstance(result, dict) else "全部模型探测"
        ),
        target_type="pool",
        detail={
            "count": (result or {}).get("count") if isinstance(result, dict) else None,
            "available_count": (result or {}).get("available_count") if isinstance(result, dict) else None,
            "unavailable_count": (result or {}).get("unavailable_count") if isinstance(result, dict) else None,
            "auto_action_count": (result or {}).get("auto_action_count") if isinstance(result, dict) else None,
        },
    )
    return result


@router.get("/model-health")
async def model_health_status(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    return model_health.status()


@router.get("/accounts/{account_id:path}/quota")
async def account_quota(
    account_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    cached: bool = False,
    refresh: bool = False,
):
    """Query billing quota for one account.

    cached=1 returns last_quota from DB when present; otherwise falls back to live query.
    """
    require_admin(request, x_admin_token)
    if cached and not refresh:
        try:
            import account_pool
            for a in account_pool.list_pool_accounts():
                if a.get("id") == account_id and isinstance(a.get("last_quota"), dict):
                    q = dict(a["last_quota"])
                    q.setdefault("account_id", account_id)
                    q.setdefault("email", a.get("email"))
                    q["cached"] = True
                    q["pool_disabled"] = bool(a.get("disabled_for_quota") or a.get("enabled") is False)
                    if not q.get("display") and q.get("summary"):
                        q["display"] = {"summary": q.get("summary")}
                    return q
        except Exception:
            pass
    try:
        # Prefer shared helper (handles id alias / remount / refresh).
        result = await asyncio.to_thread(quota.fetch_quota_by_account_id, account_id)
    except AuthError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"quota query failed: {e}") from e
    return result





@router.post("/accounts/{account_id:path}/cooldown/clear")
async def clear_account_cooldown_route(
    account_id: str,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Clear cooldown so account re-enters rotation immediately."""
    require_admin(request, x_admin_token)
    rec = account_pool.clear_account_cooldown(account_id)
    if rec is None:
        raise HTTPException(status_code=404, detail="account not found")
    audit_log(
        request,
        action="accounts.cooldown_clear",
        summary=f"清除冷却：{account_id}",
        target_type="account",
        target_id=account_id,
    )
    return {"ok": True, "account": rec}


@router.post("/accounts/{account_id:path}/kick")
async def kick_account_route(
    account_id: str,
    body: KickAccountBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Kick account from rotation: temporary cooldown or hard disable."""
    require_admin(request, x_admin_token)
    rec = account_pool.kick_from_pool(
        account_id,
        reason=body.reason,
        cooldown_sec=body.cooldown_sec,
    )
    if rec is None:
        raise HTTPException(status_code=404, detail="account not found")
    audit_log(
        request,
        action="accounts.cooldown",
        summary=f"账号进入冷却：{account_id}",
        target_type="account",
        target_id=account_id,
        detail={"reason": body.reason, "cooldown_sec": body.cooldown_sec},
    )
    return {"ok": True, "account": rec}


@router.post("/accounts/model-blocks/prune")
async def prune_model_blocks_route(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Drop expired soft model blocks across the pool."""
    require_admin(request, x_admin_token)
    n = account_pool.prune_expired_model_blocks()
    return {"ok": True, "removed": n}


@router.get("/settings")
async def get_settings(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Full public settings payload for the system settings page."""
    require_admin(request, x_admin_token)
    return {"ok": True, "settings": get_public_settings()}


@router.put("/settings")
async def put_settings(
    body: RuntimeSettingsBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Update one or more runtime settings from admin UI."""
    require_admin(request, x_admin_token)
    patch = body.model_dump(exclude_unset=True)
    if not patch:
        raise HTTPException(status_code=400, detail="没有可更新的字段")
    try:
        settings = update_runtime_settings(patch)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    audit_log(
        request,
        action="settings.update",
        summary="更新系统设置",
        target_type="settings",
        detail={"keys": sorted(list(patch.keys()))[:40]},
    )
    return {"ok": True, "settings": settings}


@router.put("/settings/password")
async def put_admin_password(
    body: ChangePasswordBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Change admin password (requires current password)."""
    require_admin(request, x_admin_token)
    new_pw = body.new_password
    if body.confirm_password is not None and body.confirm_password != new_pw:
        raise HTTPException(status_code=400, detail="两次输入的新密码不一致")
    try:
        change_admin_password(current=body.current_password, new_password=new_pw)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    audit_log(request, action="settings.password_change", summary="修改管理员密码", target_type="settings")
    return {
        "ok": True,
        "message": "密码已更新",
        "settings": get_public_settings(),
    }


@router.put("/settings/token-maintain")
async def set_token_maintain(
    body: FeatureToggleBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Enable/disable background Token auto-renewal worker."""
    require_admin(request, x_admin_token)
    enabled = set_token_maintain_enabled(bool(body.enabled))
    audit_log(
        request,
        action="settings.token_maintain",
        summary=f"Token 自动续期：{'开启' if enabled else '关闭'}",
        target_type="settings",
        detail={"enabled": bool(enabled)},
    )

    return {
        "ok": True,
        "token_maintain_enabled": enabled,
        "settings": {"token_maintain_enabled": enabled},
        "maintainer": token_maintainer.status(light=True),
        "token_maintainer": token_maintainer.status(light=True),
    }


@router.put("/settings/model-health")
async def set_model_health_flag(
    body: FeatureToggleBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Enable/disable background model health probe worker."""
    require_admin(request, x_admin_token)
    enabled = set_model_health_enabled(bool(body.enabled))
    audit_log(
        request,
        action="settings.model_health",
        summary=f"模型健康探测：{'开启' if enabled else '关闭'}",
        target_type="settings",
        detail={"enabled": bool(enabled)},
    )

    return {
        "ok": True,
        "model_health_enabled": enabled,
        "settings": {"model_health_enabled": enabled},
        "model_health": model_health.status(light=True),
    }


@router.put("/settings/account-mode")
async def set_mode(
    body: AccountModeBody,
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    try:
        mode = set_account_mode(body.mode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {
        "ok": True,
        "account_mode": mode,
        "modes": list(VALID_ACCOUNT_MODES),
    }


@router.post("/accounts/refresh")
async def refresh_accounts(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    body: RefreshBody | None = None,
):
    """
    Refresh access tokens via refresh_token and update expires_at.
    force=true (default) refreshes all accounts with a refresh_token.
    Pass body.ids to renew only selected accounts.
    """
    require_admin(request, x_admin_token)
    force = True if body is None else bool(body.force)
    ids = None
    if body is not None and body.ids:
        ids = [str(x).strip() for x in body.ids if str(x).strip()]
        if not ids:
            raise HTTPException(status_code=400, detail="ids is empty")
        if len(ids) > 2000:
            raise HTTPException(status_code=400, detail="too many ids (max 2000)")
    result = accounts.do_refresh_all(force=force, account_ids=ids)
    # also kick background maintainer
    try:
        token_maintainer.request_run_soon()
    except Exception:
        pass
    # Prefer maintainer cycle summary when available for overview widgets.
    try:
        result["maintainer"] = token_maintainer.status(light=True)
        result["token_maintainer"] = result["maintainer"]
    except Exception:
        result["maintainer"] = token_maintainer.status()
        result["token_maintainer"] = result["maintainer"]
    # Normalize common fields for frontend overview text.
    if isinstance(result, dict):
        if "refresh" not in result:
            result["refresh"] = {
                "refreshed": result.get("refreshed"),
                "attempted": result.get("attempted"),
                "failed": result.get("failed"),
                "skipped": result.get("skipped"),
                "deferred": result.get("deferred"),
            }
        audit_log(
            request,
            action="accounts.refresh",
            summary=f"Token 续期{'（强制）' if force else ''}：刷新 {result.get('refreshed') or (result.get('refresh') or {}).get('refreshed') or 0}",
            target_type="pool",
            detail=result.get("refresh") if isinstance(result.get("refresh"), dict) else None,
        )
    return result


@router.get("/maintainer")
async def maintainer_status(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Token auto-refresh worker status (no full account dump)."""
    require_admin(request, x_admin_token)
    # Full account rows already available via /accounts; keep this route small
    # so admin polling stays responsive on 400+ pools.
    return token_maintainer.status()


@router.post("/maintainer/run")
async def maintainer_run(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    body: RefreshBody | None = None,
):
    """Run one maintenance cycle immediately (normalize + refresh)."""
    require_admin(request, x_admin_token)
    force = True if body is None else bool(body.force)
    result = token_maintainer.run_once(force=force)
    audit_log(
        request,
        action="accounts.refresh",
        summary=f"Token 续期{'（强制）' if force else ''}",
        target_type="pool",
        detail=(result.get("refresh") if isinstance(result, dict) else None),
    )
    return result


@router.post("/accounts/normalize")
async def normalize_accounts(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Re-key auth.json to per-user multi-account layout."""
    require_admin(request, x_admin_token)
    return accounts.do_normalize_keys()


@router.get("/models")
async def admin_models(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    return {
        "object": "list",
        "data": load_models_from_cache(),
        "default_model": DEFAULT_MODEL,
    }


@router.post("/models/sync")
async def models_sync(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    """Fetch model list from cli-chat-proxy and update models_cache.json."""
    require_admin(request, x_admin_token)
    result = sync_models_from_upstream()
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "sync failed")
    audit_log(
        request,
        action="models.sync",
        summary=f"同步上游模型 {result.get('count') or ''}",
        target_type="models",
        detail={"count": result.get("count")},
    )
    return result



@router.get("/logs")
async def list_admin_logs(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    page: int = 1,
    page_size: int = 50,
    q: str = "",
    action: str = "",
):
    """Query admin operation logs (PostgreSQL)."""
    require_admin(request, x_admin_token)
    try:
        from store.audit_pg import list_logs

        return list_logs(q=q, action=action, page=page, page_size=page_size)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"logs query failed: {e}") from e


@router.get("/logs/actions")
async def list_admin_log_actions(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    require_admin(request, x_admin_token)
    try:
        from store.audit_pg import list_actions

        return {"ok": True, "actions": list_actions()}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e


# ── usage / token stats ─────────────────────────────────────────────────────


@router.get("/usage/summary")
async def usage_summary(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    days: int = 7,
):
    """Today / last N days / lifetime token + request aggregates."""
    require_admin(request, x_admin_token)
    try:
        import usage_stats

        return await asyncio.to_thread(usage_stats.summary, days=days)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"usage summary failed: {e}") from e


@router.get("/usage/series")
async def usage_series(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    days: int = 7,
):
    require_admin(request, x_admin_token)
    try:
        import usage_stats

        return await asyncio.to_thread(usage_stats.series, days=days)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e


@router.get("/usage/by-key")
async def usage_by_key(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    days: int = 7,
    limit: int = 50,
):
    require_admin(request, x_admin_token)
    try:
        import usage_stats

        return await asyncio.to_thread(
            usage_stats.breakdown, "key", days=days, limit=limit
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e


@router.get("/usage/by-account")
async def usage_by_account(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    days: int = 7,
    limit: int = 50,
):
    require_admin(request, x_admin_token)
    try:
        import usage_stats

        return await asyncio.to_thread(
            usage_stats.breakdown, "account", days=days, limit=limit
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e


@router.get("/usage/by-model")
async def usage_by_model(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    days: int = 7,
    limit: int = 50,
):
    require_admin(request, x_admin_token)
    try:
        import usage_stats

        return await asyncio.to_thread(
            usage_stats.breakdown, "model", days=days, limit=limit
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e


@router.get("/usage/events")
async def usage_events(
    request: Request,
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    q: str = "",
    api_key_id: str = "",
    account_id: str = "",
    model: str = "",
    protocol: str = "",
    client_ip: str = "",
    ok: str = "",
    page: int = 1,
    page_size: int = 50,
):
    """Per-request usage details: tokens, API key, client IP, cache hits."""
    require_admin(request, x_admin_token)
    ok_flag: bool | None = None
    ov = (ok or "").strip().lower()
    if ov in ("1", "true", "yes", "ok", "success"):
        ok_flag = True
    elif ov in ("0", "false", "no", "fail", "failed", "error"):
        ok_flag = False
    try:
        import usage_stats

        return await asyncio.to_thread(
            usage_stats.list_events,
            q=q,
            api_key_id=api_key_id,
            account_id=account_id,
            model=model,
            protocol=protocol,
            client_ip=client_ip,
            ok=ok_flag,
            page=page,
            page_size=page_size,
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300]) from e
