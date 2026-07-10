"""Registration runner based on vendored 509992828/grok-register.

Flow:
  1) MoeMail creates temporary email
  2) DrissionPage browser automates x.ai signup and extracts sso cookie
  3) sso_to_auth_json builds auth.json entry (access/refresh)
  4) accounts.import_auth_payload imports that JSON into the local pool immediately
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
VENDOR_DIR = ROOT / "vendors" / "grok-register"
ADAPTER_BUILD = "2026-07-10-grok-register-2"

_sessions: dict[str, dict[str, Any]] = {}
_lock = threading.RLock()


def _now() -> float:
    return time.time()


def _compact(sess: dict[str, Any]) -> dict[str, Any]:
    out = dict(sess)
    out.pop("password", None)
    if out.get("auth_json") and isinstance(out["auth_json"], dict):
        out["auth_json_count"] = len(out.get("auth_json") or {})
        # keep imported summary only
        if "imported" not in (out.get("auth_json") or {}):
            out.pop("auth_json", None)
    return out


def registration_available() -> dict[str, Any]:
    missing: list[str] = []
    if not (VENDOR_DIR / "DrissionPage_example.py").is_file():
        missing.append("vendors/grok-register/DrissionPage_example.py")
    if not (VENDOR_DIR / "email_register.py").is_file():
        missing.append("vendors/grok-register/email_register.py")
    try:
        import DrissionPage  # noqa: F401
    except Exception:
        missing.append("DrissionPage package")
    try:
        from config import MOEMAIL_API_KEY

        moemail_ok = bool(MOEMAIL_API_KEY or os.getenv("GROK2API_MOEMAIL_API_KEY"))
    except Exception:
        moemail_ok = bool(os.getenv("GROK2API_MOEMAIL_API_KEY"))
    return {
        "ok": not missing,
        "available": not missing,
        "engine": "509992828/grok-register",
        "adapter_build": ADAPTER_BUILD,
        "path": str(VENDOR_DIR),
        "moemail_configured": moemail_ok,
        "missing": missing,
        "error": (
            None
            if not missing
            else "缺少依赖: " + ", ".join(missing) + "。请 pip install -r requirements.txt"
        ),
    }


def start_registration(
    *,
    moemail_api_key: str | None = None,
    moemail_base_url: str | None = None,
    domain: str | None = None,
    prefix: str | None = None,
    proxy: str | None = None,
    browser_proxy: str | None = None,
    yescaptcha_key: str | None = None,  # accepted for API compatibility; unused
) -> dict[str, Any]:
    """Start one browser registration job in background."""
    st = registration_available()
    if not st.get("available"):
        return {"ok": False, "error": st.get("error") or "register runner unavailable"}

    from config import MOEMAIL_API_KEY, MOEMAIL_BASE_URL, MOEMAIL_DOMAIN, XAI_PROXY

    api_key = (moemail_api_key or MOEMAIL_API_KEY or "").strip()
    if not api_key:
        return {
            "ok": False,
            "error": "MoeMail API key missing. Set GROK2API_MOEMAIL_API_KEY or pass api_key.",
        }

    sid = f"gr_{uuid.uuid4().hex[:16]}"
    proxy_val = (browser_proxy or proxy or XAI_PROXY or "").strip()
    sess = {
        "id": sid,
        "status": "started",
        "created_at": _now(),
        "updated_at": _now(),
        "email": None,
        "sso": None,
        "message": "queued browser registration (grok-register + MoeMail)",
        "error": None,
        "auth_json": None,
        "adapter_build": ADAPTER_BUILD,
        "engine": "509992828/grok-register",
        "proxy": proxy_val or None,
        "domain": (domain or MOEMAIL_DOMAIN or "").strip() or None,
        "prefix": (prefix or "").strip() or None,
        "moemail_base_url": (moemail_base_url or MOEMAIL_BASE_URL or "").strip() or None,
    }
    with _lock:
        _sessions[sid] = sess

    t = threading.Thread(
        target=_run_one,
        args=(
            sid,
            api_key,
            sess.get("moemail_base_url"),
            sess.get("domain"),
            sess.get("prefix"),
            proxy_val,
        ),
        daemon=True,
        name=f"grok-register-{sid[-8:]}",
    )
    t.start()
    return {"ok": True, **_compact(sess)}


def list_registration_sessions() -> dict[str, Any]:
    with _lock:
        sessions = [_compact(s) for s in _sessions.values()]
    return {
        "sessions": sessions,
        "adapter_build": ADAPTER_BUILD,
        "available": True,
        "engine": "509992828/grok-register",
    }


def get_registration_session(
    sid: str, *, include_auth_json: bool = False
) -> dict[str, Any] | None:
    with _lock:
        sess = _sessions.get(sid)
        if not sess:
            return None
        out = dict(sess)
    out.pop("password", None)
    if not include_auth_json:
        if isinstance(out.get("auth_json"), dict) and "imported" in out["auth_json"]:
            pass
        else:
            out.pop("auth_json", None)
    return out


def _update(sid: str, **kwargs: Any) -> None:
    with _lock:
        sess = _sessions.get(sid)
        if not sess:
            return
        sess.update(kwargs)
        sess["updated_at"] = _now()


def _run_one(
    sid: str,
    api_key: str,
    base_url: str | None,
    domain: str | None,
    prefix: str | None,
    proxy: str,
) -> None:
    try:
        _update(sid, status="registering", message="preparing grok-register runtime")
        if str(VENDOR_DIR) not in sys.path:
            sys.path.insert(0, str(VENDOR_DIR))
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))

        # Configure MoeMail bridge and monkey-patch email_register interfaces
        # used by DrissionPage_example.py.
        import moemail_bridge
        import email_register as er

        moemail_bridge.configure(
            api_key=api_key,
            base_url=base_url,
            domain=domain,
            prefix=prefix,
            proxy=proxy,
        )
        er.get_email_and_token = moemail_bridge.get_email_and_token  # type: ignore[attr-defined]
        er.get_oai_code = moemail_bridge.get_oai_code  # type: ignore[attr-defined]

        # Ensure proxy is visible to browser options loader in DrissionPage_example.
        cfg_path = VENDOR_DIR / "config.json"
        try:
            import json

            cfg = {
                "run": {"count": 1},
                "temp_mail_api_base": base_url or "",
                "temp_mail_admin_password": api_key,
                "temp_mail_domain": domain or "",
                "proxy": proxy or "",
                "browser_proxy": proxy or "",
                "api": {"endpoint": "", "token": "", "append": True},
            }
            cfg_path.write_text(
                json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:  # noqa: BLE001
            print(f"[register_runner] write config.json failed: {e}")

        # Import browser runner only after patching mail APIs.
        import DrissionPage_example as runner

        # DrissionPage_example binds mail helpers at import time; re-bind explicitly.
        runner.get_email_and_token = moemail_bridge.get_email_and_token  # type: ignore[attr-defined]
        runner.get_oai_code = moemail_bridge.get_oai_code  # type: ignore[attr-defined]

        _update(sid, status="registering", message="starting browser registration")
        # Force virtual display on headless hosts when available.
        if not os.environ.get("DISPLAY"):
            os.environ["USE_XVFB"] = "1"

        out_dir = ROOT / "data" / "register_sso"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{sid}.txt"

        # Fresh browser each job.
        try:
            runner.start_browser()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"browser start failed: {e}. "
                "Install chromium/chrome + xvfb, and pip install DrissionPage pyvirtualdisplay"
            ) from e

        try:
            result = runner.run_single_registration(
                output_path=str(out_file), extract_numbers=False
            )
        finally:
            try:
                runner.stop_browser()
            except Exception:
                pass

        email = str((result or {}).get("email") or "")
        sso = str((result or {}).get("sso") or "").strip()
        password = str((result or {}).get("password") or "")
        _update(
            sid,
            email=email or None,
            password=password or None,
            sso=(sso[:24] + "...") if sso else None,
            status="importing",
            message="SSO obtained; converting to auth.json and importing",
        )
        if not sso:
            raise RuntimeError("browser registration finished without sso cookie")

        # Persist raw sso immediately for debugging / re-import.
        try:
            out_file.write_text(sso + "\n", encoding="utf-8")
        except Exception:
            pass

        import accounts
        import sso_to_auth_json as sso_import

        # SSO cookie -> token JSON (device flow) -> auth entry JSON -> import
        token = sso_import.sso_to_token(sso)
        if not token or not (token.get("access_token") or token.get("key")):
            raise RuntimeError(
                "SSO obtained but conversion to token JSON failed "
                "(device verify/approve/token poll)"
            )

        auth_key, entry = sso_import.token_to_auth_entry(token, email=email)
        if email and not entry.get("email"):
            entry["email"] = email
        # Keep a full auth.json-shaped object so import_auth_payload can ingest it
        # the same way as manual "import JSON".
        auth_json_obj = {auth_key: entry}
        try:
            auth_json_path = out_dir / f"{sid}.auth.json"
            auth_json_path.write_text(
                json.dumps(auth_json_obj, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        except Exception as e:  # noqa: BLE001
            print(f"[register_runner] write auth json file failed: {e}")

        # Prefer the shared helper that already wraps import_auth_payload.
        try:
            imported_label = sso_import.import_into_project_auth(entry)
            import_result = {
                "ok": True,
                "imported": [imported_label] if imported_label else [auth_key],
                "auth_file": str(getattr(accounts, "AUTH_FILE", ROOT / "data" / "auth.json")),
            }
        except Exception:
            # Fallback: import full auth.json object directly.
            import_result = accounts.import_auth_payload(auth_json_obj, merge=True)
            if not import_result.get("ok"):
                # Last fallback: single-entry payload.
                import_result = accounts.import_auth_payload(
                    {
                        "key": entry.get("key"),
                        "auth_mode": entry.get("auth_mode", "oidc"),
                        "email": entry.get("email") or email,
                        "refresh_token": entry.get("refresh_token", ""),
                        "expires_at": entry.get("expires_at"),
                        "oidc_issuer": entry.get("oidc_issuer", "https://auth.x.ai"),
                        "oidc_client_id": entry.get("oidc_client_id", ""),
                    },
                    merge=True,
                )
        if not import_result.get("ok"):
            raise RuntimeError(
                f"auth.json import failed: {import_result.get('error') or 'unknown'}"
            )

        imported = import_result.get("imported") or []
        _update(
            sid,
            status="imported",
            auth_json={
                "ok": True,
                "imported": imported,
                "entry": {
                    "email": entry.get("email") or email,
                    "user_id": entry.get("user_id"),
                    "has_refresh_token": bool(entry.get("refresh_token")),
                    "expires_at": entry.get("expires_at"),
                },
            },
            message=(
                f"imported auth.json directly "
                f"({len(imported) if isinstance(imported, list) else 1} account) "
                f"[{ADAPTER_BUILD}]"
            ),
            error=None,
            sso=(sso[:24] + "..."),
        )
    except Exception as e:  # noqa: BLE001
        tb = traceback.format_exc(limit=8)
        print(f"[register_runner] failed: {e}\n{tb}")
        _update(
            sid,
            status="error",
            error=str(e),
            message=f"failed: {e}",
        )
