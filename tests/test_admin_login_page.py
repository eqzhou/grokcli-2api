from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOGIN_HTML = ROOT / "static" / "admin" / "login.html"
AUTH_JS = ROOT / "static" / "js" / "auth.js"
ADMIN_CSS = ROOT / "static" / "css" / "admin-antd.css"


def test_login_page_uses_one_compact_panel() -> None:
    html = LOGIN_HTML.read_text(encoding="utf-8")

    assert html.count("g2a-login-panel g2a-card") == 1
    assert "g2a-login-hero" not in html
    assert html.index('id="auth-view"') < html.index('id="boot-view"')


def test_login_probe_does_not_report_private_store_state() -> None:
    auth_js = AUTH_JS.read_text(encoding="utf-8")

    assert 'name: "PostgreSQL"' not in auth_js
    assert 'name: "Redis"' not in auth_js
    assert 'name: "账号数据"' not in auth_js


def test_embedded_login_hidden_state_cannot_be_overridden() -> None:
    css = ADMIN_CSS.read_text(encoding="utf-8")

    assert ".g2a-login-screen-embed.hidden" in css
