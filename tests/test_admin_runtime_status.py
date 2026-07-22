from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_empty_account_pool_is_not_reported_as_login_failure() -> None:
    core_js = (ROOT / "static" / "js" / "core.js").read_text(encoding="utf-8")

    assert "未登录 / 凭证异常" not in core_js
    assert 'pill.textContent = "账号池为空"' in core_js


def test_soft_navigation_updates_the_complete_page_heading() -> None:
    core_js = (ROOT / "static" / "js" / "core.js").read_text(encoding="utf-8")

    assert 'doc.querySelector(".g2a-page-kicker")' in core_js
    assert 'document.querySelector(".g2a-page-kicker")' in core_js


def test_confirm_dialog_reuses_an_inflight_confirmation() -> None:
    utils_js = (ROOT / "static" / "js" / "utils.js").read_text(encoding="utf-8")

    assert "let confirmInFlight = null;" in utils_js
    assert "if (confirmInFlight) return Promise.resolve(false);" in utils_js


def test_proxy_password_disables_login_password_autofill() -> None:
    accounts_html = (ROOT / "static" / "admin" / "accounts.html").read_text(encoding="utf-8")

    assert 'id="reg-proxy-password"' in accounts_html
    assert 'autocomplete="off"' in accounts_html
    assert 'data-1p-ignore="true"' in accounts_html
