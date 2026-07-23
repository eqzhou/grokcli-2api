from __future__ import annotations

import importlib.util
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


pytest.importorskip("quart")
pytest.importorskip("camoufox.async_api")
pytest.importorskip("patchright.async_api")
pytest.importorskip("playwright.async_api")


SOLVER_DIR = Path(__file__).resolve().parents[1] / "turnstile-solver"
MODULE_PATH = SOLVER_DIR / "api_solver.py"
if str(SOLVER_DIR) not in sys.path:
    sys.path.insert(0, str(SOLVER_DIR))
SPEC = importlib.util.spec_from_file_location("turnstile_solver_api", MODULE_PATH)
assert SPEC and SPEC.loader
SOLVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SOLVER)


def _server(*, threads: int = 2):
    return SOLVER.TurnstileAPIServer(
        headless=True,
        useragent=None,
        debug=False,
        browser_type="camoufox",
        thread=threads,
        proxy_support=False,
    )


def _chromium_server(*, threads: int = 1):
    return SOLVER.TurnstileAPIServer(
        headless=True,
        useragent=None,
        debug=False,
        browser_type="chromium",
        thread=threads,
        proxy_support=False,
        use_random_config=True,
    )


def test_camoufox_pool_uses_one_driver_and_releases_it(monkeypatch) -> None:
    asyncio.run(_camoufox_pool_uses_one_driver_and_releases_it(monkeypatch))


async def _camoufox_pool_uses_one_driver_and_releases_it(monkeypatch) -> None:
    server = _server(threads=2)
    playwright = MagicMock()
    playwright.stop = AsyncMock()
    manager = MagicMock()
    manager.start = AsyncMock(return_value=playwright)
    browser_one = MagicMock()
    browser_one.close = AsyncMock()
    browser_two = MagicMock()
    browser_two.close = AsyncMock()
    launch_browser = AsyncMock(side_effect=[browser_one, browser_two])

    monkeypatch.setattr(SOLVER, "camoufox_async_playwright", lambda: manager)
    monkeypatch.setattr(SOLVER, "AsyncNewBrowser", launch_browser)
    monkeypatch.setattr(server, "_force_kill_browser", AsyncMock())

    await server._initialize_browser()

    manager.start.assert_awaited_once_with()
    assert launch_browser.await_count == 2
    assert all(call.args[0] is playwright for call in launch_browser.await_args_list)
    assert server.browser_pool.qsize() == 2

    await server._shutdown_browsers()

    browser_one.close.assert_awaited_once_with()
    browser_two.close.assert_awaited_once_with()
    playwright.stop.assert_awaited_once_with()
    assert server._playwright is None
    assert server.browser_pool.qsize() == 0

    await server._shutdown_browsers()

    browser_one.close.assert_awaited_once_with()
    browser_two.close.assert_awaited_once_with()
    playwright.stop.assert_awaited_once_with()


def test_camoufox_partial_startup_closes_browser_and_driver(monkeypatch) -> None:
    asyncio.run(_camoufox_partial_startup_closes_browser_and_driver(monkeypatch))


async def _camoufox_partial_startup_closes_browser_and_driver(monkeypatch) -> None:
    server = _server(threads=2)
    playwright = MagicMock()
    playwright.stop = AsyncMock()
    manager = MagicMock()
    manager.start = AsyncMock(return_value=playwright)
    browser_one = MagicMock()
    browser_one.close = AsyncMock()
    launch_browser = AsyncMock(
        side_effect=[browser_one, RuntimeError("second browser failed")]
    )

    monkeypatch.setattr(SOLVER, "camoufox_async_playwright", lambda: manager)
    monkeypatch.setattr(SOLVER, "AsyncNewBrowser", launch_browser)
    monkeypatch.setattr(server, "_force_kill_browser", AsyncMock())

    with pytest.raises(RuntimeError, match="second browser failed"):
        await server._initialize_browser()

    browser_one.close.assert_awaited_once_with()
    playwright.stop.assert_awaited_once_with()
    assert server._owned_browsers == []
    assert server.browser_pool.qsize() == 0
    assert server._playwright is None
    assert not server._pool_ready


def test_app_exit_registers_browser_cleanup() -> None:
    server = _server(threads=1)

    assert server._shutdown in server.app.after_serving_funcs


def test_config_failure_after_driver_start_releases_driver(monkeypatch) -> None:
    asyncio.run(_config_failure_after_driver_start_releases_driver(monkeypatch))


async def _config_failure_after_driver_start_releases_driver(monkeypatch) -> None:
    server = _chromium_server()
    playwright = MagicMock()
    playwright.stop = AsyncMock()
    manager = MagicMock()
    manager.start = AsyncMock(return_value=playwright)

    monkeypatch.setattr(SOLVER, "patchright_async_playwright", lambda: manager)
    monkeypatch.setattr(
        SOLVER.browser_config,
        "get_random_browser_config",
        MagicMock(side_effect=RuntimeError("config failed")),
    )

    with pytest.raises(RuntimeError, match="config failed"):
        await server._initialize_browser()

    playwright.stop.assert_awaited_once_with()
    assert server._playwright is None
    assert server._owned_browsers == []
    assert server.browser_pool.qsize() == 0
