import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_builder():
    path = ROOT / "scripts" / "build_admin_assets.py"
    spec = importlib.util.spec_from_file_location("build_admin_assets", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_build_removes_stale_hashed_assets(tmp_path) -> None:
    builder = _load_builder()
    static = tmp_path / "static"
    admin = static / "admin"
    dist = static / "dist"
    js = static / "js"
    css = static / "css"
    for directory in (admin, dist, js, css):
        directory.mkdir(parents=True, exist_ok=True)

    (js / "core.js").write_text("console.log('current');\n", encoding="utf-8")
    (css / "admin-antd.css").write_text("body {}\n", encoding="utf-8")
    (admin / "index.html").write_text(
        '<script src="/static/dist/core.aaaaaaaaaa.js"></script>'
        '<link href="/static/dist/admin-antd.aaaaaaaaaa.css" rel="stylesheet">',
        encoding="utf-8",
    )
    stale_js = dist / "core.aaaaaaaaaa.js"
    stale_css = dist / "admin-antd.aaaaaaaaaa.css"
    unrelated = dist / "keep.txt"
    stale_js.write_text("stale", encoding="utf-8")
    stale_css.write_text("stale", encoding="utf-8")
    unrelated.write_text("keep", encoding="utf-8")

    builder.STATIC = static
    builder.ADMIN = admin
    builder.DIST = dist
    builder.ASSETS = {
        "core.js": js / "core.js",
        "admin-antd.css": css / "admin-antd.css",
    }
    builder.main()

    assert not stale_js.exists()
    assert not stale_css.exists()
    assert unrelated.exists()
    assert len(list(dist.glob("core.*.js"))) == 1
    assert len(list(dist.glob("admin-antd.*.css"))) == 1


def test_failed_build_preserves_previous_assets_and_manifest(tmp_path) -> None:
    builder = _load_builder()
    static = tmp_path / "static"
    admin = static / "admin"
    dist = static / "dist"
    js = static / "js"
    for directory in (admin, dist, js):
        directory.mkdir(parents=True, exist_ok=True)
    old_asset = dist / "core.aaaaaaaaaa.js"
    old_asset.write_text("old", encoding="utf-8")
    old_manifest = '{"core.js":"/static/dist/core.aaaaaaaaaa.js"}\n'
    (dist / "manifest.json").write_text(old_manifest, encoding="utf-8")
    (js / "core.js").write_text("new", encoding="utf-8")
    builder.STATIC = static
    builder.ADMIN = admin
    builder.DIST = dist
    builder.ASSETS = {
        "core.js": js / "core.js",
        "missing.js": js / "missing.js",
    }

    try:
        builder.main()
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("build should fail when a source asset is missing")

    assert old_asset.read_text(encoding="utf-8") == "old"
    assert (dist / "manifest.json").read_text(encoding="utf-8") == old_manifest
