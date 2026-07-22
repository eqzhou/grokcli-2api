#!/usr/bin/env python3
"""Build content-hashed admin static assets into static/dist and rewrite HTML."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"
ADMIN = STATIC / "admin"
DIST = STATIC / "dist"
DIST.mkdir(exist_ok=True)

ASSETS = {
    "utils.js": STATIC / "js" / "utils.js",
    "api.js": STATIC / "js" / "api.js",
    "state.js": STATIC / "js" / "state.js",
    "auth.js": STATIC / "js" / "auth.js",
    "core.js": STATIC / "js" / "core.js",
    "admin-antd.css": STATIC / "css" / "admin-antd.css",
}


def main() -> None:
    manifest: dict[str, str] = {}
    prepared_assets: list[tuple[str, Path, bytes]] = []
    for name, src in ASSETS.items():
        data = src.read_bytes()
        h = hashlib.sha1(data).hexdigest()[:10]
        out = DIST / (
            f"{name[:-3]}.{h}.js" if name.endswith(".js") else f"{name[:-4]}.{h}.css"
        )
        prepared_assets.append((name, out, data))
        manifest[name] = f"/static/dist/{out.name}"

    prepared_html: list[tuple[Path, str]] = []
    for path in sorted(ADMIN.glob("*.html")):
        html = path.read_text()
        html = re.sub(
            r'href="/static/css/admin-antd\.css[^"]*"',
            f'href="{manifest["admin-antd.css"]}"',
            html,
        )
        html = re.sub(
            r'href="/static/dist/admin-antd\.[^"]+\.css"',
            f'href="{manifest["admin-antd.css"]}"',
            html,
        )
        for logical, hashed in manifest.items():
            if not logical.endswith(".js"):
                continue
            base = logical[:-3]
            html = re.sub(
                rf'src="/static/js/{re.escape(logical)}[^"]*"',
                f'src="{hashed}"',
                html,
            )
            html = re.sub(
                rf'src="/static/dist/{re.escape(base)}\.[^"]+\.js"',
                f'src="{hashed}"',
                html,
            )
        prepared_html.append((path, html))

    # Stage every output first. Old manifest assets remain available until all
    # replacements succeed, so an interrupted build cannot create 404s.
    staged: list[tuple[Path, Path]] = []
    try:
        for _name, out, data in prepared_assets:
            temp = out.with_name(f".{out.name}.tmp")
            temp.write_bytes(data)
            staged.append((temp, out))
        manifest_path = DIST / "manifest.json"
        manifest_temp = manifest_path.with_name(".manifest.json.tmp")
        manifest_temp.write_text(json.dumps(manifest, indent=2) + "\n")
        staged.append((manifest_temp, manifest_path))
        for path, html in prepared_html:
            temp = path.with_name(f".{path.name}.tmp")
            temp.write_text(html)
            staged.append((temp, path))

        # Assets first, then references. Stale assets are removed only after
        # the new manifest and HTML are committed.
        asset_count = len(prepared_assets)
        for temp, target in staged[:asset_count]:
            temp.replace(target)
        for temp, target in staged[asset_count + 1:]:
            temp.replace(target)
        staged[asset_count][0].replace(staged[asset_count][1])
    finally:
        for temp, _target in staged:
            temp.unlink(missing_ok=True)

    active = {out for _name, out, _data in prepared_assets}
    for name, out, _data in prepared_assets:
        stem = name.rsplit(".", 1)[0]
        for stale in DIST.glob(f"{stem}.*{out.suffix}"):
            if stale not in active:
                stale.unlink()
                print("removed", stale.name)
        print("built", name, "->", manifest[name])
    for path, _html in prepared_html:
        print("html", path.name)
    print("OK")


if __name__ == "__main__":
    main()
