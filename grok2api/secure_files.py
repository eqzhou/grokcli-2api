"""Private, atomic file writes for credentials and authentication artifacts."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def secure_write_text(
    path: str | Path,
    text: str,
    *,
    encoding: str = "utf-8",
    secure_parent: bool = False,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if secure_parent:
        os.chmod(target.parent, 0o700)
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    temp = Path(temp_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding=encoding) as handle:
            fd = -1
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, target)
        os.chmod(target, 0o600)
        return target
    except Exception:
        if fd >= 0:
            os.close(fd)
        temp.unlink(missing_ok=True)
        raise
