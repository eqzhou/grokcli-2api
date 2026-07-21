from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from grok2api.store import sessions_redis  # noqa: E402


class AdminSessionGenerationTests(unittest.TestCase):
    def test_put_stores_current_generation(self) -> None:
        with (
            patch.object(sessions_redis, "redis_enabled", return_value=True),
            patch.object(sessions_redis, "_admin_generation", return_value="7"),
            patch.object(sessions_redis, "set_json") as set_json,
        ):
            sessions_redis.admin_session_put("token", ttl=60)

        payload = set_json.call_args.args[1]
        self.assertEqual(payload["generation"], "7")

    def test_get_rejects_previous_generation(self) -> None:
        with (
            patch.object(sessions_redis, "redis_enabled", return_value=True),
            patch.object(
                sessions_redis,
                "get_json",
                return_value={"ts": 1, "generation": "6"},
            ),
            patch.object(sessions_redis, "_admin_generation", return_value="7"),
        ):
            self.assertFalse(sessions_redis.admin_session_get("old-token"))


if __name__ == "__main__":
    unittest.main()
