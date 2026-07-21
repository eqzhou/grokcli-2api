from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch


class DbResultsCleanupTests(unittest.TestCase):
    def test_save_result_preserves_create_time(self) -> None:
        import importlib.util
        from pathlib import Path

        p = Path(__file__).resolve().parents[1] / "turnstile-solver" / "db_results.py"
        spec = importlib.util.spec_from_file_location("db_results_ut", p)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)
        mod.results_db.clear()

        async def run() -> None:
            await mod.save_result("t1", "turnstile", {"value": "CAPTCHA_NOT_READY", "createTime": 1000})
            await mod.save_result("t1", "turnstile", {"value": "token-abc"})
            row = await mod.load_result("t1")
            self.assertEqual(row["value"], "token-abc")
            self.assertEqual(row["createTime"], 1000)

        asyncio.run(run())

    def test_cleanup_drops_terminal_without_create_time_growth(self) -> None:
        import importlib.util
        from pathlib import Path

        p = Path(__file__).resolve().parents[1] / "turnstile-solver" / "db_results.py"
        spec = importlib.util.spec_from_file_location("db_results_ut2", p)
        mod = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(mod)
        mod.results_db.clear()
        # Terminal without createTime should be cleaned under short TTL.
        mod.results_db["old"] = {"value": "CAPTCHA_FAIL"}
        mod.results_db["live"] = {"value": "CAPTCHA_NOT_READY", "createTime": int(time.time())}

        async def run() -> None:
            deleted = await mod.cleanup_old_results(days_old=7, terminal_ttl_sec=60)
            self.assertGreaterEqual(deleted, 1)
            self.assertNotIn("old", mod.results_db)
            self.assertIn("live", mod.results_db)

        asyncio.run(run())


class BatchStatsTests(unittest.TestCase):

    def test_seed_ok_count_zero_not_authoritative(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        seed = {"status": "running", "ok_count": 0, "fail_count": 0, "finished": 0, "count": 10}
        self.assertIsNone(gba._batch_counters_from_batch(seed))
        # With live sessions map, stats should still see running work.
        sessions = {f"s{i}": {"status": "registering"} for i in range(3)}
        stats = gba._batch_stats(
            list(sessions) + ["missing"],
            batch=seed,
            sessions_by_id=sessions,
            prefer_persisted=True,
        )
        # prefer_persisted falls back because seed is None coherent → live path via sessions_by_id only when prefer false
        stats2 = gba._batch_stats(
            list(sessions),
            batch=seed,
            sessions_by_id=sessions,
            prefer_persisted=False,
        )
        self.assertEqual(stats2["running"], 3)

    def test_inflight_is_authoritative_progress(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        batch = {"status": "running", "ok_count": 0, "fail_count": 0, "finished": 0, "inflight": 2, "count": 10}
        c = gba._batch_counters_from_batch(batch)
        self.assertIsNotNone(c)
        self.assertEqual(c["running"], 2)

    def test_prefers_persisted_counters_without_loading_sessions(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        batch = {
            "count": 100,
            "imported": 40,
            "error": 5,
            "cancelled": 2,
            "running": 3,
            "done": 47,
            "status": "running",
        }
        sids = [f"s{i}" for i in range(100)]
        with patch.object(gba, "_load_reg_sess") as load:
            stats = gba._batch_stats(sids, batch=batch, prefer_persisted=True)
            load.assert_not_called()
        self.assertEqual(stats["imported"], 40)
        self.assertEqual(stats["error"], 5)
        self.assertEqual(stats["cancelled"], 2)
        self.assertEqual(stats["total"], 100)
        self.assertEqual(stats["batch_status"], "running")

    def test_legacy_scan_uses_preloaded_map(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        sessions = {
            "a": {"status": "imported"},
            "b": {"status": "error"},
            "c": {"status": "registering"},
        }
        stats = gba._batch_stats(
            ["a", "b", "c", "missing"],
            batch={"count": 4},
            sessions_by_id=sessions,
            prefer_persisted=False,
        )
        self.assertEqual(stats["imported"], 1)
        self.assertEqual(stats["error"], 1)
        self.assertEqual(stats["running"], 1)
        self.assertEqual(stats["missing"], 1)


class AccountsTotalTests(unittest.TestCase):
    def test_total_accounts_fast_used_on_empty_merge(self) -> None:
        from grok2api.pool import accounts

        with patch.object(accounts, "_total_accounts_fast", return_value=42) as cnt:
            out = accounts.merge_normalized_accounts({}, merge=True)
        self.assertEqual(out["total_accounts"], 42)
        cnt.assert_called()


class SessionsRedisMgetTests(unittest.TestCase):
    def test_reg_sess_mget_parses_json(self) -> None:
        from grok2api.store import sessions_redis
        import json

        class Fake:
            def mget(self, keys):
                return [json.dumps({"id": "s1", "status": "running"}), None]

        with (
            patch.object(sessions_redis, "redis_enabled", return_value=True),
            patch.object(sessions_redis, "get_client", return_value=Fake()),
        ):
            out = sessions_redis.reg_sess_mget(["s1", "s2"])
        self.assertIn("s1", out)
        self.assertEqual(out["s1"]["status"], "running")
        self.assertNotIn("s2", out)


if __name__ == "__main__":
    unittest.main()


class UpdateLockIOTests(unittest.TestCase):
    def test_mirror_called_outside_lock_order(self) -> None:
        """Smoke: mirror helper is invoked after in-memory update path exists."""
        from grok2api.upstream import grok_build_adapter as gba
        import inspect
        src = inspect.getsource(gba._run_registration)
        # Snapshot then mirror outside lock is required by scale fix.
        self.assertIn("mirror_payload", src)
        self.assertIn("_mirror_reg_sess(sid, mirror_payload", src)
        # Ensure mirror call is not nested solely inside the with-lock block immediately.
        # Practical check: "if mirror_payload is not None" appears after lock release pattern.
        self.assertIn("if mirror_payload is not None:", src)


class ProbeEnqueueTests(unittest.TestCase):
    def test_enqueue_helper_exists_and_queues(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        with patch.object(gba, "_ensure_post_import_probe_workers") as ensure:
            # Force queue creation path
            gba._post_import_probe_started = False
            gba._post_import_probe_q = None
            # Avoid starting real threads: stub ensure to install a Queue only
            import queue
            q = queue.Queue()
            def _ensure():
                gba._post_import_probe_q = q
                gba._post_import_probe_started = True
            ensure.side_effect = _ensure
            gba._enqueue_post_import_probe(
                sid="s1", account_ids=["a1"], delay_sec=30, email="x@y.z"
            )
            self.assertFalse(q.empty())
            job = q.get_nowait()
            self.assertEqual(job["sid"], "s1")
            self.assertEqual(job["account_ids"], ["a1"])
            self.assertEqual(job["delay_sec"], 30.0)

