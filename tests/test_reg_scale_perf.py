from __future__ import annotations

import asyncio
import threading
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



class AdmissionAndStopTests(unittest.TestCase):
    def test_blocked_turnstile_batch_is_force_finalized_after_stop_grace(self) -> None:
        """A worker stuck inside Turnstile must not keep a stopped batch alive."""
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_blocked_turnstile_batch"
        sid = "gba_test_blocked_turnstile_session"
        now = gba._now()
        batch = {
            "id": bid,
            "status": "running",
            "created_at": now - 60,
            "updated_at": now,
            "count": 100,
            "finished": 75,
            "ok_count": 70,
            "fail_count": 5,
            "cancelled_count": 0,
            "unattempted": 22,
            "inflight": 3,
            "running": 3,
            "runner_alive": True,
            "session_ids": [sid],
        }
        session = {
            "id": sid,
            "batch_id": bid,
            "status": "solving_turnstile",
            "created_at": now - 30,
            "updated_at": now,
            "_cancel_event": threading.Event(),
        }
        task_calls: list[dict] = []
        from concurrent.futures import Future

        queued_future: Future = Future()
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._sessions[sid] = dict(session)
            # Simulates the ThreadPool runner whose Turnstile future never returns.
            gba._active_batch_runners[bid] = True
            gba._active_batch_futures[bid] = {queued_future}
        try:
            with (
                patch.object(gba, "REG_STOP_DRAIN_SEC", 0.05, create=True),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(gba, "_mirror_reg_sess"),
                patch.object(
                    gba,
                    "_record_register_task",
                    side_effect=lambda **kwargs: task_calls.append(dict(kwargs)),
                ),
            ):
                out = gba.stop_registration_batch(bid)
                self.assertTrue(out.get("ok"))
                self.assertTrue(queued_future.cancelled())

                deadline = time.monotonic() + 0.75
                while time.monotonic() < deadline:
                    with gba._lock:
                        current = dict(gba._batches.get(bid) or {})
                    if current.get("status") == "cancelled" and any(
                        call.get("status") == "cancelled" for call in task_calls
                    ):
                        break
                    time.sleep(0.01)

            self.assertEqual(current.get("status"), "cancelled")
            self.assertEqual(current.get("finished"), 75)
            self.assertEqual(current.get("unattempted"), 25)
            self.assertEqual(current.get("inflight"), 0)
            self.assertEqual(current.get("running"), 0)
            self.assertFalse(current.get("runner_alive"))
            terminal = [c for c in task_calls if c.get("status") == "cancelled"]
            self.assertEqual(len(terminal), 1)
            self.assertTrue(terminal[0].get("finished"))
            self.assertEqual(terminal[0].get("progress_done"), 75)
            self.assertEqual(terminal[0].get("progress_total"), 100)
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._active_batch_futures.pop(bid, None)
                gba._sessions.pop(sid, None)
                gba._batches.pop(bid, None)

    def test_startup_reconcile_cancels_orphan_batch_and_task_log(self) -> None:
        """A restart must close durable running UI tasks with no real runner."""
        from grok2api.store import redis_client, sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_restart_orphan_batch"
        now = gba._now()
        batch = {
            "id": bid,
            "status": "running",
            "created_at": now - 600,
            "updated_at": now - 300,
            "count": 100,
            "finished": 75,
            "ok_count": 70,
            "fail_count": 5,
            "cancelled_count": 0,
            "unattempted": 22,
            "inflight": 3,
            "running": 3,
            "runner_alive": True,
            "owner_pid": gba.os.getpid() + 1000,
            "session_ids": [],
        }
        task_calls: list[dict] = []
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(sessions_redis, "reg_batch_list", return_value=[dict(batch)]),
                patch.object(redis_client, "get_str", return_value=None),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(
                    gba,
                    "_record_register_task",
                    side_effect=lambda **kwargs: task_calls.append(dict(kwargs)),
                ),
            ):
                result = gba.reclaim_orphaned_registration_batches(
                    auto_resume=False,
                    max_batches=1,
                    stale_sec=30,
                )

            with gba._lock:
                current = dict(gba._batches.get(bid) or {})
            self.assertEqual(result.get("batches_cancelled"), 1)
            self.assertEqual(current.get("status"), "cancelled")
            self.assertTrue(current.get("cancel_requested"))
            self.assertEqual(current.get("finished"), 75)
            self.assertEqual(current.get("unattempted"), 25)
            self.assertEqual(current.get("inflight"), 0)
            self.assertEqual(current.get("running"), 0)
            self.assertFalse(current.get("runner_alive"))
            terminal = [c for c in task_calls if c.get("status") == "cancelled"]
            self.assertEqual(len(terminal), 1)
            self.assertTrue(terminal[0].get("finished"))
            self.assertEqual(terminal[0].get("task_id"), bid)
            self.assertEqual(terminal[0].get("progress_done"), 75)
            self.assertEqual(terminal[0].get("progress_total"), 100)
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_startup_reconcile_protects_remote_live_runner(self) -> None:
        from grok2api.store import redis_client, sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_remote_live_batch"
        batch = {
            "id": bid,
            "status": "running",
            "created_at": gba._now() - 300,
            "updated_at": gba._now() - 60,
            "count": 100,
            "finished": 25,
            "runner_alive": True,
            "inflight": 2,
            "session_ids": [],
        }
        task_calls: list[dict] = []
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(sessions_redis, "reg_batch_list", return_value=[dict(batch)]),
                patch.object(redis_client, "get_str", return_value="other-worker-token"),
                patch.object(gba, "_record_register_task", side_effect=lambda **kw: task_calls.append(kw)),
            ):
                result = gba.reclaim_orphaned_registration_batches(
                    auto_resume=False, max_batches=0, stale_sec=30
                )

            self.assertEqual(result.get("batches_cancelled"), 0)
            self.assertEqual(gba._batches[bid].get("status"), "running")
            self.assertFalse(task_calls)
        finally:
            with gba._lock:
                gba._batches.pop(bid, None)

    def test_restart_reconcile_cancels_after_previous_runner_lock_expires(self) -> None:
        """A lock left by the old container delays cancellation but never resumes it."""
        from grok2api.store import redis_client, sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_restart_expired_runner_lock"
        batch = {
            "id": bid,
            "status": "running",
            "created_at": gba._now() - 600,
            "updated_at": gba._now() - 300,
            "count": 100,
            "finished": 75,
            "runner_alive": True,
            "inflight": 2,
            "session_ids": [],
        }
        task_calls: list[dict] = []
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(sessions_redis, "reg_batch_list", return_value=[dict(batch)]),
                patch.object(
                    redis_client,
                    "get_str",
                    side_effect=["old-container-token", None],
                ),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(
                    gba,
                    "_record_register_task",
                    side_effect=lambda **kwargs: task_calls.append(dict(kwargs)),
                ),
            ):
                first = gba.reclaim_orphaned_registration_batches(
                    auto_resume=False, max_batches=0, stale_sec=30
                )
                second = gba.reclaim_orphaned_registration_batches(
                    auto_resume=False, max_batches=0, stale_sec=30
                )

            self.assertEqual(first.get("batches_cancelled"), 0)
            self.assertTrue(
                any(
                    item.get("reason") == "remote_runner_lock_live"
                    for item in first.get("skipped", [])
                )
            )
            self.assertEqual(second.get("batches_cancelled"), 1)
            self.assertEqual(gba._batches[bid].get("status"), "cancelled")
            self.assertEqual(len(task_calls), 1)
            self.assertEqual(task_calls[0].get("progress_done"), 75)
        finally:
            with gba._lock:
                gba._batches.pop(bid, None)

    def test_startup_reconcile_protects_batch_when_runner_lock_is_unknown(self) -> None:
        from grok2api.store import redis_client, sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_unknown_runner_lock"
        batch = {
            "id": bid,
            "status": "running",
            "created_at": gba._now() - 600,
            "updated_at": gba._now() - 300,
            "count": 100,
            "finished": 25,
            "runner_alive": True,
            "inflight": 2,
            "session_ids": [],
        }
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(sessions_redis, "reg_batch_list", return_value=[dict(batch)]),
                patch.object(redis_client, "get_str", side_effect=TimeoutError("redis timeout")),
                patch.object(gba, "_record_register_task") as task_log,
            ):
                result = gba.reclaim_orphaned_registration_batches(
                    auto_resume=False, max_batches=0, stale_sec=30
                )

            self.assertEqual(result.get("batches_cancelled"), 0)
            self.assertEqual(gba._batches[bid].get("status"), "running")
            task_log.assert_not_called()
            self.assertTrue(
                any(
                    item.get("reason") == "remote_runner_lock_unknown"
                    for item in result.get("skipped", [])
                )
            )
        finally:
            with gba._lock:
                gba._batches.pop(bid, None)

    def test_session_reclaim_protects_remote_session_when_lock_is_unknown(self) -> None:
        from grok2api.store import redis_client, sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_unknown_session_runner"
        sid = "gba_test_unknown_session"
        now = gba._now()
        batch = {
            "id": bid,
            "status": "running",
            "created_at": now - 600,
            "updated_at": now - 300,
            "count": 1,
            "finished": 0,
            "session_ids": [sid],
        }
        session = {
            "id": sid,
            "batch_id": bid,
            "status": "solving_turnstile",
            "created_at": now - 600,
            "updated_at": now - 300,
        }
        with gba._lock:
            gba._batches[bid] = dict(batch)
            gba._sessions[sid] = dict(session)
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(sessions_redis, "reg_sess_list", return_value=[dict(session)]),
                patch.object(sessions_redis, "reg_batch_get", return_value=dict(batch)),
                patch.object(redis_client, "get_str", side_effect=TimeoutError("redis timeout")),
                patch.object(gba, "_mirror_reg_sess") as mirror,
            ):
                result = gba.reclaim_orphaned_registration_sessions(stale_sec=30)

            self.assertEqual(result.get("reclaimed"), 0)
            self.assertEqual(gba._sessions[sid].get("status"), "solving_turnstile")
            mirror.assert_not_called()
        finally:
            with gba._lock:
                gba._sessions.pop(sid, None)
                gba._batches.pop(bid, None)

    def test_stop_finalizer_does_not_cancel_a_new_runner_generation(self) -> None:
        """A delayed stop timer must not fence a batch that was resumed meanwhile."""
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_stop_generation_fence"
        now = gba._now()
        task_calls: list[dict] = []
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "stopping",
                "cancel_requested": True,
                "count": 100,
                "finished": 75,
                "runner_alive": True,
                "owner_pid": gba.os.getpid(),
                "updated_at": now,
                "session_ids": [],
            }
            gba._active_batch_runners[bid] = "old-runner-token"
        try:
            with (
                patch.object(gba, "REG_STOP_DRAIN_SEC", 0.05),
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(
                    gba,
                    "_record_register_task",
                    side_effect=lambda **kwargs: task_calls.append(dict(kwargs)),
                ),
            ):
                gba._schedule_cancelled_batch_finalizer(
                    bid, reason="old runner did not exit"
                )
                # A force-resume/new runner wins before the old grace timer expires.
                with gba._lock:
                    current = dict(gba._batches[bid])
                    current.update(
                        {
                            "status": "running",
                            "cancel_requested": False,
                            "runner_alive": True,
                            "owner_pid": gba.os.getpid(),
                            "updated_at": gba._now(),
                        }
                    )
                    gba._batches[bid] = current
                    gba._active_batch_runners[bid] = "new-runner-token"
                time.sleep(0.15)

            with gba._lock:
                current = dict(gba._batches.get(bid) or {})
            self.assertEqual(current.get("status"), "running")
            self.assertFalse(current.get("cancel_requested"))
            self.assertEqual(
                gba._active_batch_runners.get(bid), "new-runner-token"
            )
            self.assertFalse(task_calls)
        finally:
            with gba._lock:
                gba._scheduled_batch_finalizers.discard(bid)
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_timeout_finalizer_restarts_sidecar_for_live_runner(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_timeout_sidecar_restart"
        token = "live-runner-token"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "stopping",
                "cancel_requested": True,
                "count": 100,
                "finished": 75,
                "runner_alive": True,
                "inflight": 2,
                "session_ids": [],
            }
            gba._active_batch_runners[bid] = token
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(gba, "_record_register_task"),
                patch.object(gba, "_terminate_registration_sidecar") as terminate,
            ):
                result = gba._finalize_cancelled_batch(
                    bid,
                    reason="worker stuck",
                    stop_kind="stop_timeout",
                    expected_runner_token=token,
                    require_stopping=True,
                )

            self.assertEqual((result or {}).get("status"), "cancelled")
            terminate.assert_called_once_with("worker stuck")
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_force_resume_rejects_local_runner_still_draining(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_force_resume_local_fence"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "stopping",
                "cancel_requested": True,
                "count": 100,
                "finished": 75,
                "state_generation": 7,
                "session_ids": [],
            }
            gba._active_batch_runners[bid] = "draining-runner-token"
        try:
            with patch.object(gba, "_reg_redis", return_value=False):
                result = gba.resume_registration_batch(bid, force=True)

            self.assertFalse(result.get("ok"))
            self.assertIn("still draining", str(result.get("error")))
            self.assertTrue(gba._batches[bid].get("cancel_requested"))
            self.assertEqual(gba._batches[bid].get("status"), "stopping")
            self.assertEqual(gba._batches[bid].get("state_generation"), 7)
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_force_resume_rejects_remote_runner_lock(self) -> None:
        from grok2api.store import redis_client
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_force_resume_remote_fence"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "stopping",
                "cancel_requested": True,
                "count": 100,
                "finished": 75,
                "session_ids": [],
            }
            gba._active_batch_runners.pop(bid, None)
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    redis_client,
                    "get_str",
                    return_value="remote-runner-token",
                ),
                patch.object(gba, "reclaim_orphaned_registration_sessions") as reclaim,
            ):
                result = gba.resume_registration_batch(bid, force=True)

            self.assertFalse(result.get("ok"))
            self.assertIn("lock is still active", str(result.get("error")))
            reclaim.assert_not_called()
            self.assertTrue(gba._batches[bid].get("cancel_requested"))
        finally:
            with gba._lock:
                gba._batches.pop(bid, None)

    def test_runner_acquire_fails_closed_when_redis_is_unavailable(self) -> None:
        from grok2api.store import redis_client
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_runner_acquire_redis_unknown"
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(
                    redis_client,
                    "set_nx_ex",
                    side_effect=TimeoutError("redis unavailable"),
                ),
            ):
                acquired, token = gba._try_acquire_batch_runner(bid)

            self.assertFalse(acquired)
            self.assertIsNone(token)
            self.assertNotIn(bid, gba._active_batch_runners)
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)

    def test_runner_renewal_failure_is_reported(self) -> None:
        from grok2api.store import redis_client
        from grok2api.upstream import grok_build_adapter as gba

        with (
            patch.object(gba, "_reg_redis", return_value=True),
            patch.object(redis_client, "renew_if_owner", return_value=False),
        ):
            self.assertFalse(gba._renew_batch_runner("batch", "token"))

    def test_runner_lock_loss_immediately_restarts_sidecar(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_runner_lock_lost"
        token = "lost-owner-token"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "stopping",
                "cancel_requested": True,
                "count": 100,
                "finished": 75,
                "session_ids": [],
            }
            gba._active_batch_runners[bid] = token
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(gba, "_record_register_task"),
                patch.object(gba, "_terminate_registration_sidecar") as terminate,
            ):
                gba._finalize_cancelled_batch(
                    bid,
                    reason="distributed runner lock renewal failed",
                    stop_kind="runner_lock_lost",
                    expected_runner_token=token,
                    require_stopping=True,
                )

            terminate.assert_called_once()
            self.assertEqual(gba._batches[bid].get("status"), "cancelled")
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_cancelled_batch_rejects_late_running_redis_snapshot(self) -> None:
        from grok2api.store import sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_cancelled_mirror_fence"
        with gba._lock:
            gba._batches[bid] = {"id": bid, "status": "cancelled"}
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=True),
                patch.object(sessions_redis, "reg_batch_put") as put,
            ):
                gba._mirror_reg_batch(
                    bid,
                    {"id": bid, "status": "running", "updated_at": gba._now() + 1},
                    force=True,
                )
            put.assert_not_called()
        finally:
            with gba._lock:
                gba._batches.pop(bid, None)

    def test_redis_cancelled_fence_uses_atomic_script(self) -> None:
        from grok2api.store import redis_client

        client = unittest.mock.Mock()
        client.eval.return_value = 0
        with patch.object(redis_client, "get_client", return_value=client):
            stored = redis_client.set_json_preserving_cancelled(
                "reg:batch:test", {"status": "running"}, 90
            )

        self.assertFalse(stored)
        client.eval.assert_called_once()
        args = client.eval.call_args.args
        self.assertEqual(args[1:3], (1, "reg:batch:test"))
        self.assertIn('"status":"running"', args[3])

    def test_force_resume_advances_generation_and_reopens_task_log(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_force_resume_generation"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "cancelled",
                "cancel_requested": True,
                "state_generation": 4,
                "count": 100,
                "finished": 75,
                "session_ids": [],
                "reg_config": {"captcha_provider": "local", "concurrency": 2},
            }
        task_calls: list[dict] = []
        try:
            with (
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_reg_redis_configured", return_value=False),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(gba, "_ensure_registration_watchdog"),
                patch.object(
                    gba,
                    "_spawn_batch_runner",
                    return_value={"ok": True, "message": "resumed"},
                ),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(
                    gba,
                    "_record_register_task",
                    side_effect=lambda **kwargs: task_calls.append(dict(kwargs)),
                ),
            ):
                result = gba.resume_registration_batch(bid, force=True)

            self.assertTrue(result.get("ok"))
            self.assertEqual(gba._batches[bid].get("state_generation"), 5)
            self.assertEqual(gba._batches[bid].get("status"), "running")
            self.assertTrue(task_calls[-1].get("allow_terminal_restart"))
            self.assertEqual(task_calls[-1].get("status"), "running")
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_failed_resume_does_not_rollback_a_new_runner_generation(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_resume_failure_generation_race"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "cancelled",
                "cancel_requested": True,
                "state_generation": 4,
                "count": 100,
                "finished": 75,
                "session_ids": [],
                "reg_config": {"captcha_provider": "local", "concurrency": 2},
            }

        def lose_lease_to_new_runner(*_args, **_kwargs):
            with gba._lock:
                current = dict(gba._batches[bid])
                current.update(
                    {
                        "status": "running",
                        "cancel_requested": False,
                        "state_generation": 6,
                    }
                )
                gba._batches[bid] = current
                gba._active_batch_runners[bid] = "new-winner-token"
            return {"ok": False, "error": "old resume validation failed"}

        try:
            with (
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_reg_redis_configured", return_value=False),
                patch.object(
                    gba,
                    "reclaim_orphaned_registration_sessions",
                    return_value={"ok": True, "reclaimed": 0, "items": []},
                ),
                patch.object(gba, "_ensure_registration_watchdog"),
                patch.object(
                    gba,
                    "_spawn_batch_runner",
                    side_effect=lose_lease_to_new_runner,
                ),
                patch.object(gba, "_mirror_reg_batch"),
            ):
                result = gba.resume_registration_batch(bid, force=True)

            self.assertFalse(result.get("ok"))
            self.assertEqual(gba._batches[bid].get("status"), "running")
            self.assertEqual(gba._batches[bid].get("state_generation"), 6)
            self.assertFalse(gba._batches[bid].get("cancel_requested"))
            self.assertEqual(
                gba._active_batch_runners.get(bid), "new-winner-token"
            )
        finally:
            with gba._lock:
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_task_log_cancelled_row_rejects_late_progress_without_duplicate(self) -> None:
        from grok2api.store import task_logs_pg

        cursor = unittest.mock.MagicMock()
        cursor.fetchone.side_effect = [None, (42,)]
        connection_cm = unittest.mock.MagicMock()
        connection_cm.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        with (
            patch.object(task_logs_pg, "enabled", return_value=True),
            patch.object(task_logs_pg, "connection", return_value=connection_cm),
        ):
            row_id = task_logs_pg.write_task(
                kind="register",
                task_id="batch",
                status="running",
                finished=False,
            )

        self.assertEqual(row_id, 42)
        statements = [str(call.args[0]) for call in cursor.execute.call_args_list]
        self.assertTrue(
            any(
                "status NOT IN ('cancelled', 'stopped')" in sql
                for sql in statements
            )
        )
        self.assertFalse(any("INSERT INTO task_logs" in sql for sql in statements))

    def test_restart_reconcile_keeps_fail_closed_on_redis_list_error(self) -> None:
        from grok2api.store import sessions_redis
        from grok2api.upstream import grok_build_adapter as gba

        with (
            patch.object(gba, "_reg_redis", return_value=True),
            patch.object(
                sessions_redis,
                "reg_sess_list",
                side_effect=TimeoutError("redis scan failed"),
            ),
        ):
            result = gba.reclaim_orphaned_registration_batches(
                auto_resume=False, max_batches=0, stale_sec=30
            )

        self.assertFalse(result.get("ok"))
        self.assertTrue(result.get("redis_read_unknown"))
        self.assertEqual(result.get("batches_cancelled"), 0)
        self.assertEqual(result.get("batches_resumed"), 0)

    def test_restart_reconcile_treats_configured_inactive_redis_as_unknown(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        with (
            patch.object(gba, "_reg_redis", return_value=False),
            patch.object(gba, "_reg_redis_configured", return_value=True),
        ):
            result = gba.reclaim_orphaned_registration_batches(
                auto_resume=False, max_batches=0, stale_sec=30
            )

        self.assertFalse(result.get("ok"))
        self.assertTrue(result.get("redis_read_unknown"))
        self.assertEqual(result.get("batches_cancelled"), 0)

    def test_sso_orphan_reconcile_preserves_live_remote_owner(self) -> None:
        from grok2api.admin import sso_import
        from grok2api.store import redis_client, sidecar_owner

        job_id = "sso_test_live_remote_owner"
        with sso_import._sso_jobs_lock:
            sso_import._sso_jobs_local[job_id] = {
                "id": job_id,
                "status": "running",
                "total": 10,
                "done": 2,
                "sidecar_owner": "remote-owner",
            }
        try:
            with (
                patch.object(redis_client, "redis_url", return_value=""),
                patch.object(sidecar_owner, "owner_alive", return_value=True),
                patch.object(sso_import, "_sso_job_put") as put,
            ):
                result = sso_import.reconcile_orphaned_sso_jobs()

            self.assertTrue(result.get("ok"))
            self.assertEqual(result.get("cancelled"), 0)
            put.assert_not_called()
        finally:
            with sso_import._sso_jobs_lock:
                sso_import._sso_jobs_local.pop(job_id, None)

    def test_sidecar_owner_lease_fails_closed_after_renew_error(self) -> None:
        from grok2api.store import redis_client, sidecar_owner

        with sidecar_owner._lock:
            previous = sidecar_owner._lease_healthy
        try:
            with (
                patch.object(redis_client, "redis_url", return_value="redis://test"),
                patch.object(redis_client, "set_ex", return_value=False),
            ):
                self.assertFalse(sidecar_owner._renew_once())
                self.assertFalse(sidecar_owner.owner_lease_valid())
                self.assertFalse(
                    sidecar_owner.owner_alive(sidecar_owner.current_owner_id())
                )
        finally:
            with sidecar_owner._lock:
                sidecar_owner._lease_healthy = previous

    def test_sidecar_restart_finalizes_other_local_batches(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        with gba._lock:
            old_runners = dict(gba._active_batch_runners)
            gba._active_batch_runners.clear()
            gba._active_batch_runners.update(
                {"target": "target-token", "sibling": "sibling-token"}
            )
        calls: list[tuple[str, str]] = []
        try:
            with (
                patch.object(
                    gba,
                    "_finalize_cancelled_batch",
                    side_effect=lambda bid, **kw: calls.append(
                        (bid, str(kw.get("stop_kind")))
                    ),
                ),
                patch.object(gba, "_mirror_reg_sess"),
                patch.object(gba, "_record_register_task"),
            ):
                gba._prepare_registration_sidecar_restart("worker stuck")

            self.assertCountEqual(
                calls,
                [("target", "sidecar_restart"), ("sibling", "sidecar_restart")],
            )
        finally:
            with gba._lock:
                gba._active_batch_runners.clear()
                gba._active_batch_runners.update(old_runners)

    def test_stop_all_includes_incomplete_partial_batch(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        batch = {
            "id": "gba_test_stop_all_partial",
            "status": "partial",
            "count": 100,
            "finished": 75,
        }
        with (
            patch.object(
                gba,
                "list_registration_sessions",
                return_value={"sessions": [], "batches": [batch]},
            ),
            patch.object(gba, "stop_registration_batch") as stop_batch,
        ):
            gba.stop_all_active_registrations()

        stop_batch.assert_called_once_with(batch["id"])

    def test_incomplete_partial_batch_is_force_finalized(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        bid = "gba_test_incomplete_partial_stop"
        with gba._lock:
            gba._batches[bid] = {
                "id": bid,
                "status": "partial",
                "count": 100,
                "finished": 75,
                "ok_count": 70,
                "fail_count": 5,
                "runner_alive": True,
                "inflight": 2,
                "session_ids": [],
            }
            # bool simulates a live runner without triggering process restart in test.
            gba._active_batch_runners[bid] = True
        try:
            with (
                patch.object(gba, "REG_STOP_DRAIN_SEC", 0.05),
                patch.object(gba, "_reg_redis", return_value=False),
                patch.object(gba, "_mirror_reg_batch"),
                patch.object(gba, "_record_register_task"),
            ):
                out = gba.stop_registration_batch(bid)
                self.assertEqual(out["batch"].get("status"), "stopping")
                time.sleep(0.15)

            self.assertEqual(gba._batches[bid].get("status"), "cancelled")
            self.assertEqual(gba._batches[bid].get("finished"), 75)
        finally:
            with gba._lock:
                gba._scheduled_batch_finalizers.discard(bid)
                gba._active_batch_runners.pop(bid, None)
                gba._batches.pop(bid, None)

    def test_entrypoint_supervises_registration_sidecar_restart(self) -> None:
        from pathlib import Path

        source = (Path(__file__).resolve().parents[1] / "entrypoint.sh").read_text()
        self.assertIn("registration_sidecar_supervisor", source)
        self.assertIn("registration sidecar exited rc=", source)

    def test_sidecar_startup_runs_fail_closed_reconciliation(self) -> None:
        from grok2api.admin import sso_import
        from grok2api.store import sidecar_owner
        from grok2api.upstream import oidc_auth
        from scripts import registration_service as service

        result = {"batches_cancelled": 0, "sessions_reclaimed": 0}
        with (
            patch.object(
                service.reg,
                "reclaim_orphaned_registration_batches",
                return_value=result,
            ) as reconcile,
            patch.object(service.reg, "_ensure_registration_watchdog") as watchdog,
            patch.object(
                sso_import,
                "reconcile_orphaned_sso_jobs",
                return_value={"ok": True, "cancelled": 0},
            ) as sso_reconcile,
            patch.object(
                oidc_auth,
                "reconcile_orphaned_device_sessions",
                return_value={"ok": True, "cancelled": 0},
            ) as device_reconcile,
            patch.object(sidecar_owner, "start_heartbeat") as heartbeat,
            patch.object(service, "_ensure_shared_sidecar_orphan_watchdog") as shared_watchdog,
        ):
            service.reconcile_orphaned_registration_tasks()

        reconcile.assert_called_once_with(auto_resume=False, max_batches=0)
        watchdog.assert_called_once_with()
        sso_reconcile.assert_called_once_with()
        device_reconcile.assert_called_once_with()
        heartbeat.assert_called_once_with()
        shared_watchdog.assert_called_once_with()

    def test_single_job_start_uses_admission(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        import inspect
        src = inspect.getsource(gba._start_one_registration)
        self.assertIn("_wait_reg_admission", src)
        self.assertIn("admission_flag", src)
        self.assertIn("_release_reg_admission_once", src)

    def test_run_registration_finally_releases_admission(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        import inspect
        src = inspect.getsource(gba._run_registration)
        self.assertIn("_release_reg_admission_once(admission_flag)", src)
        self.assertIn("_detach_reg_handles", src)
        self.assertIn("_close_reg_handles(handles)", src)
        # Promote stopping -> cancelled on worker exit
        self.assertIn('st_now == "stopping"', src)

    def test_dispose_reg_handles_closes_receiver(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        closed = {"n": 0}

        class R:
            def close(self):
                closed["n"] += 1

        ev = __import__("threading").Event()
        sess = {"_receiver": R(), "_cancel_event": ev, "status": "running"}
        gba._dispose_reg_handles(sess)
        self.assertTrue(ev.is_set())
        self.assertEqual(closed["n"], 1)
        self.assertNotIn("_receiver", sess)

    def test_stop_sets_cancel_event_and_disposes(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        sid = "gba_test_stop_1"
        closed = {"n": 0}

        class R:
            def close(self):
                closed["n"] += 1

        ev = threading.Event()
        with gba._lock:
            gba._sessions[sid] = {
                "id": sid,
                "status": "registering",
                "updated_at": gba._now(),
                "created_at": gba._now(),
                "cancel_requested": False,
                "_receiver": R(),
                "_cancel_event": ev,
            }
        try:
            out = gba.stop_registration_session(sid)
            self.assertTrue(out.get("ok"))
            self.assertTrue(ev.is_set())
            self.assertEqual(closed["n"], 1)
            with gba._lock:
                cur = gba._sessions.get(sid) or {}
            self.assertTrue(cur.get("cancel_requested"))
            self.assertEqual(str(cur.get("status")), "stopping")
            self.assertNotIn("_receiver", cur)
        finally:
            with gba._lock:
                gba._sessions.pop(sid, None)

    def test_stop_closes_handles_without_holding_global_lock(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        sid = "gba_test_stop_nonblocking_close"
        close_started = threading.Event()
        allow_close = threading.Event()

        class BlockingReceiver:
            def close(self) -> None:
                close_started.set()
                allow_close.wait(timeout=2)

        with gba._lock:
            gba._sessions[sid] = {
                "id": sid,
                "status": "registering",
                "updated_at": gba._now(),
                "created_at": gba._now(),
                "_receiver": BlockingReceiver(),
                "_cancel_event": threading.Event(),
            }

        worker = threading.Thread(target=gba.stop_registration_session, args=(sid,))
        worker.start()
        try:
            self.assertTrue(close_started.wait(timeout=1), "close was not called")
            acquired = gba._lock.acquire(timeout=0.2)
            self.assertTrue(acquired, "registration lock was held while closing a handle")
            if acquired:
                gba._lock.release()
        finally:
            allow_close.set()
            worker.join(timeout=2)
            with gba._lock:
                gba._sessions.pop(sid, None)

    def test_stop_closes_handles_before_redis_mirror(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        sid = "gba_test_stop_close_before_mirror"
        closed = threading.Event()

        class Receiver:
            def close(self) -> None:
                closed.set()

        with gba._lock:
            gba._sessions[sid] = {
                "id": sid,
                "status": "registering",
                "updated_at": gba._now(),
                "created_at": gba._now(),
                "_receiver": Receiver(),
                "_cancel_event": threading.Event(),
            }

        def mirror(*args, **kwargs) -> None:
            self.assertTrue(closed.is_set(), "Redis mirror ran before handles closed")

        try:
            with patch.object(gba, "_mirror_reg_sess", side_effect=mirror):
                out = gba.stop_registration_session(sid)
            self.assertTrue(out.get("ok"))
            self.assertTrue(closed.is_set())
        finally:
            with gba._lock:
                gba._sessions.pop(sid, None)

    def test_mail_receiver_has_close(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        import inspect
        src = inspect.getsource(gba._make_email_receiver)
        self.assertIn("def close(self)", src)
        self.assertIn("_cancel_event", src)

    def test_session_cancel_honours_event(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        import threading
        ev = threading.Event()
        self.assertFalse(gba._session_cancel_requested({"status": "running", "_cancel_event": ev}))
        ev.set()
        self.assertTrue(gba._session_cancel_requested({"status": "running", "_cancel_event": ev}))



    def test_stop_does_not_demote_terminal(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba
        sid = "gba_test_stop_terminal"
        with gba._lock:
            gba._sessions[sid] = {
                "id": sid,
                "status": "imported",
                "updated_at": gba._now(),
                "created_at": gba._now(),
                "message": "already done",
            }
        try:
            out = gba.stop_registration_session(sid)
            self.assertTrue(out.get("ok"))
            self.assertTrue(out.get("already_terminal"))
            self.assertEqual(out.get("status"), "imported")
            with gba._lock:
                cur = gba._sessions.get(sid) or {}
            self.assertEqual(cur.get("status"), "imported")
        finally:
            with gba._lock:
                gba._sessions.pop(sid, None)

    def test_run_registration_bails_admission_on_missing_session(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        admission_flag = {"released": False}
        with (
            patch.object(gba, "_load_reg_sess", return_value=None),
            patch.object(gba, "_release_reg_admission") as release,
        ):
            gba._run_registration(
                "gba_missing_session",
                "captcha-key",
                "",
                object(),
                admission_flag=admission_flag,
            )
            gba._release_reg_admission_once(admission_flag)

        release.assert_called_once_with()
        self.assertTrue(admission_flag["released"])


class TempmailCloseTests(unittest.TestCase):
    def test_tempmail_close_releases_session(self) -> None:
        from grok2api.upstream import grok_build_adapter as gba

        gba.ensure_xconsole()
        from xconsole_client.tempmail_transport import TempmailInbox

        inbox = TempmailInbox(api_key="k")
        # force-create session without network
        class Fake:
            def close(self):
                self.closed = True
        fake = Fake()
        inbox._http = fake
        inbox.close()
        self.assertIsNone(inbox._http)
        self.assertTrue(fake.closed)
