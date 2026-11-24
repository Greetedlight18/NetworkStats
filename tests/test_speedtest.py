import asyncio
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

# Lightweight container dependency stubs when running outside Docker
if "icmplib" not in sys.modules:
    icmplib_stub = types.ModuleType("icmplib")

    class _SocketPermissionError(Exception):
        pass

    async def _stub_async_ping(*args, **kwargs):
        raise NotImplementedError("Mock async_ping must be patched in tests")

    def _stub_traceroute(*args, **kwargs):
        return []

    icmplib_stub.async_ping = _stub_async_ping
    icmplib_stub.traceroute = _stub_traceroute
    icmplib_stub.SocketPermissionError = _SocketPermissionError
    sys.modules["icmplib"] = icmplib_stub

if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")
    fastapi_cors_stub = types.ModuleType("fastapi.middleware.cors")
    fastapi_mw_stub = types.ModuleType("fastapi.middleware")
    fastapi_resp_stub = types.ModuleType("fastapi.responses")
    fastapi_static_stub = types.ModuleType("fastapi.staticfiles")
    pydantic_stub = types.ModuleType("pydantic")

    class _DummyApp:
        def __init__(self, *args, **kwargs):
            pass

        def add_middleware(self, *args, **kwargs):
            pass

        def mount(self, *args, **kwargs):
            pass

        def _decorator(self, *args, **kwargs):
            return lambda fn: fn

        get = post = patch = delete = websocket = middleware = _decorator

    class _DummyHTTPException(Exception):
        def __init__(self, status_code=500, detail=""):
            self.status_code = status_code
            self.detail = detail

    class _DummyBaseModel:
        pass

    fastapi_stub.FastAPI = _DummyApp
    fastapi_stub.HTTPException = _DummyHTTPException
    fastapi_stub.Query = lambda *args, **kwargs: None
    fastapi_stub.Request = object
    fastapi_stub.WebSocket = object
    fastapi_stub.WebSocketDisconnect = Exception
    fastapi_cors_stub.CORSMiddleware = object
    fastapi_resp_stub.FileResponse = object
    fastapi_static_stub.StaticFiles = lambda *args, **kwargs: None
    pydantic_stub.BaseModel = _DummyBaseModel
    pydantic_stub.Field = lambda *args, **kwargs: None

    sys.modules["fastapi"] = fastapi_stub
    sys.modules["fastapi.middleware"] = fastapi_mw_stub
    sys.modules["fastapi.middleware.cors"] = fastapi_cors_stub
    sys.modules["fastapi.responses"] = fastapi_resp_stub
    sys.modules["fastapi.staticfiles"] = fastapi_static_stub
    sys.modules["pydantic"] = pydantic_stub

from app import database  # noqa: E402
from app import main as main_mod  # noqa: E402
from app import speedtest as st_mod  # noqa: E402


class _FakeClock:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._now = 1000.0

    def perf_counter(self) -> float:
        with self._lock:
            return self._now

    def advance(self, sec: float) -> float:
        with self._lock:
            self._now += sec
            return self._now

    def time(self) -> float:
        return time.time()

    def sleep(self, sec: float) -> None:
        self.advance(sec)


class _FakeResponse:
    def __init__(self, clock: _FakeClock, headers: dict, body_bytes: int, bps: float) -> None:
        self._clock = clock
        self._headers = dict(headers)
        self._remaining = int(body_bytes)
        self._bps = float(bps)
        self.status = 200

    def getheaders(self):
        return list(self._headers.items())

    def read(self, amt: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if amt is None or amt < 0:
            to_read = self._remaining
        else:
            to_read = min(self._remaining, amt)
        self._remaining -= to_read
        self._clock.advance(to_read / self._bps)
        return b"\x00" * to_read


class _RecordingScenario:
    def __init__(
        self,
        clock: _FakeClock,
        dl_bps: float = 12_500_000.0,  # 100 Mbps
        ul_bps: float = 2_500_000.0,   # 20 Mbps
        rtt_sec: float = 0.02,         # 20 ms
    ) -> None:
        self.clock = clock
        self.dl_bps = dl_bps
        self.ul_bps = ul_bps
        self.rtt_sec = rtt_sec
        self.requests = []
        self.conn_count = 0
        self.lock = threading.Lock()

    def make_connection(self, timeout: float = 8.0):
        with self.lock:
            self.conn_count += 1
        return _FakeHTTPSConnection(self)


class _FakeHTTPSConnection:
    def __init__(self, scenario: _RecordingScenario) -> None:
        self._s = scenario
        self._pending_resp = None
        self._upload_target = 0
        self._upload_sent = 0
        self._closed = False

    def request(self, method: str, path: str, headers=None) -> None:
        with self._s.lock:
            self._s.requests.append((method, path))
        self._s.clock.advance(self._s.rtt_sec)
        nbytes = 0
        if "bytes=" in path:
            nbytes = int(path.split("bytes=")[1].split("&")[0])
        resp_headers = {
            "cf-meta-colo": "YUL",
            "city": "Montr%C3%A9al",
        }
        self._pending_resp = _FakeResponse(
            self._s.clock, resp_headers, body_bytes=nbytes, bps=self._s.dl_bps
        )

    def putrequest(self, method: str, path: str) -> None:
        with self._s.lock:
            self._s.requests.append((method, path))
        self._upload_target = 0
        self._upload_sent = 0

    def putheader(self, name: str, value: str) -> None:
        if name.lower() == "content-length":
            self._upload_target = int(value)

    def endheaders(self) -> None:
        pass

    def send(self, data: bytes) -> None:
        n = len(data)
        self._upload_sent += n
        self._s.clock.advance(n / self._s.ul_bps)

    def getresponse(self) -> _FakeResponse:
        if self._pending_resp is not None:
            resp = self._pending_resp
            self._pending_resp = None
            return resp
        # Upload ACK response
        self._s.clock.advance(self._s.rtt_sec)
        return _FakeResponse(self._s.clock, {"cf-meta-colo": "YUL"}, body_bytes=0, bps=1e9)

    def close(self) -> None:
        self._closed = True


class SpeedtestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = database.DB_PATH
        database.DB_PATH = os.path.join(self._tmpdir.name, "test_speedtest.db")
        database.close_thread_connections()
        database.init_db()

    async def asyncTearDown(self) -> None:
        database.close_thread_connections()
        database.DB_PATH = self._orig_db_path
        self._tmpdir.cleanup()

    def test_speedtest_settings_defaults_update_and_validation(self) -> None:
        defaults = database.get_speedtest_settings()
        self.assertTrue(defaults["enabled"])
        self.assertEqual(defaults["interval_min"], 60)
        self.assertEqual(defaults["mode"], "eco")

        updated = database.update_speedtest_settings(
            enabled=False, interval_min=15, mode="eco_download_only"
        )
        self.assertFalse(updated["enabled"])
        self.assertEqual(updated["interval_min"], 15)
        self.assertEqual(updated["mode"], "eco_download_only")

        reloaded = database.get_speedtest_settings()
        self.assertEqual(reloaded, updated)

        with self.assertRaises(ValueError):
            database.update_speedtest_settings(interval_min=4)
        with self.assertRaises(ValueError):
            database.update_speedtest_settings(interval_min=2000)
        with self.assertRaises(ValueError):
            database.update_speedtest_settings(mode="gigabit_spam")

    def test_speedtest_history_summary_window_clear_and_prune(self) -> None:
        now = int(time.time())
        # Old record (400 days ago -> should be excluded from 24h window AND pruned)
        database.save_speedtest_result(
            {
                "timestamp": now - (400 * 86400),
                "download_mbps": 50.0,
                "upload_mbps": 10.0,
                "ping_ms": 25.0,
                "jitter_ms": 2.0,
                "bytes_used": 10_000_000,
                "duration_sec": 3.0,
                "mode": "eco",
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )
        # Recent ok records
        database.save_speedtest_result(
            {
                "timestamp": now - 1200,
                "download_mbps": 100.0,
                "upload_mbps": 20.0,
                "ping_ms": 18.0,
                "jitter_ms": 1.5,
                "bytes_used": 20_000_000,
                "duration_sec": 3.2,
                "mode": "eco",
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )
        database.save_speedtest_result(
            {
                "timestamp": now - 600,
                "download_mbps": 200.0,
                "upload_mbps": 40.0,
                "ping_ms": 12.0,
                "jitter_ms": 0.8,
                "bytes_used": 30_000_000,
                "duration_sec": 2.9,
                "mode": "balanced",
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": "manual",
                "status": "ok",
            }
        )
        # Recent error record (should not skew speed averages, latest != latest_ok)
        database.save_speedtest_result(
            {
                "timestamp": now - 60,
                "download_mbps": None,
                "upload_mbps": None,
                "ping_ms": None,
                "jitter_ms": None,
                "bytes_used": 500_000,
                "duration_sec": 1.1,
                "mode": "eco",
                "server_location": "Cloudflare Edge",
                "trigger": "manual",
                "status": "error",
                "error_message": "Simulated failure",
            }
        )

        hist_24h = database.get_speedtest_history(window_seconds=86400)
        summary = hist_24h["summary"]
        self.assertEqual(summary["total_runs"], 3)
        self.assertEqual(summary["successful_runs"], 2)
        self.assertEqual(summary["failed_runs"], 1)
        self.assertEqual(summary["avg_download_mbps"], 150.0)
        self.assertEqual(summary["min_download_mbps"], 100.0)
        self.assertEqual(summary["max_download_mbps"], 200.0)
        self.assertEqual(summary["avg_upload_mbps"], 30.0)
        self.assertEqual(summary["avg_ping_ms"], 15.0)
        self.assertEqual(summary["total_bytes_used"], 50_500_000)

        self.assertEqual(hist_24h["latest"]["status"], "error")
        self.assertEqual(hist_24h["latest_ok"]["download_mbps"], 200.0)

        # Prune should delete the 400-day-old row while keeping recent 3 rows
        database.prune_old_metrics()
        hist_all = database.get_speedtest_history(window_seconds=0)
        self.assertEqual(len(hist_all["results"]), 3)

        removed = database.clear_speedtest_results()
        self.assertEqual(removed, 3)
        self.assertIsNone(database.get_latest_speedtest_ts())

    def test_eco_mode_low_bandwidth_and_location(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=12_500_000.0, ul_bps=2_500_000.0, rtt_sec=0.02)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_cloudflare_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["mode"], "eco")
        self.assertEqual(res["server_location"], "Cloudflare Montréal (YUL)")
        self.assertAlmostEqual(res["ping_ms"], 20.0, delta=0.5)
        self.assertAlmostEqual(res["download_mbps"], 100.0, delta=5.0)
        self.assertAlmostEqual(res["upload_mbps"], 20.0, delta=2.0)
        # 1 MB cal + <=25 MB DL + 256 KB cal + <=10 MB UL
        self.assertLessEqual(res["bytes_used"], 36_500_000)
        self.assertEqual(scenario.conn_count, 2)  # 1 seq DL + 1 seq UL

    def test_eco_download_only_skips_upload(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=6_250_000.0, ul_bps=1_250_000.0, rtt_sec=0.015)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_cloudflare_speedtest(
                mode="eco_download_only", trigger="manual", cooldown_sec=0.0
            )

        self.assertEqual(res["status"], "ok")
        self.assertIsNone(res["upload_mbps"])
        self.assertAlmostEqual(res["download_mbps"], 50.0, delta=3.0)
        upload_reqs = [req for req in scenario.requests if req[0] == "POST"]
        self.assertEqual(upload_reqs, [])

    def test_slow_line_short_circuits_after_calibration(self) -> None:
        clock = _FakeClock()
        # 4 Mbps = 500,000 bytes/sec -> 1 MB calibration takes 2.0s (>= 1.5s threshold)
        scenario = _RecordingScenario(clock, dl_bps=500_000.0, ul_bps=250_000.0, rtt_sec=0.03)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_cloudflare_speedtest(
                mode="eco_download_only", trigger="scheduled", cooldown_sec=0.0
            )

        self.assertEqual(res["status"], "ok")
        bulk_dl_reqs = [
            req for req in scenario.requests if req[0] == "GET" and "bytes=0" not in req[1]
        ]
        self.assertEqual(len(bulk_dl_reqs), 1)  # skipped second download burst
        self.assertAlmostEqual(res["download_mbps"], 4.0, delta=0.5)

    def test_balanced_mode_runs_two_streams(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=25_000_000.0, ul_bps=12_500_000.0, rtt_sec=0.01)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_cloudflare_speedtest(
                mode="balanced", trigger="manual", cooldown_sec=0.0
            )

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["mode"], "balanced")
        self.assertGreater(res["download_mbps"], 50.0)
        self.assertGreater(res["upload_mbps"], 20.0)
        # Initial DL conn + 2 parallel DL + initial UL conn + 2 parallel UL = 6 connections
        self.assertEqual(scenario.conn_count, 6)

    def test_network_error_returns_clean_error_dict(self) -> None:
        with patch.object(st_mod, "_make_connection", side_effect=OSError("DNS lookup failed")):
            res = st_mod.run_cloudflare_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(res["status"], "error")
        self.assertIn("DNS lookup failed", res["error_message"])
        self.assertIsNone(res["download_mbps"])
        self.assertIsNone(res["upload_mbps"])

    async def test_scheduler_concurrent_manual_guard_and_ws_events(self) -> None:
        events = []

        async def _collect_ws(msg):
            events.append(msg)

        gate = threading.Event()

        def _blocking_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0):
            gate.wait(timeout=2.0)
            return {
                "timestamp": int(time.time()),
                "download_mbps": 321.5,
                "upload_mbps": 45.2,
                "ping_ms": 14.1,
                "jitter_ms": 1.2,
                "bytes_used": 22_000_000,
                "duration_sec": 2.4,
                "mode": mode,
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": trigger,
                "status": "ok",
                "error_message": None,
            }

        scheduler = st_mod.SpeedtestScheduler(broadcast_cb=_collect_ws, startup_grace_sec=3600)
        with patch.object(st_mod, "run_cloudflare_speedtest", side_effect=_blocking_speedtest):
            await scheduler.start()
            try:
                started_1 = scheduler.trigger_manual_run()
                started_2 = scheduler.trigger_manual_run()
                self.assertTrue(started_1)
                self.assertFalse(started_2)

                await asyncio.sleep(0.05)
                gate.set()
                await scheduler._manual_task
            finally:
                await scheduler.stop()

        self.assertFalse(scheduler.is_running)
        event_types = [e["type"] for e in events]
        self.assertIn("speedtest_status", event_types)
        self.assertIn("speedtest_result", event_types)

        res_evt = next(e for e in events if e["type"] == "speedtest_result")
        self.assertEqual(res_evt["result"]["download_mbps"], 321.5)
        self.assertFalse(res_evt["status"]["is_running"])

        hist = database.get_speedtest_history(window_seconds=0)
        self.assertEqual(len(hist["results"]), 1)
        self.assertEqual(hist["latest_ok"]["trigger"], "manual")

    async def test_scheduler_respects_recent_run_and_updates_on_settings_change(self) -> None:
        recent_ts = int(time.time()) - 60
        database.save_speedtest_result(
            {
                "timestamp": recent_ts,
                "download_mbps": 150.0,
                "upload_mbps": 30.0,
                "ping_ms": 15.0,
                "jitter_ms": 1.0,
                "bytes_used": 18_000_000,
                "duration_sec": 2.5,
                "mode": "eco",
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )

        called = []

        def _unexpected_run(*args, **kwargs):
            called.append(True)
            raise AssertionError("Should not run on boot when recent run is inside interval")

        scheduler = st_mod.SpeedtestScheduler(startup_grace_sec=0)
        with patch.object(st_mod, "run_cloudflare_speedtest", side_effect=_unexpected_run):
            await scheduler.start()
            try:
                await asyncio.sleep(0.08)
                self.assertEqual(called, [])
                self.assertEqual(scheduler.next_run_ts, recent_ts + 3600)

                updated = database.update_speedtest_settings(interval_min=30)
                scheduler.on_settings_updated(updated)
                await asyncio.sleep(0.05)
                self.assertEqual(scheduler.next_run_ts, recent_ts + 1800)
                self.assertEqual(called, [])
            finally:
                await scheduler.stop()

    async def test_api_speedtest_endpoints_roundtrip(self) -> None:
        status_resp = await main_mod.api_speedtest_status()
        self.assertIn("status", status_resp)
        self.assertIsNone(status_resp["latest"])

        patch_resp = await main_mod.api_update_speedtest_settings(
            SimpleNamespace(enabled=False, interval_min=30, mode="eco_download_only")
        )
        self.assertFalse(patch_resp["status"]["enabled"])
        self.assertEqual(patch_resp["status"]["interval_min"], 30)
        self.assertEqual(patch_resp["status"]["mode"], "eco_download_only")

        with self.assertRaises(main_mod.HTTPException) as ctx:
            await main_mod.api_update_speedtest_settings(
                SimpleNamespace(enabled=None, interval_min=2, mode=None)
            )
        self.assertEqual(ctx.exception.status_code, 400)

        database.save_speedtest_result(
            {
                "timestamp": int(time.time()),
                "download_mbps": 250.0,
                "upload_mbps": None,
                "ping_ms": 14.0,
                "jitter_ms": 1.0,
                "bytes_used": 14_000_000,
                "duration_sec": 1.8,
                "mode": "eco_download_only",
                "server_location": "Cloudflare Montréal (YUL)",
                "trigger": "manual",
                "status": "ok",
            }
        )
        hist_resp = await main_mod.api_speedtest_history(window=86400, limit=100)
        self.assertEqual(len(hist_resp["results"]), 1)
        self.assertEqual(hist_resp["latest_ok"]["download_mbps"], 250.0)

        clear_resp = await main_mod.api_clear_speedtest_history()
        self.assertTrue(clear_resp["cleared"])
        after_resp = await main_mod.api_speedtest_history(window=86400, limit=100)
        self.assertEqual(after_resp["results"], [])


if __name__ == "__main__":
    unittest.main()
