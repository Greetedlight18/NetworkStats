import asyncio
import json
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
    def __init__(
        self,
        clock: _FakeClock,
        headers: list,
        status: int = 200,
        body_bytes: bytes = b"",
        stream_size: int = 0,
        bps: float = 1e9,
    ) -> None:
        self._clock = clock
        self._headers = list(headers)
        self.status = status
        self._body = body_bytes
        self._offset = 0
        self._remaining = stream_size if stream_size > 0 else len(body_bytes)
        self._bps = float(bps)

    def getheaders(self):
        return list(self._headers)

    def read(self, amt: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if amt is None or amt < 0:
            to_read = self._remaining
        else:
            to_read = min(self._remaining, amt)
        self._remaining -= to_read
        self._clock.advance(to_read / self._bps)
        if to_read >= 65536:
            # Yield GIL slightly so parallel worker threads interleave like real sockets
            time.sleep(0.0001)
        if self._body:
            out = self._body[self._offset : self._offset + to_read]
            self._offset += to_read
            return out
        return b"\x00" * to_read


class _RecordingScenario:
    def __init__(
        self,
        clock: _FakeClock,
        dl_bps: float = 112_500_000.0,  # 900 Mbps aggregate
        ul_bps: float = 6_250_000.0,    # 50 Mbps aggregate
        rtt_sec: float = 0.015,         # 15 ms network RTT
        cf_server_ms: float = 150.0,    # Extra Cloudflare Worker processing latency
        fastcom_fail: bool = False,
        cf_fail: bool = False,
    ) -> None:
        self.clock = clock
        self.dl_bps = dl_bps
        self.ul_bps = ul_bps
        self.rtt_sec = rtt_sec
        self.cf_server_ms = cf_server_ms
        self.fastcom_fail = fastcom_fail
        self.cf_fail = cf_fail
        self.requests = []
        self.conns_by_host = {}
        self.lock = threading.Lock()

    def make_connection(self, host: str = st_mod.CLOUDFLARE_HOST, timeout: float = 8.0):
        with self.lock:
            self.conns_by_host[host] = self.conns_by_host.get(host, 0) + 1
        return _FakeHTTPSConnection(self, host)


class _FakeHTTPSConnection:
    def __init__(self, scenario: _RecordingScenario, host: str) -> None:
        self._s = scenario
        self._host = host
        self._pending_resp = None

    def request(self, method: str, path: str, headers=None) -> None:
        hdrs_dict = dict(headers or {})
        with self._s.lock:
            self._s.requests.append((self._host, method, path, hdrs_dict))

        if self._host == st_mod.FASTCOM_HOST:
            if self._s.fastcom_fail:
                raise OSError("Fast.com unreachable")
            if path.startswith("/app-"):
                js = b'var x={token:"fastcom_mock_token_123456"};'
                self._pending_resp = _FakeResponse(self._s.clock, [], body_bytes=js)
            else:
                html = b'<html><script src="/app-mock01.js"></script></html>'
                self._pending_resp = _FakeResponse(self._s.clock, [], body_bytes=html)
            return

        if self._host == st_mod.FASTCOM_API_HOST:
            if self._s.fastcom_fail:
                self._pending_resp = _FakeResponse(self._s.clock, [], status=503)
                return
            payload = {
                "client": {"ip": "74.56.22.210", "asn": "5769", "isp": "Videotron"},
                "targets": [
                    {
                        "location": {"city": "MONTRÉAL", "country": "CA"},
                        "url": f"https://c01.oca.nflxvideo.net/speedtest?c=ca&n=5769",
                    },
                    {
                        "location": {"city": "MONTRÉAL", "country": "CA"},
                        "url": f"https://c02.oca.nflxvideo.net/speedtest?c=ca&n=5769",
                    },
                    {
                        "location": {"city": "Saint-Hubert", "country": "CA"},
                        "url": f"https://c03.oca.nflxvideo.net/speedtest?c=ca&n=5769",
                    },
                ],
            }
            # Mirror real Fast.com API Latin-1 city encoding
            raw = json.dumps(payload, ensure_ascii=False).encode("latin-1")
            self._pending_resp = _FakeResponse(self._s.clock, [], body_bytes=raw)
            return

        if self._host.endswith(".oca.nflxvideo.net"):
            if self._s.fastcom_fail:
                self._pending_resp = _FakeResponse(self._s.clock, [], status=403)
                return
            self._s.clock.advance(self._s.rtt_sec)
            if "/speedtest/range/0-0" in path:
                self._pending_resp = _FakeResponse(
                    self._s.clock, [], body_bytes=b"\x00", bps=self._s.dl_bps
                )
            else:
                parts = path.split("/speedtest/range/")[1].split("?")[0].split("-")
                nbytes = int(parts[1]) - int(parts[0])
                self._pending_resp = _FakeResponse(
                    self._s.clock, [], stream_size=nbytes, bps=self._s.dl_bps
                )
            return

        # Cloudflare host
        if self._s.cf_fail:
            self._pending_resp = _FakeResponse(self._s.clock, [], status=403)
            return
        self._s.clock.advance(self._s.rtt_sec + (self._s.cf_server_ms / 1000.0))
        nbytes = 0
        if "bytes=" in path:
            nbytes = int(path.split("bytes=")[1].split("&")[0])
        resp_hdrs = [
            ("cf-meta-colo", "YUL"),
            ("city", "Longueuil"),
            ("Server-Timing", f"cfSpeedEdge;dur=3, cfSpeedWorker;dur={self._s.cf_server_ms - 3}"),
        ]
        self._pending_resp = _FakeResponse(
            self._s.clock, resp_hdrs, stream_size=nbytes, bps=self._s.dl_bps
        )

    def putrequest(self, method: str, path: str) -> None:
        self._cur_upload_method = method
        self._cur_upload_path = path
        self._cur_upload_hdrs = {}

    def putheader(self, name: str, value: str) -> None:
        self._cur_upload_hdrs[name] = value

    def endheaders(self) -> None:
        with self._s.lock:
            self._s.requests.append(
                (self._host, self._cur_upload_method, self._cur_upload_path, dict(self._cur_upload_hdrs))
            )

    def send(self, data: bytes) -> None:
        self._s.clock.advance(len(data) / self._s.ul_bps)

    def getresponse(self) -> _FakeResponse:
        if self._pending_resp is not None:
            resp = self._pending_resp
            self._pending_resp = None
            return resp
        if self._host == st_mod.CLOUDFLARE_HOST and self._s.cf_fail:
            return _FakeResponse(self._s.clock, [], status=403)
        self._s.clock.advance(self._s.rtt_sec)
        return _FakeResponse(self._s.clock, [("cf-meta-colo", "YUL")], body_bytes=b"")

    def close(self) -> None:
        pass


class SpeedtestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = database.DB_PATH
        database.DB_PATH = os.path.join(self._tmpdir.name, "test_speedtest.db")
        database.close_thread_connections()
        database.init_db()
        with st_mod._FASTCOM_TOKEN_LOCK:
            st_mod._FASTCOM_TOKEN_CACHE["token"] = None
            st_mod._FASTCOM_TOKEN_CACHE["expires_at"] = 0.0

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
        self.assertEqual(database.get_speedtest_settings(), updated)

        with self.assertRaises(ValueError):
            database.update_speedtest_settings(interval_min=4)
        with self.assertRaises(ValueError):
            database.update_speedtest_settings(interval_min=2000)
        with self.assertRaises(ValueError):
            database.update_speedtest_settings(mode="gigabit_spam")

    def test_speedtest_history_summary_window_clear_and_prune(self) -> None:
        now = int(time.time())
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
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )
        database.save_speedtest_result(
            {
                "timestamp": now - 1200,
                "download_mbps": 800.0,
                "upload_mbps": 40.0,
                "ping_ms": 14.0,
                "jitter_ms": 1.2,
                "bytes_used": 120_000_000,
                "duration_sec": 2.4,
                "mode": "eco",
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )
        database.save_speedtest_result(
            {
                "timestamp": now - 600,
                "download_mbps": 900.0,
                "upload_mbps": 50.0,
                "ping_ms": 12.0,
                "jitter_ms": 0.8,
                "bytes_used": 150_000_000,
                "duration_sec": 2.9,
                "mode": "balanced",
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": "manual",
                "status": "ok",
            }
        )
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
        self.assertEqual(summary["avg_download_mbps"], 850.0)
        self.assertEqual(summary["min_download_mbps"], 800.0)
        self.assertEqual(summary["max_download_mbps"], 900.0)
        self.assertEqual(summary["avg_upload_mbps"], 45.0)
        self.assertEqual(summary["avg_ping_ms"], 13.0)
        self.assertEqual(summary["total_bytes_used"], 270_500_000)

        self.assertEqual(hist_24h["latest"]["status"], "error")
        self.assertEqual(hist_24h["latest_ok"]["download_mbps"], 900.0)

        database.prune_old_metrics()
        self.assertEqual(len(database.get_speedtest_history(window_seconds=0)["results"]), 3)

        self.assertEqual(database.clear_speedtest_results(), 3)
        self.assertIsNone(database.get_latest_speedtest_ts())

    def test_compute_steady_mbps_trims_warmup_and_outlier_dips(self) -> None:
        # 200ms buckets across 2.0s: 22.5 MB/bucket == 900 Mbps
        bucket_rates_mbps = [80.0, 220.0, 900.0, 900.0, 900.0, 140.0, 900.0, 900.0, 900.0, 1080.0]
        events = [(0.0, 0)]
        for i, mbps in enumerate(bucket_rates_mbps):
            chunk_bytes = int((mbps * 1_000_000.0 / 8.0) * 0.2)
            events.append(((i * 0.2) + 0.05, chunk_bytes // 2))
            events.append(((i * 0.2) + 0.15, chunk_bytes - (chunk_bytes // 2)))
        # Incomplete trailing bucket (0.04s after t=2.0 -> ignored)
        events.append((2.04, 100_000))

        steady = st_mod._compute_steady_mbps(events, fallback_sec=2.04)
        self.assertGreaterEqual(steady, 860.0)
        self.assertLessEqual(steady, 940.0)

    def test_parse_cf_server_timing_ms(self) -> None:
        hdrs = [
            ("Server-Timing", "cfSpeedEdge;dur=3, cfSpeedWorker;dur=206"),
            ("server-timing", 'cfL4;desc="?proto=TCP&rtt=17360&min_rtt=16402"'),
        ]
        server_ms, l4_ms = st_mod._parse_cf_server_timing_ms(hdrs)
        self.assertAlmostEqual(server_ms, 209.0, places=2)
        self.assertAlmostEqual(l4_ms, 17.36, places=2)

    def test_fastcom_eco_gigabit_and_location_latin1(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=112_500_000.0, ul_bps=6_250_000.0, rtt_sec=0.015)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_fastcom_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["server_location"], "Fast.com Montréal (Videotron)")
        self.assertAlmostEqual(res["ping_ms"], 15.0, delta=1.0)
        self.assertAlmostEqual(res["download_mbps"], 900.0, delta=60.0)
        self.assertAlmostEqual(res["upload_mbps"], 50.0, delta=5.0)
        # Verify 2 download streams targeted Fast.com Open Connect hosts
        dl_hosts = {
            h for (h, m, p, _) in scenario.requests if h.endswith(".oca.nflxvideo.net") and "/range/0-26214400" in p
        }
        self.assertEqual(len(dl_hosts), 2)

    def test_fastcom_eco_download_only_skips_upload(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=100_000_000.0, ul_bps=5_000_000.0, rtt_sec=0.012)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_fastcom_speedtest(
                mode="eco_download_only", trigger="manual", cooldown_sec=0.0
            )

        self.assertEqual(res["status"], "ok")
        self.assertIsNone(res["upload_mbps"])
        self.assertGreater(res["download_mbps"], 700.0)
        self.assertEqual([r for r in scenario.requests if r[1] == "POST"], [])

    def test_fastcom_balanced_uses_four_dl_and_two_ul_streams(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, dl_bps=125_000_000.0, ul_bps=12_500_000.0, rtt_sec=0.01)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_fastcom_speedtest(mode="balanced", trigger="manual", cooldown_sec=0.0)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["mode"], "balanced")
        self.assertGreater(res["download_mbps"], 800.0)
        self.assertGreater(res["upload_mbps"], 80.0)
        cdn_conns = sum(v for k, v in scenario.conns_by_host.items() if k.endswith(".oca.nflxvideo.net"))
        # 1 ping + 4 dl + 1 ul cal + 2 parallel ul = 8
        self.assertGreaterEqual(cdn_conns, 8)

    def test_cloudflare_fallback_headers_and_server_timing_rtt(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(
            clock, dl_bps=100_000_000.0, ul_bps=5_000_000.0, rtt_sec=0.016, cf_server_ms=150.0
        )

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_cloudflare_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["server_location"], "Cloudflare Longueuil (YUL)")
        # 150ms Cloudflare Worker processing subtracted -> ping stays near true 16ms RTT
        self.assertLess(res["ping_ms"], 30.0)
        self.assertGreater(res["download_mbps"], 700.0)
        self.assertAlmostEqual(res["upload_mbps"], 40.0, delta=5.0)
        cf_reqs = [r for r in scenario.requests if r[0] == st_mod.CLOUDFLARE_HOST]
        self.assertTrue(any("measId=" in p for (_, _, p, _) in cf_reqs))
        self.assertTrue(all(h.get("Referer") == "https://speed.cloudflare.com/" for (_, _, _, h) in cf_reqs))

    def test_run_speedtest_falls_back_to_cloudflare_and_handles_both_failing(self) -> None:
        clock = _FakeClock()
        scenario = _RecordingScenario(clock, fastcom_fail=True, cf_fail=False)

        with patch.object(st_mod, "_make_connection", side_effect=scenario.make_connection), patch.object(
            st_mod, "time", clock
        ):
            res = st_mod.run_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(res["status"], "ok")
        self.assertIn("Cloudflare", res["server_location"])

        both_fail = _RecordingScenario(clock, fastcom_fail=True, cf_fail=True)
        with patch.object(st_mod, "_make_connection", side_effect=both_fail.make_connection), patch.object(
            st_mod, "time", clock
        ):
            err_res = st_mod.run_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0)

        self.assertEqual(err_res["status"], "error")
        self.assertIn("Fast.com:", err_res["error_message"])
        self.assertIn("Cloudflare:", err_res["error_message"])

    async def test_scheduler_concurrent_manual_guard_and_ws_events(self) -> None:
        events = []

        async def _collect_ws(msg):
            events.append(msg)

        gate = threading.Event()

        def _blocking_speedtest(mode="eco", trigger="scheduled", cooldown_sec=0.0):
            gate.wait(timeout=2.0)
            return {
                "timestamp": int(time.time()),
                "download_mbps": 895.4,
                "upload_mbps": 51.2,
                "ping_ms": 13.8,
                "jitter_ms": 1.1,
                "bytes_used": 95_000_000,
                "duration_sec": 2.1,
                "mode": mode,
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": trigger,
                "status": "ok",
                "error_message": None,
            }

        scheduler = st_mod.SpeedtestScheduler(broadcast_cb=_collect_ws, startup_grace_sec=3600)
        with patch.object(st_mod, "run_speedtest", side_effect=_blocking_speedtest):
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
        self.assertEqual(res_evt["result"]["download_mbps"], 895.4)
        self.assertFalse(res_evt["status"]["is_running"])

        hist = database.get_speedtest_history(window_seconds=0)
        self.assertEqual(len(hist["results"]), 1)
        self.assertEqual(hist["latest_ok"]["trigger"], "manual")

    async def test_scheduler_respects_recent_run_and_updates_on_settings_change(self) -> None:
        recent_ts = int(time.time()) - 60
        database.save_speedtest_result(
            {
                "timestamp": recent_ts,
                "download_mbps": 880.0,
                "upload_mbps": 49.0,
                "ping_ms": 14.0,
                "jitter_ms": 1.0,
                "bytes_used": 90_000_000,
                "duration_sec": 2.0,
                "mode": "eco",
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": "scheduled",
                "status": "ok",
            }
        )

        called = []

        def _unexpected_run(*args, **kwargs):
            called.append(True)
            raise AssertionError("Should not run on boot when recent run is inside interval")

        scheduler = st_mod.SpeedtestScheduler(startup_grace_sec=0)
        with patch.object(st_mod, "run_speedtest", side_effect=_unexpected_run):
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
                "download_mbps": 910.0,
                "upload_mbps": None,
                "ping_ms": 14.0,
                "jitter_ms": 1.0,
                "bytes_used": 85_000_000,
                "duration_sec": 1.8,
                "mode": "eco_download_only",
                "server_location": "Fast.com Montréal (Videotron)",
                "trigger": "manual",
                "status": "ok",
            }
        )
        hist_resp = await main_mod.api_speedtest_history(window=86400, limit=100)
        self.assertEqual(len(hist_resp["results"]), 1)
        self.assertEqual(hist_resp["latest_ok"]["download_mbps"], 910.0)

        clear_resp = await main_mod.api_clear_speedtest_history()
        self.assertTrue(clear_resp["cleared"])
        after_resp = await main_mod.api_speedtest_history(window=86400, limit=100)
        self.assertEqual(after_resp["results"], [])


if __name__ == "__main__":
    unittest.main()
