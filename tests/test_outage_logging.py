import asyncio
import os
import socket
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

# Provide lightweight module stubs only when third-party container deps
# (icmplib, fastapi, pydantic) are not installed in the local Python runtime.
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
from app.main import ConnectionManager  # noqa: E402
from app.monitor import MonitorEngine, PingProber  # noqa: E402


class OutageAndReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = database.DB_PATH
        database.DB_PATH = os.path.join(self._tmpdir.name, "test_networkstats.db")
        database.close_thread_connections()

    async def asyncTearDown(self) -> None:
        database.close_thread_connections()
        database.DB_PATH = self._orig_db_path
        self._tmpdir.cleanup()

    async def _noop_broadcast(self, msg):
        return None

    async def test_30s_isp_wan_outage(self) -> None:
        """
        Simulate 5 external targets + local router (10.0.0.1) experiencing a
        30-second ISP/WAN drop where 10.0.0.1 stays alive while all external targets fail.
        """
        database.init_db()
        database.add_target("44.192.142.31", "Fortnite Virginia US", 1.0)
        database.add_target("24.105.62.129", "Overwatch US Central", 1.0)
        targets = database.list_targets()
        self.assertEqual(len(targets), 6)  # 10.0.0.1 + 1.1.1.1, 8.8.8.8, 9.9.9.9, Fortnite, Overwatch

        engine = MonitorEngine(broadcast_cb=self._noop_broadcast)
        for t in targets:
            if t["host"] == "10.0.0.1":
                engine._locked_routes[t["id"]] = ["10.0.0.1"]
            else:
                engine._locked_routes[t["id"]] = ["10.0.0.1", "74.56.22.1", t["host"]]

        outage_active = False

        async def fake_async_ping(ip, **kwargs):
            if outage_active and ip != "10.0.0.1":
                return SimpleNamespace(address=ip, is_alive=False, packets_received=0, avg_rtt=None)
            return SimpleNamespace(address=ip, is_alive=True, packets_received=1, avg_rtt=12.4)

        base_ts = int(time.time()) - 60

        with patch("app.monitor.async_ping", side_effect=fake_async_ping):
            # 5 healthy ticks
            for i in range(5):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

            # 30-second WAN outage
            outage_active = True
            for i in range(5, 35):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

            # 5 recovery ticks
            outage_active = False
            for i in range(35, 40):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

        refreshed = database.list_targets()
        external_targets = [t for t in refreshed if t["host"] != "10.0.0.1"]
        router_target = next(t for t in refreshed if t["host"] == "10.0.0.1")

        self.assertEqual(router_target["lost_count"], 0)
        self.assertEqual(len(external_targets), 5)
        for ext in external_targets:
            self.assertEqual(ext["lost_count"], 30, f"Expected 30 lost packets for {ext['label']}")
            self.assertEqual(ext["loss_pct"], 75.0)
            hist = database.get_target_history(ext["id"], window_seconds=300)
            final_hop_key = str(ext["hop_count"])
            lost_points = [pt for pt in hist["series"][final_hop_key] if pt["loss"] == 100.0]
            self.assertEqual(len(lost_points), 30)

        loss_data = database.get_loss_events(window_seconds=0)
        self.assertEqual(loss_data["summary"]["total_incidents"], 5)
        self.assertEqual(loss_data["summary"]["total_packets_lost"], 150)
        self.assertEqual(loss_data["summary"]["total_outages"], 1)

        for ev in loss_data["events"]:
            self.assertEqual(ev["packets_lost"], 30)
            self.assertEqual(ev["end_ts"] - ev["start_ts"], 29)
            self.assertEqual(ev["severity"], "isp")
            self.assertIn("74.56.22.1", ev["cause"])

        outage = loss_data["outages"][0]
        self.assertGreaterEqual(outage["duration_sec"], 29)
        self.assertEqual(outage["affected_targets"], 5)
        self.assertEqual(outage["total_external_targets"], 5)
        self.assertTrue(outage["router_reachable"])

    async def test_30s_router_lan_outage(self) -> None:
        """When Hop #1 (10.0.0.1) also drops for 30s, outage marks router_reachable=False."""
        database.init_db()
        database.add_target("44.192.142.31", "Fortnite Virginia US", 1.0)
        targets = database.list_targets()

        engine = MonitorEngine(broadcast_cb=self._noop_broadcast)
        for t in targets:
            if t["host"] == "10.0.0.1":
                engine._locked_routes[t["id"]] = ["10.0.0.1"]
            else:
                engine._locked_routes[t["id"]] = ["10.0.0.1", "74.56.22.1", t["host"]]

        all_down = False

        async def fake_async_ping(ip, **kwargs):
            if all_down:
                return SimpleNamespace(address=ip, is_alive=False, packets_received=0, avg_rtt=None)
            return SimpleNamespace(address=ip, is_alive=True, packets_received=1, avg_rtt=4.0)

        base_ts = int(time.time()) - 50
        with patch("app.monitor.async_ping", side_effect=fake_async_ping):
            for i in range(3):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

            all_down = True
            for i in range(3, 33):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

            all_down = False
            for i in range(33, 36):
                engine._prober._cache.clear()
                await asyncio.gather(
                    *(engine._process_target_tick(t, now_ts=base_ts + i) for t in targets)
                )

        loss_data = database.get_loss_events(window_seconds=0)
        self.assertEqual(loss_data["summary"]["total_outages"], 1)
        outage = loss_data["outages"][0]
        self.assertFalse(outage["router_reachable"])
        self.assertGreaterEqual(outage["duration_sec"], 29)
        for ev in loss_data["events"]:
            self.assertEqual(ev["severity"], "router")

    async def test_hung_websocket_does_not_freeze_broadcast(self) -> None:
        """A hung WebSocket client must be evicted within timeout without stalling healthy clients."""
        manager = ConnectionManager()
        received = []

        class HungWS:
            def __init__(self):
                self.closed = False

            async def accept(self):
                pass

            async def send_json(self, data):
                await asyncio.Future()  # hangs forever

            async def close(self):
                self.closed = True

        class HealthyWS:
            async def accept(self):
                pass

            async def send_json(self, data):
                received.append(data)

            async def close(self):
                pass

        hung = HungWS()
        healthy = HealthyWS()
        await manager.connect(hung)
        await manager.connect(healthy)

        start = time.monotonic()
        await asyncio.wait_for(manager.broadcast({"type": "probe_update", "ok": True}), timeout=2.5)
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 2.2)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["ok"], True)
        self.assertNotIn(hung, manager.active_connections)
        self.assertIn(healthy, manager.active_connections)
        self.assertTrue(hung.closed)

    async def test_ping_prober_isolation_and_dedup(self) -> None:
        """
        Verify PingProber deduplicates concurrent requests for the same IP,
        isolates OSError/TimeoutError failures, and logs unresolved hostnames as loss.
        """
        prober = PingProber()
        calls = {}

        async def fake_async_ping(ip, **kwargs):
            calls[ip] = calls.get(ip, 0) + 1
            if ip == "1.1.1.1":
                await asyncio.sleep(0.02)
                return SimpleNamespace(address=ip, is_alive=True, packets_received=1, avg_rtt=5.5)
            if ip == "8.8.8.8":
                raise OSError(101, "Network is unreachable")
            if ip == "9.9.9.9":
                raise asyncio.TimeoutError()
            return SimpleNamespace(address=ip, is_alive=False, packets_received=0, avg_rtt=None)

        with patch("app.monitor.async_ping", side_effect=fake_async_ping):
            res_a, res_b = await asyncio.gather(
                prober.probe_ips(["1.1.1.1", "8.8.8.8"]),
                prober.probe_ips(["1.1.1.1", "9.9.9.9"]),
            )

        self.assertEqual(calls.get("1.1.1.1"), 1)
        self.assertEqual(res_a["1.1.1.1"], (5.5, False))
        self.assertEqual(res_b["1.1.1.1"], (5.5, False))
        self.assertEqual(res_a["8.8.8.8"], (None, True))
        self.assertEqual(res_b["9.9.9.9"], (None, True))

        # Test unresolved domain during DNS outage in _get_locked_route + PingProber
        engine = MonitorEngine(broadcast_cb=self._noop_broadcast)
        with patch("socket.gethostbyname", side_effect=socket.gaierror("DNS down")):
            route = await engine._get_locked_route(99, "ping-va.ds.on.epicgames.com")
            self.assertEqual(route, ["ping-va.ds.on.epicgames.com"])
            self.assertNotIn(99, engine._locked_routes)  # must NOT lock unresolved hostname
            dns_res = await prober.probe_ips(route)
            self.assertEqual(dns_res["ping-va.ds.on.epicgames.com"], (None, True))

    async def test_batched_pruning_preserves_permanent_logs(self) -> None:
        """Pruning deletes old hop_metrics in batches while preserving permanent outage/loss logs."""
        database.init_db()
        target = database.list_targets()[1]  # 1.1.1.1
        t_id = target["id"]
        now = int(time.time())
        old_ts = now - ((database.RETENTION_HOURS + 5) * 3600)

        with database.get_db_connection() as conn:
            old_rows = [
                (t_id, 1, "1.1.1.1", "1.1.1.1", old_ts + (i % 100), 5.0, 0)
                for i in range(12000)
            ]
            recent_rows = [
                (t_id, 1, "1.1.1.1", "1.1.1.1", now - 60 + i, 5.0, 0)
                for i in range(50)
            ]
            conn.executemany(
                """
                INSERT INTO hop_metrics (target_id, hop_num, ip_address, rdns_hostname, timestamp, latency_ms, is_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                old_rows + recent_rows,
            )
            conn.execute(
                """
                INSERT INTO loss_events (target_id, target_label, target_host, start_ts, end_ts, packets_lost, cause, severity)
                VALUES (?, ?, ?, ?, ?, 15, 'Target dropped', 'target')
                """,
                (t_id, target["label"], target["host"], old_ts, old_ts + 14),
            )
            conn.execute(
                """
                INSERT INTO network_outages (start_ts, end_ts, duration_sec, affected_targets, total_external_targets, router_reachable, affected_labels)
                VALUES (?, ?, 15, 3, 4, 1, 'Cloudflare DNS, Google DNS, Quad9 DNS')
                """,
                (old_ts, old_ts + 14),
            )

        deleted = database.prune_old_metrics(batch_size=5000, max_batches=10)
        self.assertEqual(deleted, 12000)

        with database.get_db_connection() as conn:
            remaining_metrics = conn.execute("SELECT COUNT(*) AS cnt FROM hop_metrics").fetchone()["cnt"]
            remaining_events = conn.execute("SELECT COUNT(*) AS cnt FROM loss_events").fetchone()["cnt"]
            remaining_outages = conn.execute("SELECT COUNT(*) AS cnt FROM network_outages").fetchone()["cnt"]

        self.assertEqual(remaining_metrics, 50)
        self.assertEqual(remaining_events, 1)
        self.assertEqual(remaining_outages, 1)

    async def test_init_db_idempotency_and_v2_backfill(self) -> None:
        """
        Verify v1 -> v2 migration cleans legacy 10.x intermediate hops, renumbers hops cleanly,
        backfills recent final-hop losses into loss_events, and never duplicates on re-run.
        """
        now = int(time.time())
        conn = sqlite3.connect(database.DB_PATH)
        conn.executescript(
            """
            CREATE TABLE targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL,
                interval_sec REAL NOT NULL DEFAULT 1.0,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE hop_state (
                target_id INTEGER NOT NULL,
                hop_num INTEGER NOT NULL,
                ip_address TEXT NOT NULL DEFAULT '???',
                rdns_hostname TEXT NOT NULL DEFAULT '???',
                sent INTEGER NOT NULL DEFAULT 0,
                lost INTEGER NOT NULL DEFAULT 0,
                loss_pct REAL NOT NULL DEFAULT 0.0,
                rolling_loss_pct REAL NOT NULL DEFAULT 0.0,
                last_ms REAL,
                avg_ms REAL,
                min_ms REAL,
                max_ms REAL,
                jitter_ms REAL NOT NULL DEFAULT 0.0,
                sparkline_json TEXT NOT NULL DEFAULT '[]',
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (target_id, hop_num)
            );
            CREATE TABLE hop_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER NOT NULL,
                hop_num INTEGER NOT NULL,
                ip_address TEXT NOT NULL,
                rdns_hostname TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                latency_ms REAL,
                is_loss INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        conn.execute(
            "INSERT INTO targets (id, host, label, interval_sec, enabled, created_at) VALUES (1, '1.1.1.1', 'Cloudflare DNS', 1.0, 1, ?)",
            (now - 3600,),
        )
        # Seed legacy hops including hop #3 = 10.170.193.82
        conn.executemany(
            """
            INSERT INTO hop_state (target_id, hop_num, ip_address, rdns_hostname, sent, lost, loss_pct, rolling_loss_pct, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 1, "10.0.0.1", "10.0.0.1", 10, 0, 0.0, 0.0, now),
                (1, 2, "74.56.22.1", "74.56.22.1", 10, 0, 0.0, 0.0, now),
                (1, 3, "10.170.193.82", "10.170.193.82", 10, 10, 100.0, 100.0, now),
                (1, 4, "1.1.1.1", "one.one.one.one", 10, 3, 30.0, 30.0, now),
            ],
        )
        # Seed 3 consecutive final-hop losses + fake 10.170.193.82 losses
        for offset in range(3):
            ts = now - 600 + offset
            conn.execute(
                "INSERT INTO hop_metrics (target_id, hop_num, ip_address, rdns_hostname, timestamp, latency_ms, is_loss) VALUES (1, 3, '10.170.193.82', '10.170.193.82', ?, NULL, 1)",
                (ts,),
            )
            conn.execute(
                "INSERT INTO hop_metrics (target_id, hop_num, ip_address, rdns_hostname, timestamp, latency_ms, is_loss) VALUES (1, 4, '1.1.1.1', 'one.one.one.one', ?, NULL, 1)",
                (ts,),
            )
        conn.commit()
        conn.close()

        database.init_db()
        hops = database.get_hop_states(1)
        self.assertEqual([h["hop_num"] for h in hops], [1, 2, 3])
        self.assertEqual([h["ip_address"] for h in hops], ["10.0.0.1", "74.56.22.1", "1.1.1.1"])

        loss_data = database.get_loss_events(window_seconds=0)
        self.assertEqual(loss_data["summary"]["total_incidents"], 1)
        self.assertEqual(loss_data["summary"]["total_packets_lost"], 3)

        # Second init_db() call must be a no-op (zero duplicates)
        database.init_db()
        loss_data_2 = database.get_loss_events(window_seconds=0)
        self.assertEqual(loss_data_2["summary"]["total_incidents"], 1)
        self.assertEqual(loss_data_2["summary"]["total_packets_lost"], 3)

    async def test_history_bucketing_preserves_peak_spikes_and_loss_counts(self) -> None:
        """
        Verify 1h (10s buckets) and 6h (60s buckets) preserve exact peak spike max_ms
        and exact lost packet counts instead of averaging spikes away.
        """
        database.init_db()
        target = database.list_targets()[1]  # 1.1.1.1
        t_id = target["id"]
        start_ts = ((int(time.time()) - 120) // 60) * 60

        with database.get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO hop_state (target_id, hop_num, ip_address, rdns_hostname, sent, lost, loss_pct, rolling_loss_pct, updated_at)
                VALUES (?, 1, '1.1.1.1', 'one.one.one.one', 60, 3, 5.0, 5.0, ?)
                """,
                (t_id, start_ts + 59),
            )
            rows = []
            for i in range(60):
                ts = start_ts + i
                if i in (10, 11, 12):
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, None, 1))
                elif i == 25:
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, 95.0, 0))
                else:
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, 20.0, 0))
            conn.executemany(
                """
                INSERT INTO hop_metrics (target_id, hop_num, ip_address, rdns_hostname, timestamp, latency_ms, is_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        hist_1h = database.get_target_history(t_id, window_seconds=3600)
        self.assertEqual(hist_1h["bucket_sec"], 10)
        self.assertIn("server_now", hist_1h)
        series_1h = hist_1h["series"]["1"]
        self.assertEqual(max(pt["max_ms"] for pt in series_1h if pt["max_ms"] is not None), 95.0)
        self.assertEqual(sum(pt["lost_pkts"] for pt in series_1h), 3)

        hist_6h = database.get_target_history(t_id, window_seconds=21600)
        self.assertEqual(hist_6h["bucket_sec"], 60)
        self.assertIn("server_now", hist_6h)
        series_6h = hist_6h["series"]["1"]
        self.assertEqual(len(series_6h), 1)
        self.assertEqual(series_6h[0]["max_ms"], 95.0)
        self.assertEqual(series_6h[0]["lost_pkts"], 3)
        self.assertEqual(series_6h[0]["total_pkts"], 60)


if __name__ == "__main__":
    unittest.main()
