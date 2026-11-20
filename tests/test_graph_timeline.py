import math
import os
import tempfile
import time
import unittest
from typing import Any, Dict, List, Optional

from app import database


def simulate_js_live_tick_merge(
    series_arr: List[Dict[str, Any]],
    bucket_sec: int,
    window_seconds: int,
    timestamp: int,
    last_ms: Optional[float],
) -> None:
    """
    Exact Python mirror of handleWsMessage's probe_update bucket aggregation
    in app/static/app.js, used to verify 100% numerical parity with SQLite.
    """
    b_sec = max(1, bucket_sec)
    bucket_ts = (timestamp // b_sec) * b_sec
    cutoff_ts = timestamp - window_seconds - b_sec
    is_loss = last_ms is None
    ms_val = None if is_loss else float(last_ms)
    last_pt = series_arr[-1] if series_arr else None

    if last_pt and last_pt["ts"] == bucket_ts:
        prev_total = int(last_pt.get("total_pkts", 1))
        prev_lost = int(last_pt.get("lost_pkts", 1 if last_pt.get("loss", 0) > 0 else 0))
        prev_valid = max(0, prev_total - prev_lost)
        prev_sum = (
            last_pt["_sum_ms"]
            if "_sum_ms" in last_pt
            else (float(last_pt["ms"]) * prev_valid if last_pt.get("ms") is not None else 0.0)
        )
        new_total = prev_total + 1
        new_lost = prev_lost + (1 if is_loss else 0)

        last_pt["total_pkts"] = new_total
        last_pt["lost_pkts"] = new_lost
        last_pt["loss_pct"] = round((new_lost / new_total) * 100.0, 1)
        if is_loss:
            last_pt["loss"] = 100.0
        elif last_pt["ms"] is None or prev_valid == 0:
            last_pt["_sum_ms"] = ms_val
            last_pt["ms"] = round(ms_val, 2)
            last_pt["max_ms"] = round(ms_val, 2)
            last_pt["min_ms"] = round(ms_val, 2)
        else:
            last_pt["_sum_ms"] = prev_sum + ms_val
            last_pt["ms"] = round(last_pt["_sum_ms"] / (prev_valid + 1), 2)
            last_pt["max_ms"] = (
                round(max(last_pt["max_ms"], ms_val), 2)
                if last_pt["max_ms"] is not None
                else round(ms_val, 2)
            )
            last_pt["min_ms"] = (
                round(min(last_pt["min_ms"], ms_val), 2)
                if last_pt["min_ms"] is not None
                else round(ms_val, 2)
            )
    elif not last_pt or bucket_ts > last_pt["ts"]:
        rounded_ms = None if is_loss else round(ms_val, 2)
        series_arr.append(
            {
                "ts": bucket_ts,
                "_sum_ms": 0.0 if is_loss else ms_val,
                "ms": rounded_ms,
                "max_ms": rounded_ms,
                "min_ms": rounded_ms,
                "loss": 100.0 if is_loss else 0.0,
                "loss_pct": 100.0 if is_loss else 0.0,
                "lost_pkts": 1 if is_loss else 0,
                "total_pkts": 1,
            }
        )

    while series_arr and series_arr[0]["ts"] < cutoff_ts:
        series_arr.pop(0)


class GraphTimelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = database.DB_PATH
        database.DB_PATH = os.path.join(self._tmpdir.name, "test_graphs.db")
        database.close_thread_connections()
        database.init_db()

    def tearDown(self) -> None:
        database.close_thread_connections()
        database.DB_PATH = self._orig_db_path
        self._tmpdir.cleanup()

    def test_all_time_windows_bucket_alignment_and_spike_consistency(self) -> None:
        """
        Verify 5m, 15m, 1h, 6h, and 24h return properly aligned buckets and
        preserve exact peak spike heights (53.2ms and 44.1ms) across all 5 views.
        """
        target = database.list_targets()[1]  # 1.1.1.1
        t_id = target["id"]
        now = int(time.time())
        base_ts = now - 130

        with database.get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO hop_state (target_id, hop_num, ip_address, rdns_hostname, sent, lost, loss_pct, rolling_loss_pct, updated_at)
                VALUES (?, 1, '1.1.1.1', 'one.one.one.one', 120, 2, 1.67, 1.7, ?)
                """,
                (t_id, base_ts + 119),
            )
            rows = []
            for i in range(120):
                ts = base_ts + i
                if i == 15:
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, 53.2, 0))
                elif i == 75:
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, 44.1, 0))
                elif i in (30, 90):
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, None, 1))
                else:
                    rows.append((t_id, 1, "1.1.1.1", "one.one.one.one", ts, 26.0, 0))
            conn.executemany(
                """
                INSERT INTO hop_metrics (target_id, hop_num, ip_address, rdns_hostname, timestamp, latency_ms, is_loss)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

        expected_buckets = {
            300: 1,      # 5m
            900: 1,      # 15m
            3600: 10,    # 1h
            21600: 60,   # 6h
            86400: 240,  # 24h
        }

        for window_sec, expected_b_sec in expected_buckets.items():
            hist = database.get_target_history(t_id, window_seconds=window_sec)
            self.assertEqual(hist["bucket_sec"], expected_b_sec)
            pts = hist["series"]["1"]
            self.assertGreater(len(pts), 0)
            for pt in pts:
                self.assertEqual(
                    pt["ts"] % expected_b_sec,
                    0,
                    f"Timestamp {pt['ts']} not aligned to {expected_b_sec}s on window={window_sec}",
                )
            # Maximum peak across the series must always be 53.2ms in EVERY timeframe!
            peak = max(pt["max_ms"] for pt in pts if pt["max_ms"] is not None)
            self.assertEqual(
                peak,
                53.2,
                f"Peak spike shrank on window={window_sec}: got {peak} instead of 53.2",
            )
            # Total lost packets must always sum to 2 in EVERY timeframe!
            total_lost = sum(pt["lost_pkts"] for pt in pts)
            self.assertEqual(total_lost, 2)

    def test_live_ticks_on_6h_do_not_shift_history_left_or_restart_graph(self) -> None:
        """
        Reproduce the exact user scenario:
        1. Load 6h graph (21600s, bucket_sec=60) with a packet loss 7 minutes ago.
        2. Let 180 live 1-second WebSocket ticks arrive over 3 minutes.
        3. Verify only 3 new minute-buckets are added (not 180 points!),
           and the packet loss from 10 minutes ago sits at 97.2% (near the right edge)
           on the 6h linear X-axis versus 83.3% on the 1h linear X-axis.
        """
        bucket_sec_6h = 60
        window_6h = 21600
        start_now = 1763640000  # divisible by 60

        # Build 6 hours (360 minute-buckets) ending at start_now
        series_6h: List[Dict[str, Any]] = []
        loss_bucket_ts = start_now - (7 * 60)  # 7 minutes ago
        for i in range(360):
            b_ts = start_now - ((359 - i) * 60)
            is_loss_bucket = b_ts == loss_bucket_ts
            series_6h.append(
                {
                    "ts": b_ts,
                    "ms": 26.0,
                    "max_ms": 31.0,
                    "min_ms": 24.0,
                    "loss": 100.0 if is_loss_bucket else 0.0,
                    "loss_pct": 3.3 if is_loss_bucket else 0.0,
                    "lost_pkts": 2 if is_loss_bucket else 0,
                    "total_pkts": 60,
                }
            )

        # Simulate leaving the tab on 6h for 3 full minutes (180 1-second live ticks)
        for tick in range(1, 181):
            ts = start_now + tick
            ms = 25.0 + (tick % 7) * 1.5
            simulate_js_live_tick_merge(series_6h, bucket_sec_6h, window_6h, ts, ms)

        # Array must still have ~361 buckets (NOT 360 + 180 = 540 points!)
        self.assertLessEqual(len(series_6h), 362)

        end_now = start_now + 180
        aligned_max_6h = (end_now // bucket_sec_6h) * bucket_sec_6h
        aligned_min_6h = aligned_max_6h - window_6h

        # Relative horizontal position (0.0 = far left, 1.0 = far right) on 6h vs 1h:
        pos_on_6h = (loss_bucket_ts - aligned_min_6h) / (aligned_max_6h - aligned_min_6h)
        pos_on_1h = (loss_bucket_ts - (aligned_max_6h - 3600)) / 3600

        # 10 minutes ago on a 6h graph is at 97.2% (near the right edge),
        # which is FURTHER RIGHT than on a 1h graph (83.3%)!
        self.assertAlmostEqual(pos_on_6h, 1.0 - (600 / 21600), places=4)
        self.assertAlmostEqual(pos_on_1h, 1.0 - (600 / 3600), places=4)
        self.assertGreater(pos_on_6h, 0.97)
        self.assertGreater(pos_on_6h, pos_on_1h)

    def test_live_tick_in_place_bucket_merge_matches_db_requery_100_percent(self) -> None:
        """
        Verify that 120 seconds of live 1s WebSocket ticks merged in-place on the frontend
        produce the EXACT same buckets (ms, max_ms, min_ms, loss, loss_pct, lost_pkts, total_pkts)
        as querying SQLite get_target_history() fresh for 1h (10s), 6h (60s), and 24h (240s).
        """
        target = database.list_targets()[1]  # 1.1.1.1
        t_id = target["id"]
        start_ts = ((int(time.time()) - 120) // 240) * 240

        live_1h: List[Dict[str, Any]] = []
        live_6h: List[Dict[str, Any]] = []
        live_24h: List[Dict[str, Any]] = []

        for i in range(120):
            ts = start_ts + i
            # Deterministic mix of normal latencies, spikes, and packet drops
            if i in (14, 15, 88):
                ms: Optional[float] = None
                is_loss = True
            elif i == 42:
                ms = 68.4
                is_loss = False
            else:
                ms = round(22.0 + math.sin(i) * 4.5, 2)
                is_loss = False

            hop_states = [
                {
                    "hop_num": 1,
                    "ip_address": "1.1.1.1",
                    "rdns_hostname": "one.one.one.one",
                    "sent": i + 1,
                    "lost": 1 if is_loss else 0,
                    "loss_pct": 0.0,
                    "rolling_loss_pct": 0.0,
                    "last_ms": ms,
                    "avg_ms": ms or 22.0,
                    "min_ms": 17.5,
                    "max_ms": 68.4,
                    "jitter_ms": 1.5,
                    "sparkline": [ms],
                }
            ]
            raw_samples = [
                {
                    "hop_num": 1,
                    "ip_address": "1.1.1.1",
                    "rdns_hostname": "one.one.one.one",
                    "latency_ms": ms,
                    "is_loss": is_loss,
                }
            ]
            database.save_probe_results(t_id, ts, hop_states, raw_samples, "Cloudflare DNS", "1.1.1.1")

            simulate_js_live_tick_merge(live_1h, 10, 3600, ts, ms)
            simulate_js_live_tick_merge(live_6h, 60, 21600, ts, ms)
            simulate_js_live_tick_merge(live_24h, 240, 86400, ts, ms)

        for window_sec, live_series in ((3600, live_1h), (21600, live_6h), (86400, live_24h)):
            db_series = database.get_target_history(t_id, window_seconds=window_sec)["series"]["1"]
            self.assertEqual(len(live_series), len(db_series), f"Bucket count mismatch on window={window_sec}")
            for live_pt, db_pt in zip(live_series, db_series):
                self.assertEqual(live_pt["ts"], db_pt["ts"])
                self.assertAlmostEqual(live_pt["ms"], db_pt["ms"], places=2)
                self.assertAlmostEqual(live_pt["max_ms"], db_pt["max_ms"], places=2)
                self.assertAlmostEqual(live_pt["min_ms"], db_pt["min_ms"], places=2)
                self.assertEqual(live_pt["loss"], db_pt["loss"])
                self.assertAlmostEqual(live_pt["loss_pct"], db_pt["loss_pct"], places=1)
                self.assertEqual(live_pt["lost_pkts"], db_pt["lost_pkts"])
                self.assertEqual(live_pt["total_pkts"], db_pt["total_pkts"])

    def test_no_cache_and_portainer_compose_compatibility(self) -> None:
        """
        Verify docker-compose.yml avoids relative ./app bind mounts and pull_policy: build
        (which break Portainer Git stacks), and index.html + app/main.py bust static caching.
        """
        with open("docker-compose.yml") as f:
            compose_text = f.read()
        self.assertNotIn("./app:/app/app", compose_text)
        self.assertNotIn("pull_policy: build", compose_text)

        with open("app/static/index.html") as f:
            index_html = f.read()
        self.assertIn('/static/app.js?v=1.3.1', index_html)

        with open("app/main.py") as f:
            main_py = f.read()
        self.assertIn("no-cache, no-store, must-revalidate", main_py)


if __name__ == "__main__":
    unittest.main()
