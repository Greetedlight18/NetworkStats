import asyncio
import http.client
import logging
import os
import ssl
import statistics
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from app import database

logger = logging.getLogger("NetworkStats.speedtest")

CLOUDFLARE_HOST = "speed.cloudflare.com"
USER_AGENT = "NetworkStats/1.4"
UPLOAD_CHUNK = os.urandom(65536)
DEFAULT_COOLDOWN_SEC = 0.5


@dataclass(frozen=True)
class SpeedtestModeConfig:
    id: str
    label: str
    description: str
    streams: int
    dl_target_sec: float
    dl_min_bytes: int
    dl_max_bytes: int
    dl_hard_timeout: float
    ul_target_sec: float
    ul_min_bytes: int
    ul_max_bytes: int
    ul_hard_timeout: float
    run_upload: bool


SPEEDTEST_MODES: Dict[str, SpeedtestModeConfig] = {
    "eco": SpeedtestModeConfig(
        id="eco",
        label="Eco (Low Impact)",
        description="Single polite stream, <=25 MB down / <=10 MB up (~1.5s burst)",
        streams=1,
        dl_target_sec=1.5,
        dl_min_bytes=2_000_000,
        dl_max_bytes=25_000_000,
        dl_hard_timeout=4.0,
        ul_target_sec=1.2,
        ul_min_bytes=500_000,
        ul_max_bytes=10_000_000,
        ul_hard_timeout=4.0,
        run_upload=True,
    ),
    "eco_download_only": SpeedtestModeConfig(
        id="eco_download_only",
        label="Eco (Download Only)",
        description="Single stream download only (<=25 MB), zero upload impact",
        streams=1,
        dl_target_sec=1.5,
        dl_min_bytes=2_000_000,
        dl_max_bytes=25_000_000,
        dl_hard_timeout=4.0,
        ul_target_sec=0.0,
        ul_min_bytes=0,
        ul_max_bytes=0,
        ul_hard_timeout=0.0,
        run_upload=False,
    ),
    "balanced": SpeedtestModeConfig(
        id="balanced",
        label="Balanced (Gigabit)",
        description="2 streams, <=60 MB down / <=25 MB up (~2.5s burst)",
        streams=2,
        dl_target_sec=2.5,
        dl_min_bytes=4_000_000,
        dl_max_bytes=60_000_000,
        dl_hard_timeout=6.0,
        ul_target_sec=2.0,
        ul_min_bytes=1_000_000,
        ul_max_bytes=25_000_000,
        ul_hard_timeout=6.0,
        run_upload=True,
    ),
}


def list_speedtest_modes() -> List[Dict[str, Any]]:
    return [
        {
            "id": m.id,
            "label": m.label,
            "description": m.description,
            "streams": m.streams,
            "run_upload": m.run_upload,
        }
        for m in SPEEDTEST_MODES.values()
    ]


def _make_connection(timeout: float = 8.0) -> http.client.HTTPSConnection:
    ctx = ssl.create_default_context()
    return http.client.HTTPSConnection(CLOUDFLARE_HOST, timeout=timeout, context=ctx)


def _get_header(headers: Any, name: str) -> Optional[str]:
    if headers is None:
        return None
    if hasattr(headers, "get"):
        val = headers.get(name)
        if val is not None:
            return str(val)
    target = name.lower()
    if hasattr(headers, "items"):
        for k, v in headers.items():
            if str(k).lower() == target:
                return str(v)
    elif isinstance(headers, list):
        for k, v in headers:
            if str(k).lower() == target:
                return str(v)
    return None


def _parse_server_location(headers: Any) -> str:
    colo = (_get_header(headers, "cf-meta-colo") or _get_header(headers, "colo") or "").strip()
    city_raw = (_get_header(headers, "city") or _get_header(headers, "cf-meta-city") or "").strip()
    city = urllib.parse.unquote(city_raw) if city_raw else ""

    if city and colo:
        return f"Cloudflare {city} ({colo})"
    if colo:
        return f"Cloudflare ({colo})"
    if city:
        return f"Cloudflare {city}"
    return "Cloudflare Edge"


def _compute_trimmed_mbps(
    samples: List[Tuple[float, int]],
    fallback_bytes: int,
    fallback_sec: float,
) -> float:
    """
    Calculate throughput in Mbps while discarding initial 15% TCP slow-start warmup
    whenever enough samples and elapsed span exist.
    """
    if len(samples) >= 6:
        t_start = samples[0][0]
        t_end = samples[-1][0]
        span = t_end - t_start
        if span >= 0.4:
            trim_t = t_start + (0.15 * span)
            start_idx = None
            for idx, (ts, _b) in enumerate(samples):
                if ts >= trim_t:
                    start_idx = idx
                    break
            if start_idx is not None and start_idx < len(samples) - 1:
                delta_t = samples[-1][0] - samples[start_idx][0]
                delta_bytes = samples[-1][1] - samples[start_idx][1]
                if delta_t >= 0.2 and delta_bytes > 0:
                    return (delta_bytes * 8.0) / (delta_t * 1_000_000.0)

    return (fallback_bytes * 8.0) / (max(0.01, fallback_sec) * 1_000_000.0)


def _measure_latency(conn: http.client.HTTPSConnection) -> Tuple[float, float, str]:
    """Run 1 warmup + 3 timed 0-byte requests to measure idle RTT, jitter, and edge POP."""
    server_location = "Cloudflare Edge"
    headers_req = {
        "User-Agent": USER_AGENT,
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    # Warmup (connects TCP + TLS)
    conn.request("GET", "/__down?bytes=0", headers=headers_req)
    warm_resp = conn.getresponse()
    warm_resp.read()
    if warm_resp.status < 200 or warm_resp.status >= 300:
        raise RuntimeError(f"Cloudflare latency probe returned HTTP {warm_resp.status}")
    server_location = _parse_server_location(warm_resp.getheaders())

    rtts_ms: List[float] = []
    for _ in range(3):
        t0 = time.perf_counter()
        conn.request("GET", "/__down?bytes=0", headers=headers_req)
        resp = conn.getresponse()
        resp.read()
        t1 = time.perf_counter()
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Cloudflare latency probe returned HTTP {resp.status}")
        rtts_ms.append((t1 - t0) * 1000.0)

    ping_ms = round(statistics.median(rtts_ms), 2)
    if len(rtts_ms) >= 2:
        diffs = [abs(rtts_ms[i] - rtts_ms[i - 1]) for i in range(1, len(rtts_ms))]
        jitter_ms = round(sum(diffs) / len(diffs), 2)
    else:
        jitter_ms = 0.0

    return ping_ms, jitter_ms, server_location


def _download_stream(
    num_bytes: int,
    hard_timeout: float,
    conn: Optional[http.client.HTTPSConnection] = None,
) -> Tuple[int, List[Tuple[float, int]], float, bool]:
    """Download `num_bytes` from Cloudflare /__down while tracking chunk timestamps."""
    owns_conn = conn is None
    if conn is None:
        conn = _make_connection(timeout=max(6.0, hard_timeout + 2.0))

    total_read = 0
    samples: List[Tuple[float, int]] = []
    drained = True
    t_req = time.perf_counter()

    try:
        conn.request(
            "GET",
            f"/__down?bytes={int(num_bytes)}",
            headers={
                "User-Agent": USER_AGENT,
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        resp = conn.getresponse()
        if resp.status < 200 or resp.status >= 300:
            resp.read()
            raise RuntimeError(f"Download endpoint returned HTTP {resp.status}")

        t_first = time.perf_counter()
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            total_read += len(chunk)
            now = time.perf_counter()
            samples.append((now, total_read))
            if (now - t_first) >= hard_timeout:
                drained = False
                try:
                    conn.close()
                except Exception:
                    pass
                break

        wall_sec = max(0.005, time.perf_counter() - t_req)
        return total_read, samples, wall_sec, drained
    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass


def _upload_stream(
    num_bytes: int,
    hard_timeout: float,
    conn: Optional[http.client.HTTPSConnection] = None,
) -> Tuple[int, float, bool]:
    """Upload `num_bytes` to Cloudflare /__up and include server ACK in timing."""
    owns_conn = conn is None
    if conn is None:
        conn = _make_connection(timeout=max(6.0, hard_timeout + 2.0))

    target = int(num_bytes)
    t_start = time.perf_counter()
    sent = 0

    try:
        conn.putrequest("POST", "/__up")
        conn.putheader("User-Agent", USER_AGENT)
        conn.putheader("Content-Type", "application/octet-stream")
        conn.putheader("Content-Length", str(target))
        conn.putheader("Connection", "keep-alive")
        conn.endheaders()

        chunk_len = len(UPLOAD_CHUNK)
        while sent < target:
            if (time.perf_counter() - t_start) > hard_timeout:
                try:
                    conn.close()
                except Exception:
                    pass
                raise TimeoutError("Upload exceeded hard timeout")
            remaining = target - sent
            to_send = chunk_len if remaining >= chunk_len else remaining
            payload = UPLOAD_CHUNK if to_send == chunk_len else UPLOAD_CHUNK[:to_send]
            conn.send(payload)
            sent += to_send

        resp = conn.getresponse()
        resp.read()
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Upload endpoint returned HTTP {resp.status}")

        elapsed = max(0.005, time.perf_counter() - t_start)
        return sent, elapsed, True
    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass


def run_cloudflare_speedtest(
    mode: str = "eco",
    trigger: str = "scheduled",
    cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
) -> Dict[str, Any]:
    """
    Execute an adaptive, low-bandwidth Cloudflare speedtest.
    Single-stream by default ('eco') to minimize saturation of LAN gaming/streaming devices.
    """
    mode_key = mode if mode in SPEEDTEST_MODES else "eco"
    cfg = SPEEDTEST_MODES[mode_key]

    t_overall_start = time.perf_counter()
    total_bytes = 0
    dl_conn: Optional[http.client.HTTPSConnection] = None
    up_conn: Optional[http.client.HTTPSConnection] = None

    try:
        dl_conn = _make_connection(timeout=8.0)
        ping_ms, jitter_ms, server_location = _measure_latency(dl_conn)

        # ------------------------------------------------------------------
        # 1. Adaptive Download (1 MB calibration -> sized short burst)
        # ------------------------------------------------------------------
        cal_dl_bytes = 1_000_000
        cal_read, cal_samples, cal_wall, cal_drained = _download_stream(
            cal_dl_bytes, cfg.dl_hard_timeout, conn=dl_conn
        )
        total_bytes += cal_read

        if cal_wall >= 1.5 or not cal_drained:
            # Slow link (<= ~5 Mbps): calibration burst itself is already ~1.5s+
            dl_mbps = _compute_trimmed_mbps(cal_samples, cal_read, cal_wall)
        else:
            est_bps = cal_read / max(0.02, cal_wall)
            target_dl = int(est_bps * cfg.dl_target_sec)
            target_dl = max(cfg.dl_min_bytes, min(cfg.dl_max_bytes, target_dl))

            if cfg.streams == 1:
                if not cal_drained:
                    try:
                        dl_conn.close()
                    except Exception:
                        pass
                    dl_conn = _make_connection(timeout=max(6.0, cfg.dl_hard_timeout + 2.0))
                burst_read, burst_samples, burst_wall, _ = _download_stream(
                    target_dl, cfg.dl_hard_timeout, conn=dl_conn
                )
                total_bytes += burst_read
                dl_mbps = _compute_trimmed_mbps(burst_samples, burst_read, burst_wall)
            else:
                try:
                    dl_conn.close()
                except Exception:
                    pass
                dl_conn = None

                per_stream = max(cfg.dl_min_bytes // cfg.streams, target_dl // cfg.streams)
                t_multi_start = time.perf_counter()
                with ThreadPoolExecutor(max_workers=cfg.streams) as pool:
                    futures = [
                        pool.submit(_download_stream, per_stream, cfg.dl_hard_timeout, None)
                        for _ in range(cfg.streams)
                    ]
                    stream_results = [f.result() for f in futures]
                multi_wall = max(0.01, time.perf_counter() - t_multi_start)

                multi_read = 0
                all_deltas: List[Tuple[float, int]] = []
                for s_read, s_samples, _s_wall, _s_drained in stream_results:
                    multi_read += s_read
                    prev_b = 0
                    for ts, cum_b in s_samples:
                        delta = cum_b - prev_b
                        if delta > 0:
                            all_deltas.append((ts, delta))
                        prev_b = cum_b
                total_bytes += multi_read

                all_deltas.sort(key=lambda item: item[0])
                merged_samples: List[Tuple[float, int]] = []
                running_cum = 0
                for ts, delta in all_deltas:
                    running_cum += delta
                    merged_samples.append((ts, running_cum))

                dl_mbps = _compute_trimmed_mbps(merged_samples, multi_read, multi_wall)

        if dl_conn is not None:
            try:
                dl_conn.close()
            except Exception:
                pass
            dl_conn = None

        # ------------------------------------------------------------------
        # 2. Optional Cooldown + Adaptive Upload
        # ------------------------------------------------------------------
        ul_mbps: Optional[float] = None
        if cfg.run_upload:
            if cooldown_sec > 0:
                time.sleep(cooldown_sec)

            rtt_sec = max(0.0, ping_ms / 1000.0)
            up_conn = _make_connection(timeout=max(6.0, cfg.ul_hard_timeout + 2.0))
            cal_ul_bytes = 262_144  # 256 KB calibration
            cal_sent, cal_up_elapsed, _ = _upload_stream(
                cal_ul_bytes, cfg.ul_hard_timeout, conn=up_conn
            )
            total_bytes += cal_sent

            cal_net = max(0.015, cal_up_elapsed - min(rtt_sec, cal_up_elapsed * 0.5))
            if cal_up_elapsed >= 1.5:
                ul_mbps = (cal_sent * 8.0) / (cal_net * 1_000_000.0)
            else:
                est_up_bps = cal_sent / cal_net
                target_ul = int(est_up_bps * cfg.ul_target_sec)
                target_ul = max(cfg.ul_min_bytes, min(cfg.ul_max_bytes, target_ul))

                if cfg.streams == 1:
                    try:
                        up_sent, up_elapsed, _ = _upload_stream(
                            target_ul, cfg.ul_hard_timeout, conn=up_conn
                        )
                    except (http.client.RemoteDisconnected, ConnectionResetError):
                        try:
                            up_conn.close()
                        except Exception:
                            pass
                        up_conn = _make_connection(timeout=max(6.0, cfg.ul_hard_timeout + 2.0))
                        up_sent, up_elapsed, _ = _upload_stream(
                            target_ul, cfg.ul_hard_timeout, conn=up_conn
                        )
                    total_bytes += up_sent
                    up_net = max(0.02, up_elapsed - min(rtt_sec, up_elapsed * 0.5))
                    ul_mbps = (up_sent * 8.0) / (up_net * 1_000_000.0)
                else:
                    try:
                        up_conn.close()
                    except Exception:
                        pass
                    up_conn = None

                    per_stream_ul = max(cfg.ul_min_bytes // cfg.streams, target_ul // cfg.streams)
                    t_up_multi_start = time.perf_counter()
                    with ThreadPoolExecutor(max_workers=cfg.streams) as pool:
                        futures = [
                            pool.submit(_upload_stream, per_stream_ul, cfg.ul_hard_timeout, None)
                            for _ in range(cfg.streams)
                        ]
                        up_results = [f.result() for f in futures]
                    up_multi_wall = max(0.02, time.perf_counter() - t_up_multi_start)
                    up_multi_sent = sum(r[0] for r in up_results)
                    total_bytes += up_multi_sent

                    up_net = max(0.02, up_multi_wall - min(rtt_sec, up_multi_wall * 0.5))
                    ul_mbps = (up_multi_sent * 8.0) / (up_net * 1_000_000.0)

        duration_sec = round(max(0.01, time.perf_counter() - t_overall_start), 2)
        return {
            "timestamp": int(time.time()),
            "download_mbps": round(dl_mbps, 2),
            "upload_mbps": round(ul_mbps, 2) if ul_mbps is not None else None,
            "ping_ms": round(ping_ms, 2),
            "jitter_ms": round(jitter_ms, 2),
            "bytes_used": int(total_bytes),
            "duration_sec": duration_sec,
            "mode": mode_key,
            "server_location": server_location,
            "trigger": trigger,
            "status": "ok",
            "error_message": None,
        }
    except Exception as exc:
        logger.warning("Speedtest failed (mode=%s, trigger=%s): %s", mode_key, trigger, exc)
        return {
            "timestamp": int(time.time()),
            "download_mbps": None,
            "upload_mbps": None,
            "ping_ms": None,
            "jitter_ms": None,
            "bytes_used": int(total_bytes),
            "duration_sec": round(max(0.01, time.perf_counter() - t_overall_start), 2),
            "mode": mode_key,
            "server_location": "Cloudflare Edge",
            "trigger": trigger,
            "status": "error",
            "error_message": str(exc),
        }
    finally:
        for c in (dl_conn, up_conn):
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass


class SpeedtestScheduler:
    """
    Runs periodic low-impact speedtests in a dedicated single-worker executor
    without interfering with 1s ICMP probing or FastAPI event-loop responsiveness.
    """

    def __init__(
        self,
        broadcast_cb: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        startup_grace_sec: int = 15,
        cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
    ) -> None:
        self.broadcast_cb = broadcast_cb
        self.startup_grace_sec = max(0, int(startup_grace_sec))
        self.cooldown_sec = float(cooldown_sec)

        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="speedtest-runner")
        self._run_lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._manual_task: Optional[asyncio.Task] = None

        self.is_running: bool = False
        self.active_trigger: Optional[str] = None
        self.next_run_ts: Optional[int] = None
        self.last_run_ts: Optional[int] = None
        self.last_error: Optional[str] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())
        logger.info("SpeedtestScheduler started (startup grace: %ss)", self.startup_grace_sec)

    async def stop(self) -> None:
        self._running = False
        self._wake_event.set()
        for task in (self._task, self._manual_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._task = None
        self._manual_task = None
        self._executor.shutdown(wait=False, cancel_futures=True)
        logger.info("SpeedtestScheduler stopped")

    def _compute_next_run_ts(
        self, settings: Dict[str, Any], first_loop: bool = False
    ) -> Optional[int]:
        if not settings.get("enabled", True):
            return None
        interval_sec = int(settings.get("interval_min", 60)) * 60
        now = int(time.time())
        if self.last_run_ts is not None and (self.last_run_ts + interval_sec) > now:
            return self.last_run_ts + interval_sec
        return (now + self.startup_grace_sec) if first_loop else now

    def on_settings_updated(self, settings: Optional[Dict[str, Any]] = None) -> None:
        """Wake the scheduler so updated interval/enabled settings take effect immediately."""
        if settings is not None:
            self.next_run_ts = self._compute_next_run_ts(settings, first_loop=False)
        self._wake_event.set()

    def get_status(self, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cfg = settings or {
            "enabled": database.DEFAULT_SPEEDTEST_ENABLED,
            "interval_min": database.DEFAULT_SPEEDTEST_INTERVAL_MIN,
            "mode": database.DEFAULT_SPEEDTEST_MODE,
        }
        return {
            "enabled": bool(cfg.get("enabled", True)),
            "interval_min": int(cfg.get("interval_min", 60)),
            "mode": str(cfg.get("mode", "eco")),
            "is_running": self.is_running,
            "active_trigger": self.active_trigger,
            "next_run_ts": self.next_run_ts if cfg.get("enabled", True) else None,
            "last_run_ts": self.last_run_ts,
            "last_error": self.last_error,
            "modes": list_speedtest_modes(),
        }

    async def _broadcast(self, payload: Dict[str, Any]) -> None:
        if self.broadcast_cb is not None:
            try:
                await self.broadcast_cb(payload)
            except Exception as exc:
                logger.debug("Speedtest WS broadcast failed: %s", exc)

    async def run_once(self, trigger: str = "scheduled") -> Dict[str, Any]:
        async with self._run_lock:
            settings = await database.run_db_read(database.get_speedtest_settings)
            self.is_running = True
            self.active_trigger = trigger
            await self._broadcast({"type": "speedtest_status", "status": self.get_status(settings)})

            saved: Dict[str, Any] = {}
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self._executor,
                    run_cloudflare_speedtest,
                    settings["mode"],
                    trigger,
                    self.cooldown_sec,
                )
                saved = await database.run_db_write(database.save_speedtest_result, result)
                self.last_run_ts = int(saved.get("timestamp") or time.time())
                self.last_error = (
                    saved.get("error_message") if saved.get("status") != "ok" else None
                )
            finally:
                self.is_running = False
                self.active_trigger = None
                latest_settings = await database.run_db_read(database.get_speedtest_settings)
                if latest_settings.get("enabled") and self.last_run_ts is not None:
                    self.next_run_ts = self.last_run_ts + (int(latest_settings["interval_min"]) * 60)
                else:
                    self.next_run_ts = None

            if saved:
                await self._broadcast(
                    {
                        "type": "speedtest_result",
                        "result": saved,
                        "status": self.get_status(latest_settings),
                    }
                )
            return saved

    def trigger_manual_run(self) -> bool:
        """Launch a manual speedtest in the background if one is not already active."""
        if self.is_running or self._run_lock.locked():
            return False
        self.is_running = True
        self.active_trigger = "manual"
        self._manual_task = asyncio.create_task(self.run_once(trigger="manual"))
        return True

    async def _scheduler_loop(self) -> None:
        self.last_run_ts = await database.run_db_read(database.get_latest_speedtest_ts)
        first_loop = True

        while self._running:
            try:
                settings = await database.run_db_read(database.get_speedtest_settings)
                if not settings["enabled"]:
                    self.next_run_ts = None
                    self._wake_event.clear()
                    await self._wake_event.wait()
                    self._wake_event.clear()
                    first_loop = False
                    continue

                target_ts = self._compute_next_run_ts(settings, first_loop=first_loop)
                first_loop = False
                self.next_run_ts = target_ts

                delay = max(0.0, float((target_ts or now) - time.time()))
                if delay > 0.0:
                    self._wake_event.clear()
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=delay)
                        # Woken by settings update or stop; recalculate next_run_ts without running early.
                        self._wake_event.clear()
                        continue
                    except asyncio.TimeoutError:
                        pass

                if not self._running:
                    break

                # Re-verify enabled status after waiting before executing
                settings = await database.run_db_read(database.get_speedtest_settings)
                if not settings["enabled"]:
                    self.next_run_ts = None
                    continue

                if not self.is_running and not self._run_lock.locked():
                    await self.run_once(trigger="scheduled")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Unhandled error in SpeedtestScheduler loop: %s", exc)
                await asyncio.sleep(10)
