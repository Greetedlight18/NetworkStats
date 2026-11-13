import asyncio
import collections
import ipaddress
import json
import logging
import math
import os
import shutil
import socket
import time
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

from icmplib import async_multiping, traceroute

from app import database

logger = logging.getLogger("NetworkStats.monitor")

DEFAULT_INTERVAL = float(os.getenv("PROBE_INTERVAL", "1.0"))
ROLLING_WINDOW_SIZE = 100
SPARKLINE_SIZE = 30
RDNS_TTL_SEC = 3600
SHARED_PING_CACHE_TTL = 0.75


def _is_private_or_cgnat_ip(ip_str: str) -> bool:
    """
    Return True if IP is RFC1918 private (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
    or RFC6598 CGNAT (100.64.0.0/10).
    """
    try:
        addr = ipaddress.ip_address(ip_str)
        return addr.is_private or addr in ipaddress.ip_network("100.64.0.0/10")
    except Exception:
        return False


def _is_docker_bridge_ip(ip_str: str) -> bool:
    """Return True if IP is inside Docker's default 172.16.0.0/12 bridge range."""
    try:
        addr = ipaddress.ip_address(ip_str)
        return addr in ipaddress.ip_network("172.16.0.0/12")
    except Exception:
        return False


class RDNSResolver:
    """Non-blocking reverse DNS resolver with in-memory TTL cache."""

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[str, float]] = {}
        self._in_flight: set[str] = set()

    def get_cached(self, ip: str) -> str:
        if not ip or ip == "???":
            return "???"
        now = time.time()
        cached = self._cache.get(ip)
        if cached and cached[1] > now:
            return cached[0]
        if ip not in self._in_flight:
            self._in_flight.add(ip)
            asyncio.create_task(self._resolve_bg(ip))
        return cached[0] if cached else ip

    async def _resolve_bg(self, ip: str) -> None:
        loop = asyncio.get_running_loop()
        hostname = ip
        try:
            res = await asyncio.wait_for(
                loop.run_in_executor(None, socket.gethostbyaddr, ip),
                timeout=2.0,
            )
            if res and res[0]:
                hostname = res[0]
        except Exception:
            hostname = ip
        finally:
            self._cache[ip] = (hostname, time.time() + RDNS_TTL_SEC)
            self._in_flight.discard(ip)


class SharedPingProber:
    """
    Deduplicates ICMP Echo probes across all monitored targets so shared hops
    (like Hop #1 router and Hop #2 ISP modem) are only pinged once per second total.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[Optional[float], bool, float]] = {}
        self._lock: Optional[asyncio.Lock] = None

    async def probe_ips(self, ips: List[str]) -> Dict[str, Tuple[Optional[float], bool]]:
        if self._lock is None:
            self._lock = asyncio.Lock()

        now = time.monotonic()
        results: Dict[str, Tuple[Optional[float], bool]] = {}
        direct_ips: List[str] = []

        async with self._lock:
            for ip in ips:
                if not ip or ip == "???":
                    continue
                cached = self._cache.get(ip)
                if cached and cached[2] > now:
                    results[ip] = (cached[0], cached[1])
                elif ip not in direct_ips:
                    direct_ips.append(ip)

            if direct_ips:
                try:
                    responses = await async_multiping(
                        direct_ips,
                        count=1,
                        timeout=0.8,
                        privileged=True,
                    )
                    resp_time = time.monotonic()
                    for r in responses:
                        if r.is_alive:
                            ms = round(float(r.avg_rtt), 2)
                            self._cache[r.address] = (ms, False, resp_time + SHARED_PING_CACHE_TTL)
                            results[r.address] = (ms, False)
                        else:
                            self._cache[r.address] = (None, True, resp_time + SHARED_PING_CACHE_TTL)
                            results[r.address] = (None, True)
                except Exception as exc:
                    logger.error("Error in async_multiping: %s", exc)
                    for ip in direct_ips:
                        results[ip] = (None, True)

        return results


class HopStatsAccumulator:
    """
    Tracks cumulative and rolling statistics for a single hop.
    Never resets cumulative `sent` or `lost` unless the user explicitly clicks Reset.
    """

    def __init__(
        self,
        hop_num: int,
        ip_address: str = "???",
        rdns_hostname: str = "???",
        sent: int = 0,
        lost: int = 0,
        last_ms: Optional[float] = None,
        avg_ms: Optional[float] = None,
        min_ms: Optional[float] = None,
        max_ms: Optional[float] = None,
        jitter_ms: float = 0.0,
        sparkline: Optional[List[Optional[float]]] = None,
    ) -> None:
        self.hop_num = hop_num
        self.ip_address = ip_address
        self.rdns_hostname = rdns_hostname
        self.sent = sent
        self.lost = lost
        self.last_ms = last_ms
        self.avg_ms = avg_ms
        self.min_ms = min_ms
        self.max_ms = max_ms
        self.jitter_ms = jitter_ms

        self.rolling_outcomes: Deque[bool] = collections.deque(maxlen=ROLLING_WINDOW_SIZE)
        self.rolling_latencies: Deque[float] = collections.deque(maxlen=ROLLING_WINDOW_SIZE)
        self.sparkline: Deque[Optional[float]] = collections.deque(
            sparkline or [], maxlen=SPARKLINE_SIZE
        )
        for val in self.sparkline:
            if val is None:
                self.rolling_outcomes.append(True)
            else:
                self.rolling_outcomes.append(False)
                self.rolling_latencies.append(float(val))

    def record_sample(
        self,
        ip_address: str,
        rdns_hostname: str,
        latency_ms: Optional[float],
        is_loss: bool,
    ) -> None:
        self.ip_address = ip_address
        self.rdns_hostname = rdns_hostname if ip_address != "???" else "???"

        if ip_address == "???":
            self.last_ms = None
            return

        self.sent += 1
        if is_loss or latency_ms is None:
            self.lost += 1
            self.rolling_outcomes.append(True)
            self.sparkline.append(None)
            self.last_ms = None
        else:
            ms = round(float(latency_ms), 2)
            self.rolling_outcomes.append(False)
            self.rolling_latencies.append(ms)
            self.sparkline.append(ms)
            self.last_ms = ms

            if self.min_ms is None or ms < self.min_ms:
                self.min_ms = ms
            if self.max_ms is None or ms > self.max_ms:
                self.max_ms = ms

            responded = max(1, self.sent - self.lost)
            if self.avg_ms is None or responded <= 1:
                self.avg_ms = ms
            else:
                self.avg_ms = round(
                    self.avg_ms + ((ms - self.avg_ms) / min(responded, ROLLING_WINDOW_SIZE)),
                    2,
                )

        if len(self.rolling_latencies) >= 2:
            mean_val = sum(self.rolling_latencies) / len(self.rolling_latencies)
            variance = sum((x - mean_val) ** 2 for x in self.rolling_latencies) / len(
                self.rolling_latencies
            )
            self.jitter_ms = round(math.sqrt(variance), 2)
        else:
            self.jitter_ms = 0.0

    @property
    def loss_pct(self) -> float:
        if self.sent == 0:
            return 0.0
        return round((self.lost / self.sent) * 100.0, 2)

    @property
    def rolling_loss_pct(self) -> float:
        if not self.rolling_outcomes:
            return self.loss_pct
        lost_recent = sum(1 for x in self.rolling_outcomes if x)
        return round((lost_recent / len(self.rolling_outcomes)) * 100.0, 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hop_num": self.hop_num,
            "ip_address": self.ip_address,
            "rdns_hostname": self.rdns_hostname,
            "sent": self.sent,
            "lost": self.lost,
            "loss_pct": self.loss_pct,
            "rolling_loss_pct": self.rolling_loss_pct,
            "last_ms": self.last_ms,
            "avg_ms": self.avg_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "jitter_ms": self.jitter_ms,
            "sparkline": list(self.sparkline),
        }


class MonitorEngine:
    """
    Manages per-target route discovery and continuous 1s hop probing.
    Locks each target's route once discovered (until Reset) so round-robin DNS
    or intermittent intermediate routers never shift hop numbers or wipe loss counters.
    """

    def __init__(
        self,
        broadcast_cb: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]],
    ) -> None:
        self._broadcast_cb = broadcast_cb
        self._tasks: Dict[int, asyncio.Task] = {}
        self._prune_task: Optional[asyncio.Task] = None
        self._running = False
        self._rdns = RDNSResolver()
        self._mtr_bin = shutil.which("mtr")
        self._prober = SharedPingProber()
        self._hop_accumulators: Dict[int, Dict[int, HopStatsAccumulator]] = {}
        self._locked_routes: Dict[int, List[str]] = {}

    async def start(self) -> None:
        self._running = True
        logger.info("MonitorEngine started (mtr=%s)", self._mtr_bin or "icmplib")
        await self.sync_workers()
        self._prune_task = asyncio.create_task(self._prune_loop())

    async def stop(self) -> None:
        self._running = False
        if self._prune_task:
            self._prune_task.cancel()
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def sync_workers(self, restart_target_id: Optional[int] = None) -> None:
        """Synchronize running tasks with active targets in the SQLite database."""
        targets = database.list_targets()
        active_ids = {t["id"] for t in targets if t["enabled"]}

        if restart_target_id is not None and restart_target_id in self._tasks:
            self._tasks[restart_target_id].cancel()
            del self._tasks[restart_target_id]

        for t_id in list(self._tasks.keys()):
            if t_id not in active_ids:
                self._tasks[t_id].cancel()
                del self._tasks[t_id]
                logger.info("Stopped monitoring worker for target_id=%d", t_id)

        for t in targets:
            t_id = t["id"]
            if t["enabled"] and t_id not in self._tasks:
                if t_id not in self._hop_accumulators:
                    self._load_existing_accumulators(t_id, t["host"])
                self._tasks[t_id] = asyncio.create_task(self._target_loop(t_id))
                logger.info(
                    "Started monitoring worker for target_id=%d (%s - %s, %.1fs)",
                    t_id,
                    t["label"],
                    t["host"],
                    t["interval_sec"],
                )

    def reset_target_memory(self, target_id: int) -> None:
        self._hop_accumulators.pop(target_id, None)
        self._locked_routes.pop(target_id, None)

    def _load_existing_accumulators(self, target_id: int, host: str) -> None:
        """
        Restore hop accumulators and locked route from SQLite so container restarts
        or route refreshes never wipe cumulative Sent / Lost / Loss % counters.
        """
        existing_hops = database.get_hop_states(target_id)
        acc_map: Dict[int, HopStatsAccumulator] = {}
        route_ips: List[str] = []

        # Filter out any dead ISP-internal private hops (hop > 1 with 10.x / CGNAT IP)
        valid_hops = []
        for idx, h in enumerate(existing_hops):
            ip = h["ip_address"]
            if ip == "???":
                continue
            if idx == 0 and _is_docker_bridge_ip(ip) and ip != host:
                continue
            if idx > 0 and idx < len(existing_hops) - 1 and _is_private_or_cgnat_ip(ip):
                continue
            valid_hops.append(h)

        for new_idx, h in enumerate(valid_hops):
            hop_num = new_idx + 1
            acc_map[hop_num] = HopStatsAccumulator(
                hop_num=hop_num,
                ip_address=h["ip_address"],
                rdns_hostname=h["rdns_hostname"],
                sent=h["sent"],
                lost=h["lost"],
                last_ms=h["last_ms"],
                avg_ms=h["avg_ms"],
                min_ms=h["min_ms"],
                max_ms=h["max_ms"],
                jitter_ms=h["jitter_ms"],
                sparkline=h["sparkline"],
            )
            route_ips.append(h["ip_address"])

        self._hop_accumulators[target_id] = acc_map
        if len(route_ips) >= 1:
            self._locked_routes[target_id] = route_ips

    async def _prune_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(1800)
                database.prune_old_metrics()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in database prune loop: %s", exc)

    async def _target_loop(self, target_id: int) -> None:
        while self._running:
            cycle_start = time.monotonic()
            target = database.get_target(target_id)
            if not target or not target["enabled"]:
                break

            host = target["host"]
            interval = float(target.get("interval_sec") or DEFAULT_INTERVAL)

            try:
                raw_hops = await self._probe_route(target_id, host)
                if raw_hops:
                    now_ts = int(time.time())
                    acc_map = self._hop_accumulators.setdefault(target_id, {})

                    max_hop = max(h["hop_num"] for h in raw_hops)
                    for old_hop in list(acc_map.keys()):
                        if old_hop > max_hop:
                            del acc_map[old_hop]

                    hop_states: List[Dict[str, Any]] = []
                    raw_samples: List[Dict[str, Any]] = []

                    for h in raw_hops:
                        hop_num = h["hop_num"]
                        ip_addr = h["ip_address"]
                        is_loss = h["is_loss"]
                        latency_ms = h["latency_ms"]
                        rdns = self._rdns.get_cached(ip_addr)
                        if hop_num == max_hop and rdns == ip_addr and host != ip_addr:
                            rdns = host

                        acc = acc_map.get(hop_num)
                        if not acc:
                            acc = HopStatsAccumulator(hop_num=hop_num)
                            acc_map[hop_num] = acc

                        acc.record_sample(
                            ip_address=ip_addr,
                            rdns_hostname=rdns,
                            latency_ms=latency_ms,
                            is_loss=is_loss,
                        )
                        state_dict = acc.to_dict()
                        hop_states.append(state_dict)
                        raw_samples.append(
                            {
                                "hop_num": hop_num,
                                "ip_address": acc.ip_address,
                                "rdns_hostname": acc.rdns_hostname,
                                "latency_ms": latency_ms if not is_loss else None,
                                "is_loss": is_loss,
                            }
                        )

                    database.save_probe_results(
                        target_id=target_id,
                        timestamp=now_ts,
                        hop_states=hop_states,
                        raw_samples=raw_samples,
                    )

                    final_hop = hop_states[-1] if hop_states else None
                    await self._broadcast_cb(
                        {
                            "type": "probe_update",
                            "target_id": target_id,
                            "host": host,
                            "label": target["label"],
                            "timestamp": now_ts,
                            "hops": hop_states,
                            "summary": {
                                "current_ms": final_hop["last_ms"] if final_hop else None,
                                "avg_ms": final_hop["avg_ms"] if final_hop else None,
                                "jitter_ms": final_hop["jitter_ms"] if final_hop else 0.0,
                                "rolling_loss_pct": (
                                    final_hop["rolling_loss_pct"] if final_hop else 0.0
                                ),
                                "loss_pct": final_hop["loss_pct"] if final_hop else 0.0,
                                "lost_count": final_hop["lost"] if final_hop else 0,
                                "hop_count": len(hop_states),
                            },
                        }
                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Probe error for target %s (%d): %s", host, target_id, exc)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.05, interval - elapsed)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                break

    async def _resolve_host_ip(self, host: str) -> str:
        loop = asyncio.get_running_loop()
        try:
            res = await asyncio.wait_for(
                loop.run_in_executor(None, socket.gethostbyname, host),
                timeout=2.0,
            )
            return res or host
        except Exception:
            return host

    async def _get_locked_route(self, target_id: int, host: str) -> List[str]:
        """
        Return the locked hop IP list for `target_id`.
        Discovers the route once when the target starts (or after Reset) using 2 cycles
        so hops never randomly appear/disappear mid-session.
        """
        if target_id in self._locked_routes and self._locked_routes[target_id]:
            return self._locked_routes[target_id]

        dest_ip = await self._resolve_host_ip(host)
        raw_ips: List[str] = []

        if self._mtr_bin:
            cmd = [
                self._mtr_bin,
                "--report",
                "--report-cycles",
                "2",
                "--interval",
                "0.05",
                "--gracetime",
                "0.5",
                "--max-ttl",
                "24",
                "--no-dns",
                "--json",
                dest_ip,
            ]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
                if stdout:
                    payload = json.loads(stdout.decode("utf-8", errors="ignore"))
                    hubs = payload.get("report", {}).get("hubs", [])
                    for hub in hubs:
                        ip_addr = str(hub.get("host", "???")).strip() or "???"
                        raw_ips.append(ip_addr)
            except Exception as exc:
                logger.debug("mtr route discovery error for %s: %s", host, exc)

        if not raw_ips:
            try:
                loop = asyncio.get_running_loop()
                hops = await asyncio.wait_for(
                    loop.run_in_executor(
                        None, lambda: traceroute(dest_ip, count=1, timeout=0.8, max_hops=24)
                    ),
                    timeout=10.0,
                )
                raw_ips = [h.address for h in hops if h.address]
            except Exception as exc:
                logger.debug("icmplib route discovery error for %s: %s", host, exc)

        # Clean the route:
        # 1. Strip '???' timeouts
        # 2. Strip Docker bridge hop #1 (172.16.0.0/12)
        # 3. Strip unroutable ISP-internal private/CGNAT hops (e.g. 10.170.193.x at Hop > 1)
        clean_ips: List[str] = []
        for idx, ip in enumerate(raw_ips):
            if ip == "???":
                continue
            if idx == 0 and _is_docker_bridge_ip(ip) and dest_ip != ip:
                continue
            if len(clean_ips) >= 1 and ip != dest_ip and _is_private_or_cgnat_ip(ip):
                continue
            if not clean_ips or clean_ips[-1] != ip:
                clean_ips.append(ip)

        if not clean_ips or clean_ips[-1] != dest_ip:
            if dest_ip in clean_ips:
                clean_ips = clean_ips[: clean_ips.index(dest_ip) + 1]
            else:
                clean_ips.append(dest_ip)

        self._locked_routes[target_id] = clean_ips
        return clean_ips

    async def _probe_route(self, target_id: int, host: str) -> List[Dict[str, Any]]:
        """Probe all locked hops along the route."""
        hop_ips = await self._get_locked_route(target_id, host)
        if not hop_ips:
            return []

        ping_results = await self._prober.probe_ips(hop_ips)
        results: List[Dict[str, Any]] = []

        for idx, ip in enumerate(hop_ips):
            hop_num = idx + 1
            latency_ms, is_loss = ping_results.get(ip, (None, True))
            results.append(
                {
                    "hop_num": hop_num,
                    "ip_address": ip,
                    "latency_ms": latency_ms,
                    "is_loss": is_loss,
                }
            )
        return results
