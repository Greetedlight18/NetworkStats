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
RDNS_TTL_SEC = 1800
ROUTE_REFRESH_SEC = 90.0
SHARED_PING_CACHE_TTL = 0.75


def _is_docker_bridge_ip(ip_str: str) -> bool:
    """Return True if IP is inside Docker's default 172.16.0.0/12 bridge range."""
    try:
        addr = ipaddress.ip_address(ip_str)
        return addr in ipaddress.ip_network("172.16.0.0/12")
    except Exception:
        return False


def _is_private_or_cgnat_ip(ip_str: str) -> bool:
    """Return True if IP is RFC1918 private or RFC6598 CGNAT (100.64.0.0/10)."""
    try:
        addr = ipaddress.ip_address(ip_str)
        return addr.is_private or addr in ipaddress.ip_network("100.64.0.0/10")
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
    Deduplicates probes across all monitored targets:
    - Uses direct ICMP Echo (async_multiping) for routable hops & final targets.
    - Automatically falls back to a targeted single-TTL probe for ISP-internal
      private/CGNAT hops (e.g. 10.170.x.x on Hop 3) that cannot be pinged directly.
    """

    def __init__(self, mtr_bin: Optional[str]) -> None:
        self._mtr_bin = mtr_bin
        self._cache: Dict[str, Tuple[Optional[float], bool, float]] = {}
        self._lock: Optional[asyncio.Lock] = None

    async def probe_hops(
        self,
        hops_meta: List[Dict[str, Any]],
        dest_ip: str,
    ) -> Dict[str, Tuple[Optional[float], bool]]:
        if self._lock is None:
            self._lock = asyncio.Lock()

        now = time.monotonic()
        results: Dict[str, Tuple[Optional[float], bool]] = {}
        direct_ips: List[str] = []
        ttl_hops: List[Dict[str, Any]] = []

        async with self._lock:
            for idx, h in enumerate(hops_meta):
                ip = h["ip"]
                if not ip or ip == "???":
                    continue
                cached = self._cache.get(ip)
                if cached and cached[2] > now:
                    results[ip] = (cached[0], cached[1])
                    continue

                # Hop > 1 with a private/CGNAT IP (e.g. 10.170.193.x inside ISP network)
                # is unroutable via direct IP ping and must be probed via its TTL.
                if idx > 0 and ip != dest_ip and _is_private_or_cgnat_ip(ip):
                    ttl_hops.append(h)
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

        # Probe any ISP-internal private hops via targeted single-TTL mtr probe
        for h in ttl_hops:
            ip = h["ip"]
            ttl = h["ttl"]
            cached = self._cache.get(ip)
            if cached and cached[2] > time.monotonic():
                results[ip] = (cached[0], cached[1])
                continue
            ms, is_loss = await self._probe_single_ttl(dest_ip, ttl)
            self._cache[ip] = (ms, is_loss, time.monotonic() + SHARED_PING_CACHE_TTL)
            results[ip] = (ms, is_loss)

        return results

    async def _probe_single_ttl(self, dest_ip: str, ttl: int) -> Tuple[Optional[float], bool]:
        if not self._mtr_bin:
            return (None, True)
        cmd = [
            self._mtr_bin,
            "--report",
            "--report-cycles",
            "1",
            "--first-ttl",
            str(ttl),
            "--max-ttl",
            str(ttl),
            "--gracetime",
            "0.4",
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
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=2.0)
            if not stdout:
                return (None, True)
            payload = json.loads(stdout.decode("utf-8", errors="ignore"))
            hubs = payload.get("report", {}).get("hubs", [])
            if not hubs:
                return (None, True)
            hub = hubs[0]
            host_ip = str(hub.get("host", "???")).strip()
            loss_val = float(hub.get("Loss%", 0.0))
            last_val = float(hub.get("Last", 0.0))
            if host_ip == "???" or loss_val >= 100.0:
                return (None, True)
            return (round(last_val, 2), False)
        except Exception:
            return (None, True)


class HopStatsAccumulator:
    """Tracks rolling and cumulative statistics for a single hop."""

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
        if (
            ip_address != self.ip_address
            and self.ip_address != "???"
            and ip_address != "???"
        ):
            self.sent = 0
            self.lost = 0
            self.min_ms = None
            self.max_ms = None
            self.avg_ms = None
            self.rolling_outcomes.clear()
            self.rolling_latencies.clear()
            self.sparkline.clear()

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
        return round((self.lost / self.sent) * 100.0, 1)

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
    """Manages per-target async MTR route discovery + deduplicated hop probing."""

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
        self._prober = SharedPingProber(mtr_bin=self._mtr_bin)
        self._hop_accumulators: Dict[int, Dict[int, HopStatsAccumulator]] = {}
        self._route_cache: Dict[str, Tuple[List[Dict[str, Any]], str, float]] = {}
        self._route_locks: Dict[str, asyncio.Lock] = {}

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
                    self._load_existing_accumulators(t_id)
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
        target = database.get_target(target_id)
        if target:
            self._route_cache.pop(target["host"], None)

    def _load_existing_accumulators(self, target_id: int) -> None:
        existing_hops = database.get_hop_states(target_id)
        acc_map: Dict[int, HopStatsAccumulator] = {}
        for h in existing_hops:
            acc_map[h["hop_num"]] = HopStatsAccumulator(
                hop_num=h["hop_num"],
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
        self._hop_accumulators[target_id] = acc_map

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
                raw_hops = await self._probe_route(host)
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

    async def _get_clean_route_hops(self, host: str) -> Tuple[List[Dict[str, Any]], str]:
        now = time.monotonic()
        cached = self._route_cache.get(host)
        if cached and cached[2] > now:
            return cached[0], cached[1]

        if cached:
            asyncio.create_task(self._refresh_route_bg(host))
            return cached[0], cached[1]

        return await self._discover_route(host)

    async def _refresh_route_bg(self, host: str) -> None:
        lock = self._route_locks.setdefault(host, asyncio.Lock())
        if lock.locked():
            return
        async with lock:
            await self._discover_route(host)

    async def _discover_route(self, host: str) -> Tuple[List[Dict[str, Any]], str]:
        dest_ip = await self._resolve_host_ip(host)
        raw_hops: List[Dict[str, Any]] = []

        if self._mtr_bin:
            cmd = [
                self._mtr_bin,
                "--report",
                "--report-cycles",
                "1",
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
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=6.0)
                if stdout:
                    payload = json.loads(stdout.decode("utf-8", errors="ignore"))
                    hubs = payload.get("report", {}).get("hubs", [])
                    for idx, hub in enumerate(hubs):
                        ip_addr = str(hub.get("host", "???")).strip() or "???"
                        ttl_num = int(hub.get("count", idx + 1))
                        raw_hops.append({"ip": ip_addr, "ttl": ttl_num})
            except Exception as exc:
                logger.debug("mtr route discovery error for %s: %s", host, exc)

        if not raw_hops:
            try:
                loop = asyncio.get_running_loop()
                hops = await asyncio.wait_for(
                    loop.run_in_executor(
                        None, lambda: traceroute(dest_ip, count=1, timeout=0.8, max_hops=24)
                    ),
                    timeout=10.0,
                )
                raw_hops = [{"ip": h.address, "ttl": h.distance} for h in hops if h.address]
            except Exception as exc:
                logger.debug("icmplib route discovery error for %s: %s", host, exc)

        clean_hops: List[Dict[str, Any]] = []
        for idx, item in enumerate(raw_hops):
            ip = item["ip"]
            ttl = item["ttl"]
            if ip == "???":
                continue
            if idx == 0 and _is_docker_bridge_ip(ip) and dest_ip != ip:
                continue
            if not clean_hops or clean_hops[-1]["ip"] != ip:
                clean_hops.append({"ip": ip, "ttl": ttl})

        existing_ips = [h["ip"] for h in clean_hops]
        if not clean_hops or clean_hops[-1]["ip"] != dest_ip:
            if dest_ip in existing_ips:
                cut_idx = existing_ips.index(dest_ip)
                clean_hops = clean_hops[: cut_idx + 1]
            else:
                next_ttl = (clean_hops[-1]["ttl"] + 1) if clean_hops else 1
                clean_hops.append({"ip": dest_ip, "ttl": next_ttl})

        self._route_cache[host] = (clean_hops, dest_ip, time.monotonic() + ROUTE_REFRESH_SEC)
        return clean_hops, dest_ip

    async def _probe_route(self, host: str) -> List[Dict[str, Any]]:
        """Probe all discovered hops along the route."""
        hops_meta, dest_ip = await self._get_clean_route_hops(host)
        if not hops_meta:
            return []

        ping_results = await self._prober.probe_hops(hops_meta, dest_ip)
        results: List[Dict[str, Any]] = []

        for idx, h in enumerate(hops_meta):
            hop_num = idx + 1
            ip = h["ip"]
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
