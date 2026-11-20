import asyncio
import collections
import ipaddress
import json
import logging
import math
import os
import re
import shutil
import socket
import time
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

from icmplib import async_ping, traceroute

try:
    from icmplib import SocketPermissionError
except ImportError:  # pragma: no cover
    class SocketPermissionError(Exception):
        pass

from app import database

logger = logging.getLogger("NetworkStats.monitor")

DEFAULT_INTERVAL = float(os.getenv("PROBE_INTERVAL", "1.0"))
ROLLING_WINDOW_SIZE = 100
SPARKLINE_SIZE = 30
RDNS_TTL_SEC = 3600
SHARED_PING_CACHE_TTL = 0.75
_PING_TIME_RE = re.compile(r"time[=<]\s*([\d.]+)\s*ms", re.IGNORECASE)


def _is_valid_ip(val: str) -> bool:
    try:
        ipaddress.ip_address(val)
        return True
    except Exception:
        return False


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
            if _is_valid_ip(ip):
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


class PingProber:
    """
    Lock-free, in-flight deduplicated ICMP Echo prober.
    - Never blocks one target's ping on another target's timeout.
    - Isolates individual host errors (timeouts, Network Unreachable, DNS failure)
      so a failing host always returns (None, True) without corrupting healthy hops.
    - Deduplicates shared hops (router, modem) across concurrent targets via short
      result caching and shared in-flight asyncio.Tasks.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Tuple[Optional[float], bool, float]] = {}
        self._in_flight: Dict[str, asyncio.Task] = {}
        self._icmp_id: int = 0
        self._ping_bin = shutil.which("ping")

    async def probe_ips(self, ips: List[str]) -> Dict[str, Tuple[Optional[float], bool]]:
        results: Dict[str, Tuple[Optional[float], bool]] = {}
        pending_ips: List[str] = []
        pending_tasks: List[asyncio.Task] = []
        seen: set[str] = set()

        for ip in ips:
            if not ip or ip == "???" or ip in seen:
                continue
            seen.add(ip)
            now = time.monotonic()
            cached = self._cache.get(ip)
            if cached and cached[2] > now:
                results[ip] = (cached[0], cached[1])
            else:
                task = self._in_flight.get(ip)
                if task is None or task.done():
                    task = asyncio.create_task(self._probe_one_ip(ip))
                    self._in_flight[ip] = task
                pending_ips.append(ip)
                pending_tasks.append(task)

        if pending_tasks:
            gathered = await asyncio.gather(*pending_tasks, return_exceptions=True)
            for ip, res in zip(pending_ips, gathered):
                if isinstance(res, Exception) or not isinstance(res, tuple):
                    results[ip] = (None, True)
                else:
                    results[ip] = res

        return results

    async def _probe_one_ip(self, ip: str) -> Tuple[Optional[float], bool]:
        try:
            if _is_valid_ip(ip):
                resolved_ip = ip
            else:
                loop = asyncio.get_running_loop()
                try:
                    resolved_ip = await asyncio.wait_for(
                        loop.run_in_executor(None, socket.gethostbyname, ip),
                        timeout=1.0,
                    )
                    if not resolved_ip or not _is_valid_ip(resolved_ip):
                        self._cache[ip] = (None, True, time.monotonic() + SHARED_PING_CACHE_TTL)
                        return (None, True)
                except Exception:
                    self._cache[ip] = (None, True, time.monotonic() + SHARED_PING_CACHE_TTL)
                    return (None, True)

            self._icmp_id = (self._icmp_id % 65000) + 1
            icmp_id = self._icmp_id

            try:
                host_res = await asyncio.wait_for(
                    async_ping(resolved_ip, count=1, timeout=0.8, id=icmp_id, privileged=True),
                    timeout=1.3,
                )
                if (
                    host_res.is_alive
                    and getattr(host_res, "packets_received", 1) >= 1
                    and host_res.avg_rtt is not None
                ):
                    result: Tuple[Optional[float], bool] = (round(float(host_res.avg_rtt), 2), False)
                else:
                    result = (None, True)
            except SocketPermissionError:
                result = await self._ping_cli_fallback(resolved_ip)
            except asyncio.CancelledError:
                raise
            except Exception:
                result = (None, True)

            self._cache[ip] = (result[0], result[1], time.monotonic() + SHARED_PING_CACHE_TTL)
            return result
        finally:
            if self._in_flight.get(ip) is asyncio.current_task():
                self._in_flight.pop(ip, None)

    async def _ping_cli_fallback(self, ip: str) -> Tuple[Optional[float], bool]:
        if not self._ping_bin:
            return (None, True)
        try:
            proc = await asyncio.create_subprocess_exec(
                self._ping_bin,
                "-n",
                "-c",
                "1",
                "-W",
                "1",
                ip,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=1.5)
            if proc.returncode == 0:
                text = stdout.decode("utf-8", errors="ignore")
                match = _PING_TIME_RE.search(text)
                if match:
                    return (round(float(match.group(1)), 2), False)
                return (0.5, False)
            return (None, True)
        except asyncio.CancelledError:
            raise
        except Exception:
            return (None, True)


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
    Manages per-target route discovery, continuous 1s hop probing, and
    network-wide outage correlation without ever blocking on WebSockets or SQLite.
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
        self._prober = PingProber()
        self._hop_accumulators: Dict[int, Dict[int, HopStatsAccumulator]] = {}
        self._locked_routes: Dict[int, List[str]] = {}
        self._route_needs_discovery: set[int] = set()
        self._bg_route_tasks: Dict[int, asyncio.Task] = {}
        self._target_live_status: Dict[int, Dict[str, Any]] = {}
        self.last_probe_ts: Optional[int] = None

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
        for task in self._bg_route_tasks.values():
            task.cancel()
        all_tasks = list(self._tasks.values()) + list(self._bg_route_tasks.values())
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)
        self._tasks.clear()
        self._bg_route_tasks.clear()

    async def sync_workers(self, restart_target_id: Optional[int] = None) -> None:
        """Synchronize running tasks with active targets in the SQLite database."""
        targets = await database.run_db_read(database.list_targets)
        active_ids = {t["id"] for t in targets if t["enabled"]}

        if restart_target_id is not None and restart_target_id in self._tasks:
            self._tasks[restart_target_id].cancel()
            del self._tasks[restart_target_id]

        for t_id in list(self._tasks.keys()):
            if t_id not in active_ids:
                self._tasks[t_id].cancel()
                del self._tasks[t_id]
                self._target_live_status.pop(t_id, None)
                logger.info("Stopped monitoring worker for target_id=%d", t_id)

        for t in targets:
            t_id = t["id"]
            if t["enabled"] and t_id not in self._tasks:
                if t_id not in self._hop_accumulators:
                    await self._load_existing_accumulators(t_id, t["host"])
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
        self._route_needs_discovery.discard(target_id)
        self._target_live_status.pop(target_id, None)
        bg = self._bg_route_tasks.pop(target_id, None)
        if bg:
            bg.cancel()

    async def _load_existing_accumulators(self, target_id: int, host: str) -> None:
        """
        Restore hop accumulators and locked route from SQLite so container restarts
        never wipe cumulative Sent / Lost / Loss % counters.
        """
        existing_hops = await database.run_db_read(database.get_hop_states, target_id)
        acc_map: Dict[int, HopStatsAccumulator] = {}
        route_ips: List[str] = []

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
                await asyncio.sleep(300)
                await database.run_db_write(database.prune_old_metrics)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in database prune loop: %s", exc)

    async def _safe_broadcast(self, message: Dict[str, Any]) -> None:
        try:
            await asyncio.wait_for(self._broadcast_cb(message), timeout=2.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("WebSocket broadcast error ignored: %s", exc)

    async def _check_network_outage(self, now_ts: int) -> None:
        """
        Detect when the majority of active external targets drop simultaneously and
        persist a unified network outage record in `network_outages`.
        """
        recent = [
            s for s in self._target_live_status.values()
            if now_ts - int(s["ts"]) <= 5
        ]
        external = [s for s in recent if s["is_external"]]
        if len(external) < 2:
            return

        down_ext = [s for s in external if s["is_down"]]
        threshold = max(2, math.ceil(len(external) * 0.6))
        if len(down_ext) < threshold:
            return

        local_targets = [s for s in recent if not s["is_external"]]
        if local_targets and any(s["is_down"] for s in local_targets):
            router_reachable = False
        elif all(not s["router_ok"] for s in down_ext):
            router_reachable = False
        else:
            router_reachable = True

        await database.run_db_write(
            database.upsert_network_outage,
            now_ts,
            len(down_ext),
            len(external),
            router_reachable,
            [s["label"] for s in down_ext],
        )

    async def _process_target_tick(self, target: Dict[str, Any], now_ts: Optional[int] = None) -> None:
        """Execute one full probe + persistence + outage check cycle for a target."""
        target_id = target["id"]
        host = target["host"]

        raw_hops = await self._probe_route(target_id, host)
        if not raw_hops:
            return

        if now_ts is None:
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

        loss_recorded = await database.run_db_write(
            database.save_probe_results,
            target_id,
            now_ts,
            hop_states,
            raw_samples,
            target["label"],
            host,
        )

        self.last_probe_ts = now_ts
        final_sample = raw_samples[-1]
        final_is_loss = bool(final_sample["is_loss"])
        is_external = (
            not _is_private_or_cgnat_ip(final_sample["ip_address"])
            and not _is_private_or_cgnat_ip(host)
        )
        router_ok = not (len(raw_samples) > 1 and raw_samples[0]["is_loss"])

        self._target_live_status[target_id] = {
            "is_down": final_is_loss,
            "is_external": is_external,
            "label": target["label"],
            "host": host,
            "router_ok": router_ok,
            "ts": now_ts,
        }

        if target_id in self._route_needs_discovery and not final_is_loss:
            self._route_needs_discovery.discard(target_id)
            bg = self._bg_route_tasks.get(target_id)
            if bg is None or bg.done():
                self._bg_route_tasks[target_id] = asyncio.create_task(
                    self._discover_full_route_bg(target_id, host)
                )

        if loss_recorded:
            await self._check_network_outage(now_ts)
            asyncio.create_task(
                self._safe_broadcast(
                    {
                        "type": "loss_update",
                        "target_id": target_id,
                        "timestamp": now_ts,
                    }
                )
            )

        final_hop = hop_states[-1] if hop_states else None
        asyncio.create_task(
            self._safe_broadcast(
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
        )

    async def _target_loop(self, target_id: int) -> None:
        while self._running:
            cycle_start = time.monotonic()
            target = await database.run_db_read(database.get_target, target_id)
            if not target or not target["enabled"]:
                break

            interval = float(target.get("interval_sec") or DEFAULT_INTERVAL)

            try:
                await self._process_target_tick(target)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Probe error for target %s (%d): %s", target["host"], target_id, exc)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.05, interval - elapsed)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                break

    async def _resolve_host_ip(self, host: str) -> str:
        if _is_valid_ip(host):
            return host
        loop = asyncio.get_running_loop()
        try:
            res = await asyncio.wait_for(
                loop.run_in_executor(None, socket.gethostbyname, host),
                timeout=1.5,
            )
            return res or host
        except Exception:
            return host

    async def _discover_clean_route(self, host: str) -> Tuple[List[str], bool]:
        """Discover and clean route hops to `host`. Returns (clean_ips, resolved_ok)."""
        dest_ip = await self._resolve_host_ip(host)
        if not _is_valid_ip(dest_ip):
            return [host], False

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
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=6.0)
                if stdout:
                    payload = json.loads(stdout.decode("utf-8", errors="ignore"))
                    hubs = payload.get("report", {}).get("hubs", [])
                    for hub in hubs:
                        ip_addr = str(hub.get("host", "???")).strip() or "???"
                        raw_ips.append(ip_addr)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("mtr route discovery error for %s: %s", host, exc)

        if not raw_ips:
            try:
                loop = asyncio.get_running_loop()
                hops = await asyncio.wait_for(
                    loop.run_in_executor(
                        None, lambda: traceroute(dest_ip, count=1, timeout=0.8, max_hops=24)
                    ),
                    timeout=6.0,
                )
                raw_ips = [h.address for h in hops if h.address]
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("icmplib route discovery error for %s: %s", host, exc)

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

        return clean_ips, True

    async def _discover_full_route_bg(self, target_id: int, host: str) -> None:
        """Upgrade a target added during an outage to its full hop route once reachable."""
        try:
            clean_ips, resolved_ok = await self._discover_clean_route(host)
            if not resolved_ok or len(clean_ips) <= 1:
                return

            acc_map = self._hop_accumulators.get(target_id, {})
            old_final = acc_map.get(1)
            new_acc_map: Dict[int, HopStatsAccumulator] = {}

            for idx, ip in enumerate(clean_ips):
                hop_num = idx + 1
                if idx == len(clean_ips) - 1 and old_final and old_final.ip_address == ip:
                    old_final.hop_num = hop_num
                    new_acc_map[hop_num] = old_final
                else:
                    new_acc_map[hop_num] = HopStatsAccumulator(
                        hop_num=hop_num,
                        ip_address=ip,
                        rdns_hostname=self._rdns.get_cached(ip),
                    )

            self._hop_accumulators[target_id] = new_acc_map
            self._locked_routes[target_id] = clean_ips
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Background route discovery failed for %s: %s", host, exc)

    async def _get_locked_route(self, target_id: int, host: str) -> List[str]:
        """
        Return the locked hop IP list for `target_id`.
        If DNS fails during an outage, returns `[host]` without locking so every
        outage second is still probed and logged as packet loss.
        """
        if target_id in self._locked_routes and self._locked_routes[target_id]:
            return self._locked_routes[target_id]

        clean_ips, resolved_ok = await self._discover_clean_route(host)
        if not resolved_ok:
            return clean_ips

        self._locked_routes[target_id] = clean_ips
        if len(clean_ips) == 1 and not _is_private_or_cgnat_ip(clean_ips[0]):
            self._route_needs_discovery.add(target_id)
        else:
            self._route_needs_discovery.discard(target_id)

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
