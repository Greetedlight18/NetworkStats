import asyncio
import collections
import json
import logging
import math
import os
import shutil
import socket
import time
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

from app import database

logger = logging.getLogger("NetworkStats.monitor")

DEFAULT_INTERVAL = float(os.getenv("PROBE_INTERVAL", "2.0"))
ROLLING_WINDOW_SIZE = 100
SPARKLINE_SIZE = 30
RDNS_TTL_SEC = 1800


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
        if ip_address and ip_address != "???":
            self.ip_address = ip_address
            self.rdns_hostname = rdns_hostname
        elif self.ip_address == "???":
            self.rdns_hostname = "???"

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
                # Incremental mean update blended with rolling window
                self.avg_ms = round(
                    self.avg_ms + ((ms - self.avg_ms) / min(responded, ROLLING_WINDOW_SIZE)),
                    2,
                )

        # Standard deviation (Jitter) over rolling window
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
    """Manages per-target async MTR workers and broadcasts real-time updates."""

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
        self._hop_accumulators: Dict[int, Dict[int, HopStatsAccumulator]] = {}
        self._route_cache: Dict[str, Tuple[List[str], float]] = {}

    async def start(self) -> None:
        self._running = True
        if self._mtr_bin:
            logger.info("MTR Engine initialized using binary: %s", self._mtr_bin)
        else:
            logger.warning("mtr binary not found in PATH; falling back to icmplib engine")

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

    async def sync_workers(self) -> None:
        """Synchronize running tasks with active targets in the SQLite database."""
        targets = database.list_targets()
        active_ids = {t["id"] for t in targets if t["enabled"]}

        # Cancel removed or paused targets
        for t_id in list(self._tasks.keys()):
            if t_id not in active_ids:
                self._tasks[t_id].cancel()
                del self._tasks[t_id]
                logger.info("Stopped monitoring worker for target_id=%d", t_id)

        # Start new or resumed targets
        for t in targets:
            t_id = t["id"]
            if t["enabled"] and t_id not in self._tasks:
                self._load_existing_accumulators(t_id)
                self._tasks[t_id] = asyncio.create_task(self._target_loop(t_id))
                logger.info(
                    "Started monitoring worker for target_id=%d (%s - %s)",
                    t_id,
                    t["label"],
                    t["host"],
                )

    def reset_target_memory(self, target_id: int) -> None:
        self._hop_accumulators.pop(target_id, None)

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
                await asyncio.sleep(1800)  # Prune every 30 minutes
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
                    # Trim stale hops beyond current route length
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
                                "hop_count": len(hop_states),
                            },
                        }
                    )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Probe error for target %s (%d): %s", host, target_id, exc)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.25, interval - elapsed)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                break

    async def _probe_route(self, host: str) -> List[Dict[str, Any]]:
        """Run non-blocking MTR JSON cycle, falling back to icmplib if needed."""
        if self._mtr_bin:
            hops = await self._probe_with_mtr(host)
            if hops:
                return hops
        return await self._probe_with_icmplib(host)

    async def _probe_with_mtr(self, host: str) -> List[Dict[str, Any]]:
        cmd = [
            self._mtr_bin,
            "--report",
            "--report-cycles",
            "1",
            "--interval",
            "0.2",
            "--gracetime",
            "1",
            "--no-dns",
            "--json",
            host,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
            if not stdout:
                return []

            payload = json.loads(stdout.decode("utf-8", errors="ignore"))
            hubs = payload.get("report", {}).get("hubs", [])
            if not hubs:
                return []

            results: List[Dict[str, Any]] = []
            for idx, hub in enumerate(hubs):
                hop_num = int(hub.get("count", idx + 1))
                ip_addr = str(hub.get("host", "???")).strip() or "???"
                loss_val = float(hub.get("Loss%", 0.0))
                last_val = float(hub.get("Last", 0.0))
                is_loss = ip_addr == "???" or loss_val >= 100.0
                latency = None if is_loss else round(last_val, 2)
                results.append(
                    {
                        "hop_num": hop_num,
                        "ip_address": ip_addr,
                        "latency_ms": latency,
                        "is_loss": is_loss,
                    }
                )
            return results
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("mtr execution fallback for %s: %s", host, exc)
            return []

    async def _probe_with_icmplib(self, host: str) -> List[Dict[str, Any]]:
        """Fallback concurrent ICMP hop prober using icmplib or system ping."""
        try:
            from icmplib import async_multiping, async_traceroute

            now = time.time()
            cached_route = self._route_cache.get(host)
            if not cached_route or cached_route[1] < now:
                hops = await async_traceroute(host, count=1, timeout=1.0, max_hops=24)
                hop_ips: List[str] = []
                last_distance = 0
                for h in hops:
                    while last_distance + 1 < h.distance:
                        hop_ips.append("???")
                        last_distance += 1
                    hop_ips.append(h.address)
                    last_distance = h.distance
                if not hop_ips:
                    hop_ips = [host]
                self._route_cache[host] = (hop_ips, now + 120.0)
            else:
                hop_ips = cached_route[0]

            valid_ips = [ip for ip in hop_ips if ip != "???"]
            ping_map = {}
            if valid_ips:
                responses = await async_multiping(valid_ips, count=1, timeout=1.0)
                for r in responses:
                    ping_map[r.address] = r

            results: List[Dict[str, Any]] = []
            for idx, ip in enumerate(hop_ips):
                hop_num = idx + 1
                if ip == "???":
                    results.append(
                        {
                            "hop_num": hop_num,
                            "ip_address": "???",
                            "latency_ms": None,
                            "is_loss": True,
                        }
                    )
                else:
                    r = ping_map.get(ip)
                    if r and r.is_alive:
                        results.append(
                            {
                                "hop_num": hop_num,
                                "ip_address": ip,
                                "latency_ms": round(float(r.avg_rtt), 2),
                                "is_loss": False,
                            }
                        )
                    else:
                        results.append(
                            {
                                "hop_num": hop_num,
                                "ip_address": ip,
                                "latency_ms": None,
                                "is_loss": True,
                            }
                        )
            return results
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("icmplib fallback failed for %s: %s", host, exc)
            return []
