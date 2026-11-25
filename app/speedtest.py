import asyncio
import http.client
import json
import logging
import os
import random
import re
import ssl
import statistics
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from app import database

logger = logging.getLogger("NetworkStats.speedtest")

CLOUDFLARE_HOST = "speed.cloudflare.com"
FASTCOM_HOST = "fast.com"
FASTCOM_API_HOST = "api.fast.com"

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
BROWSER_HEADERS: Dict[str, str] = {
    "User-Agent": BROWSER_USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
CLOUDFLARE_HEADERS: Dict[str, str] = {
    **BROWSER_HEADERS,
    "Referer": "https://speed.cloudflare.com/",
    "Origin": "https://speed.cloudflare.com",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

READ_CHUNK_SIZE = 262144  # 256 KB read chunks minimize Python loop overhead at 1 Gbps
UPLOAD_CHUNK = os.urandom(262144)  # 256 KB incompressible upload chunk
DEFAULT_COOLDOWN_SEC = 0.4

_FASTCOM_TOKEN_TTL_SEC = 21600  # 6 hours
_FASTCOM_TOKEN_LOCK = threading.Lock()
_FASTCOM_TOKEN_CACHE: Dict[str, Any] = {"token": None, "expires_at": 0.0}


@dataclass(frozen=True)
class SpeedtestModeConfig:
    id: str
    label: str
    description: str
    dl_streams: int
    ul_streams: int
    dl_min_steady_sec: float
    dl_max_sec: float
    dl_max_bytes: int
    ul_target_sec: float
    ul_min_bytes: int
    ul_max_bytes: int
    ul_hard_timeout: float
    run_upload: bool

    @property
    def streams(self) -> int:
        return self.dl_streams


SPEEDTEST_MODES: Dict[str, SpeedtestModeConfig] = {
    "eco": SpeedtestModeConfig(
        id="eco",
        label="Eco (Low Impact)",
        description="2 streams, ~1.5-2.5s burst (<=150 MB down / <=20 MB up), early-stop when stable",
        dl_streams=2,
        ul_streams=1,
        dl_min_steady_sec=1.5,
        dl_max_sec=2.5,
        dl_max_bytes=150_000_000,
        ul_target_sec=1.3,
        ul_min_bytes=1_000_000,
        ul_max_bytes=20_000_000,
        ul_hard_timeout=5.0,
        run_upload=True,
    ),
    "eco_download_only": SpeedtestModeConfig(
        id="eco_download_only",
        label="Eco (Download Only)",
        description="2 streams download only (~1.5-2.5s burst), zero upload impact",
        dl_streams=2,
        ul_streams=0,
        dl_min_steady_sec=1.5,
        dl_max_sec=2.5,
        dl_max_bytes=150_000_000,
        ul_target_sec=0.0,
        ul_min_bytes=0,
        ul_max_bytes=0,
        ul_hard_timeout=0.0,
        run_upload=False,
    ),
    "balanced": SpeedtestModeConfig(
        id="balanced",
        label="Balanced (Gigabit)",
        description="4 streams, ~2-3.5s burst (<=300 MB down / <=50 MB up) for 1 Gbps accuracy",
        dl_streams=4,
        ul_streams=2,
        dl_min_steady_sec=2.0,
        dl_max_sec=3.5,
        dl_max_bytes=300_000_000,
        ul_target_sec=2.0,
        ul_min_bytes=2_000_000,
        ul_max_bytes=50_000_000,
        ul_hard_timeout=7.0,
        run_upload=True,
    ),
}


def list_speedtest_modes() -> List[Dict[str, Any]]:
    return [
        {
            "id": m.id,
            "label": m.label,
            "description": m.description,
            "streams": m.dl_streams,
            "ul_streams": m.ul_streams,
            "run_upload": m.run_upload,
        }
        for m in SPEEDTEST_MODES.values()
    ]


def _make_connection(
    host: str = CLOUDFLARE_HOST,
    timeout: float = 8.0,
) -> http.client.HTTPSConnection:
    ctx = ssl.create_default_context()
    return http.client.HTTPSConnection(host, timeout=timeout, context=ctx)


def _iter_headers(headers: Any) -> List[Tuple[str, str]]:
    if headers is None:
        return []
    if hasattr(headers, "items"):
        return [(str(k), str(v)) for k, v in headers.items()]
    if isinstance(headers, list):
        return [(str(k), str(v)) for k, v in headers]
    return []


def _get_header(headers: Any, name: str) -> Optional[str]:
    target = name.lower()
    for k, v in _iter_headers(headers):
        if k.lower() == target:
            return v
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


def _parse_cf_server_timing_ms(headers: Any) -> Tuple[float, Optional[float]]:
    """
    Extract Cloudflare edge/worker processing overhead (ms) and optional L4 kernel RTT (ms)
    from Server-Timing headers so idle ping reflects network RTT rather than worker latency.
    """
    server_ms = 0.0
    l4_rtt_ms: Optional[float] = None

    for k, v in _iter_headers(headers):
        if k.lower() != "server-timing":
            continue
        for m in re.finditer(r"cfSpeed(?:Edge|Worker)\s*;\s*dur\s*=\s*([\d.]+)", v, re.IGNORECASE):
            try:
                server_ms += float(m.group(1))
            except ValueError:
                pass
        if l4_rtt_ms is None:
            l4_m = re.search(r"[?&]rtt=(\d+)", v)
            if l4_m:
                try:
                    l4_rtt_ms = float(l4_m.group(1)) / 1000.0
                except ValueError:
                    pass

    return server_ms, l4_rtt_ms


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


def _compute_steady_mbps(
    chunk_events: List[Tuple[float, int]],
    fallback_sec: float = 0.01,
) -> float:
    """
    Compute outlier-resistant steady-state throughput (Mbps) from 200ms transfer buckets:
    drops initial 30% slow-start buckets, drops incomplete trailing bucket,
    and trims bottom 20% + top 10% outlier buckets.
    """
    if not chunk_events:
        return 0.0

    events = sorted(chunk_events, key=lambda item: item[0])
    total_bytes = sum(sz for _, sz in events if sz > 0)
    if total_bytes <= 0:
        return 0.0

    t_start = events[0][0]
    t_end = events[-1][0]
    span = max(0.0, t_end - t_start)

    bucket_sec = 0.2
    buckets: Dict[int, int] = {}
    max_idx = 0
    for ts, sz in events:
        if sz <= 0:
            continue
        idx = max(0, int((ts - t_start) / bucket_sec))
        buckets[idx] = buckets.get(idx, 0) + sz
        if idx > max_idx:
            max_idx = idx

    ordered_idx = list(range(0, max_idx + 1))
    if len(ordered_idx) >= 2:
        last_bucket_elapsed = t_end - (t_start + max_idx * bucket_sec)
        if last_bucket_elapsed < (bucket_sec * 0.5):
            ordered_idx = ordered_idx[:-1]

    full_bucket_mbps = [
        (buckets.get(idx, 0) * 8.0) / (bucket_sec * 1_000_000.0) for idx in ordered_idx
    ]

    if len(full_bucket_mbps) >= 5:
        warmup_n = max(1, int(len(full_bucket_mbps) * 0.30))
        remaining = full_bucket_mbps[warmup_n:]
        if remaining:
            steady = sorted(remaining)
            lo = int(len(steady) * 0.20)
            hi = max(lo + 1, len(steady) - int(len(steady) * 0.10))
            trimmed = steady[lo:hi]
            if trimmed:
                return sum(trimmed) / len(trimmed)

    cumulative_samples: List[Tuple[float, int]] = []
    running = 0
    for ts, sz in events:
        running += sz
        cumulative_samples.append((ts, running))
    return _compute_trimmed_mbps(cumulative_samples, total_bytes, max(0.01, span, fallback_sec))


def _decode_text(raw_bytes: bytes) -> str:
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1")


def _fetch_https_bytes(
    host: str,
    path: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 8.0,
) -> Tuple[int, List[Tuple[str, str]], bytes]:
    conn = _make_connection(host=host, timeout=timeout)
    try:
        req_headers = {**BROWSER_HEADERS, **(headers or {}), "Connection": "close"}
        conn.request("GET", path, headers=req_headers)
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, _iter_headers(resp.getheaders()), body
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_fastcom_token(force_refresh: bool = False) -> str:
    now = time.time()
    with _FASTCOM_TOKEN_LOCK:
        cached = _FASTCOM_TOKEN_CACHE.get("token")
        expires_at = float(_FASTCOM_TOKEN_CACHE.get("expires_at") or 0.0)
        if not force_refresh and cached and now < expires_at:
            return str(cached)

    status, _hdrs, html_bytes = _fetch_https_bytes(FASTCOM_HOST, "/", timeout=8.0)
    if status < 200 or status >= 300:
        raise RuntimeError(f"Fast.com landing page returned HTTP {status}")
    html = _decode_text(html_bytes)

    script_match = re.search(r'src=["\']([^"\']*app-[^"\']+\.js)["\']', html)
    if not script_match:
        raise RuntimeError("Could not locate Fast.com script bundle")
    js_path = script_match.group(1)
    if js_path.startswith("http://") or js_path.startswith("https://"):
        parsed = urllib.parse.urlsplit(js_path)
        js_host = parsed.hostname or FASTCOM_HOST
        js_rel = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    else:
        js_host = FASTCOM_HOST
        js_rel = js_path if js_path.startswith("/") else f"/{js_path}"

    js_status, _js_hdrs, js_bytes = _fetch_https_bytes(js_host, js_rel, timeout=8.0)
    if js_status < 200 or js_status >= 300:
        raise RuntimeError(f"Fast.com script returned HTTP {js_status}")
    js_text = _decode_text(js_bytes)

    tok_match = re.search(r'token["\']?\s*[:=]\s*["\']([a-zA-Z0-9_-]{16,})["\']', js_text)
    if not tok_match:
        raise RuntimeError("Could not extract Fast.com API token")
    token = tok_match.group(1)

    with _FASTCOM_TOKEN_LOCK:
        _FASTCOM_TOKEN_CACHE["token"] = token
        _FASTCOM_TOKEN_CACHE["expires_at"] = time.time() + _FASTCOM_TOKEN_TTL_SEC

    return token


def _discover_fastcom_targets(url_count: int = 5) -> Tuple[List[Dict[str, Any]], str]:
    token = _get_fastcom_token(force_refresh=False)
    for attempt in range(2):
        api_path = f"/netflix/speedtest/v2?https=true&token={urllib.parse.quote(token)}&urlCount={int(url_count)}"
        status, _hdrs, raw = _fetch_https_bytes(FASTCOM_API_HOST, api_path, timeout=8.0)
        if status in (401, 403) and attempt == 0:
            token = _get_fastcom_token(force_refresh=True)
            continue
        if status < 200 or status >= 300:
            raise RuntimeError(f"Fast.com API returned HTTP {status}")

        payload = json.loads(_decode_text(raw))
        targets = payload.get("targets") or []
        if not targets:
            raise RuntimeError("Fast.com API returned zero CDN targets")

        client_isp = str((payload.get("client") or {}).get("isp") or "").strip()
        first_loc = targets[0].get("location") or {}
        raw_city = str(first_loc.get("city") or "").strip()
        city = raw_city.title() if raw_city.isupper() else raw_city
        if city and client_isp:
            server_location = f"Fast.com {city} ({client_isp})"
        elif city:
            server_location = f"Fast.com {city}"
        elif client_isp:
            server_location = f"Fast.com ({client_isp})"
        else:
            server_location = "Fast.com Open Connect"

        parsed_targets: List[Dict[str, Any]] = []
        for t in targets:
            raw_url = str(t.get("url") or "")
            if not raw_url:
                continue
            sp = urllib.parse.urlsplit(raw_url)
            if not sp.hostname:
                continue
            parsed_targets.append(
                {
                    "host": sp.hostname,
                    "base_path": sp.path,
                    "query": sp.query,
                }
            )
        if not parsed_targets:
            raise RuntimeError("Fast.com targets missing valid hostnames")

        return parsed_targets, server_location

    raise RuntimeError("Fast.com API authentication failed")


def _measure_keepalive_latency(
    conn: http.client.HTTPSConnection,
    path: str,
    headers: Dict[str, str],
    subtract_cf_server_timing: bool = False,
) -> Tuple[float, float, List[Tuple[str, str]]]:
    """Send 1 warmup + 3 timed 0-byte probes over a single warm keep-alive connection."""
    req_hdrs = {**headers, "Cache-Control": "no-cache", "Connection": "keep-alive"}
    conn.request("GET", path, headers=req_hdrs)
    warm_resp = conn.getresponse()
    warm_resp.read()
    if warm_resp.status < 200 or warm_resp.status >= 300:
        raise RuntimeError(f"Latency probe returned HTTP {warm_resp.status}")
    warm_headers = _iter_headers(warm_resp.getheaders())

    rtts_ms: List[float] = []
    for _ in range(3):
        t0 = time.perf_counter()
        conn.request("GET", path, headers=req_hdrs)
        resp = conn.getresponse()
        resp.read()
        t1 = time.perf_counter()
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Latency probe returned HTTP {resp.status}")
        raw_ms = (t1 - t0) * 1000.0
        if subtract_cf_server_timing:
            resp_hdrs = _iter_headers(resp.getheaders())
            server_ms, l4_rtt_ms = _parse_cf_server_timing_ms(resp_hdrs)
            if server_ms > 0.0:
                net_ms = max(1.0, raw_ms - server_ms)
            elif l4_rtt_ms is not None and l4_rtt_ms > 0.0:
                net_ms = min(raw_ms, l4_rtt_ms)
            else:
                net_ms = raw_ms
            rtts_ms.append(net_ms)
        else:
            rtts_ms.append(raw_ms)

    ping_ms = round(statistics.median(rtts_ms), 2)
    if len(rtts_ms) >= 2:
        diffs = [abs(rtts_ms[i] - rtts_ms[i - 1]) for i in range(1, len(rtts_ms))]
        jitter_ms = round(sum(diffs) / len(diffs), 2)
    else:
        jitter_ms = 0.0

    return ping_ms, jitter_ms, warm_headers


def _run_timeboxed_download(
    endpoints: List[Tuple[str, str, Dict[str, str]]],
    cfg: SpeedtestModeConfig,
) -> Tuple[float, int]:
    """
    Download across `cfg.dl_streams` keep-alive workers for ~1.5-2.5s (early-stopping
    once 4 consecutive 200ms buckets stabilize within 12%), returning (steady_mbps, bytes).
    """
    if not endpoints:
        raise RuntimeError("No download endpoints supplied")

    lock = threading.Lock()
    stop_event = threading.Event()
    state: Dict[str, Any] = {
        "anchor_ts": None,
        "total_bytes": 0,
        "chunk_events": [],
        "buckets": {},
        "errors": [],
    }

    def _worker(worker_idx: int) -> None:
        host, path, base_hdrs = endpoints[worker_idx % len(endpoints)]
        conn: Optional[http.client.HTTPSConnection] = None
        req_hdrs = {**base_hdrs, "Cache-Control": "no-cache", "Connection": "keep-alive"}
        consecutive_resets = 0
        t_worker_start = time.perf_counter()
        try:
            while not stop_event.is_set():
                if (time.perf_counter() - t_worker_start) > (cfg.dl_max_sec + 5.0):
                    break
                if conn is None:
                    conn = _make_connection(host=host, timeout=max(6.0, cfg.dl_max_sec + 3.0))
                try:
                    conn.request("GET", path, headers=req_hdrs)
                    resp = conn.getresponse()
                except (http.client.HTTPException, ConnectionResetError, BrokenPipeError):
                    if stop_event.is_set():
                        break
                    consecutive_resets += 1
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    if consecutive_resets > 2:
                        raise
                    continue

                if resp.status < 200 or resp.status >= 300:
                    try:
                        resp.read()
                    except Exception:
                        pass
                    raise RuntimeError(f"Download endpoint returned HTTP {resp.status}")

                t_hdr = time.perf_counter()
                with lock:
                    if state["anchor_ts"] is None:
                        state["anchor_ts"] = t_hdr
                    state["chunk_events"].append((t_hdr, 0))

                aborted_mid_stream = False
                while not stop_event.is_set():
                    chunk = resp.read(READ_CHUNK_SIZE)
                    if not chunk:
                        break
                    consecutive_resets = 0
                    now = time.perf_counter()
                    n = len(chunk)
                    with lock:
                        if state["anchor_ts"] is None:
                            state["anchor_ts"] = now
                        anchor = state["anchor_ts"]
                        state["chunk_events"].append((now, n))
                        state["total_bytes"] += n
                        elapsed = now - anchor
                        b_idx = max(0, int(elapsed / 0.2))
                        state["buckets"][b_idx] = state["buckets"].get(b_idx, 0) + n

                        if state["total_bytes"] >= cfg.dl_max_bytes or elapsed >= cfg.dl_max_sec:
                            stop_event.set()
                        elif elapsed >= cfg.dl_min_steady_sec and b_idx >= 5:
                            last4 = [
                                state["buckets"].get(i, 0) for i in range(b_idx - 4, b_idx)
                            ]
                            if all(val > 0 for val in last4):
                                mean_val = sum(last4) / 4.0
                                if ((max(last4) - min(last4)) / mean_val) <= 0.12:
                                    stop_event.set()

                    if stop_event.is_set():
                        aborted_mid_stream = True
                        break

                if aborted_mid_stream or stop_event.is_set():
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    break
        except Exception as exc:
            with lock:
                state["errors"].append(exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    with ThreadPoolExecutor(max_workers=cfg.dl_streams) as pool:
        futures = [pool.submit(_worker, i) for i in range(cfg.dl_streams)]
        for f in futures:
            f.result()

    if state["total_bytes"] <= 0:
        if state["errors"]:
            raise state["errors"][0]
        raise RuntimeError("Download received 0 bytes")

    chunk_events = state["chunk_events"]
    span = (
        max(0.01, chunk_events[-1][0] - chunk_events[0][0])
        if len(chunk_events) >= 2
        else 0.01
    )
    mbps = _compute_steady_mbps(chunk_events, fallback_sec=span)
    return mbps, int(state["total_bytes"])


def _upload_stream(
    num_bytes: int,
    hard_timeout: float,
    host: str = CLOUDFLARE_HOST,
    path: str = "/__up",
    extra_headers: Optional[Dict[str, str]] = None,
    conn: Optional[http.client.HTTPSConnection] = None,
) -> Tuple[int, float, bool]:
    """Upload `num_bytes` to an HTTP POST endpoint and include server ACK in timing."""
    owns_conn = conn is None
    if conn is None:
        conn = _make_connection(host=host, timeout=max(6.0, hard_timeout + 2.0))

    target = int(num_bytes)
    t_start = time.perf_counter()
    sent = 0
    hdrs = {**BROWSER_HEADERS, **(extra_headers or {})}

    try:
        conn.putrequest("POST", path)
        for hk, hv in hdrs.items():
            conn.putheader(hk, hv)
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


def _run_adaptive_upload(
    endpoints: List[Tuple[str, str, Dict[str, str]]],
    cfg: SpeedtestModeConfig,
    ping_ms: float,
) -> Tuple[float, int]:
    """Warm up socket with 512 KB, then send sized upload burst on warm keep-alive socket(s)."""
    host, path, extra_hdrs = endpoints[0]
    rtt_sec = max(0.0, ping_ms / 1000.0)
    up_conn = _make_connection(host=host, timeout=max(6.0, cfg.ul_hard_timeout + 2.0))
    total_sent = 0

    try:
        cal_ul_bytes = 524_288  # 512 KB warmup opens TCP cwnd
        cal_sent, cal_elapsed, _ = _upload_stream(
            cal_ul_bytes,
            cfg.ul_hard_timeout,
            host=host,
            path=path,
            extra_headers=extra_hdrs,
            conn=up_conn,
        )
        total_sent += cal_sent

        cal_net = max(0.015, cal_elapsed - min(rtt_sec, cal_elapsed * 0.5))
        if cal_elapsed >= 1.5:
            return (cal_sent * 8.0) / (cal_net * 1_000_000.0), total_sent

        est_up_bps = cal_sent / cal_net
        target_ul = int(est_up_bps * cfg.ul_target_sec)
        target_ul = max(cfg.ul_min_bytes, min(cfg.ul_max_bytes, target_ul))

        if cfg.ul_streams <= 1:
            try:
                up_sent, up_elapsed, _ = _upload_stream(
                    target_ul,
                    cfg.ul_hard_timeout,
                    host=host,
                    path=path,
                    extra_headers=extra_hdrs,
                    conn=up_conn,
                )
            except (http.client.HTTPException, ConnectionResetError, BrokenPipeError):
                try:
                    up_conn.close()
                except Exception:
                    pass
                up_conn = _make_connection(host=host, timeout=max(6.0, cfg.ul_hard_timeout + 2.0))
                up_sent, up_elapsed, _ = _upload_stream(
                    target_ul,
                    cfg.ul_hard_timeout,
                    host=host,
                    path=path,
                    extra_headers=extra_hdrs,
                    conn=up_conn,
                )
            total_sent += up_sent
            up_net = max(0.02, up_elapsed - min(rtt_sec, up_elapsed * 0.5))
            return (up_sent * 8.0) / (up_net * 1_000_000.0), total_sent

        try:
            up_conn.close()
        except Exception:
            pass
        up_conn = None

        per_stream = max(cfg.ul_min_bytes // cfg.ul_streams, target_ul // cfg.ul_streams)
        t_start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=cfg.ul_streams) as pool:
            futures = []
            for idx in range(cfg.ul_streams):
                ehost, epath, ehdrs = endpoints[idx % len(endpoints)]
                futures.append(
                    pool.submit(_upload_stream, per_stream, cfg.ul_hard_timeout, ehost, epath, ehdrs, None)
                )
            up_results = [f.result() for f in futures]
        wall = max(0.02, time.perf_counter() - t_start)
        multi_sent = sum(r[0] for r in up_results)
        total_sent += multi_sent
        up_net = max(0.02, wall - min(rtt_sec, wall * 0.5))
        return (multi_sent * 8.0) / (up_net * 1_000_000.0), total_sent
    finally:
        if up_conn is not None:
            try:
                up_conn.close()
            except Exception:
                pass


def run_fastcom_speedtest(
    mode: str = "eco",
    trigger: str = "scheduled",
    cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
) -> Dict[str, Any]:
    """
    Execute a time-boxed steady-state speedtest against Netflix Open Connect (Fast.com).
    ISP-peered CDN servers deliver accurate 1 Gbps readings without WAF/rate-limit 403s.
    """
    mode_key = mode if mode in SPEEDTEST_MODES else "eco"
    cfg = SPEEDTEST_MODES[mode_key]
    t_overall_start = time.perf_counter()
    total_bytes = 0
    ping_conn: Optional[http.client.HTTPSConnection] = None

    try:
        targets, server_location = _discover_fastcom_targets(url_count=max(3, cfg.dl_streams))
        t0_host = targets[0]["host"]
        t0_range0 = (
            targets[0]["base_path"].replace("/speedtest", "/speedtest/range/0-0")
            + (f"?{targets[0]['query']}" if targets[0]["query"] else "")
        )

        ping_conn = _make_connection(host=t0_host, timeout=8.0)
        ping_ms, jitter_ms, _ = _measure_keepalive_latency(
            ping_conn, t0_range0, BROWSER_HEADERS, subtract_cf_server_timing=False
        )
        try:
            ping_conn.close()
        except Exception:
            pass
        ping_conn = None

        dl_endpoints: List[Tuple[str, str, Dict[str, str]]] = []
        ul_endpoints: List[Tuple[str, str, Dict[str, str]]] = []
        for t in targets:
            q = f"?{t['query']}" if t["query"] else ""
            dl_path = t["base_path"].replace("/speedtest", "/speedtest/range/0-26214400") + q
            ul_path = t["base_path"].replace("/speedtest", "/speedtest/range/0-0") + q
            dl_endpoints.append((t["host"], dl_path, BROWSER_HEADERS))
            ul_endpoints.append((t["host"], ul_path, BROWSER_HEADERS))

        dl_mbps, dl_bytes = _run_timeboxed_download(dl_endpoints, cfg)
        total_bytes += dl_bytes

        ul_mbps: Optional[float] = None
        if cfg.run_upload:
            if cooldown_sec > 0:
                time.sleep(cooldown_sec)
            ul_mbps, ul_bytes = _run_adaptive_upload(ul_endpoints, cfg, ping_ms)
            total_bytes += ul_bytes

        return {
            "timestamp": int(time.time()),
            "download_mbps": round(dl_mbps, 2),
            "upload_mbps": round(ul_mbps, 2) if ul_mbps is not None else None,
            "ping_ms": round(ping_ms, 2),
            "jitter_ms": round(jitter_ms, 2),
            "bytes_used": int(total_bytes),
            "duration_sec": round(max(0.01, time.perf_counter() - t_overall_start), 2),
            "mode": mode_key,
            "server_location": server_location,
            "trigger": trigger,
            "status": "ok",
            "error_message": None,
        }
    except Exception as exc:
        logger.warning("Fast.com speedtest failed (mode=%s, trigger=%s): %s", mode_key, trigger, exc)
        return {
            "timestamp": int(time.time()),
            "download_mbps": None,
            "upload_mbps": None,
            "ping_ms": None,
            "jitter_ms": None,
            "bytes_used": int(total_bytes),
            "duration_sec": round(max(0.01, time.perf_counter() - t_overall_start), 2),
            "mode": mode_key,
            "server_location": "Fast.com Open Connect",
            "trigger": trigger,
            "status": "error",
            "error_message": str(exc),
        }
    finally:
        if ping_conn is not None:
            try:
                ping_conn.close()
            except Exception:
                pass


def run_cloudflare_speedtest(
    mode: str = "eco",
    trigger: str = "scheduled",
    cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
) -> Dict[str, Any]:
    """
    Fallback steady-state speedtest against Cloudflare Edge using browser Referer + measId
    headers and Server-Timing RTT correction.
    """
    mode_key = mode if mode in SPEEDTEST_MODES else "eco"
    cfg = SPEEDTEST_MODES[mode_key]
    t_overall_start = time.perf_counter()
    total_bytes = 0
    ping_conn: Optional[http.client.HTTPSConnection] = None

    try:
        meas_id = str(random.randint(100_000_000, 999_999_999))
        ping_conn = _make_connection(host=CLOUDFLARE_HOST, timeout=8.0)
        ping_ms, jitter_ms, warm_hdrs = _measure_keepalive_latency(
            ping_conn,
            f"/__down?measId={meas_id}&bytes=0",
            CLOUDFLARE_HEADERS,
            subtract_cf_server_timing=True,
        )
        server_location = _parse_server_location(warm_hdrs)
        try:
            ping_conn.close()
        except Exception:
            pass
        ping_conn = None

        dl_endpoints = [
            (CLOUDFLARE_HOST, f"/__down?measId={meas_id}&bytes=100000000", CLOUDFLARE_HEADERS)
            for _ in range(cfg.dl_streams)
        ]
        dl_mbps, dl_bytes = _run_timeboxed_download(dl_endpoints, cfg)
        total_bytes += dl_bytes

        ul_mbps: Optional[float] = None
        if cfg.run_upload:
            if cooldown_sec > 0:
                time.sleep(cooldown_sec)
            ul_endpoints = [
                (CLOUDFLARE_HOST, f"/__up?measId={meas_id}", CLOUDFLARE_HEADERS)
                for _ in range(max(1, cfg.ul_streams))
            ]
            ul_mbps, ul_bytes = _run_adaptive_upload(ul_endpoints, cfg, ping_ms)
            total_bytes += ul_bytes

        return {
            "timestamp": int(time.time()),
            "download_mbps": round(dl_mbps, 2),
            "upload_mbps": round(ul_mbps, 2) if ul_mbps is not None else None,
            "ping_ms": round(ping_ms, 2),
            "jitter_ms": round(jitter_ms, 2),
            "bytes_used": int(total_bytes),
            "duration_sec": round(max(0.01, time.perf_counter() - t_overall_start), 2),
            "mode": mode_key,
            "server_location": server_location,
            "trigger": trigger,
            "status": "ok",
            "error_message": None,
        }
    except Exception as exc:
        logger.warning("Cloudflare speedtest failed (mode=%s, trigger=%s): %s", mode_key, trigger, exc)
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
        if ping_conn is not None:
            try:
                ping_conn.close()
            except Exception:
                pass


def run_speedtest(
    mode: str = "eco",
    trigger: str = "scheduled",
    cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
) -> Dict[str, Any]:
    """Run Fast.com Open Connect speedtest first, automatically falling back to Cloudflare Edge."""
    res = run_fastcom_speedtest(mode=mode, trigger=trigger, cooldown_sec=cooldown_sec)
    if res.get("status") == "ok":
        return res

    fast_err = res.get("error_message") or "unknown error"
    logger.warning("Fast.com speedtest failed (%s), falling back to Cloudflare Edge", fast_err)
    cf_res = run_cloudflare_speedtest(mode=mode, trigger=trigger, cooldown_sec=cooldown_sec)
    if cf_res.get("status") == "ok":
        return cf_res

    cf_err = cf_res.get("error_message") or "unknown error"
    return {
        **cf_res,
        "bytes_used": int((res.get("bytes_used") or 0) + (cf_res.get("bytes_used") or 0)),
        "duration_sec": round(
            float(res.get("duration_sec") or 0.0) + float(cf_res.get("duration_sec") or 0.0),
            2,
        ),
        "error_message": f"Fast.com: {fast_err}; Cloudflare: {cf_err}",
    }


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
                    run_speedtest,
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

                delay = max(
                    0.0,
                    float((target_ts if target_ts is not None else int(time.time())) - time.time()),
                )
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
