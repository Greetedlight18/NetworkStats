import asyncio
import functools
import ipaddress
import json
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("NetworkStats.database")

DB_PATH = os.getenv("DB_PATH", "/data/networkstats.db")
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "48"))
LOSS_RETENTION_DAYS = int(os.getenv("LOSS_RETENTION_DAYS", "365"))
DEFAULT_INTERVAL = float(os.getenv("PROBE_INTERVAL", "1.0"))

VALID_SPEEDTEST_MODES = ("eco", "eco_download_only", "balanced")
DEFAULT_SPEEDTEST_ENABLED = (
    os.getenv("SPEEDTEST_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
)
DEFAULT_SPEEDTEST_INTERVAL_MIN = max(5, min(1440, int(os.getenv("SPEEDTEST_INTERVAL_MIN", "60"))))
_RAW_SPEEDTEST_MODE = os.getenv("SPEEDTEST_MODE", "eco").strip().lower()
DEFAULT_SPEEDTEST_MODE = _RAW_SPEEDTEST_MODE if _RAW_SPEEDTEST_MODE in VALID_SPEEDTEST_MODES else "eco"

DEFAULT_TARGETS = [
    {"host": "10.0.0.1", "label": "Local Gateway", "interval_sec": DEFAULT_INTERVAL},
    {"host": "1.1.1.1", "label": "Cloudflare DNS", "interval_sec": DEFAULT_INTERVAL},
    {"host": "8.8.8.8", "label": "Google DNS", "interval_sec": DEFAULT_INTERVAL},
    {"host": "9.9.9.9", "label": "Quad9 DNS", "interval_sec": DEFAULT_INTERVAL},
]

_write_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-writer")
_read_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="db-reader")
_thread_local = threading.local()
_conn_generation = 0


def close_thread_connections() -> None:
    """Invalidate all cached thread-local SQLite connections (e.g. when DB_PATH changes)."""
    global _conn_generation
    _conn_generation += 1
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.conn = None


async def run_db_write(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a serialized SQLite write operation off the asyncio event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_write_executor, functools.partial(fn, *args, **kwargs))


async def run_db_read(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Run a SQLite read operation off the asyncio event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_read_executor, functools.partial(fn, *args, **kwargs))


def _is_private_ip(ip_str: str) -> bool:
    try:
        return ipaddress.ip_address(ip_str).is_private
    except Exception:
        return False


@contextmanager
def get_db_connection():
    db_path = DB_PATH
    gen = _conn_generation
    conn = getattr(_thread_local, "conn", None)
    if conn is not None and (
        getattr(_thread_local, "db_path", None) != db_path
        or getattr(_thread_local, "gen", None) != gen
    ):
        try:
            conn.close()
        except Exception:
            pass
        conn = None
        _thread_local.conn = None

    if conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=15000;")
        conn.execute("PRAGMA wal_autocheckpoint=2000;")
        _thread_local.conn = conn
        _thread_local.db_path = db_path
        _thread_local.gen = gen

    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            _thread_local.conn = None
        raise


def _migrate_to_v2(conn: sqlite3.Connection) -> None:
    """One-time migration: clean legacy 10.x intermediate hops and backfill loss_events."""
    # 1. Clean bad intermediate hops from small hop_state table only (never full-scan DELETE hop_metrics)
    conn.execute(
        """
        DELETE FROM hop_state
        WHERE ip_address = '???'
           OR (hop_num > 1 AND ip_address LIKE '10.%' AND ip_address != '10.0.0.1')
        """
    )

    t_rows = conn.execute("SELECT id, host, label FROM targets ORDER BY id ASC").fetchall()
    target_max_hop: Dict[int, int] = {}
    target_meta: Dict[int, Dict[str, str]] = {}

    for tr in t_rows:
        t_id = tr["id"]
        target_meta[t_id] = {"host": tr["host"], "label": tr["label"]}
        hops = conn.execute(
            "SELECT * FROM hop_state WHERE target_id = ? ORDER BY hop_num ASC",
            (t_id,),
        ).fetchall()
        if not hops:
            continue

        # Renumber via temporary negative hop_num to guarantee zero PK collision
        max_old_hop = hops[-1]["hop_num"]
        new_max_hop = len(hops)
        target_max_hop[t_id] = new_max_hop

        for new_idx, h in enumerate(hops):
            conn.execute(
                "UPDATE hop_state SET hop_num = ? WHERE target_id = ? AND hop_num = ?",
                (-(new_idx + 1), t_id, h["hop_num"]),
            )

        for new_idx, h in enumerate(hops):
            new_hop_num = new_idx + 1
            is_final = h["hop_num"] == max_old_hop
            if is_final:
                stat = conn.execute(
                    """
                    SELECT COUNT(*) AS total_sent, COALESCE(SUM(is_loss), 0) AS total_lost
                    FROM hop_metrics
                    WHERE target_id = ? AND hop_num >= ?
                      AND ip_address != '???'
                      AND NOT (hop_num > 1 AND ip_address LIKE '10.%' AND ip_address != '10.0.0.1')
                    """,
                    (t_id, new_hop_num),
                ).fetchone()
            else:
                stat = conn.execute(
                    """
                    SELECT COUNT(*) AS total_sent, COALESCE(SUM(is_loss), 0) AS total_lost
                    FROM hop_metrics
                    WHERE target_id = ? AND ip_address = ?
                    """,
                    (t_id, h["ip_address"]),
                ).fetchone()

            s_cnt = int(stat["total_sent"]) if (stat and stat["total_sent"]) else int(h["sent"])
            l_cnt = int(stat["total_lost"]) if (stat and stat["total_sent"]) else int(h["lost"])
            l_pct = round((l_cnt / s_cnt) * 100.0, 2) if s_cnt > 0 else 0.0

            conn.execute(
                """
                UPDATE hop_state
                SET hop_num = ?, sent = ?, lost = ?, loss_pct = ?
                WHERE target_id = ? AND hop_num = ?
                """,
                (new_hop_num, s_cnt, l_cnt, l_pct, t_id, -new_hop_num),
            )

    # 2. Backfill last 48h of final-hop losses into permanent loss_events using idx_hop_metrics_loss_ts
    existing_loss_cnt = conn.execute("SELECT COUNT(*) AS cnt FROM loss_events").fetchone()["cnt"]
    if existing_loss_cnt == 0 and target_meta:
        since_ts = int(time.time()) - 172800
        loss_rows = conn.execute(
            """
            SELECT target_id, hop_num, ip_address, timestamp
            FROM hop_metrics
            WHERE is_loss = 1 AND timestamp >= ?
            ORDER BY target_id ASC, timestamp ASC
            """,
            (since_ts,),
        ).fetchall()

        upstream_by_target_ts: Dict[int, Dict[int, List[int]]] = {}
        final_ts_by_target: Dict[int, List[int]] = {}

        for r in loss_rows:
            t_id = r["target_id"]
            if t_id not in target_meta:
                continue
            ip = r["ip_address"]
            hop_num = int(r["hop_num"])
            if ip == "???" or (hop_num > 1 and ip.startswith("10.") and ip != "10.0.0.1"):
                continue

            max_hop = target_max_hop.get(t_id, 1)
            ts = int(r["timestamp"])
            if hop_num in (1, 2) and hop_num < max_hop:
                upstream_by_target_ts.setdefault(t_id, {}).setdefault(ts, []).append(hop_num)
            if hop_num >= max_hop:
                ts_list = final_ts_by_target.setdefault(t_id, [])
                if not ts_list or ts_list[-1] != ts:
                    ts_list.append(ts)

        for t_id, timestamps in final_ts_by_target.items():
            meta = target_meta[t_id]
            up_map = upstream_by_target_ts.get(t_id, {})
            cur_ev: Optional[Dict[str, Any]] = None
            for ts in timestamps:
                up_hops = up_map.get(ts, []) + up_map.get(ts - 1, []) + up_map.get(ts + 1, [])
                if 1 in up_hops:
                    cause = "Local Router (10.0.0.1) dropped"
                    severity = "router"
                elif 2 in up_hops:
                    cause = "ISP Modem (Hop #2) dropped"
                    severity = "isp"
                else:
                    cause = "Target / Internet Route dropped (Router & Modem OK)"
                    severity = "target"

                if cur_ev and ts - cur_ev["end_ts"] <= 4:
                    cur_ev["end_ts"] = ts
                    cur_ev["packets_lost"] += 1
                    if severity in ("router", "isp") and cur_ev["severity"] == "target":
                        cur_ev["cause"] = cause
                        cur_ev["severity"] = severity
                else:
                    if cur_ev:
                        conn.execute(
                            """
                            INSERT INTO loss_events (
                                target_id, target_label, target_host,
                                start_ts, end_ts, packets_lost, cause, severity
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                t_id,
                                meta["label"],
                                meta["host"],
                                cur_ev["start_ts"],
                                cur_ev["end_ts"],
                                cur_ev["packets_lost"],
                                cur_ev["cause"],
                                cur_ev["severity"],
                            ),
                        )
                    cur_ev = {
                        "start_ts": ts,
                        "end_ts": ts,
                        "packets_lost": 1,
                        "cause": cause,
                        "severity": severity,
                    }
            if cur_ev:
                conn.execute(
                    """
                    INSERT INTO loss_events (
                        target_id, target_label, target_host,
                        start_ts, end_ts, packets_lost, cause, severity
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        t_id,
                        meta["label"],
                        meta["host"],
                        cur_ev["start_ts"],
                        cur_ev["end_ts"],
                        cur_ev["packets_lost"],
                        cur_ev["cause"],
                        cur_ev["severity"],
                    ),
                )


def init_db() -> None:
    """Initialize SQLite schema, indexes, default targets, and run one-time v2 migration."""
    with get_db_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL,
                interval_sec REAL NOT NULL DEFAULT 1.0,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS hop_state (
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
                PRIMARY KEY (target_id, hop_num),
                FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS hop_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER NOT NULL,
                hop_num INTEGER NOT NULL,
                ip_address TEXT NOT NULL,
                rdns_hostname TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                latency_ms REAL,
                is_loss INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS loss_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_id INTEGER NOT NULL,
                target_label TEXT NOT NULL,
                target_host TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                packets_lost INTEGER NOT NULL DEFAULT 1,
                cause TEXT NOT NULL,
                severity TEXT NOT NULL,
                FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS network_outages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                duration_sec INTEGER NOT NULL DEFAULT 1,
                affected_targets INTEGER NOT NULL DEFAULT 0,
                total_external_targets INTEGER NOT NULL DEFAULT 0,
                router_reachable INTEGER NOT NULL DEFAULT 1,
                affected_labels TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS speedtest_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                download_mbps REAL,
                upload_mbps REAL,
                ping_ms REAL,
                jitter_ms REAL,
                bytes_used INTEGER NOT NULL DEFAULT 0,
                duration_sec REAL NOT NULL DEFAULT 0.0,
                mode TEXT NOT NULL DEFAULT 'eco',
                server_location TEXT NOT NULL DEFAULT '',
                trigger TEXT NOT NULL DEFAULT 'scheduled',
                status TEXT NOT NULL DEFAULT 'ok',
                error_message TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_target_ts
                ON hop_metrics(target_id, timestamp);

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_target_hop_ts
                ON hop_metrics(target_id, hop_num, timestamp);

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_loss_ts
                ON hop_metrics(is_loss, timestamp);

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_ts
                ON hop_metrics(timestamp);

            CREATE INDEX IF NOT EXISTS idx_loss_events_start
                ON loss_events(start_ts DESC);

            CREATE INDEX IF NOT EXISTS idx_loss_events_target_start
                ON loss_events(target_id, start_ts DESC);

            CREATE INDEX IF NOT EXISTS idx_network_outages_start
                ON network_outages(start_ts DESC);

            CREATE INDEX IF NOT EXISTS idx_speedtest_results_ts
                ON speedtest_results(timestamp DESC);
            """
        )

        cursor = conn.execute("SELECT COUNT(*) as cnt FROM targets")
        count = cursor.fetchone()["cnt"]
        if count == 0:
            now = int(time.time())
            for item in DEFAULT_TARGETS:
                conn.execute(
                    """
                    INSERT INTO targets (host, label, interval_sec, enabled, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (item["host"], item["label"], item["interval_sec"], now),
                )
            logger.info("Seeded %d default targets into %s", len(DEFAULT_TARGETS), DB_PATH)

        ver_row = conn.execute(
            "SELECT value FROM app_meta WHERE key = 'schema_version'"
        ).fetchone()
        current_ver = int(ver_row["value"]) if ver_row else 1
        if current_ver < 2:
            _migrate_to_v2(conn)
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('schema_version', '2')"
            )
            logger.info("Migrated database %s to schema_version=2", DB_PATH)


def list_targets() -> List[Dict[str, Any]]:
    """Return all targets along with their latest final-hop summary."""
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT id, host, label, interval_sec, enabled, created_at FROM targets ORDER BY id ASC"
        ).fetchall()

        result: List[Dict[str, Any]] = []
        for row in rows:
            t_id = row["id"]
            final_hop = conn.execute(
                """
                SELECT hop_num, ip_address, rdns_hostname, sent, lost, loss_pct,
                       rolling_loss_pct, last_ms, avg_ms, min_ms, max_ms, jitter_ms, updated_at
                FROM hop_state
                WHERE target_id = ?
                ORDER BY hop_num DESC
                LIMIT 1
                """,
                (t_id,),
            ).fetchone()

            total_loss_pct = round(float(final_hop["loss_pct"]), 2) if final_hop else 0.0
            rolling_loss_pct = round(float(final_hop["rolling_loss_pct"]), 1) if final_hop else 0.0
            lost_count = int(final_hop["lost"]) if final_hop else 0

            result.append(
                {
                    "id": t_id,
                    "host": row["host"],
                    "label": row["label"],
                    "interval_sec": row["interval_sec"],
                    "enabled": bool(row["enabled"]),
                    "created_at": row["created_at"],
                    "current_ms": final_hop["last_ms"] if final_hop else None,
                    "avg_ms": final_hop["avg_ms"] if final_hop else None,
                    "jitter_ms": final_hop["jitter_ms"] if final_hop else 0.0,
                    "rolling_loss_pct": rolling_loss_pct,
                    "loss_pct": total_loss_pct,
                    "loss_1h_pct": total_loss_pct,
                    "lost_count": lost_count,
                    "hop_count": final_hop["hop_num"] if final_hop else 0,
                    "updated_at": final_hop["updated_at"] if final_hop else None,
                }
            )
        return result


def get_target(target_id: int) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT id, host, label, interval_sec, enabled, created_at FROM targets WHERE id = ?",
            (target_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "host": row["host"],
            "label": row["label"],
            "interval_sec": row["interval_sec"],
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
        }


def add_target(host: str, label: str, interval_sec: float = 1.0) -> Dict[str, Any]:
    now = int(time.time())
    clean_host = host.strip()
    clean_label = label.strip() or clean_host
    interval_sec = max(0.5, min(60.0, float(interval_sec)))
    with get_db_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO targets (host, label, interval_sec, enabled, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (clean_host, clean_label, interval_sec, now),
        )
        new_id = cur.lastrowid
    return get_target(new_id)


def update_target(
    target_id: int,
    label: Optional[str] = None,
    interval_sec: Optional[float] = None,
    enabled: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    existing = get_target(target_id)
    if not existing:
        return None
    new_label = label.strip() if label is not None else existing["label"]
    new_interval = (
        max(0.5, min(60.0, float(interval_sec)))
        if interval_sec is not None
        else existing["interval_sec"]
    )
    new_enabled = (1 if enabled else 0) if enabled is not None else (1 if existing["enabled"] else 0)

    with get_db_connection() as conn:
        conn.execute(
            """
            UPDATE targets
            SET label = ?, interval_sec = ?, enabled = ?
            WHERE id = ?
            """,
            (new_label, new_interval, new_enabled, target_id),
        )
    return get_target(target_id)


def delete_target(target_id: int) -> bool:
    with get_db_connection() as conn:
        cur = conn.execute("DELETE FROM targets WHERE id = ?", (target_id,))
        return cur.rowcount > 0


def reset_target_stats(target_id: int) -> None:
    with get_db_connection() as conn:
        conn.execute("DELETE FROM hop_state WHERE target_id = ?", (target_id,))
        conn.execute("DELETE FROM hop_metrics WHERE target_id = ?", (target_id,))
        conn.execute("DELETE FROM loss_events WHERE target_id = ?", (target_id,))


def clear_loss_events(target_id: Optional[int] = None) -> None:
    with get_db_connection() as conn:
        if target_id is not None:
            conn.execute("DELETE FROM loss_events WHERE target_id = ?", (target_id,))
        else:
            conn.execute("DELETE FROM loss_events")
            conn.execute("DELETE FROM network_outages")


def get_hop_states(target_id: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT hop_num, ip_address, rdns_hostname, sent, lost, loss_pct,
                   rolling_loss_pct, last_ms, avg_ms, min_ms, max_ms, jitter_ms,
                   sparkline_json, updated_at
            FROM hop_state
            WHERE target_id = ?
            ORDER BY hop_num ASC
            """,
            (target_id,),
        ).fetchall()
        hops: List[Dict[str, Any]] = []
        for r in rows:
            try:
                sparkline = json.loads(r["sparkline_json"])
            except Exception:
                sparkline = []
            hops.append(
                {
                    "hop_num": r["hop_num"],
                    "ip_address": r["ip_address"],
                    "rdns_hostname": r["rdns_hostname"],
                    "sent": r["sent"],
                    "lost": r["lost"],
                    "loss_pct": r["loss_pct"],
                    "rolling_loss_pct": r["rolling_loss_pct"],
                    "last_ms": r["last_ms"],
                    "avg_ms": r["avg_ms"],
                    "min_ms": r["min_ms"],
                    "max_ms": r["max_ms"],
                    "jitter_ms": r["jitter_ms"],
                    "sparkline": sparkline,
                    "updated_at": r["updated_at"],
                }
            )
        return hops


def save_probe_results(
    target_id: int,
    timestamp: int,
    hop_states: List[Dict[str, Any]],
    raw_samples: List[Dict[str, Any]],
    target_label: str = "",
    target_host: str = "",
) -> bool:
    """
    Persist updated hop state rows, append time-series probe samples, and
    immediately record/extend a permanent `loss_events` row whenever the final
    destination drops a packet. Returns True if a destination loss was recorded.
    """
    max_hop = max((h["hop_num"] for h in hop_states), default=0)
    loss_recorded = False

    with get_db_connection() as conn:
        if max_hop > 0:
            conn.execute(
                "DELETE FROM hop_state WHERE target_id = ? AND hop_num > ?",
                (target_id, max_hop),
            )

        for h in hop_states:
            conn.execute(
                """
                INSERT INTO hop_state (
                    target_id, hop_num, ip_address, rdns_hostname,
                    sent, lost, loss_pct, rolling_loss_pct,
                    last_ms, avg_ms, min_ms, max_ms, jitter_ms,
                    sparkline_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(target_id, hop_num) DO UPDATE SET
                    ip_address = excluded.ip_address,
                    rdns_hostname = excluded.rdns_hostname,
                    sent = excluded.sent,
                    lost = excluded.lost,
                    loss_pct = excluded.loss_pct,
                    rolling_loss_pct = excluded.rolling_loss_pct,
                    last_ms = excluded.last_ms,
                    avg_ms = excluded.avg_ms,
                    min_ms = excluded.min_ms,
                    max_ms = excluded.max_ms,
                    jitter_ms = excluded.jitter_ms,
                    sparkline_json = excluded.sparkline_json,
                    updated_at = excluded.updated_at
                """,
                (
                    target_id,
                    h["hop_num"],
                    h["ip_address"],
                    h["rdns_hostname"],
                    h["sent"],
                    h["lost"],
                    h["loss_pct"],
                    h["rolling_loss_pct"],
                    h["last_ms"],
                    h["avg_ms"],
                    h["min_ms"],
                    h["max_ms"],
                    h["jitter_ms"],
                    json.dumps(h["sparkline"]),
                    timestamp,
                ),
            )

        conn.executemany(
            """
            INSERT INTO hop_metrics (
                target_id, hop_num, ip_address, rdns_hostname,
                timestamp, latency_ms, is_loss
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    target_id,
                    s["hop_num"],
                    s["ip_address"],
                    s["rdns_hostname"],
                    timestamp,
                    s["latency_ms"],
                    1 if s["is_loss"] else 0,
                )
                for s in raw_samples
            ],
        )

        final_sample = raw_samples[-1] if raw_samples else None
        if final_sample and final_sample["is_loss"]:
            loss_recorded = True
            if len(raw_samples) > 1 and raw_samples[0]["is_loss"]:
                cause = f"Local Router ({raw_samples[0]['ip_address']}) dropped"
                severity = "router"
            elif len(raw_samples) > 2 and raw_samples[1]["is_loss"]:
                cause = f"ISP Modem ({raw_samples[1]['ip_address']}) dropped"
                severity = "isp"
            elif len(raw_samples) == 1:
                dest_disp = target_host or final_sample["ip_address"]
                cause = f"Target ({dest_disp}) unreachable"
                severity = "router" if _is_private_ip(final_sample["ip_address"]) else "target"
            else:
                cause = "Target / Internet Route dropped (Router & Modem OK)"
                severity = "target"

            if not target_label or not target_host:
                t_row = conn.execute(
                    "SELECT host, label FROM targets WHERE id = ?", (target_id,)
                ).fetchone()
                if t_row:
                    target_label = target_label or t_row["label"]
                    target_host = target_host or t_row["host"]

            prev = conn.execute(
                """
                SELECT id, end_ts, packets_lost, cause, severity
                FROM loss_events
                WHERE target_id = ?
                ORDER BY end_ts DESC, id DESC
                LIMIT 1
                """,
                (target_id,),
            ).fetchone()

            if prev and (timestamp - int(prev["end_ts"]) <= 4):
                new_end = max(int(prev["end_ts"]), timestamp)
                new_pkts = int(prev["packets_lost"]) + 1
                new_cause = prev["cause"]
                new_sev = prev["severity"]
                if severity in ("router", "isp") and prev["severity"] == "target":
                    new_cause = cause
                    new_sev = severity
                conn.execute(
                    """
                    UPDATE loss_events
                    SET end_ts = ?, packets_lost = ?, cause = ?, severity = ?
                    WHERE id = ?
                    """,
                    (new_end, new_pkts, new_cause, new_sev, prev["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO loss_events (
                        target_id, target_label, target_host,
                        start_ts, end_ts, packets_lost, cause, severity
                    ) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        target_id,
                        target_label or f"Target #{target_id}",
                        target_host or final_sample["ip_address"],
                        timestamp,
                        timestamp,
                        cause,
                        severity,
                    ),
                )

    return loss_recorded


def upsert_network_outage(
    timestamp: int,
    affected_targets: int,
    total_external_targets: int,
    router_reachable: bool,
    affected_labels: List[str],
) -> Dict[str, Any]:
    """Record or extend a network-wide internet outage incident."""
    clean_labels: List[str] = []
    for lbl in affected_labels:
        lbl = lbl.strip()
        if lbl and lbl not in clean_labels:
            clean_labels.append(lbl)

    with get_db_connection() as conn:
        prev = conn.execute(
            """
            SELECT id, start_ts, end_ts, duration_sec, affected_targets,
                   total_external_targets, router_reachable, affected_labels
            FROM network_outages
            ORDER BY end_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        if prev and (timestamp - int(prev["end_ts"]) <= 5):
            new_start = min(int(prev["start_ts"]), timestamp)
            new_end = max(int(prev["end_ts"]), timestamp)
            duration = max(1, new_end - new_start + 1)
            new_affected = max(int(prev["affected_targets"]), affected_targets)
            new_total = max(int(prev["total_external_targets"]), total_external_targets)
            new_router = 0 if (not router_reachable or int(prev["router_reachable"]) == 0) else 1

            merged_labels: List[str] = []
            for item in (prev["affected_labels"].split(",") if prev["affected_labels"] else []) + clean_labels:
                s = item.strip()
                if s and s not in merged_labels:
                    merged_labels.append(s)
            labels_str = ", ".join(merged_labels)

            conn.execute(
                """
                UPDATE network_outages
                SET start_ts = ?, end_ts = ?, duration_sec = ?,
                    affected_targets = ?, total_external_targets = ?,
                    router_reachable = ?, affected_labels = ?
                WHERE id = ?
                """,
                (
                    new_start,
                    new_end,
                    duration,
                    new_affected,
                    new_total,
                    new_router,
                    labels_str,
                    prev["id"],
                ),
            )
            outage_id = prev["id"]
        else:
            new_start = timestamp
            new_end = timestamp
            duration = 1
            new_affected = affected_targets
            new_total = total_external_targets
            new_router = 1 if router_reachable else 0
            labels_str = ", ".join(clean_labels)

            cur = conn.execute(
                """
                INSERT INTO network_outages (
                    start_ts, end_ts, duration_sec,
                    affected_targets, total_external_targets,
                    router_reachable, affected_labels
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_start,
                    new_end,
                    duration,
                    new_affected,
                    new_total,
                    new_router,
                    labels_str,
                ),
            )
            outage_id = cur.lastrowid

    return {
        "id": outage_id,
        "start_ts": new_start,
        "end_ts": new_end,
        "duration_sec": duration,
        "affected_targets": new_affected,
        "total_external_targets": new_total,
        "router_reachable": bool(new_router),
        "affected_labels": labels_str,
    }


def get_target_history(
    target_id: int,
    window_seconds: int = 300,
    hop_nums: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Retrieve time-series metrics for the requested target and time window.
    Includes avg, peak (max_ms), min_ms, and exact packet loss counts per bucket
    so latency spikes never shrink when switching to wider time windows.
    """
    now = int(time.time())
    since_ts = now - window_seconds

    if window_seconds <= 900:       # 5m, 15m: raw 1s samples
        bucket_sec = 1
    elif window_seconds <= 3600:    # 1h: 10s buckets
        bucket_sec = 10
    elif window_seconds <= 21600:   # 6h: 60s buckets
        bucket_sec = 60
    else:                           # 24h+: 240s buckets
        bucket_sec = 240

    with get_db_connection() as conn:
        hop_rows = conn.execute(
            "SELECT hop_num, ip_address FROM hop_state WHERE target_id = ? ORDER BY hop_num ASC",
            (target_id,),
        ).fetchall()
        if not hop_rows:
            return {
                "target_id": target_id,
                "window_seconds": window_seconds,
                "bucket_sec": bucket_sec,
                "server_now": now,
                "series": {},
            }

        max_hop_num = hop_rows[-1]["hop_num"]
        hop_ip_map = {r["hop_num"]: r["ip_address"] for r in hop_rows}
        if not hop_nums:
            hop_nums = list(hop_ip_map.keys())

        series: Dict[str, List[Dict[str, Any]]] = {str(h): [] for h in hop_nums}

        for h_num in hop_nums:
            ip_addr = hop_ip_map.get(h_num)
            if not ip_addr:
                continue

            is_final = h_num == max_hop_num
            filter_col = "hop_num >= ?" if is_final else "ip_address = ?"
            filter_val: Any = max_hop_num if is_final else ip_addr

            if bucket_sec == 1:
                rows = conn.execute(
                    f"""
                    SELECT timestamp AS bucket_ts,
                           ROUND(AVG(latency_ms), 2) AS latency_ms,
                           ROUND(MAX(latency_ms), 2) AS max_ms,
                           ROUND(MIN(latency_ms), 2) AS min_ms,
                           CASE WHEN MAX(is_loss) = 1 THEN 100.0 ELSE 0.0 END AS loss_flag,
                           ROUND(AVG(is_loss) * 100.0, 1) AS loss_pct,
                           COALESCE(SUM(is_loss), 0) AS lost_pkts,
                           COUNT(*) AS total_pkts
                    FROM hop_metrics
                    WHERE target_id = ? AND {filter_col} AND timestamp >= ?
                    GROUP BY timestamp
                    ORDER BY timestamp ASC
                    """,
                    (target_id, filter_val, since_ts),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""
                    SELECT (timestamp / ?) * ? AS bucket_ts,
                           ROUND(AVG(latency_ms), 2) AS latency_ms,
                           ROUND(MAX(latency_ms), 2) AS max_ms,
                           ROUND(MIN(latency_ms), 2) AS min_ms,
                           CASE WHEN MAX(is_loss) = 1 THEN 100.0 ELSE 0.0 END AS loss_flag,
                           ROUND(AVG(is_loss) * 100.0, 1) AS loss_pct,
                           COALESCE(SUM(is_loss), 0) AS lost_pkts,
                           COUNT(*) AS total_pkts
                    FROM hop_metrics
                    WHERE target_id = ? AND {filter_col} AND timestamp >= ?
                    GROUP BY bucket_ts
                    ORDER BY bucket_ts ASC
                    """,
                    (bucket_sec, bucket_sec, target_id, filter_val, since_ts),
                ).fetchall()

            series[str(h_num)] = [
                {
                    "ts": r["bucket_ts"],
                    "ms": r["latency_ms"],
                    "max_ms": r["max_ms"],
                    "min_ms": r["min_ms"],
                    "loss": r["loss_flag"],
                    "loss_pct": r["loss_pct"],
                    "lost_pkts": int(r["lost_pkts"]),
                    "total_pkts": int(r["total_pkts"]),
                }
                for r in rows
            ]

        return {
            "target_id": target_id,
            "window_seconds": window_seconds,
            "bucket_sec": bucket_sec,
            "server_now": now,
            "series": series,
        }


def get_loss_events(
    window_seconds: int = 0,
    target_id: Optional[int] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    """
    Query permanent `loss_events` and `network_outages` tables directly via indexes.
    Instantaneous even with millions of `hop_metrics` rows.
    """
    now = int(time.time())
    since_ts = (now - window_seconds) if window_seconds > 0 else 0

    where_clauses: List[str] = []
    params: List[Any] = []
    if since_ts > 0:
        where_clauses.append("start_ts >= ?")
        params.append(since_ts)
    if target_id is not None:
        where_clauses.append("target_id = ?")
        params.append(target_id)
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    outage_where = "WHERE start_ts >= ?" if since_ts > 0 else ""
    outage_params: List[Any] = [since_ts] if since_ts > 0 else []

    with get_db_connection() as conn:
        summary_row = conn.execute(
            f"SELECT COUNT(*) AS cnt, COALESCE(SUM(packets_lost), 0) AS pkts FROM loss_events {where_sql}",
            params,
        ).fetchone()

        event_rows = conn.execute(
            f"""
            SELECT id, target_id, target_label, target_host,
                   start_ts, end_ts, packets_lost, cause, severity
            FROM loss_events
            {where_sql}
            ORDER BY start_ts DESC, id DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()

        outage_cnt_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM network_outages {outage_where}",
            outage_params,
        ).fetchone()

        outage_rows = conn.execute(
            f"""
            SELECT id, start_ts, end_ts, duration_sec,
                   affected_targets, total_external_targets,
                   router_reachable, affected_labels
            FROM network_outages
            {outage_where}
            ORDER BY start_ts DESC, id DESC
            LIMIT 100
            """,
            outage_params,
        ).fetchall()

    events = [
        {
            "id": r["id"],
            "target_id": r["target_id"],
            "target_label": r["target_label"],
            "target_host": r["target_host"],
            "start_ts": r["start_ts"],
            "end_ts": r["end_ts"],
            "packets_lost": r["packets_lost"],
            "cause": r["cause"],
            "severity": r["severity"],
        }
        for r in event_rows
    ]

    outages = [
        {
            "id": r["id"],
            "start_ts": r["start_ts"],
            "end_ts": r["end_ts"],
            "duration_sec": r["duration_sec"],
            "affected_targets": r["affected_targets"],
            "total_external_targets": r["total_external_targets"],
            "router_reachable": bool(r["router_reachable"]),
            "affected_labels": r["affected_labels"],
        }
        for r in outage_rows
    ]

    return {
        "events": events,
        "outages": outages,
        "summary": {
            "total_incidents": int(summary_row["cnt"]),
            "total_packets_lost": int(summary_row["pkts"]),
            "total_outages": int(outage_cnt_row["cnt"]),
        },
    }


def prune_old_metrics(batch_size: int = 5000, max_batches: int = 20) -> int:
    """
    Delete hop_metrics rows older than RETENTION_HOURS in small indexed batches
    so SQLite never locks for seconds. Permanent loss_events and network_outages
    are kept for LOSS_RETENTION_DAYS (default 365 days).
    """
    cutoff = int(time.time()) - (RETENTION_HOURS * 3600)
    total_deleted = 0

    for _ in range(max_batches):
        with get_db_connection() as conn:
            cur = conn.execute(
                """
                DELETE FROM hop_metrics
                WHERE id IN (
                    SELECT id FROM hop_metrics WHERE timestamp < ? LIMIT ?
                )
                """,
                (cutoff, batch_size),
            )
            deleted = cur.rowcount
        total_deleted += deleted
        if deleted < batch_size:
            break

    loss_cutoff = int(time.time()) - (LOSS_RETENTION_DAYS * 86400)
    with get_db_connection() as conn:
        conn.execute("DELETE FROM loss_events WHERE end_ts < ?", (loss_cutoff,))
        conn.execute("DELETE FROM network_outages WHERE end_ts < ?", (loss_cutoff,))
        conn.execute("DELETE FROM speedtest_results WHERE timestamp < ?", (loss_cutoff,))

    if total_deleted > 0:
        logger.info("Pruned %d historical metric rows older than %d hours", total_deleted, RETENTION_HOURS)
    return total_deleted


def _speedtest_row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "timestamp": int(row["timestamp"]),
        "download_mbps": row["download_mbps"],
        "upload_mbps": row["upload_mbps"],
        "ping_ms": row["ping_ms"],
        "jitter_ms": row["jitter_ms"],
        "bytes_used": int(row["bytes_used"]),
        "duration_sec": float(row["duration_sec"]),
        "mode": row["mode"],
        "server_location": row["server_location"],
        "trigger": row["trigger"],
        "status": row["status"],
        "error_message": row["error_message"],
    }


def _round_opt(val: Any, digits: int = 2) -> Optional[float]:
    if val is None:
        return None
    return round(float(val), digits)


def get_speedtest_settings() -> Dict[str, Any]:
    """Read speedtest settings from app_meta with environment defaults."""
    enabled = DEFAULT_SPEEDTEST_ENABLED
    interval_min = DEFAULT_SPEEDTEST_INTERVAL_MIN
    mode = DEFAULT_SPEEDTEST_MODE

    with get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT key, value FROM app_meta
            WHERE key IN ('speedtest_enabled', 'speedtest_interval_min', 'speedtest_mode')
            """
        ).fetchall()

    meta = {r["key"]: r["value"] for r in rows}
    if "speedtest_enabled" in meta:
        enabled = meta["speedtest_enabled"].strip().lower() not in ("0", "false", "no", "off")
    if "speedtest_interval_min" in meta:
        try:
            interval_min = max(5, min(1440, int(meta["speedtest_interval_min"])))
        except ValueError:
            pass
    if "speedtest_mode" in meta:
        cand = meta["speedtest_mode"].strip().lower()
        if cand in VALID_SPEEDTEST_MODES:
            mode = cand

    return {
        "enabled": enabled,
        "interval_min": interval_min,
        "mode": mode,
    }


def update_speedtest_settings(
    enabled: Optional[bool] = None,
    interval_min: Optional[int] = None,
    mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate and persist speedtest settings into app_meta."""
    updates: List[tuple] = []

    if enabled is not None:
        updates.append(("speedtest_enabled", "1" if bool(enabled) else "0"))

    if interval_min is not None:
        try:
            iv = int(interval_min)
        except (TypeError, ValueError) as exc:
            raise ValueError("interval_min must be an integer between 5 and 1440") from exc
        if iv < 5 or iv > 1440:
            raise ValueError("interval_min must be between 5 and 1440 minutes")
        updates.append(("speedtest_interval_min", str(iv)))

    if mode is not None:
        normalized_mode = str(mode).strip().lower()
        if normalized_mode not in VALID_SPEEDTEST_MODES:
            raise ValueError(f"mode must be one of {VALID_SPEEDTEST_MODES}")
        updates.append(("speedtest_mode", normalized_mode))

    if updates:
        with get_db_connection() as conn:
            for key, val in updates:
                conn.execute(
                    "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
                    (key, val),
                )

    return get_speedtest_settings()


def save_speedtest_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Insert a completed or failed speedtest measurement and return the saved record."""
    ts = int(result.get("timestamp") or time.time())
    dl = result.get("download_mbps")
    ul = result.get("upload_mbps")
    ping_ms = result.get("ping_ms")
    jitter_ms = result.get("jitter_ms")
    bytes_used = int(result.get("bytes_used") or 0)
    duration_sec = float(result.get("duration_sec") or 0.0)
    mode = str(result.get("mode") or DEFAULT_SPEEDTEST_MODE)
    server_location = str(result.get("server_location") or "")
    trigger = str(result.get("trigger") or "scheduled")
    status = str(result.get("status") or "ok")
    error_message = result.get("error_message")

    with get_db_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO speedtest_results (
                timestamp, download_mbps, upload_mbps, ping_ms, jitter_ms,
                bytes_used, duration_sec, mode, server_location, trigger,
                status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                dl,
                ul,
                ping_ms,
                jitter_ms,
                bytes_used,
                duration_sec,
                mode,
                server_location,
                trigger,
                status,
                error_message,
            ),
        )
        new_id = int(cur.lastrowid)

    return {
        "id": new_id,
        "timestamp": ts,
        "download_mbps": dl,
        "upload_mbps": ul,
        "ping_ms": ping_ms,
        "jitter_ms": jitter_ms,
        "bytes_used": bytes_used,
        "duration_sec": duration_sec,
        "mode": mode,
        "server_location": server_location,
        "trigger": trigger,
        "status": status,
        "error_message": error_message,
    }


def get_latest_speedtest_ts() -> Optional[int]:
    """Return the timestamp of the most recent speedtest run, or None if none exist."""
    with get_db_connection() as conn:
        row = conn.execute("SELECT MAX(timestamp) AS max_ts FROM speedtest_results").fetchone()
    if not row or row["max_ts"] is None:
        return None
    return int(row["max_ts"])


def get_speedtest_history(window_seconds: int = 0, limit: int = 500) -> Dict[str, Any]:
    """Query speedtest results and summary statistics within an optional time window."""
    now = int(time.time())
    since_ts = (now - window_seconds) if window_seconds > 0 else 0

    where_sql = "WHERE timestamp >= ?" if since_ts > 0 else ""
    params: List[Any] = [since_ts] if since_ts > 0 else []

    with get_db_connection() as conn:
        agg_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_runs,
                COALESCE(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END), 0) AS successful_runs,
                COALESCE(SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END), 0) AS failed_runs,
                AVG(CASE WHEN status = 'ok' THEN download_mbps END) AS avg_dl,
                MIN(CASE WHEN status = 'ok' THEN download_mbps END) AS min_dl,
                MAX(CASE WHEN status = 'ok' THEN download_mbps END) AS max_dl,
                AVG(CASE WHEN status = 'ok' THEN upload_mbps END) AS avg_ul,
                MIN(CASE WHEN status = 'ok' THEN upload_mbps END) AS min_ul,
                MAX(CASE WHEN status = 'ok' THEN upload_mbps END) AS max_ul,
                AVG(CASE WHEN status = 'ok' THEN ping_ms END) AS avg_ping,
                COALESCE(SUM(bytes_used), 0) AS total_bytes_used
            FROM speedtest_results
            {where_sql}
            """,
            params,
        ).fetchone()

        rows = conn.execute(
            f"""
            SELECT id, timestamp, download_mbps, upload_mbps, ping_ms, jitter_ms,
                   bytes_used, duration_sec, mode, server_location, trigger,
                   status, error_message
            FROM speedtest_results
            {where_sql}
            ORDER BY timestamp DESC, id DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()

        latest_row = conn.execute(
            """
            SELECT id, timestamp, download_mbps, upload_mbps, ping_ms, jitter_ms,
                   bytes_used, duration_sec, mode, server_location, trigger,
                   status, error_message
            FROM speedtest_results
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        latest_ok_row = conn.execute(
            """
            SELECT id, timestamp, download_mbps, upload_mbps, ping_ms, jitter_ms,
                   bytes_used, duration_sec, mode, server_location, trigger,
                   status, error_message
            FROM speedtest_results
            WHERE status = 'ok'
            ORDER BY timestamp DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    results = [_speedtest_row_to_dict(r) for r in rows]
    summary = {
        "total_runs": int(agg_row["total_runs"]) if agg_row else 0,
        "successful_runs": int(agg_row["successful_runs"]) if agg_row else 0,
        "failed_runs": int(agg_row["failed_runs"]) if agg_row else 0,
        "avg_download_mbps": _round_opt(agg_row["avg_dl"] if agg_row else None, 2),
        "min_download_mbps": _round_opt(agg_row["min_dl"] if agg_row else None, 2),
        "max_download_mbps": _round_opt(agg_row["max_dl"] if agg_row else None, 2),
        "avg_upload_mbps": _round_opt(agg_row["avg_ul"] if agg_row else None, 2),
        "min_upload_mbps": _round_opt(agg_row["min_ul"] if agg_row else None, 2),
        "max_upload_mbps": _round_opt(agg_row["max_ul"] if agg_row else None, 2),
        "avg_ping_ms": _round_opt(agg_row["avg_ping"] if agg_row else None, 2),
        "total_bytes_used": int(agg_row["total_bytes_used"]) if agg_row else 0,
    }

    return {
        "results": results,
        "summary": summary,
        "latest": _speedtest_row_to_dict(latest_row),
        "latest_ok": _speedtest_row_to_dict(latest_ok_row),
    }


def clear_speedtest_results() -> int:
    """Delete all stored speedtest results and return the number of rows removed."""
    with get_db_connection() as conn:
        cur = conn.execute("DELETE FROM speedtest_results")
        return int(cur.rowcount)
