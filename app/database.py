import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

logger = logging.getLogger("NetworkStats.database")

DB_PATH = os.getenv("DB_PATH", "/data/networkstats.db")
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "168"))  # 7 days default retention
DEFAULT_INTERVAL = float(os.getenv("PROBE_INTERVAL", "1.0"))

DEFAULT_TARGETS = [
    {"host": "10.0.0.1", "label": "Local Gateway", "interval_sec": DEFAULT_INTERVAL},
    {"host": "1.1.1.1", "label": "Cloudflare DNS", "interval_sec": DEFAULT_INTERVAL},
    {"host": "8.8.8.8", "label": "Google DNS", "interval_sec": DEFAULT_INTERVAL},
    {"host": "9.9.9.9", "label": "Quad9 DNS", "interval_sec": DEFAULT_INTERVAL},
]


@contextmanager
def get_db_connection():
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Initialize SQLite schema, purge unroutable ISP 10.x hops, and sync hop counters."""
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

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_target_ts
                ON hop_metrics(target_id, timestamp);

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_target_hop_ts
                ON hop_metrics(target_id, hop_num, timestamp);

            CREATE INDEX IF NOT EXISTS idx_hop_metrics_loss_ts
                ON hop_metrics(is_loss, timestamp);
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

        # 1. Purge unroutable ISP-internal 10.x hops (e.g. 10.170.193.x) and '???' hops
        conn.execute(
            """
            DELETE FROM hop_metrics
            WHERE ip_address = '???'
               OR (hop_num > 1 AND ip_address LIKE '10.%' AND ip_address != '10.0.0.1')
            """
        )
        conn.execute(
            """
            DELETE FROM hop_state
            WHERE ip_address = '???'
               OR (hop_num > 1 AND ip_address LIKE '10.%' AND ip_address != '10.0.0.1')
            """
        )

        # 2. Renumber remaining hop_state rows sequentially (1..N) per target and
        #    recalculate true Sent / Lost / Loss % from hop_metrics so round-robin DNS
        #    targets (like Fortnite) never lose their 24h+ packet loss counts!
        t_rows = conn.execute("SELECT id FROM targets").fetchall()
        for tr in t_rows:
            t_id = tr["id"]
            hops = conn.execute(
                "SELECT * FROM hop_state WHERE target_id = ? ORDER BY hop_num ASC",
                (t_id,),
            ).fetchall()
            if not hops:
                continue

            max_old_hop = hops[-1]["hop_num"]
            for new_idx, h in enumerate(hops):
                new_hop_num = new_idx + 1
                is_final = h["hop_num"] == max_old_hop

                if is_final:
                    # For the final destination hop, aggregate all final-hop probes for this target
                    stat = conn.execute(
                        """
                        SELECT COUNT(*) AS total_sent, COALESCE(SUM(is_loss), 0) AS total_lost
                        FROM hop_metrics
                        WHERE target_id = ?
                          AND hop_num >= ?
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
                    (new_hop_num, s_cnt, l_cnt, l_pct, t_id, h["hop_num"]),
                )


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
) -> None:
    """Persist updated hop state rows and append time-series probe samples."""
    max_hop = max((h["hop_num"] for h in hop_states), default=0)
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


def get_target_history(
    target_id: int,
    window_seconds: int = 300,
    hop_nums: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """
    Retrieve time-series metrics for the requested target and time window.
    For the final hop, includes all final-hop probes even if round-robin DNS changed the IP.
    """
    now = int(time.time())
    since_ts = now - window_seconds

    if window_seconds <= 1800:      # <= 30m: raw 1s samples
        bucket_sec = 1
    elif window_seconds <= 3600:    # 1h: 5s buckets
        bucket_sec = 5
    elif window_seconds <= 21600:   # 6h: 30s buckets
        bucket_sec = 30
    else:                           # 24h+: 120s buckets
        bucket_sec = 120

    with get_db_connection() as conn:
        hop_rows = conn.execute(
            "SELECT hop_num, ip_address FROM hop_state WHERE target_id = ? ORDER BY hop_num ASC",
            (target_id,),
        ).fetchall()
        if not hop_rows:
            return {"target_id": target_id, "window_seconds": window_seconds, "series": {}}

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

            if is_final:
                # Match all final-hop probes (hop_num >= max_hop_num) so round-robin DNS
                # targets keep their full 24h history intact
                if bucket_sec == 1:
                    rows = conn.execute(
                        """
                        SELECT timestamp AS bucket_ts,
                               ROUND(AVG(latency_ms), 2) AS latency_ms,
                               CASE WHEN MAX(is_loss) = 1 THEN 100.0 ELSE 0.0 END AS loss_pct
                        FROM hop_metrics
                        WHERE target_id = ? AND hop_num >= ? AND timestamp >= ?
                        GROUP BY timestamp
                        ORDER BY timestamp ASC
                        """,
                        (target_id, max_hop_num, since_ts),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT (timestamp / ?) * ? AS bucket_ts,
                               ROUND(AVG(latency_ms), 2) AS latency_ms,
                               CASE WHEN MAX(is_loss) = 1 THEN 100.0 ELSE 0.0 END AS loss_pct
                        FROM hop_metrics
                        WHERE target_id = ? AND hop_num >= ? AND timestamp >= ?
                        GROUP BY bucket_ts
                        ORDER BY bucket_ts ASC
                        """,
                        (bucket_sec, bucket_sec, target_id, max_hop_num, since_ts),
                    ).fetchall()
            else:
                if bucket_sec == 1:
                    rows = conn.execute(
                        """
                        SELECT timestamp AS bucket_ts,
                               latency_ms AS latency_ms,
                               CASE WHEN is_loss = 1 THEN 100.0 ELSE 0.0 END AS loss_pct
                        FROM hop_metrics
                        WHERE target_id = ? AND ip_address = ? AND timestamp >= ?
                        ORDER BY timestamp ASC
                        """,
                        (target_id, ip_addr, since_ts),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT (timestamp / ?) * ? AS bucket_ts,
                               ROUND(AVG(latency_ms), 2) AS latency_ms,
                               CASE WHEN MAX(is_loss) = 1 THEN 100.0 ELSE 0.0 END AS loss_pct
                        FROM hop_metrics
                        WHERE target_id = ? AND ip_address = ? AND timestamp >= ?
                        GROUP BY bucket_ts
                        ORDER BY bucket_ts ASC
                        """,
                        (bucket_sec, bucket_sec, target_id, ip_addr, since_ts),
                    ).fetchall()

            series[str(h_num)] = [
                {
                    "ts": r["bucket_ts"],
                    "ms": r["latency_ms"],
                    "loss": r["loss_pct"],
                }
                for r in rows
            ]

        return {
            "target_id": target_id,
            "window_seconds": window_seconds,
            "bucket_sec": bucket_sec,
            "series": series,
        }


def get_loss_events(
    window_seconds: int = 0,
    target_id: Optional[int] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    """
    Return a chronological list of every time a target's Final Destination lost a packet.
    Matches all final-hop probes (even across round-robin DNS IPs) and checks whether
    Hop #1 (Local Router) or Hop #2 (ISP Modem) dropped at the same second.
    """
    now = int(time.time())
    since_ts = (now - window_seconds) if window_seconds > 0 else 0

    with get_db_connection() as conn:
        targets_rows = conn.execute("SELECT id, host, label FROM targets").fetchall()
        target_map = {r["id"]: {"host": r["host"], "label": r["label"]} for r in targets_rows}

        if not target_map:
            return {"events": [], "summary": {"total_incidents": 0, "total_packets_lost": 0}}

        final_hops_rows = conn.execute(
            """
            SELECT target_id, MAX(hop_num) AS max_hop
            FROM hop_state
            GROUP BY target_id
            """
        ).fetchall()
        final_hop_num_map = {r["target_id"]: r["max_hop"] for r in final_hops_rows}

        incidents: List[Dict[str, Any]] = []
        total_packets_lost = 0

        target_ids = (
            [target_id]
            if (target_id is not None and target_id in target_map)
            else list(target_map.keys())
        )

        for t_id in target_ids:
            max_hop = final_hop_num_map.get(t_id, 1)

            # All timestamps where the final destination hop lost a packet
            lost_rows = conn.execute(
                """
                SELECT DISTINCT timestamp
                FROM hop_metrics
                WHERE target_id = ?
                  AND hop_num >= ?
                  AND is_loss = 1
                  AND timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (t_id, max_hop, since_ts),
            ).fetchall()

            if not lost_rows:
                continue

            # Check if Hop #1 (Router) or Hop #2 (ISP Modem) also dropped at those times
            upstream_drops = conn.execute(
                """
                SELECT timestamp, hop_num
                FROM hop_metrics
                WHERE target_id = ?
                  AND hop_num IN (1, 2)
                  AND hop_num < ?
                  AND is_loss = 1
                  AND timestamp >= ?
                """,
                (t_id, max_hop, since_ts),
            ).fetchall()
            upstream_by_ts: Dict[int, List[int]] = {}
            for u in upstream_drops:
                upstream_by_ts.setdefault(int(u["timestamp"]), []).append(int(u["hop_num"]))

            t_meta = target_map[t_id]
            current_inc: Optional[Dict[str, Any]] = None

            for r in lost_rows:
                ts = int(r["timestamp"])
                total_packets_lost += 1

                up_hops = (
                    upstream_by_ts.get(ts, [])
                    + upstream_by_ts.get(ts - 1, [])
                    + upstream_by_ts.get(ts + 1, [])
                )
                if 1 in up_hops:
                    cause = "Local Router (10.0.0.1) dropped"
                    severity = "router"
                elif 2 in up_hops:
                    cause = "ISP Modem (Hop #2) dropped"
                    severity = "isp"
                else:
                    cause = "Target / Internet Route dropped (Router & Modem OK)"
                    severity = "target"

                if current_inc and (ts - current_inc["end_ts"] <= 3):
                    current_inc["end_ts"] = ts
                    current_inc["packets_lost"] += 1
                    if severity in ("router", "isp") and current_inc["severity"] == "target":
                        current_inc["cause"] = cause
                        current_inc["severity"] = severity
                else:
                    if current_inc:
                        incidents.append(current_inc)
                    current_inc = {
                        "target_id": t_id,
                        "target_label": t_meta["label"],
                        "target_host": t_meta["host"],
                        "start_ts": ts,
                        "end_ts": ts,
                        "packets_lost": 1,
                        "cause": cause,
                        "severity": severity,
                    }

            if current_inc:
                incidents.append(current_inc)

        incidents.sort(key=lambda x: x["start_ts"], reverse=True)
        incidents = incidents[:limit]

        return {
            "events": incidents,
            "summary": {
                "total_incidents": len(incidents),
                "total_packets_lost": total_packets_lost,
            },
        }


def prune_old_metrics() -> int:
    """Delete hop_metrics rows older than RETENTION_HOURS."""
    cutoff = int(time.time()) - (RETENTION_HOURS * 3600)
    with get_db_connection() as conn:
        cur = conn.execute("DELETE FROM hop_metrics WHERE timestamp < ?", (cutoff,))
        deleted = cur.rowcount
    if deleted > 0:
        logger.info("Pruned %d historical metric rows older than %d hours", deleted, RETENTION_HOURS)
    return deleted
