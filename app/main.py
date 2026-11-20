import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app import database
from app.monitor import MonitorEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NetworkStats] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("NetworkStats.main")

STATIC_DIR = Path(__file__).resolve().parent / "static"
HOST_VALIDATION_RE = re.compile(r"^[a-zA-Z0-9.\-:]+$")


class ConnectionManager:
    """Manages active WebSocket clients with timeout-protected concurrent broadcasts."""

    def __init__(self) -> None:
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def _send_one(self, websocket: WebSocket, message: Dict[str, Any]) -> bool:
        try:
            await asyncio.wait_for(websocket.send_json(message), timeout=1.5)
            return True
        except Exception:
            try:
                await asyncio.wait_for(websocket.close(), timeout=0.5)
            except Exception:
                pass
            return False

    async def broadcast(self, message: Dict[str, Any]) -> None:
        clients = list(self.active_connections)
        if not clients:
            return
        results = await asyncio.gather(
            *(self._send_one(ws, message) for ws in clients),
            return_exceptions=True,
        )
        for ws, ok in zip(clients, results):
            if ok is not True and ws in self.active_connections:
                self.active_connections.remove(ws)


ws_manager = ConnectionManager()
monitor_engine = MonitorEngine(broadcast_cb=ws_manager.broadcast)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting NetworkStats Server...")
    await database.run_db_write(database.init_db)
    await database.run_db_write(database.prune_old_metrics)
    await monitor_engine.start()
    yield
    logger.info("Shutting down NetworkStats Server...")
    await monitor_engine.stop()


app = FastAPI(
    title="NetworkStats",
    description="Self-Hosted 24/7 Network Monitoring and Visual Traceroute Web Application",
    version="1.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class TargetCreateRequest(BaseModel):
    host: str = Field(..., min_length=1, max_length=253, description="IP address or hostname")
    label: Optional[str] = Field(default="", max_length=120, description="Display label")
    interval_sec: float = Field(default=1.0, ge=0.5, le=60.0, description="Probe interval in seconds")


class TargetUpdateRequest(BaseModel):
    label: Optional[str] = Field(default=None, max_length=120)
    interval_sec: Optional[float] = Field(default=None, ge=0.5, le=60.0)
    enabled: Optional[bool] = Field(default=None)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/health")
async def health_check():
    now = int(time.time())
    last_ts = monitor_engine.last_probe_ts
    return {
        "app": "NetworkStats",
        "status": "ok",
        "active_workers": len(monitor_engine._tasks),
        "last_probe_ts": last_ts,
        "seconds_since_last_probe": (now - last_ts) if last_ts else None,
    }


@app.get("/api/targets")
async def api_list_targets():
    targets = await database.run_db_read(database.list_targets)
    return {"targets": targets}


@app.post("/api/targets", status_code=201)
async def api_create_target(req: TargetCreateRequest):
    host = req.host.strip()
    if not HOST_VALIDATION_RE.match(host):
        raise HTTPException(status_code=400, detail="Invalid IP address or hostname format.")

    try:
        target = await database.run_db_write(
            database.add_target,
            host,
            req.label.strip() if req.label else host,
            req.interval_sec,
        )
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail=f"Target '{host}' already exists.")
        raise HTTPException(status_code=500, detail=str(exc))

    await monitor_engine.sync_workers()
    targets = await database.run_db_read(database.list_targets)
    asyncio.create_task(ws_manager.broadcast({"type": "targets_changed", "targets": targets}))
    return {"target": target, "targets": targets}


@app.patch("/api/targets/{target_id}")
async def api_update_target(target_id: int, req: TargetUpdateRequest):
    updated = await database.run_db_write(
        database.update_target,
        target_id,
        req.label,
        req.interval_sec,
        req.enabled,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Target not found.")

    await monitor_engine.sync_workers(
        restart_target_id=target_id if req.interval_sec is not None else None
    )
    targets = await database.run_db_read(database.list_targets)
    asyncio.create_task(ws_manager.broadcast({"type": "targets_changed", "targets": targets}))
    return {"target": updated, "targets": targets}


@app.delete("/api/targets/{target_id}")
async def api_delete_target(target_id: int):
    deleted = await database.run_db_write(database.delete_target, target_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Target not found.")

    monitor_engine.reset_target_memory(target_id)
    await monitor_engine.sync_workers()
    targets = await database.run_db_read(database.list_targets)
    asyncio.create_task(ws_manager.broadcast({"type": "targets_changed", "targets": targets}))
    asyncio.create_task(ws_manager.broadcast({"type": "loss_update"}))
    return {"deleted": True, "targets": targets}


@app.post("/api/targets/{target_id}/reset")
async def api_reset_target(target_id: int):
    existing = await database.run_db_read(database.get_target, target_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Target not found.")

    await database.run_db_write(database.reset_target_stats, target_id)
    monitor_engine.reset_target_memory(target_id)
    targets = await database.run_db_read(database.list_targets)
    asyncio.create_task(ws_manager.broadcast({"type": "targets_changed", "targets": targets}))
    asyncio.create_task(ws_manager.broadcast({"type": "loss_update"}))
    return {"reset": True, "targets": targets}


@app.get("/api/targets/{target_id}/hops")
async def api_get_target_hops(target_id: int):
    target = await database.run_db_read(database.get_target, target_id)
    if not target:
        raise HTTPException(status_code=404, detail="Target not found.")
    hops = await database.run_db_read(database.get_hop_states, target_id)
    return {"target": target, "hops": hops}


@app.get("/api/targets/{target_id}/history")
async def api_get_target_history(
    target_id: int,
    window: int = Query(default=300, ge=60, le=172800, description="Time window in seconds"),
    hops: Optional[str] = Query(default=None, description="Comma-separated hop numbers"),
):
    target = await database.run_db_read(database.get_target, target_id)
    if not target:
        raise HTTPException(status_code=404, detail="Target not found.")

    hop_list: Optional[List[int]] = None
    if hops:
        hop_list = [int(x.strip()) for x in hops.split(",") if x.strip().isdigit()]

    history = await database.run_db_read(
        database.get_target_history,
        target_id,
        window,
        hop_list,
    )
    return history


@app.get("/api/loss-events")
async def api_get_loss_events(
    window: int = Query(default=0, ge=0, le=31536000, description="Time window in seconds (0 = all time)"),
    target_id: Optional[int] = Query(default=None, description="Optional target ID filter"),
):
    return await database.run_db_read(
        database.get_loss_events,
        window,
        target_id,
    )


@app.delete("/api/loss-events")
async def api_clear_loss_events(
    target_id: Optional[int] = Query(default=None, description="Optional target ID filter"),
):
    await database.run_db_write(database.clear_loss_events, target_id)
    asyncio.create_task(ws_manager.broadcast({"type": "loss_update"}))
    return {"cleared": True}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        targets = await database.run_db_read(database.list_targets)
        await asyncio.wait_for(
            websocket.send_json({"type": "init", "targets": targets}),
            timeout=2.0,
        )
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await asyncio.wait_for(websocket.send_json({"type": "pong"}), timeout=1.5)
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception:
        await ws_manager.disconnect(websocket)
