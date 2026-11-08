import asyncio
import logging
import re
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
    """Manages active WebSocket clients for sub-second UI broadcasts."""

    def __init__(self) -> None:
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        clients = list(self.active_connections)
        if not clients:
            return
        stale: List[WebSocket] = []
        for ws in clients:
            try:
                await ws.send_json(message)
            except Exception:
                stale.append(ws)
        for ws in stale:
            if ws in self.active_connections:
                self.active_connections.remove(ws)


ws_manager = ConnectionManager()
monitor_engine = MonitorEngine(broadcast_cb=ws_manager.broadcast)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting NetworkStats Server...")
    database.init_db()
    database.prune_old_metrics()
    await monitor_engine.start()
    yield
    logger.info("Shutting down NetworkStats Server...")
    await monitor_engine.stop()


app = FastAPI(
    title="NetworkStats",
    description="Self-Hosted 24/7 Network Monitoring and Visual Traceroute Web Application",
    version="1.1.0",
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
    return {"app": "NetworkStats", "status": "ok"}


@app.get("/api/targets")
async def api_list_targets():
    return {"targets": database.list_targets()}


@app.post("/api/targets", status_code=201)
async def api_create_target(req: TargetCreateRequest):
    host = req.host.strip()
    if not HOST_VALIDATION_RE.match(host):
        raise HTTPException(status_code=400, detail="Invalid IP address or hostname format.")

    try:
        target = database.add_target(
            host=host,
            label=req.label.strip() if req.label else host,
            interval_sec=req.interval_sec,
        )
    except Exception as exc:
        if "UNIQUE" in str(exc).upper():
            raise HTTPException(status_code=409, detail=f"Target '{host}' already exists.")
        raise HTTPException(status_code=500, detail=str(exc))

    await monitor_engine.sync_workers()
    targets = database.list_targets()
    await ws_manager.broadcast({"type": "targets_changed", "targets": targets})
    return {"target": target, "targets": targets}


@app.patch("/api/targets/{target_id}")
async def api_update_target(target_id: int, req: TargetUpdateRequest):
    updated = database.update_target(
        target_id=target_id,
        label=req.label,
        interval_sec=req.interval_sec,
        enabled=req.enabled,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Target not found.")

    await monitor_engine.sync_workers(
        restart_target_id=target_id if req.interval_sec is not None else None
    )
    targets = database.list_targets()
    await ws_manager.broadcast({"type": "targets_changed", "targets": targets})
    return {"target": updated, "targets": targets}


@app.delete("/api/targets/{target_id}")
async def api_delete_target(target_id: int):
    deleted = database.delete_target(target_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Target not found.")

    monitor_engine.reset_target_memory(target_id)
    await monitor_engine.sync_workers()
    targets = database.list_targets()
    await ws_manager.broadcast({"type": "targets_changed", "targets": targets})
    return {"deleted": True, "targets": targets}


@app.post("/api/targets/{target_id}/reset")
async def api_reset_target(target_id: int):
    existing = database.get_target(target_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Target not found.")

    database.reset_target_stats(target_id)
    monitor_engine.reset_target_memory(target_id)
    targets = database.list_targets()
    await ws_manager.broadcast({"type": "targets_changed", "targets": targets})
    return {"reset": True, "targets": targets}


@app.get("/api/targets/{target_id}/hops")
async def api_get_target_hops(target_id: int):
    target = database.get_target(target_id)
    if not target:
        raise HTTPException(status_code=404, detail="Target not found.")
    hops = database.get_hop_states(target_id)
    return {"target": target, "hops": hops}


@app.get("/api/targets/{target_id}/history")
async def api_get_target_history(
    target_id: int,
    window: int = Query(default=300, ge=60, le=172800, description="Time window in seconds"),
    hops: Optional[str] = Query(default=None, description="Comma-separated hop numbers"),
):
    target = database.get_target(target_id)
    if not target:
        raise HTTPException(status_code=404, detail="Target not found.")

    hop_list: Optional[List[int]] = None
    if hops:
        hop_list = [int(x.strip()) for x in hops.split(",") if x.strip().isdigit()]

    history = database.get_target_history(
        target_id=target_id,
        window_seconds=window,
        hop_nums=hop_list,
    )
    return history


@app.get("/api/loss-events")
async def api_get_loss_events(
    window: int = Query(default=86400, ge=60, le=172800, description="Time window in seconds"),
    target_id: Optional[int] = Query(default=None, description="Optional target ID filter"),
):
    return database.get_loss_events(window_seconds=window, target_id=target_id)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        targets = database.list_targets()
        await websocket.send_json({"type": "init", "targets": targets})
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception:
        await ws_manager.disconnect(websocket)
