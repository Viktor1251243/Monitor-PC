import asyncio
import json
from collections import defaultdict
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware  # <-- 1. ДОБАВЛЕНО
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="See Screen")

# <-- 2. ДОБАВЛЕНО: Разрешаем подключение с любых источников
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# device_id -> connected clients
screen_sources = {}
# device_id -> set(viewer websocket)
viewers = defaultdict(set)


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.websocket("/ws/client/{device_id}")
async def client_ws(websocket: WebSocket, device_id: str):
    await websocket.accept()

    # One active source per ID. A reconnect replaces the old one.
    old = screen_sources.get(device_id)
    if old:
        try:
            await old.close()
        except Exception:
            pass

    screen_sources[device_id] = websocket

    try:
        while True:
            raw = await websocket.receive_text()

            # Validate that it is JSON before relaying.
            data = json.loads(raw)
            if data.get("type") != "frame":
                continue

            dead = []
            for viewer in list(viewers.get(device_id, ())):
                try:
                    await viewer.send_text(raw)
                except Exception:
                    dead.append(viewer)

            for viewer in dead:
                viewers[device_id].discard(viewer)

    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if screen_sources.get(device_id) is websocket:
            del screen_sources[device_id]


@app.websocket("/ws/viewer/{device_id}")
async def viewer_ws(websocket: WebSocket, device_id: str):
    await websocket.accept()

    viewers[device_id].add(websocket)

    try:
        # Keep the socket alive. Frames are pushed by client_ws.
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        viewers[device_id].discard(websocket)
        if not viewers[device_id]:
            viewers.pop(device_id, None)


@app.get("/api/status/{device_id}")
async def status(device_id: str):
    return {
        "online": device_id in screen_sources,
        "viewers": len(viewers.get(device_id, ()))
    }


app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
