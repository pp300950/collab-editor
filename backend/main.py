import asyncio
import json
import subprocess
import sys
import tempfile
import os
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend static files
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

# ── State ──────────────────────────────────────────────────────
shared_code: str = '# Welcome to CollabPy!\nprint("Hello, World!")\n'
connections: Dict[str, WebSocket] = {}   # user_id -> websocket
user_cursors: Dict[str, dict]    = {}    # user_id -> {line, ch, name, color}

COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
    "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F"
]


async def broadcast(message: dict, exclude: str = None):
    """Send a message to all connected clients, optionally skipping one."""
    dead = []
    for uid, ws in connections.items():
        if uid == exclude:
            continue
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(uid)
    for uid in dead:
        connections.pop(uid, None)
        user_cursors.pop(uid, None)


@app.get("/")
async def root():
    index = os.path.join(frontend_path, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return {"status": "CollabPy backend running"}


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    global shared_code

    await websocket.accept()

    # Assign a color based on current connection count
    color_index = len(connections) % len(COLORS)
    color = COLORS[color_index]

    connections[user_id] = websocket
    user_cursors[user_id] = {"line": 0, "ch": 0, "name": user_id, "color": color}

    # ── Send current state to the joining user ──
    await websocket.send_json({
        "type":       "init",
        "code":       shared_code,
        "cursors":    user_cursors,
        "your_color": color,
        "user_id":    user_id,
    })

    # ── Notify everyone else ──
    await broadcast({
        "type":    "user_joined",
        "user_id": user_id,
        "color":   color,
        "name":    user_id,
        "cursors": user_cursors,
    }, exclude=user_id)

    try:
        while True:
            data = await websocket.receive_json()
            msg_type = data.get("type")

            # ── Code change ──────────────────────────────────────
            if msg_type == "code_change":
                shared_code = data["code"]
                await broadcast({
                    "type": "code_change",
                    "code": shared_code,
                    "from": user_id,
                }, exclude=user_id)

            # ── Cursor movement ──────────────────────────────────
            elif msg_type == "cursor_move":
                user_cursors[user_id] = {
                    "line":  data["line"],
                    "ch":    data["ch"],
                    "name":  user_id,
                    "color": color,
                }
                await broadcast({
                    "type":    "cursor_update",
                    "user_id": user_id,
                    "line":    data["line"],
                    "ch":      data["ch"],
                    "color":   color,
                    "name":    user_id,
                }, exclude=user_id)

            # ── Selection ────────────────────────────────────────
            elif msg_type == "selection":
                await broadcast({
                    "type":    "selection",
                    "user_id": user_id,
                    "from":    data["from"],
                    "to":      data["to"],
                    "color":   color,
                    "name":    user_id,
                }, exclude=user_id)

            # ── Run code ─────────────────────────────────────────
            elif msg_type == "run_code":
                # Notify all users that execution has started
                await broadcast({
                    "type":    "run_start",
                    "user_id": user_id,
                })

                output = await run_python(shared_code)

                await broadcast({
                    "type":      "run_result",
                    "stdout":    output["stdout"],
                    "stderr":    output["stderr"],
                    "exit_code": output["exit_code"],
                    "user_id":   user_id,
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[error] {user_id}: {e}")
    finally:
        connections.pop(user_id, None)
        user_cursors.pop(user_id, None)
        await broadcast({
            "type":    "user_left",
            "user_id": user_id,
            "cursors": user_cursors,
        })


async def run_python(code: str) -> dict:
    """Execute Python code in a subprocess with a 10-second timeout."""
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            tmp_path = f.name

        proc = await asyncio.create_subprocess_exec(
            sys.executable, tmp_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        except asyncio.TimeoutError:
            proc.kill()
            return {
                "stdout":    "",
                "stderr":    "⏱ Timeout: code ran for more than 10 seconds",
                "exit_code": -1,
            }

        return {
            "stdout":    stdout.decode("utf-8", errors="replace"),
            "stderr":    stderr.decode("utf-8", errors="replace"),
            "exit_code": proc.returncode,
        }

    except Exception as e:
        return {"stdout": "", "stderr": str(e), "exit_code": -1}

    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)