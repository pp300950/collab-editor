import asyncio
import json
import subprocess
import sys
import tempfile
import os
from typing import Dict
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

frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.exists(frontend_path):
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")

# ── State ──────────────────────────────────────────────────────
shared_code: str = '# Welcome to CollabPy!\nprint("Hello, World!")\n'
connections: Dict[str, WebSocket] = {}
user_cursors: Dict[str, dict]    = {}

# Per-user interactive process queues
# user_id -> asyncio.Queue (receives stdin lines from the websocket)
stdin_queues: Dict[str, asyncio.Queue] = {}

COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4",
    "#FFEAA7", "#DDA0DD", "#98D8C8", "#F7DC6F"
]


async def broadcast(message: dict, exclude: str = None):
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

    color_index = len(connections) % len(COLORS)
    color = COLORS[color_index]

    connections[user_id] = websocket
    user_cursors[user_id] = {"line": 0, "ch": 0, "name": user_id, "color": color}
    stdin_queues[user_id] = asyncio.Queue()

    await websocket.send_json({
        "type":       "init",
        "code":       shared_code,
        "cursors":    user_cursors,
        "your_color": color,
        "user_id":    user_id,
    })

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

            if msg_type == "code_change":
                shared_code = data["code"]
                await broadcast({
                    "type": "code_change",
                    "code": shared_code,
                    "from": user_id,
                }, exclude=user_id)

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

            elif msg_type == "selection":
                await broadcast({
                    "type":    "selection",
                    "user_id": user_id,
                    "from":    data["from"],
                    "to":      data["to"],
                    "color":   color,
                    "name":    user_id,
                }, exclude=user_id)

            # ── Interactive run ──────────────────────────────────
            elif msg_type == "run_code":
                # Clear any leftover stdin
                q = stdin_queues[user_id]
                while not q.empty():
                    try: q.get_nowait()
                    except: break

                await broadcast({
                    "type":    "run_start",
                    "user_id": user_id,
                })

                # Run in background so we can keep receiving stdin
                asyncio.create_task(
                    run_interactive(shared_code, user_id, websocket)
                )

            # ── Stdin line from terminal ─────────────────────────
            elif msg_type == "stdin_line":
                q = stdin_queues.get(user_id)
                if q:
                    await q.put(data.get("line", "") + "\n")

            # ── Kill running process ─────────────────────────────
            elif msg_type == "kill":
                q = stdin_queues.get(user_id)
                if q:
                    await q.put(None)   # None = signal kill

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[error] {user_id}: {e}")
    finally:
        connections.pop(user_id, None)
        user_cursors.pop(user_id, None)
        stdin_queues.pop(user_id, None)
        await broadcast({
            "type":    "user_left",
            "user_id": user_id,
            "cursors": user_cursors,
        })


async def run_interactive(code: str, user_id: str, websocket: WebSocket) -> None:
    """
    Execute Python code with fully interactive stdin/stdout streaming.
    - stdout/stderr chunks are sent to all clients as they arrive.
    - When the process is blocked waiting for stdin, the server waits
      for a 'stdin_line' message from the triggering user, then pipes
      it in.
    """
    tmp_path = None
    proc = None

    async def send_all(msg: dict):
        dead = []
        for uid, ws in connections.items():
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(uid)
        for uid in dead:
            connections.pop(uid, None)

    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".py", delete=False, encoding="utf-8"
        ) as f:
            f.write(code)
            tmp_path = f.name

        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-u", tmp_path,   # -u = unbuffered
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
        )

        q = stdin_queues.get(user_id)
        stdout_buf = b""
        stderr_done = False

        async def drain_stderr():
            """Read stderr concurrently and broadcast chunks."""
            nonlocal stderr_done
            while True:
                chunk = await proc.stderr.read(256)
                if not chunk:
                    break
                await send_all({
                    "type":    "terminal_stderr",
                    "text":    chunk.decode("utf-8", errors="replace"),
                    "user_id": user_id,
                })
            stderr_done = True

        stderr_task = asyncio.create_task(drain_stderr())

        deadline = asyncio.get_event_loop().time() + 30.0  # 30-second total timeout

        while True:
            if asyncio.get_event_loop().time() > deadline:
                proc.kill()
                await send_all({
                    "type":    "terminal_stderr",
                    "text":    "\n⏱ Timeout: รันเกิน 30 วินาที\n",
                    "user_id": user_id,
                })
                break

            # Try to read stdout (non-blocking, up to 256 bytes)
            try:
                chunk = await asyncio.wait_for(
                    proc.stdout.read(256), timeout=0.05
                )
                if chunk:
                    await send_all({
                        "type":    "terminal_stdout",
                        "text":    chunk.decode("utf-8", errors="replace"),
                        "user_id": user_id,
                    })
                    continue
                else:
                    # stdout EOF — process finished
                    break
            except asyncio.TimeoutError:
                pass

            # Check if process ended
            if proc.returncode is not None:
                break

            # Process is alive but no stdout — may be waiting for stdin
            if q and not q.empty():
                line = await q.get()
                if line is None:
                    proc.kill()
                    await send_all({
                        "type":    "terminal_stderr",
                        "text":    "\n🛑 โปรแกรมถูกหยุดโดยผู้ใช้\n",
                        "user_id": user_id,
                    })
                    break
                try:
                    proc.stdin.write(line.encode("utf-8"))
                    await proc.stdin.drain()
                    # Echo the input back so everyone sees it
                    await send_all({
                        "type":    "terminal_stdin_echo",
                        "text":    line,
                        "user_id": user_id,
                    })
                except Exception:
                    break
            else:
                # Yield briefly and loop again
                await asyncio.sleep(0.02)

        # Drain remaining stdout
        try:
            rest = await asyncio.wait_for(proc.stdout.read(), timeout=2.0)
            if rest:
                await send_all({
                    "type":    "terminal_stdout",
                    "text":    rest.decode("utf-8", errors="replace"),
                    "user_id": user_id,
                })
        except Exception:
            pass

        await stderr_task

        try:
            await asyncio.wait_for(proc.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            proc.kill()

        exit_code = proc.returncode if proc.returncode is not None else -1
        await send_all({
            "type":      "run_result",
            "stdout":    "",   # terminal mode — already streamed
            "stderr":    "",
            "exit_code": exit_code,
            "user_id":   user_id,
        })

    except Exception as e:
        await send_all({
            "type":    "terminal_stderr",
            "text":    f"\n[server error] {e}\n",
            "user_id": user_id,
        })
        await send_all({
            "type": "run_result", "stdout": "", "stderr": str(e),
            "exit_code": -1, "user_id": user_id,
        })
    finally:
        if proc and proc.returncode is None:
            try: proc.kill()
            except: pass
        if tmp_path:
            try: os.unlink(tmp_path)
            except: pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)