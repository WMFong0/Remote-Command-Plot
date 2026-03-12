# main.py

import os
import time
import uuid
import asyncio
import threading
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

import paramiko
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables if present (optional)
load_dotenv()

app = FastAPI(
    title="Remote Command Plot API",
    description="For AS Watson GIT Use only",
    version="1.0.0",
    lifespan=lifespan
)

# =============================================================================
# Models
# =============================================================================

class OpenRequest(BaseModel):
    host: str
    username: str
    password: str
    home_dir: Optional[str] = None # optional override directory


class CommandRequest(BaseModel):
    id: str
    command: str


class CloseRequest(BaseModel):
    id: Optional[str] = None # optional; if omitted, closes all sessions


# =============================================================================
# Session Store & Config
# =============================================================================

# session_id -> { client, channel, sudo_pw, created_at, last_seen, host, username }
sessions: Dict[str, Dict[str, Any]] = {}

# Protects the sessions dict for concurrent access
_sessions_lock = threading.Lock()

# Auto-expire sessions idle for this many seconds (5 minutes)
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "300"))

# Cleaner interval seconds
CLEANER_INTERVAL_SECONDS = int(os.getenv("CLEANER_INTERVAL_SECONDS", "60"))

_cleaner_task: Optional[asyncio.Task] = None


# =============================================================================
# Helpers
# =============================================================================

def _now() -> float:
    return time.time()

def _touch_session(session_id: str) -> None:
    with _sessions_lock:
        s = sessions.get(session_id)
        if s:
            s["last_seen"] = _now()

def _close_session_resources(s: Dict[str, Any]) -> None:
    """Safely close channel and client."""
    try:
        ch = s.get("channel")
        if ch:
            ch.close()
    except Exception:
        pass
    try:
        cl = s.get("client")
        if cl:
            cl.close()
    except Exception:
        pass

def _expire_idle_sessions() -> int:
    """Close and remove sessions idle beyond SESSION_TTL_SECONDS. Returns count removed."""
    cutoff = _now() - SESSION_TTL_SECONDS
    removed:int = 0
    to_close: List[Dict[str, Any]] = []
    with _sessions_lock:
        dead_ids: List[str] = []
        for sid, s in sessions.items():
            last_seen = s.get("last_seen", s.get("created_at", 0))
            if last_seen < cutoff:
                dead_ids.append(sid)
        for sid in dead_ids:
            s = sessions.pop(sid, None)
            if s:
                to_close.append(s)
                removed += 1
    for s in to_close:
        _close_session_resources(s)
    return removed

async def _session_cleaner():
    """Background task: periodically expire idle sessions."""
    while True:
        try:
            _expire_idle_sessions()
        except Exception:
            # Never let the cleaner crash the app
            pass
        await asyncio.sleep(CLEANER_INTERVAL_SECONDS)

def _read_all(channel: paramiko.Channel, quiet_timeout: float = 1.0, chunk_timeout: float = 0.2) -> str:
    """
    Read from the interactive channel until no new data arrives for ~quiet_timeout.
    """
    end_by = time.time() + quiet_timeout
    buf: List[bytes] = []
    try:
        channel.settimeout(chunk_timeout)
    except Exception:
        # If setting timeout fails, continue with defaults
        pass

    while True:
        got_data = False
        try:
            while channel.recv_ready():
                buf.append(channel.recv(65535))
                got_data = True
        except Exception:
            break

        if got_data:
            end_by = time.time() + quiet_timeout

        if time.time() > end_by:
            break

        time.sleep(0.05)

    return b"".join(buf).decode(errors="replace")

def _send_and_collect(channel: paramiko.Channel, cmd: str, settle: float = 0.12, quiet_timeout: float = 1.0) -> str:
    channel.send(cmd + "\n")
    time.sleep(settle)
    return _read_all(channel, quiet_timeout=quiet_timeout)

def _require_session(session_id: str) -> Dict[str, Any]:
    with _sessions_lock:
        s = sessions.get(session_id)
        if not s:
            raise HTTPException(status_code=400, detail="Invalid or expired session id. Call /open first.")
        ch: paramiko.Channel = s["channel"]
        if ch is None or getattr(ch, "closed", True):
            raise HTTPException(status_code=400, detail="SSH channel is not open. Call /open first.")
        return s

def run_sudo_when_prompted(channel: paramiko.Channel, user_cmd: str, sudo_pw: str) -> str:
    """
    Send sudo password ONLY when prompted.
    To detect the prompt deterministically (and avoid locale issues), we rewrite
    the command to include -S (read from stdin) and a custom prompt marker via -p.
    We still only send the password if the prompt appears.
    """
    assert user_cmd.startswith("sudo ")
    marker = "[SUDO-PROMPT]"
    cmd = f"sudo -S -p '{marker}:' {user_cmd[len('sudo '):]}"

    # Send the command
    channel.send(cmd + "\n")
    time.sleep(0.15)

    # Collect initial output
    out = _read_all(channel, quiet_timeout=1.0)

    # Only send the password if the marker appears
    if marker.lower() in out.lower():
        channel.send(sudo_pw + "\n")
        time.sleep(0.15)
        out += _read_all(channel, quiet_timeout=2.0)

    return out


# =============================================================================
# FastAPI Lifecycle (Lifespan API)
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleaner_task
    # Startup
    _cleaner_task = asyncio.create_task(_session_cleaner())
    try:
        yield
    finally:
        # Shutdown
        if _cleaner_task:
            _cleaner_task.cancel()
            try:
                await _cleaner_task
            except Exception:
                pass
        _cleaner_task = None
        # Close all sessions
        to_close: List[Dict[str, Any]] = []
        with _sessions_lock:
            for _, s in list(sessions.items()):
                to_close.append(s)
            sessions.clear()
        for s in to_close:
            _close_session_resources(s)


# =============================================================================
# Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def redirect_to_docs():
    """Redirects the root URL to the /docs documentation page."""
    return RedirectResponse(url="/docs")

@app.get("/health")
def health_check():
    with _sessions_lock:
        def _is_active(s: Dict[str, Any]) -> bool:
            client = s.get("client")
            if not client:
                return False
            try:
                transport = client.get_transport()
                return bool(transport and transport.is_active())
            except Exception:
                return False
        any_active = any(_is_active(s) for s in sessions.values())
        count = len(sessions)
    return {
        "status": "ok",
        "any_ssh_connected": any_active,
        "active_sessions": count,
        "session_ttl_seconds": SESSION_TTL_SECONDS,
    }

@app.post("/open")
async def open_connection(data: OpenRequest):
    if not data.host or not data.username or not data.password:
        raise HTTPException(status_code=400, detail="host, username, and password are required.")
    client = None
    channel = None
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=data.host,
            username=data.username,
            password=data.password,
            allow_agent=False,
            look_for_keys=False,
            timeout=20,
        )
        # Interactive shell (PTY) for sudo TTY behavior
        channel = client.invoke_shell(term="xterm")
        # invoke_shell already allocates a pty; keep try/except if the server behaves differently
        try:
            channel.get_pty()
        except Exception:
            pass
        time.sleep(0.2)

        # Optional: move to a directory, then enter login shell
        initial = ""
        if data.home_dir:
            initial += _send_and_collect(channel, f"cd {data.home_dir}", settle=0.1)
        initial += _send_and_collect(channel, "bash -l", settle=0.1)

        # Create session
        session_id = str(uuid.uuid4())
        session_obj = {
            "client": client,
            "channel": channel,
            "sudo_pw": data.password,
            "created_at": _now(),
            "last_seen": _now(),
            "host": data.host,
            "username": data.username,
        }

        with _sessions_lock:
            sessions[session_id] = session_obj

        return {"status": "connected", "output": initial, "id": session_id}

    except Exception as e:
        # Cleanup on failure
        try:
            if channel:
                channel.close()
        except Exception:
            pass
        try:
            if client:
                client.close()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Connection failed: {e}")

@app.post("/input")
async def post_input(data: CommandRequest):
    if not data.id:
        raise HTTPException(status_code=400, detail="Missing session id.")

    s = _require_session(data.id)
    ch: paramiko.Channel = s["channel"]
    sudo_pw: str = s["sudo_pw"]

    cmd = data.command.strip()
    if not cmd:
        raise HTTPException(status_code=400, detail="Empty command.")

    # Touch for activity (prevents early expiry)
    _touch_session(data.id)

    try:
        if cmd.startswith("sudo "):
            out = run_sudo_when_prompted(ch, cmd, sudo_pw)
        else:
            out = _send_and_collect(ch, cmd, settle=0.1, quiet_timeout=1.2)

        # Touch again after successful I/O
        _touch_session(data.id)

        return {"id": data.id, "command": cmd, "output": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/close")
async def close_connection(data: Optional[CloseRequest] = None):
    """
    Close a specific session if 'id' is provided; if omitted, closes all sessions.
    """
    try:
        if data and data.id:
            sid = data.id
            s = None
            with _sessions_lock:
                s = sessions.pop(sid, None)
            if not s:
                return {"status": "not_found", "id": sid}

            _close_session_resources(s)
            return {"status": "closed", "id": sid}
        else:
            # Close all
            to_close: List[Dict[str, Any]] = []
            with _sessions_lock:
                for _, s in sessions.items():
                    to_close.append(s)
                sessions.clear()

            for s in to_close:
                _close_session_resources(s)

            return {"status": "closed_all"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# =============================================================================
# Local dev entrypoint (optional)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )