# main.py

import time
import uuid
import asyncio
import threading
import shlex
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

import paramiko
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv

# ---- Local modules ----
from config import load_settings, Settings
from app_logging import setup_logging
from helper import (
    now,
    mask_host,
    get_client_ip,
    is_session_active,
    close_session_resources,
    send_and_collect,
    await_shell_ready,
    run_sudo_when_prompted,
)

# Load environment variables
load_dotenv()

# Load settings & logger
settings: Settings = load_settings()
logger = setup_logging(settings)

# =============================================================================
# Models
# =============================================================================

class OpenRequest(BaseModel):
    host: str
    username: str
    password: str
    home_dir: Optional[str] = None


class CommandRequest(BaseModel):
    id: str
    command: str


class CloseRequest(BaseModel):
    id: Optional[str] = None
    force_inactive: Optional[bool] = False


# =============================================================================
# Session Store
# =============================================================================

# session_id -> session dict
sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock = threading.Lock()

SESSION_TTL_SECONDS = settings.SESSION_TTL_SECONDS
CLEANER_INTERVAL_SECONDS = settings.CLEANER_INTERVAL_SECONDS

_cleaner_task: Optional[asyncio.Task] = None


# =============================================================================
# Session helpers
# =============================================================================

def _touch_session(session_id: str) -> None:
    with _sessions_lock:
        s = sessions.get(session_id)
        if s:
            s["last_seen"] = now()
            logger.debug(
                "Session touched",
                extra={
                    "session_id": session_id,
                    "username": s.get("username"),
                    "host": mask_host(s.get("host", "")),
                },
            )


def _require_session(session_id: str) -> Dict[str, Any]:
    with _sessions_lock:
        s = sessions.get(session_id)
        if not s:
            logger.warning("Invalid session id", extra={"session_id": session_id})
            raise HTTPException(400, "Invalid or expired session id")

        ch = s.get("channel")
        if not ch or getattr(ch, "closed", True):
            logger.warning("SSH channel not open", extra={"session_id": session_id})
            raise HTTPException(400, "SSH channel not open")

        return s


def _expire_idle_sessions() -> int:
    cutoff = now() - SESSION_TTL_SECONDS
    expired: List[Dict[str, Any]] = []

    with _sessions_lock:
        for sid, s in list(sessions.items()):
            if s.get("last_seen", 0) < cutoff:
                expired.append(sessions.pop(sid))

    for s in expired:
        logger.info(
            "Expiring idle session",
            extra={
                "session_id": s.get("id"),
                "username": s.get("username"),
                "host": mask_host(s.get("host", "")),
            },
        )
        close_session_resources(s, logger)

    return len(expired)


async def _session_cleaner():
    logger.info(
        "Session cleaner started",
        extra={
            "ttl_seconds": SESSION_TTL_SECONDS,
            "interval_seconds": CLEANER_INTERVAL_SECONDS,
        },
    )
    while True:
        try:
            removed = _expire_idle_sessions()
            logger.debug("Cleaner sweep done", extra={"expired": removed})
        except Exception:
            logger.exception("Cleaner error")
        await asyncio.sleep(CLEANER_INTERVAL_SECONDS)


# =============================================================================
# FastAPI lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleaner_task
    _cleaner_task = asyncio.create_task(_session_cleaner())
    logger.info("Application startup complete")
    try:
        yield
    finally:
        logger.info("Application shutdown initiated")
        if _cleaner_task:
            _cleaner_task.cancel()
            try:
                await _cleaner_task
            except Exception:
                pass

        with _sessions_lock:
            all_sessions = list(sessions.values())
            sessions.clear()

        for s in all_sessions:
            close_session_resources(s, logger)

        logger.info("Application shutdown complete")


# =============================================================================
# App
# =============================================================================

app = FastAPI(
    title="Remote Command Plot API",
    description="For AS Watson GIT Use only",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# =============================================================================
# Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.get("/health")
def health(request: Request):
    client_ip = get_client_ip(request)
    with _sessions_lock:
        active = any(is_session_active(s) for s in sessions.values())
        count = len(sessions)

    payload = {
        "status": "ok",
        "any_ssh_connected": active,
        "active_sessions": count,
        "session_ttl_seconds": SESSION_TTL_SECONDS,
    }
    logger.debug("Health check", extra={**payload, "client_ip": client_ip})
    return payload


@app.get("/sessions")
def list_sessions():
    with _sessions_lock:
        return {
            "count": len(sessions),
            "sessions": [
                {
                    "id": sid,
                    "username": s.get("username"),
                    "host": mask_host(s.get("host", "")),
                    "active": is_session_active(s),
                    "last_seen": s.get("last_seen"),
                }
                for sid, s in sessions.items()
            ],
        }


@app.post("/open")
async def open_connection(data: OpenRequest, request: Request):
    client_ip = get_client_ip(request)

    logger.info(
        "Attempting SSH connection",
        extra={
            "client_ip": client_ip,
            "username": data.username,
            "host": mask_host(data.host),
        },
    )

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

    # Keepalive
    try:
        t = client.get_transport()
        if t:
            t.set_keepalive(30)
    except Exception:
        pass

    channel = client.invoke_shell(term="xterm")
    time.sleep(0.25)

    initial = await_shell_ready(channel, logger=logger)

    if data.home_dir:
        safe_dir = shlex.quote(data.home_dir)
        initial += send_and_collect(
            channel, f"cd {safe_dir} || echo 'cd_failed:$PWD'", logger=logger
        )

    session_id = str(uuid.uuid4())
    with _sessions_lock:
        sessions[session_id] = {
            "id": session_id,
            "client": client,
            "channel": channel,
            "sudo_pw": data.password,
            "created_at": now(),
            "last_seen": now(),
            "host": data.host,
            "username": data.username,
        }

    logger.info(
        "Session created",
        extra={
            "session_id": session_id,
            "client_ip": client_ip,
            "username": data.username,
            "host": mask_host(data.host),
        },
    )

    return {"status": "connected", "id": session_id, "output": initial}


@app.post("/input")
async def post_input(data: CommandRequest, request: Request):
    client_ip = get_client_ip(request)
    s = _require_session(data.id)
    _touch_session(data.id)

    cmd = data.command.strip()
    if not cmd:
        raise HTTPException(400, "Empty command")

    logger.info(
        "Executing command",
        extra={
            "session_id": data.id,
            "client_ip": client_ip,
            "command": cmd[:200],
        },
    )

    if cmd.startswith("sudo "):
        out = run_sudo_when_prompted(
            s["channel"], cmd, s["sudo_pw"], logger=logger
        )
    else:
        out = send_and_collect(s["channel"], cmd, logger=logger)

    _touch_session(data.id)
    return {"id": data.id, "command": cmd, "output": out}


@app.post("/close")
async def close_connection(data: Optional[CloseRequest] = None, request: Request = None):
    client_ip = get_client_ip(request) if request else "unknown"
    force = bool(data and data.force_inactive)

    def close_inactive() -> List[str]:
        closed = []
        with _sessions_lock:
            for sid, s in list(sessions.items()):
                if not is_session_active(s):
                    closed.append(sid)
                    close_session_resources(sessions.pop(sid), logger)
        return closed

    if data and data.id:
        with _sessions_lock:
            s = sessions.pop(data.id, None)
        if s:
            close_session_resources(s, logger)
            logger.info(
                "Session closed",
                extra={"session_id": data.id, "client_ip": client_ip},
            )
            return {"status": "closed", "id": data.id}

        if force:
            return {
                "status": "closed_inactive",
                "ids": close_inactive(),
            }

        return {"status": "not_found", "id": data.id}

    if force:
        return {"status": "closed_inactive", "ids": close_inactive()}

    with _sessions_lock:
        all_sessions = list(sessions.values())
        sessions.clear()

    for s in all_sessions:
        close_session_resources(s, logger)

    logger.info("All sessions closed", extra={"client_ip": client_ip})
    return {"status": "closed_all"}


# =============================================================================
# Entrypoint
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn (dev)")
    uvicorn.run("main:app",
                host="0.0.0.0",
                port=8000,
                reload=True
                )