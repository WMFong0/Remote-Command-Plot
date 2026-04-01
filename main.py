# main.py

"""FastAPI service for remote SSH command execution through interactive sessions.

Production-focused behaviors in this module include:
- Structured logging with request IDs
- Session lifecycle management with idle cleanup
- Bounded command length checks
- Explicit SSH error handling with stable API responses
"""

import time
import uuid
import asyncio
import threading
import shlex
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager
from logging import Logger

import paramiko
from paramiko.ssh_exception import AuthenticationException, SSHException
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
logger: Logger = setup_logging(settings)

# =============================================================================
# Models
# =============================================================================

class OpenRequest(BaseModel):
    """Payload for creating a new SSH interactive session."""

    host: str
    username: str
    password: str
    home_dir: Optional[str] = None


class CommandRequest(BaseModel):
    """Payload for sending a command to an existing session."""

    id: str
    command: str


class CloseRequest(BaseModel):
    """Payload for closing one or more active sessions."""

    id: Optional[str] = None
    force_inactive: Optional[bool] = False


# =============================================================================
# Session Store
# =============================================================================

# session_id -> session dict
sessions: Dict[str, Dict[str, Any]] = {}
_sessions_lock: threading.Lock = threading.Lock()

SESSION_TTL_SECONDS: int = settings.SESSION_TTL_SECONDS
CLEANER_INTERVAL_SECONDS: int = settings.CLEANER_INTERVAL_SECONDS
COMMAND_MAX_LENGTH: int = settings.COMMAND_MAX_LENGTH

_cleaner_task: Optional[asyncio.Task] = None


# =============================================================================
# Session helpers
# =============================================================================

def _touch_session(session_id: str) -> None:
    """Refresh the activity timestamp for a tracked SSH session.

    Args:
        session_id (str): Session identifier to mark as recently active.

    Returns:
        None: The session dictionary is updated in-place when present.
    """
    with _sessions_lock:
        s: Optional[Dict[str, Any]] = sessions.get(session_id)
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
    """Retrieve and validate a live session object by ID.

    Args:
        session_id (str): Session identifier supplied by the API caller.

    Returns:
        Dict[str, Any]: Valid session dictionary with an open SSH channel.
    """
    with _sessions_lock:
        s: Optional[Dict[str, Any]] = sessions.get(session_id)
        if not s:
            logger.warning("Invalid session id", extra={"session_id": session_id})
            raise HTTPException(400, "Invalid or expired session id")

        ch = s.get("channel")
        if not ch or getattr(ch, "closed", True):
            logger.warning("SSH channel not open", extra={"session_id": session_id})
            raise HTTPException(400, "SSH channel not open")

        return s


def _expire_idle_sessions() -> int:
    """Expire and close sessions that exceed the configured inactivity TTL.

    Args:
        None

    Returns:
        int: Number of sessions expired during the current sweep.
    """
    cutoff: float = now() - SESSION_TTL_SECONDS
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
    """Run periodic background cleanup for stale SSH sessions.

    Args:
        None

    Returns:
        None: Runs until cancellation during application shutdown.
    """
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
    """Manage application startup and shutdown lifecycle resources.

    Args:
        app (FastAPI): FastAPI application instance.

    Returns:
        None: Yield-based lifespan context for FastAPI runtime hooks.
    """
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

app: FastAPI = FastAPI(
    title=settings.APP_NAME,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    """Attach request correlation IDs for end-to-end traceability.

    Args:
        request (Request): Incoming FastAPI request.
        call_next (Callable): Next middleware/route handler in the chain.

    Returns:
        Response: Downstream response including `X-Request-ID` header.
    """
    request_id: str = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# =============================================================================
# Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def root():
    """Redirect service root to interactive API documentation.

    Args:
        None

    Returns:
        RedirectResponse: HTTP redirect response targeting `/docs`.
    """
    return RedirectResponse("/docs")


@app.get("/health")
def health(request: Request):
    """Provide health probe metadata and current session summary.

    Args:
        request (Request): Incoming request used for client IP logging context.

    Returns:
        Dict[str, Any]: Service health payload for probes and dashboards.
    """
    client_ip: str = get_client_ip(request)
    with _sessions_lock:
        active: bool = any(is_session_active(s) for s in sessions.values())
        count: int = len(sessions)

    payload: Dict[str, Any] = {
        "status": "ok",
        "any_ssh_connected": active,
        "active_sessions": count,
        "session_ttl_seconds": SESSION_TTL_SECONDS,
    }
    logger.debug("Health check", extra={**payload, "client_ip": client_ip})
    return payload


@app.get("/sessions")
def list_sessions():
    """List all tracked sessions with redacted host metadata.

    Args:
        None

    Returns:
        Dict[str, Any]: Session count and per-session public status details.
    """
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
    """Create a new SSH client, interactive channel, and tracked session.

    Args:
        data (OpenRequest): Connection payload with host, username, and password.
        request (Request): Incoming request used for access logging context.

    Returns:
        Dict[str, Any]: Connection status, session identifier, and initial shell output.
    """
    client_ip: str = get_client_ip(request)

    logger.info(
        "Attempting SSH connection",
        extra={
            "client_ip": client_ip,
            "username": data.username,
            "host": mask_host(data.host),
        },
    )

    client: paramiko.SSHClient = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(
            hostname=data.host,
            username=data.username,
            password=data.password,
            allow_agent=False,
            look_for_keys=False,
            timeout=settings.SSH_CONNECT_TIMEOUT,
        )
    except AuthenticationException as exc:
        logger.warning(
            "SSH authentication failed",
            extra={"client_ip": client_ip, "username": data.username, "host": mask_host(data.host), "error": str(exc)},
        )
        raise HTTPException(status_code=401, detail="SSH authentication failed") from exc
    except (SSHException, TimeoutError, OSError) as exc:
        logger.error(
            "SSH connection failed",
            extra={"client_ip": client_ip, "username": data.username, "host": mask_host(data.host), "error": str(exc)},
        )
        raise HTTPException(status_code=502, detail="Unable to establish SSH connection") from exc

    # Keepalive
    try:
        t: Optional[paramiko.Transport] = client.get_transport()
        if t:
            t.set_keepalive(settings.SSH_KEEPALIVE_SECONDS)
    except Exception:
        pass

    try:
        channel: paramiko.Channel = client.invoke_shell(term="xterm")
    except Exception as exc:
        client.close()
        raise HTTPException(status_code=502, detail="Failed to open interactive SSH shell") from exc

    time.sleep(0.25)

    initial: str = await_shell_ready(channel, timeout=settings.SSH_READY_TIMEOUT, logger=logger)

    if data.home_dir:
        safe_dir: str = shlex.quote(data.home_dir)
        initial += send_and_collect(
            channel, f"cd {safe_dir} || echo 'cd_failed:$PWD'", logger=logger
        )

    session_id: str = str(uuid.uuid4())
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
    """Execute a command against an existing interactive SSH session.

    Args:
        data (CommandRequest): Session ID and command payload.
        request (Request): Incoming request used for client metadata logging.

    Returns:
        Dict[str, Any]: Echoed command metadata and captured command output.
    """
    client_ip: str = get_client_ip(request)
    s: Dict[str, Any] = _require_session(data.id)
    _touch_session(data.id)

    cmd: str = data.command.strip()
    if not cmd:
        raise HTTPException(400, "Empty command")
    if len(cmd) > COMMAND_MAX_LENGTH:
        raise HTTPException(400, f"Command exceeds max length ({COMMAND_MAX_LENGTH})")

    logger.info(
        "Executing command",
        extra={
            "session_id": data.id,
            "client_ip": client_ip,
            "command": cmd[:200],
        },
    )

    if cmd.startswith("sudo "):
        out: str = run_sudo_when_prompted(
            s["channel"], cmd, s["sudo_pw"], logger=logger
        )
    else:
        out = send_and_collect(s["channel"], cmd, logger=logger)

    _touch_session(data.id)
    return {"id": data.id, "command": cmd, "output": out}


@app.post("/close")
async def close_connection(data: Optional[CloseRequest] = None, request: Optional[Request] = None):
    """Close specific, inactive, or all SSH sessions based on request intent.

    Args:
        data (Optional[CloseRequest], optional): Closure options including target ID
            and inactive-only mode. Defaults to None.
        request (Optional[Request], optional): Incoming request for logging context.
            Defaults to None.

    Returns:
        Dict[str, Any]: Closure result payload indicating action and affected session IDs.
    """
    client_ip: str = get_client_ip(request) if request else "unknown"
    force: bool = bool(data and data.force_inactive)

    def close_inactive() -> List[str]:
        """Close and remove sessions whose transport is no longer active.

        Args:
            None

        Returns:
            List[str]: Session IDs closed during the inactive cleanup.
        """
        closed: List[str] = []
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
                host=settings.APP_HOST,
                port=settings.APP_PORT,
                reload=settings.APP_RELOAD
                )
