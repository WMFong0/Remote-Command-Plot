# main.py

import os
import time
import uuid
import asyncio
import threading
import json
import shlex
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager
from datetime import datetime

import paramiko
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv

import logging
from logging.handlers import RotatingFileHandler

# Load environment variables if present (optional)
load_dotenv()

# =============================================================================
# Logging Setup
# =============================================================================

def _ensure_dir(path: str) -> None:
    try:
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
    except Exception:
        pass

class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter for ingestion by log systems."""
    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        req_id = getattr(record, "request_id", None)
        if req_id:
            log_obj["request_id"] = req_id
        return json.dumps(log_obj, ensure_ascii=False)

def setup_logging() -> logging.Logger:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "logs/app.log")
    log_max_bytes = int(os.getenv("LOG_MAX_BYTES", "5000000"))  # ~5MB default
    log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", "3"))  # keep 3 backups
    use_json = os.getenv("LOG_JSON", "0") in ("1", "true", "True")

    _ensure_dir(log_file)

    logger = logging.getLogger("remote_command_plot")
    logger.setLevel(log_level)
    logger.propagate = False

    # Remove existing handlers (useful for reload=True)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=log_max_bytes, backupCount=log_backup_count
    )
    console_handler = logging.StreamHandler()

    formatter = JsonFormatter() if use_json else logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(module)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Align uvicorn loggers too
    for uv_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(uv_name).setLevel(log_level)

    logger.info(
        "Logger initialized",
        extra={
            "config": {
                "level": log_level,
                "file": log_file,
                "max_bytes": log_max_bytes,
                "backup_count": log_backup_count,
                "json": use_json,
            }
        },
    )
    return logger

logger = setup_logging()

# =============================================================================
# Models
# =============================================================================

class OpenRequest(BaseModel):
    host: str
    username: str
    password: str
    home_dir: Optional[str] = None  # optional override directory


class CommandRequest(BaseModel):
    id: str
    command: str


class CloseRequest(BaseModel):
    id: Optional[str] = None  # optional; if omitted, closes all sessions (unless force_inactive is true)
    force_inactive: Optional[bool] = False  # when true, close only inactive sessions if id omitted or not found


# =============================================================================
# Session Store & Config
# =============================================================================

# session_id -> { client, channel, sudo_pw, created_at, last_seen, host, username, id }
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
            logger.debug(
                "Session touched",
                extra={"session_id": session_id, "username": s.get("username"), "host": _mask_host(s.get("host", ""))}
            )

def _close_session_resources(s: Dict[str, Any]) -> None:
    """Safely close channel and client."""
    sid = s.get("id")
    host = s.get("host")
    username = s.get("username")
    try:
        ch = s.get("channel")
        if ch:
            ch.close()
            logger.debug("SSH channel closed", extra={"session_id": sid, "host": _mask_host(host), "username": username})
    except Exception as e:
        logger.warning("Error closing channel", extra={"error": str(e), "session_id": sid})
    try:
        cl = s.get("client")
        if cl:
            cl.close()
            logger.debug("SSH client closed", extra={"session_id": sid, "host": _mask_host(host), "username": username})
    except Exception as e:
        logger.warning("Error closing SSH client", extra={"error": str(e), "session_id": sid})

def _is_client_active(client: Optional[paramiko.SSHClient]) -> bool:
    if not client:
        return False
    try:
        transport = client.get_transport()
        return bool(transport and transport.is_active())
    except Exception:
        return False

def _is_session_active(s: Dict[str, Any]) -> bool:
    return _is_client_active(s.get("client"))

def _expire_idle_sessions() -> int:
    """Close and remove sessions idle beyond SESSION_TTL_SECONDS. Returns count removed."""
    cutoff = _now() - SESSION_TTL_SECONDS
    removed: int = 0
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
        logger.info(
            "Expiring idle session",
            extra={"session_id": s.get("id"), "host": _mask_host(s.get("host", "")), "username": s.get("username")}
        )
        _close_session_resources(s)
    if removed:
        logger.info("Expired idle sessions", extra={"count": removed})
    return removed

async def _session_cleaner():
    """Background task: periodically expire idle sessions."""
    logger.info(
        "Session cleaner started",
        extra={"interval_seconds": CLEANER_INTERVAL_SECONDS, "ttl_seconds": SESSION_TTL_SECONDS}
    )
    while True:
        try:
            expired = _expire_idle_sessions()
            logger.debug("Cleaner sweep done", extra={"expired": expired, "active_sessions": len(sessions)})
        except Exception:
            logger.exception("Cleaner encountered an error")
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
        except Exception as e:
            logger.debug("Channel recv error", extra={"error": str(e)})
            break

        if got_data:
            end_by = time.time() + quiet_timeout

        if time.time() > end_by:
            break

        time.sleep(0.05)

    out = b"".join(buf).decode(errors="replace")
    return out

def _send_and_collect(channel: paramiko.Channel, cmd: str, settle: float = 0.12, quiet_timeout: float = 1.0) -> str:
    if getattr(channel, "closed", False):
        logger.error("Attempted to send on a closed channel")
        raise RuntimeError("SSH channel is closed by remote")
    logger.debug("Sending command", extra={"command": cmd})
    channel.send(cmd + "\n")
    time.sleep(settle)
    out = _read_all(channel, quiet_timeout=quiet_timeout)
    logger.debug("Command output collected", extra={"bytes": len(out)})
    return out

def _require_session(session_id: str) -> Dict[str, Any]:
    with _sessions_lock:
        s = sessions.get(session_id)
        if not s:
            logger.warning("Invalid session request", extra={"session_id": session_id})
            raise HTTPException(status_code=400, detail="Invalid or expired session id. Call /open first.")
        ch: paramiko.Channel = s["channel"]
        if ch is None or getattr(ch, "closed", True):
            logger.warning("SSH channel not open", extra={"session_id": session_id})
            raise HTTPException(status_code=400, detail="SSH channel is not open. Call /open first.")
        return s

def _mask_host(host: str) -> str:
    """Mask IP/host for privacy in logs."""
    try:
        if not host:
            return host
        parts = host.split(".")
        if len(parts) == 4 and all(part.isdigit() for part in parts):
            return ".".join(p if len(p) <= 2 else (p[0] + "*" + p[-1]) for p in parts)
        # hostname form
        return host[:2] + "***" + host[-2:] if len(host) > 6 else "***"
    except Exception:
        return "***"

def _get_client_ip(request: Request) -> str:
    """Best-effort client IP extraction with proxy awareness."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"

def _await_shell_ready(channel: paramiko.Channel, timeout: float = 6.0) -> str:
    """
    Ensure the interactive shell is ready by sending a readiness marker and waiting for it.
    Returns the accumulated output read during readiness wait.
    """
    # Flush any initial banner/motd
    _ = _read_all(channel, quiet_timeout=0.3)
    marker = "__RC_READY__"
    try:
        channel.send(f"echo {marker}\n")
    except Exception as e:
        logger.error("Failed to send readiness marker; channel likely closed", extra={"error": str(e)})
        raise

    end_by = time.time() + timeout
    acc = ""
    while time.time() < end_by:
        chunk = _read_all(channel, quiet_timeout=0.5)
        if chunk:
            acc += chunk
            if marker in chunk:
                logger.debug("Interactive shell is ready")
                return acc
        time.sleep(0.05)
    logger.error("Timed out waiting for interactive shell readiness")
    raise RuntimeError("Interactive shell did not become ready in time")

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

    logger.info("Executing sudo command", extra={"sudo": True, "command": user_cmd[:200]})

    # Send the command
    channel.send(cmd + "\n")
    time.sleep(0.15)

    # Collect initial output
    out = _read_all(channel, quiet_timeout=1.0)

    # Only send the password if the marker appears
    if marker.lower() in out.lower():
        logger.debug("Sudo prompt detected; sending password securely (not logged)")
        channel.send(sudo_pw + "\n")
        time.sleep(0.15)
        out += _read_all(channel, quiet_timeout=2.0)
    else:
        logger.debug("Sudo prompt not detected; password not sent")

    return out


# =============================================================================
# FastAPI Lifecycle (Lifespan API)
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleaner_task
    # Startup
    _cleaner_task = asyncio.create_task(_session_cleaner())
    logger.info("Application startup complete")
    try:
        yield
    finally:
        # Shutdown
        logger.info("Application shutdown initiated")
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
            logger.info(
                "Closing session on shutdown",
                extra={"host": _mask_host(s.get("host", "")), "username": s.get("username")}
            )
            _close_session_resources(s)
        logger.info("Application shutdown complete")

# Instantiate FastAPI AFTER lifespan is defined
app = FastAPI(
    title="Remote Command Plot API",
    description="For AS Watson GIT Use only",
    version="1.0.0",
    lifespan=lifespan,
)

# Request ID middleware (kept; no client IP logging here per Option B)
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# =============================================================================
# Routes
# =============================================================================

@app.get("/", include_in_schema=False)
async def redirect_to_docs():
    """Redirects the root URL to the /docs documentation page."""
    return RedirectResponse(url="/docs")

@app.get("/health")
def health_check(request: Request):
    client_ip = _get_client_ip(request)
    with _sessions_lock:
        any_active = any(_is_session_active(s) for s in sessions.values())
        count = len(sessions)
    payload = {
        "status": "ok",
        "any_ssh_connected": any_active,
        "active_sessions": count,
        "session_ttl_seconds": SESSION_TTL_SECONDS,
    }
    logger.debug("Health check", extra={**payload, "client_ip": client_ip})
    return payload

@app.get("/sessions")
def list_sessions():
    """List current session ids with minimal safe metadata for debugging."""
    with _sessions_lock:
        items = []
        for sid, s in sessions.items():
            active = _is_session_active(s)
            items.append({
                "id": sid,
                "username": s.get("username"),
                "host": _mask_host(s.get("host", "")),
                "active": active,
                "last_seen": s.get("last_seen"),
                "created_at": s.get("created_at"),
            })
    return {"count": len(items), "sessions": items}

@app.post("/open")
async def open_connection(data: OpenRequest, request: Request):
    client_ip = _get_client_ip(request)
    if not data.host or not data.username or not data.password:
        logger.warning("Open connection missing required fields", extra={"client_ip": client_ip})
        raise HTTPException(status_code=400, detail="host, username, and password are required.")
    client = None
    channel = None
    masked_host = _mask_host(data.host)
    try:
        logger.info("Attempting SSH connection", extra={"host": masked_host, "username": data.username, "client_ip": client_ip})
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
        logger.info("SSH connection established", extra={"host": masked_host, "username": data.username, "client_ip": client_ip})

        # Keepalive helps avoid early idle closures on some networks
        try:
            transport = client.get_transport()
            if transport:
                transport.set_keepalive(30)
        except Exception:
            pass

        # Interactive shell (PTY) for sudo TTY behavior
        channel = client.invoke_shell(term="xterm")
        time.sleep(0.25)

        # Wait for interactive shell readiness by echoing a marker
        try:
            initial = _await_shell_ready(channel, timeout=6.0)
        except Exception as e:
            logger.warning("Shell readiness check failed; continuing", extra={"error": str(e)})
            initial = ""

        # Optional: change to a directory (quote safely). Do NOT run another login shell.
        if data.home_dir:
            safe_dir = shlex.quote(data.home_dir)
            logger.debug("Changing directory", extra={"dir": data.home_dir, "client_ip": client_ip})
            initial += _send_and_collect(channel, f"cd {safe_dir} || echo 'cd_failed:$PWD'", settle=0.12)

        # Create session
        session_id = str(uuid.uuid4())
        session_obj = {
            "client": client,
            "channel": channel,
            "sudo_pw": data.password,  # never logged
            "created_at": _now(),
            "last_seen": _now(),
            "host": data.host,
            "username": data.username,
            "id": session_id,
        }

        with _sessions_lock:
            sessions[session_id] = session_obj

        logger.info("Session created", extra={"session_id": session_id, "host": masked_host, "username": data.username, "client_ip": client_ip})
        return {"status": "connected", "output": initial, "id": session_id}

    except Exception as e:
        logger.exception("Connection failed", extra={"host": masked_host, "username": data.username, "client_ip": client_ip})
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
async def post_input(data: CommandRequest, request: Request):
    client_ip = _get_client_ip(request)
    if not data.id:
        logger.warning("Input missing session id", extra={"client_ip": client_ip})
        raise HTTPException(status_code=400, detail="Missing session id.")

    s = _require_session(data.id)
    ch: paramiko.Channel = s["channel"]
    sudo_pw: str = s["sudo_pw"]

    cmd = data.command.strip()
    if not cmd:
        logger.warning("Input empty command", extra={"session_id": data.id, "client_ip": client_ip})
        raise HTTPException(status_code=400, detail="Empty command.")

    # Touch for activity (prevents early expiry)
    _touch_session(data.id)

    try:
        logger.info("Executing command", extra={"session_id": data.id, "command": cmd[:200], "client_ip": client_ip})
        if cmd.startswith("sudo "):
            out = run_sudo_when_prompted(ch, cmd, sudo_pw)
        else:
            out = _send_and_collect(ch, cmd, settle=0.1, quiet_timeout=1.2)

        # Touch again after successful I/O
        _touch_session(data.id)

        logger.debug("Command executed", extra={"session_id": data.id, "output_bytes": len(out), "client_ip": client_ip})
        return {"id": data.id, "command": cmd, "output": out}
    except Exception:
        logger.exception("Command execution failed", extra={"session_id": data.id, "command": cmd[:200], "client_ip": client_ip})
        raise HTTPException(status_code=500, detail="Command execution failed")

@app.post("/close")
async def close_connection(data: Optional[CloseRequest] = None, request: Request = None):
    """
    Close a specific session if 'id' is provided; if omitted:
      - if force_inactive=True, close only inactive sessions;
      - else, close all sessions.
    If id is provided but not found:
      - if force_inactive=True, close all inactive sessions and return diagnostics;
      - else, return not_found with diagnostics.
    """
    client_ip = _get_client_ip(request) if request else "unknown"
    force_inactive = bool(data and data.force_inactive)
    try:
        # Helper to close all inactive sessions and return their IDs
        def _close_inactive_sessions() -> List[str]:
            to_close_ids: List[str] = []
            to_close_objs: List[Dict[str, Any]] = []
            with _sessions_lock:
                for sid, s in list(sessions.items()):
                    if not _is_session_active(s):
                        to_close_ids.append(sid)
                        to_close_objs.append(sessions.pop(sid))
            logger.info("Closing inactive sessions", extra={"count": len(to_close_ids), "client_ip": client_ip})
            for s in to_close_objs:
                _close_session_resources(s)
            return to_close_ids

        if data and data.id:
            sid = data.id.strip()
            with _sessions_lock:
                s = sessions.pop(sid, None)
            if not s:
                # Diagnostics about known sessions
                with _sessions_lock:
                    diag = []
                    for k, v in sessions.items():
                        diag.append({
                            "id": k,
                            "host": _mask_host(v.get("host", "")),
                            "username": v.get("username"),
                            "active": _is_session_active(v),
                        })
                if force_inactive:
                    closed_ids = _close_inactive_sessions()
                    logger.info(
                        "Close requested for non-existent session; closed inactive instead",
                        extra={"requested_id": sid, "client_ip": client_ip, "closed_inactive": closed_ids}
                    )
                    return {
                        "status": "not_found_closed_inactive",
                        "id": sid,
                        "closed_inactive_ids": closed_ids,
                        "known_sessions": diag,
                    }
                else:
                    logger.info("Close requested for non-existent session", extra={"requested_id": sid, "client_ip": client_ip, "known_sessions": diag})
                    return {"status": "not_found", "id": sid, "known_sessions": diag}

            logger.info("Closing session", extra={"session_id": sid, "host": _mask_host(s.get("host", "")), "username": s.get("username"), "client_ip": client_ip})
            _close_session_resources(s)
            return {"status": "closed", "id": sid}

        else:
            # No id provided
            if force_inactive:
                closed_ids = _close_inactive_sessions()
                return {"status": "closed_inactive", "closed_inactive_ids": closed_ids}
            # Close all sessions
            to_close: List[Dict[str, Any]] = []
            with _sessions_lock:
                for _, s in sessions.items():
                    to_close.append(s)
                sessions.clear()

            logger.info("Closing all sessions", extra={"count": len(to_close), "client_ip": client_ip})
            for s in to_close:
                _close_session_resources(s)

            return {"status": "closed_all"}
    except Exception:
        logger.exception("Error while closing session(s)", extra={"client_ip": client_ip})
        return {"status": "error", "message": "Error while closing session(s)"}


# =============================================================================
# Local dev entrypoint (optional)
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn (dev entrypoint)")
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )