# helper.py

import time
from typing import Optional, List, Dict, Any
from logging import Logger

import paramiko
from fastapi import Request


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def now() -> float:
    """Return current epoch time (seconds)."""
    return time.time()


# -----------------------------------------------------------------------------
# Network / privacy helpers
# -----------------------------------------------------------------------------
def mask_host(host: str) -> str:
    """
    Mask IP/host for privacy in logs.
    - IPv4: '10.123.45.67' -> '10.1*3.4*5.6*7'
    - Hostname: keep first/last 2 chars if long, else '***'
    """
    try:
        if not host:
            return host
        parts = host.split(".")
        if len(parts) == 4 and all(part.isdigit() for part in parts):
            return ".".join(p if len(p) <= 2 else (p[0] + "*" + p[-1]) for p in parts)
        return host[:2] + "***" + host[-2:] if len(host) > 6 else "***"
    except Exception:
        return "***"


def get_client_ip(request: Request) -> str:
    """
    Best-effort client IP extraction with proxy awareness.
    Prefers 'X-Forwarded-For' then falls back to request.client.host.
    """
    try:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        if request.client:
            return request.client.host
    except Exception:
        pass
    return "unknown"


# -----------------------------------------------------------------------------
# Paramiko / SSH session helpers
# -----------------------------------------------------------------------------
def is_client_active(client: Optional[paramiko.SSHClient]) -> bool:
    """Check if the SSH client's transport is active."""
    if not client:
        return False
    try:
        transport = client.get_transport()
        return bool(transport and transport.is_active())
    except Exception:
        return False


def is_session_active(s: Dict[str, Any]) -> bool:
    """Check if a session dict has an active SSH client transport."""
    return is_client_active(s.get("client"))


def close_session_resources(s: Dict[str, Any], logger: Optional[Logger] = None) -> None:
    """
    Safely close the Paramiko channel and client in a session object.
    """
    sid = s.get("id")
    host = s.get("host")
    username = s.get("username")

    try:
        ch = s.get("channel")
        if ch:
            ch.close()
            if logger:
                logger.debug("SSH channel closed", extra={"session_id": sid, "host": mask_host(host), "username": username})
    except Exception as e:
        if logger:
            logger.warning("Error closing channel", extra={"error": str(e), "session_id": sid})

    try:
        cl = s.get("client")
        if cl:
            cl.close()
            if logger:
                logger.debug("SSH client closed", extra={"session_id": sid, "host": mask_host(host), "username": username})
    except Exception as e:
        if logger:
            logger.warning("Error closing SSH client", extra={"error": str(e), "session_id": sid})


def read_all(
    channel: paramiko.Channel,
    quiet_timeout: float = 1.0,
    chunk_timeout: float = 0.2,
    logger: Optional[Logger] = None,
) -> str:
    """
    Read from the interactive channel until no new data arrives for ~quiet_timeout.
    """
    end_by = time.time() + quiet_timeout
    buf: List[bytes] = []
    try:
        channel.settimeout(chunk_timeout)
    except Exception:
        pass

    while True:
        got_data = False
        try:
            while channel.recv_ready():
                buf.append(channel.recv(65535))
                got_data = True
        except Exception as e:
            if logger:
                logger.debug("Channel recv error", extra={"error": str(e)})
            break

        if got_data:
            end_by = time.time() + quiet_timeout

        if time.time() > end_by:
            break

        time.sleep(0.05)

    return b"".join(buf).decode(errors="replace")


def send_and_collect(
    channel: paramiko.Channel,
    cmd: str,
    settle: float = 0.12,
    quiet_timeout: float = 1.0,
    logger: Optional[Logger] = None,
) -> str:
    """
    Send a command to the interactive shell and collect output after a short settle time.
    """
    if getattr(channel, "closed", False):
        if logger:
            logger.error("Attempted to send on a closed channel")
        raise RuntimeError("SSH channel is closed by remote")
    if logger:
        logger.debug("Sending command", extra={"command": cmd})
    channel.send(cmd + "\n")
    time.sleep(settle)
    out = read_all(channel, quiet_timeout=quiet_timeout, logger=logger)
    if logger:
        logger.debug("Command output collected", extra={"bytes": len(out)})
    return out


def await_shell_ready(
    channel: paramiko.Channel,
    timeout: float = 6.0,
    logger: Optional[Logger] = None,
) -> str:
    """
    Ensure the interactive shell is ready by sending a readiness marker and waiting for it.
    Returns the accumulated output read during readiness wait.
    """
    # Flush any initial banner/motd
    _ = read_all(channel, quiet_timeout=0.3, logger=logger)
    marker = "__RC_READY__"
    try:
        channel.send(f"echo {marker}\n")
    except Exception as e:
        if logger:
            logger.error("Failed to send readiness marker; channel likely closed", extra={"error": str(e)})
        raise

    end_by = time.time() + timeout
    acc = ""
    while time.time() < end_by:
        chunk = read_all(channel, quiet_timeout=0.5, logger=logger)
        if chunk:
            acc += chunk
            if marker in chunk:
                if logger:
                    logger.debug("Interactive shell is ready")
                return acc
        time.sleep(0.05)

    if logger:
        logger.error("Timed out waiting for interactive shell readiness")
    raise RuntimeError("Interactive shell did not become ready in time")


def run_sudo_when_prompted(
    channel: paramiko.Channel,
    user_cmd: str,
    sudo_pw: str,
    logger: Optional[Logger] = None,
) -> str:
    """
    Send sudo password ONLY when prompted.
    To detect the prompt deterministically (and avoid locale issues), we rewrite
    the command to include -S (read from stdin) and a custom prompt marker via -p.
    We still only send the password if the prompt appears.
    """
    assert user_cmd.startswith("sudo ")
    marker = "[SUDO-PROMPT]"
    cmd = f"sudo -S -p '{marker}:' {user_cmd[len('sudo '):]}"

    if logger:
        logger.info("Executing sudo command", extra={"sudo": True, "command": user_cmd[:200]})

    channel.send(cmd + "\n")
    time.sleep(0.15)

    out = read_all(channel, quiet_timeout=1.0, logger=logger)

    if marker.lower() in out.lower():
        if logger:
            logger.debug("Sudo prompt detected; sending password securely (not logged)")
        channel.send(sudo_pw + "\n")
        time.sleep(0.15)
        out += read_all(channel, quiet_timeout=2.0, logger=logger)
    else:
        if logger:
            logger.debug("Sudo prompt not detected; password not sent")

    return out