# helper.py

"""Shared helper utilities for time, request metadata, and SSH channel I/O.

This module intentionally isolates low-level SSH/session logic from route
handlers to keep endpoint code concise and easier to reason about.
"""

import time
from typing import Optional, List, Dict, Any
from logging import Logger

import paramiko
from fastapi import Request


SessionDict = Dict[str, Any]


# -----------------------------------------------------------------------------
# Time helpers
# -----------------------------------------------------------------------------
def now() -> float:
    """Return the current UNIX epoch timestamp.

    Args:
        None

    Returns:
        float: Current UTC epoch time in seconds.
    """
    return time.time()


# -----------------------------------------------------------------------------
# Network / privacy helpers
# -----------------------------------------------------------------------------
def mask_host(host: str) -> str:
    """Mask host or IP values before writing them to logs.

    Args:
        host (str): Raw hostname or IPv4 address.

    Returns:
        str: Redacted host string that preserves limited structure for debugging.
    """
    try:
        if not host:
            return host
        parts: List[str] = host.split(".")
        if len(parts) == 4 and all(part.isdigit() for part in parts):
            return ".".join(p if len(p) <= 2 else (p[0] + "*" + p[-1]) for p in parts)
        return host[:2] + "***" + host[-2:] if len(host) > 6 else "***"
    except Exception:
        return "***"


def get_client_ip(request: Request) -> str:
    """Extract the best-available client IP address from an HTTP request.

    Args:
        request (Request): Incoming FastAPI request object.

    Returns:
        str: First `X-Forwarded-For` hop when present, otherwise socket peer IP,
            or `"unknown"` if unavailable.
    """
    try:
        xff: Optional[str] = request.headers.get("x-forwarded-for")
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
    """Determine whether a Paramiko SSH client has an active transport.

    Args:
        client (Optional[paramiko.SSHClient]): SSH client instance.

    Returns:
        bool: `True` when transport exists and is active, otherwise `False`.
    """
    if not client:
        return False
    try:
        transport = client.get_transport()
        return bool(transport and transport.is_active())
    except Exception:
        return False


def is_session_active(s: SessionDict) -> bool:
    """Check whether a session dictionary represents an active SSH session.

    Args:
        s (SessionDict): Session object containing at least a `client` key.

    Returns:
        bool: `True` when the underlying SSH client transport is active.
    """
    return is_client_active(s.get("client"))


def close_session_resources(s: SessionDict, logger: Optional[Logger] = None) -> None:
    """Close all SSH resources in a session dictionary safely and idempotently.

    Args:
        s (SessionDict): Session dictionary containing Paramiko client/channel objects.
        logger (Optional[Logger], optional): Logger used for operational events.
            Defaults to None.

    Returns:
        None: This helper performs side effects only.
    """
    sid: Optional[str] = s.get("id")
    host: Optional[str] = s.get("host")
    username: Optional[str] = s.get("username")

    try:
        ch: Optional[paramiko.Channel] = s.get("channel")
        if ch:
            ch.close()
            if logger:
                logger.debug("SSH channel closed", extra={"session_id": sid, "host": mask_host(host), "username": username})
    except Exception as e:
        if logger:
            logger.warning("Error closing channel", extra={"error": str(e), "session_id": sid})

    try:
        cl: Optional[paramiko.SSHClient] = s.get("client")
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
    """Read all currently available bytes from an interactive SSH channel.

    Args:
        channel (paramiko.Channel): Interactive shell channel to read from.
        quiet_timeout (float, optional): Time window with no new data after which
            reading stops. Defaults to 1.0.
        chunk_timeout (float, optional): Socket-level timeout for each receive loop.
            Defaults to 0.2.
        logger (Optional[Logger], optional): Logger for debug/error events.
            Defaults to None.

    Returns:
        str: Decoded channel output collected during the read window.
    """
    
    end_by: float = time.time() + quiet_timeout
    buf: List[bytes] = []
    try:
        channel.settimeout(chunk_timeout)
    except Exception:
        pass

    while True:
        got_data: bool = False
        try:
            if getattr(channel, "closed", False):
                break
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
    """Send a shell command and collect resulting output from the channel.

    Args:
        channel (paramiko.Channel): Interactive shell channel for command execution.
        cmd (str): Shell command to send.
        settle (float, optional): Delay after send before reading output.
            Defaults to 0.12.
        quiet_timeout (float, optional): Read inactivity window before read completion.
            Defaults to 1.0.
        logger (Optional[Logger], optional): Logger for debug and error events.
            Defaults to None.

    Returns:
        str: Collected command output from the remote shell.
    """
    if getattr(channel, "closed", False):
        if logger:
            logger.error("Attempted to send on a closed channel")
        raise RuntimeError("SSH channel is closed by remote")
    if logger:
        logger.debug("Sending command", extra={"command": cmd})
    try:
        channel.send(cmd + "\n")
    except Exception as e:
        if logger:
            logger.error("Failed to send command", extra={"error": str(e)})
        raise RuntimeError("Failed to send command to SSH channel") from e
    time.sleep(settle)
    out: str = read_all(channel, quiet_timeout=quiet_timeout, logger=logger)
    if logger:
        logger.debug("Command output collected", extra={"bytes": len(out)})
    return out


def await_shell_ready(
    channel: paramiko.Channel,
    timeout: float = 6.0,
    logger: Optional[Logger] = None,
) -> str:
    """Wait for an interactive shell to become responsive.

    Args:
        channel (paramiko.Channel): Interactive shell channel.
        timeout (float, optional): Maximum wait duration in seconds.
            Defaults to 6.0.
        logger (Optional[Logger], optional): Logger for readiness diagnostics.
            Defaults to None.

    Returns:
        str: Accumulated output captured while waiting for readiness.
    """
    # Flush any initial banner/motd
    _ = read_all(channel, quiet_timeout=0.3, logger=logger)
    marker: str = "__RC_READY__"
    try:
        channel.send(f"echo {marker}\n")
    except Exception as e:
        if logger:
            logger.error("Failed to send readiness marker; channel likely closed", extra={"error": str(e)})
        raise

    end_by: float = time.time() + timeout
    acc: str = ""
    while time.time() < end_by:
        # Read in short windows so we can repeatedly check for readiness marker.
        chunk: str = read_all(channel, quiet_timeout=0.5, logger=logger)
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
    """Execute a sudo command and send password only after explicit prompt detection.

    Args:
        channel (paramiko.Channel): Interactive shell channel.
        user_cmd (str): User-supplied command expected to begin with `sudo `.
        sudo_pw (str): Password sent only if sudo prompt marker appears.
        logger (Optional[Logger], optional): Logger for command lifecycle events.
            Defaults to None.

    Returns:
        str: Combined sudo command output, including post-password output when used.
    """
    if not user_cmd.startswith("sudo "):
        raise ValueError("run_sudo_when_prompted requires a command starting with 'sudo '")
    marker: str = "[SUDO-PROMPT]"
    cmd: str = f"sudo -S -p '{marker}:' {user_cmd[len('sudo '):]}"

    if logger:
        logger.info("Executing sudo command", extra={"sudo": True, "command": user_cmd[:200]})

    channel.send(cmd + "\n")
    time.sleep(0.15)

    out: str = read_all(channel, quiet_timeout=1.0, logger=logger)

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