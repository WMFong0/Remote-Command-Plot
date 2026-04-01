# app_logging.py

"""Structured and human-friendly logging setup for the API service.

The logger supports either JSON output (for aggregation systems) or text output
with optional color in console mode. File logging always rotates.
"""

import os
import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any

from config import Settings


def _ensure_dir(path: str) -> None:
    """Ensure the parent directory for a file path exists."""
    try:
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
    except Exception:
        pass


class JsonFormatter(logging.Formatter):
    """JSON log formatter that merges all `extra` fields safely into the JSON object."""
    _reserved = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:
        obj: dict[str, Any] = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
        }
        if record.exc_info:
            obj["exc_info"] = self.formatException(record.exc_info)

        # Merge any extra fields attached to the record
        for k, v in record.__dict__.items():
            if k in self._reserved or k.startswith("_") or k in obj:
                continue
            try:
                json.dumps(v)  # test serializability
                obj[k] = v
            except Exception:
                obj[k] = str(v)

        return json.dumps(obj, ensure_ascii=False)


class ColorContextFormatter(logging.Formatter):
    """Colorful text formatter that appends context fields (client_ip, session_id, username)."""
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[1;31m" # Bold Red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        asctime = self.formatTime(record, "%Y-%m-%d %H:%M:%S")

        base = (
            f"{asctime} | "
            f"{record.levelname:<8} | "
            f"{record.name} | "
            f"{record.funcName}:{record.lineno} | "
            f"{record.getMessage()}"
        )

        # Pull values from `extra`
        context = []
        if hasattr(record, "client_ip"):
            context.append(f"ip={record.client_ip}")
        if hasattr(record, "session_id"):
            context.append(f"sid={record.session_id}")
        if hasattr(record, "username"):
            context.append(f"user={record.username}")

        if context:
            base += " | " + " ".join(context)

        return f"{color}{base}{self.RESET}"


class PlainContextFormatter(ColorContextFormatter):
    """
    Plain-text variant of ColorContextFormatter with ANSI colors disabled.
    Use this for file logs when LOG_JSON=0 to avoid embedding color codes.
    """
    COLORS = {  # override to disable colors
        "DEBUG": "",
        "INFO": "",
        "WARNING": "",
        "ERROR": "",
        "CRITICAL": "",
    }
    RESET = ""


def setup_logging(settings: Settings) -> logging.Logger:
    """
    Logger setup:
      - File handler: rotating
          * LOG_JSON=1 -> JSON (machine-friendly)
          * LOG_JSON=0 -> Plain text (NO color) in app.log
      - Console handler:
          * LOG_JSON=1 -> JSON
          * LOG_JSON=0 -> Colorful text
      - Avoid duplicate handlers on reload
      - Make LOG_FILE absolute relative to this file by default
    """
    log_level: str = settings.LOG_LEVEL
    cfg_log_file: str = settings.LOG_FILE
    log_max_bytes: int = settings.LOG_MAX_BYTES
    log_backup_count: int = settings.LOG_BACKUP_COUNT
    log_json: bool = settings.LOG_JSON

    # Resolve to absolute path relative to this file when LOG_FILE is relative
    if os.path.isabs(cfg_log_file):
        log_file: str = cfg_log_file
    else:
        base_dir: str = os.path.dirname(os.path.abspath(__file__))
        log_file = os.path.join(base_dir, cfg_log_file)

    _ensure_dir(log_file)

    logger: logging.Logger = logging.getLogger("remote_command_plot")
    logger.setLevel(log_level)
    logger.propagate = False

    # Clear existing handlers (important with uvicorn --reload)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # ----- File handler -----
    # Persist logs to disk with rotation to avoid unbounded file growth.
    fh: RotatingFileHandler = RotatingFileHandler(
        log_file,
        maxBytes=log_max_bytes,
        backupCount=log_backup_count,
        encoding="utf-8",
    )
    fh.setLevel(log_level)
    # File: JSON if LOG_JSON=1; plain text (no colors) if LOG_JSON=0
    fh.setFormatter(JsonFormatter() if log_json else PlainContextFormatter())
    logger.addHandler(fh)

    # ----- Console handler -----
    ch: logging.StreamHandler = logging.StreamHandler()  # stderr by default
    ch.setLevel(log_level)
    # Console: JSON if LOG_JSON=1; colorful if LOG_JSON=0
    ch.setFormatter(JsonFormatter() if log_json else ColorContextFormatter())
    logger.addHandler(ch)

    # Align uvicorn loggers to our level
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(log_level)

    # Print resolved configuration
    logger.info(
        "Logger initialized",
        extra={
            "log_level": log_level,
            "log_file": os.path.abspath(log_file),
            "log_max_bytes": log_max_bytes,
            "log_backup_count": log_backup_count,
            "log_json": log_json,
            "cwd": os.getcwd(),
        },
    )
    return logger