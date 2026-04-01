# config.py

"""Application configuration and environment parsing utilities.

This module centralizes all environment-driven settings so that runtime behavior
is deterministic and easy to audit in production deployments.
"""

import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable with safe fallback."""
    raw: str = os.getenv(name, str(int(default))).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _get_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    """Parse an integer environment variable and optionally clamp it to bounds."""
    try:
        value: int = int(os.getenv(name, str(default)).strip())
    except Exception:
        value = default

    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value

@dataclass
class Settings:
    """Strongly-typed runtime settings for API and SSH session behavior."""

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/app.log")
    LOG_MAX_BYTES: int = _get_int("LOG_MAX_BYTES", 5_000_000, minimum=10_000)  # ~5MB
    LOG_BACKUP_COUNT: int = _get_int("LOG_BACKUP_COUNT", 3, minimum=1, maximum=20)
    LOG_JSON: bool = _get_bool("LOG_JSON", False)

    # API service
    APP_NAME: str = os.getenv("APP_NAME", "Remote Command Plot API")
    APP_DESCRIPTION: str = os.getenv("APP_DESCRIPTION", "For AS Watson GIT Use only")
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = _get_int("APP_PORT", 8000, minimum=1, maximum=65535)
    APP_RELOAD: bool = _get_bool("APP_RELOAD", False)

    # Sessions
    SESSION_TTL_SECONDS: int = _get_int("SESSION_TTL_SECONDS", 300, minimum=30, maximum=86_400)
    CLEANER_INTERVAL_SECONDS: int = _get_int("CLEANER_INTERVAL_SECONDS", 60, minimum=5, maximum=3600)

    # SSH runtime
    SSH_CONNECT_TIMEOUT: int = _get_int("SSH_CONNECT_TIMEOUT", 20, minimum=3, maximum=120)
    SSH_KEEPALIVE_SECONDS: int = _get_int("SSH_KEEPALIVE_SECONDS", 30, minimum=10, maximum=300)
    SSH_READY_TIMEOUT: float = float(_get_int("SSH_READY_TIMEOUT", 6, minimum=1, maximum=30))
    COMMAND_MAX_LENGTH: int = _get_int("COMMAND_MAX_LENGTH", 4096, minimum=64, maximum=65535)


def load_settings() -> Settings:
    """Return a fully-parsed settings object for the current process."""
    return Settings()
