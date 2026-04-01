# config.py

"""Application configuration and environment parsing utilities.

This module centralizes all environment-driven settings so that runtime behavior
is deterministic and easy to audit in production deployments.
"""

import os
from dataclasses import dataclass


def _get_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable with a safe default fallback.

    Args:
        name (str): Environment variable name.
        default (bool): Value used when the variable is missing.

    Returns:
        bool: Parsed boolean equivalent of the configured environment value.
    """
    raw: str = os.getenv(name, str(int(default))).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _get_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    """Parse and bounds-check an integer environment variable.

    Args:
        name (str): Environment variable name.
        default (int): Fallback value when parsing fails.
        minimum (int | None, optional): Lower bound if provided. Defaults to None.
        maximum (int | None, optional): Upper bound if provided. Defaults to None.

    Returns:
        int: Parsed and optionally clamped integer value.
    """
    try:
        value: int = int(os.getenv(name, str(default)).strip())
    except Exception:
        value = default

    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _get_float(
    name: str,
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    """Parse and bounds-check a float environment variable.

    Args:
        name (str): Environment variable name.
        default (float): Fallback value when parsing fails.
        minimum (float | None, optional): Lower bound if provided. Defaults to None.
        maximum (float | None, optional): Upper bound if provided. Defaults to None.

    Returns:
        float: Parsed and optionally clamped floating-point value.
    """
    try:
        value: float = float(os.getenv(name, str(default)).strip())
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
    SSH_READY_TIMEOUT: float = _get_float("SSH_READY_TIMEOUT", 6.0, minimum=1.0, maximum=30.0)
    COMMAND_MAX_LENGTH: int = _get_int("COMMAND_MAX_LENGTH", 4096, minimum=64, maximum=65535)


def load_settings() -> Settings:
    """Create a runtime settings object from environment variables.

    Args:
        None

    Returns:
        Settings: Fully parsed and validated application configuration.
    """
    return Settings()
