# config.py

import os
from dataclasses import dataclass

@dataclass
class Settings:
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/app.log")
    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", "5000000"))  # ~5MB
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "3"))  # keep 3 backups
    LOG_JSON: bool = os.getenv("LOG_JSON", "0").lower() in ("1", "true", "yes")

    # Sessions
    SESSION_TTL_SECONDS: int = int(os.getenv("SESSION_TTL_SECONDS", "300"))
    CLEANER_INTERVAL_SECONDS: int = int(os.getenv("CLEANER_INTERVAL_SECONDS", "60"))


def load_settings() -> Settings:
    return Settings()