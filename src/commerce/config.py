from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


def _truthy(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    db_path: Path
    timezone: str
    web_host: str
    web_port: int
    telegram_bot_token: str | None
    telegram_allowed_chat_id: int | None
    demo_mode: bool
    execution_mode: str

    @staticmethod
    def load() -> "Settings":
        load_dotenv()

        db_path = Path(os.getenv("ADS_DB_PATH", "./data/ads.sqlite3"))
        timezone = os.getenv("ADS_TIMEZONE", "Asia/Seoul").strip() or "Asia/Seoul"
        web_host = os.getenv("ADS_WEB_HOST", "127.0.0.1")
        web_port = int(os.getenv("ADS_WEB_PORT", "8010"))

        token = os.getenv("TELEGRAM_BOT_TOKEN") or None
        allowed_chat_id_raw = os.getenv("TELEGRAM_ALLOWED_CHAT_ID") or None
        allowed_chat_id = int(allowed_chat_id_raw) if allowed_chat_id_raw else None

        demo_mode = _truthy(os.getenv("ADS_DEMO_MODE", "0"))
        execution_mode = os.getenv("ADS_EXECUTION_MODE", "manual").strip().lower()

        return Settings(
            db_path=db_path,
            timezone=timezone,
            web_host=web_host,
            web_port=web_port,
            telegram_bot_token=token,
            telegram_allowed_chat_id=allowed_chat_id,
            demo_mode=demo_mode,
            execution_mode=execution_mode,
        )
