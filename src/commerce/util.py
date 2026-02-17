from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def new_id(prefix: str) -> str:
    # URL-safe, reasonably short, no external deps
    return f"{prefix}_{secrets.token_urlsafe(10)}"


def now_kst_date_str(timezone_name: str) -> str:
    return datetime.now(tz=ZoneInfo(timezone_name)).date().isoformat()


def to_kst_date_str(dt: datetime, timezone_name: str) -> str:
    return dt.astimezone(ZoneInfo(timezone_name)).date().isoformat()


def to_kst_hour_iso(dt: datetime, timezone_name: str) -> str:
    k = dt.astimezone(ZoneInfo(timezone_name)).replace(minute=0, second=0, microsecond=0)
    return k.isoformat()


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="strict")).hexdigest()
