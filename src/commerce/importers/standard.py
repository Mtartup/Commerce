from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from commerce.repo import Repo


def _read_text_best_effort(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(v: Any) -> int | None:
    f = _parse_float(v)
    if f is None:
        return None
    return int(f)


def _parse_json(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    s = str(v).strip()
    if s == "":
        return {}
    try:
        x = json.loads(s)
        return x if isinstance(x, dict) else {"_raw": x}
    except Exception:
        return {"_raw": s}


def import_intraday_csv(repo: Repo, *, path: Path) -> dict[str, Any]:
    """
    Import Commerce standard intraday CSV:

    Required columns:
    - platform, entity_type, entity_id, hour_ts
    Optional:
    - account_id, spend, impressions, clicks, conversions, conversion_value, metrics_json
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    imported = 0
    for row in rows:
        platform = (row.get("platform") or "").strip()
        entity_type = (row.get("entity_type") or "").strip()
        entity_id = (row.get("entity_id") or "").strip()
        hour_ts = (row.get("hour_ts") or "").strip()
        if not platform or not entity_type or not entity_id or not hour_ts:
            continue

        repo.upsert_metric_intraday(
            platform=platform,
            account_id=(row.get("account_id") or "").strip() or None,
            entity_type=entity_type,
            entity_id=entity_id,
            hour_ts=hour_ts,
            spend=_parse_float(row.get("spend")),
            impressions=_parse_int(row.get("impressions")),
            clicks=_parse_int(row.get("clicks")),
            conversions=_parse_float(row.get("conversions")),
            conversion_value=_parse_float(row.get("conversion_value")),
            metrics_json=_parse_json(row.get("metrics_json")),
        )
        imported += 1

    return {"ok": True, "rows": len(rows), "imported": imported}


def import_daily_csv(repo: Repo, *, path: Path) -> dict[str, Any]:
    """
    Import Commerce standard daily CSV:

    Required columns:
    - platform, entity_type, entity_id, date
    Optional:
    - account_id, spend, impressions, clicks, conversions, conversion_value, metrics_json
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    imported = 0
    for row in rows:
        platform = (row.get("platform") or "").strip()
        entity_type = (row.get("entity_type") or "").strip()
        entity_id = (row.get("entity_id") or "").strip()
        day = (row.get("date") or "").strip()
        if not platform or not entity_type or not entity_id or not day:
            continue

        repo.upsert_metric_daily(
            platform=platform,
            account_id=(row.get("account_id") or "").strip() or None,
            entity_type=entity_type,
            entity_id=entity_id,
            day=day,
            spend=_parse_float(row.get("spend")),
            impressions=_parse_int(row.get("impressions")),
            clicks=_parse_int(row.get("clicks")),
            conversions=_parse_float(row.get("conversions")),
            conversion_value=_parse_float(row.get("conversion_value")),
            metrics_json=_parse_json(row.get("metrics_json")),
        )
        imported += 1

    return {"ok": True, "rows": len(rows), "imported": imported}
