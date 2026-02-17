from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


def fixture_dir(platform: str, config: dict[str, Any]) -> Path:
    raw = (config or {}).get("fixture_dir")
    if raw:
        return Path(str(raw))
    return Path("./fixtures") / platform / "sample"


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_entities(path: Path) -> list[dict[str, Any]]:
    p = path / "entities.json"
    if not p.exists():
        return []
    data = _read_json(p)
    if isinstance(data, list):
        return [dict(x) for x in data]
    raise ValueError("entities.json must be a JSON list")


def _parse_int(v: str | None) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(v: str | None) -> float | None:
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


def _parse_json(v: str | None) -> dict[str, Any]:
    if not v:
        return {}
    try:
        x = json.loads(v)
        return x if isinstance(x, dict) else {"_raw": x}
    except Exception:
        return {"_raw": str(v)}


def load_metrics_daily_rows(path: Path) -> Iterable[dict[str, Any]]:
    p = path / "metrics_daily.csv"
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            yield {
                "platform": (row.get("platform") or "").strip(),
                "account_id": (row.get("account_id") or "").strip() or None,
                "entity_type": (row.get("entity_type") or "").strip(),
                "entity_id": (row.get("entity_id") or "").strip(),
                "date": (row.get("date") or "").strip(),
                "spend": _parse_float(row.get("spend")),
                "impressions": _parse_int(row.get("impressions")),
                "clicks": _parse_int(row.get("clicks")),
                "conversions": _parse_float(row.get("conversions")),
                "conversion_value": _parse_float(row.get("conversion_value")),
                "metrics_json": _parse_json(row.get("metrics_json")),
            }


def load_metrics_intraday_rows(path: Path) -> Iterable[dict[str, Any]]:
    p = path / "metrics_intraday.csv"
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            yield {
                "platform": (row.get("platform") or "").strip(),
                "account_id": (row.get("account_id") or "").strip() or None,
                "entity_type": (row.get("entity_type") or "").strip(),
                "entity_id": (row.get("entity_id") or "").strip(),
                "hour_ts": (row.get("hour_ts") or "").strip(),
                "spend": _parse_float(row.get("spend")),
                "impressions": _parse_int(row.get("impressions")),
                "clicks": _parse_int(row.get("clicks")),
                "conversions": _parse_float(row.get("conversions")),
                "conversion_value": _parse_float(row.get("conversion_value")),
                "metrics_json": _parse_json(row.get("metrics_json")),
            }

