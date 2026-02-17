from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
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


def _first(row: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        if k in row and str(row.get(k) or "").strip() != "":
            return str(row.get(k)).strip()
    return None


def _hash_id(*parts: str) -> str:
    h = hashlib.sha256(("|".join(parts)).encode("utf-8", errors="strict")).hexdigest()[:16]
    return f"imp_{h}"


@dataclass(frozen=True)
class GoogleImportOptions:
    level: str  # campaign|adgroup|keyword
    account_id: str | None = None
    day_override: str | None = None


def import_google_ads_csv(repo: Repo, *, path: Path, opts: GoogleImportOptions) -> dict[str, Any]:
    """
    Import a Google Ads UI/GAQL CSV export into:
    - entities (campaign/adgroup/keyword)
    - metrics_daily

    Notes:
    - If the export is from GAQL, ensure you include date, ids, and cost/conversions fields.
    - Cost micros is handled if the header indicates micros.
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    level = opts.level.strip().lower()
    if level not in {"campaign", "adgroup", "keyword"}:
        return {"ok": False, "error": "level must be campaign|adgroup|keyword", "rows": len(rows)}

    date_keys = ["Date", "Day", "segments.date", "date", "일자", "날짜"]
    camp_id_keys = ["Campaign ID", "campaign.id", "campaign_id", "캠페인 ID", "캠페인ID"]
    camp_name_keys = ["Campaign", "Campaign name", "campaign.name", "캠페인", "캠페인명"]
    ag_id_keys = ["Ad group ID", "ad_group.id", "ad_group_id", "광고그룹 ID", "광고 그룹 ID", "광고그룹ID"]
    ag_name_keys = ["Ad group", "Ad group name", "ad_group.name", "광고그룹", "광고 그룹", "광고그룹명"]
    kw_id_keys = ["Keyword ID", "criterion.id", "keyword_id", "키워드 ID", "키워드ID"]
    kw_text_keys = ["Keyword", "Keyword text", "criterion.keyword.text", "키워드", "키워드 텍스트"]

    cost_keys = ["Cost", "비용", "광고비", "총비용"]
    cost_micros_keys = ["Cost (micros)", "cost_micros"]
    impressions_keys = ["Impressions", "Impr.", "impressions", "노출수", "노출"]
    clicks_keys = ["Clicks", "clicks", "클릭수", "클릭"]
    conversions_primary_keys = ["Conversions", "conversions", "전환수", "전환"]
    conversions_all_keys = ["All conv.", "All conversions", "all_conversions", "전체전환", "전체 전환"]
    conv_value_primary_keys = [
        "Conversion value",
        "Conv. value",
        "conversion_value",
        "전환가치",
        "전환 가치",
        "전환값",
        "매출",
    ]
    conv_value_all_keys = ["All conv. value", "All conversions value", "all_conversion_value", "전체전환가치", "전체 전환가치"]

    imported = 0
    skipped = 0

    for row in rows:
        day = _first(row, date_keys) or opts.day_override
        if not day:
            skipped += 1
            continue

        camp_id = _first(row, camp_id_keys)
        camp_name = _first(row, camp_name_keys)
        if not camp_id:
            camp_id = _hash_id("google", "campaign", camp_name or "", opts.account_id or "")

        ag_id = _first(row, ag_id_keys)
        ag_name = _first(row, ag_name_keys)
        if ag_name and not ag_id:
            ag_id = _hash_id("google", "adgroup", camp_id or "", ag_name or "")

        kw_id = _first(row, kw_id_keys)
        kw_text = _first(row, kw_text_keys)
        if kw_text and not kw_id:
            kw_id = _hash_id("google", "keyword", ag_id or camp_id or "", kw_text or "")

        if level == "campaign":
            entity_type = "campaign"
            entity_id = camp_id
            parent_type = None
            parent_id = None
            name = camp_name
        elif level == "adgroup":
            entity_type = "adgroup"
            entity_id = ag_id or _hash_id("google", "adgroup", camp_id or "", "unknown")
            parent_type = "campaign"
            parent_id = camp_id
            name = ag_name
        else:
            entity_type = "keyword"
            entity_id = kw_id or _hash_id("google", "keyword", ag_id or camp_id or "", "unknown")
            parent_type = "adgroup" if ag_id else "campaign"
            parent_id = ag_id or camp_id
            name = kw_text

        repo.upsert_entity(
            platform="google",
            account_id=opts.account_id,
            entity_type="campaign",
            entity_id=camp_id,
            parent_type=None,
            parent_id=None,
            name=camp_name,
            status=None,
            meta_json={"source": "import", "row_level": level},
        )
        if ag_id:
            repo.upsert_entity(
                platform="google",
                account_id=opts.account_id,
                entity_type="adgroup",
                entity_id=ag_id,
                parent_type="campaign",
                parent_id=camp_id,
                name=ag_name,
                status=None,
                meta_json={"source": "import", "row_level": level},
            )
        if kw_id:
            repo.upsert_entity(
                platform="google",
                account_id=opts.account_id,
                entity_type="keyword",
                entity_id=kw_id,
                parent_type="adgroup" if ag_id else "campaign",
                parent_id=ag_id or camp_id,
                name=kw_text,
                status=None,
                meta_json={"source": "import", "row_level": level},
            )

        # Prefer currency cost; fallback to micros.
        cost = _parse_float(_first(row, cost_keys))
        if cost is None:
            micros = _parse_float(_first(row, cost_micros_keys))
            cost = (micros / 1_000_000.0) if micros is not None else None

        conv_primary = _parse_float(_first(row, conversions_primary_keys))
        conv_all = _parse_float(_first(row, conversions_all_keys))
        conv_value_primary = _parse_float(_first(row, conv_value_primary_keys))
        conv_value_all = _parse_float(_first(row, conv_value_all_keys))

        conversions = conv_primary if conv_primary is not None else conv_all
        conversion_value = conv_value_primary if conv_value_primary is not None else conv_value_all

        repo.upsert_metric_daily(
            platform="google",
            account_id=opts.account_id,
            entity_type=entity_type,
            entity_id=entity_id,
            day=str(day),
            spend=cost,
            impressions=_parse_int(_first(row, impressions_keys)),
            clicks=_parse_int(_first(row, clicks_keys)),
            conversions=conversions,
            conversion_value=conversion_value,
            metrics_json={
                "_raw": row,
                "parent_type": parent_type,
                "parent_id": parent_id,
                "name": name,
                "conversions_all": conv_all,
                "conversion_value_all": conv_value_all,
                "conversions_primary": conv_primary,
                "conversion_value_primary": conv_value_primary,
            },
        )
        imported += 1

    return {
        "ok": True,
        "rows": len(rows),
        "imported": imported,
        "skipped": skipped,
        "platform": "google",
        "level": level,
    }
