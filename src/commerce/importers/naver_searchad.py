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
    # Last resort: replace invalid chars
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
class NaverImportOptions:
    product_type: str
    level: str  # campaign|adgroup|keyword
    account_id: str | None = None
    day_override: str | None = None


def import_naver_searchad_csv(repo: Repo, *, path: Path, opts: NaverImportOptions) -> dict[str, Any]:
    """
    Import a Naver SearchAd CSV export into:
    - entities (campaign/adgroup/keyword)
    - metrics_daily (date granularity)

    This is heuristic-based: it tries common KR/EN header variants.
    If IDs are missing it will generate deterministic IDs from (name + parent).
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    level = opts.level.strip().lower()
    if level not in {"campaign", "adgroup", "keyword"}:
        return {"ok": False, "error": "level must be campaign|adgroup|keyword", "rows": len(rows)}

    # Common header variants (KR exports vary; keep this list extensible)
    date_keys = ["date", "Date", "일자", "날짜"]

    camp_id_keys = ["campaign_id", "Campaign ID", "캠페인ID", "캠페인 ID", "캠페인ID(번호)"]
    camp_name_keys = ["campaign_name", "Campaign", "캠페인명", "캠페인 이름", "캠페인"]

    grp_id_keys = ["adgroup_id", "Adgroup ID", "광고그룹ID", "광고그룹 ID", "그룹ID"]
    grp_name_keys = ["adgroup_name", "Adgroup", "광고그룹명", "광고그룹 이름", "광고그룹"]

    kw_id_keys = ["keyword_id", "Keyword ID", "키워드ID", "키워드 ID"]
    kw_name_keys = ["keyword", "Keyword", "키워드", "키워드명"]

    impr_keys = ["impressions", "Impressions", "노출수", "노출 수"]
    click_keys = ["clicks", "Clicks", "클릭수", "클릭 수"]
    spend_keys = ["spend", "cost", "Cost", "비용", "총비용", "총 비용", "광고비"]
    conv_all_keys = ["conversions", "Conversions", "전환수", "전환 수", "전체전환수", "전체 전환수"]
    conv_purchase_keys = ["구매전환수", "구매 전환수", "구매수", "구매 수"]
    value_all_keys = [
        "conversion_value",
        "Conv. value",
        "전환매출",
        "전환 매출",
        "전환매출액",
        "전환가치",
        "전환 가치",
        "매출",
    ]
    value_purchase_keys = ["구매전환매출", "구매 전환매출", "구매금액", "구매 금액", "구매매출", "구매 매출"]

    imported = 0
    entity_upserts = 0

    for row in rows:
        day = opts.day_override or _first(row, date_keys)
        if not day:
            # If no day in CSV, require override
            return {"ok": False, "error": "missing date column; pass --day", "rows": len(rows)}

        camp_id = _first(row, camp_id_keys)
        camp_name = _first(row, camp_name_keys)

        grp_id = _first(row, grp_id_keys)
        grp_name = _first(row, grp_name_keys)

        kw_id = _first(row, kw_id_keys)
        kw_name = _first(row, kw_name_keys)

        if not camp_id:
            if not camp_name:
                continue
            camp_id = _hash_id("naver", "campaign", camp_name)
        if level in {"adgroup", "keyword"} and not grp_id:
            if not grp_name:
                grp_id = _hash_id("naver", "adgroup", camp_id)
            else:
                grp_id = _hash_id("naver", "adgroup", camp_id, grp_name)
        if level == "keyword" and not kw_id:
            if not kw_name:
                kw_id = _hash_id("naver", "keyword", grp_id or camp_id)
            else:
                kw_id = _hash_id("naver", "keyword", grp_id or camp_id, kw_name)

        # Upsert entities for hierarchy
        repo.upsert_entity(
            platform="naver",
            account_id=opts.account_id,
            entity_type="campaign",
            entity_id=camp_id,
            parent_type=None,
            parent_id=None,
            name=camp_name,
            status=None,
            meta_json={"product_type": opts.product_type},
        )
        entity_upserts += 1

        if level in {"adgroup", "keyword"}:
            repo.upsert_entity(
                platform="naver",
                account_id=opts.account_id,
                entity_type="adgroup",
                entity_id=str(grp_id),
                parent_type="campaign",
                parent_id=camp_id,
                name=grp_name,
                status=None,
                meta_json={"product_type": opts.product_type},
            )
            entity_upserts += 1

        if level == "keyword":
            repo.upsert_entity(
                platform="naver",
                account_id=opts.account_id,
                entity_type="keyword",
                entity_id=str(kw_id),
                parent_type="adgroup" if grp_id else "campaign",
                parent_id=str(grp_id) if grp_id else camp_id,
                name=kw_name,
                status=None,
                meta_json={"product_type": opts.product_type},
            )
            entity_upserts += 1

        # Choose target entity based on import level
        if level == "campaign":
            entity_type = "campaign"
            entity_id = camp_id
        elif level == "adgroup":
            entity_type = "adgroup"
            entity_id = str(grp_id)
        else:
            entity_type = "keyword"
            entity_id = str(kw_id)

        spend = _parse_float(_first(row, spend_keys))
        impressions = _parse_int(_first(row, impr_keys))
        clicks = _parse_int(_first(row, click_keys))
        conv_all = _parse_float(_first(row, conv_all_keys))
        conv_purchase = _parse_float(_first(row, conv_purchase_keys))
        conv = conv_purchase if conv_purchase is not None else conv_all

        value_all = _parse_float(_first(row, value_all_keys))
        value_purchase = _parse_float(_first(row, value_purchase_keys))
        value = value_purchase if value_purchase is not None else value_all

        # Keep raw row for debugging; user said DB growth is ok early-stage.
        metrics_json = {
            "product_type": opts.product_type,
            "raw": row,
            "conversions_all": conv_all,
            "conversions_purchase": conv_purchase,
            "conversion_value_all": value_all,
            "conversion_value_purchase": value_purchase,
        }

        repo.upsert_metric_daily(
            platform="naver",
            account_id=opts.account_id,
            entity_type=entity_type,
            entity_id=entity_id,
            day=day,
            spend=spend,
            impressions=impressions,
            clicks=clicks,
            conversions=conv,
            conversion_value=value,
            metrics_json=metrics_json,
        )
        imported += 1

    return {
        "ok": True,
        "rows": len(rows),
        "imported": imported,
        "entity_upserts": entity_upserts,
        "product_type": opts.product_type,
        "level": level,
    }
