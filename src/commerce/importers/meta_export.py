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
class MetaImportOptions:
    level: str  # campaign|adset|ad
    account_id: str | None = None
    day_override: str | None = None


def import_meta_ads_csv(repo: Repo, *, path: Path, opts: MetaImportOptions) -> dict[str, Any]:
    """
    Import a Meta Ads Manager CSV export into:
    - entities (campaign/adset/ad)
    - metrics_daily (date granularity)

    Heuristic-based: handles common EN/KR header variants.
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    level = opts.level.strip().lower()
    if level not in {"campaign", "adset", "ad"}:
        return {"ok": False, "error": "level must be campaign|adset|ad", "rows": len(rows)}

    # IDs / names
    campaign_id_keys = ["Campaign ID", "campaign_id", "캠페인 ID", "캠페인ID"]
    campaign_name_keys = ["Campaign name", "Campaign Name", "캠페인 이름", "캠페인명"]
    adset_id_keys = ["Ad set ID", "Ad Set ID", "adset_id", "광고 세트 ID", "광고세트 ID"]
    adset_name_keys = ["Ad set name", "Ad Set name", "Ad Set Name", "광고 세트 이름", "광고세트 이름"]
    ad_id_keys = ["Ad ID", "ad_id", "광고 ID", "광고ID"]
    ad_name_keys = ["Ad name", "Ad Name", "광고 이름", "광고명"]

    # Metrics
    date_keys = ["Day", "Date", "date", "날짜", "일자", "Reporting starts", "보고 시작"]
    spend_keys = ["Amount spent (KRW)", "Amount spent", "Spend", "spend", "사용한 금액", "사용 금액", "지출"]
    impressions_keys = ["Impressions", "impressions", "노출", "노출수"]
    clicks_keys = [
        "Link clicks",
        "Outbound clicks",
        "Clicks (all)",
        "Clicks",
        "link_clicks",
        "clicks",
        "링크 클릭",
        "클릭",
        "클릭수",
    ]
    purchases_keys = [
        "Purchases",
        "Website purchases",
        "Purchases (Website)",
        "구매",
        "웹사이트 구매",
    ]
    results_keys = ["Results", "결과", "전환", "전환수"]
    purchase_value_keys = [
        "Purchases conversion value",
        "Purchase conversion value",
        "Website purchases conversion value",
        "구매 전환 값",
    ]
    conversion_value_keys = ["Conversion value", "전환 값", "전환가치", "전환 가치", "매출"]

    imported = 0
    skipped = 0

    for row in rows:
        day = _first(row, date_keys) or opts.day_override
        if not day:
            skipped += 1
            continue

        camp_id = _first(row, campaign_id_keys)
        camp_name = _first(row, campaign_name_keys)
        if not camp_id:
            camp_id = _hash_id("meta", "campaign", camp_name or "", opts.account_id or "")

        adset_id = _first(row, adset_id_keys)
        adset_name = _first(row, adset_name_keys)
        if adset_name and not adset_id:
            adset_id = _hash_id("meta", "adset", camp_id or "", adset_name or "")

        ad_id = _first(row, ad_id_keys)
        ad_name = _first(row, ad_name_keys)
        if ad_name and not ad_id:
            ad_id = _hash_id("meta", "ad", adset_id or camp_id or "", ad_name or "")

        if level == "campaign":
            entity_type = "campaign"
            entity_id = camp_id
            parent_type = None
            parent_id = None
            name = camp_name
        elif level == "adset":
            entity_type = "adset"
            entity_id = adset_id or _hash_id("meta", "adset", camp_id or "", "unknown")
            parent_type = "campaign"
            parent_id = camp_id
            name = adset_name
        else:
            entity_type = "ad"
            entity_id = ad_id or _hash_id("meta", "ad", adset_id or camp_id or "", "unknown")
            parent_type = "adset" if adset_id else "campaign"
            parent_id = adset_id or camp_id
            name = ad_name

        # Ensure entities exist (best-effort hierarchy)
        repo.upsert_entity(
            platform="meta",
            account_id=opts.account_id,
            entity_type="campaign",
            entity_id=camp_id,
            parent_type=None,
            parent_id=None,
            name=camp_name,
            status=None,
            meta_json={"source": "import", "row_level": level},
        )
        if adset_id:
            repo.upsert_entity(
                platform="meta",
                account_id=opts.account_id,
                entity_type="adset",
                entity_id=adset_id,
                parent_type="campaign",
                parent_id=camp_id,
                name=adset_name,
                status=None,
                meta_json={"source": "import", "row_level": level},
            )
        if ad_id:
            repo.upsert_entity(
                platform="meta",
                account_id=opts.account_id,
                entity_type="ad",
                entity_id=ad_id,
                parent_type="adset" if adset_id else "campaign",
                parent_id=adset_id or camp_id,
                name=ad_name,
                status=None,
                meta_json={"source": "import", "row_level": level},
            )

        spend = _parse_float(_first(row, spend_keys))
        impressions = _parse_int(_first(row, impressions_keys))
        clicks = _parse_int(_first(row, clicks_keys))
        purchases = _parse_float(_first(row, purchases_keys))
        results = _parse_float(_first(row, results_keys))
        # Primary conversions: prefer purchases if present; else fall back to results.
        conversions = purchases if purchases is not None else results
        # "All" conversions: best-effort. For Meta this is not as standardized as Google.
        conversions_all = results if results is not None else purchases

        purchase_value = _parse_float(_first(row, purchase_value_keys))
        any_value = _parse_float(_first(row, conversion_value_keys))
        conversion_value = purchase_value if purchase_value is not None else any_value

        repo.upsert_metric_daily(
            platform="meta",
            account_id=opts.account_id,
            entity_type=entity_type,
            entity_id=entity_id,
            day=str(day),
            spend=spend,
            impressions=impressions,
            clicks=clicks,
            conversions=conversions,
            conversion_value=conversion_value,
            metrics_json={
                "_raw": row,
                "parent_type": parent_type,
                "parent_id": parent_id,
                "name": name,
                "conversions_all": conversions_all,
                "conversions_purchase": purchases,
                "conversions_results": results,
                "conversion_value_purchase": purchase_value,
            },
        )
        imported += 1

    return {
        "ok": True,
        "rows": len(rows),
        "imported": imported,
        "skipped": skipped,
        "platform": "meta",
        "level": level,
    }
