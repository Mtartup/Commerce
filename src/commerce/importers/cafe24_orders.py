from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from commerce.repo import Repo


def _read_text_best_effort(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _first(row: dict[str, Any], keys: list[str]) -> str | None:
    for k in keys:
        if k in row and str(row.get(k) or "").strip() != "":
            return str(row.get(k)).strip()
    return None


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


def _hash_id(*parts: str) -> str:
    h = hashlib.sha256(("|".join(parts)).encode("utf-8", errors="strict")).hexdigest()[:16]
    return f"imp_{h}"


def _parse_kst_datetime_best_effort(s: str, *, timezone_name: str) -> tuple[str | None, str | None]:
    raw = (s or "").strip()
    if not raw:
        return None, None
    # Common Cafe24 export formats (KST)
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y.%m.%d %H:%M:%S",
        "%Y.%m.%d %H:%M",
        "%Y.%m.%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    ]
    tz = ZoneInfo(timezone_name)
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt)
            # Treat naive timestamps as already in KST.
            dt_kst = dt.replace(tzinfo=tz)
            return dt_kst.replace(microsecond=0).isoformat(), dt_kst.date().isoformat()
        except ValueError:
            continue
    m = re.search(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})", raw)
    if m:
        y, mo, d = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        dt_kst = datetime(y, mo, d, tzinfo=tz)
        return dt_kst.replace(microsecond=0).isoformat(), dt_kst.date().isoformat()
    return None, None


@dataclass(frozen=True)
class Cafe24OrdersImportOptions:
    store: str = "cafe24"
    timezone: str = "Asia/Seoul"
    currency_default: str = "KRW"
    day_override: str | None = None


def import_cafe24_orders_csv(repo: Repo, *, path: Path, opts: Cafe24OrdersImportOptions) -> dict[str, Any]:
    """
    Import Cafe24 orders export CSV into store_orders.

    Goals (MVP):
    - Keep it robust to KR/EN header variants.
    - Capture "where they came from" roughly via inflow_path / referer if available.
    - No attribution to campaigns/adgroups/keywords in this step.
    """
    text = _read_text_best_effort(path)
    rows = list(csv.DictReader(text.splitlines()))
    if not rows:
        return {"ok": False, "error": "empty csv", "rows": 0}

    inserted = 0
    skipped = 0

    for r in rows:
        order_id = _first(
            r,
            [
                "order_id",
                "orderId",
                "Order ID",
                "Order No",
                "Order No.",
                "주문번호",
                "주문 번호",
                "주문번호(필수)",
            ],
        )
        if not order_id:
            # Deterministic fallback to avoid losing the row entirely.
            order_id = _hash_id(str(r.get("주문일시") or ""), str(r.get("결제일시") or ""), str(r.get("결제금액") or ""))

        ordered_at_raw = _first(
            r,
            [
                "payment_date",
                "paymentDate",
                "결제일시",
                "결제 일시",
                "결제일",
                "주문일시",
                "주문 일시",
                "주문일",
                "주문일자",
                "date",
                "일자",
            ],
        )
        ordered_at, date_kst = _parse_kst_datetime_best_effort(
            ordered_at_raw or "", timezone_name=opts.timezone
        )
        if not date_kst:
            date_kst = opts.day_override
        if not date_kst:
            skipped += 1
            continue

        status = _first(
            r,
            [
                "status",
                "order_status",
                "주문상태",
                "주문 상태",
                "결제상태",
                "결제 상태",
            ],
        )
        amount = _parse_float(
            _first(
                r,
                [
                    "payment_amount",
                    "amount",
                    "결제금액",
                    "결제 금액",
                    "총결제금액",
                    "총 결제금액",
                    "주문금액",
                    "주문 금액",
                ],
            )
        )
        currency = (
            _first(r, ["currency", "통화"])
            or opts.currency_default
            or "KRW"
        ).strip().upper()

        order_place_id = _first(r, ["order_place_id", "order place id", "주문경로ID", "주문경로 ID"])
        order_place_name = _first(r, ["order_place_name", "order place name", "주문경로명", "주문경로 명"])

        inflow_path = _first(r, ["inflow_path", "inflow path", "유입경로", "유입 경로", "방문경로", "방문 경로"])
        inflow_path_detail = _first(
            r,
            ["inflow_path_detail", "inflow path detail", "유입경로상세", "유입 경로 상세", "유입상세", "유입 상세"],
        )
        referer = _first(r, ["referer", "referrer", "유입URL", "유입 URL", "참조URL", "참조 URL"])
        source_raw = inflow_path_detail or inflow_path or referer

        repo.upsert_store_order(
            store=opts.store,
            order_id=str(order_id).strip(),
            ordered_at=ordered_at,
            date_kst=date_kst,
            status=status,
            amount=amount,
            currency=currency,
            order_place_id=order_place_id,
            order_place_name=order_place_name,
            inflow_path=inflow_path,
            inflow_path_detail=inflow_path_detail,
            referer=referer,
            source_raw=source_raw,
            meta_json={"row": r},
        )
        inserted += 1

    return {
        "ok": True,
        "rows": len(rows),
        "inserted": inserted,
        "skipped": skipped,
        "store": opts.store,
    }

