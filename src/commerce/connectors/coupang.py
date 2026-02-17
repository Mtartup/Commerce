from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir

_BASE_URL = "https://api-gateway.coupang.com"


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _sum_order_amount(order: dict) -> float | None:
    """Sum (salesPrice - discountPrice) across all orderItems."""
    items = order.get("orderItems")
    if not items:
        return _parse_float(order.get("orderPrice") or order.get("amount"))
    total = 0.0
    for item in items:
        qty = int(item.get("shippingCount", 1) or 1)
        sales = float(item.get("salesPrice", 0) or 0)
        discount = float(item.get("discountPrice", 0) or 0)
        total += (sales - discount) * qty
    return total if total else None


def _to_date_kst(ts: str) -> str:
    """Extract YYYY-MM-DD from an ISO-ish timestamp, fallback to today KST."""
    if not ts:
        return datetime.now(tz=ZoneInfo("Asia/Seoul")).date().isoformat()
    return ts[:10]


def _load_orders_json(path: Path) -> list[dict[str, Any]]:
    p = path / "orders.json"
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    return []


class _CoupangClient:
    """Thin wrapper around Coupang Wing Open API with HMAC-SHA256 auth."""

    def __init__(self) -> None:
        self.access_key = (os.getenv("COUPANG_ACCESS_KEY") or "").strip()
        self.secret_key = (os.getenv("COUPANG_SECRET_KEY") or "").strip()

    def _authorization_header(self, method: str, path: str, query: str) -> str:
        now = datetime.utcnow().strftime("%y%m%dT%H%M%SZ")
        message = f"{now}{method}{path}{query}"
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return (
            f"CEA algorithm=HmacSHA256, access-key={self.access_key}, "
            f"signed-date={now}, signature={signature}"
        )

    async def request_json(
        self, method: str, path: str, params: dict | None = None
    ) -> Any:
        query = ""
        if params:
            query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))

        auth = self._authorization_header(method.upper(), path, query)
        url = _BASE_URL + path
        if query:
            url = f"{url}?{query}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method, url, headers={"Authorization": auth}
            )
            resp.raise_for_status()
            return resp.json()

    async def fetch_orders(
        self, vendor_id: str, date_from: str, date_to: str
    ) -> list[dict[str, Any]]:
        """Fetch orders in 1-day windows (API limit). Paging within each window."""
        all_orders: list[dict[str, Any]] = []
        start = datetime.fromisoformat(date_from).date()
        end = datetime.fromisoformat(date_to).date()

        day = start
        while day <= end:
            day_str = day.isoformat()
            next_token: str | None = None
            while True:
                path = f"/v2/providers/openapi/apis/api/v4/vendors/{vendor_id}/ordersheets"
                params: dict[str, str] = {
                    "createdAtFrom": f"{day_str}T00:00",
                    "createdAtTo": f"{day_str}T23:59",
                    "searchType": "timeFrame",
                    "status": "ACCEPT",
                }
                if next_token:
                    params["nextToken"] = next_token

                data = await self.request_json("GET", path, params)
                items = data.get("data", []) if isinstance(data, dict) else []
                all_orders.extend(items)

                next_token = data.get("nextToken") if isinstance(data, dict) else None
                if not next_token:
                    break
                await asyncio.sleep(0.15)

            day += timedelta(days=1)
            if day <= end:
                await asyncio.sleep(0.15)

        return all_orders


class CoupangConnector:
    """
    Coupang connector — Wing Open API.

    Modes:
    - "import": CSV import only (sync_entities is a no-op)
    - "fixture": load sample orders from fixtures/coupang/sample/orders.json
    - "api": HMAC-SHA256 authenticated calls to Wing Open API
    """

    capabilities = ConnectorCapabilities(
        read_orders=True,
    )

    def __init__(self, ctx: ConnectorContext, repo):
        self.ctx = ctx
        self.repo = repo

    async def health_check(self) -> tuple[bool, str | None]:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return True, None
        if mode != "api":
            return False, "bad mode"
        ak = (os.getenv("COUPANG_ACCESS_KEY") or "").strip()
        sk = (os.getenv("COUPANG_SECRET_KEY") or "").strip()
        vid = (os.getenv("COUPANG_VENDOR_ID") or "").strip()
        if not ak:
            return False, "Missing COUPANG_ACCESS_KEY"
        if not sk:
            return False, "Missing COUPANG_SECRET_KEY"
        if not vid:
            return False, "Missing COUPANG_VENDOR_ID"
        return True, None

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            d = fixture_dir(self.ctx.platform, self.ctx.config)
            orders = _load_orders_json(d)
            for o in orders:
                self.repo.upsert_store_order(
                    store="coupang",
                    order_id=str(o.get("orderId") or o.get("order_id") or ""),
                    ordered_at=o.get("orderedAt") or o.get("ordered_at"),
                    date_kst=_to_date_kst(o.get("orderedAt") or o.get("ordered_at") or o.get("date_kst", "")),
                    status=o.get("status"),
                    amount=_sum_order_amount(o),
                    currency=o.get("currency", "KRW"),
                    order_place_id=None,
                    order_place_name=None,
                    inflow_path=o.get("inflow_path"),
                    inflow_path_detail=None,
                    referer=None,
                    source_raw=None,
                    meta_json=o,
                )
            return

        # API mode
        client = _CoupangClient()
        vendor_id = (os.getenv("COUPANG_VENDOR_ID") or "").strip()
        cursor_key = f"coupang:{self.ctx.connector_id}:last_sync_date"
        last_sync = self.repo.get_meta(cursor_key)

        now_kst = datetime.now(tz=ZoneInfo("Asia/Seoul"))
        if last_sync:
            date_from = last_sync
        else:
            date_from = (now_kst.date() - timedelta(days=14)).isoformat()
        date_to = now_kst.date().isoformat()

        orders = await client.fetch_orders(vendor_id, date_from, date_to)
        for o in orders:
            oid = str(o.get("orderId", ""))
            if not oid:
                continue
            self.repo.upsert_store_order(
                store="coupang",
                order_id=oid,
                ordered_at=o.get("orderedAt"),
                date_kst=_to_date_kst(o.get("orderedAt", "")),
                status=o.get("status"),
                amount=_sum_order_amount(o),
                currency="KRW",
                order_place_id=None,
                order_place_name=None,
                inflow_path=None,
                inflow_path_detail=None,
                referer=None,
                source_raw=None,
                meta_json=o,
            )

        self.repo.set_meta(cursor_key, date_to)

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        return

    async def fetch_metrics_intraday(self, day: str) -> None:
        return

    async def apply_action(self, proposal: dict) -> dict:
        raise NotImplementedError
