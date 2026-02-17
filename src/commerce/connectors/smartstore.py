from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import bcrypt
import httpx

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir

_BASE_URL = "https://api.commerce.naver.com"


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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


class _SmartStoreClient:
    """Thin wrapper around Naver Commerce API with bcrypt-based auth."""

    def __init__(self) -> None:
        self.client_id = (os.getenv("SMARTSTORE_CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("SMARTSTORE_CLIENT_SECRET") or "").strip()
        self._token: str | None = None
        self._token_expires: float = 0.0

    async def _get_token(self) -> str:
        """Obtain (or reuse cached) OAuth access token via bcrypt signature."""
        now = time.time()
        if self._token and now < self._token_expires - 60:
            return self._token

        timestamp_ms = str(int(now * 1000))
        password = f"{self.client_id}_{timestamp_ms}"
        hashed = bcrypt.hashpw(
            password.encode("utf-8"),
            self.client_secret.encode("utf-8"),
        )
        client_secret_sign = base64.urlsafe_b64encode(hashed).decode("utf-8")

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{_BASE_URL}/external/v1/oauth2/token",
                data={
                    "client_id": self.client_id,
                    "timestamp": timestamp_ms,
                    "client_secret_sign": client_secret_sign,
                    "grant_type": "client_credentials",
                    "type": "SELF",
                },
            )
            resp.raise_for_status()
            body = resp.json()

        self._token = body["access_token"]
        expires_in = int(body.get("expires_in", 10800))
        self._token_expires = now + expires_in
        return self._token

    async def request_json(
        self, method: str, uri: str, params: dict | None = None, body: Any = None
    ) -> Any:
        token = await self._get_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = _BASE_URL + uri

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(
                method,
                url,
                headers=headers,
                params=params,
                json=body,
            )
            resp.raise_for_status()
            return resp.json()


class SmartStoreConnector:
    """
    Naver Smart Store connector — Commerce API.

    Modes:
    - "import": CSV import only (sync_entities is a no-op)
    - "fixture": load sample orders from fixtures/smartstore/sample/orders.json
    - "api": bcrypt-authenticated calls to Naver Commerce API
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
        cid = (os.getenv("SMARTSTORE_CLIENT_ID") or "").strip()
        cs = (os.getenv("SMARTSTORE_CLIENT_SECRET") or "").strip()
        if not cid:
            return False, "Missing SMARTSTORE_CLIENT_ID"
        if not cs:
            return False, "Missing SMARTSTORE_CLIENT_SECRET"
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
                    store="smartstore",
                    order_id=str(o.get("productOrderId") or o.get("order_id") or ""),
                    ordered_at=o.get("orderDate") or o.get("ordered_at"),
                    date_kst=_to_date_kst(o.get("orderDate") or o.get("ordered_at") or o.get("date_kst", "")),
                    status=o.get("productOrderStatus") or o.get("status"),
                    amount=_parse_float(o.get("totalPaymentAmount") or o.get("amount")),
                    currency=o.get("currency", "KRW"),
                    order_place_id=o.get("orderPlaceId") or o.get("order_place_id"),
                    order_place_name=o.get("orderPlaceName") or o.get("order_place_name"),
                    inflow_path=None,
                    inflow_path_detail=None,
                    referer=None,
                    source_raw=None,
                    meta_json=o,
                )
            return

        # API mode — incremental polling via lastChangedFrom
        # Naver Commerce API limits each query window to max 24 hours.
        client = _SmartStoreClient()
        cursor_key = f"smartstore:{self.ctx.connector_id}:last_changed_from"
        last_changed = self.repo.get_meta(cursor_key)

        now_kst = datetime.now(tz=ZoneInfo("Asia/Seoul"))
        if last_changed:
            window_start = datetime.fromisoformat(last_changed)
        else:
            window_start = now_kst - timedelta(days=30)

        all_po_ids: list[str] = []

        # Step 1: collect changed product order IDs in 24h windows
        while window_start < now_kst:
            window_end = min(window_start + timedelta(hours=23, minutes=59), now_kst)
            cf = window_start.strftime("%Y-%m-%dT%H:%M:%S.000+09:00")
            ct = window_end.strftime("%Y-%m-%dT%H:%M:%S.000+09:00")

            data = await client.request_json(
                "GET",
                "/external/v1/pay-order/seller/product-orders/last-changed-statuses",
                params={"lastChangedFrom": cf, "lastChangedTo": ct},
            )
            changed_items = data.get("data", {}).get("lastChangeStatuses", [])
            for item in changed_items:
                pid = item.get("productOrderId")
                if pid:
                    all_po_ids.append(str(pid))

            window_start = window_end
            if window_start < now_kst:
                await asyncio.sleep(0.3)

        if not all_po_ids:
            self.repo.set_meta(cursor_key, now_kst.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"))
            return

        # Deduplicate while preserving order
        seen: set[str] = set()
        po_ids: list[str] = []
        for pid in all_po_ids:
            if pid not in seen:
                seen.add(pid)
                po_ids.append(pid)

        # Step 2: batch query product order details (max 300 per request)
        for i in range(0, len(po_ids), 300):
            batch = po_ids[i : i + 300]
            detail_data = await client.request_json(
                "POST",
                "/external/v1/pay-order/seller/product-orders/query",
                body={"productOrderIds": batch},
            )
            orders = detail_data.get("data", [])
            for o in orders:
                po = o.get("productOrder", o)
                po_id = str(po.get("productOrderId", ""))
                if not po_id:
                    continue
                self.repo.upsert_store_order(
                    store="smartstore",
                    order_id=po_id,
                    ordered_at=po.get("orderDate"),
                    date_kst=_to_date_kst(po.get("orderDate", "")),
                    status=po.get("productOrderStatus"),
                    amount=_parse_float(po.get("totalPaymentAmount")),
                    currency="KRW",
                    order_place_id=po.get("orderPlaceId"),
                    order_place_name=po.get("orderPlaceName"),
                    inflow_path=None,
                    inflow_path_detail=None,
                    referer=None,
                    source_raw=None,
                    meta_json=po,
                )
            if i + 300 < len(po_ids):
                await asyncio.sleep(0.2)

        self.repo.set_meta(cursor_key, now_kst.strftime("%Y-%m-%dT%H:%M:%S.000+09:00"))

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        return

    async def fetch_metrics_intraday(self, day: str) -> None:
        return

    async def apply_action(self, proposal: dict) -> dict:
        raise NotImplementedError
