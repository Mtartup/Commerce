from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir

_BASE_URL = "https://ca-api.cafe24data.com"


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _load_fixture_json(path: Path, name: str) -> list[dict[str, Any]]:
    p = path / name
    if not p.exists():
        return []
    data = json.loads(p.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    return []



def _to_date_kst(ts: str) -> str:
    """Extract YYYY-MM-DD from an ISO-ish timestamp, fallback to today KST."""
    if not ts:
        return datetime.now(tz=ZoneInfo("Asia/Seoul")).date().isoformat()
    return ts[:10]

class _Cafe24AnalyticsClient:
    """Thin wrapper around Cafe24 Analytics API with OAuth 2.0 and rate limiting."""

    def __init__(self, connector_id: str, repo) -> None:
        self.connector_id = connector_id
        self.repo = repo
        self.client_id = (os.getenv("CAFE24_ANALYTICS_CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("CAFE24_ANALYTICS_CLIENT_SECRET") or "").strip()
        self.mall_id = (os.getenv("CAFE24_ANALYTICS_MALL_ID") or "").strip()
        self._token: str | None = None
        self._token_expires: float = 0.0

    def _load_tokens(self) -> dict[str, Any]:
        """Load stored tokens from connector config in DB."""
        conn_row = self.repo.get_connector(self.connector_id)
        if not conn_row:
            return {}
        config = json.loads(conn_row.get("config_json") or "{}")
        return config.get("oauth_tokens", {})

    def _save_tokens(self, tokens: dict[str, Any]) -> None:
        """Persist tokens back to connector config."""
        conn_row = self.repo.get_connector(self.connector_id)
        if not conn_row:
            return
        config = json.loads(conn_row.get("config_json") or "{}")
        config["oauth_tokens"] = tokens
        self.repo.update_connector_config(self.connector_id, config)

    async def _ensure_token(self) -> str:
        """Get a valid access token, refreshing if needed."""
        now = time.time()
        if self._token and now < self._token_expires - 60:
            return self._token

        tokens = self._load_tokens()

        # Bootstrap from .env if DB has no tokens yet
        if not tokens:
            env_access = (os.getenv("CAFE24_ANALYTICS_ACCESS_TOKEN") or "").strip()
            env_refresh = (os.getenv("CAFE24_ANALYTICS_REFRESH_TOKEN") or "").strip()
            if env_access or env_refresh:
                tokens = {
                    "access_token": env_access,
                    "refresh_token": env_refresh,
                    "expires_at": 0,  # treat as expired so refresh runs
                }
                self._save_tokens(tokens)

        access_token = tokens.get("access_token", "")
        expires_at = float(tokens.get("expires_at", 0))
        refresh_token = tokens.get("refresh_token", "")

        if access_token and now < expires_at - 60:
            self._token = access_token
            self._token_expires = expires_at
            return access_token

        if not refresh_token:
            raise RuntimeError(
                "Cafe24 Analytics: no refresh_token available. "
                "Complete the OAuth flow first (authorization_code grant)."
            )

        # Refresh the token
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"https://{self.mall_id}.cafe24api.com/api/v2/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                auth=(self.client_id, self.client_secret),
            )
            resp.raise_for_status()
            body = resp.json()

        new_access = body["access_token"]
        new_refresh = body.get("refresh_token", refresh_token)
        expires_in = int(body.get("expires_in", 7200))
        new_expires_at = now + expires_in

        self._save_tokens({
            "access_token": new_access,
            "refresh_token": new_refresh,
            "expires_at": new_expires_at,
        })
        self._token = new_access
        self._token_expires = new_expires_at
        return new_access

    async def request_json(
        self, method: str, path: str, params: dict | None = None
    ) -> Any:
        token = await self._ensure_token()
        url = _BASE_URL + path
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Cafe24-Api-Version": "2024-06-01",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(method, url, headers=headers, params=params)

            # Rate limit handling: token bucket (40 tokens, 2/sec refill)
            remaining = resp.headers.get("X-RateLimit-Remaining")
            if remaining is not None and int(remaining) < 5:
                await asyncio.sleep(2.0)

            resp.raise_for_status()
            return resp.json()

    async def admin_request_json(
        self, method: str, path: str, params: dict | None = None
    ) -> Any:
        """Admin API call (different base URL and API version from Analytics)."""
        token = await self._ensure_token()
        url = f"https://{self.mall_id}.cafe24api.com{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Cafe24-Api-Version": "2025-12-01",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.request(method, url, headers=headers, params=params)

            remaining = resp.headers.get("X-Api-Call-Limit")
            if remaining:
                # Format: "N/M" — sleep if close to limit
                parts = remaining.split("/")
                if len(parts) == 2:
                    used, total = int(parts[0]), int(parts[1])
                    if total - used < 5:
                        await asyncio.sleep(2.0)

            resp.raise_for_status()
            return resp.json()


class Cafe24AnalyticsConnector:
    """
    Cafe24 Analytics connector.

    Partner approval required — code prepared, API testing deferred.

    Modes:
    - "import": no-op
    - "fixture": load sample data from fixtures/cafe24_analytics/sample/
    - "api": OAuth 2.0 authenticated calls to Cafe24 Analytics API
    """

    capabilities = ConnectorCapabilities(
        read_metrics=True,
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
        cid = (os.getenv("CAFE24_ANALYTICS_CLIENT_ID") or "").strip()
        cs = (os.getenv("CAFE24_ANALYTICS_CLIENT_SECRET") or "").strip()
        mid = (os.getenv("CAFE24_ANALYTICS_MALL_ID") or "").strip()
        if not cid:
            return False, "Missing CAFE24_ANALYTICS_CLIENT_ID"
        if not cs:
            return False, "Missing CAFE24_ANALYTICS_CLIENT_SECRET"
        if not mid:
            return False, "Missing CAFE24_ANALYTICS_MALL_ID"
        return True, None

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode != "api":
            return

        client = _Cafe24AnalyticsClient(self.ctx.connector_id, self.repo)
        cursor_key = f"cafe24:{self.ctx.connector_id}:last_order_date"
        last_sync = self.repo.get_meta(cursor_key)

        now_kst = datetime.now(tz=ZoneInfo("Asia/Seoul"))
        date_from = last_sync or (now_kst.date() - timedelta(days=30)).isoformat()
        date_to = now_kst.date().isoformat()

        offset = 0
        limit = 100
        total_synced = 0
        while True:
            data = await client.admin_request_json(
                "GET", "/api/v2/admin/orders",
                params={
                    "start_date": date_from,
                    "end_date": date_to,
                    "limit": str(limit),
                    "offset": str(offset),
                },
            )
            orders = data.get("orders", [])
            if not orders:
                break

            for o in orders:
                oid = str(o.get("order_id", ""))
                if not oid:
                    continue
                # status: first item's status_text, fallback to paid/canceled
                items = o.get("items") or []
                status = items[0].get("status_text") if items else None
                if not status:
                    if o.get("canceled") == "T":
                        status = "취소"
                    elif o.get("paid") == "T":
                        status = "결제완료"

                self.repo.upsert_store_order(
                    store="cafe24",
                    order_id=oid,
                    ordered_at=o.get("order_date"),
                    date_kst=_to_date_kst(o.get("order_date", "")),
                    status=status,
                    amount=_parse_float(o.get("payment_amount")),
                    currency=o.get("currency", "KRW"),
                    order_place_id=o.get("order_place_id"),
                    order_place_name=o.get("order_place_name"),
                    inflow_path=o.get("market_id"),
                    inflow_path_detail=None,
                    referer=None,
                    source_raw=None,
                    meta_json=o,
                )
                total_synced += 1

            if len(orders) < limit:
                break
            offset += limit
            await asyncio.sleep(0.5)

        self.repo.set_meta(cursor_key, date_to)

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            self._ingest_fixture_data(date_from, date_to)
            return

        # API mode
        client = _Cafe24AnalyticsClient(self.ctx.connector_id, self.repo)
        mall_id = (os.getenv("CAFE24_ANALYTICS_MALL_ID") or "").strip()

        base_params = {"mall_id": mall_id, "start_date": date_from, "end_date": date_to}

        # Visitors (impressions = visit count) — daily breakdown
        visitors = await client.request_json(
            "GET", "/visitors/view", params=base_params,
        )
        # Build a lookup so we can merge pageview data later
        visitor_by_day: dict[str, dict[str, Any]] = {}
        for item in visitors.get("view", []):
            day = item.get("date", "")[:10]  # "2026-02-10T00:00+09:00" → "2026-02-10"
            if not day:
                continue
            visitor_by_day[day] = item

        # Pageviews — daily breakdown
        pageviews = await client.request_json(
            "GET", "/visitors/pageview", params=base_params,
        )
        pv_by_day: dict[str, int] = {}
        for item in pageviews.get("pageview", []):
            day = item.get("date", "")[:10]
            if day:
                pv_by_day[day] = _parse_int(item.get("page_view")) or 0

        # Merge visitors + pageviews into store-level daily metrics
        all_days = sorted(set(visitor_by_day) | set(pv_by_day))
        for day in all_days:
            v = visitor_by_day.get(day, {})
            self.repo.upsert_metric_daily(
                platform="cafe24_analytics",
                account_id=mall_id,
                entity_type="store",
                entity_id=mall_id,
                day=day,
                spend=None,
                impressions=_parse_int(v.get("visit_count")),
                clicks=pv_by_day.get(day),
                conversions=None,
                conversion_value=None,
                metrics_json={
                    "source": "visitors",
                    "first_visit_count": v.get("first_visit_count"),
                    "re_visit_count": v.get("re_visit_count"),
                    "page_view": pv_by_day.get(day),
                },
            )
        await asyncio.sleep(0.5)

        # Product sales (aggregated over period)
        sales = await client.request_json(
            "GET", "/products/sales", params=base_params,
        )
        for item in sales.get("sales", []):
            product_id = str(item.get("product_no", "unknown"))
            self.repo.upsert_metric_daily(
                platform="cafe24_analytics",
                account_id=mall_id,
                entity_type="product",
                entity_id=product_id,
                day=date_to,  # aggregated: store on last date
                spend=None,
                impressions=None,
                clicks=None,
                conversions=_parse_float(item.get("order_count")),
                conversion_value=_parse_float(item.get("order_amount")),
                metrics_json={"source": "products/sales", **item},
            )
        await asyncio.sleep(0.5)

        # Domain referrals (aggregated over period)
        domains = await client.request_json(
            "GET", "/visitpaths/domains", params=base_params,
        )
        for item in domains.get("domains", []):
            domain = str(item.get("domain", "unknown"))
            self.repo.upsert_metric_daily(
                platform="cafe24_analytics",
                account_id=mall_id,
                entity_type="domain",
                entity_id=domain,
                day=date_to,  # aggregated: store on last date
                spend=None,
                impressions=None,
                clicks=_parse_int(item.get("visit_count")),
                conversions=None,
                conversion_value=None,
                metrics_json={"source": "visitpaths/domains", **item},
            )

    def _ingest_fixture_data(self, date_from: str, date_to: str) -> None:
        d = fixture_dir(self.ctx.platform, self.ctx.config)

        # visitors.json
        for item in _load_fixture_json(d, "visitors.json"):
            day = item.get("date", date_from)
            self.repo.upsert_metric_daily(
                platform="cafe24_analytics",
                account_id="fixture",
                entity_type="store",
                entity_id="fixture_mall",
                day=day,
                spend=None,
                impressions=_parse_int(item.get("visitCount")),
                clicks=_parse_int(item.get("pageviewCount")),
                conversions=None,
                conversion_value=None,
                metrics_json=item,
            )

        # sales.json
        for item in _load_fixture_json(d, "sales.json"):
            day = item.get("date", date_from)
            product_id = str(item.get("productNo", "fixture_product"))
            self.repo.upsert_metric_daily(
                platform="cafe24_analytics",
                account_id="fixture",
                entity_type="product",
                entity_id=product_id,
                day=day,
                spend=None,
                impressions=None,
                clicks=None,
                conversions=_parse_float(item.get("orderCount")),
                conversion_value=_parse_float(item.get("salesAmount")),
                metrics_json=item,
            )

    async def fetch_metrics_intraday(self, day: str) -> None:
        return

    async def apply_action(self, proposal: dict) -> dict:
        raise NotImplementedError
