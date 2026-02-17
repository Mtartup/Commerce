from __future__ import annotations

import asyncio
import os
import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir, load_entities, load_metrics_daily_rows, load_metrics_intraday_rows


def _normalize_customer_id(raw: str) -> str:
    # Google Ads customer id is digits only (UI shows hyphens).
    return re.sub(r"\D+", "", str(raw or ""))


def _safe_levels(raw: Any) -> list[str]:
    if isinstance(raw, list):
        levels = [str(x).strip().lower() for x in raw]
    elif isinstance(raw, str) and raw.strip():
        levels = [s.strip().lower() for s in raw.split(",")]
    else:
        levels = ["campaign"]
    ok: list[str] = []
    for lv in levels:
        if lv in {"campaign", "adgroup", "keyword"} and lv not in ok:
            ok.append(lv)
    return ok or ["campaign"]


def _to_float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _cost_micros_to_currency(cost_micros: Any) -> float:
    return _to_float(cost_micros) / 1_000_000.0


def _date_range(date_from: str, date_to: str) -> tuple[date, date]:
    d0 = date.fromisoformat(date_from)
    d1 = date.fromisoformat(date_to)
    if d1 < d0:
        d0, d1 = d1, d0
    return d0, d1


class GoogleAdsConnector:
    """
    Google Ads connector.

    API mode uses GAQL via the official `google-ads` Python client.
    """

    capabilities = ConnectorCapabilities(
        read_metrics=True,
        read_entities=True,
        write_pause=True,
        write_budget=True,
        write_bid=True,
        write_negatives=True,
    )

    def __init__(self, ctx: ConnectorContext, repo):
        self.ctx = ctx
        self.repo = repo

    def _google_client(self):
        try:
            from google.ads.googleads.client import GoogleAdsClient  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise RuntimeError("Missing dependency: google-ads") from e

        developer_token = (os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip()
        client_id = (os.getenv("GOOGLE_ADS_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("GOOGLE_ADS_CLIENT_SECRET") or "").strip()
        refresh_token = (os.getenv("GOOGLE_ADS_REFRESH_TOKEN") or "").strip()
        login_customer_id = _normalize_customer_id(os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or "")

        cfg: dict[str, Any] = {
            "developer_token": developer_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            # Keep proto-plus; it's fine for this project.
            "use_proto_plus": True,
        }
        if login_customer_id:
            cfg["login_customer_id"] = login_customer_id
        return GoogleAdsClient.load_from_dict(cfg)

    def _google_customer_id(self) -> str:
        # Prefer env for single-operator simplicity.
        raw = (os.getenv("GOOGLE_ADS_CUSTOMER_ID") or "").strip()
        if not raw:
            raw = str(self.ctx.config.get("customer_id") or "").strip()
        return _normalize_customer_id(raw)

    async def health_check(self) -> tuple[bool, str | None]:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return True, None
        if mode != "api":
            return False, "bad mode"

        customer_id = self._google_customer_id()
        if not customer_id:
            return False, "Missing GOOGLE_ADS_CUSTOMER_ID (or connector config customer_id)"

        # Required creds
        if not (os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") or "").strip():
            return False, "Missing GOOGLE_ADS_DEVELOPER_TOKEN"
        if not (os.getenv("GOOGLE_ADS_CLIENT_ID") or "").strip():
            return False, "Missing GOOGLE_ADS_CLIENT_ID"
        if not (os.getenv("GOOGLE_ADS_CLIENT_SECRET") or "").strip():
            return False, "Missing GOOGLE_ADS_CLIENT_SECRET"
        if not (os.getenv("GOOGLE_ADS_REFRESH_TOKEN") or "").strip():
            return False, "Missing GOOGLE_ADS_REFRESH_TOKEN"

        # Dependency + client init check
        try:
            self._google_client()
        except Exception as e:  # noqa: BLE001
            return False, f"Google Ads client init failed: {e}"

        return True, None

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            d = fixture_dir(self.ctx.platform, self.ctx.config)
            for e in load_entities(d):
                self.repo.upsert_entity(
                    platform=e.get("platform") or self.ctx.platform,
                    account_id=e.get("account_id"),
                    entity_type=e.get("entity_type") or "",
                    entity_id=e.get("entity_id") or "",
                    parent_type=e.get("parent_type"),
                    parent_id=e.get("parent_id"),
                    name=e.get("name"),
                    status=e.get("status"),
                    meta_json=e.get("meta_json") or {},
                )
            return

        # API mode (best-effort, read-only)
        await asyncio.to_thread(self._sync_entities_api)

    def _sync_entities_api(self) -> None:
        customer_id = self._google_customer_id()
        if not customer_id:
            return
        client = self._google_client()
        ga_service = client.get_service("GoogleAdsService")

        def stream(query: str):
            for batch in ga_service.search_stream(customer_id=customer_id, query=query):
                for row in batch.results:
                    yield row

        # Campaigns
        q_campaigns = """
        SELECT
          campaign.id,
          campaign.name,
          campaign.status
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        """
        for row in stream(q_campaigns):
            cid = str(getattr(row.campaign, "id", "") or "").strip()
            if not cid:
                continue
            self.repo.upsert_entity(
                platform="google",
                account_id=customer_id,
                entity_type="campaign",
                entity_id=cid,
                parent_type=None,
                parent_id=None,
                name=str(getattr(row.campaign, "name", "") or "") or None,
                status=str(getattr(row.campaign, "status", "") or "") or None,
                meta_json={"source": "google_ads_api"},
            )

        # Ad groups
        q_adgroups = """
        SELECT
          campaign.id,
          ad_group.id,
          ad_group.name,
          ad_group.status
        FROM ad_group
        WHERE ad_group.status != 'REMOVED'
        """
        for row in stream(q_adgroups):
            gid = str(getattr(row.ad_group, "id", "") or "").strip()
            if not gid:
                continue
            parent = str(getattr(row.campaign, "id", "") or "").strip() or None
            self.repo.upsert_entity(
                platform="google",
                account_id=customer_id,
                entity_type="adgroup",
                entity_id=gid,
                parent_type="campaign" if parent else None,
                parent_id=parent,
                name=str(getattr(row.ad_group, "name", "") or "") or None,
                status=str(getattr(row.ad_group, "status", "") or "") or None,
                meta_json={"source": "google_ads_api"},
            )

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        if mode == "fixture":
            d0, d1 = _date_range(date_from, date_to)
            d = fixture_dir(self.ctx.platform, self.ctx.config)
            for row in load_metrics_daily_rows(d):
                day = str(row.get("date") or "")
                if not day:
                    continue
                dd = date.fromisoformat(day)
                if dd < d0 or dd > d1:
                    continue
                self.repo.upsert_metric_daily(
                    platform=row.get("platform") or self.ctx.platform,
                    account_id=row.get("account_id"),
                    entity_type=row.get("entity_type") or "",
                    entity_id=row.get("entity_id") or "",
                    day=day,
                    spend=row.get("spend"),
                    impressions=row.get("impressions"),
                    clicks=row.get("clicks"),
                    conversions=row.get("conversions"),
                    conversion_value=row.get("conversion_value"),
                    metrics_json=row.get("metrics_json") or {},
                )
            return

        await asyncio.to_thread(self._fetch_metrics_daily_api, date_from, date_to)

    def _fetch_metrics_daily_api(self, date_from: str, date_to: str) -> None:
        customer_id = self._google_customer_id()
        if not customer_id:
            return

        # Avoid creating load/queries too frequently.
        min_interval_min = float(self.ctx.config.get("api_min_interval_minutes", 60))
        key = f"google:{self.ctx.connector_id}:last_fetch_daily"
        last = self.repo.get_meta(key)
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                now = datetime.now(tz=last_dt.tzinfo) if last_dt.tzinfo else datetime.now()
                if (now - last_dt).total_seconds() < (min_interval_min * 60):
                    return
            except Exception:
                pass

        d0, d1 = _date_range(date_from, date_to)
        include_today = bool(self.ctx.config.get("include_today", False))
        if not include_today:
            tz = ZoneInfo(os.getenv("ADS_TIMEZONE", "Asia/Seoul"))
            today_kst = datetime.now(tz=tz).date()
            if d1 >= today_kst:
                d1 = today_kst - timedelta(days=1)
        if d1 < d0:
            return

        levels = _safe_levels(self.ctx.config.get("ingest_levels"))

        client = self._google_client()
        ga_service = client.get_service("GoogleAdsService")

        def stream(query: str):
            for batch in ga_service.search_stream(customer_id=customer_id, query=query):
                for row in batch.results:
                    yield row

        def upsert_metric(
            *,
            day: str,
            entity_type: str,
            entity_id: str,
            spend: float,
            impressions: int,
            clicks: int,
            conversions: float,
            conversion_value: float,
            conversions_all: float | None,
            conversion_value_all: float | None,
            extra: dict[str, Any],
        ) -> None:
            self.repo.upsert_metric_daily(
                platform="google",
                account_id=customer_id,
                entity_type=entity_type,
                entity_id=entity_id,
                day=day,
                spend=spend,
                impressions=impressions,
                clicks=clicks,
                conversions=conversions,
                conversion_value=conversion_value,
                metrics_json={
                    "source": "google_ads_api",
                    "conversions_all": conversions_all,
                    "conversion_value_all": conversion_value_all,
                    **extra,
                },
            )

        date_from_s = d0.isoformat()
        date_to_s = d1.isoformat()

        if "campaign" in levels:
            q = f"""
            SELECT
              segments.date,
              campaign.id,
              campaign.name,
              campaign.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value,
              metrics.all_conversions,
              metrics.all_conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{date_from_s}' AND '{date_to_s}'
              AND campaign.status != 'REMOVED'
            """
            for row in stream(q):
                day = str(getattr(row.segments, "date", "") or "")
                cid = str(getattr(row.campaign, "id", "") or "").strip()
                if not day or not cid:
                    continue
                self.repo.upsert_entity(
                    platform="google",
                    account_id=customer_id,
                    entity_type="campaign",
                    entity_id=cid,
                    parent_type=None,
                    parent_id=None,
                    name=str(getattr(row.campaign, "name", "") or "") or None,
                    status=str(getattr(row.campaign, "status", "") or "") or None,
                    meta_json={"source": "google_ads_api"},
                )
                upsert_metric(
                    day=day,
                    entity_type="campaign",
                    entity_id=cid,
                    spend=_cost_micros_to_currency(getattr(row.metrics, "cost_micros", 0)),
                    impressions=int(getattr(row.metrics, "impressions", 0) or 0),
                    clicks=int(getattr(row.metrics, "clicks", 0) or 0),
                    conversions=_to_float(getattr(row.metrics, "conversions", 0)),
                    conversion_value=_to_float(getattr(row.metrics, "conversions_value", 0)),
                    conversions_all=_to_float(getattr(row.metrics, "all_conversions", 0)),
                    conversion_value_all=_to_float(getattr(row.metrics, "all_conversions_value", 0)),
                    extra={},
                )

        if "adgroup" in levels:
            q = f"""
            SELECT
              segments.date,
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              ad_group.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value,
              metrics.all_conversions,
              metrics.all_conversions_value
            FROM ad_group
            WHERE segments.date BETWEEN '{date_from_s}' AND '{date_to_s}'
              AND ad_group.status != 'REMOVED'
            """
            for row in stream(q):
                day = str(getattr(row.segments, "date", "") or "")
                gid = str(getattr(row.ad_group, "id", "") or "").strip()
                parent = str(getattr(row.campaign, "id", "") or "").strip() or None
                if not day or not gid:
                    continue
                if parent:
                    self.repo.upsert_entity(
                        platform="google",
                        account_id=customer_id,
                        entity_type="campaign",
                        entity_id=parent,
                        parent_type=None,
                        parent_id=None,
                        name=str(getattr(row.campaign, "name", "") or "") or None,
                        status=None,
                        meta_json={"source": "google_ads_api"},
                    )
                self.repo.upsert_entity(
                    platform="google",
                    account_id=customer_id,
                    entity_type="adgroup",
                    entity_id=gid,
                    parent_type="campaign" if parent else None,
                    parent_id=parent,
                    name=str(getattr(row.ad_group, "name", "") or "") or None,
                    status=str(getattr(row.ad_group, "status", "") or "") or None,
                    meta_json={"source": "google_ads_api"},
                )
                upsert_metric(
                    day=day,
                    entity_type="adgroup",
                    entity_id=gid,
                    spend=_cost_micros_to_currency(getattr(row.metrics, "cost_micros", 0)),
                    impressions=int(getattr(row.metrics, "impressions", 0) or 0),
                    clicks=int(getattr(row.metrics, "clicks", 0) or 0),
                    conversions=_to_float(getattr(row.metrics, "conversions", 0)),
                    conversion_value=_to_float(getattr(row.metrics, "conversions_value", 0)),
                    conversions_all=_to_float(getattr(row.metrics, "all_conversions", 0)),
                    conversion_value_all=_to_float(getattr(row.metrics, "all_conversions_value", 0)),
                    extra={"parent_campaign_id": parent},
                )

        if "keyword" in levels:
            # Keyword_view is keyword-only and provides criterion id + keyword text.
            q = f"""
            SELECT
              segments.date,
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              ad_group_criterion.criterion_id,
              ad_group_criterion.keyword.text,
              ad_group_criterion.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value,
              metrics.all_conversions,
              metrics.all_conversions_value
            FROM keyword_view
            WHERE segments.date BETWEEN '{date_from_s}' AND '{date_to_s}'
              AND ad_group_criterion.status != 'REMOVED'
            """
            for row in stream(q):
                day = str(getattr(row.segments, "date", "") or "")
                kid = str(getattr(row.ad_group_criterion, "criterion_id", "") or "").strip()
                gid = str(getattr(row.ad_group, "id", "") or "").strip() or None
                cid = str(getattr(row.campaign, "id", "") or "").strip() or None
                if not day or not kid:
                    continue
                if cid:
                    self.repo.upsert_entity(
                        platform="google",
                        account_id=customer_id,
                        entity_type="campaign",
                        entity_id=cid,
                        parent_type=None,
                        parent_id=None,
                        name=str(getattr(row.campaign, "name", "") or "") or None,
                        status=None,
                        meta_json={"source": "google_ads_api"},
                    )
                if gid:
                    self.repo.upsert_entity(
                        platform="google",
                        account_id=customer_id,
                        entity_type="adgroup",
                        entity_id=gid,
                        parent_type="campaign" if cid else None,
                        parent_id=cid,
                        name=str(getattr(row.ad_group, "name", "") or "") or None,
                        status=None,
                        meta_json={"source": "google_ads_api"},
                    )

                kw_text = None
                try:
                    kw_text = str(getattr(getattr(row.ad_group_criterion, "keyword", None), "text", "") or "") or None
                except Exception:
                    kw_text = None

                self.repo.upsert_entity(
                    platform="google",
                    account_id=customer_id,
                    entity_type="keyword",
                    entity_id=kid,
                    parent_type="adgroup" if gid else ("campaign" if cid else None),
                    parent_id=gid or cid,
                    name=kw_text,
                    status=str(getattr(row.ad_group_criterion, "status", "") or "") or None,
                    meta_json={"source": "google_ads_api"},
                )
                upsert_metric(
                    day=day,
                    entity_type="keyword",
                    entity_id=kid,
                    spend=_cost_micros_to_currency(getattr(row.metrics, "cost_micros", 0)),
                    impressions=int(getattr(row.metrics, "impressions", 0) or 0),
                    clicks=int(getattr(row.metrics, "clicks", 0) or 0),
                    conversions=_to_float(getattr(row.metrics, "conversions", 0)),
                    conversion_value=_to_float(getattr(row.metrics, "conversions_value", 0)),
                    conversions_all=_to_float(getattr(row.metrics, "all_conversions", 0)),
                    conversion_value_all=_to_float(getattr(row.metrics, "all_conversions_value", 0)),
                    extra={"parent_adgroup_id": gid, "parent_campaign_id": cid, "keyword_text": kw_text},
                )

        self.repo.set_meta(key, datetime.now().astimezone().replace(microsecond=0).isoformat())

    async def fetch_metrics_intraday(self, day: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode != "fixture":
            return
        d = fixture_dir(self.ctx.platform, self.ctx.config)
        for row in load_metrics_intraday_rows(d):
            hour_ts = str(row.get("hour_ts") or "")
            if not hour_ts.startswith(day):
                continue
            self.repo.upsert_metric_intraday(
                platform=row.get("platform") or self.ctx.platform,
                account_id=row.get("account_id"),
                entity_type=row.get("entity_type") or "",
                entity_id=row.get("entity_id") or "",
                hour_ts=hour_ts,
                spend=row.get("spend"),
                impressions=row.get("impressions"),
                clicks=row.get("clicks"),
                conversions=row.get("conversions"),
                conversion_value=row.get("conversion_value"),
                metrics_json=row.get("metrics_json") or {},
            )

    async def apply_action(self, proposal: dict) -> dict:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return {
                "simulated": True,
                "mode": mode,
                "platform": self.ctx.platform,
                "action_type": proposal.get("action_type"),
                "entity_type": proposal.get("entity_type"),
                "entity_id": proposal.get("entity_id"),
            }
        raise NotImplementedError("Google Ads API write actions not implemented yet")
