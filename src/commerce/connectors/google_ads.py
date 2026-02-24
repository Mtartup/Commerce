from __future__ import annotations

import asyncio
import json
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

    # ------------------------------------------------------------------ #
    # Write helpers                                                        #
    # ------------------------------------------------------------------ #

    def _payload(self, proposal: dict) -> dict:
        """Extract and parse payload_json from a proposal dict."""
        raw = proposal.get("payload_json") or "{}"
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def _query_single(self, client: Any, cid: str, gaql: str) -> Any:
        """Run a GAQL query and return the first result row, or None."""
        ga_service = client.get_service("GoogleAdsService")
        q = gaql.strip()
        if "LIMIT" not in q.upper():
            q = q + " LIMIT 1"
        response = ga_service.search(customer_id=cid, query=q)
        for row in response:
            return row
        return None

    def _apply_pause(self, client: Any, cid: str, proposal: dict, payload: dict) -> dict:
        entity_type = str(proposal.get("entity_type") or "").lower().strip()
        entity_id = str(proposal.get("entity_id") or "").strip()
        op_str = str(payload.get("op") or "pause").lower()
        new_status_name = "ENABLED" if op_str in {"enable", "resume", "unpause"} else "PAUSED"

        if entity_type == "campaign":
            row = self._query_single(
                client, cid,
                f"SELECT campaign.status FROM campaign WHERE campaign.id = {entity_id}",
            )
            before_status = (
                str(getattr(getattr(row, "campaign", None), "status", "UNKNOWN") or "UNKNOWN")
                if row else "UNKNOWN"
            )
            svc = client.get_service("CampaignService")
            op = client.get_type("CampaignOperation")
            op.update.resource_name = f"customers/{cid}/campaigns/{entity_id}"
            op.update.status = getattr(client.enums.CampaignStatusEnum, new_status_name)
            op.update_mask.paths.extend(["status"])
            resp = svc.mutate_campaigns(customer_id=cid, operations=[op])
            resource_name = (
                resp.results[0].resource_name
                if resp.results
                else f"customers/{cid}/campaigns/{entity_id}"
            )

        elif entity_type == "adgroup":
            row = self._query_single(
                client, cid,
                f"SELECT ad_group.status FROM ad_group WHERE ad_group.id = {entity_id}",
            )
            before_status = (
                str(getattr(getattr(row, "ad_group", None), "status", "UNKNOWN") or "UNKNOWN")
                if row else "UNKNOWN"
            )
            svc = client.get_service("AdGroupService")
            op = client.get_type("AdGroupOperation")
            op.update.resource_name = f"customers/{cid}/adGroups/{entity_id}"
            op.update.status = getattr(client.enums.AdGroupStatusEnum, new_status_name)
            op.update_mask.paths.extend(["status"])
            resp = svc.mutate_ad_groups(customer_id=cid, operations=[op])
            resource_name = (
                resp.results[0].resource_name
                if resp.results
                else f"customers/{cid}/adGroups/{entity_id}"
            )

        elif entity_type == "keyword":
            ad_group_id = str(payload.get("parent_id") or "").strip()
            if not ad_group_id:
                row_ag = self._query_single(
                    client, cid,
                    f"SELECT ad_group.id FROM keyword_view "
                    f"WHERE ad_group_criterion.criterion_id = {entity_id}",
                )
                if not row_ag:
                    raise RuntimeError(
                        f"Cannot find ad_group for keyword criterion_id={entity_id}"
                    )
                ad_group_id = str(
                    getattr(getattr(row_ag, "ad_group", None), "id", "") or ""
                ).strip()
                if not ad_group_id:
                    raise RuntimeError(
                        f"Cannot resolve ad_group_id for keyword {entity_id}"
                    )
            row_s = self._query_single(
                client, cid,
                f"SELECT ad_group_criterion.status FROM ad_group_criterion "
                f"WHERE ad_group_criterion.criterion_id = {entity_id} "
                f"AND ad_group.id = {ad_group_id}",
            )
            before_status = (
                str(
                    getattr(getattr(row_s, "ad_group_criterion", None), "status", "UNKNOWN")
                    or "UNKNOWN"
                )
                if row_s else "UNKNOWN"
            )
            resource_name = f"customers/{cid}/adGroupCriteria/{ad_group_id}~{entity_id}"
            svc = client.get_service("AdGroupCriterionService")
            op = client.get_type("AdGroupCriterionOperation")
            op.update.resource_name = resource_name
            op.update.status = getattr(client.enums.AdGroupCriterionStatusEnum, new_status_name)
            op.update_mask.paths.extend(["status"])
            svc.mutate_ad_group_criteria(customer_id=cid, operations=[op])

        else:
            raise RuntimeError(f"Unsupported entity_type for pause_entity: {entity_type!r}")

        return {
            "action": "pause_entity",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": {"status": before_status},
            "after": {"status": new_status_name},
            "resource_name": resource_name,
        }

    def _apply_set_budget(self, client: Any, cid: str, proposal: dict, payload: dict) -> dict:
        entity_id = str(proposal.get("entity_id") or "").strip()
        new_budget_krw = int(payload.get("budget") or 0)
        new_amount_micros = new_budget_krw * 1_000_000

        # Step 1: get the campaign_budget resource name
        row = self._query_single(
            client, cid,
            f"SELECT campaign.campaign_budget FROM campaign WHERE campaign.id = {entity_id}",
        )
        if not row:
            raise RuntimeError(f"Campaign not found: entity_id={entity_id}")
        budget_resource = str(
            getattr(getattr(row, "campaign", None), "campaign_budget", "") or ""
        ).strip()
        if not budget_resource:
            raise RuntimeError(f"No campaign_budget resource for campaign {entity_id}")

        # Step 2: get current amount (before state)
        budget_id = budget_resource.rsplit("/", 1)[-1]
        row_b = self._query_single(
            client, cid,
            f"SELECT campaign_budget.amount_micros FROM campaign_budget "
            f"WHERE campaign_budget.id = {budget_id}",
        )
        before_micros = (
            int(getattr(getattr(row_b, "campaign_budget", None), "amount_micros", 0) or 0)
            if row_b else 0
        )

        # Step 3: mutate
        svc = client.get_service("CampaignBudgetService")
        op = client.get_type("CampaignBudgetOperation")
        op.update.resource_name = budget_resource
        op.update.amount_micros = new_amount_micros
        op.update_mask.paths.extend(["amount_micros"])
        svc.mutate_campaign_budgets(customer_id=cid, operations=[op])

        return {
            "action": "set_budget",
            "entity_type": "campaign",
            "entity_id": entity_id,
            "before": {"amount_micros": before_micros, "budget_krw": before_micros // 1_000_000},
            "after": {"amount_micros": new_amount_micros, "budget_krw": new_budget_krw},
            "resource_name": budget_resource,
        }

    def _apply_set_bid(self, client: Any, cid: str, proposal: dict, payload: dict) -> dict:
        entity_type = str(proposal.get("entity_type") or "").lower().strip()
        entity_id = str(proposal.get("entity_id") or "").strip()
        new_bid_krw = int(payload.get("bid") or 0)
        new_cpc_micros = new_bid_krw * 1_000_000

        if entity_type == "adgroup":
            row = self._query_single(
                client, cid,
                f"SELECT ad_group.cpc_bid_micros FROM ad_group WHERE ad_group.id = {entity_id}",
            )
            before_micros = (
                int(getattr(getattr(row, "ad_group", None), "cpc_bid_micros", 0) or 0)
                if row else 0
            )
            svc = client.get_service("AdGroupService")
            op = client.get_type("AdGroupOperation")
            op.update.resource_name = f"customers/{cid}/adGroups/{entity_id}"
            op.update.cpc_bid_micros = new_cpc_micros
            op.update_mask.paths.extend(["cpc_bid_micros"])
            resp = svc.mutate_ad_groups(customer_id=cid, operations=[op])
            resource_name = (
                resp.results[0].resource_name
                if resp.results
                else f"customers/{cid}/adGroups/{entity_id}"
            )

        elif entity_type == "keyword":
            ad_group_id = str(payload.get("parent_id") or "").strip()
            if not ad_group_id:
                row_ag = self._query_single(
                    client, cid,
                    f"SELECT ad_group.id FROM keyword_view "
                    f"WHERE ad_group_criterion.criterion_id = {entity_id}",
                )
                if not row_ag:
                    raise RuntimeError(
                        f"Cannot find ad_group for keyword criterion_id={entity_id}"
                    )
                ad_group_id = str(
                    getattr(getattr(row_ag, "ad_group", None), "id", "") or ""
                ).strip()
                if not ad_group_id:
                    raise RuntimeError(
                        f"Cannot resolve ad_group_id for keyword {entity_id}"
                    )
            row = self._query_single(
                client, cid,
                f"SELECT ad_group_criterion.cpc_bid_micros FROM ad_group_criterion "
                f"WHERE ad_group_criterion.criterion_id = {entity_id} "
                f"AND ad_group.id = {ad_group_id}",
            )
            before_micros = (
                int(getattr(getattr(row, "ad_group_criterion", None), "cpc_bid_micros", 0) or 0)
                if row else 0
            )
            resource_name = f"customers/{cid}/adGroupCriteria/{ad_group_id}~{entity_id}"
            svc = client.get_service("AdGroupCriterionService")
            op = client.get_type("AdGroupCriterionOperation")
            op.update.resource_name = resource_name
            op.update.cpc_bid_micros = new_cpc_micros
            op.update_mask.paths.extend(["cpc_bid_micros"])
            svc.mutate_ad_group_criteria(customer_id=cid, operations=[op])

        else:
            raise RuntimeError(f"Unsupported entity_type for set_bid: {entity_type!r}")

        return {
            "action": "set_bid",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": {"cpc_bid_micros": before_micros, "bid_krw": before_micros // 1_000_000},
            "after": {"cpc_bid_micros": new_cpc_micros, "bid_krw": new_bid_krw},
            "resource_name": resource_name,
        }

    def _apply_add_negatives(self, client: Any, cid: str, proposal: dict, payload: dict) -> dict:
        entity_type = str(proposal.get("entity_type") or "").lower().strip()
        entity_id = str(proposal.get("entity_id") or "").strip()
        keywords = list(payload.get("keywords") or [])
        added_resource_names: list[str] = []

        if entity_type == "campaign":
            svc = client.get_service("CampaignCriterionService")
            operations = []
            for kw in keywords:
                text = str(kw.get("text") or "").strip()
                if not text:
                    continue
                match_type_str = str(kw.get("match_type") or "EXACT").upper()
                op = client.get_type("CampaignCriterionOperation")
                c = op.create
                c.campaign = f"customers/{cid}/campaigns/{entity_id}"
                c.negative = True
                c.keyword.text = text
                c.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type_str)
                operations.append(op)
            if operations:
                resp = svc.mutate_campaign_criteria(customer_id=cid, operations=operations)
                added_resource_names = [r.resource_name for r in resp.results]

        elif entity_type == "adgroup":
            svc = client.get_service("AdGroupCriterionService")
            operations = []
            for kw in keywords:
                text = str(kw.get("text") or "").strip()
                if not text:
                    continue
                match_type_str = str(kw.get("match_type") or "EXACT").upper()
                op = client.get_type("AdGroupCriterionOperation")
                c = op.create
                c.ad_group = f"customers/{cid}/adGroups/{entity_id}"
                c.negative = True
                c.keyword.text = text
                c.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type_str)
                operations.append(op)
            if operations:
                resp = svc.mutate_ad_group_criteria(customer_id=cid, operations=operations)
                added_resource_names = [r.resource_name for r in resp.results]

        else:
            raise RuntimeError(f"Unsupported entity_type for add_negatives: {entity_type!r}")

        return {
            "action": "add_negatives",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "before": {"count": 0},
            "after": {"count": len(added_resource_names), "added": added_resource_names},
            "resource_name": f"customers/{cid}/{entity_type}s/{entity_id}",
        }

    def _apply_action_api(self, proposal: dict) -> dict:
        """Synchronous dispatcher for API write actions. Called from asyncio.to_thread."""
        client = self._google_client()
        cid = self._google_customer_id()
        if not cid:
            raise RuntimeError("Missing Google Ads customer ID")

        action_type = str(proposal.get("action_type") or "").strip()
        payload = self._payload(proposal)

        if action_type == "pause_entity":
            return self._apply_pause(client, cid, proposal, payload)
        elif action_type == "set_budget":
            return self._apply_set_budget(client, cid, proposal, payload)
        elif action_type == "set_bid":
            return self._apply_set_bid(client, cid, proposal, payload)
        elif action_type == "add_negatives":
            return self._apply_add_negatives(client, cid, proposal, payload)
        else:
            raise ValueError(f"Unsupported action_type for Google Ads: {action_type!r}")

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
        # API mode
        return await asyncio.to_thread(self._apply_action_api, proposal)
