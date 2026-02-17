from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir, load_entities, load_metrics_daily_rows, load_metrics_intraday_rows


class MetaAdsConnector:
    """
    Meta Ads connector (Graph API).

    Design assumes daily ingestion into SQLite to avoid "can't query later" issues.
    """

    capabilities = ConnectorCapabilities(
        read_metrics=True,
        read_entities=True,
        write_pause=True,
        write_budget=True,
        read_creatives=True,
    )

    def __init__(self, ctx: ConnectorContext, repo):
        self.ctx = ctx
        self.repo = repo

    def _account_id(self) -> str:
        # Prefer env for single-operator simplicity; allow per-connector override.
        raw = (os.getenv("META_AD_ACCOUNT_ID") or "").strip()
        if not raw:
            raw = str(self.ctx.config.get("ad_account_id") or "").strip()
        raw = raw.removeprefix("act_").strip()
        # keep digits only (UI sometimes includes separators)
        return re.sub(r"\D+", "", raw)

    def _graph_base_url(self) -> str:
        return (os.getenv("META_GRAPH_BASE_URL") or "https://graph.facebook.com").strip().rstrip("/")

    def _graph_version(self) -> str:
        v = (os.getenv("META_GRAPH_API_VERSION") or "").strip()
        return v if v else "v21.0"

    def _access_token(self) -> str:
        return (os.getenv("META_ACCESS_TOKEN") or "").strip()

    def _appsecret_proof(self) -> str | None:
        # Optional hardening: https://developers.facebook.com/docs/graph-api/securing-requests/
        # Not required for the MVP, but supported if META_APP_SECRET is set.
        app_secret = (os.getenv("META_APP_SECRET") or "").strip()
        token = self._access_token()
        if not app_secret or not token:
            return None
        digest = hmac.new(
            app_secret.encode("utf-8", errors="strict"),
            token.encode("utf-8", errors="strict"),
            hashlib.sha256,
        ).hexdigest()
        return digest

    def _safe_levels(self, raw: Any) -> list[str]:
        if isinstance(raw, list):
            levels = [str(x).strip().lower() for x in raw]
        elif isinstance(raw, str) and raw.strip():
            levels = [s.strip().lower() for s in raw.split(",")]
        else:
            levels = ["campaign", "adset"]
        ok: list[str] = []
        for lv in levels:
            if lv in {"campaign", "adset", "ad"} and lv not in ok:
                ok.append(lv)
        return ok or ["campaign"]

    def _to_float(self, v: Any) -> float:
        try:
            return float(str(v).replace(",", "")) if v is not None else 0.0
        except Exception:
            return 0.0

    def _to_int(self, v: Any) -> int:
        try:
            return int(float(str(v).replace(",", ""))) if v is not None else 0
        except Exception:
            return 0

    def _action_map(self, items: Any) -> dict[str, float]:
        out: dict[str, float] = {}
        if not isinstance(items, list):
            return out
        for it in items:
            if not isinstance(it, dict):
                continue
            t = str(it.get("action_type") or "").strip()
            if not t:
                continue
            out[t] = out.get(t, 0.0) + self._to_float(it.get("value"))
        return out

    def _purchase_action_types(self) -> list[str]:
        raw = self.ctx.config.get("conversion_action_types")
        if isinstance(raw, list):
            lst = [str(x).strip() for x in raw if str(x).strip()]
        elif isinstance(raw, str) and raw.strip():
            lst = [s.strip() for s in raw.split(",") if s.strip()]
        else:
            # Reasonable defaults for ecommerce. Users can override per account.
            lst = [
                "purchase",
                "omni_purchase",
                "offsite_conversion.fb_pixel_purchase",
            ]
        # keep stable order + uniqueness
        out: list[str] = []
        for x in lst:
            if x not in out:
                out.append(x)
        return out

    async def _iter_graph_data(self, *, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Return full data list for a Graph API collection endpoint with cursor pagination.

        We keep it simple (one call site per connector tick) and avoid adding extra deps.
        """
        base = self._graph_base_url()
        ver = self._graph_version()
        token = self._access_token()
        if not token:
            return []

        p = dict(params)
        p["access_token"] = token
        proof = self._appsecret_proof()
        if proof:
            p["appsecret_proof"] = proof

        url: str | None = f"{base}/{ver}/{path.lstrip('/')}"
        out: list[dict[str, Any]] = []
        timeout = float(self.ctx.config.get("http_timeout_sec", 30.0))
        async with httpx.AsyncClient(timeout=timeout) as client:
            next_params: dict[str, Any] | None = p
            while url:
                r = await client.get(url, params=next_params)
                try:
                    obj = r.json()
                except Exception as e:  # noqa: BLE001
                    raise RuntimeError(f"Meta Graph API non-JSON response: {r.status_code}") from e
                if isinstance(obj, dict) and obj.get("error"):
                    err = obj.get("error") or {}
                    msg = str(err.get("message") or "unknown error")
                    code = err.get("code")
                    raise RuntimeError(f"Meta Graph API error: {msg} (code={code})")
                data = obj.get("data") if isinstance(obj, dict) else None
                if isinstance(data, list):
                    for it in data:
                        if isinstance(it, dict):
                            out.append(it)
                paging = obj.get("paging") if isinstance(obj, dict) else None
                next_url = paging.get("next") if isinstance(paging, dict) else None
                url = str(next_url) if next_url else None
                next_params = None  # next URL already includes query params.
        return out

    async def health_check(self) -> tuple[bool, str | None]:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return True, None
        if mode != "api":
            return False, "bad mode"

        if not self._access_token():
            return False, "Missing META_ACCESS_TOKEN"
        if not self._account_id():
            return False, "Missing META_AD_ACCOUNT_ID (or connector config ad_account_id)"
        return True, None

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode != "fixture":
            if mode != "api":
                return
            account_id = self._account_id()
            if not account_id:
                return

            levels = self._safe_levels(self.ctx.config.get("ingest_levels"))
            want_campaigns = "campaign" in levels
            want_adsets = "adset" in levels or "ad" in levels
            want_ads = "ad" in levels

            if want_campaigns:
                camps = await self._iter_graph_data(
                    path=f"act_{account_id}/campaigns",
                    params={
                        "fields": "id,name,status,effective_status,objective",
                        "limit": 200,
                    },
                )
                for c in camps:
                    cid = str(c.get("id") or "").strip()
                    if not cid:
                        continue
                    status = str(c.get("effective_status") or c.get("status") or "").strip() or None
                    self.repo.upsert_entity(
                        platform=self.ctx.platform,
                        account_id=account_id,
                        entity_type="campaign",
                        entity_id=cid,
                        parent_type=None,
                        parent_id=None,
                        name=str(c.get("name") or "").strip() or None,
                        status=status,
                        meta_json={"source": "meta_graph_api", "objective": c.get("objective")},
                    )

            if want_adsets:
                adsets = await self._iter_graph_data(
                    path=f"act_{account_id}/adsets",
                    params={
                        "fields": "id,name,status,effective_status,campaign_id",
                        "limit": 200,
                    },
                )
                for s in adsets:
                    sid = str(s.get("id") or "").strip()
                    if not sid:
                        continue
                    parent = str(s.get("campaign_id") or "").strip() or None
                    status = str(s.get("effective_status") or s.get("status") or "").strip() or None
                    self.repo.upsert_entity(
                        platform=self.ctx.platform,
                        account_id=account_id,
                        entity_type="adset",
                        entity_id=sid,
                        parent_type="campaign" if parent else None,
                        parent_id=parent,
                        name=str(s.get("name") or "").strip() or None,
                        status=status,
                        meta_json={"source": "meta_graph_api"},
                    )

            if want_ads:
                ads = await self._iter_graph_data(
                    path=f"act_{account_id}/ads",
                    params={
                        "fields": "id,name,status,effective_status,campaign_id,adset_id",
                        "limit": 200,
                    },
                )
                for a in ads:
                    aid = str(a.get("id") or "").strip()
                    if not aid:
                        continue
                    parent = str(a.get("adset_id") or "").strip() or None
                    status = str(a.get("effective_status") or a.get("status") or "").strip() or None
                    self.repo.upsert_entity(
                        platform=self.ctx.platform,
                        account_id=account_id,
                        entity_type="ad",
                        entity_id=aid,
                        parent_type="adset" if parent else None,
                        parent_id=parent,
                        name=str(a.get("name") or "").strip() or None,
                        status=status,
                        meta_json={"source": "meta_graph_api", "campaign_id": a.get("campaign_id")},
                    )
            return
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

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode == "import":
            return
        d0 = date.fromisoformat(date_from)
        d1 = date.fromisoformat(date_to)
        if mode == "fixture":
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

        if mode != "api":
            return

        account_id = self._account_id()
        if not account_id:
            return

        # Avoid hammering the API (worker ticks every 5 minutes).
        min_interval_min = float(self.ctx.config.get("api_min_interval_minutes", 60))
        key = f"meta:{self.ctx.connector_id}:last_fetch_daily"
        last = self.repo.get_meta(key)
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                now = datetime.now(tz=last_dt.tzinfo) if last_dt.tzinfo else datetime.now()
                if (now - last_dt).total_seconds() < (min_interval_min * 60):
                    return
            except Exception:
                pass

        include_today = bool(self.ctx.config.get("include_today", False))
        if not include_today:
            tz = ZoneInfo(os.getenv("ADS_TIMEZONE", "Asia/Seoul"))
            today_kst = datetime.now(tz=tz).date()
            if d1 >= today_kst:
                d1 = today_kst - timedelta(days=1)
        if d1 < d0:
            return

        levels = self._safe_levels(self.ctx.config.get("ingest_levels"))
        purchase_types = self._purchase_action_types()

        async def ingest_level(lv: str) -> None:
            fields = [
                "date_start",
                "date_stop",
                "account_id",
                "spend",
                "impressions",
                "clicks",
                "actions",
                "action_values",
            ]
            if lv == "campaign":
                fields.extend(["campaign_id", "campaign_name"])
            elif lv == "adset":
                fields.extend(["campaign_id", "adset_id", "adset_name"])
            else:
                fields.extend(["campaign_id", "adset_id", "ad_id", "ad_name"])

            rows = await self._iter_graph_data(
                path=f"act_{account_id}/insights",
                params={
                    "level": lv,
                    "time_increment": 1,
                    "fields": ",".join(fields),
                    "time_range[since]": d0.isoformat(),
                    "time_range[until]": d1.isoformat(),
                    "limit": 5000,
                },
            )

            for r in rows:
                day = str(r.get("date_start") or "").strip()
                if not day:
                    continue
                # ID/name columns depend on level
                if lv == "campaign":
                    entity_id = str(r.get("campaign_id") or "").strip()
                    name = str(r.get("campaign_name") or "").strip() or None
                    parent_type = None
                    parent_id = None
                elif lv == "adset":
                    entity_id = str(r.get("adset_id") or "").strip()
                    name = str(r.get("adset_name") or "").strip() or None
                    parent_id = str(r.get("campaign_id") or "").strip() or None
                    parent_type = "campaign" if parent_id else None
                else:
                    entity_id = str(r.get("ad_id") or "").strip()
                    name = str(r.get("ad_name") or "").strip() or None
                    parent_id = str(r.get("adset_id") or "").strip() or None
                    parent_type = "adset" if parent_id else None
                if not entity_id:
                    continue

                spend = self._to_float(r.get("spend"))
                impressions = self._to_int(r.get("impressions"))
                clicks = self._to_int(r.get("clicks"))
                actions = self._action_map(r.get("actions"))
                action_values = self._action_map(r.get("action_values"))

                # "All" conversions: if Meta provides an aggregate, use it.
                conv_all = float(actions.get("offsite_conversion") or 0.0)
                value_all = float(action_values.get("offsite_conversion") or 0.0)
                if conv_all <= 0:
                    # fallback: sum any granular offsite conversion actions.
                    conv_all = sum(v for k, v in actions.items() if k.startswith("offsite_conversion."))
                if value_all <= 0:
                    value_all = sum(v for k, v in action_values.items() if k.startswith("offsite_conversion."))

                conv_purchase = sum(actions.get(t, 0.0) for t in purchase_types)
                value_purchase = sum(action_values.get(t, 0.0) for t in purchase_types)

                self.repo.upsert_entity(
                    platform=self.ctx.platform,
                    account_id=account_id,
                    entity_type=lv,
                    entity_id=entity_id,
                    parent_type=parent_type,
                    parent_id=parent_id,
                    name=name,
                    status=None,
                    meta_json={"source": "meta_graph_api"},
                )

                self.repo.upsert_metric_daily(
                    platform=self.ctx.platform,
                    account_id=account_id,
                    entity_type=lv,
                    entity_id=entity_id,
                    day=day,
                    spend=spend,
                    impressions=impressions,
                    clicks=clicks,
                    conversions=conv_purchase,
                    conversion_value=value_purchase,
                    metrics_json={
                        "source": "meta_graph_api",
                        "conversions_all": conv_all,
                        "conversion_value_all": value_all,
                        "conversions_purchase": conv_purchase,
                        "conversion_value_purchase": value_purchase,
                        # Keep raw maps for later tuning without re-pulling.
                        "actions": actions,
                        "action_values": action_values,
                        "purchase_action_types": purchase_types,
                    },
                )

        for lv in levels:
            await ingest_level(lv)

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
        raise NotImplementedError
