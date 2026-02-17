from __future__ import annotations

from datetime import date

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.fixtures import fixture_dir, load_entities, load_metrics_daily_rows, load_metrics_intraday_rows


class TikTokAdsConnector:
    """
    TikTok Ads connector (Marketing API).
    Not in use yet, but the system is designed to attach this without rewriting core logic.
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

    async def health_check(self) -> tuple[bool, str | None]:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode in {"import", "fixture"}:
            return True, None
        return False, "not implemented"

    async def sync_entities(self) -> None:
        mode = str(self.ctx.config.get("mode", "import")).strip().lower()
        if mode != "fixture":
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
        if mode != "fixture":
            return
        d0 = date.fromisoformat(date_from)
        d1 = date.fromisoformat(date_to)
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
