from __future__ import annotations

import random
from datetime import datetime
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from commerce.connectors.base import ConnectorCapabilities, ConnectorContext
from commerce.util import to_kst_hour_iso


class DemoConnector:
    """
    Generates fake data so the UI/bot/worker flows can be validated without real API keys.
    """

    capabilities = ConnectorCapabilities(
        read_metrics=True,
        read_entities=True,
        write_pause=False,
        write_budget=False,
    )

    def __init__(self, ctx: ConnectorContext, repo):
        self.ctx = ctx
        self.repo = repo

    async def health_check(self) -> tuple[bool, str | None]:
        return True, None

    async def sync_entities(self) -> None:
        return

    async def fetch_metrics_daily(self, date_from: str, date_to: str) -> None:
        start = date.fromisoformat(date_from)
        end = date.fromisoformat(date_to)
        cur = start
        while cur <= end:
            spend = random.uniform(1000, 80000)
            conv = 0.0 if spend > 50000 else random.choice([0.0, 1.0, 2.0])
            self.repo.upsert_metric_daily(
                platform="demo",
                account_id="demo_account",
                entity_type="campaign",
                entity_id="demo_campaign_1",
                day=cur.isoformat(),
                spend=spend,
                impressions=int(spend * 5),
                clicks=int(spend / 100),
                conversions=conv,
                conversion_value=float(conv * 30000),
                metrics_json={"demo": True},
            )
            cur += timedelta(days=1)

    async def fetch_metrics_intraday(self, day: str) -> None:
        # Single-hour "intraday" snapshot for demo.
        now = datetime.now(tz=ZoneInfo("Asia/Seoul"))
        hour_ts = to_kst_hour_iso(now, "Asia/Seoul")
        spend = random.uniform(1000, 80000)
        conv = 0.0 if spend > 50000 else random.choice([0.0, 1.0])
        self.repo.upsert_metric_intraday(
            platform="demo",
            account_id="demo_account",
            entity_type="campaign",
            entity_id="demo_campaign_1",
            hour_ts=hour_ts,
            spend=spend,
            impressions=int(spend * 5),
            clicks=int(spend / 100),
            conversions=conv,
            conversion_value=float(conv * 30000),
            metrics_json={"demo": True},
        )

    async def apply_action(self, proposal: dict) -> dict:
        return {"simulated": True, "platform": "demo", "action_type": proposal.get("action_type")}
